/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.ra.inflow;

import javax.jms.MessageListener;
import javax.resource.ResourceException;
import javax.resource.spi.endpoint.MessageEndpoint;
import javax.resource.spi.endpoint.MessageEndpointFactory;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.activemq.api.core.ActiveMQException;
import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.api.core.client.ClientMessage;
import org.apache.activemq.api.core.client.ClientSession.QueueQuery;
import org.apache.activemq.api.core.client.ClientSessionFactory;
import org.apache.activemq.api.core.client.MessageHandler;
import org.apache.activemq.core.client.impl.ClientConsumerInternal;
import org.apache.activemq.core.client.impl.ClientSessionFactoryInternal;
import org.apache.activemq.core.client.impl.ClientSessionInternal;
import org.apache.activemq.jms.client.ActiveMQDestination;
import org.apache.activemq.jms.client.ActiveMQMessage;
import org.apache.activemq.ra.ActiveMQRALogger;
import org.apache.activemq.ra.ActiveMQResourceAdapter;
import org.apache.activemq.service.extensions.ServiceUtils;
import org.apache.activemq.service.extensions.xa.ActiveMQXAResourceWrapper;
import org.apache.activemq.utils.FutureLatch;
import org.apache.activemq.utils.VersionLoader;

/**
 * The message handler
 */
public class ActiveMQMessageHandler implements MessageHandler
{
   /**
    * Trace enabled
    */
   private static boolean trace = ActiveMQRALogger.LOGGER.isTraceEnabled();
   /**
    * The session
    */
   private final ClientSessionInternal session;

   private ClientConsumerInternal consumer;

   /**
    * The endpoint
    */
   private MessageEndpoint endpoint;

   private final ActiveMQActivation activation;

   private boolean useLocalTx;

   private boolean transacted;

   private boolean useXA = false;

   private final int sessionNr;

   private final TransactionManager tm;

   private ClientSessionFactory cf;

   public ActiveMQMessageHandler(final ActiveMQActivation activation,
                                 final TransactionManager tm,
                                 final ClientSessionInternal session,
                                 final ClientSessionFactory cf,
                                 final int sessionNr)
   {
      this.activation = activation;
      this.session = session;
      this.cf = cf;
      this.sessionNr = sessionNr;
      this.tm = tm;
   }

   public void setup() throws Exception
   {
      if (ActiveMQMessageHandler.trace)
      {
         ActiveMQRALogger.LOGGER.trace("setup()");
      }

      ActiveMQActivationSpec spec = activation.getActivationSpec();
      String selector = spec.getMessageSelector();

      // Create the message consumer
      SimpleString selectorString = selector == null || selector.trim().equals("") ? null : new SimpleString(selector);
      if (activation.isTopic() && spec.isSubscriptionDurable())
      {
         SimpleString queueName = new SimpleString(ActiveMQDestination.createQueueNameForDurableSubscription(true,
                                                                                                             spec.getClientID(),
                                                                                                             spec.getSubscriptionName()));

         QueueQuery subResponse = session.queueQuery(queueName);

         if (!subResponse.isExists())
         {
            session.createQueue(activation.getAddress(), queueName, selectorString, true);
         }
         else
         {
            // The check for already exists should be done only at the first session
            // As a deployed MDB could set up multiple instances in order to process messages in parallel.
            if (sessionNr == 0 && subResponse.getConsumerCount() > 0)
            {
               if (!spec.isShareSubscriptions())
               {
                  throw new javax.jms.IllegalStateException("Cannot create a subscriber on the durable subscription since it already has subscriber(s)");
               }
               else if (ActiveMQRALogger.LOGGER.isDebugEnabled())
               {
                  ActiveMQRALogger.LOGGER.debug("the mdb on destination " + queueName + " already had " +
                                                  subResponse.getConsumerCount() +
                                                  " consumers but the MDB is configured to share subscriptions, so no exceptions are thrown");
               }
            }

            SimpleString oldFilterString = subResponse.getFilterString();

            boolean selectorChanged = selector == null && oldFilterString != null ||
               oldFilterString == null &&
                  selector != null ||
               (oldFilterString != null && selector != null && !oldFilterString.toString()
                  .equals(selector));

            SimpleString oldTopicName = subResponse.getAddress();

            boolean topicChanged = !oldTopicName.equals(activation.getAddress());

            if (selectorChanged || topicChanged)
            {
               // Delete the old durable sub
               session.deleteQueue(queueName);

               // Create the new one
               session.createQueue(activation.getAddress(), queueName, selectorString, true);
            }
         }
         consumer = (ClientConsumerInternal) session.createConsumer(queueName, null, false);
      }
      else
      {
         SimpleString tempQueueName;
         if (activation.isTopic())
         {
            if (activation.getTopicTemporaryQueue() == null)
            {
               tempQueueName = new SimpleString(UUID.randomUUID().toString());
               session.createTemporaryQueue(activation.getAddress(), tempQueueName, selectorString);
               activation.setTopicTemporaryQueue(tempQueueName);
            }
            else
            {
               tempQueueName = activation.getTopicTemporaryQueue();
               QueueQuery queueQuery = session.queueQuery(tempQueueName);
               if (!queueQuery.isExists())
               {
                  // this is because we could be using remote servers (in cluster maybe)
                  // and the queue wasn't created on that node yet.
                  session.createTemporaryQueue(activation.getAddress(), tempQueueName, selectorString);
               }
            }
         }
         else
         {
            tempQueueName = activation.getAddress();
         }
         consumer = (ClientConsumerInternal) session.createConsumer(tempQueueName, selectorString);
      }

      // Create the endpoint, if we are transacted pass the session so it is enlisted, unless using Local TX
      MessageEndpointFactory endpointFactory = activation.getMessageEndpointFactory();
      useLocalTx = !activation.isDeliveryTransacted() && activation.getActivationSpec().isUseLocalTx();
      transacted = activation.isDeliveryTransacted();
      if (activation.isDeliveryTransacted() && !activation.getActivationSpec().isUseLocalTx())
      {
         Map<String, Object> xaResourceProperties = new HashMap<String, Object>();
         xaResourceProperties.put(ActiveMQXAResourceWrapper.ACTIVEMQ_JNDI_NAME, ((ActiveMQResourceAdapter) spec.getResourceAdapter()).getJndiName());
         xaResourceProperties.put(ActiveMQXAResourceWrapper.ACTIVEMQ_NODE_ID, ((ClientSessionFactoryInternal) cf).getLiveNodeId());
         xaResourceProperties.put(ActiveMQXAResourceWrapper.ACTIVEMQ_PRODUCT_NAME, ActiveMQResourceAdapter.PRODUCT_NAME);
         xaResourceProperties.put(ActiveMQXAResourceWrapper.ACTIVEMQ_PRODUCT_VERSION, VersionLoader.getVersion().getFullVersion());
         XAResource xaResource = ServiceUtils.wrapXAResource(session, xaResourceProperties);

         endpoint = endpointFactory.createEndpoint(xaResource);
         useXA = true;
      }
      else
      {
         endpoint = endpointFactory.createEndpoint(null);
         useXA = false;
      }
      consumer.setMessageHandler(this);
   }

   XAResource getXAResource()
   {
      return useXA ? session : null;
   }

   public Thread interruptConsumer(FutureLatch future)
   {
      try
      {
         if (consumer != null)
         {
            return consumer.prepareForClose(future);
         }
      }
      catch (Throwable e)
      {
         ActiveMQRALogger.LOGGER.warn("Error interrupting handler on endpoint " + endpoint + " handler=" + consumer);
      }
      return null;
   }

   /**
    * Stop the handler
    */
   public void teardown()
   {
      if (ActiveMQMessageHandler.trace)
      {
         ActiveMQRALogger.LOGGER.trace("teardown()");
      }

      try
      {
         if (endpoint != null)
         {
            endpoint.release();
            endpoint = null;
         }
      }
      catch (Throwable t)
      {
         ActiveMQRALogger.LOGGER.debug("Error releasing endpoint " + endpoint, t);
      }

      try
      {
         consumer.close();
         if (activation.getTopicTemporaryQueue() != null)
         {
            // We need to delete temporary topics when the activation is stopped or messages will build up on the server
            SimpleString tmpQueue = activation.getTopicTemporaryQueue();
            QueueQuery subResponse = session.queueQuery(tmpQueue);
            if (subResponse.getConsumerCount() == 0)
            {
               // This is optional really, since we now use temporaryQueues, we could simply ignore this
               // and the server temporary queue would remove this as soon as the queue was removed
               session.deleteQueue(tmpQueue);
            }
         }
      }
      catch (Throwable t)
      {
         ActiveMQRALogger.LOGGER.debug("Error closing core-queue consumer", t);
      }

      try
      {
         if (session != null)
         {
            session.close();
         }
      }
      catch (Throwable t)
      {
         ActiveMQRALogger.LOGGER.debug("Error releasing session " + session, t);
      }

      try
      {
         if (cf != null)
         {
            cf.close();
         }
      }
      catch (Throwable t)
      {
         ActiveMQRALogger.LOGGER.debug("Error releasing session factory " + session, t);
      }
   }

   public void onMessage(final ClientMessage message)
   {
      if (ActiveMQMessageHandler.trace)
      {
         ActiveMQRALogger.LOGGER.trace("onMessage(" + message + ")");
      }

      ActiveMQMessage msg = ActiveMQMessage.createMessage(message, session);
      boolean beforeDelivery = false;

      try
      {
         if (activation.getActivationSpec().getTransactionTimeout() > 0 && tm != null)
         {
            tm.setTransactionTimeout(activation.getActivationSpec().getTransactionTimeout());
         }
         endpoint.beforeDelivery(ActiveMQActivation.ONMESSAGE);
         beforeDelivery = true;
         msg.doBeforeReceive();

         //In the transacted case the message must be acked *before* onMessage is called

         if (transacted)
         {
            message.acknowledge();
         }

         ((MessageListener) endpoint).onMessage(msg);

         if (!transacted)
         {
            message.acknowledge();
         }

         try
         {
            endpoint.afterDelivery();
         }
         catch (ResourceException e)
         {
            ActiveMQRALogger.LOGGER.unableToCallAfterDelivery(e);
            return;
         }
         if (useLocalTx)
         {
            session.commit();
         }

         if (trace)
         {
            ActiveMQRALogger.LOGGER.trace("finished onMessage on " + message);
         }
      }
      catch (Throwable e)
      {
         ActiveMQRALogger.LOGGER.errorDeliveringMessage(e);
         // we need to call before/afterDelivery as a pair
         if (beforeDelivery)
         {
            if (useXA && tm != null)
            {
               // This is the job for the container,
               // however if the container throws an exception because of some other errors,
               // there are situations where the container is not setting the rollback only
               // this is to avoid a scenario where afterDelivery would kick in
               try
               {
                  Transaction tx = tm.getTransaction();
                  if (tx != null)
                  {
                     tx.setRollbackOnly();
                  }
               }
               catch (Exception e1)
               {
                  ActiveMQRALogger.LOGGER.warn("unnable to clear the transaction", e1);
                  try
                  {
                     session.rollback();
                  }
                  catch (ActiveMQException e2)
                  {
                     ActiveMQRALogger.LOGGER.warn("Unable to rollback", e2);
                     return;
                  }
               }
            }

            MessageEndpoint endToUse = endpoint;
            try
            {
               // to avoid a NPE that would happen while the RA is in tearDown
               if (endToUse != null)
               {
                  endToUse.afterDelivery();
               }
            }
            catch (ResourceException e1)
            {
               ActiveMQRALogger.LOGGER.unableToCallAfterDelivery(e1);
            }
         }
         if (useLocalTx || !activation.isDeliveryTransacted())
         {
            try
            {
               session.rollback(true);
            }
            catch (ActiveMQException e1)
            {
               ActiveMQRALogger.LOGGER.unableToRollbackTX();
            }
         }
      }
      finally
      {
         try
         {
            session.resetIfNeeded();
         }
         catch (ActiveMQException e)
         {
            ActiveMQRALogger.LOGGER.unableToResetSession();
         }
      }

   }

   public void start() throws ActiveMQException
   {
      session.start();
   }
}
