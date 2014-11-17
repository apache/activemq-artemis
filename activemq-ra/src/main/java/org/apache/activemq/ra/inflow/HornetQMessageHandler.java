/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.activemq.ra.inflow;

import javax.jms.MessageListener;
import javax.resource.ResourceException;
import javax.resource.spi.endpoint.MessageEndpoint;
import javax.resource.spi.endpoint.MessageEndpointFactory;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;
import java.util.UUID;

import org.apache.activemq.api.core.HornetQException;
import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.api.core.client.ClientMessage;
import org.apache.activemq.api.core.client.ClientSession.QueueQuery;
import org.apache.activemq.api.core.client.ClientSessionFactory;
import org.apache.activemq.api.core.client.MessageHandler;
import org.apache.activemq.core.client.impl.ClientConsumerInternal;
import org.apache.activemq.core.client.impl.ClientSessionFactoryInternal;
import org.apache.activemq.core.client.impl.ClientSessionInternal;
import org.apache.activemq.jms.client.HornetQDestination;
import org.apache.activemq.jms.client.HornetQMessage;
import org.apache.activemq.ra.HornetQRALogger;
import org.apache.activemq.ra.HornetQResourceAdapter;
import org.apache.activemq.ra.HornetQXAResourceWrapper;
import org.apache.activemq.utils.FutureLatch;

/**
 * The message handler
 *
 * @author <a href="adrian@jboss.com">Adrian Brock</a>
 * @author <a href="mailto:jesper.pedersen@jboss.org">Jesper Pedersen</a>
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 * @author <a href="mailto:mtaylor@redhat.com">Martyn Taylor</a>
 */
public class HornetQMessageHandler implements MessageHandler
{
   /**
    * Trace enabled
    */
   private static boolean trace = HornetQRALogger.LOGGER.isTraceEnabled();
   /**
    * The session
    */
   private final ClientSessionInternal session;

   private ClientConsumerInternal consumer;

   /**
    * The endpoint
    */
   private MessageEndpoint endpoint;

   private final HornetQActivation activation;

   private boolean useLocalTx;

   private boolean transacted;

   private boolean useXA = false;

   private final int sessionNr;

   private final TransactionManager tm;

   private ClientSessionFactory cf;

   public HornetQMessageHandler(final HornetQActivation activation,
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
      if (HornetQMessageHandler.trace)
      {
         HornetQRALogger.LOGGER.trace("setup()");
      }

      HornetQActivationSpec spec = activation.getActivationSpec();
      String selector = spec.getMessageSelector();

      // Create the message consumer
      SimpleString selectorString = selector == null || selector.trim().equals("") ? null : new SimpleString(selector);
      if (activation.isTopic() && spec.isSubscriptionDurable())
      {
         SimpleString queueName = new SimpleString(HornetQDestination.createQueueNameForDurableSubscription(true,
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
               else if (HornetQRALogger.LOGGER.isDebugEnabled())
               {
                  HornetQRALogger.LOGGER.debug("the mdb on destination " + queueName + " already had " +
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
         XAResource xaResource = new HornetQXAResourceWrapper(session,
                                                     ((HornetQResourceAdapter) spec.getResourceAdapter()).getJndiName(),
                                                     ((ClientSessionFactoryInternal) cf).getLiveNodeId());
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
         HornetQRALogger.LOGGER.warn("Error interrupting handler on endpoint " + endpoint + " handler=" + consumer);
      }
      return null;
   }

   /**
    * Stop the handler
    */
   public void teardown()
   {
      if (HornetQMessageHandler.trace)
      {
         HornetQRALogger.LOGGER.trace("teardown()");
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
         HornetQRALogger.LOGGER.debug("Error releasing endpoint " + endpoint, t);
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
         HornetQRALogger.LOGGER.debug("Error closing core-queue consumer", t);
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
         HornetQRALogger.LOGGER.debug("Error releasing session " + session, t);
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
         HornetQRALogger.LOGGER.debug("Error releasing session factory " + session, t);
      }
   }

   public void onMessage(final ClientMessage message)
   {
      if (HornetQMessageHandler.trace)
      {
         HornetQRALogger.LOGGER.trace("onMessage(" + message + ")");
      }

      HornetQMessage msg = HornetQMessage.createMessage(message, session);
      boolean beforeDelivery = false;

      try
      {
         if (activation.getActivationSpec().getTransactionTimeout() > 0 && tm != null)
         {
            tm.setTransactionTimeout(activation.getActivationSpec().getTransactionTimeout());
         }
         endpoint.beforeDelivery(HornetQActivation.ONMESSAGE);
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
            HornetQRALogger.LOGGER.unableToCallAfterDelivery(e);
            return;
         }
         if (useLocalTx)
         {
            session.commit();
         }

         if (trace)
         {
            HornetQRALogger.LOGGER.trace("finished onMessage on " + message);
         }
      }
      catch (Throwable e)
      {
         HornetQRALogger.LOGGER.errorDeliveringMessage(e);
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
                  HornetQRALogger.LOGGER.warn("unnable to clear the transaction", e1);
                  try
                  {
                     session.rollback();
                  }
                  catch (HornetQException e2)
                  {
                     HornetQRALogger.LOGGER.warn("Unable to rollback", e2);
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
               HornetQRALogger.LOGGER.unableToCallAfterDelivery(e1);
            }
         }
         if (useLocalTx || !activation.isDeliveryTransacted())
         {
            try
            {
               session.rollback(true);
            }
            catch (HornetQException e1)
            {
               HornetQRALogger.LOGGER.unableToRollbackTX();
            }
         }
      }
      finally
      {
         try
         {
            session.resetIfNeeded();
         }
         catch (HornetQException e)
         {
            HornetQRALogger.LOGGER.unableToResetSession();
         }
      }

   }

   public void start() throws HornetQException
   {
      session.start();
   }
}
