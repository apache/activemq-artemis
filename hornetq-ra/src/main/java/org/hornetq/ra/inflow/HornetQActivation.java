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
package org.hornetq.ra.inflow;

import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.Topic;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.resource.ResourceException;
import javax.resource.spi.endpoint.MessageEndpointFactory;
import javax.resource.spi.work.Work;
import javax.resource.spi.work.WorkManager;
import javax.transaction.xa.XAResource;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.HornetQExceptionType;
import org.hornetq.api.core.HornetQNonExistentQueueException;
import org.hornetq.api.core.HornetQNotConnectedException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.client.HornetQDestination;
import org.hornetq.jms.server.recovery.XARecoveryConfig;
import org.hornetq.ra.HornetQRABundle;
import org.hornetq.ra.HornetQRAConnectionFactory;
import org.hornetq.ra.HornetQRALogger;
import org.hornetq.ra.HornetQRaUtils;
import org.hornetq.ra.HornetQResourceAdapter;
import org.hornetq.utils.FutureLatch;
import org.hornetq.utils.SensitiveDataCodec;

/**
 * The activation.
 *
 * @author <a href="adrian@jboss.com">Adrian Brock</a>
 * @author <a href="jesper.pedersen@jboss.org">Jesper Pedersen</a>
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class HornetQActivation
{
   /**
    * Trace enabled
    */
   private static boolean trace = HornetQRALogger.LOGGER.isTraceEnabled();

   /**
    * The onMessage method
    */
   public static final Method ONMESSAGE;

   /**
    * The resource adapter
    */
   private final HornetQResourceAdapter ra;

   /**
    * The activation spec
    */
   private final HornetQActivationSpec spec;

   /**
    * The message endpoint factory
    */
   private final MessageEndpointFactory endpointFactory;

   /**
    * Whether delivery is active
    */
   private final AtomicBoolean deliveryActive = new AtomicBoolean(false);

   /**
    * The destination type
    */
   private boolean isTopic = false;

   /**
    * Is the delivery transacted
    */
   private boolean isDeliveryTransacted;

   private HornetQDestination destination;

   /**
    * The name of the temporary subscription name that all the sessions will share
    */
   private SimpleString topicTemporaryQueue;

   private final List<HornetQMessageHandler> handlers = new ArrayList<HornetQMessageHandler>();

   private HornetQConnectionFactory factory;

   // Whether we are in the failure recovery loop
   private final AtomicBoolean inFailure = new AtomicBoolean(false);
   private XARecoveryConfig resourceRecovery;

   static
   {
      try
      {
         ONMESSAGE = MessageListener.class.getMethod("onMessage", new Class[]{Message.class});
      }
      catch (Exception e)
      {
         throw new RuntimeException(e);
      }
   }

   /**
    * Constructor
    *
    * @param ra              The resource adapter
    * @param endpointFactory The endpoint factory
    * @param spec            The activation spec
    * @throws ResourceException Thrown if an error occurs
    */
   public HornetQActivation(final HornetQResourceAdapter ra,
                            final MessageEndpointFactory endpointFactory,
                            final HornetQActivationSpec spec) throws ResourceException
   {
      spec.validate();

      if (HornetQActivation.trace)
      {
         HornetQRALogger.LOGGER.trace("constructor(" + ra + ", " + endpointFactory + ", " + spec + ")");
      }

      if (ra.isUseMaskedPassword())
      {
         String pass = spec.getOwnPassword();
         if (pass != null)
         {
            SensitiveDataCodec<String> codec = ra.getCodecInstance();

            try
            {
               spec.setPassword(codec.decode(pass));
            }
            catch (Exception e)
            {
               throw new ResourceException(e);
            }
         }
      }

      this.ra = ra;
      this.endpointFactory = endpointFactory;
      this.spec = spec;
      try
      {
         isDeliveryTransacted = endpointFactory.isDeliveryTransacted(HornetQActivation.ONMESSAGE);
      }
      catch (Exception e)
      {
         throw new ResourceException(e);
      }
   }

   /**
    * Get the activation spec
    *
    * @return The value
    */
   public HornetQActivationSpec getActivationSpec()
   {
      if (HornetQActivation.trace)
      {
         HornetQRALogger.LOGGER.trace("getActivationSpec()");
      }

      return spec;
   }

   /**
    * Get the message endpoint factory
    *
    * @return The value
    */
   public MessageEndpointFactory getMessageEndpointFactory()
   {
      if (HornetQActivation.trace)
      {
         HornetQRALogger.LOGGER.trace("getMessageEndpointFactory()");
      }

      return endpointFactory;
   }

   /**
    * Get whether delivery is transacted
    *
    * @return The value
    */
   public boolean isDeliveryTransacted()
   {
      if (HornetQActivation.trace)
      {
         HornetQRALogger.LOGGER.trace("isDeliveryTransacted()");
      }

      return isDeliveryTransacted;
   }

   /**
    * Get the work manager
    *
    * @return The value
    */
   public WorkManager getWorkManager()
   {
      if (HornetQActivation.trace)
      {
         HornetQRALogger.LOGGER.trace("getWorkManager()");
      }

      return ra.getWorkManager();
   }

   /**
    * Is the destination a topic
    *
    * @return The value
    */
   public boolean isTopic()
   {
      if (HornetQActivation.trace)
      {
         HornetQRALogger.LOGGER.trace("isTopic()");
      }

      return isTopic;
   }

   /**
    * Start the activation
    *
    * @throws ResourceException Thrown if an error occurs
    */
   public void start() throws ResourceException
   {
      if (HornetQActivation.trace)
      {
         HornetQRALogger.LOGGER.trace("start()");
      }
      deliveryActive.set(true);
      ra.getWorkManager().scheduleWork(new SetupActivation());
   }

   /**
    * @return the topicTemporaryQueue
    */
   public SimpleString getTopicTemporaryQueue()
   {
      return topicTemporaryQueue;
   }

   /**
    * @param topicTemporaryQueue the topicTemporaryQueue to set
    */
   public void setTopicTemporaryQueue(SimpleString topicTemporaryQueue)
   {
      this.topicTemporaryQueue = topicTemporaryQueue;
   }

   /**
    * @return the list of XAResources for this activation endpoint
    */
   public List<XAResource> getXAResources()
   {
      List<XAResource> xaresources = new ArrayList<XAResource>();
      for (HornetQMessageHandler handler : handlers)
      {
         XAResource xares = handler.getXAResource();
         if (xares != null)
         {
            xaresources.add(xares);
         }
      }
      return xaresources;
   }

   /**
    * Stop the activation
    */
   public void stop()
   {
      if (HornetQActivation.trace)
      {
         HornetQRALogger.LOGGER.trace("stop()");
      }

      deliveryActive.set(false);
      teardown();
   }

   /**
    * Setup the activation
    *
    * @throws Exception Thrown if an error occurs
    */
   protected synchronized void setup() throws Exception
   {
      HornetQRALogger.LOGGER.debug("Setting up " + spec);

      setupCF();

      setupDestination();

      Exception firstException = null;

      for (int i = 0; i < spec.getMaxSession(); i++)
      {
         ClientSessionFactory cf = null;
         ClientSession session = null;

         try
         {
            cf = factory.getServerLocator().createSessionFactory();
            session = setupSession(cf);
            HornetQMessageHandler handler = new HornetQMessageHandler(this, ra.getTM(), (ClientSessionInternal) session, cf, i);
            handler.setup();
            handlers.add(handler);
         }
         catch (Exception e)
         {
            if (cf != null)
            {
               cf.close();
            }
            if (session != null)
            {
               session.close();
            }
            if (firstException == null)
            {
               firstException = e;
            }
         }
      }
      //if we have any exceptions close all the handlers and throw the first exception.
      //we don't want partially configured activations, i.e. only 8 out of 15 sessions started so best to stop and log the error.
      if (firstException != null)
      {
         for (HornetQMessageHandler handler : handlers)
         {
            handler.teardown();
         }
         throw firstException;
      }

      //now start them all together.
      for (HornetQMessageHandler handler : handlers)
      {
         handler.start();
      }

      resourceRecovery = ra.getRecoveryManager().register(factory, spec.getUser(), spec.getPassword());

      HornetQRALogger.LOGGER.debug("Setup complete " + this);
   }

   /**
    * Teardown the activation
    */
   protected synchronized void teardown()
   {
      HornetQRALogger.LOGGER.debug("Tearing down " + spec);

      if (resourceRecovery != null)
      {
         ra.getRecoveryManager().unRegister(resourceRecovery);
      }

      final HornetQMessageHandler[] handlersCopy = new HornetQMessageHandler[handlers.size()];

      // We need to do from last to first as any temporary queue will have been created on the first handler
      // So we invert the handlers here
      for (int i = 0; i < handlers.size(); i++)
      {
         // The index here is the complimentary so it's inverting the array
         handlersCopy[i] = handlers.get(handlers.size() - i - 1);
      }

      handlers.clear();

      FutureLatch future = new FutureLatch(handlersCopy.length);
      List<Thread> interruptThreads = new ArrayList<>();
      for (HornetQMessageHandler handler : handlersCopy)
      {
         Thread thread = handler.interruptConsumer(future);
         if (thread != null)
         {
            interruptThreads.add(thread);
         }
      }

      //wait for all the consumers to complete any onmessage calls
      boolean stuckThreads = !future.await(factory.getCallTimeout());
      //if any are stuck then we need to interrupt them
      if (stuckThreads)
      {
         for (Thread interruptThread : interruptThreads)
         {
            try
            {
               interruptThread.interrupt();
            }
            catch (Exception e)
            {
               //ok
            }
         }
      }

      Thread threadTearDown = new Thread("TearDown/HornetQActivation")
      {
         public void run()
         {
            for (HornetQMessageHandler handler : handlersCopy)
            {
               handler.teardown();
            }
         }
      };

      // We will first start a new thread that will call tearDown on all the instances, trying to graciously shutdown everything.
      // We will then use the call-timeout to determine a timeout.
      // if that failed we will then close the connection factory, and interrupt the thread
      threadTearDown.start();

      try
      {
         threadTearDown.join(factory.getCallTimeout());
      }
      catch (InterruptedException e)
      {
         // nothing to be done on this context.. we will just keep going as we need to send an interrupt to threadTearDown and give up
      }

      if (threadTearDown.isAlive())
      {
         if (factory != null)
         {
            // This will interrupt any threads waiting on reconnect
            factory.close();
            factory = null;
         }
         threadTearDown.interrupt();

         try
         {
            threadTearDown.join(5000);
         }
         catch (InterruptedException e)
         {
            // nothing to be done here.. we are going down anyways
         }

         if (threadTearDown.isAlive())
         {
            HornetQRALogger.LOGGER.warn("Thread " + threadTearDown + " couldn't be finished");
         }
      }

      if (spec.isHasBeenUpdated() && factory != null)
      {
         factory.close();
         factory = null;
      }


      HornetQRALogger.LOGGER.debug("Tearing down complete " + this);
   }

   protected void setupCF() throws Exception
   {
      if (spec.getConnectionFactoryLookup() != null)
      {
         Context ctx;
         if (spec.getParsedJndiParams() == null)
         {
            ctx = new InitialContext();
         }
         else
         {
            ctx = new InitialContext(spec.getParsedJndiParams());
         }
         Object fac = ctx.lookup(spec.getConnectionFactoryLookup());
         if (fac instanceof HornetQConnectionFactory)
         {
            factory = (HornetQConnectionFactory) fac;
         }
         else
         {
            HornetQRAConnectionFactory raFact = (HornetQRAConnectionFactory) fac;
            if (spec.isHasBeenUpdated())
            {
               factory = raFact.getResourceAdapter().createHornetQConnectionFactory(spec);
            }
            else
            {
               factory = raFact.getDefaultFactory();
               if (factory != ra.getDefaultHornetQConnectionFactory())
               {
                  HornetQRALogger.LOGGER.warnDifferentConnectionfactory();
               }
            }
         }
      }
      else if (spec.isHasBeenUpdated())
      {
         factory = ra.createHornetQConnectionFactory(spec);
      }
      else
      {
         factory = ra.getDefaultHornetQConnectionFactory();
      }
   }

   /**
    * Setup a session
    *
    * @param cf
    * @return The connection
    * @throws Exception Thrown if an error occurs
    */
   protected ClientSession setupSession(ClientSessionFactory cf) throws Exception
   {
      ClientSession result = null;

      try
      {
         result = ra.createSession(cf,
                                   spec.getAcknowledgeModeInt(),
                                   spec.getUser(),
                                   spec.getPassword(),
                                   ra.getPreAcknowledge(),
                                   ra.getDupsOKBatchSize(),
                                   ra.getTransactionBatchSize(),
                                   isDeliveryTransacted,
                                   spec.isUseLocalTx(),
                                   spec.getTransactionTimeout());

         result.addMetaData("resource-adapter", "inbound");
         result.addMetaData("jms-session", "");
         String clientID = ra.getClientID() == null ? spec.getClientID() : ra.getClientID();
         if (clientID != null)
         {
            result.addMetaData("jms-client-id", clientID);
         }

         HornetQRALogger.LOGGER.debug("Using queue connection " + result);

         return result;
      }
      catch (Throwable t)
      {
         try
         {
            if (result != null)
            {
               result.close();
            }
         }
         catch (Exception e)
         {
            HornetQRALogger.LOGGER.trace("Ignored error closing connection", e);
         }
         if (t instanceof Exception)
         {
            throw (Exception) t;
         }
         throw new RuntimeException("Error configuring connection", t);
      }
   }

   public SimpleString getAddress()
   {
      return destination.getSimpleAddress();
   }

   protected void setupDestination() throws Exception
   {

      String destinationName = spec.getDestination();

      if (spec.isUseJNDI())
      {
         Context ctx;
         if (spec.getParsedJndiParams() == null)
         {
            ctx = new InitialContext();
         }
         else
         {
            ctx = new InitialContext(spec.getParsedJndiParams());
         }
         HornetQRALogger.LOGGER.debug("Using context " + ctx.getEnvironment() + " for " + spec);
         if (HornetQActivation.trace)
         {
            HornetQRALogger.LOGGER.trace("setupDestination(" + ctx + ")");
         }

         String destinationTypeString = spec.getDestinationType();
         if (destinationTypeString != null && !destinationTypeString.trim().equals(""))
         {
            HornetQRALogger.LOGGER.debug("Destination type defined as " + destinationTypeString);

            Class<?> destinationType;
            if (Topic.class.getName().equals(destinationTypeString))
            {
               destinationType = Topic.class;
               isTopic = true;
            }
            else
            {
               destinationType = Queue.class;
            }

            HornetQRALogger.LOGGER.debug("Retrieving " + destinationType.getName() + " \"" + destinationName + "\" from JNDI");

            try
            {
               destination = (HornetQDestination) HornetQRaUtils.lookup(ctx, destinationName, destinationType);
            }
            catch (Exception e)
            {
               if (destinationName == null)
               {
                  throw HornetQRABundle.BUNDLE.noDestinationName();
               }

               String calculatedDestinationName = destinationName.substring(destinationName.lastIndexOf('/') + 1);

               HornetQRALogger.LOGGER.debug("Unable to retrieve " + destinationName +
                                               " from JNDI. Creating a new " + destinationType.getName() +
                                               " named " + calculatedDestinationName + " to be used by the MDB.");

               // If there is no binding on naming, we will just create a new instance
               if (isTopic)
               {
                  destination = (HornetQDestination) HornetQJMSClient.createTopic(calculatedDestinationName);
               }
               else
               {
                  destination = (HornetQDestination) HornetQJMSClient.createQueue(calculatedDestinationName);
               }
            }
         }
         else
         {
            HornetQRALogger.LOGGER.debug("Destination type not defined in MDB activation configuration.");
            HornetQRALogger.LOGGER.debug("Retrieving " + Destination.class.getName() + " \"" + destinationName + "\" from JNDI");

            destination = (HornetQDestination) HornetQRaUtils.lookup(ctx, destinationName, Destination.class);
            if (destination instanceof Topic)
            {
               isTopic = true;
            }
         }
      }
      else
      {
         HornetQRALogger.LOGGER.instantiatingDestination(spec.getDestinationType(), spec.getDestination());

         if (Topic.class.getName().equals(spec.getDestinationType()))
         {
            destination = (HornetQDestination) HornetQJMSClient.createTopic(spec.getDestination());
            isTopic = true;
         }
         else
         {
            destination = (HornetQDestination) HornetQJMSClient.createQueue(spec.getDestination());
         }
      }
   }

   /**
    * Get a string representation
    *
    * @return The value
    */
   @Override
   public String toString()
   {
      StringBuffer buffer = new StringBuffer();
      buffer.append(HornetQActivation.class.getName()).append('(');
      buffer.append("spec=").append(spec.getClass().getName());
      buffer.append(" mepf=").append(endpointFactory.getClass().getName());
      buffer.append(" active=").append(deliveryActive.get());
      if (spec.getDestination() != null)
      {
         buffer.append(" destination=").append(spec.getDestination());
      }
      buffer.append(" transacted=").append(isDeliveryTransacted);
      buffer.append(')');
      return buffer.toString();
   }

   /**
    * Handles any failure by trying to reconnect
    *
    * @param failure the reason for the failure
    */
   public void handleFailure(Throwable failure)
   {
      if (failure instanceof HornetQException && ((HornetQException) failure).getType() == HornetQExceptionType.QUEUE_DOES_NOT_EXIST)
      {
         HornetQRALogger.LOGGER.awaitingTopicQueueCreation(getActivationSpec().getDestination());
      }
      else if (failure instanceof HornetQException && ((HornetQException) failure).getType() == HornetQExceptionType.NOT_CONNECTED)
      {
         HornetQRALogger.LOGGER.awaitingJMSServerCreation();
      }
      else
      {
         HornetQRALogger.LOGGER.failureInActivation(failure, spec);
      }
      int reconnectCount = 0;
      int setupAttempts = spec.getSetupAttempts();
      long setupInterval = spec.getSetupInterval();

      // Only enter the failure loop once
      if (inFailure.getAndSet(true))
         return;
      try
      {
         Throwable lastException = failure;
         while (deliveryActive.get() && (setupAttempts == -1 || reconnectCount < setupAttempts))
         {
            teardown();

            try
            {
               Thread.sleep(setupInterval);
            }
            catch (InterruptedException e)
            {
               HornetQRALogger.LOGGER.debug("Interrupted trying to reconnect " + spec, e);
               break;
            }

            if (reconnectCount < 1)
            {
               HornetQRALogger.LOGGER.attemptingReconnect(spec);
            }
            try
            {
               setup();
               HornetQRALogger.LOGGER.reconnected();
               break;
            }
            catch (Throwable t)
            {
               if (failure instanceof HornetQException && ((HornetQException) failure).getType() == HornetQExceptionType.QUEUE_DOES_NOT_EXIST)
               {
                  if (lastException == null || !(t instanceof HornetQNonExistentQueueException))
                  {
                     lastException = t;
                     HornetQRALogger.LOGGER.awaitingTopicQueueCreation(getActivationSpec().getDestination());
                  }
               }
               else if (failure instanceof HornetQException && ((HornetQException) failure).getType() == HornetQExceptionType.NOT_CONNECTED)
               {
                  if (lastException == null || !(t instanceof HornetQNotConnectedException))
                  {
                     lastException = t;
                     HornetQRALogger.LOGGER.awaitingJMSServerCreation();
                  }
               }
               else
               {
                  HornetQRALogger.LOGGER.errorReconnecting(t, spec);
               }
            }
            ++reconnectCount;
         }
      }
      finally
      {
         // Leaving failure recovery loop
         inFailure.set(false);
      }
   }

   public HornetQConnectionFactory getConnectionFactory()
   {
      return this.factory;
   }

   /**
    * Handles the setup
    */
   private class SetupActivation implements Work
   {
      public void run()
      {
         try
         {
            setup();
         }
         catch (Throwable t)
         {
            handleFailure(t);
         }
      }

      public void release()
      {
      }
   }
}
