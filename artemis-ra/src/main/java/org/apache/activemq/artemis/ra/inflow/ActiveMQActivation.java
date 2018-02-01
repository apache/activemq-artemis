/*
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
package org.apache.activemq.artemis.ra.inflow;

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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.api.core.ActiveMQNonExistentQueueException;
import org.apache.activemq.artemis.api.core.ActiveMQNotConnectedException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ClusterTopologyListener;
import org.apache.activemq.artemis.api.core.client.TopologyMember;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.core.client.impl.ClientSessionInternal;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQDestination;
import org.apache.activemq.artemis.ra.ActiveMQRABundle;
import org.apache.activemq.artemis.ra.ActiveMQRALogger;
import org.apache.activemq.artemis.ra.ActiveMQRaUtils;
import org.apache.activemq.artemis.ra.ActiveMQResourceAdapter;
import org.apache.activemq.artemis.service.extensions.xa.recovery.XARecoveryConfig;
import org.apache.activemq.artemis.utils.FutureLatch;
import org.apache.activemq.artemis.utils.PasswordMaskingUtil;
import org.jboss.logging.Logger;

/**
 * The activation.
 */
public class ActiveMQActivation {

   private static final Logger logger = Logger.getLogger(ActiveMQActivation.class);

   /**
    * The onMessage method
    */
   public static final Method ONMESSAGE;

   /**
    * The resource adapter
    */
   private final ActiveMQResourceAdapter ra;

   /**
    * The activation spec
    */
   private final ActiveMQActivationSpec spec;

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

   private ActiveMQDestination destination;

   /**
    * The name of the temporary subscription name that all the sessions will share
    */
   private SimpleString topicTemporaryQueue;

   private final List<ActiveMQMessageHandler> handlers = new ArrayList<>();

   private ActiveMQConnectionFactory factory;

   private final List<String> nodes = Collections.synchronizedList(new ArrayList<String>());

   private final Map<String, Long> removedNodes = new ConcurrentHashMap<>();

   private boolean lastReceived = false;

   // Whether we are in the failure recovery loop
   private final AtomicBoolean inReconnect = new AtomicBoolean(false);
   private XARecoveryConfig resourceRecovery;

   static {
      try {
         ONMESSAGE = MessageListener.class.getMethod("onMessage", new Class[]{Message.class});
      } catch (Exception e) {
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
   public ActiveMQActivation(final ActiveMQResourceAdapter ra,
                             final MessageEndpointFactory endpointFactory,
                             final ActiveMQActivationSpec spec) throws ResourceException {
      spec.validate();

      if (logger.isTraceEnabled()) {
         logger.trace("constructor(" + ra + ", " + endpointFactory + ", " + spec + ")");
      }

      String pass = spec.getOwnPassword();
      if (pass != null) {
         try {
            spec.setPassword(PasswordMaskingUtil.resolveMask(ra.isUseMaskedPassword(), pass, ra.getCodec()));
         } catch (Exception e) {
            throw new ResourceException(e);
         }
      }

      this.ra = ra;
      this.endpointFactory = endpointFactory;
      this.spec = spec;
      try {
         isDeliveryTransacted = endpointFactory.isDeliveryTransacted(ActiveMQActivation.ONMESSAGE);
      } catch (Exception e) {
         throw new ResourceException(e);
      }
   }

   /**
    * Get the activation spec
    *
    * @return The value
    */
   public ActiveMQActivationSpec getActivationSpec() {
      if (logger.isTraceEnabled()) {
         logger.trace("getActivationSpec()");
      }

      return spec;
   }

   /**
    * Get the message endpoint factory
    *
    * @return The value
    */
   public MessageEndpointFactory getMessageEndpointFactory() {
      if (logger.isTraceEnabled()) {
         logger.trace("getMessageEndpointFactory()");
      }

      return endpointFactory;
   }

   /**
    * Get whether delivery is transacted
    *
    * @return The value
    */
   public boolean isDeliveryTransacted() {
      if (logger.isTraceEnabled()) {
         logger.trace("isDeliveryTransacted()");
      }

      return isDeliveryTransacted;
   }

   /**
    * Get the work manager
    *
    * @return The value
    */
   public WorkManager getWorkManager() {
      if (logger.isTraceEnabled()) {
         logger.trace("getWorkManager()");
      }

      return ra.getWorkManager();
   }

   /**
    * Is the destination a topic
    *
    * @return The value
    */
   public boolean isTopic() {
      if (logger.isTraceEnabled()) {
         logger.trace("isTopic()");
      }

      return isTopic;
   }

   /**
    * Start the activation
    *
    * @throws ResourceException Thrown if an error occurs
    */
   public void start() throws ResourceException {
      if (logger.isTraceEnabled()) {
         logger.trace("start()");
      }
      deliveryActive.set(true);
      ra.getWorkManager().scheduleWork(new SetupActivation());
   }

   /**
    * @return the topicTemporaryQueue
    */
   public SimpleString getTopicTemporaryQueue() {
      return topicTemporaryQueue;
   }

   /**
    * @param topicTemporaryQueue the topicTemporaryQueue to set
    */
   public void setTopicTemporaryQueue(SimpleString topicTemporaryQueue) {
      this.topicTemporaryQueue = topicTemporaryQueue;
   }

   /**
    * @return the list of XAResources for this activation endpoint
    */
   public List<XAResource> getXAResources() {
      List<XAResource> xaresources = new ArrayList<>();
      for (ActiveMQMessageHandler handler : handlers) {
         XAResource xares = handler.getXAResource();
         if (xares != null) {
            xaresources.add(xares);
         }
      }
      return xaresources;
   }

   /**
    * Stop the activation
    */
   public void stop() {
      if (logger.isTraceEnabled()) {
         logger.trace("stop()");
      }

      deliveryActive.set(false);
      teardown();
   }

   /**
    * Setup the activation
    *
    * @throws Exception Thrown if an error occurs
    */
   protected synchronized void setup() throws Exception {
      logger.debug("Setting up " + spec);

      setupCF();

      setupDestination();

      Exception firstException = null;

      for (int i = 0; i < spec.getMaxSession(); i++) {
         ClientSessionFactory cf = null;
         ClientSession session = null;

         try {
            cf = factory.getServerLocator().createSessionFactory();
            session = setupSession(cf);
            ActiveMQMessageHandler handler = new ActiveMQMessageHandler(factory, this, ra.getTM(), (ClientSessionInternal) session, cf, i);
            handler.setup();
            handlers.add(handler);
         } catch (Exception e) {
            if (cf != null) {
               cf.close();
            }
            if (session != null) {
               session.close();
            }
            if (firstException == null) {
               firstException = e;
            }
         }
      }
      //if we have any exceptions close all the handlers and throw the first exception.
      //we don't want partially configured activations, i.e. only 8 out of 15 sessions started so best to stop and log the error.
      if (firstException != null) {
         for (ActiveMQMessageHandler handler : handlers) {
            handler.teardown();
         }
         throw firstException;
      }

      //now start them all together.
      for (ActiveMQMessageHandler handler : handlers) {
         handler.start();
      }

      Map<String, String> recoveryConfProps = new HashMap<>();
      recoveryConfProps.put(XARecoveryConfig.JNDI_NAME_PROPERTY_KEY, ra.getJndiName());
      resourceRecovery = ra.getRecoveryManager().register(factory, spec.getUser(), spec.getPassword(), recoveryConfProps);
      if (spec.isRebalanceConnections()) {
         factory.getServerLocator().addClusterTopologyListener(new RebalancingListener());
      }

      logger.debug("Setup complete " + this);
   }

   /**
    * Teardown the activation
    */
   protected synchronized void teardown() {
      logger.debug("Tearing down " + spec);

      long timeout = factory == null ? ActiveMQClient.DEFAULT_CALL_TIMEOUT : factory.getCallTimeout();

      if (resourceRecovery != null) {
         ra.getRecoveryManager().unRegister(resourceRecovery);
      }

      final ActiveMQMessageHandler[] handlersCopy = new ActiveMQMessageHandler[handlers.size()];

      // We need to do from last to first as any temporary queue will have been created on the first handler
      // So we invert the handlers here
      for (int i = 0; i < handlers.size(); i++) {
         // The index here is the complimentary so it's inverting the array
         handlersCopy[i] = handlers.get(handlers.size() - i - 1);
      }

      handlers.clear();

      FutureLatch future = new FutureLatch(handlersCopy.length);
      List<Thread> interruptThreads = new ArrayList<>();
      for (ActiveMQMessageHandler handler : handlersCopy) {
         Thread thread = handler.interruptConsumer(future);
         if (thread != null) {
            interruptThreads.add(thread);
         }
      }

      //wait for all the consumers to complete any onmessage calls
      boolean stuckThreads = !future.await(timeout);
      //if any are stuck then we need to interrupt them
      if (stuckThreads) {
         for (Thread interruptThread : interruptThreads) {
            try {
               interruptThread.interrupt();
            } catch (Exception e) {
               //ok
            }
         }
      }

      Thread threadTearDown = new Thread("TearDown/ActiveMQActivation") {
         @Override
         public void run() {
            for (ActiveMQMessageHandler handler : handlersCopy) {
               handler.teardown();
            }
         }
      };

      // We will first start a new thread that will call tearDown on all the instances, trying to graciously shutdown everything.
      // We will then use the call-timeout to determine a timeout.
      // if that failed we will then close the connection factory, and interrupt the thread
      threadTearDown.start();

      try {
         threadTearDown.join(timeout);
      } catch (InterruptedException e) {
         // nothing to be done on this context.. we will just keep going as we need to send an interrupt to threadTearDown and give up
      }

      if (factory != null) {
         try {
            // closing the factory will help making sure pending threads are closed
            factory.close();
         } catch (Throwable e) {
            ActiveMQRALogger.LOGGER.unableToCloseFactory(e);
         }

         factory = null;
      }

      if (threadTearDown.isAlive()) {
         threadTearDown.interrupt();

         try {
            threadTearDown.join(5000);
         } catch (InterruptedException e) {
            // nothing to be done here.. we are going down anyways
         }

         if (threadTearDown.isAlive()) {
            ActiveMQRALogger.LOGGER.threadCouldNotFinish(threadTearDown.toString());
         }
      }

      nodes.clear();
      lastReceived = false;

      logger.debug("Tearing down complete " + this);
   }

   protected void setupCF() throws Exception {
      if (spec.getConnectionFactoryLookup() != null) {
         Context ctx;
         if (spec.getParsedJndiParams() == null) {
            ctx = new InitialContext();
         } else {
            ctx = new InitialContext(spec.getParsedJndiParams());
         }
         Object fac = ctx.lookup(spec.getConnectionFactoryLookup());
         if (fac instanceof ActiveMQConnectionFactory) {
            // This will clone the connection factory
            // to make sure we won't close anyone's connection factory when we stop the MDB
            factory = ActiveMQJMSClient.createConnectionFactory(((ActiveMQConnectionFactory) fac).toURI().toString(), "internalConnection");
         } else {
            factory = ra.newConnectionFactory(spec);
         }
      } else {
         factory = ra.newConnectionFactory(spec);
      }
   }

   /**
    * Setup a session
    *
    * @param cf
    * @return The connection
    * @throws Exception Thrown if an error occurs
    */
   protected ClientSession setupSession(ClientSessionFactory cf) throws Exception {
      ClientSession result = null;

      try {
         result = ra.createSession(cf, spec.getAcknowledgeModeInt(), spec.getUser(), spec.getPassword(), ra.getPreAcknowledge(), ra.getDupsOKBatchSize(), ra.getTransactionBatchSize(), isDeliveryTransacted, spec.isUseLocalTx(), spec.getTransactionTimeout());

         result.addMetaData("resource-adapter", "inbound");
         result.addMetaData(ClientSession.JMS_SESSION_IDENTIFIER_PROPERTY, "");
         String clientID = ra.getClientID() == null ? spec.getClientID() : ra.getClientID();
         if (clientID != null) {
            result.addMetaData(ClientSession.JMS_SESSION_CLIENT_ID_PROPERTY, clientID);
         }

         logger.debug("Using queue connection " + result);

         return result;
      } catch (Throwable t) {
         try {
            if (result != null) {
               result.close();
            }
         } catch (Exception e) {
            logger.trace("Ignored error closing connection", e);
         }
         if (t instanceof Exception) {
            throw (Exception) t;
         }
         throw new RuntimeException("Error configuring connection", t);
      }
   }

   public SimpleString getAddress() {
      return destination.getSimpleAddress();
   }

   protected void setupDestination() throws Exception {

      String destinationName = spec.getDestination();

      if (spec.isUseJNDI()) {
         Context ctx;
         if (spec.getParsedJndiParams() == null) {
            ctx = new InitialContext();
         } else {
            ctx = new InitialContext(spec.getParsedJndiParams());
         }
         logger.debug("Using context " + ctx.getEnvironment() + " for " + spec);
         if (logger.isTraceEnabled()) {
            logger.trace("setupDestination(" + ctx + ")");
         }

         String destinationTypeString = spec.getDestinationType();
         if (destinationTypeString != null && !destinationTypeString.trim().equals("")) {
            logger.debug("Destination type defined as " + destinationTypeString);

            Class<?> destinationType;
            if (Topic.class.getName().equals(destinationTypeString)) {
               destinationType = Topic.class;
               isTopic = true;
            } else {
               destinationType = Queue.class;
            }

            logger.debug("Retrieving " + destinationType.getName() + " \"" + destinationName + "\" from JNDI");

            try {
               destination = (ActiveMQDestination) ActiveMQRaUtils.lookup(ctx, destinationName, destinationType);
            } catch (Exception e) {
               if (destinationName == null) {
                  throw ActiveMQRABundle.BUNDLE.noDestinationName();
               }

               String calculatedDestinationName = destinationName.substring(destinationName.lastIndexOf('/') + 1);
               if (isTopic && spec.getTopicPrefix() != null) {
                  calculatedDestinationName = spec.getTopicPrefix() + calculatedDestinationName;
               } else if (!isTopic && spec.getQueuePrefix() != null) {
                  calculatedDestinationName = spec.getQueuePrefix() + calculatedDestinationName;
               }

               logger.debug("Unable to retrieve " + destinationName +
                                                " from JNDI. Creating a new " + destinationType.getName() +
                                                " named " + calculatedDestinationName + " to be used by the MDB.");

               // If there is no binding on naming, we will just create a new instance
               if (isTopic) {
                  destination = (ActiveMQDestination) ActiveMQJMSClient.createTopic(calculatedDestinationName);
               } else {
                  destination = (ActiveMQDestination) ActiveMQJMSClient.createQueue(calculatedDestinationName);
               }
            }
         } else {
            logger.debug("Destination type not defined in MDB activation configuration.");
            logger.debug("Retrieving " + Destination.class.getName() + " \"" + destinationName + "\" from JNDI");

            destination = (ActiveMQDestination) ActiveMQRaUtils.lookup(ctx, destinationName, Destination.class);
            if (destination instanceof Topic) {
               isTopic = true;
            }
         }
      } else {
         ActiveMQRALogger.LOGGER.instantiatingDestination(spec.getDestinationType(), spec.getDestination());

         if (Topic.class.getName().equals(spec.getDestinationType())) {
            destination = (ActiveMQDestination) ActiveMQJMSClient.createTopic(spec.getDestination());
            isTopic = true;
         } else {
            destination = (ActiveMQDestination) ActiveMQJMSClient.createQueue(spec.getDestination());
         }
      }
   }

   /**
    * Get a string representation
    *
    * @return The value
    */
   @Override
   public String toString() {
      StringBuffer buffer = new StringBuffer();
      buffer.append(ActiveMQActivation.class.getName()).append('(');
      buffer.append("spec=").append(spec.getClass().getName());
      buffer.append(" mepf=").append(endpointFactory.getClass().getName());
      buffer.append(" active=").append(deliveryActive.get());
      if (spec.getDestination() != null) {
         buffer.append(" destination=").append(spec.getDestination());
      }
      buffer.append(" transacted=").append(isDeliveryTransacted);
      buffer.append(')');
      return buffer.toString();
   }

   public void startReconnectThread(final String threadName) {
      if (logger.isTraceEnabled()) {
         logger.trace("Starting reconnect Thread " + threadName + " on MDB activation " + this);
      }
      Runnable runnable = new Runnable() {
         @Override
         public void run() {
            reconnect(null);
         }
      };
      Thread t = new Thread(runnable, threadName);
      t.start();
   }

   /**
    * Drops all existing connection-related resources and reconnects
    *
    * @param failure if reconnecting in the event of a failure
    */
   public void reconnect(Throwable failure) {
      if (logger.isTraceEnabled()) {
         logger.trace("reconnecting activation " + this);
      }
      if (failure != null) {
         if (failure instanceof ActiveMQException && ((ActiveMQException) failure).getType() == ActiveMQExceptionType.QUEUE_DOES_NOT_EXIST) {
            ActiveMQRALogger.LOGGER.awaitingTopicQueueCreation(getActivationSpec().getDestination());
         } else if (failure instanceof ActiveMQException && ((ActiveMQException) failure).getType() == ActiveMQExceptionType.NOT_CONNECTED) {
            ActiveMQRALogger.LOGGER.awaitingJMSServerCreation();
         } else {
            ActiveMQRALogger.LOGGER.failureInActivation(failure, spec);
         }
      }
      int reconnectCount = 0;
      int setupAttempts = spec.getSetupAttempts();
      long setupInterval = spec.getSetupInterval();

      // Only enter the reconnect loop once
      if (inReconnect.getAndSet(true))
         return;
      try {
         Throwable lastException = failure;
         while (deliveryActive.get() && (setupAttempts == -1 || reconnectCount < setupAttempts)) {
            teardown();

            try {
               Thread.sleep(setupInterval);
            } catch (InterruptedException e) {
               logger.debug("Interrupted trying to reconnect " + spec, e);
               break;
            }

            if (reconnectCount < 1) {
               ActiveMQRALogger.LOGGER.attemptingReconnect(spec);
            }
            try {
               setup();
               ActiveMQRALogger.LOGGER.reconnected();
               break;
            } catch (Throwable t) {
               if (failure instanceof ActiveMQException && ((ActiveMQException) failure).getType() == ActiveMQExceptionType.QUEUE_DOES_NOT_EXIST) {
                  if (lastException == null || !(t instanceof ActiveMQNonExistentQueueException)) {
                     lastException = t;
                     ActiveMQRALogger.LOGGER.awaitingTopicQueueCreation(getActivationSpec().getDestination());
                  }
               } else if (failure instanceof ActiveMQException && ((ActiveMQException) failure).getType() == ActiveMQExceptionType.NOT_CONNECTED) {
                  if (lastException == null || !(t instanceof ActiveMQNotConnectedException)) {
                     lastException = t;
                     ActiveMQRALogger.LOGGER.awaitingJMSServerCreation();
                  }
               } else {
                  ActiveMQRALogger.LOGGER.errorReconnecting(t, spec);
               }
            }
            ++reconnectCount;
         }
      } finally {
         // Leaving failure recovery loop
         inReconnect.set(false);
      }
   }

   public ActiveMQConnectionFactory getConnectionFactory() {
      return this.factory;
   }

   /**
    * Handles the setup
    */
   private class SetupActivation implements Work {

      @Override
      public void run() {
         try {
            setup();
         } catch (Throwable t) {
            reconnect(t);
         }
      }

      @Override
      public void release() {
      }
   }

   private class RebalancingListener implements ClusterTopologyListener {

      @Override
      public void nodeUP(TopologyMember member, boolean last) {
         boolean newNode = false;

         String id = member.getNodeId();
         if (!nodes.contains(id)) {
            if (removedNodes.get(id) == null || (removedNodes.get(id) != null && removedNodes.get(id) < member.getUniqueEventID())) {
               nodes.add(id);
               newNode = true;
            }
         }

         if (lastReceived && newNode) {
            ActiveMQRALogger.LOGGER.rebalancingConnections("nodeUp " + member.toString());
            startReconnectThread("NodeUP Connection Rebalancer");
         } else if (last) {
            lastReceived = true;
         }
      }

      @Override
      public void nodeDown(long eventUID, String nodeID) {
         if (nodes.remove(nodeID)) {
            removedNodes.put(nodeID, eventUID);
            ActiveMQRALogger.LOGGER.rebalancingConnections("nodeDown " + nodeID);
            startReconnectThread("NodeDOWN Connection Rebalancer");
         }
      }
   }
}
