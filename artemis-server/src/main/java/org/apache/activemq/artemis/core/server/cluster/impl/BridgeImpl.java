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
package org.apache.activemq.artemis.core.server.cluster.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClusterTopologyListener;
import org.apache.activemq.artemis.api.core.client.SendAcknowledgementHandler;
import org.apache.activemq.artemis.api.core.client.SessionFailureListener;
import org.apache.activemq.artemis.api.core.client.TopologyMember;
import org.apache.activemq.artemis.api.core.management.CoreNotificationType;
import org.apache.activemq.artemis.core.client.impl.ClientSessionFactoryImpl;
import org.apache.activemq.artemis.core.client.impl.ClientSessionFactoryInternal;
import org.apache.activemq.artemis.core.client.impl.ClientSessionInternal;
import org.apache.activemq.artemis.core.client.impl.ServerLocatorInternal;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.message.impl.MessageImpl;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.HandleStatus;
import org.apache.activemq.artemis.core.server.LargeServerMessage;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.ServerMessage;
import org.apache.activemq.artemis.core.server.cluster.Bridge;
import org.apache.activemq.artemis.core.server.cluster.Transformer;
import org.apache.activemq.artemis.core.server.impl.QueueImpl;
import org.apache.activemq.artemis.core.server.management.Notification;
import org.apache.activemq.artemis.core.server.management.NotificationService;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.apache.activemq.artemis.spi.core.remoting.ConnectionLifeCycleListener;
import org.apache.activemq.artemis.utils.FutureLatch;
import org.apache.activemq.artemis.utils.ReusableLatch;
import org.apache.activemq.artemis.utils.TypedProperties;
import org.apache.activemq.artemis.utils.UUID;

/**
 * A Core BridgeImpl
 */

public class BridgeImpl implements Bridge, SessionFailureListener, SendAcknowledgementHandler, ConnectionLifeCycleListener {
   // Constants -----------------------------------------------------

   private static final boolean isTrace = ActiveMQServerLogger.LOGGER.isTraceEnabled();

   // Attributes ----------------------------------------------------

   private static final SimpleString JMS_QUEUE_ADDRESS_PREFIX = new SimpleString("jms.queue.");

   private static final SimpleString JMS_TOPIC_ADDRESS_PREFIX = new SimpleString("jms.topic.");

   protected final ServerLocatorInternal serverLocator;

   protected final Executor executor;

   protected final ScheduledExecutorService scheduledExecutor;

   private final ReusableLatch pendingAcks = new ReusableLatch(0);

   private final UUID nodeUUID;

   private final SimpleString name;

   private final Queue queue;

   private final Filter filter;

   private final SimpleString forwardingAddress;

   private final java.util.Queue<MessageReference> refs = new ConcurrentLinkedQueue<MessageReference>();

   private final Transformer transformer;

   private final Object connectionGuard = new Object();

   private final boolean useDuplicateDetection;

   private final String user;

   private final String password;

   private final int reconnectAttempts;

   private final int reconnectAttemptsSameNode;

   private final long retryInterval;

   private final double retryMultiplier;

   private final long maxRetryInterval;

   /**
    * Used when there's a scheduled reconnection
    */
   protected ScheduledFuture<?> futureScheduledReconnection;

   protected volatile ClientSessionInternal session;

   protected String targetNodeID;

   protected TopologyMember targetNode;

   private volatile ClientSessionFactoryInternal csf;

   private volatile ClientProducer producer;

   private volatile boolean connectionWritable = false;

   private volatile boolean started;

   private volatile boolean stopping = false;

   private volatile boolean active;

   private boolean deliveringLargeMessage;

   private int reconnectAttemptsInUse;

   private int retryCount = 0;

   private NotificationService notificationService;

   private boolean keepConnecting = true;

   public BridgeImpl(final ServerLocatorInternal serverLocator,
                     final int initialConnectAttempts,
                     final int reconnectAttempts,
                     final int reconnectAttemptsSameNode,
                     final long retryInterval,
                     final double retryMultiplier,
                     final long maxRetryInterval,
                     final UUID nodeUUID,
                     final SimpleString name,
                     final Queue queue,
                     final Executor executor,
                     final Filter filter,
                     final SimpleString forwardingAddress,
                     final ScheduledExecutorService scheduledExecutor,
                     final Transformer transformer,
                     final boolean useDuplicateDetection,
                     final String user,
                     final String password,
                     final StorageManager storageManager) {

      this.reconnectAttempts = reconnectAttempts;

      this.reconnectAttemptsInUse = initialConnectAttempts;

      this.reconnectAttemptsSameNode = reconnectAttemptsSameNode;

      this.retryInterval = retryInterval;

      this.retryMultiplier = retryMultiplier;

      this.maxRetryInterval = maxRetryInterval;

      this.serverLocator = serverLocator;

      this.nodeUUID = nodeUUID;

      this.name = name;

      this.queue = queue;

      this.executor = executor;

      this.scheduledExecutor = scheduledExecutor;

      this.filter = filter;

      this.forwardingAddress = forwardingAddress;

      this.transformer = transformer;

      this.useDuplicateDetection = useDuplicateDetection;

      this.user = user;

      this.password = password;
   }

   public static final byte[] getDuplicateBytes(final UUID nodeUUID, final long messageID) {
      byte[] bytes = new byte[24];

      ByteBuffer bb = ByteBuffer.wrap(bytes);

      bb.put(nodeUUID.asBytes());

      bb.putLong(messageID);

      return bytes;
   }

   /* (non-Javadoc)
    * @see org.apache.activemq.artemis.core.server.Consumer#getDeliveringMessages()
    */
   @Override
   public List<MessageReference> getDeliveringMessages() {
      synchronized (this) {
         return new ArrayList<MessageReference>(refs);
      }
   }

   private static void cleanUpSessionFactory(ClientSessionFactoryInternal factory) {
      if (factory != null)
         factory.cleanup();
   }

   public void setNotificationService(final NotificationService notificationService) {
      this.notificationService = notificationService;
   }

   public synchronized void start() throws Exception {
      if (started) {
         return;
      }

      started = true;

      stopping = false;

      activate();

      if (notificationService != null) {
         TypedProperties props = new TypedProperties();
         props.putSimpleStringProperty(new SimpleString("name"), name);
         Notification notification = new Notification(nodeUUID.toString(), CoreNotificationType.BRIDGE_STARTED, props);
         notificationService.sendNotification(notification);
      }
   }

   public String debug() {
      return toString();
   }

   private void cancelRefs() {
      MessageReference ref;

      LinkedList<MessageReference> list = new LinkedList<MessageReference>();

      while ((ref = refs.poll()) != null) {
         if (isTrace) {
            ActiveMQServerLogger.LOGGER.trace("Cancelling reference " + ref + " on bridge " + this);
         }
         list.addFirst(ref);
      }

      if (isTrace && list.isEmpty()) {
         ActiveMQServerLogger.LOGGER.trace("didn't have any references to cancel on bridge " + this);
      }

      Queue refqueue = null;

      long timeBase = System.currentTimeMillis();

      for (MessageReference ref2 : list) {
         refqueue = ref2.getQueue();

         try {
            refqueue.cancel(ref2, timeBase);
         }
         catch (Exception e) {
            // There isn't much we can do besides log an error
            ActiveMQServerLogger.LOGGER.errorCancellingRefOnBridge(e, ref2);
         }
      }
   }

   public void flushExecutor() {
      // Wait for any create objects runnable to complete
      FutureLatch future = new FutureLatch();

      executor.execute(future);

      boolean ok = future.await(10000);

      if (!ok) {
         ActiveMQServerLogger.LOGGER.timedOutWaitingToStopBridge();
      }
   }

   public void disconnect() {
      executor.execute(new Runnable() {
         public void run() {
            if (session != null) {
               try {
                  session.cleanUp(false);
               }
               catch (Exception dontcare) {
                  ActiveMQServerLogger.LOGGER.debug(dontcare.getMessage(), dontcare);
               }
               session = null;
            }
         }
      });
   }

   public boolean isConnected() {
      return session != null;
   }

   /**
    * The cluster manager needs to use the same executor to close the serverLocator, otherwise the stop will break.
    * This method is intended to expose this executor to the ClusterManager
    */
   public Executor getExecutor() {
      return executor;
   }

   public void stop() throws Exception {
      if (stopping) {
         return;
      }

      stopping = true;

      if (ActiveMQServerLogger.LOGGER.isDebugEnabled()) {
         ActiveMQServerLogger.LOGGER.debug("Bridge " + this.name + " being stopped");
      }

      if (futureScheduledReconnection != null) {
         futureScheduledReconnection.cancel(true);
      }

      executor.execute(new StopRunnable());

      if (notificationService != null) {
         TypedProperties props = new TypedProperties();
         props.putSimpleStringProperty(new SimpleString("name"), name);
         Notification notification = new Notification(nodeUUID.toString(), CoreNotificationType.BRIDGE_STOPPED, props);
         try {
            notificationService.sendNotification(notification);
         }
         catch (Exception e) {
            ActiveMQServerLogger.LOGGER.broadcastBridgeStoppedError(e);
         }
      }
   }

   public void pause() throws Exception {
      if (ActiveMQServerLogger.LOGGER.isDebugEnabled()) {
         ActiveMQServerLogger.LOGGER.debug("Bridge " + this.name + " being paused");
      }

      executor.execute(new PauseRunnable());

      if (notificationService != null) {
         TypedProperties props = new TypedProperties();
         props.putSimpleStringProperty(new SimpleString("name"), name);
         Notification notification = new Notification(nodeUUID.toString(), CoreNotificationType.BRIDGE_STOPPED, props);
         try {
            notificationService.sendNotification(notification);
         }
         catch (Exception e) {
            ActiveMQServerLogger.LOGGER.notificationBridgeStoppedError(e);
         }
      }
   }

   public void resume() throws Exception {
      queue.addConsumer(BridgeImpl.this);
      queue.deliverAsync();
   }

   public boolean isStarted() {
      return started;
   }

   public synchronized void activate() {
      executor.execute(new ConnectRunnable(this));
   }

   public SimpleString getName() {
      return name;
   }

   public Queue getQueue() {
      return queue;
   }

   public Filter getFilter() {
      return filter;
   }

   // SendAcknowledgementHandler implementation ---------------------

   public SimpleString getForwardingAddress() {
      return forwardingAddress;
   }

   // For testing only
   public RemotingConnection getForwardingConnection() {
      if (session == null) {
         return null;
      }
      else {
         return session.getConnection();
      }
   }

   // Consumer implementation ---------------------------------------

   public void sendAcknowledged(final Message message) {
      if (active) {
         try {
            final MessageReference ref = refs.poll();

            if (ref != null) {
               if (isTrace) {
                  ActiveMQServerLogger.LOGGER.trace(this + " Acking " + ref + " on queue " + ref.getQueue());
               }
               ref.getQueue().acknowledge(ref);
               pendingAcks.countDown();
            }
         }
         catch (Exception e) {
            ActiveMQServerLogger.LOGGER.bridgeFailedToAck(e);
         }
      }
   }

   protected boolean isPlainCoreBridge() {
      return true;
   }

   /* Hook for processing message before forwarding */
   protected ServerMessage beforeForward(final ServerMessage message) {
      if (useDuplicateDetection) {
         // We keep our own DuplicateID for the Bridge, so bouncing back and forths will work fine
         byte[] bytes = getDuplicateBytes(nodeUUID, message.getMessageID());

         message.putBytesProperty(MessageImpl.HDR_BRIDGE_DUPLICATE_ID, bytes);
      }

      if (transformer != null) {
         final ServerMessage transformedMessage = transformer.transform(message);
         if (transformedMessage != message) {
            if (ActiveMQServerLogger.LOGGER.isDebugEnabled()) {
               ActiveMQServerLogger.LOGGER.debug("The transformer " + transformer +
                                                    " made a copy of the message " +
                                                    message +
                                                    " as transformedMessage");
            }
         }
         return transformedMessage;
      }
      else {
         return message;
      }
   }

   public HandleStatus handle(final MessageReference ref) throws Exception {
      if (filter != null && !filter.match(ref.getMessage())) {
         return HandleStatus.NO_MATCH;
      }

      synchronized (this) {
         if (!active || !connectionWritable) {
            if (ActiveMQServerLogger.LOGGER.isDebugEnabled()) {
               ActiveMQServerLogger.LOGGER.debug(this + "::Ignoring reference on bridge as it is set to inactive ref=" + ref);
            }
            return HandleStatus.BUSY;
         }

         if (deliveringLargeMessage) {
            return HandleStatus.BUSY;
         }

         if (isTrace) {
            ActiveMQServerLogger.LOGGER.trace("Bridge " + this + " is handling reference=" + ref);
         }

         ref.handled();

         refs.add(ref);

         final ServerMessage message = beforeForward(ref.getMessage());

         final SimpleString dest;

         if (forwardingAddress != null) {
            dest = forwardingAddress;
         }
         else {
            // Preserve the original address
            dest = message.getAddress();
         }

         pendingAcks.countUp();

         try {
            if (message.isLargeMessage()) {
               deliveringLargeMessage = true;
               deliverLargeMessage(dest, ref, (LargeServerMessage) message);
               return HandleStatus.HANDLED;
            }
            else {
               return deliverStandardMessage(dest, ref, message);
            }
         }
         catch (Exception e) {
            // If an exception happened, we must count down immediately
            pendingAcks.countDown();
            throw e;
         }
      }
   }

   @Override
   public void connectionCreated(ActiveMQComponent component, Connection connection, String protocol) {

   }

   @Override
   public void connectionDestroyed(Object connectionID) {

   }

   @Override
   public void connectionException(Object connectionID, ActiveMQException me) {

   }

   @Override
   public void connectionReadyForWrites(Object connectionID, boolean ready) {
      connectionWritable = ready;
      if (connectionWritable) {
         queue.deliverAsync();
      }
   }

   // FailureListener implementation --------------------------------

   public void proceedDeliver(MessageReference ref) {
      // no op
   }

   public void connectionFailed(final ActiveMQException me, boolean failedOver) {
      connectionFailed(me, failedOver, null);
   }

   public void connectionFailed(final ActiveMQException me, boolean failedOver, String scaleDownTargetNodeID) {
      ActiveMQServerLogger.LOGGER.bridgeConnectionFailed(failedOver);

      synchronized (connectionGuard) {
         keepConnecting = true;
      }

      try {
         if (producer != null) {
            producer.close();
         }

         cleanUpSessionFactory(csf);
      }
      catch (Throwable dontCare) {
      }

      try {
         session.cleanUp(false);
      }
      catch (Throwable dontCare) {
      }

      if (scaleDownTargetNodeID != null && !scaleDownTargetNodeID.equals(nodeUUID.toString())) {
         synchronized (this) {
            try {
               ActiveMQServerLogger.LOGGER.debug("Moving " + queue.getMessageCount() + " messages from " + queue.getName() + " to " + scaleDownTargetNodeID);
               ((QueueImpl) queue).moveReferencesBetweenSnFQueues(SimpleString.toSimpleString(scaleDownTargetNodeID));

               // stop the bridge from trying to reconnect and clean up all the bindings
               fail(true);
            }
            catch (Exception e) {
               ActiveMQServerLogger.LOGGER.warn(e.getMessage(), e);
            }
         }
      }
      else if (scaleDownTargetNodeID != null) {
         // the disconnected node is scaling down to me, no need to reconnect to it
         ActiveMQServerLogger.LOGGER.debug("Received scaleDownTargetNodeID: " + scaleDownTargetNodeID + "; cancelling reconnect.");
         fail(true);
      }
      else {
         ActiveMQServerLogger.LOGGER.debug("Received invalid scaleDownTargetNodeID: " + scaleDownTargetNodeID);

         fail(me.getType() == ActiveMQExceptionType.DISCONNECTED);
      }

      tryScheduleRetryReconnect(me.getType());
   }

   protected void tryScheduleRetryReconnect(final ActiveMQExceptionType type) {
      scheduleRetryConnect();
   }

   public void beforeReconnect(final ActiveMQException exception) {
      // log.warn(name + "::Connection failed before reconnect ", exception);
      // fail(false);
   }

   private void deliverLargeMessage(final SimpleString dest,
                                    final MessageReference ref,
                                    final LargeServerMessage message) {
      executor.execute(new Runnable() {
         public void run() {
            try {
               producer.send(dest, message);

               // as soon as we are done sending the large message
               // we unset the delivery flag and we will call the deliveryAsync on the queue
               // so the bridge will be able to resume work
               unsetLargeMessageDelivery();

               if (queue != null) {
                  queue.deliverAsync();
               }
            }
            catch (final ActiveMQException e) {
               unsetLargeMessageDelivery();
               ActiveMQServerLogger.LOGGER.bridgeUnableToSendMessage(e, ref);

               connectionFailed(e, false);
            }
         }
      });
   }

   /**
    * @param ref
    * @param message
    * @return
    */
   private HandleStatus deliverStandardMessage(SimpleString dest, final MessageReference ref, ServerMessage message) {
      // if we failover during send then there is a chance that the
      // that this will throw a disconnect, we need to remove the message
      // from the acks so it will get resent, duplicate detection will cope
      // with any messages resent

      if (ActiveMQServerLogger.LOGGER.isTraceEnabled()) {
         ActiveMQServerLogger.LOGGER.trace("going to send message: " + message + " from " + this.getQueue());
      }

      try {
         producer.send(dest, message);
      }
      catch (final ActiveMQException e) {
         ActiveMQServerLogger.LOGGER.bridgeUnableToSendMessage(e, ref);

         // We remove this reference as we are returning busy which means the reference will never leave the Queue.
         // because of this we have to remove the reference here
         refs.remove(ref);

         connectionFailed(e, false);

         return HandleStatus.BUSY;
      }

      return HandleStatus.HANDLED;
   }

   /**
    * for use in tests mainly
    *
    * @return
    */
   public TopologyMember getTargetNodeFromTopology() {
      return this.targetNode;
   }

   @Override
   public String toString() {
      return this.getClass().getSimpleName() + "@" +
         Integer.toHexString(System.identityHashCode(this)) +
         " [name=" +
         name +
         ", queue=" +
         queue +
         " targetConnector=" +
         this.serverLocator +
         "]";
   }

   @Override
   public String toManagementString() {
      return this.getClass().getSimpleName() +
         " [name=" +
         name +
         ", queue=" +
         queue.getName() + "/" + queue.getID() + "]";
   }

   public ClientSessionFactoryImpl getCSF() {
      return (ClientSessionFactoryImpl) csf;
   }

   public Transformer getTransformer() {
      return transformer;
   }

   protected void fail(final boolean permanently) {
      ActiveMQServerLogger.LOGGER.debug(this + "\n\t::fail being called, permanently=" + permanently);

      if (queue != null) {
         try {
            if (isTrace) {
               ActiveMQServerLogger.LOGGER.trace("Removing consumer on fail " + this + " from queue " + queue);
            }
            queue.removeConsumer(this);
         }
         catch (Exception dontcare) {
            ActiveMQServerLogger.LOGGER.debug(dontcare);
         }
      }

      cancelRefs();
      if (queue != null) {
         queue.deliverAsync();
      }
   }

   /* Hook for doing extra stuff after connection */
   protected void afterConnect() throws Exception {
      retryCount = 0;
      reconnectAttemptsInUse = reconnectAttempts;
      if (futureScheduledReconnection != null) {
         futureScheduledReconnection.cancel(true);
         futureScheduledReconnection = null;
      }
   }

   /* Hook for creating session factory */
   protected ClientSessionFactoryInternal createSessionFactory() throws Exception {
      if (targetNodeID != null && (this.reconnectAttemptsSameNode < 0 || retryCount <= this.reconnectAttemptsSameNode)) {
         csf = reconnectOnOriginalNode();
      }
      else {
         serverLocator.resetToInitialConnectors();
         csf = (ClientSessionFactoryInternal) serverLocator.createSessionFactory();
      }

      // null here means the targetNodeIS is not available yet
      if (csf != null) {
         csf.setReconnectAttempts(0);
      }

      return csf;
   }

   private ClientSessionFactoryInternal reconnectOnOriginalNode() throws Exception {
      String targetNodeIdUse = targetNodeID;
      TopologyMember nodeUse = targetNode;
      if (targetNodeIdUse != null && nodeUse != null) {
         TransportConfiguration[] configs = new TransportConfiguration[2]; // live and backup
         int numberOfConfigs = 0;

         if (nodeUse.getLive() != null) {
            configs[numberOfConfigs++] = nodeUse.getLive();
         }
         if (nodeUse.getBackup() != null) {
            configs[numberOfConfigs++] = nodeUse.getBackup();
         }

         if (numberOfConfigs > 0) {
            // It will bounce between all the available configs
            int nodeTry = (retryCount - 1) % numberOfConfigs;

            return (ClientSessionFactoryInternal) serverLocator.createSessionFactory(configs[nodeTry]);
         }
      }

      return null;
   }

   protected void setSessionFactory(ClientSessionFactoryInternal sfi) {
      csf = sfi;
   }

   /* This is called only when the bridge is activated */
   protected void connect() {
      if (stopping)
         return;

      synchronized (connectionGuard) {
         if (!keepConnecting)
            return;

         ActiveMQServerLogger.LOGGER.debug("Connecting  " + this + " to its destination [" + nodeUUID.toString() + "], csf=" + this.csf);

         retryCount++;

         try {
            if (csf == null || csf.isClosed()) {
               if (stopping)
                  return;
               csf = createSessionFactory();
               if (csf == null) {
                  // Retrying. This probably means the node is not available (for the cluster connection case)
                  scheduleRetryConnect();
                  return;
               }
               // Session is pre-acknowledge
               session = (ClientSessionInternal) csf.createSession(user, password, false, true, true, true, 1);
            }

            if (forwardingAddress != null) {
               ClientSession.AddressQuery query = null;

               try {
                  query = session.addressQuery(forwardingAddress);
               }
               catch (Throwable e) {
                  ActiveMQServerLogger.LOGGER.errorQueryingBridge(e, name);
                  // This was an issue during startup, we will not count this retry
                  retryCount--;

                  scheduleRetryConnectFixedTimeout(100);
                  return;
               }

               if (forwardingAddress.startsWith(BridgeImpl.JMS_QUEUE_ADDRESS_PREFIX) || forwardingAddress.startsWith(BridgeImpl.JMS_TOPIC_ADDRESS_PREFIX)) {
                  if (!query.isExists()) {
                     ActiveMQServerLogger.LOGGER.errorQueryingBridge(forwardingAddress, retryCount);
                     scheduleRetryConnect();
                     return;
                  }
               }
               else {
                  if (!query.isExists()) {
                     ActiveMQServerLogger.LOGGER.bridgeNoBindings(getName(), getForwardingAddress(), getForwardingAddress());
                  }
               }
            }

            producer = session.createProducer();
            session.addFailureListener(BridgeImpl.this);

            session.setSendAcknowledgementHandler(BridgeImpl.this);

            session.addLifeCycleListener(BridgeImpl.this);

            afterConnect();

            active = true;

            queue.addConsumer(BridgeImpl.this);
            queue.deliverAsync();

            ActiveMQServerLogger.LOGGER.bridgeConnected(this);

            // We only do this on plain core bridges
            if (isPlainCoreBridge()) {
               serverLocator.addClusterTopologyListener(new TopologyListener());
            }

            keepConnecting = false;
            return;
         }
         catch (ActiveMQException e) {
            // the session was created while its server was starting, retry it:
            if (e.getType() == ActiveMQExceptionType.SESSION_CREATION_REJECTED) {
               ActiveMQServerLogger.LOGGER.errorStartingBridge(name);

               // We are not going to count this one as a retry
               retryCount--;

               scheduleRetryConnectFixedTimeout(this.retryInterval);
               return;
            }
            else {
               if (ActiveMQServerLogger.LOGGER.isDebugEnabled()) {
                  ActiveMQServerLogger.LOGGER.debug("Bridge " + this + " is unable to connect to destination. Retrying", e);
               }

               scheduleRetryConnect();
            }
         }
         catch (Exception e) {
            ActiveMQServerLogger.LOGGER.errorConnectingBridge(e, this);
         }
      }
   }

   protected void scheduleRetryConnect() {
      if (serverLocator.isClosed()) {
         ActiveMQServerLogger.LOGGER.bridgeLocatorShutdown();
         return;
      }

      if (stopping) {
         ActiveMQServerLogger.LOGGER.bridgeStopping();
         return;
      }

      if (reconnectAttemptsInUse >= 0 && retryCount > reconnectAttemptsInUse) {
         ActiveMQServerLogger.LOGGER.bridgeAbortStart(name, retryCount, reconnectAttempts);
         fail(true);
         return;
      }

      long timeout = (long) (this.retryInterval * Math.pow(this.retryMultiplier, retryCount));
      if (timeout == 0) {
         timeout = this.retryInterval;
      }
      if (timeout > maxRetryInterval) {
         timeout = maxRetryInterval;
      }

      ActiveMQServerLogger.LOGGER.debug("Bridge " + this +
                                           " retrying connection #" +
                                           retryCount +
                                           ", maxRetry=" +
                                           reconnectAttemptsInUse +
                                           ", timeout=" +
                                           timeout);

      scheduleRetryConnectFixedTimeout(timeout);
   }

   // Inner classes -------------------------------------------------

   protected void scheduleRetryConnectFixedTimeout(final long milliseconds) {
      try {
         cleanUpSessionFactory(csf);
      }
      catch (Throwable ignored) {
      }

      if (stopping)
         return;

      if (ActiveMQServerLogger.LOGGER.isDebugEnabled()) {
         ActiveMQServerLogger.LOGGER.debug("Scheduling retry for bridge " + this.name + " in " + milliseconds + " milliseconds");
      }

      futureScheduledReconnection = scheduledExecutor.schedule(new FutureConnectRunnable(executor, this), milliseconds, TimeUnit.MILLISECONDS);
   }

   private void internalCancelReferences() {
      cancelRefs();

      if (queue != null) {
         queue.deliverAsync();
      }
   }

   /**
    * just set deliveringLargeMessage to false
    */
   private synchronized void unsetLargeMessageDelivery() {
      deliveringLargeMessage = false;
   }

   // The scheduling will still use the main executor here
   private static class FutureConnectRunnable implements Runnable {

      private final BridgeImpl bridge;

      private final Executor executor;

      public FutureConnectRunnable(Executor exe, BridgeImpl bridge) {
         executor = exe;
         this.bridge = bridge;
      }

      public void run() {
         if (bridge.isStarted())
            executor.execute(new ConnectRunnable(bridge));
      }
   }

   private static final class ConnectRunnable implements Runnable {

      private final BridgeImpl bridge;

      public ConnectRunnable(BridgeImpl bridge2) {
         bridge = bridge2;
      }

      public void run() {
         bridge.connect();
      }
   }

   private class StopRunnable implements Runnable {

      public void run() {
         try {
            ActiveMQServerLogger.LOGGER.debug("stopping bridge " + BridgeImpl.this);
            queue.removeConsumer(BridgeImpl.this);

            if (!pendingAcks.await(10, TimeUnit.SECONDS)) {
               ActiveMQServerLogger.LOGGER.timedOutWaitingCompletions(BridgeImpl.this.toString(), pendingAcks.getCount());
            }

            synchronized (BridgeImpl.this) {
               ActiveMQServerLogger.LOGGER.debug("Closing Session for bridge " + BridgeImpl.this.name);

               started = false;

               active = false;

            }

            internalCancelReferences();

            if (session != null) {
               ActiveMQServerLogger.LOGGER.debug("Cleaning up session " + session);
               session.removeFailureListener(BridgeImpl.this);
               try {
                  session.close();
                  session = null;
               }
               catch (ActiveMQException dontcare) {
               }
            }

            if (csf != null) {
               csf.cleanup();
            }

            synchronized (connectionGuard) {
               keepConnecting = true;
            }

            if (isTrace) {
               ActiveMQServerLogger.LOGGER.trace("Removing consumer on stopRunnable " + this + " from queue " + queue);
            }
            ActiveMQServerLogger.LOGGER.bridgeStopped(name);
         }
         catch (RuntimeException e) {
            ActiveMQServerLogger.LOGGER.error("Failed to stop bridge", e);
         }
         catch (InterruptedException e) {
            ActiveMQServerLogger.LOGGER.error("Failed to stop bridge", e);
         }
      }
   }

   private class PauseRunnable implements Runnable {

      public void run() {
         try {
            queue.removeConsumer(BridgeImpl.this);

            if (!pendingAcks.await(60, TimeUnit.SECONDS)) {
               ActiveMQServerLogger.LOGGER.timedOutWaitingCompletions(BridgeImpl.this.toString(), pendingAcks.getCount());
            }

            synchronized (BridgeImpl.this) {
               started = false;
               active = false;
            }

            internalCancelReferences();

            ActiveMQServerLogger.LOGGER.bridgePaused(name);
         }
         catch (Exception e) {
            ActiveMQServerLogger.LOGGER.errorPausingBridge(e);
         }
      }

   }

   private class TopologyListener implements ClusterTopologyListener {

      // ClusterListener
      @Override
      public void nodeUP(TopologyMember member, boolean last) {
         ClientSessionInternal sessionToUse = session;
         RemotingConnection connectionToUse = sessionToUse != null ? sessionToUse.getConnection() : null;

         if (member != null && BridgeImpl.this.targetNodeID != null && BridgeImpl.this.targetNodeID.equals(member.getNodeId())) {
            // this could be an update of the topology say after a backup started
            BridgeImpl.this.targetNode = member;
         }
         else {
            // we don't need synchronization here, but we need to make sure we won't get a NPE on races
            if (connectionToUse != null && member.isMember(connectionToUse)) {
               BridgeImpl.this.targetNode = member;
               BridgeImpl.this.targetNodeID = member.getNodeId();
            }
         }

      }

      @Override
      public void nodeDown(long eventUID, String nodeID) {

      }
   }
}
