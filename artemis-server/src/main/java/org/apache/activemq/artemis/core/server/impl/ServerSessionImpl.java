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
package org.apache.activemq.artemis.core.server.impl;

import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQNonExistentQueueException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.management.CoreNotificationType;
import org.apache.activemq.artemis.api.core.management.ManagementHelper;
import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.core.client.impl.ClientMessageImpl;
import org.apache.activemq.artemis.core.exception.ActiveMQXAException;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.filter.impl.FilterImpl;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.message.impl.MessageInternal;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.persistence.OperationContext;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.BindingType;
import org.apache.activemq.artemis.core.postoffice.Bindings;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.postoffice.QueueBinding;
import org.apache.activemq.artemis.core.remoting.CloseListener;
import org.apache.activemq.artemis.core.remoting.FailureListener;
import org.apache.activemq.artemis.core.security.CheckType;
import org.apache.activemq.artemis.core.security.SecurityStore;
import org.apache.activemq.artemis.core.server.ActiveMQMessageBundle;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.BindingQueryResult;
import org.apache.activemq.artemis.core.server.LargeServerMessage;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.QueueCreator;
import org.apache.activemq.artemis.core.server.QueueQueryResult;
import org.apache.activemq.artemis.core.server.RoutingContext;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.ServerMessage;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.core.server.management.ManagementService;
import org.apache.activemq.artemis.core.server.management.Notification;
import org.apache.activemq.artemis.core.transaction.ResourceManager;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.core.transaction.Transaction.State;
import org.apache.activemq.artemis.core.transaction.TransactionFactory;
import org.apache.activemq.artemis.core.transaction.TransactionOperationAbstract;
import org.apache.activemq.artemis.core.transaction.TransactionPropertyIndexes;
import org.apache.activemq.artemis.core.transaction.impl.TransactionImpl;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.protocol.SessionCallback;
import org.apache.activemq.artemis.utils.TypedProperties;
import org.apache.activemq.artemis.utils.UUID;
import org.apache.activemq.artemis.utils.json.JSONArray;
import org.apache.activemq.artemis.utils.json.JSONObject;

/**
 * Server side Session implementation
 */
public class ServerSessionImpl implements ServerSession, FailureListener {
   // Constants -----------------------------------------------------------------------------

   private static final boolean isTrace = ActiveMQServerLogger.LOGGER.isTraceEnabled();

   // Static -------------------------------------------------------------------------------

   // Attributes ----------------------------------------------------------------------------

   protected final String username;

   protected final String password;

   private final int minLargeMessageSize;

   protected boolean autoCommitSends;

   protected boolean autoCommitAcks;

   protected final boolean preAcknowledge;

   protected final boolean strictUpdateDeliveryCount;

   protected final RemotingConnection remotingConnection;

   protected final Map<Long, ServerConsumer> consumers = new ConcurrentHashMap<Long, ServerConsumer>();

   protected Transaction tx;

   protected boolean xa;

   protected final StorageManager storageManager;

   private final ResourceManager resourceManager;

   public final PostOffice postOffice;

   private final SecurityStore securityStore;

   protected final ManagementService managementService;

   protected volatile boolean started = false;

   protected final Map<SimpleString, TempQueueCleanerUpper> tempQueueCleannerUppers = new HashMap<SimpleString, TempQueueCleanerUpper>();

   protected final String name;

   protected final ActiveMQServer server;

   private final SimpleString managementAddress;

   // The current currentLargeMessage being processed
   private volatile LargeServerMessage currentLargeMessage;

   protected final RoutingContext routingContext = new RoutingContextImpl(null);

   protected final SessionCallback callback;

   private volatile SimpleString defaultAddress;

   private volatile int timeoutSeconds;

   private Map<String, String> metaData;

   private final OperationContext context;

   private QueueCreator queueCreator;

   // Session's usage should be by definition single threaded, hence it's not needed to use a concurrentHashMap here
   protected final Map<SimpleString, Pair<UUID, AtomicLong>> targetAddressInfos = new HashMap<SimpleString, Pair<UUID, AtomicLong>>();

   private final long creationTime = System.currentTimeMillis();

   // to prevent session from being closed twice.
   // this can happen when a session close from client just
   // arrives while the connection failure is detected at the
   // server. Both the request and failure listener will
   // try to close one session from different threads
   // concurrently.
   private volatile boolean closed = false;

   private final TransactionFactory transactionFactory;

   public ServerSessionImpl(final String name,
                            final String username,
                            final String password,
                            final int minLargeMessageSize,
                            final boolean autoCommitSends,
                            final boolean autoCommitAcks,
                            final boolean preAcknowledge,
                            final boolean strictUpdateDeliveryCount,
                            final boolean xa,
                            final RemotingConnection remotingConnection,
                            final StorageManager storageManager,
                            final PostOffice postOffice,
                            final ResourceManager resourceManager,
                            final SecurityStore securityStore,
                            final ManagementService managementService,
                            final ActiveMQServer server,
                            final SimpleString managementAddress,
                            final SimpleString defaultAddress,
                            final SessionCallback callback,
                            final OperationContext context,
                            final QueueCreator queueCreator) throws Exception {
      this(name, username, password, minLargeMessageSize, autoCommitSends, autoCommitAcks, preAcknowledge, strictUpdateDeliveryCount, xa, remotingConnection, storageManager, postOffice, resourceManager, securityStore, managementService, server, managementAddress, defaultAddress, callback, context, null, queueCreator);
   }

   public ServerSessionImpl(final String name,
                            final String username,
                            final String password,
                            final int minLargeMessageSize,
                            final boolean autoCommitSends,
                            final boolean autoCommitAcks,
                            final boolean preAcknowledge,
                            final boolean strictUpdateDeliveryCount,
                            final boolean xa,
                            final RemotingConnection remotingConnection,
                            final StorageManager storageManager,
                            final PostOffice postOffice,
                            final ResourceManager resourceManager,
                            final SecurityStore securityStore,
                            final ManagementService managementService,
                            final ActiveMQServer server,
                            final SimpleString managementAddress,
                            final SimpleString defaultAddress,
                            final SessionCallback callback,
                            final OperationContext context,
                            TransactionFactory transactionFactory,
                            final QueueCreator queueCreator) throws Exception {
      this.username = username;

      this.password = password;

      this.minLargeMessageSize = minLargeMessageSize;

      this.autoCommitSends = autoCommitSends;

      this.autoCommitAcks = autoCommitAcks;

      this.preAcknowledge = preAcknowledge;

      this.remotingConnection = remotingConnection;

      this.storageManager = storageManager;

      this.postOffice = postOffice;

      this.resourceManager = resourceManager;

      this.securityStore = securityStore;

      timeoutSeconds = resourceManager.getTimeoutSeconds();
      this.xa = xa;

      this.strictUpdateDeliveryCount = strictUpdateDeliveryCount;

      this.managementService = managementService;

      this.name = name;

      this.server = server;

      this.managementAddress = managementAddress;

      this.callback = callback;

      this.defaultAddress = defaultAddress;

      remotingConnection.addFailureListener(this);
      this.context = context;

      this.queueCreator = queueCreator;

      if (transactionFactory == null) {
         this.transactionFactory = new DefaultTransactionFactory();
      }
      else {
         this.transactionFactory = transactionFactory;
      }

      if (!xa) {
         tx = newTransaction();
      }
   }

   // ServerSession implementation ----------------------------------------------------------------------------

   /**
    * @return the sessionContext
    */
   public OperationContext getSessionContext() {
      return context;
   }

   public String getUsername() {
      return username;
   }

   public String getPassword() {
      return password;
   }

   public int getMinLargeMessageSize() {
      return minLargeMessageSize;
   }

   public String getName() {
      return name;
   }

   public Object getConnectionID() {
      return remotingConnection.getID();
   }

   public Set<ServerConsumer> getServerConsumers() {
      Set<ServerConsumer> consumersClone = new HashSet<ServerConsumer>(consumers.values());
      return Collections.unmodifiableSet(consumersClone);
   }

   public boolean removeConsumer(final long consumerID) throws Exception {
      return consumers.remove(consumerID) != null;
   }

   protected void doClose(final boolean failed) throws Exception {
      synchronized (this) {
         if (closed)
            return;

         if (tx != null && tx.getXid() == null) {
            // We only rollback local txs on close, not XA tx branches

            try {
               rollback(failed, false);
            }
            catch (Exception e) {
               ActiveMQServerLogger.LOGGER.warn(e.getMessage(), e);
            }
         }
      }

      //putting closing of consumers outside the sync block
      //https://issues.jboss.org/browse/HORNETQ-1141
      Set<ServerConsumer> consumersClone = new HashSet<ServerConsumer>(consumers.values());

      for (ServerConsumer consumer : consumersClone) {
         try {
            consumer.close(failed);
         }
         catch (Throwable e) {
            ActiveMQServerLogger.LOGGER.warn(e.getMessage(), e);
            try {
               consumer.removeItself();
            }
            catch (Throwable e2) {
               ActiveMQServerLogger.LOGGER.warn(e2.getMessage(), e2);
            }
         }
      }

      consumers.clear();

      if (currentLargeMessage != null) {
         try {
            currentLargeMessage.deleteFile();
         }
         catch (Throwable error) {
            ActiveMQServerLogger.LOGGER.errorDeletingLargeMessageFile(error);
         }
      }

      synchronized (this) {
         server.removeSession(name);

         remotingConnection.removeFailureListener(this);

         callback.closed();

         closed = true;
      }
   }

   public QueueCreator getQueueCreator() {
      return queueCreator;
   }

   public ServerConsumer createConsumer(final long consumerID,
                                        final SimpleString queueName,
                                        final SimpleString filterString,
                                        final boolean browseOnly) throws Exception {
      return this.createConsumer(consumerID, queueName, filterString, browseOnly, true, null);
   }

   public ServerConsumer createConsumer(final long consumerID,
                                        final SimpleString queueName,
                                        final SimpleString filterString,
                                        final boolean browseOnly,
                                        final boolean supportLargeMessage,
                                        final Integer credits) throws Exception {
      Binding binding = postOffice.getBinding(queueName);

      if (binding == null || binding.getType() != BindingType.LOCAL_QUEUE) {
         throw ActiveMQMessageBundle.BUNDLE.noSuchQueue(queueName);
      }

      securityStore.check(binding.getAddress(), CheckType.CONSUME, this);

      Filter filter = FilterImpl.createFilter(filterString);

      ServerConsumer consumer = newConsumer(consumerID, this, (QueueBinding) binding, filter, started, browseOnly, storageManager, callback, preAcknowledge, strictUpdateDeliveryCount, managementService, supportLargeMessage, credits);
      consumers.put(consumer.getID(), consumer);

      if (!browseOnly) {
         TypedProperties props = new TypedProperties();

         props.putSimpleStringProperty(ManagementHelper.HDR_ADDRESS, binding.getAddress());

         props.putSimpleStringProperty(ManagementHelper.HDR_CLUSTER_NAME, binding.getClusterName());

         props.putSimpleStringProperty(ManagementHelper.HDR_ROUTING_NAME, binding.getRoutingName());

         props.putIntProperty(ManagementHelper.HDR_DISTANCE, binding.getDistance());

         Queue theQueue = (Queue) binding.getBindable();

         props.putIntProperty(ManagementHelper.HDR_CONSUMER_COUNT, theQueue.getConsumerCount());

         // HORNETQ-946
         props.putSimpleStringProperty(ManagementHelper.HDR_USER, SimpleString.toSimpleString(username));

         props.putSimpleStringProperty(ManagementHelper.HDR_REMOTE_ADDRESS, SimpleString.toSimpleString(this.remotingConnection.getRemoteAddress()));

         props.putSimpleStringProperty(ManagementHelper.HDR_SESSION_NAME, SimpleString.toSimpleString(name));

         if (filterString != null) {
            props.putSimpleStringProperty(ManagementHelper.HDR_FILTERSTRING, filterString);
         }

         Notification notification = new Notification(null, CoreNotificationType.CONSUMER_CREATED, props);

         if (ActiveMQServerLogger.LOGGER.isDebugEnabled()) {
            ActiveMQServerLogger.LOGGER.debug("Session with user=" + username +
                                                 ", connection=" + this.remotingConnection +
                                                 " created a consumer on queue " + queueName +
                                                 ", filter = " + filterString);
         }

         managementService.sendNotification(notification);
      }

      return consumer;
   }

   protected ServerConsumer newConsumer(long consumerID,
                                        ServerSessionImpl serverSessionImpl,
                                        QueueBinding binding,
                                        Filter filter,
                                        boolean started2,
                                        boolean browseOnly,
                                        StorageManager storageManager2,
                                        SessionCallback callback2,
                                        boolean preAcknowledge2,
                                        boolean strictUpdateDeliveryCount2,
                                        ManagementService managementService2,
                                        boolean supportLargeMessage,
                                        Integer credits) throws Exception {
      return new ServerConsumerImpl(consumerID, this, (QueueBinding) binding, filter, started, browseOnly, storageManager, callback, preAcknowledge, strictUpdateDeliveryCount, managementService, supportLargeMessage, credits);
   }

   public Queue createQueue(final SimpleString address,
                            final SimpleString name,
                            final SimpleString filterString,
                            final boolean temporary,
                            final boolean durable) throws Exception {
      if (durable) {
         // make sure the user has privileges to create this queue
         securityStore.check(address, CheckType.CREATE_DURABLE_QUEUE, this);
      }
      else {
         securityStore.check(address, CheckType.CREATE_NON_DURABLE_QUEUE, this);
      }

      server.checkQueueCreationLimit(getUsername());

      Queue queue;

      // any non-temporary JMS queue created via this method should be marked as auto-created
      if (!temporary && address.toString().startsWith(ResourceNames.JMS_QUEUE) && address.equals(name)) {
         queue = server.createQueue(address, name, filterString, SimpleString.toSimpleString(getUsername()), durable, temporary, true);
      }
      else {
         queue = server.createQueue(address, name, filterString, SimpleString.toSimpleString(getUsername()), durable, temporary);
      }

      if (temporary) {
         // Temporary queue in core simply means the queue will be deleted if
         // the remoting connection
         // dies. It does not mean it will get deleted automatically when the
         // session is closed.
         // It is up to the user to delete the queue when finished with it

         TempQueueCleanerUpper cleaner = new TempQueueCleanerUpper(server, name);

         remotingConnection.addCloseListener(cleaner);
         remotingConnection.addFailureListener(cleaner);

         tempQueueCleannerUppers.put(name, cleaner);
      }

      if (ActiveMQServerLogger.LOGGER.isDebugEnabled()) {
         ActiveMQServerLogger.LOGGER.debug("Queue " + name + " created on address " + address +
                                              " with filter=" + filterString + " temporary = " +
                                              temporary + " durable=" + durable + " on session user=" + this.username + ", connection=" + this.remotingConnection);
      }

      return queue;

   }

   @Override
   public void createSharedQueue(final SimpleString address,
                                 final SimpleString name,
                                 boolean durable,
                                 final SimpleString filterString) throws Exception {
      securityStore.check(address, CheckType.CREATE_NON_DURABLE_QUEUE, this);

      server.checkQueueCreationLimit(getUsername());

      server.createSharedQueue(address, name, filterString, SimpleString.toSimpleString(getUsername()), durable);
   }

   public RemotingConnection getRemotingConnection() {
      return remotingConnection;
   }

   public static class TempQueueCleanerUpper implements CloseListener, FailureListener {

      private final SimpleString bindingName;

      private final ActiveMQServer server;

      public TempQueueCleanerUpper(final ActiveMQServer server, final SimpleString bindingName) {
         this.server = server;

         this.bindingName = bindingName;
      }

      private void run() {
         try {
            if (ActiveMQServerLogger.LOGGER.isDebugEnabled()) {
               ActiveMQServerLogger.LOGGER.debug("deleting temporary queue " + bindingName);
            }
            try {
               server.destroyQueue(bindingName, null, false);
            }
            catch (ActiveMQException e) {
               // that's fine.. it can happen due to queue already been deleted
               ActiveMQServerLogger.LOGGER.debug(e.getMessage(), e);
            }
         }
         catch (Exception e) {
            ActiveMQServerLogger.LOGGER.errorRemovingTempQueue(e, bindingName);
         }
      }

      public void connectionFailed(ActiveMQException exception, boolean failedOver) {
         run();
      }

      @Override
      public void connectionFailed(final ActiveMQException me, boolean failedOver, String scaleDownTargetNodeID) {
         connectionFailed(me, failedOver);
      }

      public void connectionClosed() {
         run();
      }

      @Override
      public String toString() {
         return "Temporary Cleaner for queue " + bindingName;
      }

   }

   public void deleteQueue(final SimpleString queueToDelete) throws Exception {
      Binding binding = postOffice.getBinding(queueToDelete);

      if (binding == null || binding.getType() != BindingType.LOCAL_QUEUE) {
         throw new ActiveMQNonExistentQueueException();
      }

      server.destroyQueue(queueToDelete, this, true);

      TempQueueCleanerUpper cleaner = this.tempQueueCleannerUppers.remove(queueToDelete);

      if (cleaner != null) {
         remotingConnection.removeCloseListener(cleaner);

         remotingConnection.removeFailureListener(cleaner);
      }
   }

   public QueueQueryResult executeQueueQuery(final SimpleString name) throws Exception {
      boolean autoCreateJmsQueues = name.toString().startsWith(ResourceNames.JMS_QUEUE) && server.getAddressSettingsRepository().getMatch(name.toString()).isAutoCreateJmsQueues();

      if (name == null) {
         throw ActiveMQMessageBundle.BUNDLE.queueNameIsNull();
      }

      QueueQueryResult response;

      Binding binding = postOffice.getBinding(name);

      if (binding != null && binding.getType() == BindingType.LOCAL_QUEUE) {
         Queue queue = (Queue) binding.getBindable();

         Filter filter = queue.getFilter();

         SimpleString filterString = filter == null ? null : filter.getFilterString();

         response = new QueueQueryResult(name, binding.getAddress(), queue.isDurable(), queue.isTemporary(), filterString, queue.getConsumerCount(), queue.getMessageCount(), autoCreateJmsQueues);
      }
      // make an exception for the management address (see HORNETQ-29)
      else if (name.equals(managementAddress)) {
         response = new QueueQueryResult(name, managementAddress, true, false, null, -1, -1, autoCreateJmsQueues);
      }
      else if (autoCreateJmsQueues) {
         response = new QueueQueryResult(name, name, true, false, null, 0, 0, true, false);
      }
      else {
         response = new QueueQueryResult(null, null, false, false, null, 0, 0, false, false);
      }

      return response;
   }

   public BindingQueryResult executeBindingQuery(final SimpleString address) throws Exception {
      boolean autoCreateJmsQueues = address.toString().startsWith(ResourceNames.JMS_QUEUE) && server.getAddressSettingsRepository().getMatch(address.toString()).isAutoCreateJmsQueues();

      if (address == null) {
         throw ActiveMQMessageBundle.BUNDLE.addressIsNull();
      }

      List<SimpleString> names = new ArrayList<SimpleString>();

      // make an exception for the management address (see HORNETQ-29)
      if (address.equals(managementAddress)) {
         return new BindingQueryResult(true, names, autoCreateJmsQueues);
      }

      Bindings bindings = postOffice.getMatchingBindings(address);

      for (Binding binding : bindings.getBindings()) {
         if (binding.getType() == BindingType.LOCAL_QUEUE || binding.getType() == BindingType.REMOTE_QUEUE) {
            names.add(binding.getUniqueName());
         }
      }

      return new BindingQueryResult(!names.isEmpty(), names, autoCreateJmsQueues);
   }

   public void forceConsumerDelivery(final long consumerID, final long sequence) throws Exception {
      ServerConsumer consumer = consumers.get(consumerID);

      // this would be possible if the server consumer was closed by pings/pongs.. etc
      if (consumer != null) {
         consumer.forceDelivery(sequence);
      }
   }

   public void promptDelivery(long consumerID) {
      ServerConsumer consumer = consumers.get(consumerID);

      // this would be possible if the server consumer was closed by pings/pongs.. etc
      if (consumer != null) {
         consumer.promptDelivery();
      }
   }

   public void acknowledge(final long consumerID, final long messageID) throws Exception {
      ServerConsumer consumer = consumers.get(consumerID);

      if (consumer == null) {
         throw ActiveMQMessageBundle.BUNDLE.consumerDoesntExist(consumerID);
      }

      if (tx != null && tx.getState() == State.ROLLEDBACK) {
         // JBPAPP-8845 - if we let stuff to be acked on a rolled back TX, we will just
         // have these messages to be stuck on the limbo until the server is restarted
         // The tx has already timed out, so we need to ack and rollback immediately
         Transaction newTX = newTransaction();
         consumer.acknowledge(newTX, messageID);
         newTX.rollback();
      }
      else {
         consumer.acknowledge(autoCommitAcks ? null : tx, messageID);
      }
   }

   public void individualAcknowledge(final long consumerID, final long messageID) throws Exception {
      ServerConsumer consumer = consumers.get(consumerID);

      if (tx != null && tx.getState() == State.ROLLEDBACK) {
         // JBPAPP-8845 - if we let stuff to be acked on a rolled back TX, we will just
         // have these messages to be stuck on the limbo until the server is restarted
         // The tx has already timed out, so we need to ack and rollback immediately
         Transaction newTX = newTransaction();
         consumer.individualAcknowledge(tx, messageID);
         newTX.rollback();
      }
      else {
         consumer.individualAcknowledge(autoCommitAcks ? null : tx, messageID);
      }

   }

   public void individualCancel(final long consumerID, final long messageID, boolean failed) throws Exception {
      ServerConsumer consumer = consumers.get(consumerID);

      if (consumer != null) {
         consumer.individualCancel(messageID, failed);
      }

   }

   public void expire(final long consumerID, final long messageID) throws Exception {
      MessageReference ref = consumers.get(consumerID).removeReferenceByID(messageID);

      if (ref != null) {
         ref.getQueue().expire(ref);
      }
   }

   public synchronized void commit() throws Exception {
      if (isTrace) {
         ActiveMQServerLogger.LOGGER.trace("Calling commit");
      }
      try {
         if (tx != null) {
            tx.commit();
         }
      }
      finally {
         if (xa) {
            tx = null;
         }
         else {
            tx = newTransaction();
         }
      }
   }

   public void rollback(final boolean considerLastMessageAsDelivered) throws Exception {
      rollback(false, considerLastMessageAsDelivered);
   }

   /**
    * @param clientFailed                   If the client has failed, we can't decrease the delivery-counts, and the close may issue a rollback
    * @param considerLastMessageAsDelivered
    * @throws Exception
    */
   private synchronized void rollback(final boolean clientFailed,
                                      final boolean considerLastMessageAsDelivered) throws Exception {
      if (tx == null) {
         // Might be null if XA

         tx = newTransaction();
      }

      doRollback(clientFailed, considerLastMessageAsDelivered, tx);

      if (xa) {
         tx = null;
      }
      else {
         tx = newTransaction();
      }
   }

   /**
    * @return
    */
   protected Transaction newTransaction() {
      return transactionFactory.newTransaction(null, storageManager, timeoutSeconds);
   }

   /**
    * @param xid
    * @return
    */
   private Transaction newTransaction(final Xid xid) {
      return transactionFactory.newTransaction(xid, storageManager, timeoutSeconds);
   }

   public synchronized void xaCommit(final Xid xid, final boolean onePhase) throws Exception {

      if (tx != null && tx.getXid().equals(xid)) {
         final String msg = "Cannot commit, session is currently doing work in transaction " + tx.getXid();

         throw new ActiveMQXAException(XAException.XAER_PROTO, msg);
      }
      else {
         Transaction theTx = resourceManager.removeTransaction(xid);

         if (isTrace) {
            ActiveMQServerLogger.LOGGER.trace("XAcommit into " + theTx + ", xid=" + xid);
         }

         if (theTx == null) {
            // checked heuristic committed transactions
            if (resourceManager.getHeuristicCommittedTransactions().contains(xid)) {
               throw new ActiveMQXAException(XAException.XA_HEURCOM, "transaction has been heuristically committed: " + xid);
            }
            // checked heuristic rolled back transactions
            else if (resourceManager.getHeuristicRolledbackTransactions().contains(xid)) {
               throw new ActiveMQXAException(XAException.XA_HEURRB, "transaction has been heuristically rolled back: " + xid);
            }
            else {
               if (isTrace) {
                  ActiveMQServerLogger.LOGGER.trace("XAcommit into " + theTx + ", xid=" + xid + " cannot find it");
               }

               throw new ActiveMQXAException(XAException.XAER_NOTA, "Cannot find xid in resource manager: " + xid);
            }
         }
         else {
            if (theTx.getState() == Transaction.State.SUSPENDED) {
               // Put it back
               resourceManager.putTransaction(xid, theTx);

               throw new ActiveMQXAException(XAException.XAER_PROTO, "Cannot commit transaction, it is suspended " + xid);
            }
            else {
               theTx.commit(onePhase);
            }
         }
      }
   }

   public synchronized void xaEnd(final Xid xid) throws Exception {
      if (tx != null && tx.getXid().equals(xid)) {
         if (tx.getState() == Transaction.State.SUSPENDED) {
            final String msg = "Cannot end, transaction is suspended";

            throw new ActiveMQXAException(XAException.XAER_PROTO, msg);
         }
         else if (tx.getState() == Transaction.State.ROLLEDBACK) {
            final String msg = "Cannot end, transaction is rolled back";

            tx = null;

            throw new ActiveMQXAException(XAException.XAER_PROTO, msg);
         }
         else {
            tx = null;
         }
      }
      else {
         // It's also legal for the TM to call end for a Xid in the suspended
         // state
         // See JTA 1.1 spec 3.4.4 - state diagram
         // Although in practice TMs rarely do this.
         Transaction theTx = resourceManager.getTransaction(xid);

         if (theTx == null) {
            final String msg = "Cannot find suspended transaction to end " + xid;

            throw new ActiveMQXAException(XAException.XAER_NOTA, msg);
         }
         else {
            if (theTx.getState() != Transaction.State.SUSPENDED) {
               final String msg = "Transaction is not suspended " + xid;

               throw new ActiveMQXAException(XAException.XAER_PROTO, msg);
            }
            else {
               theTx.resume();
            }
         }
      }
   }

   public synchronized void xaForget(final Xid xid) throws Exception {
      long id = resourceManager.removeHeuristicCompletion(xid);

      if (id != -1) {
         try {
            storageManager.deleteHeuristicCompletion(id);
         }
         catch (Exception e) {
            e.printStackTrace();

            throw new ActiveMQXAException(XAException.XAER_RMFAIL);
         }
      }
      else {
         throw new ActiveMQXAException(XAException.XAER_NOTA);
      }
   }

   public synchronized void xaJoin(final Xid xid) throws Exception {
      Transaction theTx = resourceManager.getTransaction(xid);

      if (theTx == null) {
         final String msg = "Cannot find xid in resource manager: " + xid;

         throw new ActiveMQXAException(XAException.XAER_NOTA, msg);
      }
      else {
         if (theTx.getState() == Transaction.State.SUSPENDED) {
            throw new ActiveMQXAException(XAException.XAER_PROTO, "Cannot join tx, it is suspended " + xid);
         }
         else {
            tx = theTx;
         }
      }
   }

   public synchronized void xaResume(final Xid xid) throws Exception {
      if (tx != null) {
         final String msg = "Cannot resume, session is currently doing work in a transaction " + tx.getXid();

         throw new ActiveMQXAException(XAException.XAER_PROTO, msg);
      }
      else {
         Transaction theTx = resourceManager.getTransaction(xid);

         if (theTx == null) {
            final String msg = "Cannot find xid in resource manager: " + xid;

            throw new ActiveMQXAException(XAException.XAER_NOTA, msg);
         }
         else {
            if (theTx.getState() != Transaction.State.SUSPENDED) {
               throw new ActiveMQXAException(XAException.XAER_PROTO, "Cannot resume transaction, it is not suspended " + xid);
            }
            else {
               tx = theTx;

               tx.resume();
            }
         }
      }
   }

   public synchronized void xaRollback(final Xid xid) throws Exception {
      if (tx != null && tx.getXid().equals(xid)) {
         final String msg = "Cannot roll back, session is currently doing work in a transaction " + tx.getXid();

         throw new ActiveMQXAException(XAException.XAER_PROTO, msg);
      }
      else {
         Transaction theTx = resourceManager.removeTransaction(xid);
         if (isTrace) {
            ActiveMQServerLogger.LOGGER.trace("xarollback into " + theTx);
         }

         if (theTx == null) {
            // checked heuristic committed transactions
            if (resourceManager.getHeuristicCommittedTransactions().contains(xid)) {
               throw new ActiveMQXAException(XAException.XA_HEURCOM, "transaction has ben heuristically committed: " + xid);
            }
            // checked heuristic rolled back transactions
            else if (resourceManager.getHeuristicRolledbackTransactions().contains(xid)) {
               throw new ActiveMQXAException(XAException.XA_HEURRB, "transaction has ben heuristically rolled back: " + xid);
            }
            else {
               if (isTrace) {
                  ActiveMQServerLogger.LOGGER.trace("xarollback into " + theTx + ", xid=" + xid + " forcing a rollback regular");
               }

               try {
                  // jbpapp-8845
                  // This could have happened because the TX timed out,
                  // at this point we would be better on rolling back this session as a way to prevent consumers from holding their messages
                  this.rollback(false);
               }
               catch (Exception e) {
                  ActiveMQServerLogger.LOGGER.warn(e.getMessage(), e);
               }

               throw new ActiveMQXAException(XAException.XAER_NOTA, "Cannot find xid in resource manager: " + xid);
            }
         }
         else {
            if (theTx.getState() == Transaction.State.SUSPENDED) {
               if (isTrace) {
                  ActiveMQServerLogger.LOGGER.trace("xarollback into " + theTx + " sending tx back as it was suspended");
               }

               // Put it back
               resourceManager.putTransaction(xid, tx);

               throw new ActiveMQXAException(XAException.XAER_PROTO, "Cannot rollback transaction, it is suspended " + xid);
            }
            else {
               doRollback(false, false, theTx);
            }
         }
      }
   }

   public synchronized void xaStart(final Xid xid) throws Exception {
      if (tx != null) {
         ActiveMQServerLogger.LOGGER.xidReplacedOnXStart(tx.getXid().toString(), xid.toString());

         try {
            if (!tx.isEffective()) {
               // we don't want to rollback anything prepared here
               if (tx.getXid() != null) {
                  resourceManager.removeTransaction(tx.getXid());
               }
               tx.rollback();
            }
         }
         catch (Exception e) {
            ActiveMQServerLogger.LOGGER.debug("An exception happened while we tried to debug the previous tx, we can ignore this exception", e);
         }
      }

      tx = newTransaction(xid);

      if (isTrace) {
         ActiveMQServerLogger.LOGGER.trace("xastart into tx= " + tx);
      }

      boolean added = resourceManager.putTransaction(xid, tx);

      if (!added) {
         final String msg = "Cannot start, there is already a xid " + tx.getXid();

         throw new ActiveMQXAException(XAException.XAER_DUPID, msg);
      }
   }

   public synchronized void xaFailed(final Xid xid) throws Exception {
      Transaction theTX = resourceManager.getTransaction(xid);

      if (theTX == null) {
         theTX = newTransaction(xid);
         resourceManager.putTransaction(xid, theTX);
      }

      if (theTX.isEffective()) {
         ActiveMQServerLogger.LOGGER.debug("Client failed with Xid " + xid + " but the server already had it prepared");
         tx = null;
      }
      else {
         theTX.markAsRollbackOnly(new ActiveMQException("Can't commit as a Failover happened during the operation"));
         tx = theTX;
      }

      if (isTrace) {
         ActiveMQServerLogger.LOGGER.trace("xastart into tx= " + tx);
      }
   }

   public synchronized void xaSuspend() throws Exception {

      if (isTrace) {
         ActiveMQServerLogger.LOGGER.trace("xasuspend on " + this.tx);
      }

      if (tx == null) {
         final String msg = "Cannot suspend, session is not doing work in a transaction ";

         throw new ActiveMQXAException(XAException.XAER_PROTO, msg);
      }
      else {
         if (tx.getState() == Transaction.State.SUSPENDED) {
            final String msg = "Cannot suspend, transaction is already suspended " + tx.getXid();

            throw new ActiveMQXAException(XAException.XAER_PROTO, msg);
         }
         else {
            tx.suspend();

            tx = null;
         }
      }
   }

   public synchronized void xaPrepare(final Xid xid) throws Exception {
      if (tx != null && tx.getXid().equals(xid)) {
         final String msg = "Cannot commit, session is currently doing work in a transaction " + tx.getXid();

         throw new ActiveMQXAException(XAException.XAER_PROTO, msg);
      }
      else {
         Transaction theTx = resourceManager.getTransaction(xid);

         if (isTrace) {
            ActiveMQServerLogger.LOGGER.trace("xaprepare into " + ", xid=" + xid + ", tx= " + tx);
         }

         if (theTx == null) {
            final String msg = "Cannot find xid in resource manager: " + xid;

            throw new ActiveMQXAException(XAException.XAER_NOTA, msg);
         }
         else {
            if (theTx.getState() == Transaction.State.SUSPENDED) {
               throw new ActiveMQXAException(XAException.XAER_PROTO, "Cannot prepare transaction, it is suspended " + xid);
            }
            else if (theTx.getState() == Transaction.State.PREPARED) {
               ActiveMQServerLogger.LOGGER.info("ignoring prepare on xid as already called :" + xid);
            }
            else {
               theTx.prepare();
            }
         }
      }
   }

   public List<Xid> xaGetInDoubtXids() {
      List<Xid> xids = new ArrayList<Xid>();

      xids.addAll(resourceManager.getPreparedTransactions());
      xids.addAll(resourceManager.getHeuristicCommittedTransactions());
      xids.addAll(resourceManager.getHeuristicRolledbackTransactions());

      return xids;
   }

   public int xaGetTimeout() {
      return resourceManager.getTimeoutSeconds();
   }

   public void xaSetTimeout(final int timeout) {
      timeoutSeconds = timeout;
      if (tx != null) {
         tx.setTimeout(timeout);
      }
   }

   public void start() {
      setStarted(true);
   }

   public void stop() {
      setStarted(false);
   }

   public void waitContextCompletion() {
      try {
         if (!context.waitCompletion(10000)) {
            ActiveMQServerLogger.LOGGER.errorCompletingContext(new Exception("warning"));
         }
      }
      catch (Exception e) {
         ActiveMQServerLogger.LOGGER.warn(e.getMessage(), e);
      }
   }

   public void close(final boolean failed) {
      if (closed)
         return;
      context.executeOnCompletion(new IOCallback() {
         public void onError(int errorCode, String errorMessage) {
         }

         public void done() {
            try {
               doClose(failed);
            }
            catch (Exception e) {
               ActiveMQServerLogger.LOGGER.errorClosingSession(e);
            }
         }
      });
   }

   public void closeConsumer(final long consumerID) throws Exception {
      final ServerConsumer consumer = consumers.get(consumerID);

      if (consumer != null) {
         consumer.close(false);
      }
      else {
         ActiveMQServerLogger.LOGGER.cannotFindConsumer(consumerID);
      }
   }

   public void receiveConsumerCredits(final long consumerID, final int credits) throws Exception {
      ServerConsumer consumer = consumers.get(consumerID);

      if (consumer == null) {
         ActiveMQServerLogger.LOGGER.debug("There is no consumer with id " + consumerID);

         return;
      }

      consumer.receiveCredits(credits);
   }

   @Override
   public Transaction getCurrentTransaction() {
      if (tx == null) {
         tx = newTransaction();
      }
      return tx;
   }

   public void sendLarge(final MessageInternal message) throws Exception {
      // need to create the LargeMessage before continue
      long id = storageManager.generateID();

      LargeServerMessage largeMsg = storageManager.createLargeMessage(id, message);

      if (ActiveMQServerLogger.LOGGER.isTraceEnabled()) {
         ActiveMQServerLogger.LOGGER.trace("sendLarge::" + largeMsg);
      }

      if (currentLargeMessage != null) {
         ActiveMQServerLogger.LOGGER.replacingIncompleteLargeMessage(currentLargeMessage.getMessageID());
      }

      currentLargeMessage = largeMsg;
   }

   public void send(final ServerMessage message, final boolean direct) throws Exception {
      //large message may come from StompSession directly, in which
      //case the id header already generated.
      if (!message.isLargeMessage()) {
         long id = storageManager.generateID();

         message.setMessageID(id);
         message.encodeMessageIDToBuffer();
      }

      SimpleString address = message.getAddress();

      if (defaultAddress == null && address != null) {
         defaultAddress = address;
      }

      if (address == null) {
         if (message.isDurable()) {
            // We need to force a re-encode when the message gets persisted or when it gets reloaded
            // it will have no address
            message.setAddress(defaultAddress);
         }
         else {
            // We don't want to force a re-encode when the message gets sent to the consumer
            message.setAddressTransient(defaultAddress);
         }
      }

      if (isTrace) {
         ActiveMQServerLogger.LOGGER.trace("send(message=" + message + ", direct=" + direct + ") being called");
      }

      if (message.getAddress() == null) {
         // This could happen with some tests that are ignoring messages
         throw ActiveMQMessageBundle.BUNDLE.noAddress();
      }

      if (message.getAddress().equals(managementAddress)) {
         // It's a management message

         handleManagementMessage(message, direct);
      }
      else {
         doSend(message, direct);
      }
   }

   public void sendContinuations(final int packetSize,
                                 final long messageBodySize,
                                 final byte[] body,
                                 final boolean continues) throws Exception {
      if (currentLargeMessage == null) {
         throw ActiveMQMessageBundle.BUNDLE.largeMessageNotInitialised();
      }

      // Immediately release the credits for the continuations- these don't contribute to the in-memory size
      // of the message

      currentLargeMessage.addBytes(body);

      if (!continues) {
         currentLargeMessage.releaseResources();

         if (messageBodySize >= 0) {
            currentLargeMessage.putLongProperty(Message.HDR_LARGE_BODY_SIZE, messageBodySize);
         }

         doSend(currentLargeMessage, false);

         currentLargeMessage = null;
      }
   }

   public void requestProducerCredits(final SimpleString address, final int credits) throws Exception {
      PagingStore store = server.getPagingManager().getPageStore(address);

      if (!store.checkMemory(new Runnable() {
         public void run() {
            callback.sendProducerCreditsMessage(credits, address);
         }
      })) {
         callback.sendProducerCreditsFailMessage(credits, address);
      }
   }

   public void setTransferring(final boolean transferring) {
      Set<ServerConsumer> consumersClone = new HashSet<ServerConsumer>(consumers.values());

      for (ServerConsumer consumer : consumersClone) {
         consumer.setTransferring(transferring);
      }
   }

   public void addMetaData(String key, String data) {
      if (metaData == null) {
         metaData = new HashMap<String, String>();
      }
      metaData.put(key, data);
   }

   public boolean addUniqueMetaData(String key, String data) {
      ServerSession sessionWithMetaData = server.lookupSession(key, data);
      if (sessionWithMetaData != null && sessionWithMetaData != this) {
         // There is a duplication of this property
         return false;
      }
      else {
         addMetaData(key, data);
         return true;
      }
   }

   public String getMetaData(String key) {
      String data = null;
      if (metaData != null) {
         data = metaData.get(key);
      }

      if (key.equals(ClientSession.JMS_SESSION_CLIENT_ID_PROPERTY)) {
         // we know it's a JMS Session, we now install JMS Hooks of any kind
         installJMSHooks();
      }
      return data;
   }

   public String[] getTargetAddresses() {
      Map<SimpleString, Pair<UUID, AtomicLong>> copy = cloneTargetAddresses();
      Iterator<SimpleString> iter = copy.keySet().iterator();
      int num = copy.keySet().size();
      String[] addresses = new String[num];
      int i = 0;
      while (iter.hasNext()) {
         addresses[i] = iter.next().toString();
         i++;
      }
      return addresses;
   }

   public String getLastSentMessageID(String address) {
      Pair<UUID, AtomicLong> value = targetAddressInfos.get(SimpleString.toSimpleString(address));
      if (value != null) {
         return value.getA().toString();
      }
      else {
         return null;
      }
   }

   public long getCreationTime() {
      return this.creationTime;
   }

   public StorageManager getStorageManager() {
      return this.storageManager;
   }

   @Override
   public void describeProducersInfo(JSONArray array) throws Exception {
      Map<SimpleString, Pair<UUID, AtomicLong>> targetCopy = cloneTargetAddresses();

      for (Map.Entry<SimpleString, Pair<UUID, AtomicLong>> entry : targetCopy.entrySet()) {
         JSONObject producerInfo = new JSONObject();
         producerInfo.put("connectionID", this.getConnectionID().toString());
         producerInfo.put("sessionID", this.getName());
         producerInfo.put("destination", entry.getKey().toString());
         producerInfo.put("lastUUIDSent", entry.getValue().getA());
         producerInfo.put("msgSent", entry.getValue().getB().longValue());
         array.put(producerInfo);
      }
   }

   @Override
   public String toString() {
      StringBuffer buffer = new StringBuffer();
      if (this.metaData != null) {
         for (Map.Entry<String, String> value : metaData.entrySet()) {
            if (buffer.length() != 0) {
               buffer.append(",");
            }
            Object tmpValue = value.getValue();
            if (tmpValue == null || tmpValue.toString().isEmpty()) {
               buffer.append(value.getKey() + "=*N/A*");
            }
            else {
               buffer.append(value.getKey() + "=" + tmpValue);
            }
         }
      }
      // This will actually appear on some management operations
      // so please don't clog this with debug objects
      // unless you provide a special way for management to translate sessions
      return "ServerSessionImpl(" + buffer.toString() + ")";
   }

   // FailureListener implementation
   // --------------------------------------------------------------------

   @Override
   public void connectionFailed(final ActiveMQException me, boolean failedOver) {
      try {
         ActiveMQServerLogger.LOGGER.clientConnectionFailed(name);

         close(true);

         ActiveMQServerLogger.LOGGER.clientConnectionFailedClearingSession(name);
      }
      catch (Throwable t) {
         ActiveMQServerLogger.LOGGER.errorClosingConnection(this);
      }
   }

   @Override
   public void connectionFailed(final ActiveMQException me, boolean failedOver, String scaleDownTargetNodeID) {
      connectionFailed(me, failedOver);
   }

   public void clearLargeMessage() {
      currentLargeMessage = null;
   }

   private void installJMSHooks() {
      this.queueCreator = server.getJMSQueueCreator();
   }

   private Map<SimpleString, Pair<UUID, AtomicLong>> cloneTargetAddresses() {
      return new HashMap<SimpleString, Pair<UUID, AtomicLong>>(targetAddressInfos);
   }

   private void setStarted(final boolean s) {
      Set<ServerConsumer> consumersClone = new HashSet<ServerConsumer>(consumers.values());

      for (ServerConsumer consumer : consumersClone) {
         consumer.setStarted(s);
      }

      started = s;
   }

   private void handleManagementMessage(final ServerMessage message, final boolean direct) throws Exception {
      try {
         securityStore.check(message.getAddress(), CheckType.MANAGE, this);
      }
      catch (ActiveMQException e) {
         if (!autoCommitSends) {
            tx.markAsRollbackOnly(e);
         }
         throw e;
      }

      ServerMessage reply = managementService.handleMessage(message);

      SimpleString replyTo = message.getSimpleStringProperty(ClientMessageImpl.REPLYTO_HEADER_NAME);

      if (replyTo != null) {
         reply.setAddress(replyTo);

         doSend(reply, direct);
      }
   }

   private void doRollback(final boolean clientFailed,
                           final boolean lastMessageAsDelived,
                           final Transaction theTx) throws Exception {
      boolean wasStarted = started;

      List<MessageReference> toCancel = new ArrayList<MessageReference>();

      for (ServerConsumer consumer : consumers.values()) {
         if (wasStarted) {
            consumer.setStarted(false);
         }

         toCancel.addAll(consumer.cancelRefs(clientFailed, lastMessageAsDelived, theTx));
      }

      //we need to check this before we cancel the refs and add them to the tx, any delivering refs will have been delivered
      //after the last tx was rolled back so we should handle them separately. if not they
      //will end up added to the tx but never ever handled even tho they were removed from the consumers delivering refs.
      //we add them to a new tx and roll them back as the calling client will assume that this has happened.
      if (theTx.getState() == State.ROLLEDBACK) {
         Transaction newTX = newTransaction();
         cancelAndRollback(clientFailed, newTX, wasStarted, toCancel);
         throw new IllegalStateException("Transaction has already been rolled back");
      }
      cancelAndRollback(clientFailed, theTx, wasStarted, toCancel);
   }

   private void cancelAndRollback(boolean clientFailed,
                                  Transaction theTx,
                                  boolean wasStarted,
                                  List<MessageReference> toCancel) throws Exception {
      for (MessageReference ref : toCancel) {
         ref.getQueue().cancel(theTx, ref);
      }
      //if we failed don't restart as an attempt to deliver messages may be made before we actually close the consumer
      if (wasStarted && !clientFailed) {
         theTx.addOperation(new TransactionOperationAbstract() {

            @Override
            public void afterRollback(Transaction tx) {
               for (ServerConsumer consumer : consumers.values()) {
                  consumer.setStarted(true);
               }
            }

         });
      }

      theTx.rollback();
   }

   protected void doSend(final ServerMessage msg, final boolean direct) throws Exception {
      // check the user has write access to this address.
      try {
         securityStore.check(msg.getAddress(), CheckType.SEND, this);
      }
      catch (ActiveMQException e) {
         if (!autoCommitSends && tx != null) {
            tx.markAsRollbackOnly(e);
         }
         throw e;
      }

      if (tx == null || autoCommitSends) {
      }
      else {
         routingContext.setTransaction(tx);
      }

      try {
         postOffice.route(msg, queueCreator, routingContext, direct);

         Pair<UUID, AtomicLong> value = targetAddressInfos.get(msg.getAddress());

         if (value == null) {
            targetAddressInfos.put(msg.getAddress(), new Pair<UUID, AtomicLong>(msg.getUserID(), new AtomicLong(1)));
         }
         else {
            value.setA(msg.getUserID());
            value.getB().incrementAndGet();
         }
      }
      finally {
         routingContext.clear();
      }
   }

   @Override
   public List<MessageReference> getInTXMessagesForConsumer(long consumerId) {
      if (this.tx != null) {
         RefsOperation oper = (RefsOperation) tx.getProperty(TransactionPropertyIndexes.REFS_OPERATION);

         if (oper == null) {
            return Collections.emptyList();
         }
         else {
            return oper.getListOnConsumer(consumerId);
         }
      }
      else {
         return Collections.emptyList();
      }
   }

   private static class DefaultTransactionFactory implements TransactionFactory {

      @Override
      public Transaction newTransaction(Xid xid, StorageManager storageManager, int timeoutSeconds) {
         return new TransactionImpl(xid, storageManager, timeoutSeconds);
      }
   }
}
