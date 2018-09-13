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
package org.apache.activemq.artemis.protocol.amqp.broker;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.artemis.api.core.ActiveMQAddressExistsException;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQQueueExistsException;
import org.apache.activemq.artemis.api.core.ActiveMQSecurityException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.message.impl.CoreMessageObjectPools;
import org.apache.activemq.artemis.core.paging.PagingManager;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.persistence.OperationContext;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.security.CheckType;
import org.apache.activemq.artemis.core.security.SecurityAuth;
import org.apache.activemq.artemis.core.server.AddressQueryResult;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.QueueQueryResult;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.ServerProducer;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.server.impl.ServerConsumerImpl;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPException;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPInternalErrorException;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPResourceLimitExceededException;
import org.apache.activemq.artemis.protocol.amqp.logger.ActiveMQAMQPProtocolMessageBundle;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPConnectionContext;
import org.apache.activemq.artemis.protocol.amqp.proton.AMQPSessionContext;
import org.apache.activemq.artemis.protocol.amqp.proton.AmqpSupport;
import org.apache.activemq.artemis.protocol.amqp.proton.ProtonServerReceiverContext;
import org.apache.activemq.artemis.protocol.amqp.proton.ProtonServerSenderContext;
import org.apache.activemq.artemis.protocol.amqp.sasl.PlainSASLResult;
import org.apache.activemq.artemis.protocol.amqp.sasl.SASLResult;
import org.apache.activemq.artemis.spi.core.protocol.SessionCallback;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.apache.activemq.artemis.spi.core.remoting.ReadyListener;
import org.apache.activemq.artemis.utils.IDGenerator;
import org.apache.activemq.artemis.utils.RunnableEx;
import org.apache.activemq.artemis.utils.SelectorTranslator;
import org.apache.activemq.artemis.utils.SimpleIDGenerator;
import org.apache.activemq.artemis.utils.UUIDGenerator;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.transaction.TransactionalState;
import org.apache.qpid.proton.amqp.transport.AmqpError;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.codec.ReadableBuffer;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Receiver;
import org.jboss.logging.Logger;

public class AMQPSessionCallback implements SessionCallback {

   private static final Logger logger = Logger.getLogger(AMQPSessionCallback.class);

   protected final IDGenerator consumerIDGenerator = new SimpleIDGenerator(0);

   private final AMQPConnectionCallback protonSPI;

   private final ProtonProtocolManager manager;

   private final StorageManager storageManager;

   private final AMQPConnectionContext connection;

   private final Connection transportConnection;

   private ServerSession serverSession;

   private final OperationContext operationContext;

   private AMQPSessionContext protonSession;

   private final Executor sessionExecutor;

   private final AtomicBoolean draining = new AtomicBoolean(false);

   private CoreMessageObjectPools coreMessageObjectPools = new CoreMessageObjectPools();

   private final AddressQueryCache<AddressQueryResult> addressQueryCache = new AddressQueryCache<>();

   private CreditRunnable creditRunnable;

   public AMQPSessionCallback(AMQPConnectionCallback protonSPI,
                              ProtonProtocolManager manager,
                              AMQPConnectionContext connection,
                              Connection transportConnection,
                              Executor executor,
                              OperationContext operationContext) {
      this.protonSPI = protonSPI;
      this.manager = manager;
      this.storageManager = manager.getServer().getStorageManager();
      this.connection = connection;
      this.transportConnection = transportConnection;
      this.sessionExecutor = executor;
      this.operationContext = operationContext;
   }

   @Override
   public boolean isWritable(ReadyListener callback, Object protocolContext) {
      ProtonServerSenderContext senderContext = (ProtonServerSenderContext) protocolContext;
      return transportConnection.isWritable(callback) && senderContext.getSender().getLocalState() != EndpointState.CLOSED;
   }

   public void onFlowConsumer(Object consumer, int credits, final boolean drain) {
      ServerConsumerImpl serverConsumer = (ServerConsumerImpl) consumer;
      if (drain) {
         // If the draining is already running, then don't do anything
         if (draining.compareAndSet(false, true)) {
            final ProtonServerSenderContext plugSender = (ProtonServerSenderContext) serverConsumer.getProtocolContext();
            serverConsumer.forceDelivery(1, new Runnable() {
               @Override
               public void run() {
                  try {
                     plugSender.reportDrained();
                  } finally {
                     draining.set(false);
                  }
               }
            });
         }
      } else {
         serverConsumer.receiveCredits(-1);
      }
   }

   public void withinContext(RunnableEx run) throws Exception {
      OperationContext context = recoverContext();
      try {
         run.run();
      } finally {
         resetContext(context);
      }
   }

   public void afterIO(IOCallback ioCallback) {
      OperationContext context = recoverContext();
      try {
         manager.getServer().getStorageManager().afterCompleteOperations(ioCallback);
      } finally {
         resetContext(context);
      }
   }

   @Override
   public void browserFinished(ServerConsumer consumer) {

   }

   @Override
   public boolean supportsDirectDelivery() {
      return false;
   }

   public void init(AMQPSessionContext protonSession, SASLResult saslResult) throws Exception {

      this.protonSession = protonSession;

      String name = UUIDGenerator.getInstance().generateStringUUID();

      String user = null;
      String passcode = null;
      if (saslResult != null) {
         user = saslResult.getUser();
         if (saslResult instanceof PlainSASLResult) {
            passcode = ((PlainSASLResult) saslResult).getPassword();
         }
      }

      serverSession = manager.getServer().createSession(name, user, passcode, ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE, protonSPI.getProtonConnectionDelegate(), // RemotingConnection remotingConnection,
                                                        false, // boolean autoCommitSends
                                                        false, // boolean autoCommitAcks,
                                                        false, // boolean preAcknowledge,
                                                        true, //boolean xa,
                                                        (String) null, this, true, operationContext, manager.getPrefixes());
   }

   @Override
   public void afterDelivery() throws Exception {

   }

   public void start() {

   }

   public Object createSender(ProtonServerSenderContext protonSender,
                              SimpleString queue,
                              String filter,
                              boolean browserOnly) throws Exception {
      long consumerID = consumerIDGenerator.generateID();

      filter = SelectorTranslator.convertToActiveMQFilterString(filter);

      ServerConsumer consumer = serverSession.createConsumer(consumerID, queue, SimpleString.toSimpleString(filter), browserOnly, false, null);

      // AMQP handles its own flow control for when it's started
      consumer.setStarted(true);

      consumer.setProtocolContext(protonSender);

      return consumer;
   }

   public void startSender(Object brokerConsumer) throws Exception {
      ServerConsumer serverConsumer = (ServerConsumer) brokerConsumer;
      // flow control is done at proton
      serverConsumer.receiveCredits(-1);
   }

   public void createTemporaryQueue(SimpleString queueName, RoutingType routingType) throws Exception {
      createTemporaryQueue(queueName, queueName, routingType, null);
   }

   public void createTemporaryQueue(SimpleString address,
                                    SimpleString queueName,
                                    RoutingType routingType,
                                    SimpleString filter) throws Exception {
      try {
         serverSession.createQueue(address, queueName, routingType, filter, true, false);
      } catch (ActiveMQSecurityException se) {
         throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.securityErrorCreatingTempDestination(se.getMessage());
      }
   }

   public void createUnsharedDurableQueue(SimpleString address,
                                          RoutingType routingType,
                                          SimpleString queueName,
                                          SimpleString filter) throws Exception {
      try {
         serverSession.createQueue(address, queueName, routingType, filter, false, true, 1, false, false);
      } catch (ActiveMQSecurityException se) {
         throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.securityErrorCreatingConsumer(se.getMessage());
      }
   }

   public void createSharedDurableQueue(SimpleString address,
                                        RoutingType routingType,
                                        SimpleString queueName,
                                        SimpleString filter) throws Exception {
      try {
         serverSession.createQueue(address, queueName, routingType, filter, false, true, -1, false, false);
      } catch (ActiveMQSecurityException se) {
         throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.securityErrorCreatingConsumer(se.getMessage());
      }
   }

   public void createSharedVolatileQueue(SimpleString address,
                                         RoutingType routingType,
                                         SimpleString queueName,
                                         SimpleString filter) throws Exception {
      try {
         serverSession.createQueue(address, queueName, routingType, filter, false, false, -1, true, true);
      } catch (ActiveMQSecurityException se) {
         throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.securityErrorCreatingConsumer(se.getMessage());
      }
   }

   public QueueQueryResult queueQuery(SimpleString queueName, RoutingType routingType, boolean autoCreate) throws Exception {
      QueueQueryResult queueQueryResult = serverSession.executeQueueQuery(queueName);

      if (!queueQueryResult.isExists() && queueQueryResult.isAutoCreateQueues() && autoCreate) {
         try {
            serverSession.createQueue(queueName, queueName, routingType, null, false, true, true);
         } catch (ActiveMQQueueExistsException e) {
            // The queue may have been created by another thread in the mean time.  Catch and do nothing.
         }
         queueQueryResult = serverSession.executeQueueQuery(queueName);
      }

      // if auto-create we will return whatever type was used before
      if (queueQueryResult.isExists() && !queueQueryResult.isAutoCreated() && queueQueryResult.getRoutingType() != routingType) {
         throw new IllegalStateException("Incorrect Routing Type for queue, expecting: " + routingType);
      }

      return queueQueryResult;
   }



   public boolean checkAddressAndAutocreateIfPossible(SimpleString address, RoutingType routingType) throws Exception {
      boolean result = false;
      SimpleString unPrefixedAddress = serverSession.removePrefix(address);
      AddressSettings addressSettings = manager.getServer().getAddressSettingsRepository().getMatch(unPrefixedAddress.toString());

      if (routingType == RoutingType.MULTICAST) {
         if (manager.getServer().getAddressInfo(unPrefixedAddress) == null) {
            if (addressSettings.isAutoCreateAddresses()) {
               try {
                  serverSession.createAddress(address, routingType, true);
               } catch (ActiveMQAddressExistsException e) {
                  // The address may have been created by another thread in the mean time.  Catch and do nothing.
               }
               result = true;
            }
         } else {
            result = true;
         }
      } else if (routingType == RoutingType.ANYCAST) {
         if (manager.getServer().locateQueue(unPrefixedAddress) == null) {
            if (addressSettings.isAutoCreateQueues()) {
               try {
                  serverSession.createQueue(address, address, routingType, null, false, true, true);
               } catch (ActiveMQQueueExistsException e) {
                  // The queue may have been created by another thread in the mean time.  Catch and do nothing.
               }
               result = true;
            }
         } else {
            result = true;
         }
      }

      return result;
   }


   public AddressQueryResult addressQuery(SimpleString addressName,
                                          RoutingType routingType,
                                          boolean autoCreate) throws Exception {

      AddressQueryResult addressQueryResult = addressQueryCache.getResult(addressName);
      if (addressQueryResult != null) {
         return addressQueryResult;
      }

      addressQueryResult = serverSession.executeAddressQuery(addressName);

      if (!addressQueryResult.isExists() && addressQueryResult.isAutoCreateAddresses() && autoCreate) {
         try {
            serverSession.createAddress(addressName, routingType, true);
         } catch (ActiveMQQueueExistsException e) {
            // The queue may have been created by another thread in the mean time.  Catch and do nothing.
         }
         addressQueryResult = serverSession.executeAddressQuery(addressName);
      }

      addressQueryCache.setResult(addressName, addressQueryResult);
      return addressQueryResult;
   }

   public void closeSender(final Object brokerConsumer) throws Exception {

      final ServerConsumer consumer = ((ServerConsumer) brokerConsumer);
      final CountDownLatch latch = new CountDownLatch(1);

      Runnable runnable = new Runnable() {
         @Override
         public void run() {
            try {
               consumer.close(false);
               latch.countDown();
            } catch (Exception e) {
            }
         }
      };

      // Due to the nature of proton this could be happening within flushes from the queue-delivery (depending on how it happened on the protocol)
      // to avoid deadlocks the close has to be done outside of the main thread on an executor
      // otherwise you could get a deadlock
      Executor executor = protonSPI.getExeuctor();

      if (executor != null) {
         executor.execute(runnable);
      } else {
         runnable.run();
      }

      try {
         // a short timeout will do.. 1 second is already long enough
         if (!latch.await(1, TimeUnit.SECONDS)) {
            logger.debug("Could not close consumer on time");
         }
      } catch (InterruptedException e) {
         throw new ActiveMQAMQPInternalErrorException("Unable to close consumers for queue: " + consumer.getQueue());
      }

      consumer.getQueue().recheckRefCount(serverSession.getSessionContext());
   }

   public String tempQueueName() {
      return UUIDGenerator.getInstance().generateStringUUID();
   }

   public void close() throws Exception {
      //need to check here as this can be called if init fails
      if (serverSession != null) {
         OperationContext context = recoverContext();
         try {
            serverSession.close(false);
         } finally {
            resetContext(context);
         }
      }
   }

   public void ack(Transaction transaction, Object brokerConsumer, Message message) throws Exception {
      if (transaction == null) {
         transaction = serverSession.getCurrentTransaction();
      }
      OperationContext oldContext = recoverContext();
      try {
         ((ServerConsumer) brokerConsumer).individualAcknowledge(transaction, message.getMessageID());
      } finally {
         resetContext(oldContext);
      }
   }

   public void cancel(Object brokerConsumer, Message message, boolean updateCounts) throws Exception {
      OperationContext oldContext = recoverContext();
      try {
         ((ServerConsumer) brokerConsumer).individualCancel(message.getMessageID(), updateCounts);
         ((ServerConsumer) brokerConsumer).getQueue().forceDelivery();
      } finally {
         resetContext(oldContext);
      }
   }

   public void reject(Object brokerConsumer, Message message) throws Exception {
      OperationContext oldContext = recoverContext();
      try {
         ((ServerConsumer) brokerConsumer).reject(message.getMessageID());
      } finally {
         resetContext(oldContext);
      }
   }

   public void resumeDelivery(Object consumer) {
      ((ServerConsumer) consumer).receiveCredits(-1);
   }

   public void serverSend(final ProtonServerReceiverContext context,
                          final Transaction transaction,
                          final Receiver receiver,
                          final Delivery delivery,
                          SimpleString address,
                          int messageFormat,
                          ReadableBuffer data) throws Exception {
      AMQPMessage message = new AMQPMessage(messageFormat, data, null, coreMessageObjectPools);
      if (address != null) {
         message.setAddress(address);
      } else {
         // Anonymous relay must set a To value
         address = message.getAddressSimpleString();
         if (address == null) {
            rejectMessage(delivery, Symbol.valueOf("failed"), "Missing 'to' field for message sent to an anonymous producer");
            return;
         }
      }

      //here check queue-autocreation
      RoutingType routingType = context.getRoutingType(receiver, address);
      if (!checkAddressAndAutocreateIfPossible(address, routingType)) {
         throw ActiveMQAMQPProtocolMessageBundle.BUNDLE.addressDoesntExist();
      }

      OperationContext oldcontext = recoverContext();

      try {
         PagingStore store = manager.getServer().getPagingManager().getPageStore(message.getAddressSimpleString());
         if (store.isRejectingMessages()) {
            // We drop pre-settled messages (and abort any associated Tx)
            if (delivery.remotelySettled()) {
               if (transaction != null) {
                  String amqpAddress = delivery.getLink().getTarget().getAddress();
                  ActiveMQException e = new ActiveMQAMQPResourceLimitExceededException("Address is full: " + amqpAddress);
                  transaction.markAsRollbackOnly(e);
               }
            } else {
               rejectMessage(delivery, AmqpError.RESOURCE_LIMIT_EXCEEDED, "Address is full: " + address);
            }
         } else {
            serverSend(transaction, message, delivery, receiver);
         }
      } finally {
         resetContext(oldcontext);
      }
   }

   private void rejectMessage(Delivery delivery, Symbol errorCondition, String errorMessage) {
      ErrorCondition condition = new ErrorCondition();
      condition.setCondition(errorCondition);
      condition.setDescription(errorMessage);
      Rejected rejected = new Rejected();
      rejected.setError(condition);

      afterIO(new IOCallback() {
         @Override
         public void done() {
            connection.lock();
            try {
               delivery.disposition(rejected);
               delivery.settle();
            } finally {
               connection.unlock();
            }
            connection.flush();
         }

         @Override
         public void onError(int errorCode, String errorMessage) {

         }
      });

   }

   private void serverSend(final Transaction transaction,
                           final Message message,
                           final Delivery delivery,
                           final Receiver receiver) throws Exception {
      message.setConnectionID(receiver.getSession().getConnection().getRemoteContainer());
      invokeIncoming((AMQPMessage) message, (ActiveMQProtonRemotingConnection) transportConnection.getProtocolConnection());
      serverSession.send(transaction, message, false, false);

      afterIO(new IOCallback() {
         @Override
         public void done() {
            connection.lock();
            try {
               if (delivery.getRemoteState() instanceof TransactionalState) {
                  TransactionalState txAccepted = new TransactionalState();
                  txAccepted.setOutcome(Accepted.getInstance());
                  txAccepted.setTxnId(((TransactionalState) delivery.getRemoteState()).getTxnId());

                  delivery.disposition(txAccepted);
               } else {
                  delivery.disposition(Accepted.getInstance());
               }
               delivery.settle();
            } finally {
               connection.unlock();
            }
            connection.flush();
         }

         @Override
         public void onError(int errorCode, String errorMessage) {
            connection.lock();
            try {
               receiver.setCondition(new ErrorCondition(AmqpError.ILLEGAL_STATE, errorCode + ":" + errorMessage));
               connection.flush();
            } finally {
               connection.unlock();
            }
         }
      });
   }

   public void offerProducerCredit(final SimpleString address,
                                   final int credits,
                                   final int threshold,
                                   final Receiver receiver) {
      try {
         /*
         * The credit runnable will always be run in this thread unless the address or disc is full. If this is the case the
         * runnable is run once the memory or disc is free, if this happens we don't want to keep adding runnables as this
         * may cause a memory leak, one is enough.
         * */
         if (creditRunnable != null && !creditRunnable.isRun())
            return;
         PagingManager pagingManager = manager.getServer().getPagingManager();
         creditRunnable = new CreditRunnable() {
            boolean isRun = false;
            @Override
            public boolean isRun() {
               return isRun;
            }

            @Override
            public void run() {
               connection.lock();
               try {
                  if (receiver.getCredit() <= threshold) {
                     int topUp = credits - receiver.getCredit();
                     if (topUp > 0) {
                        receiver.flow(topUp);
                     }
                  }
               } finally {
                  isRun = true;
                  connection.unlock();
               }
               connection.flush();
            }
         };

         if (address == null) {
            pagingManager.checkMemory(creditRunnable);
         } else {
            final PagingStore store = manager.getServer().getPagingManager().getPageStore(address);
            store.checkMemory(creditRunnable);
         }
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   public void deleteQueue(SimpleString queueName) throws Exception {
      manager.getServer().destroyQueue(queueName);
   }

   public void resetContext(OperationContext oldContext) {
      storageManager.setContext(oldContext);
   }

   public OperationContext recoverContext() {

      OperationContext oldContext = storageManager.getContext();
      manager.getServer().getStorageManager().setContext(serverSession.getSessionContext());
      return oldContext;
   }

   @Override
   public void sendProducerCreditsMessage(int credits, SimpleString address) {
   }

   @Override
   public boolean updateDeliveryCountAfterCancel(ServerConsumer consumer, MessageReference ref, boolean failed) {
      return false;
   }

   @Override
   public void sendProducerCreditsFailMessage(int credits, SimpleString address) {
   }

   @Override
   public int sendMessage(MessageReference ref, Message message, ServerConsumer consumer, int deliveryCount) {

      ProtonServerSenderContext plugSender = (ProtonServerSenderContext) consumer.getProtocolContext();

      try {
         return plugSender.deliverMessage(ref, deliveryCount, transportConnection);
      } catch (Exception e) {
         connection.lock();
         try {
            plugSender.getSender().setCondition(new ErrorCondition(AmqpError.INTERNAL_ERROR, e.getMessage()));
            connection.flush();
         } finally {
            connection.unlock();
         }
         throw new IllegalStateException("Can't deliver message " + e, e);
      }

   }

   @Override
   public int sendLargeMessage(MessageReference ref,
                               Message message,
                               ServerConsumer consumer,
                               long bodySize,
                               int deliveryCount) {
      return 0;
   }

   @Override
   public int sendLargeMessageContinuation(ServerConsumer consumer,
                                           byte[] body,
                                           boolean continues,
                                           boolean requiresResponse) {
      return 0;
   }

   @Override
   public void closed() {
   }

   @Override
   public void disconnect(ServerConsumer consumer, SimpleString queueName) {
      ErrorCondition ec = new ErrorCondition(AmqpSupport.RESOURCE_DELETED, "Queue was deleted: " + queueName);
      connection.lock();
      try {
         ((ProtonServerSenderContext) consumer.getProtocolContext()).close(ec);
         connection.flush();
      } catch (ActiveMQAMQPException e) {
         logger.error("Error closing link for " + consumer.getQueue().getAddress());
      } finally {
         connection.unlock();
      }
   }

   @Override
   public boolean hasCredits(ServerConsumer consumer) {
      ProtonServerSenderContext plugSender = (ProtonServerSenderContext) consumer.getProtocolContext();

      if (plugSender != null && plugSender.getSender().getCredit() > 0) {
         return true;
      } else {
         return false;
      }
   }

   public Transaction getTransaction(Binary txid, boolean remove) throws ActiveMQAMQPException {
      return protonSPI.getTransaction(txid, remove);
   }

   public Binary newTransaction() {
      return protonSPI.newTransaction();
   }

   public SimpleString getMatchingQueue(SimpleString address, RoutingType routingType) throws Exception {
      return serverSession.getMatchingQueue(address, routingType);
   }

   public SimpleString getMatchingQueue(SimpleString address,
                                        SimpleString queueName,
                                        RoutingType routingType) throws Exception {
      return serverSession.getMatchingQueue(address, queueName, routingType);
   }

   public AddressInfo getAddress(SimpleString address) {
      return serverSession.getAddress(address);
   }

   public void removeTemporaryQueue(SimpleString address) throws Exception {
      serverSession.deleteQueue(address);
   }

   public RoutingType getDefaultRoutingType(SimpleString address) {
      return manager.getServer().getAddressSettingsRepository().getMatch(address.toString()).getDefaultAddressRoutingType();
   }

   public void check(SimpleString address, CheckType checkType, SecurityAuth session) throws Exception {
      manager.getServer().getSecurityStore().check(address, checkType, session);
   }

   public void invokeIncoming(AMQPMessage message, ActiveMQProtonRemotingConnection connection) {
      protonSPI.invokeIncomingInterceptors(message, connection);
   }

   public void invokeOutgoing(AMQPMessage message, ActiveMQProtonRemotingConnection connection) {
      protonSPI.invokeOutgoingInterceptors(message, connection);
   }

   public void addProducer(ServerProducer serverProducer) {
      serverSession.addProducer(serverProducer);
   }

   public void removeProducer(String name) {
      serverSession.removeProducer(name);
   }


   class AddressQueryCache<T> {
      SimpleString address;
      T result;

      public synchronized T getResult(SimpleString parameterAddress) {
         if (address != null && address.equals(parameterAddress)) {
            return result;
         } else {
            result = null;
            address = null;
            return null;
         }
      }

      public synchronized void setResult(SimpleString parameterAddress, T result) {
         this.address = parameterAddress;
         this.result = result;
      }

   }
   interface CreditRunnable extends Runnable {
      boolean isRun();
   }
}
