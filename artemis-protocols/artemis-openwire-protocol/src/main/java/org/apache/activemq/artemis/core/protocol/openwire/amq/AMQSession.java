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
package org.apache.activemq.artemis.core.protocol.openwire.amq;

import javax.jms.ResourceAllocationException;
import javax.transaction.xa.Xid;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.protocol.openwire.OpenWireConnection;
import org.apache.activemq.artemis.core.protocol.openwire.OpenWireMessageConverter;
import org.apache.activemq.artemis.core.protocol.openwire.OpenWireProtocolManager;
import org.apache.activemq.artemis.core.protocol.openwire.OpenWireUtil;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.ServerMessage;
import org.apache.activemq.artemis.core.server.SlowConsumerDetectionListener;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.transaction.impl.XidImpl;
import org.apache.activemq.artemis.spi.core.protocol.SessionCallback;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.apache.activemq.artemis.spi.core.remoting.ReadyListener;
import org.apache.activemq.artemis.utils.IDGenerator;
import org.apache.activemq.artemis.utils.SimpleIDGenerator;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.ProducerAck;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.TransactionInfo;
import org.apache.activemq.command.XATransactionId;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.wireformat.WireFormat;

public class AMQSession implements SessionCallback {

   // ConsumerID is generated inside the session, 0, 1, 2, ... as many consumers as you have on the session
   protected final IDGenerator idGenerator = new SimpleIDGenerator(0);

   private ConnectionInfo connInfo;
   private AMQServerSession coreSession;
   private SessionInfo sessInfo;
   private ActiveMQServer server;
   private OpenWireConnection connection;

   private Map<Long, AMQConsumer> consumers = new ConcurrentHashMap<>();

   private AtomicBoolean started = new AtomicBoolean(false);

   private TransactionId txId = null;

   private boolean isTx;

   private final ScheduledExecutorService scheduledPool;

   private OpenWireProtocolManager manager;

   // The sessionWireformat used by the session
   // this object is meant to be used per thread / session
   // so we make a new one per AMQSession
   private final OpenWireMessageConverter converter;

   public AMQSession(ConnectionInfo connInfo,
                     SessionInfo sessInfo,
                     ActiveMQServer server,
                     OpenWireConnection connection,
                     ScheduledExecutorService scheduledPool,
                     OpenWireProtocolManager manager) {
      this.connInfo = connInfo;
      this.sessInfo = sessInfo;

      this.server = server;
      this.connection = connection;
      this.scheduledPool = scheduledPool;
      this.manager = manager;
      OpenWireFormat marshaller = (OpenWireFormat) connection.getMarshaller();

      this.converter = new OpenWireMessageConverter(marshaller.copy());
   }

   public OpenWireMessageConverter getConverter() {
      return converter;
   }

   public void initialize() {
      String name = sessInfo.getSessionId().toString();
      String username = connInfo.getUserName();
      String password = connInfo.getPassword();

      int minLargeMessageSize = Integer.MAX_VALUE; // disable
      // minLargeMessageSize for
      // now

      try {
         coreSession = (AMQServerSession) server.createSession(name, username, password, minLargeMessageSize, connection, true, false, false, false, null, this, AMQServerSessionFactory.getInstance(), true);

         long sessionId = sessInfo.getSessionId().getValue();
         if (sessionId == -1) {
            this.connection.setAdvisorySession(this);
         }
      }
      catch (Exception e) {
         ActiveMQServerLogger.LOGGER.error("error init session", e);
      }

   }

   public List<AMQConsumer> createConsumer(ConsumerInfo info,
                              AMQSession amqSession,
                              SlowConsumerDetectionListener slowConsumerDetectionListener) throws Exception {
      //check destination
      ActiveMQDestination dest = info.getDestination();
      ActiveMQDestination[] dests = null;
      if (dest.isComposite()) {
         dests = dest.getCompositeDestinations();
      }
      else {
         dests = new ActiveMQDestination[]{dest};
      }
//      Map<ActiveMQDestination, AMQConsumer> consumerMap = new HashMap<>();
      List<AMQConsumer> consumersList = new java.util.LinkedList<>();

      for (ActiveMQDestination openWireDest : dests) {
         if (openWireDest.isQueue()) {
            SimpleString queueName = OpenWireUtil.toCoreAddress(openWireDest);
            getCoreServer().getJMSQueueCreator().create(queueName);
         }
         AMQConsumer consumer = new AMQConsumer(this, openWireDest, info, scheduledPool);

         consumer.init(slowConsumerDetectionListener, idGenerator.generateID());
         consumersList.add(consumer);
         consumers.put(consumer.getNativeId(), consumer);
      }

      return consumersList;
   }

   public void start() {

      coreSession.start();
      started.set(true);

   }

   // rename actualDest to destination
   @Override
   public void afterDelivery() throws Exception {

   }

   @Override
   public void browserFinished(ServerConsumer consumer) {
      AMQConsumer theConsumer = ((AMQServerConsumer) consumer).getAmqConsumer();
      if (theConsumer != null) {
         theConsumer.browseFinished();
      }
   }

   @Override
   public boolean isWritable(ReadyListener callback) {
      return connection.isWritable(callback);
   }

   @Override
   public void sendProducerCreditsMessage(int credits, SimpleString address) {
      // TODO Auto-generated method stub

   }

   @Override
   public void sendProducerCreditsFailMessage(int credits, SimpleString address) {
      // TODO Auto-generated method stub

   }

   @Override
   public int sendMessage(ServerMessage message, ServerConsumer consumerID, int deliveryCount) {
      AMQConsumer consumer = consumers.get(consumerID.getID());
      return consumer.handleDeliver(message, deliveryCount);
   }

   @Override
   public int sendLargeMessage(ServerMessage message, ServerConsumer consumerID, long bodySize, int deliveryCount) {
      // TODO Auto-generated method stub
      return 0;
   }

   @Override
   public int sendLargeMessageContinuation(ServerConsumer consumerID,
                                           byte[] body,
                                           boolean continues,
                                           boolean requiresResponse) {
      // TODO Auto-generated method stub
      return 0;
   }

   @Override
   public void closed() {
      // TODO Auto-generated method stub

   }

   @Override
   public boolean hasCredits(ServerConsumer consumerID) {

      AMQConsumer amqConsumer;

      amqConsumer = consumers.get(consumerID.getID());

      if (amqConsumer != null) {
         return amqConsumer.hasCredits();
      }
      return false;
   }

   @Override
   public void disconnect(ServerConsumer consumerId, String queueName) {
      // TODO Auto-generated method stub

   }

   public void send(final ProducerInfo producerInfo,
                    final Message messageSend,
                    boolean sendProducerAck) throws Exception {
      TransactionId tid = messageSend.getTransactionId();
      if (tid != null) {
         resetSessionTx(tid);
      }

      messageSend.setBrokerInTime(System.currentTimeMillis());

      ActiveMQDestination destination = messageSend.getDestination();
      ActiveMQDestination[] actualDestinations = null;
      if (destination.isComposite()) {
         actualDestinations = destination.getCompositeDestinations();
         messageSend.setOriginalDestination(destination);
      }
      else {
         actualDestinations = new ActiveMQDestination[]{destination};
      }

      ServerMessage originalCoreMsg = getConverter().inbound(messageSend);

      /* ActiveMQ failover transport will attempt to reconnect after connection failure.  Any sent messages that did
      * not receive acks will be resent.  (ActiveMQ broker handles this by returning a last sequence id received to
      * the client).  To handle this in Artemis we use a duplicate ID cache.  To do this we check to see if the
      * message comes from failover connection.  If so we add a DUPLICATE_ID to handle duplicates after a resend. */
      if (connection.getContext().isFaultTolerant() && !messageSend.getProperties().containsKey(org.apache.activemq.artemis.api.core.Message.HDR_DUPLICATE_DETECTION_ID)) {
         originalCoreMsg.putStringProperty(org.apache.activemq.artemis.api.core.Message.HDR_DUPLICATE_DETECTION_ID.toString(), messageSend.getMessageId().toString());
      }

      Runnable runnable;

      if (sendProducerAck) {
         runnable = new Runnable() {
            public void run() {
               try {
                  ProducerAck ack = new ProducerAck(producerInfo.getProducerId(), messageSend.getSize());
                  connection.dispatchSync(ack);
               }
               catch (Exception e) {
                  ActiveMQServerLogger.LOGGER.warn(e.getMessage(), e);
                  connection.sendException(e);
               }

            }
         };
      }
      else {
         final Connection transportConnection = connection.getTransportConnection();

         //         new Exception("Setting to false").printStackTrace();

         if (transportConnection == null) {
            // I don't think this could happen, but just in case, avoiding races
            runnable = null;
         }
         else {
            runnable = new Runnable() {
               public void run() {
                  transportConnection.setAutoRead(true);
               }
            };
         }
      }

      internalSend(actualDestinations, originalCoreMsg, runnable);
   }

   private void internalSend(ActiveMQDestination[] actualDestinations,
                             ServerMessage originalCoreMsg,
                             final Runnable onComplete) throws Exception {

      Runnable runToUse;

      if (actualDestinations.length <= 1 || onComplete == null) {
         // if onComplete is null, this will be null ;)
         runToUse = onComplete;
      }
      else {
         final AtomicInteger count = new AtomicInteger(actualDestinations.length);
         runToUse = new Runnable() {
            @Override
            public void run() {
               if (count.decrementAndGet() == 0) {
                  onComplete.run();
               }
            }
         };
      }

      SimpleString[] addresses = new SimpleString[actualDestinations.length];
      PagingStore[] pagingStores = new PagingStore[actualDestinations.length];

      // We fillup addresses, pagingStores and we will throw failure if that's the case
      for (int i = 0; i < actualDestinations.length; i++) {
         ActiveMQDestination dest = actualDestinations[i];
         addresses[i] = OpenWireUtil.toCoreAddress(dest);
         pagingStores[i] = server.getPagingManager().getPageStore(addresses[i]);
         if (pagingStores[i].getAddressFullMessagePolicy() == AddressFullMessagePolicy.FAIL && pagingStores[i].isFull()) {
            throw new ResourceAllocationException("Queue is full");
         }
      }

      for (int i = 0; i < actualDestinations.length; i++) {

         ServerMessage coreMsg = originalCoreMsg.copy();

         coreMsg.setAddress(addresses[i]);

         PagingStore store = pagingStores[i];

         if (store.isFull()) {
            connection.getTransportConnection().setAutoRead(false);
         }

         getCoreSession().send(coreMsg, false);

         if (runToUse != null) {
            // if the timeout is >0, it will wait this much milliseconds
            // before running the the runToUse
            // this will eventually unblock blocked destinations
            // playing flow control
            store.checkMemory(runToUse);
         }
      }
   }

   public AMQServerSession getCoreSession() {
      return this.coreSession;
   }

   public ActiveMQServer getCoreServer() {
      return this.server;
   }

   public void removeConsumer(long consumerId) throws Exception {
      boolean failed = !(this.txId != null || this.isTx);

      coreSession.amqCloseConsumer(consumerId, failed);
      consumers.remove(consumerId);
   }

   public WireFormat getMarshaller() {
      return this.connection.getMarshaller();
   }

   public void acknowledge(MessageAck ack, AMQConsumer consumer) throws Exception {
      TransactionId tid = ack.getTransactionId();
      if (tid != null) {
         this.resetSessionTx(ack.getTransactionId());
      }
      consumer.acknowledge(ack);

      if (tid == null && ack.getAckType() == MessageAck.STANDARD_ACK_TYPE) {
         this.coreSession.commit();
      }
   }

   //AMQ session and transactions are create separately. Whether a session
   //is transactional or not is known only when a TransactionInfo command
   //comes in.
   public void resetSessionTx(TransactionId xid) throws Exception {
      if ((this.txId != null) && (!this.txId.equals(xid))) {
         throw new IllegalStateException("Session already associated with a tx");
      }

      this.isTx = true;
      if (this.txId == null) {
         //now reset session
         this.txId = xid;

         if (xid.isXATransaction()) {
            XATransactionId xaXid = (XATransactionId) xid;
            coreSession.enableXA();
            XidImpl coreXid = new XidImpl(xaXid.getBranchQualifier(), xaXid.getFormatId(), xaXid.getGlobalTransactionId());
            coreSession.xaStart(coreXid);
         }
         else {
            coreSession.enableTx();
         }

         this.manager.registerTx(this.txId, this);
      }
   }

   private void checkTx(TransactionId inId) {
      if (this.txId == null) {
         throw new IllegalStateException("Session has no transaction associated with it");
      }

      if (!this.txId.equals(inId)) {
         throw new IllegalStateException("Session already associated with another tx");
      }

      this.isTx = true;
   }

   public void endTransaction(TransactionInfo info) throws Exception {
      checkTx(info.getTransactionId());

      if (txId.isXATransaction()) {
         XATransactionId xid = (XATransactionId) txId;
         XidImpl coreXid = new XidImpl(xid.getBranchQualifier(), xid.getFormatId(), xid.getGlobalTransactionId());
         this.coreSession.xaEnd(coreXid);
      }
   }

   public void commitOnePhase(TransactionInfo info) throws Exception {
      checkTx(info.getTransactionId());

      if (txId.isXATransaction()) {
         XATransactionId xid = (XATransactionId) txId;
         XidImpl coreXid = new XidImpl(xid.getBranchQualifier(), xid.getFormatId(), xid.getGlobalTransactionId());
         this.coreSession.xaCommit(coreXid, true);
      }
      else {
         Iterator<AMQConsumer> iter = consumers.values().iterator();
         while (iter.hasNext()) {
            AMQConsumer consumer = iter.next();
            consumer.finishTx();
         }
         this.coreSession.commit();
      }

      this.txId = null;
   }

   public void prepareTransaction(XATransactionId xid) throws Exception {
      checkTx(xid);
      XidImpl coreXid = new XidImpl(xid.getBranchQualifier(), xid.getFormatId(), xid.getGlobalTransactionId());
      this.coreSession.xaPrepare(coreXid);
   }

   public void commitTwoPhase(XATransactionId xid) throws Exception {
      checkTx(xid);
      XidImpl coreXid = new XidImpl(xid.getBranchQualifier(), xid.getFormatId(), xid.getGlobalTransactionId());
      this.coreSession.xaCommit(coreXid, false);

      this.txId = null;
   }

   public void rollback(TransactionInfo info) throws Exception {
      checkTx(info.getTransactionId());
      if (this.txId.isXATransaction()) {
         XATransactionId xid = (XATransactionId) txId;
         XidImpl coreXid = new XidImpl(xid.getBranchQualifier(), xid.getFormatId(), xid.getGlobalTransactionId());
         this.coreSession.xaRollback(coreXid);
      }
      else {
         Iterator<AMQConsumer> iter = consumers.values().iterator();
         Set<Long> acked = new HashSet<>();
         while (iter.hasNext()) {
            AMQConsumer consumer = iter.next();
            consumer.rollbackTx(acked);
         }
         //on local rollback, amq broker doesn't do anything about the delivered
         //messages, which stay at clients until next time
         this.coreSession.amqRollback(acked);
      }

      this.txId = null;
   }

   public void recover(List<TransactionId> recovered) {
      List<Xid> xids = this.coreSession.xaGetInDoubtXids();
      for (Xid xid : xids) {
         XATransactionId amqXid = new XATransactionId(xid);
         recovered.add(amqXid);
      }
   }

   public void forget(final TransactionId tid) throws Exception {
      checkTx(tid);
      XATransactionId xid = (XATransactionId) tid;
      XidImpl coreXid = new XidImpl(xid.getBranchQualifier(), xid.getFormatId(), xid.getGlobalTransactionId());
      this.coreSession.xaForget(coreXid);
      this.txId = null;
   }

   public ConnectionInfo getConnectionInfo() {
      return this.connInfo;
   }

   public void setInternal(boolean internal) {
      this.coreSession.setInternal(internal);
   }

   public boolean isInternal() {
      return this.coreSession.isInternal();
   }

   public void deliverMessage(MessageDispatch dispatch) {
      this.connection.deliverMessage(dispatch);
   }

   public void close() throws Exception {
      this.coreSession.close(false);
   }

   public AMQConsumer getConsumer(Long coreConsumerId) {
      return consumers.get(coreConsumerId);
   }

   public void updateConsumerPrefetchSize(ConsumerId consumerId, int prefetch) {
      Iterator<AMQConsumer> iterator = consumers.values().iterator();
      while (iterator.hasNext()) {
         AMQConsumer consumer = iterator.next();
         if (consumer.getId().equals(consumerId)) {
            consumer.setPrefetchSize(prefetch);
         }
      }
   }

   public OpenWireConnection getConnection() {
      return connection;
   }
}
