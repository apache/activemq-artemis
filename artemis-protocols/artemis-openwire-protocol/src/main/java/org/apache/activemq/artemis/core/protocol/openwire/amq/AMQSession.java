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

import javax.transaction.xa.Xid;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.ExceptionResponse;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.ProducerAck;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.TransactionInfo;
import org.apache.activemq.command.XATransactionId;
import org.apache.activemq.artemis.core.paging.impl.PagingStoreImpl;
import org.apache.activemq.artemis.core.protocol.openwire.OpenWireConnection;
import org.apache.activemq.artemis.core.protocol.openwire.OpenWireMessageConverter;
import org.apache.activemq.artemis.core.protocol.openwire.OpenWireProtocolManager;
import org.apache.activemq.artemis.core.protocol.openwire.OpenWireUtil;
import org.apache.activemq.artemis.core.protocol.openwire.SendingResult;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.ServerMessage;
import org.apache.activemq.artemis.core.server.impl.ServerMessageImpl;
import org.apache.activemq.artemis.core.transaction.impl.XidImpl;
import org.apache.activemq.artemis.spi.core.protocol.SessionCallback;
import org.apache.activemq.artemis.spi.core.remoting.ReadyListener;
import org.apache.activemq.wireformat.WireFormat;

public class AMQSession implements SessionCallback
{
   private AMQServerSession coreSession;
   private ConnectionInfo connInfo;
   private SessionInfo sessInfo;
   private ActiveMQServer server;
   private OpenWireConnection connection;
   //native id -> consumer
   private Map<Long, AMQConsumer> consumers = new ConcurrentHashMap<Long, AMQConsumer>();
   //amq id -> native id
   private Map<Long, Long> consumerIdMap = new HashMap<Long, Long>();

   private Map<Long, AMQProducer> producers = new HashMap<Long, AMQProducer>();

   private AtomicBoolean started = new AtomicBoolean(false);

   private TransactionId txId = null;

   private boolean isTx;

   private OpenWireProtocolManager manager;

   public AMQSession(ConnectionInfo connInfo, SessionInfo sessInfo,
         ActiveMQServer server, OpenWireConnection connection, OpenWireProtocolManager manager)
   {
      this.connInfo = connInfo;
      this.sessInfo = sessInfo;
      this.server = server;
      this.connection = connection;
      this.manager = manager;
   }

   public void initialize()
   {
      String name = sessInfo.getSessionId().toString();
      String username = connInfo.getUserName();
      String password = connInfo.getPassword();

      int minLargeMessageSize = Integer.MAX_VALUE; // disable
                                                   // minLargeMessageSize for
                                                   // now

      try
      {
         coreSession = (AMQServerSession) server.createSession(name, username, password,
               minLargeMessageSize, connection, true, false, false, false,
               null, this, new AMQServerSessionFactory(), true);

         long sessionId = sessInfo.getSessionId().getValue();
         if (sessionId == -1)
         {
            this.connection.setAdvisorySession(this);
         }
      }
      catch (Exception e)
      {
         ActiveMQServerLogger.LOGGER.error("error init session", e);
      }

   }

   public void createConsumer(ConsumerInfo info) throws Exception
   {
      //check destination
      ActiveMQDestination dest = info.getDestination();
      ActiveMQDestination[] dests = null;
      if (dest.isComposite())
      {
         dests = dest.getCompositeDestinations();
      }
      else
      {
         dests = new ActiveMQDestination[] {dest};
      }

      for (ActiveMQDestination d : dests)
      {
         if (d.isQueue())
         {
            SimpleString queueName = OpenWireUtil.toCoreAddress(d);
            getCoreServer().getJMSQueueCreator().create(queueName);
         }
         AMQConsumer consumer = new AMQConsumer(this, d, info);
         consumer.init();
         consumers.put(consumer.getNativeId(), consumer);
         this.consumerIdMap.put(info.getConsumerId().getValue(), consumer.getNativeId());
      }
      coreSession.start();
      started.set(true);
   }

   @Override
   public void sendProducerCreditsMessage(int credits, SimpleString address)
   {
      // TODO Auto-generated method stub

   }

   @Override
   public void sendProducerCreditsFailMessage(int credits, SimpleString address)
   {
      // TODO Auto-generated method stub

   }

   @Override
   public int sendMessage(ServerMessage message, ServerConsumer consumerID, int deliveryCount)
   {
      AMQConsumer consumer = consumers.get(consumerID.getID());
      return consumer.handleDeliver(message, deliveryCount);
   }

   @Override
   public int sendLargeMessage(ServerMessage message, ServerConsumer consumerID,
         long bodySize, int deliveryCount)
   {
      // TODO Auto-generated method stub
      return 0;
   }

   @Override
   public int sendLargeMessageContinuation(ServerConsumer consumerID, byte[] body,
         boolean continues, boolean requiresResponse)
   {
      // TODO Auto-generated method stub
      return 0;
   }

   @Override
   public void closed()
   {
      // TODO Auto-generated method stub

   }

   @Override
   public void addReadyListener(ReadyListener listener)
   {
      // TODO Auto-generated method stub

   }

   @Override
   public void removeReadyListener(ReadyListener listener)
   {
      // TODO Auto-generated method stub

   }

   @Override
   public boolean hasCredits(ServerConsumer consumerID)
   {
      return true;
   }

   @Override
   public void disconnect(ServerConsumer consumerId, String queueName)
   {
      // TODO Auto-generated method stub

   }

   public AMQServerSession getCoreSession()
   {
      return this.coreSession;
   }

   public ActiveMQServer getCoreServer()
   {
      return this.server;
   }

   public void removeConsumer(ConsumerInfo info) throws Exception
   {
      long consumerId = info.getConsumerId().getValue();
      long nativeId = this.consumerIdMap.remove(consumerId);
      if (this.txId != null || this.isTx)
      {
         ((AMQServerSession)coreSession).amqCloseConsumer(nativeId, false);
      }
      else
      {
         ((AMQServerSession)coreSession).amqCloseConsumer(nativeId, true);
      }
      AMQConsumer consumer = consumers.remove(nativeId);
   }

   public void createProducer(ProducerInfo info) throws Exception
   {
      AMQProducer producer = new AMQProducer(this, info);
      producer.init();
      producers.put(info.getProducerId().getValue(), producer);
   }

   public void removeProducer(ProducerInfo info)
   {
      removeProducer(info.getProducerId());
   }

   public void removeProducer(ProducerId id)
   {
      producers.remove(id.getValue());
   }

   public SendingResult send(AMQProducerBrokerExchange producerExchange,
         Message messageSend, boolean sendProducerAck) throws Exception
   {
      SendingResult result = new SendingResult();
      TransactionId tid = messageSend.getTransactionId();
      if (tid != null)
      {
         resetSessionTx(tid);
      }

      messageSend.setBrokerInTime(System.currentTimeMillis());

      ActiveMQDestination destination = messageSend.getDestination();
      ActiveMQDestination[] actualDestinations = null;
      if (destination.isComposite())
      {
         actualDestinations = destination.getCompositeDestinations();
      }
      else
      {
         actualDestinations = new ActiveMQDestination[] {destination};
      }

      for (ActiveMQDestination dest : actualDestinations)
      {
         ServerMessageImpl coreMsg = new ServerMessageImpl(-1, 1024);

         /* ActiveMQ failover transport will attempt to reconnect after connection failure.  Any sent messages that did
         * not receive acks will be resent.  (ActiveMQ broker handles this by returning a last sequence id received to
         * the client).  To handle this in Artemis we use a duplicate ID cache.  To do this we check to see if the
         * message comes from failover connection.  If so we add a DUPLICATE_ID to handle duplicates after a resend. */
         if (producerExchange.getConnectionContext().isFaultTolerant() && !messageSend.getProperties().containsKey(ServerMessage.HDR_DUPLICATE_DETECTION_ID))
         {
            coreMsg.putStringProperty(ServerMessage.HDR_DUPLICATE_DETECTION_ID.toString(), messageSend.getMessageId().toString());
         }
         OpenWireMessageConverter.toCoreMessage(coreMsg, messageSend, connection.getMarshaller());
         SimpleString address = OpenWireUtil.toCoreAddress(dest);
         coreMsg.setAddress(address);

         PagingStoreImpl store = (PagingStoreImpl)server.getPagingManager().getPageStore(address);
         if (store.isFull())
         {
            result.setBlockNextSend(true);
            result.setBlockPagingStore(store);
            result.setBlockingAddress(address);
            //now we hold this message send until the store has space.
            //we do this by put it in a scheduled task
            ScheduledExecutorService scheduler = server.getScheduledPool();
            Runnable sendRetryTask = new SendRetryTask(coreMsg, producerExchange, sendProducerAck,
                                                       messageSend.getSize(), messageSend.getCommandId());
            scheduler.schedule(sendRetryTask, 10, TimeUnit.MILLISECONDS);
         }
         else
         {
            coreSession.send(coreMsg, false);
         }
      }
      return result;
   }

   public WireFormat getMarshaller()
   {
      return this.connection.getMarshaller();
   }

   public void acknowledge(MessageAck ack) throws Exception
   {
      TransactionId tid = ack.getTransactionId();
      if (tid != null)
      {
         this.resetSessionTx(ack.getTransactionId());
      }
      ConsumerId consumerId = ack.getConsumerId();
      long nativeConsumerId = consumerIdMap.get(consumerId.getValue());
      AMQConsumer consumer = consumers.get(nativeConsumerId);
      consumer.acknowledge(ack);

      if (tid == null && ack.getAckType() == MessageAck.STANDARD_ACK_TYPE)
      {
         this.coreSession.commit();
      }
   }

   //AMQ session and transactions are create separately. Whether a session
   //is transactional or not is known only when a TransactionInfo command
   //comes in.
   public void resetSessionTx(TransactionId xid) throws Exception
   {
      if ((this.txId != null) && (!this.txId.equals(xid)))
      {
         throw new IllegalStateException("Session already associated with a tx");
      }

      this.isTx = true;
      if (this.txId == null)
      {
         //now reset session
         this.txId = xid;

         if (xid.isXATransaction())
         {
            XATransactionId xaXid = (XATransactionId)xid;
            coreSession.enableXA();
            XidImpl coreXid = new XidImpl(xaXid.getBranchQualifier(), xaXid.getFormatId(), xaXid.getGlobalTransactionId());
            coreSession.xaStart(coreXid);
         }
         else
         {
            coreSession.enableTx();
         }

         this.manager.registerTx(this.txId, this);
      }
   }

   private void checkTx(TransactionId inId)
   {
      if (this.txId == null)
      {
         throw new IllegalStateException("Session has no transaction associated with it");
      }

      if (!this.txId.equals(inId))
      {
         throw new IllegalStateException("Session already associated with another tx");
      }

      this.isTx = true;
   }

   public void commitOnePhase(TransactionInfo info) throws Exception
   {
      checkTx(info.getTransactionId());

      if (txId.isXATransaction())
      {
         XATransactionId xid = (XATransactionId) txId;
         XidImpl coreXid = new XidImpl(xid.getBranchQualifier(), xid.getFormatId(), xid.getGlobalTransactionId());
         this.coreSession.xaCommit(coreXid, true);
      }
      else
      {
         Iterator<AMQConsumer> iter = consumers.values().iterator();
         while (iter.hasNext())
         {
            AMQConsumer consumer = iter.next();
            consumer.finishTx();
         }
         this.coreSession.commit();
      }

      this.txId = null;
   }

   public void prepareTransaction(XATransactionId xid) throws Exception
   {
      checkTx(xid);
      XidImpl coreXid = new XidImpl(xid.getBranchQualifier(), xid.getFormatId(), xid.getGlobalTransactionId());
      this.coreSession.xaPrepare(coreXid);
   }

   public void commitTwoPhase(XATransactionId xid) throws Exception
   {
      checkTx(xid);
      XidImpl coreXid = new XidImpl(xid.getBranchQualifier(), xid.getFormatId(), xid.getGlobalTransactionId());
      this.coreSession.xaCommit(coreXid, false);

      this.txId = null;
   }

   public void rollback(TransactionInfo info) throws Exception
   {
      checkTx(info.getTransactionId());
      if (this.txId.isXATransaction())
      {
         XATransactionId xid = (XATransactionId) txId;
         XidImpl coreXid = new XidImpl(xid.getBranchQualifier(), xid.getFormatId(), xid.getGlobalTransactionId());
         this.coreSession.xaRollback(coreXid);
      }
      else
      {
         Iterator<AMQConsumer> iter = consumers.values().iterator();
         Set<Long> acked = new HashSet<Long>();
         while (iter.hasNext())
         {
            AMQConsumer consumer = iter.next();
            consumer.rollbackTx(acked);
         }
         //on local rollback, amq broker doesn't do anything about the delivered
         //messages, which stay at clients until next time
         this.coreSession.amqRollback(acked);
      }

      this.txId = null;
   }

   public void recover(List<TransactionId> recovered)
   {
      List<Xid> xids = this.coreSession.xaGetInDoubtXids();
      for (Xid xid : xids)
      {
         XATransactionId amqXid = new XATransactionId(xid);
         recovered.add(amqXid);
      }
   }

   public void forget(final TransactionId tid) throws Exception
   {
      checkTx(tid);
      XATransactionId xid = (XATransactionId) tid;
      XidImpl coreXid = new XidImpl(xid.getBranchQualifier(), xid.getFormatId(), xid.getGlobalTransactionId());
      this.coreSession.xaForget(coreXid);
      this.txId = null;
   }

   public ConnectionInfo getConnectionInfo()
   {
      return this.connInfo;
   }

   public void setInternal(boolean internal)
   {
      this.coreSession.setInternal(internal);
   }

   public boolean isInternal()
   {
      return this.coreSession.isInternal();
   }

   public void deliverMessage(MessageDispatch dispatch)
   {
      this.connection.deliverMessage(dispatch);
   }

   public void close() throws Exception
   {
      this.coreSession.close(false);
   }

   private class SendRetryTask implements Runnable
   {
      private ServerMessage coreMsg;
      private AMQProducerBrokerExchange producerExchange;
      private boolean sendProducerAck;
      private int msgSize;
      private int commandId;

      public SendRetryTask(ServerMessage coreMsg, AMQProducerBrokerExchange producerExchange,
            boolean sendProducerAck, int msgSize, int commandId)
      {
         this.coreMsg = coreMsg;
         this.producerExchange = producerExchange;
         this.sendProducerAck = sendProducerAck;
         this.msgSize = msgSize;
         this.commandId = commandId;
      }

      @Override
      public void run()
      {
         synchronized (AMQSession.this)
         {
            try
            {
               // check pageStore
               SimpleString address = coreMsg.getAddress();
               PagingStoreImpl store = (PagingStoreImpl) server
                     .getPagingManager().getPageStore(address);
               if (store.isFull())
               {
                  // if store is still full, schedule another
                  server.getScheduledPool().schedule(this, 10, TimeUnit.MILLISECONDS);
               }
               else
               {
                  // now send the message again.
                  coreSession.send(coreMsg, false);

                  if (sendProducerAck)
                  {
                     ProducerInfo producerInfo = producerExchange
                           .getProducerState().getInfo();
                     ProducerAck ack = new ProducerAck(
                           producerInfo.getProducerId(), msgSize);
                     connection.dispatchAsync(ack);
                  }
                  else
                  {
                     Response response = new Response();
                     response.setCorrelationId(commandId);
                     connection.dispatchAsync(response);
                  }
               }
            }
            catch (Exception e)
            {
               ExceptionResponse response = new ExceptionResponse(e);
               response.setCorrelationId(commandId);
               connection.dispatchAsync(response);
            }
         }

      }
   }

   public void blockingWaitForSpace(AMQProducerBrokerExchange producerExchange, SendingResult result) throws IOException
   {
      long start = System.currentTimeMillis();
      long nextWarn = start;
      producerExchange.blockingOnFlowControl(true);

      AMQConnectionContext context = producerExchange.getConnectionContext();
      PagingStoreImpl store = result.getBlockPagingStore();

      //Destination.DEFAULT_BLOCKED_PRODUCER_WARNING_INTERVAL
      long blockedProducerWarningInterval = 30000;
      ProducerId producerId = producerExchange.getProducerState().getInfo().getProducerId();

      while (store.isFull())
      {
         if (context.getStopping().get())
         {
            throw new IOException("Connection closed, send aborted.");
         }

         long now = System.currentTimeMillis();
         if (now >= nextWarn)
         {
            ActiveMQServerLogger.LOGGER.warn("Memory Limit reached. Producer (" + producerId + ") stopped to prevent flooding "
                               + result.getBlockingAddress()
                               + " See http://activemq.apache.org/producer-flow-control.html for more info"
                               + " (blocking for " + ((now - start) / 1000) + "s");
            nextWarn = now + blockedProducerWarningInterval;
         }
      }
      producerExchange.blockingOnFlowControl(false);
   }
}
