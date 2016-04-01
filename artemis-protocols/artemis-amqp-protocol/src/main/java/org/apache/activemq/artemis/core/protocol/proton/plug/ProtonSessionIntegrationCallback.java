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
package org.apache.activemq.artemis.core.protocol.proton.plug;

import java.util.concurrent.Executor;

import io.netty.buffer.ByteBuf;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.apache.activemq.artemis.spi.core.remoting.ReadyListener;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.transport.AmqpError;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.jms.EncodedMessage;
import org.apache.qpid.proton.message.ProtonJMessage;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.core.protocol.proton.ProtonProtocolManager;
import org.apache.activemq.artemis.core.server.QueueQueryResult;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.ServerMessage;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.spi.core.protocol.SessionCallback;
import org.apache.activemq.artemis.utils.ByteUtil;
import org.apache.activemq.artemis.utils.IDGenerator;
import org.apache.activemq.artemis.utils.SimpleIDGenerator;
import org.apache.activemq.artemis.utils.UUIDGenerator;
import org.proton.plug.AMQPConnectionContext;
import org.proton.plug.AMQPSessionCallback;
import org.proton.plug.AMQPSessionContext;
import org.proton.plug.SASLResult;
import org.proton.plug.context.ProtonPlugSender;
import org.proton.plug.sasl.PlainSASLResult;

public class ProtonSessionIntegrationCallback implements AMQPSessionCallback, SessionCallback {

   protected final IDGenerator consumerIDGenerator = new SimpleIDGenerator(0);

   private final ActiveMQProtonConnectionCallback protonSPI;

   private final ProtonProtocolManager manager;

   private final AMQPConnectionContext connection;

   private final Connection transportConnection;


   private ServerSession serverSession;

   private AMQPSessionContext protonSession;

   private final Executor closeExecutor;

   public ProtonSessionIntegrationCallback(ActiveMQProtonConnectionCallback protonSPI,
                                           ProtonProtocolManager manager,
                                           AMQPConnectionContext connection,
                                           Connection transportConnection,
                                           Executor executor) {
      this.protonSPI = protonSPI;
      this.manager = manager;
      this.connection = connection;
      this.transportConnection = transportConnection;
      this.closeExecutor = executor;
   }

   @Override
   public boolean isWritable(ReadyListener callback) {
      return transportConnection.isWritable(callback);
   }

   @Override
   public void onFlowConsumer(Object consumer, int credits) {
      // We have our own flow control on AMQP, so we set activemq's flow control to 0
      ((ServerConsumer) consumer).receiveCredits(-1);
   }

   @Override
   public void browserFinished(ServerConsumer consumer) {

   }

   @Override
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
                                                        (String) null, this, null, true);
   }

   @Override
   public void afterDelivery() throws Exception {

   }

   @Override
   public void start() {

   }

   @Override
   public Object createSender(ProtonPlugSender protonSender,
                              String queue,
                              String filer,
                              boolean browserOnly) throws Exception {
      long consumerID = consumerIDGenerator.generateID();

      ServerConsumer consumer = serverSession.createConsumer(consumerID, SimpleString.toSimpleString(queue), SimpleString.toSimpleString(filer), browserOnly);

      // AMQP handles its own flow control for when it's started
      consumer.setStarted(true);

      consumer.setProtocolContext(protonSender);

      return consumer;
   }

   @Override
   public void startSender(Object brokerConsumer) throws Exception {
      ServerConsumer serverConsumer = (ServerConsumer) brokerConsumer;
      // flow control is done at proton
      serverConsumer.receiveCredits(-1);
   }

   @Override
   public void createTemporaryQueue(String queueName) throws Exception {
      serverSession.createQueue(SimpleString.toSimpleString(queueName), SimpleString.toSimpleString(queueName), null, true, false);
   }

   @Override
   public boolean queueQuery(String queueName) throws Exception {
      boolean queryResult = false;

      QueueQueryResult queueQuery = serverSession.executeQueueQuery(SimpleString.toSimpleString(queueName));

      if (queueQuery.isExists()) {
         queryResult = true;
      }
      else {
         if (queueQuery.isAutoCreateJmsQueues()) {
            serverSession.createQueue(new SimpleString(queueName), new SimpleString(queueName), null, false, true);
            queryResult = true;
         }
         else {
            queryResult = false;
         }
      }

      return queryResult;
   }

   @Override
   public void closeSender(final Object brokerConsumer) throws Exception {
      Runnable runnable = new Runnable() {
         @Override
         public void run() {
            try {
               ((ServerConsumer) brokerConsumer).close(false);
            }
            catch (Exception e) {
            }
         }
      };

      // Due to the nature of proton this could be happening within flushes from the queue-delivery (depending on how it happened on the protocol)
      // to avoid deadlocks the close has to be done outside of the main thread on an executor
      // otherwise you could get a deadlock
      Executor executor = protonSPI.getExeuctor();

      if (executor != null) {
         executor.execute(runnable);
      }
      else {
         runnable.run();
      }
   }

   @Override
   public ProtonJMessage encodeMessage(Object message, int deliveryCount) throws Exception {
      return (ProtonJMessage) manager.getConverter().outbound((ServerMessage) message, deliveryCount);
   }

   @Override
   public Binary getCurrentTXID() {
      return new Binary(ByteUtil.longToBytes(serverSession.getCurrentTransaction().getID()));
   }

   @Override
   public String tempQueueName() {
      return UUIDGenerator.getInstance().generateStringUUID();
   }

   @Override
   public void commitCurrentTX() throws Exception {
      recoverContext();
      try {
         serverSession.commit();
      }
      finally {
         resetContext();
      }
   }

   @Override
   public void rollbackCurrentTX() throws Exception {
      recoverContext();
      try {
         serverSession.rollback(false);
      }
      finally {
         resetContext();
      }
   }

   @Override
   public void close() throws Exception {
      recoverContext();
      try {
         serverSession.close(false);
      }
      finally {
         resetContext();
      }
   }

   @Override
   public void ack(Object brokerConsumer, Object message) throws Exception {
      recoverContext();
      try {
         ((ServerConsumer) brokerConsumer).individualAcknowledge(null, ((ServerMessage) message).getMessageID());
      }
      finally {
         resetContext();
      }
   }

   @Override
   public void cancel(Object brokerConsumer, Object message, boolean updateCounts) throws Exception {
      recoverContext();
      try {
         ((ServerConsumer) brokerConsumer).individualCancel(((ServerMessage) message).getMessageID(), updateCounts);
      }
      finally {
         resetContext();
      }
   }

   @Override
   public void resumeDelivery(Object consumer) {
      ((ServerConsumer) consumer).receiveCredits(-1);
   }

   @Override
   public void serverSend(final Receiver receiver,
                          final Delivery delivery,
                          String address,
                          int messageFormat,
                          ByteBuf messageEncoded) throws Exception {
      EncodedMessage encodedMessage = new EncodedMessage(messageFormat, messageEncoded.array(), messageEncoded.arrayOffset(), messageEncoded.writerIndex());

      ServerMessage message = manager.getConverter().inbound(encodedMessage);
      //use the address on the receiver if not null, if null let's hope it was set correctly on the message
      if (address != null) {
         message.setAddress(new SimpleString(address));
      }

      recoverContext();

      try {
         serverSession.send(message, false);

         manager.getServer().getStorageManager().afterCompleteOperations(new IOCallback() {
            @Override
            public void done() {
               synchronized (connection.getLock()) {
                  delivery.settle();
                  connection.flush();
               }
            }

            @Override
            public void onError(int errorCode, String errorMessage) {
               synchronized (connection.getLock()) {
                  receiver.setCondition(new ErrorCondition(AmqpError.ILLEGAL_STATE, errorCode + ":" + errorMessage));
                  connection.flush();
               }
            }
         });
      }
      finally {
         resetContext();
      }
   }

   private void resetContext() {
      manager.getServer().getStorageManager().setContext(null);
   }

   private void recoverContext() {
      manager.getServer().getStorageManager().setContext(serverSession.getSessionContext());
   }

   @Override
   public void sendProducerCreditsMessage(int credits, SimpleString address) {
   }

   @Override
   public void sendProducerCreditsFailMessage(int credits, SimpleString address) {
   }

   @Override
   public int sendMessage(ServerMessage message, ServerConsumer consumer, int deliveryCount) {

      ProtonPlugSender plugSender = (ProtonPlugSender) consumer.getProtocolContext();

      try {
         return plugSender.deliverMessage(message, deliveryCount);
      }
      catch (Exception e) {
         synchronized (connection.getLock()) {
            plugSender.getSender().setCondition(new ErrorCondition(AmqpError.INTERNAL_ERROR, e.getMessage()));
            connection.flush();
         }
         throw new IllegalStateException("Can't deliver message " + e, e);
      }

   }

   @Override
   public int sendLargeMessage(ServerMessage message, ServerConsumer consumer, long bodySize, int deliveryCount) {
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
   public void disconnect(ServerConsumer consumer, String queueName) {
      synchronized (connection.getLock()) {
         ((Link) consumer.getProtocolContext()).close();
         connection.flush();
      }
   }

   @Override
   public boolean hasCredits(ServerConsumer consumer) {
      ProtonPlugSender plugSender = (ProtonPlugSender) consumer.getProtocolContext();

      if (plugSender != null && plugSender.getSender().getCredit() > 0) {
         return true;
      }
      else {
         return false;
      }
   }

}
