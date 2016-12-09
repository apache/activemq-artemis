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
package org.apache.activemq.artemis.spi.core.remoting;

import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;
import java.util.HashMap;
import java.util.concurrent.Executor;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.SendAcknowledgementHandler;
import org.apache.activemq.artemis.core.client.impl.ClientConsumerInternal;
import org.apache.activemq.artemis.core.client.impl.ClientLargeMessageInternal;
import org.apache.activemq.artemis.core.client.impl.ClientMessageInternal;
import org.apache.activemq.artemis.core.client.impl.ClientProducerCreditsImpl;
import org.apache.activemq.artemis.core.client.impl.ClientSessionInternal;
import org.apache.activemq.artemis.core.message.impl.MessageInternal;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.utils.IDGenerator;
import org.apache.activemq.artemis.utils.SimpleIDGenerator;

public abstract class SessionContext {

   protected ClientSessionInternal session;

   protected SendAcknowledgementHandler sendAckHandler;

   protected volatile RemotingConnection remotingConnection;

   protected final IDGenerator idGenerator = new SimpleIDGenerator(0);

   public SessionContext(RemotingConnection remotingConnection) {
      this.remotingConnection = remotingConnection;
   }

   public ClientSessionInternal getSession() {
      return session;
   }

   public void setSession(ClientSessionInternal session) {
      this.session = session;
   }

   public abstract void resetName(String name);

   public abstract int getReconnectID();

   /**
    * it will eather reattach or reconnect, preferably reattaching it.
    *
    * @param newConnection
    * @return true if it was possible to reattach
    * @throws ActiveMQException
    */
   public abstract boolean reattachOnNewConnection(RemotingConnection newConnection) throws ActiveMQException;

   public RemotingConnection getRemotingConnection() {
      return remotingConnection;
   }

   public abstract void closeConsumer(ClientConsumer consumer) throws ActiveMQException;

   public abstract void sendConsumerCredits(ClientConsumer consumer, int credits);

   public abstract boolean supportsLargeMessage();

   protected void handleReceiveLargeMessage(ConsumerContext consumerID,
                                            ClientLargeMessageInternal clientLargeMessage,
                                            long largeMessageSize) throws Exception {
      ClientSessionInternal session = this.session;
      if (session != null) {
         session.handleReceiveLargeMessage(consumerID, clientLargeMessage, largeMessageSize);
      }
   }

   protected void handleReceiveMessage(ConsumerContext consumerID,
                                       final ClientMessageInternal message) throws Exception {

      ClientSessionInternal session = this.session;
      if (session != null) {
         session.handleReceiveMessage(consumerID, message);
      }
   }

   protected void handleReceiveContinuation(final ConsumerContext consumerID,
                                            byte[] chunk,
                                            int flowControlSize,
                                            boolean isContinues) throws Exception {
      ClientSessionInternal session = this.session;
      if (session != null) {
         session.handleReceiveContinuation(consumerID, chunk, flowControlSize, isContinues);
      }
   }

   protected void handleReceiveProducerCredits(SimpleString address, int credits) {
      ClientSessionInternal session = this.session;
      if (session != null) {
         session.handleReceiveProducerCredits(address, credits);
      }

   }

   protected void handleReceiveProducerFailCredits(SimpleString address, int credits) {
      ClientSessionInternal session = this.session;
      if (session != null) {
         session.handleReceiveProducerFailCredits(address, credits);
      }

   }

   public abstract int getCreditsOnSendingFull(MessageInternal msgI);

   public abstract void sendFullMessage(MessageInternal msgI,
                                        boolean sendBlocking,
                                        SendAcknowledgementHandler handler,
                                        SimpleString defaultAddress) throws ActiveMQException;

   /**
    * it should return the number of credits (or bytes) used to send this packet
    *
    * @param msgI
    * @return
    * @throws ActiveMQException
    */
   public abstract int sendInitialChunkOnLargeMessage(MessageInternal msgI) throws ActiveMQException;

   public abstract int sendLargeMessageChunk(MessageInternal msgI,
                                             long messageBodySize,
                                             boolean sendBlocking,
                                             boolean lastChunk,
                                             byte[] chunk,
                                             int reconnectID,
                                             SendAcknowledgementHandler messageHandler) throws ActiveMQException;

   public abstract int sendServerLargeMessageChunk(MessageInternal msgI,
                                                   long messageBodySize,
                                                   boolean sendBlocking,
                                                   boolean lastChunk,
                                                   byte[] chunk,
                                                   SendAcknowledgementHandler messageHandler) throws ActiveMQException;

   public abstract void setSendAcknowledgementHandler(final SendAcknowledgementHandler handler);

   public abstract void createSharedQueue(SimpleString address,
                                          SimpleString queueName,
                                          SimpleString filterString,
                                          boolean durable) throws ActiveMQException;

   public abstract void deleteQueue(SimpleString queueName) throws ActiveMQException;

   public abstract void createAddress(SimpleString address, boolean multicast, boolean autoCreated) throws ActiveMQException;

   public abstract void createQueue(SimpleString address,
                                    SimpleString queueName,
                                    SimpleString filterString,
                                    boolean durable,
                                    boolean temp,
                                    boolean autoCreated) throws ActiveMQException;

   public abstract ClientSession.QueueQuery queueQuery(SimpleString queueName) throws ActiveMQException;

   public abstract void forceDelivery(ClientConsumer consumer, long sequence) throws ActiveMQException;

   public abstract ClientSession.AddressQuery addressQuery(final SimpleString address) throws ActiveMQException;

   public abstract void simpleCommit() throws ActiveMQException;

   /**
    * If we are doing a simple rollback on the RA, we need to ack the last message sent to the consumer,
    * otherwise DLQ won't work.
    * <p>
    * this is because we only ACK after on the RA, We may review this if we always acked earlier.
    *
    * @param lastMessageAsDelivered
    * @throws ActiveMQException
    */
   public abstract void simpleRollback(boolean lastMessageAsDelivered) throws ActiveMQException;

   public abstract void sessionStart() throws ActiveMQException;

   public abstract void sessionStop() throws ActiveMQException;

   public abstract void sendACK(boolean individual,
                                boolean block,
                                final ClientConsumer consumer,
                                final Message message) throws ActiveMQException;

   public abstract void expireMessage(final ClientConsumer consumer, Message message) throws ActiveMQException;

   public abstract void sessionClose() throws ActiveMQException;

   public abstract void addSessionMetadata(String key, String data) throws ActiveMQException;

   public abstract void addUniqueMetaData(String key, String data) throws ActiveMQException;

   public abstract void sendProducerCreditsMessage(final int credits, final SimpleString address);

   public abstract void xaCommit(Xid xid, boolean onePhase) throws XAException, ActiveMQException;

   public abstract void xaEnd(Xid xid, int flags) throws XAException, ActiveMQException;

   public abstract void xaForget(Xid xid) throws XAException, ActiveMQException;

   public abstract int xaPrepare(Xid xid) throws XAException, ActiveMQException;

   public abstract Xid[] xaScan() throws ActiveMQException;

   public abstract void xaRollback(Xid xid, boolean wasStarted) throws ActiveMQException, XAException;

   public abstract void xaStart(Xid xid, int flags) throws XAException, ActiveMQException;

   public abstract boolean configureTransactionTimeout(int seconds) throws ActiveMQException;

   public abstract ClientConsumerInternal createConsumer(SimpleString queueName,
                                                         SimpleString filterString,
                                                         int windowSize,
                                                         int maxRate,
                                                         int ackBatchSize,
                                                         boolean browseOnly,
                                                         Executor executor,
                                                         Executor flowControlExecutor) throws ActiveMQException;

   /**
    * Performs a round trip to the server requesting what is the current tx timeout on the session
    *
    * @return
    */
   public abstract int recoverSessionTimeout() throws ActiveMQException;

   public abstract int getServerVersion();

   public abstract void recreateSession(final String username,
                                        final String password,
                                        final int minLargeMessageSize,
                                        final boolean xa,
                                        final boolean autoCommitSends,
                                        final boolean autoCommitAcks,
                                        final boolean preAcknowledge) throws ActiveMQException;

   public abstract void recreateConsumerOnServer(ClientConsumerInternal consumerInternal) throws ActiveMQException;

   public abstract void xaFailed(Xid xid) throws ActiveMQException;

   public abstract void restartSession() throws ActiveMQException;

   public abstract void resetMetadata(HashMap<String, String> metaDataToSend);

   // Failover utility classes

   /**
    * Interrupt and return any blocked calls
    */
   public abstract void returnBlocking(ActiveMQException cause);

   /**
    * it will lock the communication channel of the session avoiding anything to come while failover is happening.
    * It happens on preFailover from ClientSessionImpl
    */
   public abstract void lockCommunications();

   public abstract void releaseCommunications();

   public abstract void cleanup();

   public abstract void linkFlowControl(SimpleString address, ClientProducerCreditsImpl clientProducerCredits);

   public abstract boolean isWritable(ReadyListener callback);
}
