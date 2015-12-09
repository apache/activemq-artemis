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
package org.apache.activemq.artemis.core.client.impl;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.Set;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.FailoverEventListener;
import org.apache.activemq.artemis.api.core.client.SendAcknowledgementHandler;
import org.apache.activemq.artemis.api.core.client.SessionFailureListener;
import org.apache.activemq.artemis.core.client.ActiveMQClientLogger;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.remoting.ConsumerContext;
import org.apache.activemq.artemis.spi.core.remoting.ReadyListener;
import org.apache.activemq.artemis.utils.ConcurrentHashSet;

/**
 * A DelegatingSession
 * <p>
 * We wrap the real session, so we can add a finalizer on this and close the session
 * on GC if it has not already been closed
 */
public class DelegatingSession implements ClientSessionInternal {

   private final ClientSessionInternal session;

   private final Exception creationStack;

   private volatile boolean closed;

   private static Set<DelegatingSession> sessions = new ConcurrentHashSet<>();

   public static volatile boolean debug;

   public static void dumpSessionCreationStacks() {
      ActiveMQClientLogger.LOGGER.dumpingSessionStacks();

      for (DelegatingSession session : DelegatingSession.sessions) {
         ActiveMQClientLogger.LOGGER.dumpingSessionStack(session.creationStack);
      }
   }

   public ClientSessionInternal getInternalSession() {
      return session;
   }

   @Override
   protected void finalize() throws Throwable {
      // In some scenarios we have seen the JDK finalizing the DelegatingSession while the call to session.close() was still in progress
      //
      if (!closed && !session.isClosed()) {
         ActiveMQClientLogger.LOGGER.clientSessionNotClosed(creationStack, System.identityHashCode(this));

         close();
      }

      super.finalize();
   }

   public DelegatingSession(final ClientSessionInternal session) {
      this.session = session;

      creationStack = new Exception();

      if (DelegatingSession.debug) {
         DelegatingSession.sessions.add(this);
      }
   }

   @Override
   public boolean isClosing() {
      return session.isClosing();
   }

   @Override
   public void acknowledge(final ClientConsumer consumer, final Message message) throws ActiveMQException {
      session.acknowledge(consumer, message);
   }

   @Override
   public boolean isWritable(ReadyListener callback) {
      return session.isWritable(callback);
   }

   @Override
   public void individualAcknowledge(final ClientConsumer consumer, final Message message) throws ActiveMQException {
      session.individualAcknowledge(consumer, message);
   }

   @Override
   public void addConsumer(final ClientConsumerInternal consumer) {
      session.addConsumer(consumer);
   }

   @Override
   public void addFailureListener(final SessionFailureListener listener) {
      session.addFailureListener(listener);
   }

   @Override
   public void addFailoverListener(FailoverEventListener listener) {
      session.addFailoverListener(listener);
   }

   @Override
   public void addProducer(final ClientProducerInternal producer) {
      session.addProducer(producer);
   }

   @Override
   public AddressQuery addressQuery(final SimpleString address) throws ActiveMQException {
      return session.addressQuery(address);
   }

   @Override
   public void cleanUp(boolean failingOver) throws ActiveMQException {
      session.cleanUp(failingOver);
   }

   @Override
   public void close() throws ActiveMQException {
      closed = true;

      if (DelegatingSession.debug) {
         DelegatingSession.sessions.remove(this);
      }

      session.close();
   }

   @Override
   public void markRollbackOnly() {
      session.markRollbackOnly();
   }

   @Override
   public void commit() throws ActiveMQException {
      session.commit();
   }

   @Override
   public void commit(final Xid xid, final boolean onePhase) throws XAException {
      session.commit(xid, onePhase);
   }

   @Override
   public ClientMessage createMessage(final boolean durable) {
      return session.createMessage(durable);
   }

   @Override
   public ClientMessage createMessage(final byte type,
                                      final boolean durable,
                                      final long expiration,
                                      final long timestamp,
                                      final byte priority) {
      return session.createMessage(type, durable, expiration, timestamp, priority);
   }

   @Override
   public ClientMessage createMessage(final byte type, final boolean durable) {
      return session.createMessage(type, durable);
   }

   @Override
   public ClientConsumer createConsumer(final SimpleString queueName,
                                        final SimpleString filterString,
                                        final boolean browseOnly) throws ActiveMQException {
      return session.createConsumer(queueName, filterString, browseOnly);
   }

   @Override
   public ClientConsumer createConsumer(final SimpleString queueName,
                                        final SimpleString filterString,
                                        final int windowSize,
                                        final int maxRate,
                                        final boolean browseOnly) throws ActiveMQException {
      return session.createConsumer(queueName, filterString, windowSize, maxRate, browseOnly);
   }

   @Override
   public ClientConsumer createConsumer(final SimpleString queueName,
                                        final SimpleString filterString) throws ActiveMQException {
      return session.createConsumer(queueName, filterString);
   }

   @Override
   public ClientConsumer createConsumer(final SimpleString queueName) throws ActiveMQException {
      return session.createConsumer(queueName);
   }

   @Override
   public ClientConsumer createConsumer(final String queueName,
                                        final String filterString,
                                        final boolean browseOnly) throws ActiveMQException {
      return session.createConsumer(queueName, filterString, browseOnly);
   }

   @Override
   public ClientConsumer createConsumer(final String queueName,
                                        final String filterString,
                                        final int windowSize,
                                        final int maxRate,
                                        final boolean browseOnly) throws ActiveMQException {
      return session.createConsumer(queueName, filterString, windowSize, maxRate, browseOnly);
   }

   @Override
   public ClientConsumer createConsumer(final String queueName, final String filterString) throws ActiveMQException {
      return session.createConsumer(queueName, filterString);
   }

   @Override
   public ClientConsumer createConsumer(final String queueName) throws ActiveMQException {
      return session.createConsumer(queueName);
   }

   @Override
   public ClientConsumer createConsumer(final SimpleString queueName,
                                        final boolean browseOnly) throws ActiveMQException {
      return session.createConsumer(queueName, browseOnly);
   }

   @Override
   public ClientConsumer createConsumer(final String queueName, final boolean browseOnly) throws ActiveMQException {
      return session.createConsumer(queueName, browseOnly);
   }

   @Override
   public ClientProducer createProducer() throws ActiveMQException {
      return session.createProducer();
   }

   @Override
   public ClientProducer createProducer(final SimpleString address, final int rate) throws ActiveMQException {
      return session.createProducer(address, rate);
   }

   @Override
   public ClientProducer createProducer(final SimpleString address) throws ActiveMQException {
      return session.createProducer(address);
   }

   @Override
   public ClientProducer createProducer(final String address) throws ActiveMQException {
      return session.createProducer(address);
   }

   @Override
   public void createQueue(final String address, final String queueName) throws ActiveMQException {
      session.createQueue(address, queueName);
   }

   @Override
   public void createQueue(final SimpleString address, final SimpleString queueName) throws ActiveMQException {
      session.createQueue(address, queueName);
   }

   @Override
   public void createQueue(final SimpleString address,
                           final SimpleString queueName,
                           final boolean durable) throws ActiveMQException {
      session.createQueue(address, queueName, durable);
   }

   @Override
   public void createSharedQueue(SimpleString address,
                                 SimpleString queueName,
                                 boolean durable) throws ActiveMQException {
      session.createSharedQueue(address, queueName, durable);
   }

   @Override
   public void createSharedQueue(SimpleString address,
                                 SimpleString queueName,
                                 SimpleString filter,
                                 boolean durable) throws ActiveMQException {
      session.createSharedQueue(address, queueName, filter, durable);
   }

   @Override
   public void createQueue(final SimpleString address,
                           final SimpleString queueName,
                           final SimpleString filterString,
                           final boolean durable) throws ActiveMQException {
      session.createQueue(address, queueName, filterString, durable);
   }

   @Override
   public void createQueue(final String address,
                           final String queueName,
                           final boolean durable) throws ActiveMQException {
      session.createQueue(address, queueName, durable);
   }

   @Override
   public void createQueue(final String address,
                           final String queueName,
                           final String filterString,
                           final boolean durable) throws ActiveMQException {
      session.createQueue(address, queueName, filterString, durable);
   }

   @Override
   public void createTemporaryQueue(final SimpleString address,
                                    final SimpleString queueName,
                                    final SimpleString filter) throws ActiveMQException {
      session.createTemporaryQueue(address, queueName, filter);
   }

   @Override
   public void createTemporaryQueue(final SimpleString address, final SimpleString queueName) throws ActiveMQException {
      session.createTemporaryQueue(address, queueName);
   }

   @Override
   public void createTemporaryQueue(final String address,
                                    final String queueName,
                                    final String filter) throws ActiveMQException {
      session.createTemporaryQueue(address, queueName, filter);
   }

   @Override
   public void createTemporaryQueue(final String address, final String queueName) throws ActiveMQException {
      session.createTemporaryQueue(address, queueName);
   }

   @Override
   public void deleteQueue(final SimpleString queueName) throws ActiveMQException {
      session.deleteQueue(queueName);
   }

   @Override
   public void deleteQueue(final String queueName) throws ActiveMQException {
      session.deleteQueue(queueName);
   }

   @Override
   public void end(final Xid xid, final int flags) throws XAException {
      session.end(xid, flags);
   }

   @Override
   public void expire(final ClientConsumer consumer, final Message message) throws ActiveMQException {
      session.expire(consumer, message);
   }

   @Override
   public void forget(final Xid xid) throws XAException {
      session.forget(xid);
   }

   @Override
   public RemotingConnection getConnection() {
      return session.getConnection();
   }

   @Override
   public int getMinLargeMessageSize() {
      return session.getMinLargeMessageSize();
   }

   @Override
   public String getName() {
      return session.getName();
   }

   @Override
   public int getTransactionTimeout() throws XAException {
      return session.getTransactionTimeout();
   }

   @Override
   public int getVersion() {
      return session.getVersion();
   }

   @Override
   public XAResource getXAResource() {
      return session.getXAResource();
   }

   @Override
   public void preHandleFailover(RemotingConnection connection) {
      session.preHandleFailover(connection);
   }

   @Override
   public void handleFailover(final RemotingConnection backupConnection, ActiveMQException cause) {
      session.handleFailover(backupConnection, cause);
   }

   @Override
   public void handleReceiveMessage(ConsumerContext consumerID, ClientMessageInternal message) throws Exception {
      session.handleReceiveMessage(consumerID, message);
   }

   @Override
   public void handleReceiveLargeMessage(ConsumerContext consumerID,
                                         ClientLargeMessageInternal clientLargeMessage,
                                         long largeMessageSize) throws Exception {
      session.handleReceiveLargeMessage(consumerID, clientLargeMessage, largeMessageSize);
   }

   @Override
   public void handleReceiveContinuation(ConsumerContext consumerID,
                                         byte[] chunk,
                                         int flowControlSize,
                                         boolean isContinues) throws Exception {
      session.handleReceiveContinuation(consumerID, chunk, flowControlSize, isContinues);
   }

   @Override
   public void handleConsumerDisconnect(ConsumerContext consumerContext) throws ActiveMQException {
      session.handleConsumerDisconnect(consumerContext);
   }

   @Override
   public boolean isAutoCommitAcks() {
      return session.isAutoCommitAcks();
   }

   @Override
   public boolean isAutoCommitSends() {
      return session.isAutoCommitSends();
   }

   @Override
   public boolean isBlockOnAcknowledge() {
      return session.isBlockOnAcknowledge();
   }

   @Override
   public boolean isCacheLargeMessageClient() {
      return session.isCacheLargeMessageClient();
   }

   @Override
   public boolean isClosed() {
      return session.isClosed();
   }

   @Override
   public boolean isSameRM(final XAResource xares) throws XAException {
      return session.isSameRM(xares);
   }

   @Override
   public boolean isXA() {
      return session.isXA();
   }

   @Override
   public int prepare(final Xid xid) throws XAException {
      return session.prepare(xid);
   }

   @Override
   public QueueQuery queueQuery(final SimpleString queueName) throws ActiveMQException {
      return session.queueQuery(queueName);
   }

   @Override
   public Xid[] recover(final int flag) throws XAException {
      return session.recover(flag);
   }

   @Override
   public void removeConsumer(final ClientConsumerInternal consumer) throws ActiveMQException {
      session.removeConsumer(consumer);
   }

   @Override
   public boolean removeFailureListener(final SessionFailureListener listener) {
      return session.removeFailureListener(listener);
   }

   @Override
   public boolean removeFailoverListener(FailoverEventListener listener) {
      return session.removeFailoverListener(listener);
   }

   @Override
   public void removeProducer(final ClientProducerInternal producer) {
      session.removeProducer(producer);
   }

   @Override
   public void rollback() throws ActiveMQException {
      session.rollback();
   }

   @Override
   public boolean isRollbackOnly() {
      return session.isRollbackOnly();
   }

   @Override
   public void rollback(final boolean considerLastMessageAsDelivered) throws ActiveMQException {
      session.rollback(considerLastMessageAsDelivered);
   }

   @Override
   public void rollback(final Xid xid) throws XAException {
      session.rollback(xid);
   }

   @Override
   public DelegatingSession setSendAcknowledgementHandler(final SendAcknowledgementHandler handler) {
      session.setSendAcknowledgementHandler(handler);
      return this;
   }

   @Override
   public boolean setTransactionTimeout(final int seconds) throws XAException {
      return session.setTransactionTimeout(seconds);
   }

   @Override
   public void resetIfNeeded() throws ActiveMQException {
      session.resetIfNeeded();
   }

   @Override
   public DelegatingSession start() throws ActiveMQException {
      session.start();
      return this;
   }

   @Override
   public void start(final Xid xid, final int flags) throws XAException {
      session.start(xid, flags);
   }

   @Override
   public void stop() throws ActiveMQException {
      session.stop();
   }

   @Override
   public ClientSessionFactory getSessionFactory() {
      return session.getSessionFactory();
   }

   @Override
   public void setForceNotSameRM(final boolean force) {
      session.setForceNotSameRM(force);
   }

   @Override
   public void workDone() {
      session.workDone();
   }

   @Override
   public void sendProducerCreditsMessage(final int credits, final SimpleString address) {
      session.sendProducerCreditsMessage(credits, address);
   }

   @Override
   public ClientProducerCredits getCredits(final SimpleString address, final boolean anon) {
      return session.getCredits(address, anon);
   }

   @Override
   public void returnCredits(final SimpleString address) {
      session.returnCredits(address);
   }

   @Override
   public void handleReceiveProducerCredits(final SimpleString address, final int credits) {
      session.handleReceiveProducerCredits(address, credits);
   }

   @Override
   public void handleReceiveProducerFailCredits(final SimpleString address, final int credits) {
      session.handleReceiveProducerFailCredits(address, credits);
   }

   @Override
   public ClientProducerCreditManager getProducerCreditManager() {
      return session.getProducerCreditManager();
   }

   @Override
   public void setAddress(Message message, SimpleString address) {
      session.setAddress(message, address);
   }

   @Override
   public void setPacketSize(int packetSize) {
      session.setPacketSize(packetSize);
   }

   @Override
   public void addMetaData(String key, String data) throws ActiveMQException {
      session.addMetaData(key, data);
   }

   @Override
   public boolean isCompressLargeMessages() {
      return session.isCompressLargeMessages();
   }

   @Override
   public String toString() {
      return "DelegatingSession [session=" + session + "]";
   }

   @Override
   public void addUniqueMetaData(String key, String data) throws ActiveMQException {
      session.addUniqueMetaData(key, data);

   }

   @Override
   public void startCall() {
      session.startCall();
   }

   @Override
   public void endCall() {
      session.endCall();
   }

   @Override
   public void setStopSignal() {
      session.setStopSignal();
   }

   @Override
   public boolean isConfirmationWindowEnabled() {
      return session.isConfirmationWindowEnabled();
   }

   @Override
   public void scheduleConfirmation(SendAcknowledgementHandler handler, Message msg) {
      session.scheduleConfirmation(handler, msg);
   }

   @Override
   public String getNodeId() {
      return session.getNodeId();
   }
}
