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
package org.apache.activemq.artemis.core.server;

import javax.json.JsonArrayBuilder;
import javax.transaction.xa.Xid;
import java.util.List;
import java.util.Set;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.message.impl.MessageInternal;
import org.apache.activemq.artemis.core.persistence.OperationContext;
import org.apache.activemq.artemis.core.postoffice.RoutingStatus;
import org.apache.activemq.artemis.core.security.SecurityAuth;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;

public interface ServerSession extends SecurityAuth {

   String getName();

   int getMinLargeMessageSize();

   Object getConnectionID();

   /**
    * Certain protocols may create an internal session that shouldn't go through security checks.
    * make sure you don't expose this property through any protocol layer as that would be a security breach
    */
   void enableSecurity();

   void disableSecurity();

   @Override
   RemotingConnection getRemotingConnection();

   Transaction newTransaction();

   boolean removeConsumer(long consumerID) throws Exception;

   void acknowledge(long consumerID, long messageID) throws Exception;

   void individualAcknowledge(long consumerID, long messageID) throws Exception;

   void individualCancel(final long consumerID, final long messageID, boolean failed) throws Exception;

   void expire(long consumerID, long messageID) throws Exception;

   void rollback(boolean considerLastMessageAsDelivered) throws Exception;

   void commit() throws Exception;

   void xaCommit(Xid xid, boolean onePhase) throws Exception;

   void xaEnd(Xid xid) throws Exception;

   void xaForget(Xid xid) throws Exception;

   void xaJoin(Xid xid) throws Exception;

   void xaPrepare(Xid xid) throws Exception;

   void xaResume(Xid xid) throws Exception;

   void xaRollback(Xid xid) throws Exception;

   void xaStart(Xid xid) throws Exception;

   void xaFailed(Xid xid) throws Exception;

   void xaSuspend() throws Exception;

   void markTXFailed(Throwable e);

   QueueCreator getQueueCreator();

   List<Xid> xaGetInDoubtXids();

   int xaGetTimeout();

   void xaSetTimeout(int timeout);

   void start();

   void stop();

   /**
    * To be used by protocol heads that needs to control the transaction outside the session context.
    */
   void resetTX(Transaction transaction);

   Queue createQueue(SimpleString address,
                     SimpleString name,
                     SimpleString filterString,
                     boolean temporary,
                     boolean durable) throws Exception;

   void deleteQueue(SimpleString name) throws Exception;

   ServerConsumer createConsumer(long consumerID,
                                 SimpleString queueName,
                                 SimpleString filterString,
                                 boolean browseOnly) throws Exception;

   ServerConsumer createConsumer(final long consumerID,
                                 final SimpleString queueName,
                                 final SimpleString filterString,
                                 final boolean browseOnly,
                                 final boolean supportLargeMessage,
                                 final Integer credits) throws Exception;

   QueueQueryResult executeQueueQuery(SimpleString name) throws Exception;

   BindingQueryResult executeBindingQuery(SimpleString address) throws Exception;

   void closeConsumer(long consumerID) throws Exception;

   void receiveConsumerCredits(long consumerID, int credits) throws Exception;

   void sendContinuations(int packetSize, long totalBodySize, byte[] body, boolean continues) throws Exception;

   RoutingStatus send(ServerMessage message, boolean direct, boolean noAutoCreateQueue) throws Exception;

   RoutingStatus send(ServerMessage message, boolean direct) throws Exception;

   void sendLarge(MessageInternal msg) throws Exception;

   void forceConsumerDelivery(long consumerID, long sequence) throws Exception;

   void requestProducerCredits(SimpleString address, int credits) throws Exception;

   void close(boolean failed) throws Exception;

   void waitContextCompletion() throws Exception;

   void setTransferring(boolean transferring);

   Set<ServerConsumer> getServerConsumers();

   void addMetaData(String key, String data);

   boolean addUniqueMetaData(String key, String data);

   String getMetaData(String key);

   String[] getTargetAddresses();

   /**
    * Add all the producers detail to the JSONArray object.
    * This is a method to be used by the management layer.
    *
    * @param objs
    * @throws Exception
    */
   void describeProducersInfo(JsonArrayBuilder objs) throws Exception;

   String getLastSentMessageID(String address);

   long getCreationTime();

   OperationContext getSessionContext();

   Transaction getCurrentTransaction();

   ServerConsumer locateConsumer(long consumerID) throws Exception;

   boolean isClosed();

   void createSharedQueue(SimpleString address,
                          SimpleString name,
                          boolean durable,
                          SimpleString filterString) throws Exception;

   List<MessageReference> getInTXMessagesForConsumer(long consumerId);
}
