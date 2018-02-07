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
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.activemq.artemis.Closeable;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.persistence.OperationContext;
import org.apache.activemq.artemis.core.postoffice.RoutingStatus;
import org.apache.activemq.artemis.core.security.SecurityAuth;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
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

   void individualCancel(long consumerID, long messageID, boolean failed) throws Exception;

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

   List<Xid> xaGetInDoubtXids();

   int xaGetTimeout();

   void xaSetTimeout(int timeout);

   void start();

   void stop();

   void addCloseable(Closeable closeable);

   /**
    * To be used by protocol heads that needs to control the transaction outside the session context.
    */
   void resetTX(Transaction transaction);

   Queue createQueue(SimpleString address,
                     SimpleString name,
                     RoutingType routingType,
                     SimpleString filterString,
                     boolean temporary,
                     boolean durable) throws Exception;

   Queue createQueue(AddressInfo address,
                     SimpleString name,
                     SimpleString filterString,
                     boolean temporary,
                     boolean durable) throws Exception;

   /**
    * Create queue with default delivery mode
    *
    * @param address
    * @param name
    * @param filterString
    * @param temporary
    * @param durable
    * @return
    * @throws Exception
    */
   Queue createQueue(SimpleString address,
                     SimpleString name,
                     SimpleString filterString,
                     boolean temporary,
                     boolean durable) throws Exception;

   Queue createQueue(SimpleString address,
                     SimpleString name,
                     RoutingType routingType,
                     SimpleString filterString,
                     boolean temporary,
                     boolean durable,
                     int maxConsumers,
                     boolean purgeOnNoConsumers,
                     boolean autoCreated) throws Exception;

   Queue createQueue(SimpleString address,
                     SimpleString name,
                     RoutingType routingType,
                     SimpleString filterString,
                     boolean temporary,
                     boolean durable,
                     int maxConsumers,
                     boolean purgeOnNoConsumers,
                     Boolean exclusive,
                     Boolean lastValue,
                     boolean autoCreated) throws Exception;

   Queue createQueue(SimpleString address,
                     SimpleString name,
                     RoutingType routingType,
                     SimpleString filterString,
                     boolean temporary,
                     boolean durable,
                     boolean autoCreated) throws Exception;

   Queue createQueue(AddressInfo addressInfo,
                     SimpleString name,
                     SimpleString filterString,
                     boolean temporary,
                     boolean durable,
                     boolean autoCreated) throws Exception;

   Queue createQueue(AddressInfo addressInfo,
                     SimpleString name,
                     SimpleString filterString,
                     boolean temporary,
                     boolean durable,
                     Boolean exclusive,
                     Boolean lastValue,
                     boolean autoCreated) throws Exception;

   AddressInfo createAddress(SimpleString address,
                             EnumSet<RoutingType> routingTypes,
                             boolean autoCreated) throws Exception;

   AddressInfo createAddress(SimpleString address,
                             RoutingType routingType,
                             boolean autoCreated) throws Exception;

   AddressInfo createAddress(AddressInfo addressInfo,
                             boolean autoCreated) throws Exception;

   void deleteQueue(SimpleString name) throws Exception;

   ServerConsumer createConsumer(long consumerID,
                                 SimpleString queueName,
                                 SimpleString filterString,
                                 boolean browseOnly) throws Exception;

   ServerConsumer createConsumer(long consumerID,
                                 SimpleString queueName,
                                 SimpleString filterString,
                                 boolean browseOnly,
                                 boolean supportLargeMessage,
                                 Integer credits) throws Exception;

   QueueQueryResult executeQueueQuery(SimpleString name) throws Exception;

   AddressQueryResult executeAddressQuery(SimpleString name) throws Exception;

   BindingQueryResult executeBindingQuery(SimpleString address) throws Exception;

   void closeConsumer(long consumerID) throws Exception;

   void receiveConsumerCredits(long consumerID, int credits) throws Exception;

   RoutingStatus send(Transaction tx,
                      Message message,
                      boolean direct,
                      boolean noAutoCreateQueue) throws Exception;

   RoutingStatus doSend(Transaction tx,
                        Message msg,
                        SimpleString originalAddress,
                        boolean direct,
                        boolean noAutoCreateQueue) throws Exception;

   RoutingStatus send(Message message, boolean direct, boolean noAutoCreateQueue) throws Exception;

   RoutingStatus send(Message message, boolean direct) throws Exception;

   void forceConsumerDelivery(long consumerID, long sequence) throws Exception;

   void requestProducerCredits(SimpleString address, int credits) throws Exception;

   void close(boolean failed) throws Exception;

   void setTransferring(boolean transferring);

   Set<ServerConsumer> getServerConsumers();

   void addMetaData(String key, String data) throws Exception;

   boolean addUniqueMetaData(String key, String data) throws Exception;

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
                     RoutingType routingType,
                     SimpleString filterString,
                     boolean durable,
                     Integer maxConsumers,
                     Boolean purgeOnNoConsumers,
                     Boolean exclusive,
                     Boolean lastValue) throws Exception;

   void createSharedQueue(SimpleString address,
                          SimpleString name,
                          RoutingType routingType,
                          boolean durable,
                          SimpleString filterString) throws Exception;

   void createSharedQueue(SimpleString address,
                          SimpleString name,
                          boolean durable,
                          SimpleString filterString) throws Exception;

   List<MessageReference> getInTXMessagesForConsumer(long consumerId);

   String getValidatedUser();

   SimpleString getMatchingQueue(SimpleString address, RoutingType routingType) throws Exception;

   SimpleString getMatchingQueue(SimpleString address,
                                 SimpleString queueName,
                                 RoutingType routingType) throws Exception;

   AddressInfo getAddress(SimpleString address);

   /**
    * Strip the prefix (if it exists) from the address based on the prefixes provided to the ServerSession constructor.
    *
    * @param address the address to inspect
    * @return the canonical (i.e. non-prefixed) address name
    */
   SimpleString removePrefix(SimpleString address);

   /**
    * Get the canonical (i.e. non-prefixed) address and the corresponding routing-type.
    *
    * @param addressInfo the address to inspect
    * @return a {@code org.apache.activemq.artemis.api.core.Pair} representing the canonical (i.e. non-prefixed) address
    *         name and the {@code org.apache.activemq.artemis.api.core.RoutingType} corresponding to the that prefix.
    */
   AddressInfo getAddressAndRoutingType(AddressInfo addressInfo);

   /**
    * Get the canonical (i.e. non-prefixed) address and the corresponding routing-type.
    *
    * @param address the address to inspect
    * @param defaultRoutingTypes a the {@code java.util.Set} of {@code org.apache.activemq.artemis.api.core.RoutingType}
    *                            objects to return if no prefix match is found.
    * @return a {@code org.apache.activemq.artemis.api.core.Pair} representing the canonical (i.e. non-prefixed) address
    *         name and the {@code java.util.Set} of {@code org.apache.activemq.artemis.api.core.RoutingType} objects
    *         corresponding to the that prefix.
    */
   Pair<SimpleString, EnumSet<RoutingType>> getAddressAndRoutingTypes(SimpleString address,
                                                                      EnumSet<RoutingType> defaultRoutingTypes);

   void addProducer(ServerProducer serverProducer);

   void removeProducer(String ID);

   Map<String, ServerProducer> getServerProducers();

   String getDefaultAddress();

   int getConsumerCount();

   int getProducerCount();
}