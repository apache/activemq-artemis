/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.core.server.plugin;

import java.util.Map;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.BridgeConfiguration;
import org.apache.activemq.artemis.core.persistence.OperationContext;
import org.apache.activemq.artemis.core.postoffice.RoutingStatus;
import org.apache.activemq.artemis.core.security.SecurityAuth;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.QueueConfig;
import org.apache.activemq.artemis.core.server.RoutingContext;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.core.server.cluster.Bridge;
import org.apache.activemq.artemis.core.server.impl.AckReason;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.protocol.SessionCallback;
import org.apache.activemq.artemis.utils.critical.CriticalComponent;

public interface ActiveMQServerPlugin {


   /**
    * A connection has been created.
    *
    * @param connection The newly created connection
    */
   default void afterCreateConnection(RemotingConnection connection) throws ActiveMQException {

   }

   /**
    * A connection has been destroyed.
    *
    * @param connection
    */
   default void afterDestroyConnection(RemotingConnection connection) throws ActiveMQException {

   }

   /**
    * Before a session is created.
    *
    * @param name
    * @param username
    * @param minLargeMessageSize
    * @param connection
    * @param autoCommitSends
    * @param autoCommitAcks
    * @param preAcknowledge
    * @param xa
    * @param defaultAddress
    * @param callback
    * @param autoCreateQueues
    * @param context
    * @param prefixes
    */
   default void beforeCreateSession(String name, String username, int minLargeMessageSize,
         RemotingConnection connection, boolean autoCommitSends, boolean autoCommitAcks, boolean preAcknowledge,
         boolean xa, String defaultAddress, SessionCallback callback, boolean autoCreateQueues, OperationContext context,
         Map<SimpleString, RoutingType> prefixes) throws ActiveMQException {

   }

   /**
    * After a session has been created.
    *
    * @param session The newly created session
    */
   default void afterCreateSession(ServerSession session) throws ActiveMQException {

   }

   /**
    * Before a session is closed
    *
    * @param session
    * @param failed
    */
   default void beforeCloseSession(ServerSession session, boolean failed) throws ActiveMQException {

   }

   /**
    * After a session is closed
    *
    * @param session
    * @param failed
    */
   default void afterCloseSession(ServerSession session, boolean failed) throws ActiveMQException {

   }

   /**
    * Before session metadata is added to the session
    *
    * @param session
    * @param key
    * @param data
    */
   default void beforeSessionMetadataAdded(ServerSession session, String key, String data) throws ActiveMQException {

   }

   /**
    * After session metadata is added to the session
    *
    * @param session
    * @param key
    * @param data
    */
   default void afterSessionMetadataAdded(ServerSession session, String key, String data) throws ActiveMQException {

   }

   /**
    * Before a consumer is created
    *
    * @param consumerID
    * @param queueName
    * @param filterString
    * @param browseOnly
    * @param supportLargeMessage
    */
   default void beforeCreateConsumer(long consumerID, SimpleString queueName, SimpleString filterString,
         boolean browseOnly, boolean supportLargeMessage) throws ActiveMQException {

   }

   /**
    * After a consumer has been created
    *
    * @param consumer the created consumer
    */
   default void afterCreateConsumer(ServerConsumer consumer) throws ActiveMQException {

   }

   /**
    * Before a consumer is closed
    *
    * @param consumer
    * @param failed
    */
   default void beforeCloseConsumer(ServerConsumer consumer, boolean failed) throws ActiveMQException {

   }

   /**
    * After a consumer is closed
    *
    * @param consumer
    * @param failed
    */
   default void afterCloseConsumer(ServerConsumer consumer, boolean failed) throws ActiveMQException {

   }

   /**
    * Before a queue is created
    *
    * @param queueConfig
    */
   default void beforeCreateQueue(QueueConfig queueConfig) throws ActiveMQException {

   }

   /**
    * After a queue has been created
    *
    * @param queue The newly created queue
    */
   default void afterCreateQueue(Queue queue) throws ActiveMQException {

   }

   /**
    * Before a queue is destroyed
    *
    * @param queueName
    * @param session
    * @param checkConsumerCount
    * @param removeConsumers
    * @param autoDeleteAddress
    */
   default void beforeDestroyQueue(SimpleString queueName, final SecurityAuth session, boolean checkConsumerCount,
         boolean removeConsumers, boolean autoDeleteAddress) throws ActiveMQException {

   }

   /**
    * After a queue has been destroyed
    *
    * @param queue
    * @param address
    * @param session
    * @param checkConsumerCount
    * @param removeConsumers
    * @param autoDeleteAddress
    */
   default void afterDestroyQueue(Queue queue, SimpleString address, final SecurityAuth session, boolean checkConsumerCount,
         boolean removeConsumers, boolean autoDeleteAddress) throws ActiveMQException {

   }

   /**
    * Before a message is sent
    *
    * @param session the session that sends the message
    * @param tx
    * @param message
    * @param direct
    * @param noAutoCreateQueue
    */
   default void beforeSend(ServerSession session, Transaction tx, Message message, boolean direct, boolean noAutoCreateQueue) throws ActiveMQException {
      //by default call the old method for backwards compatibility
      this.beforeSend(tx, message, direct, noAutoCreateQueue);
   }

   /**
    * After a message is sent
    *
    * @param session the session that sends the message
    * @param tx
    * @param message
    * @param direct
    * @param noAutoCreateQueue
    * @param result
    */
   default void afterSend(ServerSession session, Transaction tx, Message message, boolean direct, boolean noAutoCreateQueue,
         RoutingStatus result) throws ActiveMQException {
      //by default call the old method for backwards compatibility
      this.afterSend(tx, message, direct, noAutoCreateQueue, result);
   }


   /**
    * Before a message is sent
    *
    * @param tx
    * @param message
    * @param direct
    * @param noAutoCreateQueue
    *
    * @deprecated use throws ActiveMQException {@link #beforeSend(ServerSession, Transaction, Message, boolean, boolean)}
    */
   @Deprecated
   default void beforeSend(Transaction tx, Message message, boolean direct, boolean noAutoCreateQueue) throws ActiveMQException {

   }

   /**
    * After a message is sent
    *
    * @param tx
    * @param message
    * @param direct
    * @param noAutoCreateQueue
    * @param result
    *
    * @deprecated use throws ActiveMQException {@link #afterSend(ServerSession, Transaction, Message, boolean, boolean, RoutingStatus)}
    */
   @Deprecated
   default void afterSend(Transaction tx, Message message, boolean direct, boolean noAutoCreateQueue,
         RoutingStatus result) throws ActiveMQException {

   }

   /**
    * Before a message is routed
    *
    * @param message
    * @param context
    * @param direct
    * @param rejectDuplicates
    */
   default void beforeMessageRoute(Message message, RoutingContext context, boolean direct, boolean rejectDuplicates) throws ActiveMQException {

   }

   /**
    * After a message is routed
    *
    * @param message
    * @param context
    * @param direct
    * @param rejectDuplicates
    * @param result
    */
   default void afterMessageRoute(Message message, RoutingContext context, boolean direct, boolean rejectDuplicates,
         RoutingStatus result) throws ActiveMQException {

   }

   /**
    * Before a message is delivered to a client consumer
    *
    * @param consumer the consumer the message will be delivered to
    * @param reference message reference
    */
   default void beforeDeliver(ServerConsumer consumer, MessageReference reference) throws ActiveMQException {
      //by default call the old method for backwards compatibility
      this.beforeDeliver(reference);
   }

   /**
    * After a message is delivered to a client consumer
    *
    * @param consumer the consumer the message was delivered to
    * @param reference message reference
    */
   default void afterDeliver(ServerConsumer consumer, MessageReference reference) throws ActiveMQException {
      //by default call the old method for backwards compatibility
      this.afterDeliver(reference);
   }

   /**
    * Before a message is delivered to a client consumer
    *
    * @param reference
    *
    * @deprecated use throws ActiveMQException {@link #beforeDeliver(ServerConsumer, MessageReference)}
    */
   @Deprecated
   default void beforeDeliver(MessageReference reference) throws ActiveMQException {

   }

   /**
    * After a message is delivered to a client consumer
    *
    * @param reference
    *
    * @deprecated use throws ActiveMQException {@link #afterDeliver(ServerConsumer, MessageReference)}
    */
   @Deprecated
   default void afterDeliver(MessageReference reference) throws ActiveMQException {

   }

   /**
    * A message has been expired
    *
    * @param message The expired message
    * @param messageExpiryAddress The message expiry address if exists
    */
   default void messageExpired(MessageReference message, SimpleString messageExpiryAddress) throws ActiveMQException {

   }

   /**
    * A message has been acknowledged
    *
    * @param ref The acked message
    * @param reason The ack reason
    */
   default void messageAcknowledged(MessageReference ref, AckReason reason) throws ActiveMQException {

   }

   /**
    * Before a bridge is deployed
    *
    * @param config The bridge configuration
    */
   default void beforeDeployBridge(BridgeConfiguration config) throws ActiveMQException {

   }

   /**
    * After a bridge has been deployed
    *
    * @param bridge The newly deployed bridge
    */
   default void afterDeployBridge(Bridge bridge) throws ActiveMQException {

   }

   /**
    * A Critical failure has been detected.
    * This will be called before the broker is stopped
    * @param components
    * @throws ActiveMQException
    */
   default void criticalFailure(CriticalComponent components) throws ActiveMQException {
   }

}
