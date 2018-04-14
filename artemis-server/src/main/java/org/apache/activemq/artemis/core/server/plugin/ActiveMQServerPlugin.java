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

import java.util.EnumSet;
import java.util.Map;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.BridgeConfiguration;
import org.apache.activemq.artemis.core.persistence.OperationContext;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.QueueBinding;
import org.apache.activemq.artemis.core.postoffice.RoutingStatus;
import org.apache.activemq.artemis.core.security.SecurityAuth;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.QueueConfig;
import org.apache.activemq.artemis.core.server.RoutingContext;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.core.server.cluster.Bridge;
import org.apache.activemq.artemis.core.server.impl.AckReason;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.protocol.SessionCallback;
import org.apache.activemq.artemis.utils.critical.CriticalComponent;

/**
 *
 */
public interface ActiveMQServerPlugin {

   /**
    * The plugin has been registered with the server
    *
    * @param server The ActiveMQServer the plugin has been registered to
    */
   default void registered(ActiveMQServer server) {

   }

   /**
    * The plugin has been unregistered with the server
    *
    * @param server The ActiveMQServer the plugin has been unregistered to
    */
   default void unregistered(ActiveMQServer server) {

   }

   /**
    * A connection has been created.
    *
    * @param connection The newly created connection
    * @throws ActiveMQException
    */
   default void afterCreateConnection(RemotingConnection connection) throws ActiveMQException {

   }

   /**
    * A connection has been destroyed.
    *
    * @param connection
    * @throws ActiveMQException
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
    * @throws ActiveMQException
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
    * @throws ActiveMQException
    */
   default void afterCreateSession(ServerSession session) throws ActiveMQException {

   }

   /**
    * Before a session is closed
    *
    * @param session
    * @param failed
    * @throws ActiveMQException
    */
   default void beforeCloseSession(ServerSession session, boolean failed) throws ActiveMQException {

   }

   /**
    * After a session is closed
    *
    * @param session
    * @param failed
    * @throws ActiveMQException
    */
   default void afterCloseSession(ServerSession session, boolean failed) throws ActiveMQException {

   }

   /**
    * Before session metadata is added to the session
    *
    * @param session
    * @param key
    * @param data
    * @throws ActiveMQException
    */
   default void beforeSessionMetadataAdded(ServerSession session, String key, String data) throws ActiveMQException {

   }

   /**
    * After session metadata is added to the session
    *
    * @param session
    * @param key
    * @param data
    * @throws ActiveMQException
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
    * @throws ActiveMQException
    *
    * @deprecated use {@link #beforeCreateConsumer(long, QueueBinding, SimpleString, boolean, boolean)
    */
   @Deprecated
   default void beforeCreateConsumer(long consumerID, SimpleString queueName, SimpleString filterString,
         boolean browseOnly, boolean supportLargeMessage) throws ActiveMQException {

   }


   /**
    *
    * Before a consumer is created
    *
    * @param consumerID
    * @param QueueBinding
    * @param filterString
    * @param browseOnly
    * @param supportLargeMessage
    * @throws ActiveMQException
    */
   default void beforeCreateConsumer(long consumerID, QueueBinding queueBinding, SimpleString filterString,
         boolean browseOnly, boolean supportLargeMessage) throws ActiveMQException {
      //by default call the old method for backwards compatibility
      this.beforeCreateConsumer(consumerID, queueBinding.getQueue().getName(), filterString, browseOnly, supportLargeMessage);
   }

   /**
    * After a consumer has been created
    *
    * @param consumer the created consumer
    * @throws ActiveMQException
    */
   default void afterCreateConsumer(ServerConsumer consumer) throws ActiveMQException {

   }

   /**
    * Before a consumer is closed
    *
    * @param consumer
    * @param failed
    * @throws ActiveMQException
    */
   default void beforeCloseConsumer(ServerConsumer consumer, boolean failed) throws ActiveMQException {

   }

   /**
    * After a consumer is closed
    *
    * @param consumer
    * @param failed
    * @throws ActiveMQException
    */
   default void afterCloseConsumer(ServerConsumer consumer, boolean failed) throws ActiveMQException {

   }

   /**
    * Before an address is added tot he broker
    *
    * @param addressInfo The addressInfo that will be added
    * @param reload If the address is being reloaded
    * @throws ActiveMQException
    */
   default void beforeAddAddress(AddressInfo addressInfo, boolean reload) throws ActiveMQException {

   }

   /**
    * After an address has been added tot he broker
    *
    * @param addressInfo The newly added address
    * @param reload If the address is being reloaded
    * @throws ActiveMQException
    */
   default void afterAddAddress(AddressInfo addressInfo, boolean reload) throws ActiveMQException {

   }


   /**
    * Before an address is updated
    *
    * @param address The existing address info that is about to be updated
    * @param routingTypes The new routing types that the address will be updated with
    * @throws ActiveMQException
    */
   default void beforeUpdateAddress(SimpleString address, EnumSet<RoutingType> routingTypes) throws ActiveMQException {

   }

   /**
    * After an address has been updated
    *
    * @param addressInfo The newly updated address info
    * @throws ActiveMQException
    */
   default void afterUpdateAddress(AddressInfo addressInfo) throws ActiveMQException {

   }

   /**
    * Before an address is removed
    *
    * @param address The address that will be removed
    * @throws ActiveMQException
    */
   default void beforeRemoveAddress(SimpleString address) throws ActiveMQException {

   }

   /**
    * After an address has been removed
    *
    * @param address The address that has been removed
    * @param addressInfo The address info that has been removed or null if not removed
    * @throws ActiveMQException
    */
   default void afterRemoveAddress(SimpleString address, AddressInfo addressInfo) throws ActiveMQException {

   }

   /**
    * Before a queue is created
    *
    * @param queueConfig
    * @throws ActiveMQException
    */
   default void beforeCreateQueue(QueueConfig queueConfig) throws ActiveMQException {

   }

   /**
    * After a queue has been created
    *
    * @param queue The newly created queue
    * @throws ActiveMQException
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
    * @throws ActiveMQException
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
    * @throws ActiveMQException
    */
   default void afterDestroyQueue(Queue queue, SimpleString address, final SecurityAuth session, boolean checkConsumerCount,
         boolean removeConsumers, boolean autoDeleteAddress) throws ActiveMQException {

   }

   /**
    * Before a binding is added
    *
    * @param binding
    * @throws ActiveMQException
    */
   default void beforeAddBinding(Binding binding) throws ActiveMQException {

   }

   /**
    * After a binding has been added
    *
    * @param binding The newly added binding
    * @throws ActiveMQException
    */
   default void afterAddBinding(Binding binding) throws ActiveMQException {

   }

   /**
    * Before a binding is removed
    *
    * @param uniqueName
    * @param tx
    * @param deleteData
    * @throws ActiveMQException
    */
   default void beforeRemoveBinding(SimpleString uniqueName, Transaction tx, boolean deleteData) throws ActiveMQException {

   }

   /**
    * After a binding is removed
    *
    * @param binding
    * @param tx
    * @param deleteData
    * @throws ActiveMQException
    */
   default void afterRemoveBinding(Binding binding, Transaction tx, boolean deleteData) throws ActiveMQException {

   }

   /**
    * Before a message is sent
    *
    * @param session the session that sends the message
    * @param tx
    * @param message
    * @param direct
    * @param noAutoCreateQueue
    * @throws ActiveMQException
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
    * @throws ActiveMQException
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
    * @throws ActiveMQException
    *
    * @deprecated use {@link #beforeSend(ServerSession, Transaction, Message, boolean, boolean)}
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
    * @throws ActiveMQException
    *
    * @deprecated use {@link #afterSend(ServerSession, Transaction, Message, boolean, boolean, RoutingStatus)}
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
    * @throws ActiveMQException
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
    * @throws ActiveMQException
    */
   default void afterMessageRoute(Message message, RoutingContext context, boolean direct, boolean rejectDuplicates,
         RoutingStatus result) throws ActiveMQException {

   }

   /**
    * Before a message is delivered to a client consumer
    *
    * @param consumer the consumer the message will be delivered to
    * @param reference message reference
    * @throws ActiveMQException
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
    * @throws ActiveMQException
    */
   default void afterDeliver(ServerConsumer consumer, MessageReference reference) throws ActiveMQException {
      //by default call the old method for backwards compatibility
      this.afterDeliver(reference);
   }

   /**
    * Before a message is delivered to a client consumer
    *
    * @param reference
    * @throws ActiveMQException
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
    * @throws ActiveMQException
    *
    * @deprecated use {@link #afterDeliver(ServerConsumer, MessageReference)}
    */
   @Deprecated
   default void afterDeliver(MessageReference reference) throws ActiveMQException {

   }

   /**
    * A message has been expired
    *
    * @param message The expired message
    * @param messageExpiryAddress The message expiry address if exists
    * @throws ActiveMQException
    *
    * @deprecated use {@link #messageExpired(MessageReference, SimpleString, ServerConsumer)}
    */
   @Deprecated
   default void messageExpired(MessageReference message, SimpleString messageExpiryAddress) throws ActiveMQException {

   }

   /**
    * A message has been expired
    *
    * @param message The expired message
    * @param messageExpiryAddress The message expiry address if exists
    * @param consumer the Consumer that acknowledged the message - this field is optional
    * and can be null
    * @throws ActiveMQException
    */
   default void messageExpired(MessageReference message, SimpleString messageExpiryAddress, ServerConsumer consumer) throws ActiveMQException {

   }

   /**
    * A message has been acknowledged
    *
    * @param ref The acked message
    * @param reason The ack reason
    * @throws ActiveMQException
    *
    * @deprecated use {@link #messageAcknowledged(MessageReference, AckReason, ServerConsumer)}
    */
   @Deprecated
   default void messageAcknowledged(MessageReference ref, AckReason reason) throws ActiveMQException {

   }

   /**
    * A message has been acknowledged
    *
    * @param ref The acked message
    * @param reason The ack reason
    * @param consumer the Consumer that acknowledged the message - this field is optional
    * and can be null
    * @throws ActiveMQException
    *
    */
   default void messageAcknowledged(MessageReference ref, AckReason reason, ServerConsumer consumer) throws ActiveMQException {
      //by default call the old method for backwards compatibility
      this.messageAcknowledged(ref, reason);
   }


   /**
    * Before a bridge is deployed
    *
    * @param config The bridge configuration
    * @throws ActiveMQException
    */
   default void beforeDeployBridge(BridgeConfiguration config) throws ActiveMQException {

   }

   /**
    * After a bridge has been deployed
    *
    * @param bridge The newly deployed bridge
    * @throws ActiveMQException
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

   /**
    * used to pass configured properties to Plugin
    *
    * @param properties
    */
   default void init(Map<String, String> properties) {
   }
}
