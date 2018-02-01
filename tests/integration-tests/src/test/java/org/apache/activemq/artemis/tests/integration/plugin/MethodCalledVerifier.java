/**
 *
 */
package org.apache.activemq.artemis.tests.integration.plugin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Preconditions;

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
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.QueueConfig;
import org.apache.activemq.artemis.core.server.RoutingContext;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.core.server.cluster.Bridge;
import org.apache.activemq.artemis.core.server.impl.AckReason;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerPlugin;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.protocol.SessionCallback;
import org.apache.activemq.artemis.tests.util.Wait;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
public class MethodCalledVerifier implements ActiveMQServerPlugin {

   private final Map<String, AtomicInteger> methodCalls;

   public static final String AFTER_CREATE_CONNECTION = "afterCreateConnection";
   public static final String AFTER_DESTROY_CONNECTION = "afterDestroyConnection";
   public static final String BEFORE_CREATE_SESSION = "beforeCreateSession";
   public static final String AFTER_CREATE_SESSION = "afterCreateSession";
   public static final String BEFORE_CLOSE_SESSION = "beforeCloseSession";
   public static final String AFTER_CLOSE_SESSION = "afterCloseSession";
   public static final String BEFORE_SESSION_METADATA_ADDED = "beforeSessionMetadataAdded";
   public static final String AFTER_SESSION_METADATA_ADDED = "afterSessionMetadataAdded";
   public static final String BEFORE_CREATE_CONSUMER = "beforeCreateConsumer";
   public static final String AFTER_CREATE_CONSUMER = "afterCreateConsumer";
   public static final String BEFORE_CLOSE_CONSUMER = "beforeCloseConsumer";
   public static final String AFTER_CLOSE_CONSUMER = "afterCloseConsumer";
   public static final String BEFORE_ADD_ADDRESS = "beforeAddAddress";
   public static final String AFTER_ADD_ADDRESS = "afterAddAddress";
   public static final String BEFORE_UPDATE_ADDRESS = "beforeUpdateAddress";
   public static final String AFTER_UPDATE_ADDRESS = "afterUpdateAddress";
   public static final String BEFORE_REMOVE_ADDRESS = "beforeRemoveAddress";
   public static final String AFTER_REMOVE_ADDRESS = "afterRemoveAddress";
   public static final String BEFORE_CREATE_QUEUE = "beforeCreateQueue";
   public static final String AFTER_CREATE_QUEUE = "afterCreateQueue";
   public static final String BEFORE_DESTROY_QUEUE = "beforeDestroyQueue";
   public static final String AFTER_DESTROY_QUEUE = "afterDestroyQueue";
   public static final String BEFORE_ADD_BINDING = "beforeAddBinding";
   public static final String AFTER_ADD_BINDING = "afterAddBinding";
   public static final String BEFORE_REMOVE_BINDING = "beforeRemoveBinding";
   public static final String AFTER_REMOVE_BINDING = "afterRemoveBinding";
   public static final String MESSAGE_EXPIRED = "messageExpired";
   public static final String MESSAGE_ACKED = "messageAcknowledged";
   public static final String BEFORE_SEND = "beforeSend";
   public static final String AFTER_SEND = "afterSend";
   public static final String BEFORE_MESSAGE_ROUTE = "beforeMessageRoute";
   public static final String AFTER_MESSAGE_ROUTE = "afterMessageRoute";
   public static final String BEFORE_DELIVER = "beforeDeliver";
   public static final String AFTER_DELIVER = "afterDeliver";
   public static final String BEFORE_DEPLOY_BRIDGE = "beforeDeployBridge";
   public static final String AFTER_DEPLOY_BRIDGE = "afterDeployBridge";

   public MethodCalledVerifier(Map<String, AtomicInteger> methodCalls) {
      super();
      this.methodCalls = methodCalls;
   }

   public MethodCalledVerifier() {
      this(new HashMap<String, AtomicInteger>());
   }

   @Override
   public void afterCreateConnection(RemotingConnection connection) {
      Preconditions.checkNotNull(connection);
      methodCalled(AFTER_CREATE_CONNECTION);
   }

   @Override
   public void afterDestroyConnection(RemotingConnection connection) {
      Preconditions.checkNotNull(connection);
      methodCalled(AFTER_DESTROY_CONNECTION);
   }

   @Override
   public void beforeCreateSession(String name, String username, int minLargeMessageSize, RemotingConnection connection,
                                   boolean autoCommitSends, boolean autoCommitAcks, boolean preAcknowledge, boolean xa,
                                   String defaultAddress, SessionCallback callback, boolean autoCreateQueues,
                                   OperationContext context, Map<SimpleString, RoutingType> prefixes) {
      Preconditions.checkNotNull(connection);
      methodCalled(BEFORE_CREATE_SESSION);
   }

   @Override
   public void afterCreateSession(ServerSession session) {
      Preconditions.checkNotNull(session);
      methodCalled(AFTER_CREATE_SESSION);
   }

   @Override
   public void beforeCloseSession(ServerSession session, boolean failed) {
      Preconditions.checkNotNull(session);
      methodCalled(BEFORE_CLOSE_SESSION);
   }

   @Override
   public void afterCloseSession(ServerSession session, boolean failed) {
      Preconditions.checkNotNull(session);
      methodCalled(AFTER_CLOSE_SESSION);
   }

   @Override
   public void beforeSessionMetadataAdded(ServerSession session, String key, String data) {
      Preconditions.checkNotNull(key);
      methodCalled(BEFORE_SESSION_METADATA_ADDED);
   }

   @Override
   public void afterSessionMetadataAdded(ServerSession session, String key, String data) {
      Preconditions.checkNotNull(key);
      methodCalled(AFTER_SESSION_METADATA_ADDED);
   }

   @Override
   public void beforeCreateConsumer(long consumerID, QueueBinding queueBinding, SimpleString filterString,
                                    boolean browseOnly, boolean supportLargeMessage) {
      Preconditions.checkNotNull(queueBinding);
      methodCalled(BEFORE_CREATE_CONSUMER);
   }

   @Override
   public void afterCreateConsumer(ServerConsumer consumer) {
      Preconditions.checkNotNull(consumer);
      methodCalled(AFTER_CREATE_CONSUMER);
   }

   @Override
   public void beforeCloseConsumer(ServerConsumer consumer, boolean failed) {
      Preconditions.checkNotNull(consumer);
      methodCalled(BEFORE_CLOSE_CONSUMER);
   }

   @Override
   public void afterCloseConsumer(ServerConsumer consumer, boolean failed) {
      Preconditions.checkNotNull(consumer);
      methodCalled(AFTER_CLOSE_CONSUMER);
   }

   @Override
   public void beforeAddAddress(AddressInfo addressInfo, boolean reload) throws ActiveMQException {
      Preconditions.checkNotNull(addressInfo);
      methodCalled(BEFORE_ADD_ADDRESS);
   }

   @Override
   public void afterAddAddress(AddressInfo addressInfo, boolean reload) throws ActiveMQException {
      Preconditions.checkNotNull(addressInfo);
      methodCalled(AFTER_ADD_ADDRESS);
   }

   @Override
   public void beforeUpdateAddress(SimpleString address, EnumSet<RoutingType> routingTypes)
         throws ActiveMQException {
      Preconditions.checkNotNull(address);
      Preconditions.checkNotNull(routingTypes);
      methodCalled(BEFORE_UPDATE_ADDRESS);
   }

   @Override
   public void afterUpdateAddress(AddressInfo addressInfo) throws ActiveMQException {
      Preconditions.checkNotNull(addressInfo);
      methodCalled(AFTER_UPDATE_ADDRESS);
   }

   @Override
   public void beforeRemoveAddress(SimpleString address) throws ActiveMQException {
      Preconditions.checkNotNull(address);
      methodCalled(BEFORE_REMOVE_ADDRESS);
   }

   @Override
   public void afterRemoveAddress(SimpleString address, AddressInfo addressInfo) throws ActiveMQException {
      Preconditions.checkNotNull(address);
      Preconditions.checkNotNull(addressInfo);
      methodCalled(AFTER_REMOVE_ADDRESS);
   }

   @Override
   public void beforeCreateQueue(QueueConfig queueConfig) {
      Preconditions.checkNotNull(queueConfig);
      methodCalled(BEFORE_CREATE_QUEUE);
   }

   @Override
   public void afterCreateQueue(org.apache.activemq.artemis.core.server.Queue queue) {
      Preconditions.checkNotNull(queue);
      methodCalled(AFTER_CREATE_QUEUE);
   }

   @Override
   public void beforeDestroyQueue(SimpleString queueName, SecurityAuth session, boolean checkConsumerCount,
         boolean removeConsumers, boolean autoDeleteAddress) {
      Preconditions.checkNotNull(queueName);
      methodCalled(BEFORE_DESTROY_QUEUE);
   }

   @Override
   public void afterDestroyQueue(Queue queue, SimpleString address, SecurityAuth session, boolean checkConsumerCount,
         boolean removeConsumers, boolean autoDeleteAddress) {
      Preconditions.checkNotNull(queue);
      methodCalled(AFTER_DESTROY_QUEUE);
   }

   @Override
   public void beforeAddBinding(Binding binding) throws ActiveMQException {
      Preconditions.checkNotNull(binding);
      methodCalled(BEFORE_ADD_BINDING);
   }

   @Override
   public void afterAddBinding(Binding binding) throws ActiveMQException {
      Preconditions.checkNotNull(binding);
      methodCalled(AFTER_ADD_BINDING);
   }

   @Override
   public void beforeRemoveBinding(SimpleString uniqueName, Transaction tx, boolean deleteData)
         throws ActiveMQException {
      Preconditions.checkNotNull(uniqueName);
      methodCalled(BEFORE_REMOVE_BINDING);
   }

   @Override
   public void afterRemoveBinding(Binding binding, Transaction tx, boolean deleteData) throws ActiveMQException {
      Preconditions.checkNotNull(binding);
      methodCalled(AFTER_REMOVE_BINDING);
   }

   @Override
   public void messageExpired(MessageReference message, SimpleString messageExpiryAddress) {
      Preconditions.checkNotNull(message);
      methodCalled(MESSAGE_EXPIRED);
   }

   @Override
   public void messageAcknowledged(MessageReference ref, AckReason reason) {
      Preconditions.checkNotNull(ref);
      Preconditions.checkNotNull(reason);
      methodCalled(MESSAGE_ACKED);
   }

   @Override
   public void beforeSend(ServerSession session, Transaction tx, Message message, boolean direct,
         boolean noAutoCreateQueue) {
      Preconditions.checkNotNull(message);
      methodCalled(BEFORE_SEND);
   }

   @Override
   public void afterSend(ServerSession session, Transaction tx, Message message, boolean direct,
         boolean noAutoCreateQueue,
                         RoutingStatus result) {
      Preconditions.checkNotNull(message);
      Preconditions.checkNotNull(result);
      methodCalled(AFTER_SEND);
   }

   @Override
   public void beforeMessageRoute(Message message, RoutingContext context, boolean direct, boolean rejectDuplicates) {
      Preconditions.checkNotNull(message);
      Preconditions.checkNotNull(context);
      methodCalled(BEFORE_MESSAGE_ROUTE);
   }

   @Override
   public void afterMessageRoute(Message message, RoutingContext context, boolean direct, boolean rejectDuplicates,
                                 RoutingStatus result) {
      Preconditions.checkNotNull(message);
      Preconditions.checkNotNull(context);
      Preconditions.checkNotNull(result);
      methodCalled(AFTER_MESSAGE_ROUTE);
   }

   @Override
   public void beforeDeliver(ServerConsumer consumer, MessageReference reference) {
      Preconditions.checkNotNull(reference);
      methodCalled(BEFORE_DELIVER);
   }

   @Override
   public void afterDeliver(ServerConsumer consumer, MessageReference reference) {
      Preconditions.checkNotNull(reference);
      methodCalled(AFTER_DELIVER);
   }

   @Override
   public void beforeDeployBridge(BridgeConfiguration config) {
      Preconditions.checkNotNull(config);
      methodCalled(BEFORE_DEPLOY_BRIDGE);
   }

   @Override
   public void afterDeployBridge(Bridge bridge) {
      Preconditions.checkNotNull(bridge);
      methodCalled(AFTER_DEPLOY_BRIDGE);
   }

   public void validatePluginMethodsEquals(int count, String... names) {
      Arrays.asList(names).forEach(name -> {
         try {
            Wait.waitFor(() -> count == methodCalls.getOrDefault(name, new AtomicInteger()).get());
         } catch (Throwable ignored) {
         }
         assertEquals("validating method " + name, count, methodCalls.getOrDefault(name, new AtomicInteger()).get());
      });
   }

   public void validatePluginMethodsAtLeast(int count, String... names) {
      Arrays.asList(names).forEach(name -> {
         try {
            Wait.waitFor(() -> count <= methodCalls.getOrDefault(name, new AtomicInteger()).get());
         } catch (Exception e) {
            e.printStackTrace();
         }
         assertTrue("validating method " + name + " expected less than " + count, count <= methodCalls.getOrDefault(name, new AtomicInteger()).get());
      });
   }

   private void methodCalled(String name) {
      methodCalls.computeIfAbsent(name, k -> new AtomicInteger()).incrementAndGet();
   }

}
