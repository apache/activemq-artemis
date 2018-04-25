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

package org.apache.activemq.artemis.core.server.plugin.impl;

import java.io.Serializable;
import java.util.Map;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.BridgeConfiguration;
import org.apache.activemq.artemis.core.persistence.OperationContext;
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
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerPlugin;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.protocol.SessionCallback;
import org.apache.activemq.artemis.utils.critical.CriticalComponent;

/**
 * plugin to log various events within the broker, configured with the following booleans
 *
 * LOG_CONNECTION_EVENTS - connections creation/destroy
 * LOG_SESSION_EVENTS - sessions creation/close
 * LOG_CONSUMER_EVENTS - consumers creation/close
 * LOG_DELIVERING_EVENTS - messages delivered to consumer, acked by consumer
 * LOG_SENDING_EVENTS -  messaged is sent, message is routed
 * LOG_INTERNAL_EVENTS - critical failures, bridge deployments, queue creation/destroyed, message expired
 */

public class LoggingActiveMQServerPlugin implements ActiveMQServerPlugin, Serializable {

   private static final long serialVersionUID = 1L;

   public static final String LOG_ALL_EVENTS = "LOG_ALL_EVENTS";
   public static final String LOG_CONNECTION_EVENTS = "LOG_CONNECTION_EVENTS";
   public static final String LOG_SESSION_EVENTS = "LOG_SESSION_EVENTS";
   public static final String LOG_CONSUMER_EVENTS = "LOG_CONSUMER_EVENTS";
   public static final String LOG_DELIVERING_EVENTS = "LOG_DELIVERING_EVENTS";
   public static final String LOG_SENDING_EVENTS = "LOG_SENDING_EVENTS";
   public static final String LOG_INTERNAL_EVENTS = "LOG_INTERNAL_EVENTS";

   public static final String UNAVAILABLE = "UNAVAILABLE";

   private boolean logAll = false;
   private boolean logConnectionEvents = false;
   private boolean logSessionEvents = false;
   private boolean logConsumerEvents = false;
   private boolean logDeliveringEvents = false;
   private boolean logSendingEvents = false;
   private boolean logInternalEvents = false;

   public boolean isLogAll() {
      return logAll;
   }

   public boolean isLogConnectionEvents() {
      return logConnectionEvents;
   }

   public boolean isLogSessionEvents() {
      return logSessionEvents;
   }

   public boolean isLogConsumerEvents() {
      return logConsumerEvents;
   }

   public boolean isLogDeliveringEvents() {
      return logDeliveringEvents;
   }

   public boolean isLogSendingEvents() {
      return logSendingEvents;
   }

   public boolean isLogInternalEvents() {
      return logInternalEvents;
   }

   /**
    * used to pass configured properties to Plugin
    *
    * @param properties
    */
   @Override
   public void init(Map<String, String> properties) {

      logAll = Boolean.parseBoolean(properties.getOrDefault(LOG_ALL_EVENTS, "false"));
      logConnectionEvents = Boolean.parseBoolean(properties.getOrDefault(LOG_CONNECTION_EVENTS, "false"));
      logSessionEvents = Boolean.parseBoolean(properties.getOrDefault(LOG_SESSION_EVENTS, "false"));
      logConsumerEvents = Boolean.parseBoolean(properties.getOrDefault(LOG_CONSUMER_EVENTS, "false"));
      logDeliveringEvents = Boolean.parseBoolean(properties.getOrDefault(LOG_DELIVERING_EVENTS, "false"));
      logSendingEvents = Boolean.parseBoolean(properties.getOrDefault(LOG_SENDING_EVENTS, "false"));
      logInternalEvents = Boolean.parseBoolean(properties.getOrDefault(LOG_INTERNAL_EVENTS, "false"));

      if (LoggingActiveMQServerPluginLogger.LOGGER.isDebugEnabled()) {
         dumpConfiguration();
      }

   }

   /**
    * A connection has been created.
    *
    * @param connection The newly created connection
    * @throws ActiveMQException
    */
   @Override
   public void afterCreateConnection(RemotingConnection connection) throws ActiveMQException {
      if (logAll || logConnectionEvents) {
         LoggingActiveMQServerPluginLogger.LOGGER.afterCreateConnection(connection);
      }
   }

   /**
    * A connection has been destroyed.
    *
    * @param connection
    * @throws ActiveMQException
    */
   @Override
   public void afterDestroyConnection(RemotingConnection connection) throws ActiveMQException {
      if (logAll || logConnectionEvents) {
         LoggingActiveMQServerPluginLogger.LOGGER.afterDestroyConnection(connection);
      }
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
    * @param publicAddress
    * @param callback
    * @param autoCreateQueues
    * @param context
    * @param prefixes
    * @throws ActiveMQException
    */
   @Override
   public void beforeCreateSession(String name,
                                   String username,
                                   int minLargeMessageSize,
                                   RemotingConnection connection,
                                   boolean autoCommitSends,
                                   boolean autoCommitAcks,
                                   boolean preAcknowledge,
                                   boolean xa,
                                   String publicAddress,
                                   SessionCallback callback,
                                   boolean autoCreateQueues,
                                   OperationContext context,
                                   Map<SimpleString, RoutingType> prefixes) throws ActiveMQException {

      if (logAll || logSessionEvents) {
         LoggingActiveMQServerPluginLogger.LOGGER.beforeCreateSession(name, username, minLargeMessageSize, connection,
                                                                      autoCommitSends, autoCommitAcks, preAcknowledge,
                                                                      xa, publicAddress, context);
      }

   }

   /**
    * After a session has been created.
    *
    * @param session The newly created session
    * @throws ActiveMQException
    */
   @Override
   public void afterCreateSession(ServerSession session) throws ActiveMQException {
      if (logAll || logSessionEvents) {
         LoggingActiveMQServerPluginLogger.LOGGER.afterCreateSession((session == null ? UNAVAILABLE : session.getName()),
                                                                     (session == null ? UNAVAILABLE : session.getConnectionID()));
      }

   }

   /**
    * Before a session is closed
    *
    * @param session
    * @param failed
    * @throws ActiveMQException
    */
   @Override
   public void beforeCloseSession(ServerSession session, boolean failed) throws ActiveMQException {
      if (logAll || logSessionEvents) {
         LoggingActiveMQServerPluginLogger.LOGGER.beforeCloseSession((session == null ? UNAVAILABLE : session.getName()),
                                                                     session, failed);
      }
   }

   /**
    * After a session is closed
    *
    * @param session
    * @param failed
    * @throws ActiveMQException
    */
   @Override
   public void afterCloseSession(ServerSession session, boolean failed) throws ActiveMQException {
      if (logAll || logSessionEvents) {
         LoggingActiveMQServerPluginLogger.LOGGER.afterCloseSession((session == null ? UNAVAILABLE : session.getName()),
                                                                    failed);
      }
   }

   /**
    * Before session metadata is added to the session
    *
    * @param session
    * @param key
    * @param data
    * @throws ActiveMQException
    */
   @Override
   public void beforeSessionMetadataAdded(ServerSession session, String key, String data) throws ActiveMQException {
      if (logAll || logSessionEvents) {
         LoggingActiveMQServerPluginLogger.LOGGER.beforeSessionMetadataAdded((session == null ? UNAVAILABLE : session.getName()),
                                                                             session, key, data);
      }
   }

   /**
    * After session metadata is added to the session
    *
    * @param session
    * @param key
    * @param data
    * @throws ActiveMQException
    */
   @Override
   public void afterSessionMetadataAdded(ServerSession session, String key, String data) throws ActiveMQException {
      if (logAll || logSessionEvents) {

         //Details - debug level
         LoggingActiveMQServerPluginLogger.LOGGER.afterSessionMetadataAddedDetails((session == null ? UNAVAILABLE : session.getName()),
                                                                                   session, key, data);

         // info level  log
         LoggingActiveMQServerPluginLogger.LOGGER.afterSessionMetadataAdded((session == null ? UNAVAILABLE : session.getName()),
                                                                            key, data);
      }
   }

   /**
    * Before a consumer is created
    *
    * @param consumerID
    * @param queueBinding
    * @param filterString
    * @param browseOnly
    * @param supportLargeMessage
    * @throws ActiveMQException
    */
   @Override
   public void beforeCreateConsumer(long consumerID,
                                    QueueBinding queueBinding,
                                    SimpleString filterString,
                                    boolean browseOnly,
                                    boolean supportLargeMessage) throws ActiveMQException {

      if (logAll || logConsumerEvents) {
         if (LoggingActiveMQServerPluginLogger.LOGGER.isInfoEnabled()) {
            LoggingActiveMQServerPluginLogger.LOGGER.beforeCreateConsumer(Long.toString(consumerID), queueBinding,
                                                                          filterString, browseOnly, supportLargeMessage);
         }
      }

   }

   /**
    * After a consumer has been created
    *
    * @param consumer the created consumer
    * @throws ActiveMQException
    */
   @Override
   public void afterCreateConsumer(ServerConsumer consumer) throws ActiveMQException {

      if (logAll || logConsumerEvents) {
         if (LoggingActiveMQServerPluginLogger.LOGGER.isInfoEnabled()) {
            LoggingActiveMQServerPluginLogger.LOGGER.afterCreateConsumer((consumer == null ? UNAVAILABLE : Long.toString(consumer.getID())),
                                                                         (consumer == null ? UNAVAILABLE : consumer.getSessionID()));
         }
      }

   }

   /**
    * Before a consumer is closed
    *
    * @param consumer
    * @param failed
    * @throws ActiveMQException
    */
   @Override
   public void beforeCloseConsumer(ServerConsumer consumer, boolean failed) throws ActiveMQException {

      if (logAll || logConsumerEvents) {
         LoggingActiveMQServerPluginLogger.LOGGER.beforeCloseConsumer(consumer, (consumer == null ? UNAVAILABLE : consumer.getSessionID()),
                                                                      failed);
      }
   }

   /**
    * After a consumer is closed
    *
    * @param consumer
    * @param failed
    * @throws ActiveMQException
    */
   @Override
   public void afterCloseConsumer(ServerConsumer consumer, boolean failed) throws ActiveMQException {

      if (logAll || logConsumerEvents) {
         if (LoggingActiveMQServerPluginLogger.LOGGER.isInfoEnabled()) {
            LoggingActiveMQServerPluginLogger.LOGGER.afterCloseConsumer((consumer == null ? UNAVAILABLE : Long.toString(consumer.getID())),
                                                                        (consumer == null ? UNAVAILABLE : consumer.getSessionID()), failed);
         }
      }

   }

   /**
    * Before a queue is created
    *
    * @param queueConfig
    * @throws ActiveMQException
    */
   @Override
   public void beforeCreateQueue(QueueConfig queueConfig) throws ActiveMQException {
      if (logAll || logInternalEvents) {
         LoggingActiveMQServerPluginLogger.LOGGER.beforeCreateQueue(queueConfig);
      }

   }

   /**
    * After a queue has been created
    *
    * @param queue The newly created queue
    * @throws ActiveMQException
    */
   @Override
   public void afterCreateQueue(Queue queue) throws ActiveMQException {
      if (logAll || logInternalEvents) {
         LoggingActiveMQServerPluginLogger.LOGGER.afterCreateQueue(queue);
      }
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
   @Override
   public void beforeDestroyQueue(SimpleString queueName,
                                  final SecurityAuth session,
                                  boolean checkConsumerCount,
                                  boolean removeConsumers,
                                  boolean autoDeleteAddress) throws ActiveMQException {
      if (logAll || logInternalEvents) {
         LoggingActiveMQServerPluginLogger.LOGGER.beforeDestroyQueue(queueName, session, checkConsumerCount, removeConsumers,
                                                                     autoDeleteAddress);
      }

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
   @Override
   public void afterDestroyQueue(Queue queue,
                                 SimpleString address,
                                 final SecurityAuth session,
                                 boolean checkConsumerCount,
                                 boolean removeConsumers,
                                 boolean autoDeleteAddress) throws ActiveMQException {

      if (logAll || logInternalEvents) {
         LoggingActiveMQServerPluginLogger.LOGGER.afterDestroyQueue(queue, address, session, checkConsumerCount,
                                                                    removeConsumers, autoDeleteAddress);
      }

   }

   /**
    * Before a message is sent
    *
    * @param session           the session that sends the message
    * @param tx
    * @param message
    * @param direct
    * @param noAutoCreateQueue
    * @throws ActiveMQException
    */
   @Override
   public void beforeSend(ServerSession session,
                          Transaction tx,
                          Message message,
                          boolean direct,
                          boolean noAutoCreateQueue) throws ActiveMQException {

      if (logAll || logSendingEvents) {
         LoggingActiveMQServerPluginLogger.LOGGER.beforeSend(message, tx, session, direct, noAutoCreateQueue);
      }
   }

   /**
    * After a message is sent
    *
    * @param session           the session that sends the message
    * @param tx
    * @param message
    * @param direct
    * @param noAutoCreateQueue
    * @param result
    * @throws ActiveMQException
    */
   @Override
   public void afterSend(ServerSession session,
                         Transaction tx,
                         Message message,
                         boolean direct,
                         boolean noAutoCreateQueue,
                         RoutingStatus result) throws ActiveMQException {

      if (logAll || logSendingEvents) {

         if (LoggingActiveMQServerPluginLogger.LOGGER.isDebugEnabled()) {
            //details - debug level
            LoggingActiveMQServerPluginLogger.LOGGER.afterSendDetails((message == null ? UNAVAILABLE : Long.toString(message.getMessageID())),
                                                                      message, (session == null ? UNAVAILABLE : session.getName()),
                                                                      tx, session, direct, noAutoCreateQueue);
         }

         if (LoggingActiveMQServerPluginLogger.LOGGER.isInfoEnabled()) {
            //info level log
            LoggingActiveMQServerPluginLogger.LOGGER.afterSend((message == null ? UNAVAILABLE : Long.toString(message.getMessageID())),
                                                               (session == null ? UNAVAILABLE : session.getName()),
                                                               (session == null ? UNAVAILABLE : session.getConnectionID().toString()),
                                                               result);
         }
      }
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
   @Override
   public void beforeMessageRoute(Message message,
                                  RoutingContext context,
                                  boolean direct,
                                  boolean rejectDuplicates) throws ActiveMQException {
      if (logAll || logSendingEvents) {
         LoggingActiveMQServerPluginLogger.LOGGER.beforeMessageRoute(message, context, direct, rejectDuplicates);
      }
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
   @Override
   public void afterMessageRoute(Message message,
                                 RoutingContext context,
                                 boolean direct,
                                 boolean rejectDuplicates,
                                 RoutingStatus result) throws ActiveMQException {
      if (logAll || logSendingEvents) {

         //details - debug level logging
         LoggingActiveMQServerPluginLogger.LOGGER.afterMessageRouteDetails(message, context, direct, rejectDuplicates);

         if (LoggingActiveMQServerPluginLogger.LOGGER.isInfoEnabled()) {
            //info level log
            LoggingActiveMQServerPluginLogger.LOGGER.afterMessageRoute((message == null ? UNAVAILABLE : Long.toString(message.getMessageID())),
                                                                       result);
         }

      }
   }

   /**
    * Before a message is delivered to a client consumer
    *
    * @param consumer  the consumer the message will be delivered to
    * @param reference message reference
    * @throws ActiveMQException
    */
   @Override
   public void beforeDeliver(ServerConsumer consumer, MessageReference reference) throws ActiveMQException {

      if (logAll || logDeliveringEvents) {
         LoggingActiveMQServerPluginLogger.LOGGER.beforeDeliver(consumer, reference);
      }
   }

   /**
    * After a message is delivered to a client consumer
    *
    * @param consumer  the consumer the message was delivered to
    * @param reference message reference
    * @throws ActiveMQException
    */
   @Override
   public void afterDeliver(ServerConsumer consumer, MessageReference reference) throws ActiveMQException {

      if (logAll || logDeliveringEvents) {
         Message message = (reference == null ? null : reference.getMessage());

         if (consumer == null) {
            // log at info level and exit
            if (LoggingActiveMQServerPluginLogger.LOGGER.isInfoEnabled()) {
               LoggingActiveMQServerPluginLogger.LOGGER.afterDeliverNoConsumer((message == null ? UNAVAILABLE : Long.toString(message.getMessageID())));
            }
            return;
         }

         if (LoggingActiveMQServerPluginLogger.LOGGER.isDebugEnabled()) {
            //details --- debug level
            LoggingActiveMQServerPluginLogger.LOGGER.afterDeliverDetails((message == null ? UNAVAILABLE : Long.toString(message.getMessageID())),
                                                                         consumer.getQueueAddress(), consumer.getQueueName(),
                                                                         consumer.getSessionID(), consumer.getID(),
                                                                         reference, consumer);
         }

         if (LoggingActiveMQServerPluginLogger.LOGGER.isInfoEnabled()) {
            //info level log
            LoggingActiveMQServerPluginLogger.LOGGER.afterDeliver((message == null ? UNAVAILABLE : Long.toString(message.getMessageID())),
                                                                  consumer.getQueueAddress(),
                                                                  consumer.getQueueName(),
                                                                  consumer.getSessionID(), consumer.getID());
         }

      }
   }

   @Override
   public void messageExpired(MessageReference message,
                              SimpleString messageExpiryAddress,
                              ServerConsumer consumer) {
      if (logAll || logInternalEvents) {
         LoggingActiveMQServerPluginLogger.LOGGER.messageExpired(message, messageExpiryAddress);
      }
   }

   /**
    * A message has been acknowledged
    *
    * @param ref    The acked message
    * @param reason The ack reason
    * @throws ActiveMQException
    */
   @Override
   public void messageAcknowledged(MessageReference ref, AckReason reason, ServerConsumer consumer) throws ActiveMQException {
      if (logAll || logDeliveringEvents) {

         //details - debug logging
         LoggingActiveMQServerPluginLogger.LOGGER.messageAcknowledgedDetails(ref, reason);

         if (LoggingActiveMQServerPluginLogger.LOGGER.isInfoEnabled()) {
            Message message = (ref == null ? null : ref.getMessage());
            Queue queue = (ref == null ? null : ref.getQueue());

            // info level logging
            LoggingActiveMQServerPluginLogger.LOGGER.messageAcknowledged((message == null ? UNAVAILABLE : Long.toString(message.getMessageID())),
                                                                         (consumer == null ? UNAVAILABLE : consumer.getSessionID() != null ? consumer.getSessionID() : null),
                                                                         (consumer == null ? UNAVAILABLE : Long.toString(consumer.getID())),
                                                                         (queue == null ? UNAVAILABLE : queue.getName().toString()),
                                                                         reason);
         }
      }
   }

   /**
    * Before a bridge is deployed
    *
    * @param config The bridge configuration
    * @throws ActiveMQException
    */
   @Override
   public void beforeDeployBridge(BridgeConfiguration config) throws ActiveMQException {
      if (logAll || logInternalEvents) {
         LoggingActiveMQServerPluginLogger.LOGGER.beforeDeployBridge(config);
      }
   }

   /**
    * After a bridge has been deployed
    *
    * @param bridge The newly deployed bridge
    * @throws ActiveMQException
    */
   @Override
   public void afterDeployBridge(Bridge bridge) throws ActiveMQException {
      if (logAll || logInternalEvents) {
         LoggingActiveMQServerPluginLogger.LOGGER.afterDeployBridge(bridge);
      }
   }

   /**
    * A Critical failure has been detected.
    * This will be called before the broker is stopped
    *
    * @param components
    * @throws ActiveMQException
    */
   @Override
   public void criticalFailure(CriticalComponent components) throws ActiveMQException {
      if (logAll || logInternalEvents) {
         LoggingActiveMQServerPluginLogger.LOGGER.criticalFailure(components);
      }
   }

   /**
    * dump the configuration of the logging Plugin
    */
   private void dumpConfiguration() {
      if (LoggingActiveMQServerPluginLogger.LOGGER.isDebugEnabled()) {
         LoggingActiveMQServerPluginLogger.LOGGER.debug("LoggingPlugin logAll=" + logAll);
         LoggingActiveMQServerPluginLogger.LOGGER.debug("LoggingPlugin logConnectionEvents=" + logConnectionEvents);
         LoggingActiveMQServerPluginLogger.LOGGER.debug("LoggingPlugin logSessionEvents=" + logSessionEvents);
         LoggingActiveMQServerPluginLogger.LOGGER.debug("LoggingPlugin logConsumerEvents=" + logConsumerEvents);
         LoggingActiveMQServerPluginLogger.LOGGER.debug("LoggingPlugin logSendingEvents=" + logSendingEvents);
         LoggingActiveMQServerPluginLogger.LOGGER.debug("LoggingPlugin logDeliveringEvents=" + logDeliveringEvents);
         LoggingActiveMQServerPluginLogger.LOGGER.debug("LoggingPlugin logInternalEvents=" + logInternalEvents);
      }

   }

}
