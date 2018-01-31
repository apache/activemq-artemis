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
package org.apache.activemq.artemis.core.server.plugin.impl;

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
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.utils.critical.CriticalComponent;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * Logger Code 84
 *
 * each message id must be 6 digits long starting with 84, the 3rd digit donates the level so
 *
 * INF0  1
 * WARN  2
 * DEBUG 3
 * ERROR 4
 * TRACE 5
 * FATAL 6
 *
 * so an INFO message would be 841000 to 841999
 */
@MessageLogger(projectCode = "AMQ")
public interface LoggingActiveMQServerPluginLogger extends BasicLogger {

   /**
    * The LoggingPlugin logger.
    */
   LoggingActiveMQServerPluginLogger LOGGER = Logger.getMessageLogger(LoggingActiveMQServerPluginLogger.class, LoggingActiveMQServerPluginLogger.class.getPackage().getName());

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 841000, value = "created connection: {0}", format = Message.Format.MESSAGE_FORMAT)
   void afterCreateConnection(RemotingConnection connection);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 841001, value = "destroyed connection: {0}", format = Message.Format.MESSAGE_FORMAT)
   void afterDestroyConnection(RemotingConnection connection);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 841002, value = "created session name: {0}, session connectionID: {1}", format = Message.Format.MESSAGE_FORMAT)
   void afterCreateSession(String sessionName, Object sesssionConnectionID);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 841003, value = "closed session with session name: {0}, failed: {1}", format = Message.Format.MESSAGE_FORMAT)
   void afterCloseSession(String sessionName, boolean sesssionConnectionID);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 841004, value = "added session metadata for session name : {0}, key: {1}, data: {2}", format = Message.Format.MESSAGE_FORMAT)
   void afterSessionMetadataAdded(String sessionName, String key, String data);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 841005, value = "created consumer with ID: {0}, with session name: {1}", format = Message.Format.MESSAGE_FORMAT)
   void afterCreateConsumer(String consumerID, String sessionID);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 841006, value = "closed consumer ID: {0}, with  consumer Session: {1}, failed: {2}", format = Message.Format.MESSAGE_FORMAT)
   void afterCloseConsumer(String consumerID, String sessionID, boolean failed);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 841007, value = "created queue: {0}", format = Message.Format.MESSAGE_FORMAT)
   void afterCreateQueue(Queue queue);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 841008, value = "destroyed queue: {0}, with args address: {1}, session: {2}, checkConsumerCount: {3}," +
      " removeConsumers: {4}, autoDeleteAddress: {5}", format = Message.Format.MESSAGE_FORMAT)
   void afterDestroyQueue(Queue queue,
                          SimpleString address,
                          SecurityAuth session,
                          boolean checkConsumerCount,
                          boolean removeConsumers,
                          boolean autoDeleteAddress);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 841009, value = "sent message with ID: {0}, session name: {1}, session connectionID: {2}, result: {3}", format = Message.Format.MESSAGE_FORMAT)
   void afterSend(String messageID, String sessionName, String sessionConnectionID, RoutingStatus result);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 841010, value = "routed message with ID: {0}, result: {1}", format = Message.Format.MESSAGE_FORMAT)
   void afterMessageRoute(String messageID, RoutingStatus result);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 841011, value = "delivered message with message ID: {0}, consumer info UNAVAILABLE", format = Message.Format.MESSAGE_FORMAT)
   void afterDeliverNoConsumer(String messageID);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 841012, value = "delivered message with message ID: {0}, to consumer on address: {1}, queue: {2}," +
      " consumer sessionID: {3}, consumerID: {4}", format = Message.Format.MESSAGE_FORMAT)
   void afterDeliver(String messageID,
                     SimpleString queueAddress,
                     SimpleString queueName,
                     String consumerSessionID,
                     long consumerID);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 841013, value = "expired message: {0}, messageExpiryAddress: {1}", format = Message.Format.MESSAGE_FORMAT)
   void messageExpired(MessageReference message, SimpleString messageExpiryAddress);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 841014, value = "acknowledged message ID: {0}, with messageRef consumerID: {1}, messageRef QueueName: {2}," +
      "  with ackReason: {3}", format = Message.Format.MESSAGE_FORMAT)
   void messageAcknowledged(String messageID, String consumerID, String queueName, AckReason reason);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 841015, value = "deployed bridge: {0}", format = Message.Format.MESSAGE_FORMAT)
   void afterDeployBridge(Bridge config);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 841016, value = "criticalFailure called with criticalComponent: {0}", format = Message.Format.MESSAGE_FORMAT)
   void criticalFailure(CriticalComponent components);

   //DEBUG messages

   @LogMessage(level = Logger.Level.DEBUG)
   @Message(id = 843000, value = "beforeCreateSession called with name: {0}, username: {1}, minLargeMessageSize: {2}, connection: {3},"
      + " autoCommitSends: {4}, autoCommitAcks: {5}, preAcknowledge: {6}, xa: {7}, publicAddress: {8}, context: {9}",
      format = Message.Format.MESSAGE_FORMAT)
   void beforeCreateSession(String name,
                            String username,
                            int minLargeMessageSize,
                            RemotingConnection connection,
                            boolean autoCommitSends,
                            boolean autoCommitAcks,
                            boolean preAcknowledge,
                            boolean xa,
                            String publicAddress,
                            OperationContext context);

   @LogMessage(level = Logger.Level.DEBUG)
   @Message(id = 843001, value = "beforeCloseSession called with session name : {0}, session: {1}, failed: {2}", format = Message.Format.MESSAGE_FORMAT)
   void beforeCloseSession(String sessionName, ServerSession session, boolean failed);

   @LogMessage(level = Logger.Level.DEBUG)
   @Message(id = 843002, value = "beforeSessionMetadataAdded called with session name: {0} , session: {1}, key: {2}," +
      " data: {3}", format = Message.Format.MESSAGE_FORMAT)
   void beforeSessionMetadataAdded(String sessionName, ServerSession session, String key, String data);

   @LogMessage(level = Logger.Level.DEBUG)
   @Message(id = 843003, value = "added session metadata for session name : {0}, session: {1}, key: {2}, data: {3}",
      format = Message.Format.MESSAGE_FORMAT)
   void afterSessionMetadataAddedDetails(String sessionName, ServerSession session, String key, String data);

   @LogMessage(level = Logger.Level.DEBUG)
   @Message(id = 843004, value = "beforeCreateConsumer called with ConsumerID: {0}, QueueBinding: {1}, filterString: {2}," +
      " browseOnly: {3}, supportLargeMessage: {4}", format = Message.Format.MESSAGE_FORMAT)
   void beforeCreateConsumer(String consumerID,
                             QueueBinding queueBinding,
                             SimpleString filterString,
                             boolean browseOnly,
                             boolean supportLargeMessage);

   @LogMessage(level = Logger.Level.DEBUG)
   @Message(id = 843005, value = "beforeCloseConsumer called with consumer: {0}, consumer sessionID: {1}, failed: {2}",
      format = Message.Format.MESSAGE_FORMAT)
   void beforeCloseConsumer(ServerConsumer consumer, String sessionID, boolean failed);

   @LogMessage(level = Logger.Level.DEBUG)
   @Message(id = 843006, value = "beforeCreateQueue called with queueConfig: {0}", format = Message.Format.MESSAGE_FORMAT)
   void beforeCreateQueue(QueueConfig queueConfig);

   @LogMessage(level = Logger.Level.DEBUG)
   @Message(id = 843007, value = "beforeDestroyQueue called with queueName: {0}, session: {1}, checkConsumerCount: {2}," +
      " removeConsumers: {3}, autoDeleteAddress: {4}", format = Message.Format.MESSAGE_FORMAT)
   void beforeDestroyQueue(SimpleString queueName,
                           SecurityAuth session,
                           boolean checkConsumerCount,
                           boolean removeConsumers,
                           boolean autoDeleteAddress);

   @LogMessage(level = Logger.Level.DEBUG)
   @Message(id = 843008, value = "beforeSend called with message: {0}, tx: {1}, session: {2}, direct: {3}," +
      " noAutoCreateQueue: {4}", format = Message.Format.MESSAGE_FORMAT)
   void beforeSend(org.apache.activemq.artemis.api.core.Message message,
                   Transaction tx,
                   ServerSession session,
                   boolean direct,
                   boolean noAutoCreateQueue);

   @LogMessage(level = Logger.Level.DEBUG)
   @Message(id = 843009, value = "message ID: {0}, message {1}, session name: {2} with tx: {3}, session: {4}, direct: {5}," +
      " noAutoCreateQueue: {6}", format = Message.Format.MESSAGE_FORMAT)
   void afterSendDetails(String messageID,
                         org.apache.activemq.artemis.api.core.Message message,
                         String sessionName,
                         Transaction tx,
                         ServerSession session,
                         boolean direct,
                         boolean noAutoCreateQueue);

   @LogMessage(level = Logger.Level.DEBUG)
   @Message(id = 843010, value = "beforeMessageRoute called with message: {0}, context: {1}, direct: {2}, rejectDuplicates: {3}",
      format = Message.Format.MESSAGE_FORMAT)
   void beforeMessageRoute(org.apache.activemq.artemis.api.core.Message message,
                           RoutingContext context,
                           boolean direct,
                           boolean rejectDuplicates);

   @LogMessage(level = Logger.Level.DEBUG)
   @Message(id = 843011, value = "afterMessageRoute message: {0}, with context: {1}, direct: {2}, rejectDuplicates: {3}",
      format = Message.Format.MESSAGE_FORMAT)
   void afterMessageRouteDetails(org.apache.activemq.artemis.api.core.Message message,
                                 RoutingContext context,
                                 boolean direct,
                                 boolean rejectDuplicates);

   @LogMessage(level = Logger.Level.DEBUG)
   @Message(id = 843012, value = "beforeDeliver called with consumer: {0}, reference: {1}", format = Message.Format.MESSAGE_FORMAT)
   void beforeDeliver(ServerConsumer consumer, MessageReference reference);

   @LogMessage(level = Logger.Level.DEBUG)
   @Message(id = 843013, value = "delivered message with message ID: {0} to consumer on address: {1}, queue: {2}, consumer sessionID: {3}," +
      " consumerID: {4}, full message reference: {5}, full consumer: {6}", format = Message.Format.MESSAGE_FORMAT)
   void afterDeliverDetails(String messageID,
                            SimpleString queueAddress,
                            SimpleString queueName,
                            String consumerSessionID,
                            long consumerID,
                            MessageReference reference,
                            ServerConsumer consumer);

   @LogMessage(level = Logger.Level.DEBUG)
   @Message(id = 843014, value = "acknowledged message: {0}, with ackReason: {1}", format = Message.Format.MESSAGE_FORMAT)
   void messageAcknowledgedDetails(MessageReference ref, AckReason reason);

   @LogMessage(level = Logger.Level.DEBUG)
   @Message(id = 843015, value = "beforeDeployBridge called with bridgeConfiguration: {0}", format = Message.Format.MESSAGE_FORMAT)
   void beforeDeployBridge(BridgeConfiguration config);

}
