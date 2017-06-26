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
package org.apache.activemq.artemis.jms.bridge;

import javax.management.ObjectName;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * Logger Code 34
 *
 * each message id must be 6 digits long starting with 12, the 3rd digit donates the level so
 *
 * INF0  1
 * WARN  2
 * DEBUG 3
 * ERROR 4
 * TRACE 5
 * FATAL 6
 *
 * so an INFO message would be 341000 to 341999
 */
@MessageLogger(projectCode = "AMQ")
public interface ActiveMQJMSBridgeLogger extends BasicLogger {

   /**
    * The default logger.
    */
   ActiveMQJMSBridgeLogger LOGGER = Logger.getMessageLogger(ActiveMQJMSBridgeLogger.class, ActiveMQJMSBridgeLogger.class.getPackage().getName());

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 341000, value = "Failed to set up JMS bridge {1} connections. Most probably the source or target servers are unavailable." + " Will retry after a pause of {0} ms", format = Message.Format.MESSAGE_FORMAT)
   void failedToSetUpBridge(long failureRetryInterval, String bridgeName);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 341001, value = "JMS Bridge {0} succeeded in reconnecting to servers", format = Message.Format.MESSAGE_FORMAT)
   void bridgeReconnected(String bridgeName);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 341002, value = "JMSBridge {0} succeeded in connecting to servers", format = Message.Format.MESSAGE_FORMAT)
   void bridgeConnected(String bridgeName);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 342000, value = "Attempt to start JMS Bridge {0}, but is already started", format = Message.Format.MESSAGE_FORMAT)
   void errorBridgeAlreadyStarted(String bridgeName);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 342001, value = "Failed to start JMS Bridge {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorStartingBridge(String bridgeName);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 342002, value = "Failed to unregisted JMS Bridge {0} - {1}", format = Message.Format.MESSAGE_FORMAT)
   void errorUnregisteringBridge(ObjectName objectName, String bridgeName);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 342003, value = "JMS Bridge {0} unable to set up connections, bridge will be stopped", format = Message.Format.MESSAGE_FORMAT)
   void errorConnectingBridge(String bridgeName);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 342004, value = "JMS Bridge {1}, will retry after a pause of {0} ms", format = Message.Format.MESSAGE_FORMAT)
   void bridgeRetry(long failureRetryInterval, String bridgeName);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 342005, value = "JMS Bridge {0} unable to set up connections, bridge will not be started", format = Message.Format.MESSAGE_FORMAT)
   void bridgeNotStarted(String bridgeName);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 342006, value = "JMS Bridge {0}, detected failure on bridge connection", format = Message.Format.MESSAGE_FORMAT)
   void bridgeFailure(@Cause Exception e, String bridgeName);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 342009, value = "JMS Bridge {0} failed to send + acknowledge batch, closing JMS objects", format = Message.Format.MESSAGE_FORMAT)
   void bridgeAckError(@Cause Exception e, String bridgeName);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 342010, value = "Failed to connect JMS Bridge {0}", format = Message.Format.MESSAGE_FORMAT)
   void bridgeConnectError(@Cause Exception e, String bridgeName);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 342011, value = "Transaction rolled back, retrying TX", format = Message.Format.MESSAGE_FORMAT)
   void transactionRolledBack(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 344001, value = "JMS Bridge {0}, failed to start source connection", format = Message.Format.MESSAGE_FORMAT)
   void jmsBridgeSrcConnectError(@Cause Exception e, String bridgeName);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 344002, value = "Failed to start JMS Bridge {1}.  QoS Mode: {0} requires a Transaction Manager, none found", format = Message.Format.MESSAGE_FORMAT)
   void jmsBridgeTransactionManagerMissing(QualityOfServiceMode qosMode, String bridgeName);
}
