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
package org.apache.activemq.artemis.integration.kafka.bridge;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * Logger Code 36
 *
 * each message id must be 6 digits long starting with 36, the 3rd digit donates the level so
 *
 * INF0  1
 * WARN  2
 * DEBUG 3
 * ERROR 4
 * TRACE 5
 * FATAL 6
 *
 * so an INFO message would be 361000 to 361999
 */
@MessageLogger(projectCode = "AMQ")
interface ActiveMQKafkaLogger extends BasicLogger {

   /**
    * The kafka logger.
    */
   ActiveMQKafkaLogger LOGGER = Logger.getMessageLogger(ActiveMQKafkaLogger.class, ActiveMQKafkaLogger.class.getPackage().getName());

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 364001, value = "Could not cancel reference {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorCancellingRefOnBridge(@Cause Exception e, MessageReference ref2);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 362002, value = "Bridge unable to send message {0}, will try again once bridge reconnects", format = Message.Format.MESSAGE_FORMAT)
   void bridgeUnableToSendMessage(@Cause Exception e, MessageReference ref);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 362003, value = "Bridge {0} achieved {1} maxattempts={2} it will stop retrying to reconnect", format = Message.Format.MESSAGE_FORMAT)
   void bridgeAbortStart(SimpleString name, Integer retryCount, Integer reconnectAttempts);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 364004, value = "Bridge Failed to ack", format = Message.Format.MESSAGE_FORMAT)
   void bridgeFailedToAck(@Cause Throwable t);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 362005, value = "KafkaProducerBridge :: {0} : started.", format = Message.Format.MESSAGE_FORMAT)
   void bridgeStarted(String name);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 362006, value = "KafkaProducerBridge :: {0} : connected.", format = Message.Format.MESSAGE_FORMAT)
   void bridgeConnected(String name);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 362007, value = "KafkaProducerBridge :: {0} : failed to start.", format = Message.Format.MESSAGE_FORMAT)
   void bridgeFailedToStart(@Cause Exception e, String name);

   @LogMessage(level = Logger.Level.DEBUG)
   @Message(id = 362008, value = "KafkaProducerBridge :: {0}  : received stop request.", format = Message.Format.MESSAGE_FORMAT)
   void bridgeReceivedStopRequest(String name);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 362009, value = "KafkaProducerBridge :: {0}  : stopped.", format = Message.Format.MESSAGE_FORMAT)
   void bridgeStopped(String name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 362010, value = "KafkaProducerBridge :: {0}  : disconnected.", format = Message.Format.MESSAGE_FORMAT)
   void bridgeDisconnected(String name);

}
