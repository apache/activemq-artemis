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

package org.apache.activemq.artemis.core.protocol.mqtt;

import org.apache.activemq.artemis.core.server.MessageReference;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * Logger Code 83
 *
 * each message id must be 6 digits long starting with 83, the 3rd digit donates the level so
 *
 * INF0  1
 * WARN  2
 * DEBUG 3
 * ERROR 4
 * TRACE 5
 * FATAL 6
 *
 * so an INFO message would be 831000 to 831999
 */

@MessageLogger(projectCode = "AMQ")
public interface MQTTLogger extends BasicLogger {

   MQTTLogger LOGGER = Logger.getMessageLogger(MQTTLogger.class, MQTTLogger.class.getPackage().getName());

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 832000, value = "Unable to send message: {0}", format = Message.Format.MESSAGE_FORMAT)
   void unableToSendMessage(MessageReference message, @Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 832001, value = "MQTT client({0}) attempted to ack already ack'd message: ", format = Message.Format.MESSAGE_FORMAT)
   void failedToAckMessage(String clientId, @Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 834000, value = "Error removing subscription.", format = Message.Format.MESSAGE_FORMAT)
   void errorRemovingSubscription(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 834001, value = "Error disconnecting client.", format = Message.Format.MESSAGE_FORMAT)
   void errorDisconnectingClient(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 834002, value = "Error processing control packet: {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorProcessingControlPacket(String packet, @Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 834003, value = "Error sending will message.", format = Message.Format.MESSAGE_FORMAT)
   void errorSendingWillMessage(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 834004, value = "Error disconnecting consumer.", format = Message.Format.MESSAGE_FORMAT)
   void errorDisconnectingConsumer(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 834005, value = "Failed to cast property {0}.", format = Message.Format.MESSAGE_FORMAT)
   void failedToCastProperty(String property);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 834006, value = "Failed to publish MQTT message: {0}.", format = Message.Format.MESSAGE_FORMAT)
   void failedToPublishMqttMessage(String exceptionMessage, @Cause Throwable t);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 834007, value = "Authorization failure sending will message: {0}", format = Message.Format.MESSAGE_FORMAT)
   void authorizationFailureSendingWillMessage(String message);
}