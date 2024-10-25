/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.core.protocol.mqtt;

import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.logs.annotation.LogBundle;
import org.apache.activemq.artemis.logs.annotation.LogMessage;
import org.apache.activemq.artemis.logs.BundleFactory;

/**
 * Logger Codes 830000 - 839999
 */
@LogBundle(projectCode = "AMQ", regexID = "83[0-9]{4}")
public interface MQTTLogger {

   MQTTLogger LOGGER = BundleFactory.newBundle(MQTTLogger.class, MQTTLogger.class.getPackage().getName());

   @LogMessage(id = 832000, value = "Unable to send message: {}", level = LogMessage.Level.WARN)
   void unableToSendMessage(MessageReference message, Exception e);

   @LogMessage(id = 832001, value = "MQTT client({}) attempted to ack already ack'd message: ", level = LogMessage.Level.WARN)
   void failedToAckMessage(String clientId, Exception e);

   @LogMessage(id = 834000, value = "Error removing subscription.", level = LogMessage.Level.ERROR)
   void errorRemovingSubscription(Exception e);

   @LogMessage(id = 834001, value = "Error disconnecting client.", level = LogMessage.Level.ERROR)
   void errorDisconnectingClient(Exception e);

   @LogMessage(id = 834002, value = "Error processing control packet: {}", level = LogMessage.Level.ERROR)
   void errorProcessingControlPacket(String packet, Exception e);

   @LogMessage(id = 834003, value = "Error sending will message.", level = LogMessage.Level.ERROR)
   void errorSendingWillMessage(Exception e);

   @LogMessage(id = 834004, value = "Error disconnecting consumer.", level = LogMessage.Level.ERROR)
   void errorDisconnectingConsumer(Exception e);

   @LogMessage(id = 834005, value = "Failed to cast property {}.", level = LogMessage.Level.ERROR)
   void failedToCastProperty(String property);

   @LogMessage(id = 834006, value = "Failed to publish MQTT message: {}.", level = LogMessage.Level.ERROR)
   void failedToPublishMqttMessage(String exceptionMessage, Throwable t);

   @LogMessage(id = 834007, value = "Authorization failure sending will message: {}", level = LogMessage.Level.ERROR)
   void authorizationFailureSendingWillMessage(String message);

   @LogMessage(id = 834008, value = "Failed to remove session state for client with ID: {}", level = LogMessage.Level.ERROR)
   void failedToRemoveSessionState(String clientID, Exception e);

   @LogMessage(id = 834009, value = "Ignoring duplicate MQTT QoS2 PUBLISH packet for packet ID {} from client with ID {}.", level = LogMessage.Level.WARN)
   void ignoringQoS2Publish(String clientId, long packetId);

   @LogMessage(id = 834010, value = "Unable to scan MQTT sessions", level = LogMessage.Level.ERROR)
   void unableToScanSessions(Exception e);

   @LogMessage(id = 834011, value = "Invalid MQTT session state message. Type is incorrect: {}. Will not load this state into memory.", level = LogMessage.Level.WARN)
   void sessionStateMessageIncorrectType(String type);

   @LogMessage(id = 834012, value = "Invalid MQTT session state message. Client ID is empty or null. Will not load this state into memory.", level = LogMessage.Level.WARN)
   void sessionStateMessageBadClientId();

   @LogMessage(id = 834013, value = "Invalid MQTT session state message. Will not load this state into memory.", level = LogMessage.Level.WARN)
   void errorDeserializingStateMessage(Exception e);
}
