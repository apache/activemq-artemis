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

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.buffers.impl.ChannelBufferWrapper;
import org.apache.activemq.artemis.core.server.ServerMessage;
import org.apache.activemq.artemis.core.server.impl.ServerMessageImpl;

/**
 * A Utility Class for creating Server Side objects and converting MQTT concepts to/from Artemis.
 */

public class MQTTUtil {

   // TODO These settings should be configurable.
   public static final int DEFAULT_SERVER_MESSAGE_BUFFER_SIZE = 512;

   public static final boolean DURABLE_MESSAGES = true;

   public static final boolean SESSION_AUTO_COMMIT_SENDS = true;

   public static final boolean SESSION_AUTO_COMMIT_ACKS = false;

   public static final boolean SESSION_PREACKNOWLEDGE = false;

   public static final boolean SESSION_XA = false;

   public static final boolean SESSION_AUTO_CREATE_QUEUE = false;

   public static final int MAX_MESSAGE_SIZE = 268435455;

   public static final String MQTT_RETAIN_ADDRESS_PREFIX = "$sys.mqtt.retain.";

   public static final String MQTT_QOS_LEVEL_KEY = "mqtt.qos.level";

   public static final String MQTT_MESSAGE_ID_KEY = "mqtt.message.id";

   public static final String MQTT_MESSAGE_TYPE_KEY = "mqtt.message.type";

   public static final String MQTT_MESSAGE_RETAIN_KEY = "mqtt.message.retain";

   public static final int DEFAULT_KEEP_ALIVE_FREQUENCY = 5000;

   public static String convertMQTTAddressFilterToCore(String filter) {
      return swapMQTTAndCoreWildCards(filter);
   }

   public static String convertCoreAddressFilterToMQTT(String filter) {
      if (filter.startsWith(MQTT_RETAIN_ADDRESS_PREFIX)) {
         filter = filter.substring(MQTT_RETAIN_ADDRESS_PREFIX.length(), filter.length());
      }
      return swapMQTTAndCoreWildCards(filter);
   }

   public static String convertMQTTAddressFilterToCoreRetain(String filter) {
      return MQTT_RETAIN_ADDRESS_PREFIX + swapMQTTAndCoreWildCards(filter);
   }

   public static String swapMQTTAndCoreWildCards(String filter) {
      char[] topicFilter = filter.toCharArray();
      for (int i = 0; i < topicFilter.length; i++) {
         switch (topicFilter[i]) {
            case '/':
               topicFilter[i] = '.';
               break;
            case '.':
               topicFilter[i] = '/';
               break;
            case '*':
               topicFilter[i] = '+';
               break;
            case '+':
               topicFilter[i] = '*';
               break;
            default:
               break;
         }
      }
      return String.valueOf(topicFilter);
   }

   private static ServerMessage createServerMessage(MQTTSession session,
                                                    SimpleString address,
                                                    boolean retain,
                                                    int qos) {
      long id = session.getServer().getStorageManager().generateID();

      ServerMessageImpl message = new ServerMessageImpl(id, DEFAULT_SERVER_MESSAGE_BUFFER_SIZE);
      message.setAddress(address);
      message.putBooleanProperty(new SimpleString(MQTT_MESSAGE_RETAIN_KEY), retain);
      message.putIntProperty(new SimpleString(MQTT_QOS_LEVEL_KEY), qos);
      // For JMS Consumption
      message.setType(Message.BYTES_TYPE);
      return message;
   }

   public static ServerMessage createServerMessageFromByteBuf(MQTTSession session,
                                                              String topic,
                                                              boolean retain,
                                                              int qos,
                                                              ByteBuf payload) {
      String coreAddress = convertMQTTAddressFilterToCore(topic);
      ServerMessage message = createServerMessage(session, new SimpleString(coreAddress), retain, qos);

      // FIXME does this involve a copy?
      message.getBodyBuffer().writeBytes(new ChannelBufferWrapper(payload), payload.readableBytes());
      return message;
   }

   public static ServerMessage createServerMessageFromString(MQTTSession session,
                                                             String payload,
                                                             String topic,
                                                             int qos,
                                                             boolean retain) {
      ServerMessage message = createServerMessage(session, new SimpleString(topic), retain, qos);
      message.getBodyBuffer().writeString(payload);
      return message;
   }

   public static ServerMessage createPubRelMessage(MQTTSession session, SimpleString address, int messageId) {
      ServerMessage message = createServerMessage(session, address, false, 1);
      message.putIntProperty(new SimpleString(MQTTUtil.MQTT_MESSAGE_ID_KEY), messageId);
      message.putIntProperty(new SimpleString(MQTTUtil.MQTT_MESSAGE_TYPE_KEY), MqttMessageType.PUBREL.value());
      return message;
   }

   public static void logMessage(MQTTLogger logger, MqttMessage message, boolean inbound) {
      StringBuilder log = inbound ? new StringBuilder("Received ") : new StringBuilder("Sent ");

      if (message.fixedHeader() != null) {
         log.append(message.fixedHeader().messageType().toString());

         if (message.variableHeader() instanceof MqttPublishVariableHeader) {
            log.append("(" + ((MqttPublishVariableHeader) message.variableHeader()).messageId() + ") " + message.fixedHeader().qosLevel());
         } else if (message.variableHeader() instanceof MqttMessageIdVariableHeader) {
            log.append("(" + ((MqttMessageIdVariableHeader) message.variableHeader()).messageId() + ")");
         }

         if (message.fixedHeader().messageType() == MqttMessageType.SUBSCRIBE) {
            for (MqttTopicSubscription sub : ((MqttSubscribeMessage) message).payload().topicSubscriptions()) {
               log.append("\n\t" + sub.topicName() + " : " + sub.qualityOfService());
            }
         }

         logger.debug(log.toString());
      }
   }

}
