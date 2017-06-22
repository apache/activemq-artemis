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
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.WildcardConfiguration;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;

/**
 * A Utility Class for creating Server Side objects and converting MQTT concepts to/from Artemis.
 */

public class MQTTUtil {

   // TODO These settings should be configurable.
   public static final int DEFAULT_SERVER_MESSAGE_BUFFER_SIZE = 512;

   public static final boolean DURABLE_MESSAGES = true;

   public static final boolean SESSION_AUTO_COMMIT_SENDS = true;

   public static final boolean SESSION_AUTO_COMMIT_ACKS = true;

   public static final boolean SESSION_PREACKNOWLEDGE = false;

   public static final boolean SESSION_XA = false;

   public static final boolean SESSION_AUTO_CREATE_QUEUE = false;

   public static final int MAX_MESSAGE_SIZE = 268435455;

   public static final String MQTT_RETAIN_ADDRESS_PREFIX = "$sys.mqtt.retain.";

   public static final String MQTT_QOS_LEVEL_KEY = "mqtt.qos.level";

   public static final String MQTT_MESSAGE_ID_KEY = "mqtt.message.id";

   public static final String MQTT_MESSAGE_TYPE_KEY = "mqtt.message.type";

   public static final SimpleString MQTT_MESSAGE_RETAIN_KEY = new SimpleString("mqtt.message.retain");

   public static final String MANAGEMENT_QUEUE_PREFIX = "$sys.mqtt.queue.qos2.";

   public static final int DEFAULT_KEEP_ALIVE_FREQUENCY = 5000;

   public static String convertMQTTAddressFilterToCore(String filter, WildcardConfiguration wildcardConfiguration) {
      return MQTT_WILDCARD.convert(filter, wildcardConfiguration);
   }

   public static class MQTTWildcardConfiguration extends WildcardConfiguration {
      public MQTTWildcardConfiguration() {
         setDelimiter('/');
         setSingleWord('+');
         setAnyWords('#');
      }
   }

   public static final WildcardConfiguration MQTT_WILDCARD = new MQTTWildcardConfiguration();

   private static final MQTTLogger logger = MQTTLogger.LOGGER;

   public static String convertCoreAddressFilterToMQTT(String filter, WildcardConfiguration wildcardConfiguration) {
      if (filter.startsWith(MQTT_RETAIN_ADDRESS_PREFIX)) {
         filter = filter.substring(MQTT_RETAIN_ADDRESS_PREFIX.length(), filter.length());
      }
      return wildcardConfiguration.convert(filter, MQTT_WILDCARD);
   }

   public static String convertMQTTAddressFilterToCoreRetain(String filter, WildcardConfiguration wildcardConfiguration) {
      return MQTT_RETAIN_ADDRESS_PREFIX + MQTT_WILDCARD.convert(filter, wildcardConfiguration);
   }

   private static ICoreMessage createServerMessage(MQTTSession session,
                                                    SimpleString address,
                                                    boolean retain,
                                                    int qos) {
      long id = session.getServer().getStorageManager().generateID();

      CoreMessage message = new CoreMessage(id, DEFAULT_SERVER_MESSAGE_BUFFER_SIZE);
      message.setAddress(address);
      message.putBooleanProperty(MQTT_MESSAGE_RETAIN_KEY, retain);
      message.putIntProperty(new SimpleString(MQTT_QOS_LEVEL_KEY), qos);
      message.setType(Message.BYTES_TYPE);
      return message;
   }

   public static Message createServerMessageFromByteBuf(MQTTSession session,
                                                              String topic,
                                                              boolean retain,
                                                              int qos,
                                                              ByteBuf payload) {
      String coreAddress = convertMQTTAddressFilterToCore(topic, session.getWildcardConfiguration());
      ICoreMessage message = createServerMessage(session, new SimpleString(coreAddress), retain, qos);

      message.getBodyBuffer().writeBytes(payload, 0, payload.readableBytes());
      return message;
   }

   public static Message createPubRelMessage(MQTTSession session, SimpleString address, int messageId) {
      Message message = createServerMessage(session, address, false, 1);
      message.putIntProperty(new SimpleString(MQTTUtil.MQTT_MESSAGE_ID_KEY), messageId);
      message.putIntProperty(new SimpleString(MQTTUtil.MQTT_MESSAGE_TYPE_KEY), MqttMessageType.PUBREL.value());
      return message;
   }

   public static void logMessage(MQTTSessionState state, MqttMessage message, boolean inbound) {
      if (logger.isTraceEnabled()) {

         StringBuilder log = new StringBuilder("MQTT(");

         if (state != null) {
            log.append(state.getClientId());
         }

         if (inbound) {
            log.append("): IN << ");
         } else {
            log.append("): OUT >> ");
         }

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

            logger.trace(log.toString());
         }
      }
   }

}
