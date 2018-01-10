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

import java.nio.charset.StandardCharsets;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttConnAckVariableHeader;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.netty.handler.codec.mqtt.MqttConnectVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
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

   public static final SimpleString MQTT_QOS_LEVEL_KEY = SimpleString.toSimpleString("mqtt.qos.level");

   public static final SimpleString MQTT_MESSAGE_ID_KEY = SimpleString.toSimpleString("mqtt.message.id");

   public static final SimpleString MQTT_MESSAGE_TYPE_KEY = SimpleString.toSimpleString("mqtt.message.type");

   public static final SimpleString MQTT_MESSAGE_RETAIN_KEY = SimpleString.toSimpleString("mqtt.message.retain");

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
      if (filter == null) {
         return "";
      }

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

      CoreMessage message = new CoreMessage(id, DEFAULT_SERVER_MESSAGE_BUFFER_SIZE, session.getCoreMessageObjectPools());
      message.setAddress(address);
      message.putBooleanProperty(MQTT_MESSAGE_RETAIN_KEY, retain);
      message.putIntProperty(MQTT_QOS_LEVEL_KEY, qos);
      message.setType(Message.BYTES_TYPE);
      return message;
   }

   public static Message createServerMessageFromByteBuf(MQTTSession session,
                                                              String topic,
                                                              boolean retain,
                                                              int qos,
                                                              ByteBuf payload) {
      String coreAddress = convertMQTTAddressFilterToCore(topic, session.getWildcardConfiguration());
      SimpleString address = SimpleString.toSimpleString(coreAddress, session.getCoreMessageObjectPools().getAddressStringSimpleStringPool());
      ICoreMessage message = createServerMessage(session, address, retain, qos);

      message.getBodyBuffer().writeBytes(payload, 0, payload.readableBytes());
      return message;
   }

   public static Message createPubRelMessage(MQTTSession session, SimpleString address, int messageId) {
      Message message = createServerMessage(session, address, false, 1);
      message.putIntProperty(MQTTUtil.MQTT_MESSAGE_ID_KEY, messageId);
      message.putIntProperty(MQTTUtil.MQTT_MESSAGE_TYPE_KEY, MqttMessageType.PUBREL.value());
      return message;
   }

   public static void logMessage(MQTTSessionState state, MqttMessage message, boolean inbound) {
      if (logger.isTraceEnabled()) {
         traceMessage(state, message, inbound);
      }
   }

   public static void traceMessage(MQTTSessionState state, MqttMessage message, boolean inbound) {
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

         if (message.variableHeader() instanceof MqttMessageIdVariableHeader) {
            log.append("(" + ((MqttMessageIdVariableHeader) message.variableHeader()).messageId() + ")");
         }

         switch (message.fixedHeader().messageType()) {
            case PUBLISH:
               MqttPublishVariableHeader publishHeader = (MqttPublishVariableHeader) message.variableHeader();
               String publishPayload = ((MqttPublishMessage)message).payload().toString(StandardCharsets.UTF_8);
               final int maxPayloadLogSize = 256;
               log.append("(" + publishHeader.packetId() + ")")
                  .append(" topic=" + publishHeader.topicName())
                  .append(", qos=" + message.fixedHeader().qosLevel())
                  .append(", retain=" + message.fixedHeader().isRetain())
                  .append(", dup=" + message.fixedHeader().isDup())
                  .append(", payload=" + (publishPayload.length() > maxPayloadLogSize ? publishPayload.substring(0, maxPayloadLogSize) : publishPayload));
               break;
            case CONNECT:
               MqttConnectVariableHeader connectHeader = (MqttConnectVariableHeader) message.variableHeader();
               MqttConnectPayload payload = ((MqttConnectMessage)message).payload();
               log.append(" protocol=(").append(connectHeader.name()).append(", ").append(connectHeader.version()).append(")")
                  .append(", hasPassword=").append(connectHeader.hasPassword())
                  .append(", isCleanSession=").append(connectHeader.isCleanSession())
                  .append(", keepAliveTimeSeconds=").append(connectHeader.keepAliveTimeSeconds())
                  .append(", clientIdentifier=").append(payload.clientIdentifier())
                  .append(", hasUserName=").append(connectHeader.hasUserName());
               if (connectHeader.hasUserName()) {
                  log.append(", userName=").append(payload.userName());
               }
               log.append(", isWillFlag=").append(connectHeader.isWillFlag());
               if (connectHeader.isWillFlag()) {
                  log.append(", willQos=").append(connectHeader.willQos())
                     .append(", isWillRetain=").append(connectHeader.isWillRetain())
                     .append(", willTopic=").append(payload.willTopic());
               }
               break;
            case CONNACK:
               MqttConnAckVariableHeader connackHeader = (MqttConnAckVariableHeader) message.variableHeader();
               log.append(" connectReturnCode=").append(connackHeader.connectReturnCode().byteValue())
                  .append(", sessionPresent=").append(connackHeader.isSessionPresent());
               break;
            case SUBSCRIBE:
               for (MqttTopicSubscription sub : ((MqttSubscribeMessage) message).payload().topicSubscriptions()) {
                  log.append("\n\t" + sub.topicName() + " : " + sub.qualityOfService());
               }
               break;
            case SUBACK:
               for (Integer qos : ((MqttSubAckMessage) message).payload().grantedQoSLevels()) {
                  log.append("\n\t" + qos);
               }
               break;
            case UNSUBSCRIBE:
               for (String topic : ((MqttUnsubscribeMessage) message).payload().topics()) {
                  log.append("\n\t" + topic);
               }
               break;
         }

         logger.trace(log.toString());
      }
   }
}
