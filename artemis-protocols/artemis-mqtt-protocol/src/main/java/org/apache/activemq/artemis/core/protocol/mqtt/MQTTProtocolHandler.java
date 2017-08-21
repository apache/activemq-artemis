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
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnAckVariableHeader;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubAckPayload;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttUnsubAckMessage;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import io.netty.util.ReferenceCountUtil;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.spi.core.protocol.ConnectionEntry;

/**
 * This class is responsible for receiving and sending MQTT packets, delegating behaviour to one of the
 * MQTTConnectionManager, MQTTPublishManager, MQTTSubscriptionManager classes.
 */
public class MQTTProtocolHandler extends ChannelInboundHandlerAdapter {

   private ConnectionEntry connectionEntry;

   private MQTTConnection connection;

   private MQTTSession session;

   private ActiveMQServer server;

   private MQTTProtocolManager protocolManager;

   // This Channel Handler is not sharable, therefore it can only ever be associated with a single ctx.
   private ChannelHandlerContext ctx;

   private final MQTTLogger log = MQTTLogger.LOGGER;

   private boolean stopped = false;

   public MQTTProtocolHandler(ActiveMQServer server, MQTTProtocolManager protocolManager) {
      this.server = server;
      this.protocolManager = protocolManager;
   }

   void setConnection(MQTTConnection connection, ConnectionEntry entry) throws Exception {
      this.connectionEntry = entry;
      this.connection = connection;
      this.session = new MQTTSession(this, connection, protocolManager, server.getConfiguration().getWildcardConfiguration());
   }

   void stop() {
      stopped = true;
   }

   @Override
   public void channelRead(ChannelHandlerContext ctx, Object msg) {
      try {
         if (stopped) {
            disconnect(true);
            return;
         }

         MqttMessage message = (MqttMessage) msg;

         // Disconnect if Netty codec failed to decode the stream.
         if (message.decoderResult().isFailure()) {
            log.debug("Bad Message Disconnecting Client.");
            disconnect(true);
            return;
         }

         connection.dataReceived();

         MQTTUtil.logMessage(session.getState(), message, true);

         this.protocolManager.invokeIncoming(message, this.connection);

         switch (message.fixedHeader().messageType()) {
            case CONNECT:
               handleConnect((MqttConnectMessage) message, ctx);
               break;
            case PUBLISH:
               handlePublish((MqttPublishMessage) message);
               break;
            case PUBACK:
               handlePuback((MqttPubAckMessage) message);
               break;
            case PUBREC:
               handlePubrec(message);
               break;
            case PUBREL:
               handlePubrel(message);
               break;
            case PUBCOMP:
               handlePubcomp(message);
               break;
            case SUBSCRIBE:
               handleSubscribe((MqttSubscribeMessage) message);
               break;
            case UNSUBSCRIBE:
               handleUnsubscribe((MqttUnsubscribeMessage) message);
               break;
            case PINGREQ:
               handlePingreq();
               break;
            case DISCONNECT:
               disconnect(false);
               break;
            case UNSUBACK:
            case SUBACK:
            case PINGRESP:
            case CONNACK: // The server does not instantiate connections therefore any CONNACK received over a connection is an invalid control message.
            default:
               disconnect(true);
         }
      } catch (Exception e) {
         log.debug("Error processing Control Packet, Disconnecting Client", e);
         disconnect(true);
      } finally {
         ReferenceCountUtil.release(msg);
      }
   }

   /**
    * Called during connection.
    *
    * @param connect
    */
   void handleConnect(MqttConnectMessage connect, ChannelHandlerContext ctx) throws Exception {
      this.ctx = ctx;
      connectionEntry.ttl = connect.variableHeader().keepAliveTimeSeconds() * 1500L;

      String clientId = connect.payload().clientIdentifier();
      session.getConnectionManager().connect(clientId, connect.payload().userName(), connect.payload().passwordInBytes(), connect.variableHeader().isWillFlag(), connect.payload().willMessageInBytes(), connect.payload().willTopic(), connect.variableHeader().isWillRetain(), connect.variableHeader().willQos(), connect.variableHeader().isCleanSession());
   }

   void disconnect(boolean error) {
      session.getConnectionManager().disconnect(error);
   }

   void sendConnack(MqttConnectReturnCode returnCode) {
      MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
      MqttConnAckVariableHeader varHeader = new MqttConnAckVariableHeader(returnCode, true);
      MqttConnAckMessage message = new MqttConnAckMessage(fixedHeader, varHeader);
      sendToClient(message);
   }

   void handlePublish(MqttPublishMessage message) throws Exception {
      session.getMqttPublishManager().handleMessage(message.variableHeader().packetId(), message.variableHeader().topicName(), message.fixedHeader().qosLevel().value(), message.payload(), message.fixedHeader().isRetain());
   }

   void sendPubAck(int messageId) {
      sendPublishProtocolControlMessage(messageId, MqttMessageType.PUBACK);
   }

   void sendPubRel(int messageId) {
      sendPublishProtocolControlMessage(messageId, MqttMessageType.PUBREL);
   }

   void sendPubRec(int messageId) {
      sendPublishProtocolControlMessage(messageId, MqttMessageType.PUBREC);
   }

   void sendPubComp(int messageId) {
      sendPublishProtocolControlMessage(messageId, MqttMessageType.PUBCOMP);
   }

   void sendPublishProtocolControlMessage(int messageId, MqttMessageType messageType) {
      MqttQoS qos = (messageType == MqttMessageType.PUBREL) ? MqttQoS.AT_LEAST_ONCE : MqttQoS.AT_MOST_ONCE;
      MqttFixedHeader fixedHeader = new MqttFixedHeader(messageType, false, qos, // Spec requires 01 in header for rel
                                                        false, 0);
      MqttPubAckMessage rel = new MqttPubAckMessage(fixedHeader, MqttMessageIdVariableHeader.from(messageId));
      sendToClient(rel);
   }

   void handlePuback(MqttPubAckMessage message) throws Exception {
      session.getMqttPublishManager().handlePubAck(getMessageId(message));
   }

   void handlePubrec(MqttMessage message) throws Exception {
      session.getMqttPublishManager().handlePubRec(getMessageId(message));
   }

   void handlePubrel(MqttMessage message) {
      session.getMqttPublishManager().handlePubRel(getMessageId(message));
   }

   void handlePubcomp(MqttMessage message) throws Exception {
      session.getMqttPublishManager().handlePubComp(getMessageId(message));
   }

   void handleSubscribe(MqttSubscribeMessage message) throws Exception {
      MQTTSubscriptionManager subscriptionManager = session.getSubscriptionManager();
      int[] qos = subscriptionManager.addSubscriptions(message.payload().topicSubscriptions());

      MqttFixedHeader header = new MqttFixedHeader(MqttMessageType.SUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
      MqttSubAckMessage ack = new MqttSubAckMessage(header, message.variableHeader(), new MqttSubAckPayload(qos));
      sendToClient(ack);
   }

   void handleUnsubscribe(MqttUnsubscribeMessage message) throws Exception {
      session.getSubscriptionManager().removeSubscriptions(message.payload().topics());
      MqttFixedHeader header = new MqttFixedHeader(MqttMessageType.UNSUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
      MqttUnsubAckMessage m = new MqttUnsubAckMessage(header, message.variableHeader());
      sendToClient(m);
   }

   void handlePingreq() {
      MqttMessage pingResp = new MqttMessage(new MqttFixedHeader(MqttMessageType.PINGRESP, false, MqttQoS.AT_MOST_ONCE, false, 0));
      sendToClient(pingResp);
   }

   protected void send(int messageId, String topicName, int qosLevel, boolean isRetain, ByteBuf payload, int deliveryCount) {
      boolean redelivery = qosLevel == 0 ? false : (deliveryCount > 0);
      MqttFixedHeader header = new MqttFixedHeader(MqttMessageType.PUBLISH, redelivery, MqttQoS.valueOf(qosLevel), isRetain, 0);
      MqttPublishVariableHeader varHeader = new MqttPublishVariableHeader(topicName, messageId);
      MqttMessage publish = new MqttPublishMessage(header, varHeader, payload);
      sendToClient(publish);
   }

   private void sendToClient(MqttMessage message) {
      MQTTUtil.logMessage(session.getSessionState(), message, false);
      this.protocolManager.invokeOutgoing(message, connection);
      ctx.write(message);
      ctx.flush();
   }

   private int getMessageId(MqttMessage message) {
      return ((MqttMessageIdVariableHeader) message.variableHeader()).messageId();
   }

   ActiveMQServer getServer() {
      return server;
   }
}
