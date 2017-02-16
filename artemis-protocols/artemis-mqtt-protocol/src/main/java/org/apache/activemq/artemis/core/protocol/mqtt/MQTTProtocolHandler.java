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

import java.util.Map;

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
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.spi.core.protocol.ConnectionEntry;

/**
 * This class is responsible for receiving and sending MQTT packets, delegating behaviour to one of the
 * MQTTConnectionManager, MQTTPublishMananger, MQTTSubscriptionManager classes.
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

   private Map<SimpleString, RoutingType> prefixes;

   public MQTTProtocolHandler(ActiveMQServer server, MQTTProtocolManager protocolManager) {
      this.server = server;
      this.protocolManager = protocolManager;
      this.prefixes = protocolManager.getPrefixes();
   }

   void setConnection(MQTTConnection connection, ConnectionEntry entry) throws Exception {
      this.connectionEntry = entry;
      this.connection = connection;
      this.session = new MQTTSession(this, connection, protocolManager, server.getConfiguration().getWildcardConfiguration());
   }

   void stop(boolean error) {
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

         switch (message.fixedHeader().messageType()) {
            case CONNECT:
               handleConnect((MqttConnectMessage) message, ctx);
               break;
            case CONNACK:
               handleConnack((MqttConnAckMessage) message);
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
               handleSubscribe((MqttSubscribeMessage) message, ctx);
               break;
            case SUBACK:
               handleSuback((MqttSubAckMessage) message);
               break;
            case UNSUBSCRIBE:
               handleUnsubscribe((MqttUnsubscribeMessage) message);
               break;
            case UNSUBACK:
               handleUnsuback((MqttUnsubAckMessage) message);
               break;
            case PINGREQ:
               handlePingreq(message, ctx);
               break;
            case PINGRESP:
               handlePingresp(message);
               break;
            case DISCONNECT:
               handleDisconnect(message);
               break;
            default:
               disconnect(true);
         }
      } catch (Exception e) {
         log.debug("Error processing Control Packet, Disconnecting Client", e);
         disconnect(true);
      }
   }

   /**
    * Called during connection.
    *
    * @param connect
    */
   void handleConnect(MqttConnectMessage connect, ChannelHandlerContext ctx) throws Exception {
      this.ctx = ctx;
      connectionEntry.ttl = connect.variableHeader().keepAliveTimeSeconds() * 1500;

      String clientId = connect.payload().clientIdentifier();
      session.getConnectionManager().connect(clientId, connect.payload().userName(), connect.payload().password(), connect.variableHeader().isWillFlag(), connect.payload().willMessage(), connect.payload().willTopic(), connect.variableHeader().isWillRetain(), connect.variableHeader().willQos(), connect.variableHeader().isCleanSession());
   }

   void disconnect(boolean error) {
      session.getConnectionManager().disconnect(error);
   }

   void sendConnack(MqttConnectReturnCode returnCode) {
      MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
      MqttConnAckVariableHeader varHeader = new MqttConnAckVariableHeader(returnCode, true);
      MqttConnAckMessage message = new MqttConnAckMessage(fixedHeader, varHeader);

      ctx.write(message);
      ctx.flush();
   }

   /**
    * The server does not instantiate connections therefore any CONNACK received over a connection is an invalid
    * control message.
    *
    * @param message
    */
   void handleConnack(MqttConnAckMessage message) {
      log.debug("Received invalid CONNACK from client: " + session.getSessionState().getClientId());
      log.debug("Disconnecting client: " + session.getSessionState().getClientId());
      disconnect(true);
   }

   void handlePublish(MqttPublishMessage message) throws Exception {
      this.protocolManager.invokeIncoming(message, this.connection);
      session.getMqttPublishManager().handleMessage(message.variableHeader().messageId(), message.variableHeader().topicName(), message.fixedHeader().qosLevel().value(), message.payload(), message.fixedHeader().isRetain());
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
      ctx.write(rel);
      ctx.flush();
   }

   void handlePuback(MqttPubAckMessage message) throws Exception {
      session.getMqttPublishManager().handlePubAck(message.variableHeader().messageId());
   }

   void handlePubrec(MqttMessage message) throws Exception {
      int messageId = ((MqttMessageIdVariableHeader) message.variableHeader()).messageId();
      session.getMqttPublishManager().handlePubRec(messageId);
   }

   void handlePubrel(MqttMessage message) {
      int messageId = ((MqttMessageIdVariableHeader) message.variableHeader()).messageId();
      session.getMqttPublishManager().handlePubRel(messageId);
   }

   void handlePubcomp(MqttMessage message) throws Exception {
      int messageId = ((MqttMessageIdVariableHeader) message.variableHeader()).messageId();
      session.getMqttPublishManager().handlePubComp(messageId);
   }

   void handleSubscribe(MqttSubscribeMessage message, ChannelHandlerContext ctx) throws Exception {
      MQTTSubscriptionManager subscriptionManager = session.getSubscriptionManager();
      int[] qos = subscriptionManager.addSubscriptions(message.payload().topicSubscriptions());

      MqttFixedHeader header = new MqttFixedHeader(MqttMessageType.SUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
      MqttSubAckMessage ack = new MqttSubAckMessage(header, message.variableHeader(), new MqttSubAckPayload(qos));
      MQTTUtil.logMessage(session.getSessionState(), ack, false);
      ctx.write(ack);
      ctx.flush();
   }

   void handleSuback(MqttSubAckMessage message) {
      disconnect(true);
   }

   void handleUnsubscribe(MqttUnsubscribeMessage message) throws Exception {
      session.getSubscriptionManager().removeSubscriptions(message.payload().topics());
      MqttFixedHeader header = new MqttFixedHeader(MqttMessageType.UNSUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
      MqttUnsubAckMessage m = new MqttUnsubAckMessage(header, message.variableHeader());
      MQTTUtil.logMessage(session.getSessionState(), m, false);
      ctx.write(m);
      ctx.flush();
   }

   void handleUnsuback(MqttUnsubAckMessage message) {
      disconnect(true);
   }

   void handlePingreq(MqttMessage message, ChannelHandlerContext ctx) {
      MqttMessage pingResp = new MqttMessage(new MqttFixedHeader(MqttMessageType.PINGRESP, false, MqttQoS.AT_MOST_ONCE, false, 0));
      MQTTUtil.logMessage(session.getSessionState(), pingResp, false);
      ctx.write(pingResp);
      ctx.flush();
   }

   void handlePingresp(MqttMessage message) {
      disconnect(true);
   }

   void handleDisconnect(MqttMessage message) {
      disconnect(false);
   }

   protected int send(int messageId, String topicName, int qosLevel, ByteBuf payload, int deliveryCount) {
      boolean redelivery = qosLevel == 0 ? false : (deliveryCount > 0);
      MqttFixedHeader header = new MqttFixedHeader(MqttMessageType.PUBLISH, redelivery, MqttQoS.valueOf(qosLevel), false, 0);
      MqttPublishVariableHeader varHeader = new MqttPublishVariableHeader(topicName, messageId);
      MqttMessage publish = new MqttPublishMessage(header, varHeader, payload);
      this.protocolManager.invokeOutgoing(publish, connection);

      MQTTUtil.logMessage(session.getSessionState(), publish, false);

      ctx.write(publish);
      ctx.flush();

      return 1;
   }

   ActiveMQServer getServer() {
      return server;
   }
}
