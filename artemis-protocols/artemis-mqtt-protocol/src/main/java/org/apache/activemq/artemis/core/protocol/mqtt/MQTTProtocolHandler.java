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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttMessageIdAndPropertiesVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttPubReplyMessageVariableHeader;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttReasonCodeAndPropertiesVariableHeader;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubAckPayload;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttUnsubAckMessage;
import io.netty.handler.codec.mqtt.MqttUnsubAckPayload;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import io.netty.handler.codec.mqtt.MqttVersion;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import org.apache.activemq.artemis.api.core.ActiveMQSecurityException;
import org.apache.activemq.artemis.core.protocol.mqtt.exceptions.DisconnectException;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.logs.AuditLogger;
import org.apache.activemq.artemis.spi.core.protocol.ConnectionEntry;
import org.apache.activemq.artemis.utils.actors.Actor;
import org.jboss.logging.Logger;

import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.AUTHENTICATION_DATA;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.AUTHENTICATION_METHOD;

/**
 * This class is responsible for receiving and sending MQTT packets, delegating behaviour to one of the
 * MQTTConnectionManager, MQTTPublishManager, MQTTSubscriptionManager classes.
 */
public class MQTTProtocolHandler extends ChannelInboundHandlerAdapter {

   private static final Logger logger = Logger.getLogger(MQTTProtocolHandler.class);

   private ConnectionEntry connectionEntry;

   private MQTTConnection connection;

   private MQTTSession session;

   private ActiveMQServer server;

   private MQTTProtocolManager protocolManager;

   // This Channel Handler is not sharable, therefore it can only ever be associated with a single ctx.
   private ChannelHandlerContext ctx;

   private boolean stopped = false;

   private final Actor<MqttMessage> mqttMessageActor;

   public MQTTProtocolHandler(ActiveMQServer server, MQTTProtocolManager protocolManager) {
      this.server = server;
      this.protocolManager = protocolManager;
      this.mqttMessageActor = new Actor<>(server.getThreadPool(), this::act);
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
      MqttMessage message = (MqttMessage) msg;

      if (stopped) {
         if (session.getVersion() == MQTTVersion.MQTT_5) {
            sendDisconnect(MQTTReasonCodes.IMPLEMENTATION_SPECIFIC_ERROR);
         }
         disconnect(true);
         return;
      }

      // Disconnect if Netty codec failed to decode the stream.
      if (message.decoderResult().isFailure()) {
         logger.debugf(message.decoderResult().cause(), "Disconnecting client due to message decoding failure.");
         if (session.getVersion() == MQTTVersion.MQTT_5) {
            sendDisconnect(MQTTReasonCodes.MALFORMED_PACKET);
         }
         disconnect(true);
         return;
      }

      String interceptResult = this.protocolManager.invokeIncoming(message, this.connection);
      if (interceptResult != null) {
         logger.debugf("Interceptor %s rejected MQTT control packet: %s", interceptResult, message);
         disconnect(true);
         return;
      }

      connection.dataReceived();

      if (AuditLogger.isAnyLoggingEnabled()) {
         AuditLogger.setRemoteAddress(connection.getRemoteAddress());
      }

      MQTTUtil.logMessage(session.getState(), message, true);

      if (this.ctx == null) {
         this.ctx = ctx;
      }

      // let Netty handle client pings (i.e. connection keep-alive)
      if (MqttMessageType.PINGREQ == message.fixedHeader().messageType()) {
         handlePingreq();
      } else {
         mqttMessageActor.act(message);
      }
   }

   public void act(MqttMessage message) {
      try {
         switch (message.fixedHeader().messageType()) {
            case AUTH:
               handleAuth(message);
               break;
            case CONNECT:
               handleConnect((MqttConnectMessage) message);
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
            case DISCONNECT:
               disconnect(false);
               break;
            case UNSUBACK:
            case SUBACK:
            case PINGREQ: // These are actually handled by the Netty thread directly so this packet should never make it here
            case PINGRESP:
            case CONNACK: // The server does not instantiate connections therefore any CONNACK received over a connection is an invalid control message.
            default:
               disconnect(true);
         }
      } catch (Exception e) {
         MQTTLogger.LOGGER.errorProcessingControlPacket(message.toString(), e);
         if (session.getVersion() == MQTTVersion.MQTT_5) {
            sendDisconnect(MQTTReasonCodes.IMPLEMENTATION_SPECIFIC_ERROR);
         }
         disconnect(true);
      } finally {
         ReferenceCountUtil.release(message);
      }
   }

   /*
    * Scaffolding for "enhanced authentication" implementation.
    *
    * See:
    *   https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901217
    *   https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901256
    *
    * Tests for this are in:
    *   org.apache.activemq.artemis.tests.integration.mqtt5.spec.controlpackets.AuthTests
    *   org.apache.activemq.artemis.tests.integration.mqtt5.spec.EnhancedAuthenticationTests
    *
    * This should integrate somehow with our existing SASL implementation for challenge/response conversations.
    */
   void handleAuth(MqttMessage auth) throws Exception {
      byte[] authenticationData = MQTTUtil.getProperty(byte[].class, ((MqttReasonCodeAndPropertiesVariableHeader)auth.variableHeader()).properties(), AUTHENTICATION_DATA);
      String authenticationMethod = MQTTUtil.getProperty(String.class, ((MqttReasonCodeAndPropertiesVariableHeader)auth.variableHeader()).properties(), AUTHENTICATION_METHOD);

      MqttReasonCodeAndPropertiesVariableHeader header = (MqttReasonCodeAndPropertiesVariableHeader) auth.variableHeader();
      if (header.reasonCode() == MQTTReasonCodes.RE_AUTHENTICATE) {

      } else if (header.reasonCode() == MQTTReasonCodes.CONTINUE_AUTHENTICATION) {

      } else if (header.reasonCode() == MQTTReasonCodes.SUCCESS) {

      }
   }

   void handleConnect(MqttConnectMessage connect) throws Exception {
      /*
       * Perform authentication *before* attempting redirection because redirection may be based on the user's role.
       */
      String password = connect.payload().passwordInBytes() == null ? null : new String(connect.payload().passwordInBytes(), CharsetUtil.UTF_8);
      String username = connect.payload().userName();
      String validatedUser;
      try {
         validatedUser = session.getServer().validateUser(username, password, session.getConnection(), session.getProtocolManager().getSecurityDomain());
      } catch (ActiveMQSecurityException e) {
         if (session.getVersion() == MQTTVersion.MQTT_5) {
            session.getProtocolHandler().sendConnack(MQTTReasonCodes.BAD_USER_NAME_OR_PASSWORD);
         } else {
            session.getProtocolHandler().sendConnack(MQTTReasonCodes.NOT_AUTHORIZED_3);
         }
         disconnect(true);
         return;
      }

      if (connection.getTransportConnection().getRouter() == null || !protocolManager.getRoutingHandler().route(connection, session, connect)) {
         /* [MQTT-3.1.2-2] Reject unsupported clients. */
         int packetVersion = connect.variableHeader().version();
         if (packetVersion != MqttVersion.MQTT_3_1.protocolLevel() &&
            packetVersion != MqttVersion.MQTT_3_1_1.protocolLevel() &&
            packetVersion != MqttVersion.MQTT_5.protocolLevel()) {

            if (packetVersion <= MqttVersion.MQTT_3_1_1.protocolLevel()) {
               // See MQTT-3.1.2-2 at http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718030
               sendConnack(MQTTReasonCodes.UNACCEPTABLE_PROTOCOL_VERSION_3);
            } else {
               // See MQTT-3.1.2-2 at https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901037
               sendConnack(MQTTReasonCodes.UNSUPPORTED_PROTOCOL_VERSION);
            }

            disconnect(true);
            return;
         }

         /*
          * If the server's keep-alive has been disabled (-1) or if the client is using a lower value than the server
          * then we use the client's keep-alive.
          *
          * We must adjust the keep-alive because MQTT communicates keep-alive values in *seconds*, but the broker uses
          * *milliseconds*. Also, the connection keep-alive is effectively "one and a half times" the configured
          * keep-alive value. See [MQTT-3.1.2-22].
          */
         int serverKeepAlive = session.getProtocolManager().getServerKeepAlive();
         int clientKeepAlive = connect.variableHeader().keepAliveTimeSeconds();
         if (serverKeepAlive == -1 || (clientKeepAlive <= serverKeepAlive && clientKeepAlive != 0)) {
            connectionEntry.ttl = clientKeepAlive * MQTTUtil.KEEP_ALIVE_ADJUSTMENT;
         } else {
            session.setUsingServerKeepAlive(true);
         }

         session.getConnectionManager().connect(connect, validatedUser);
      }
   }

   void disconnect(boolean error) {
      session.getConnectionManager().disconnect(error);
   }

   void sendConnack(byte returnCode) {
      sendConnack(returnCode, MqttProperties.NO_PROPERTIES);
   }

   void sendConnack(byte returnCode, MqttProperties properties) {
      sendConnack(returnCode, true, properties);
   }

   void sendConnack(byte returnCode, boolean sessionPresent, MqttProperties properties) {
      // 3.1.1 - [MQTT-3.2.2-4] If a server sends a CONNACK packet containing a non-zero return code it MUST set Session Present to 0.
      // 5     - [MQTT-3.2.2-6] If a Server sends a CONNACK packet containing a non-zero Reason Code it MUST set Session Present to 0.
      if (returnCode != MQTTReasonCodes.SUCCESS) {
         sessionPresent = false;
      }
      sendToClient(MqttMessageBuilders
                      .connAck()
                      .returnCode(MqttConnectReturnCode.valueOf(returnCode))
                      .properties(properties)
                      .sessionPresent(sessionPresent)
                      .build());
   }

   void sendDisconnect(byte reasonCode) {
      sendToClient(MqttMessageBuilders
                      .disconnect()
                      .reasonCode(reasonCode)
                      .build());
   }

   void handlePublish(MqttPublishMessage message) throws Exception {
      if (session.getVersion() == MQTTVersion.MQTT_5 && session.getProtocolManager().getMaximumPacketSize() != -1 && MQTTUtil.calculateMessageSize(message) > session.getProtocolManager().getMaximumPacketSize()) {
         sendDisconnect(MQTTReasonCodes.PACKET_TOO_LARGE);
         disconnect(true);
         return;
      }

      try {
         session.getMqttPublishManager().sendToQueue(message, false);
      } catch (DisconnectException e) {
         sendDisconnect(e.getCode());
         disconnect(true);
      }
   }

   void sendPubAck(int messageId, byte reasonCode) {
      sendPublishProtocolControlMessage(messageId, MqttMessageType.PUBACK, reasonCode);
   }

   void sendPubRel(int messageId) {
      sendPublishProtocolControlMessage(messageId, MqttMessageType.PUBREL);
   }

   void sendPubRec(int messageId, byte reasonCode) {
      sendPublishProtocolControlMessage(messageId, MqttMessageType.PUBREC, reasonCode);
   }

   void sendPubComp(int messageId) {
      sendPublishProtocolControlMessage(messageId, MqttMessageType.PUBCOMP);
   }

   void sendPublishProtocolControlMessage(int messageId, MqttMessageType messageType) {
      sendPublishProtocolControlMessage(messageId, messageType, MQTTReasonCodes.SUCCESS);
   }

   void sendPublishProtocolControlMessage(int messageId, MqttMessageType messageType, byte reasonCode) {
      // [MQTT-3.6.1-1] Spec requires 01 in header for rel
      MqttFixedHeader fixedHeader = new MqttFixedHeader(messageType, false, (messageType == MqttMessageType.PUBREL) ? MqttQoS.AT_LEAST_ONCE : MqttQoS.AT_MOST_ONCE, false, 0);

      MqttMessageIdVariableHeader variableHeader;
      if (session.getVersion() == MQTTVersion.MQTT_5) {
         variableHeader = new MqttPubReplyMessageVariableHeader(messageId, reasonCode, MqttProperties.NO_PROPERTIES);
      } else {
         variableHeader = MqttMessageIdVariableHeader.from(messageId);
      }
      MqttPubAckMessage pubAck = new MqttPubAckMessage(fixedHeader, variableHeader);
      sendToClient(pubAck);
   }

   void handlePuback(MqttPubAckMessage message) throws Exception {
      // ((MqttPubReplyMessageVariableHeader)message.variableHeader()).reasonCode();
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
      int[] qos = session.getSubscriptionManager().addSubscriptions(message.payload().topicSubscriptions(), message.idAndPropertiesVariableHeader().properties());
      MqttFixedHeader header = new MqttFixedHeader(MqttMessageType.SUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
      MqttMessageIdAndPropertiesVariableHeader variableHeader = new MqttMessageIdAndPropertiesVariableHeader(message.variableHeader().messageId(), MqttProperties.NO_PROPERTIES);
      MqttSubAckMessage subAck = new MqttSubAckMessage(header, variableHeader, new MqttSubAckPayload(qos));
      sendToClient(subAck);
   }

   void handleUnsubscribe(MqttUnsubscribeMessage message) throws Exception {
      short[] reasonCodes = session.getSubscriptionManager().removeSubscriptions(message.payload().topics());
      MqttFixedHeader header = new MqttFixedHeader(MqttMessageType.UNSUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
      MqttUnsubAckMessage unsubAck;
      if (session.getVersion() == MQTTVersion.MQTT_5) {
         unsubAck = new MqttUnsubAckMessage(header, message.variableHeader(), new MqttUnsubAckPayload(reasonCodes));
      } else {
         unsubAck = new MqttUnsubAckMessage(header, message.variableHeader());
      }
      sendToClient(unsubAck);
   }

   void handlePingreq() {
      MqttMessage pingResp = new MqttMessage(new MqttFixedHeader(MqttMessageType.PINGRESP, false, MqttQoS.AT_MOST_ONCE, false, 0));
      sendToClient(pingResp);
   }

   protected void sendToClient(MqttMessage message) {
      if (this.protocolManager.invokeOutgoing(message, connection) != null) {
         return;
      }
      MQTTUtil.logMessage(session.getState(), message, false);
      ctx.writeAndFlush(message, ctx.voidPromise());
   }

   private int getMessageId(MqttMessage message) {
      return ((MqttMessageIdVariableHeader) message.variableHeader()).messageId();
   }

   ActiveMQServer getServer() {
      return server;
   }
}
