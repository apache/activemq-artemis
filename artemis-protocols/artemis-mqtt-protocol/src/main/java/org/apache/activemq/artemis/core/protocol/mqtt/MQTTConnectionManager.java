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

import java.util.UUID;

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttVersion;
import io.netty.util.CharsetUtil;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.core.server.impl.ServerSessionImpl;
import org.apache.activemq.artemis.utils.UUIDGenerator;
import org.jboss.logging.Logger;

import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.ASSIGNED_CLIENT_IDENTIFIER;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.AUTHENTICATION_METHOD;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.MAXIMUM_PACKET_SIZE;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.RECEIVE_MAXIMUM;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.SERVER_KEEP_ALIVE;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.SESSION_EXPIRY_INTERVAL;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.TOPIC_ALIAS_MAXIMUM;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.WILL_DELAY_INTERVAL;

/**
 * MQTTConnectionManager is responsible for handle Connect and Disconnect packets and any resulting behaviour of these
 * events.
 */
public class MQTTConnectionManager {

   private static final Logger logger = Logger.getLogger(MQTTConnectionManager.class);

   private MQTTSession session;

   public MQTTConnectionManager(MQTTSession session) {
      this.session = session;
      MQTTFailureListener failureListener = new MQTTFailureListener(this);
      session.getConnection().addFailureListener(failureListener);
   }

   void connect(MqttConnectMessage connect, String validatedUser) throws Exception {
      if (session.getVersion() == MQTTVersion.MQTT_5) {
         session.getConnection().setProtocolVersion(Byte.toString(MqttVersion.MQTT_5.protocolLevel()));
         String authenticationMethod = MQTTUtil.getProperty(String.class, connect.variableHeader().properties(), AUTHENTICATION_METHOD);

         if (authenticationMethod != null) {
            session.getProtocolHandler().sendConnack(MQTTReasonCodes.BAD_AUTHENTICATION_METHOD);
            disconnect(true);
            return;
         }
      }

      String password = connect.payload().passwordInBytes() == null ? null : new String( connect.payload().passwordInBytes(), CharsetUtil.UTF_8);
      String username = connect.payload().userName();

      // the Netty codec uses "CleanSession" for both 3.1.1 "clean session" and 5 "clean start" which have slightly different semantics
      boolean cleanStart = connect.variableHeader().isCleanSession();

      Pair<String, Boolean> clientIdValidation = validateClientId(connect.payload().clientIdentifier(), cleanStart);
      if (clientIdValidation == null) {
         // this represents an invalid client ID for MQTT 5 clients
         session.getProtocolHandler().sendConnack(MQTTReasonCodes.CLIENT_IDENTIFIER_NOT_VALID);
         disconnect(true);
         return;
      } else if (clientIdValidation.getA() == null) {
         // this represents an invalid client ID for MQTT 3.x clients
         session.getProtocolHandler().sendConnack(MQTTReasonCodes.IDENTIFIER_REJECTED_3);
         disconnect(true);
         return;
      }
      String clientId = clientIdValidation.getA();
      boolean assignedClientId = clientIdValidation.getB();

      boolean sessionPresent = session.getProtocolManager().getSessionStates().containsKey(clientId);
      MQTTSessionState sessionState = getSessionState(clientId);
      synchronized (sessionState) {
         session.setSessionState(sessionState);
         session.getConnection().setClientID(clientId);
         sessionState.setFailed(false);
         ServerSessionImpl serverSession = createServerSession(username, password, validatedUser);
         serverSession.start();
         ServerSessionImpl internalServerSession = createServerSession(username, password, validatedUser);
         internalServerSession.disableSecurity();
         internalServerSession.start();
         session.setServerSession(serverSession, internalServerSession);

         if (cleanStart) {
            /* [MQTT-3.1.2-6] If CleanSession is set to 1, the Client and Server MUST discard any previous Session and
             * start a new one. This Session lasts as long as the Network Connection. State data associated with this Session
             * MUST NOT be reused in any subsequent Session */
            session.clean();
            session.setClean(true);
         }

         if (connect.variableHeader().isWillFlag()) {
            session.getState().setWill(true);
            byte[] willMessage = connect.payload().willMessageInBytes();
            session.getState().setWillMessage(ByteBufAllocator.DEFAULT.buffer(willMessage.length).writeBytes(willMessage));
            session.getState().setWillQoSLevel(connect.variableHeader().willQos());
            session.getState().setWillRetain(connect.variableHeader().isWillRetain());
            session.getState().setWillTopic(connect.payload().willTopic());

            if (session.getVersion() == MQTTVersion.MQTT_5) {
               MqttProperties willProperties = connect.payload().willProperties();
               if (willProperties != null) {
                  MqttProperties.MqttProperty willDelayInterval = willProperties.getProperty(WILL_DELAY_INTERVAL.value());
                  if (willDelayInterval != null) {
                     session.getState().setWillDelayInterval(( int) willDelayInterval.value());
                  }
               }
            }
         }

         MqttProperties connackProperties;
         if (session.getVersion() == MQTTVersion.MQTT_5) {
            session.getConnection().setReceiveMaximum(MQTTUtil.getProperty(Integer.class, connect.variableHeader().properties(), RECEIVE_MAXIMUM, -1));

            sessionState.setClientSessionExpiryInterval(MQTTUtil.getProperty(Integer.class, connect.variableHeader().properties(), SESSION_EXPIRY_INTERVAL, 0));
            sessionState.setClientMaxPacketSize(MQTTUtil.getProperty(Integer.class, connect.variableHeader().properties(), MAXIMUM_PACKET_SIZE, 0));
            sessionState.setClientTopicAliasMaximum(MQTTUtil.getProperty(Integer.class, connect.variableHeader().properties(), TOPIC_ALIAS_MAXIMUM));

            connackProperties = getConnackProperties(clientId, assignedClientId);
         } else {
            connackProperties = MqttProperties.NO_PROPERTIES;
         }

         session.getConnection().setConnected(true);
         session.getProtocolHandler().sendConnack(MQTTReasonCodes.SUCCESS, sessionPresent && !cleanStart, connackProperties);
         // ensure we don't publish before the CONNACK
         session.start();
      }
   }

   private MqttProperties getConnackProperties(String clientId, boolean assignedClientId) {
      MqttProperties connackProperties = new MqttProperties();

      if (assignedClientId) {
         connackProperties.add(new MqttProperties.StringProperty(ASSIGNED_CLIENT_IDENTIFIER.value(), clientId));
      }

      if (this.session.getProtocolManager().getTopicAliasMaximum() != -1) {
         connackProperties.add(new MqttProperties.IntegerProperty(TOPIC_ALIAS_MAXIMUM.value(), this.session.getProtocolManager().getTopicAliasMaximum()));
      }

      if (this.session.isUsingServerKeepAlive()) {
         connackProperties.add(new MqttProperties.IntegerProperty(SERVER_KEEP_ALIVE.value(), this.session.getProtocolManager().getServerKeepAlive()));
      }

      if (this.session.getProtocolManager().getMaximumPacketSize() != -1) {
         connackProperties.add(new MqttProperties.IntegerProperty(MAXIMUM_PACKET_SIZE.value(), this.session.getProtocolManager().getMaximumPacketSize()));
      }

      return connackProperties;
   }

   ServerSessionImpl createServerSession(String username, String password, String validatedUser) throws Exception {
      String id = UUIDGenerator.getInstance().generateStringUUID();
      ActiveMQServer server = session.getServer();
      ServerSession serverSession = server.createSession(id,
                                                         username,
                                                         password,
                                                         ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                                         session.getConnection(),
                                                         MQTTUtil.SESSION_AUTO_COMMIT_SENDS,
                                                         MQTTUtil.SESSION_AUTO_COMMIT_ACKS,
                                                         MQTTUtil.SESSION_PREACKNOWLEDGE,
                                                         MQTTUtil.SESSION_XA,
                                                         null,
                                                         session.getSessionCallback(),
                                                         MQTTUtil.SESSION_AUTO_CREATE_QUEUE,
                                                         server.newOperationContext(),
                                                         session.getProtocolManager().getPrefixes(),
                                                         session.getProtocolManager().getSecurityDomain(), validatedUser);
      return (ServerSessionImpl) serverSession;
   }

   void disconnect(boolean failure) {
      if (session == null || session.getStopped()) {
         return;
      }

      synchronized (session.getState()) {
         try {
            session.stop(failure);
            session.getConnection().destroy();
         } catch (Exception e) {
            MQTTLogger.LOGGER.errorDisconnectingClient(e);
         } finally {
            if (session.getState() != null) {
               String clientId = session.getState().getClientId();
               /**
                *  ensure that the connection for the client ID matches *this* connection otherwise we could remove the
                *  entry for the client who "stole" this client ID via [MQTT-3.1.4-2]
                */
               if (clientId != null && session.getProtocolManager().isClientConnected(clientId, session.getConnection())) {
                  session.getProtocolManager().removeConnectedClient(clientId);
               }
            }
         }
      }
   }

   private synchronized MQTTSessionState getSessionState(String clientId) {
      return session.getProtocolManager().getSessionState(clientId);
   }

   private Pair<String, Boolean> validateClientId(String clientId, boolean cleanSession) {
      Boolean assigned = Boolean.FALSE;
      if (clientId == null || clientId.isEmpty()) {
         // [MQTT-3.1.3-7] [MQTT-3.1.3-6] If client does not specify a client ID and clean session is set to 1 create it.
         if (cleanSession) {
            assigned = Boolean.TRUE;
            clientId = UUID.randomUUID().toString();
         } else {
            // [MQTT-3.1.3-8] Return ID rejected and disconnect if clean session = false and client id is null
            return null;
         }
      } else {
         MQTTConnection connection = session.getProtocolManager().addConnectedClient(clientId, session.getConnection());

         if (connection != null) {
            MQTTSession existingSession = session.getProtocolManager().getSessionState(clientId).getSession();
            if (session.getVersion() == MQTTVersion.MQTT_5) {
               existingSession.getProtocolHandler().sendDisconnect(MQTTReasonCodes.SESSION_TAKEN_OVER);
            }
            // [MQTT-3.1.4-2] If the client ID represents a client already connected to the server then the server MUST disconnect the existing client
            existingSession.getConnectionManager().disconnect(false);
         }
      }
      return new Pair<>(clientId, assigned);
   }
}
