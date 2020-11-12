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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.util.CharsetUtil;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.core.server.impl.ServerSessionImpl;
import org.apache.activemq.artemis.utils.UUIDGenerator;

/**
 * MQTTConnectionManager is responsible for handle Connect and Disconnect packets and any resulting behaviour of these
 * events.
 */
public class MQTTConnectionManager {

   private MQTTSession session;

   private MQTTLogger log = MQTTLogger.LOGGER;

   private boolean isWill = false;

   private ByteBuf willMessage;

   private String willTopic;

   private int willQoSLevel;

   private boolean willRetain;

   public MQTTConnectionManager(MQTTSession session) {
      this.session = session;
      MQTTFailureListener failureListener = new MQTTFailureListener(this);
      session.getConnection().addFailureListener(failureListener);
   }

   /**
    * Handles the connect packet.  See spec for details on each of parameters.
    */
   void connect(String cId,
                String username,
                byte[] passwordInBytes,
                boolean will,
                byte[] willMessage,
                String willTopic,
                boolean willRetain,
                int willQosLevel,
                boolean cleanSession) throws Exception {
      String clientId = validateClientId(cId, cleanSession);
      if (clientId == null) {
         session.getProtocolHandler().sendConnack(MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED);
         session.getProtocolHandler().disconnect(true);
         return;
      }

      MQTTSessionState sessionState = getSessionState(clientId);
      synchronized (sessionState) {
         session.setSessionState(sessionState);
         String password = passwordInBytes == null ? null : new String(passwordInBytes, CharsetUtil.UTF_8);
         session.getConnection().setClientID(clientId);
         ServerSessionImpl serverSession = createServerSession(username, password);
         serverSession.start();
         ServerSessionImpl internalServerSession = createServerSession(username, password);
         internalServerSession.disableSecurity();
         internalServerSession.start();
         session.setServerSession(serverSession, internalServerSession);

         if (cleanSession) {
            /* [MQTT-3.1.2-6] If CleanSession is set to 1, the Client and Server MUST discard any previous Session and
             * start a new one. This Session lasts as long as the Network Connection. State data associated with this Session
             * MUST NOT be reused in any subsequent Session */
            session.clean();
            session.setClean(true);
         }

         if (will) {
            isWill = true;
            this.willMessage = ByteBufAllocator.DEFAULT.buffer(willMessage.length);
            this.willMessage.writeBytes(willMessage);
            this.willQoSLevel = willQosLevel;
            this.willRetain = willRetain;
            this.willTopic = willTopic;
         }

         session.getConnection().setConnected(true);
         session.getProtocolHandler().sendConnack(MqttConnectReturnCode.CONNECTION_ACCEPTED);
         // ensure we don't publish before the CONNACK
         session.start();
      }
   }

   /**
    * Creates an internal Server Session.
    *
    * @param username
    * @param password
    * @return
    * @throws Exception
    */
   ServerSessionImpl createServerSession(String username, String password) throws Exception {
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
                                                         session.getProtocolManager().getSecurityDomain());
      return (ServerSessionImpl) serverSession;
   }

   void disconnect(boolean failure) {
      if (session == null || session.getStopped()) {
         return;
      }

      synchronized (session.getSessionState()) {
         try {
            if (isWill && failure) {
               session.getMqttPublishManager().sendInternal(0, willTopic, willQoSLevel, willMessage, willRetain, true);
            }
            session.stop();
            session.getConnection().destroy();
         } catch (Exception e) {
            log.error("Error disconnecting client: " + e.getMessage());
         } finally {
            if (session.getSessionState() != null) {
               session.getSessionState().setAttached(false);
               String clientId = session.getSessionState().getClientId();
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

   private String validateClientId(String clientId, boolean cleanSession) {
      if (clientId == null || clientId.isEmpty()) {
         // [MQTT-3.1.3-7] [MQTT-3.1.3-6] If client does not specify a client ID and clean session is set to 1 create it.
         if (cleanSession) {
            clientId = UUID.randomUUID().toString();
         } else {
            // [MQTT-3.1.3-8] Return ID rejected and disconnect if clean session = false and client id is null
            return null;
         }
      } else {
         MQTTConnection connection = session.getProtocolManager().addConnectedClient(clientId, session.getConnection());

         if (connection != null) {
            // [MQTT-3.1.4-2] If the client ID represents a client already connected to the server then the server MUST disconnect the existing client
            connection.disconnect(false);
         }
      }
      return clientId;
   }
}
