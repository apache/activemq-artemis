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

import java.nio.charset.Charset;
import java.util.Set;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.core.server.impl.ServerSessionImpl;
import org.apache.activemq.artemis.utils.collections.ConcurrentHashSet;
import org.apache.activemq.artemis.utils.UUIDGenerator;

/**
 * MQTTConnectionMananager is responsible for handle Connect and Disconnect packets and any resulting behaviour of these
 * events.
 */
public class MQTTConnectionManager {

   private MQTTSession session;

   //TODO Read in a list of existing client IDs from stored Sessions.
   public static Set<String> CONNECTED_CLIENTS = new ConcurrentHashSet<>();

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
   synchronized void connect(String cId,
                             String username,
                             String password,
                             boolean will,
                             String willMessage,
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

      session.setSessionState(getSessionState(clientId));
      ServerSessionImpl serverSession = createServerSession(username, password);
      serverSession.start();

      session.setServerSession(serverSession);
      session.setIsClean(cleanSession);

      if (will) {
         isWill = true;
         byte[] payload = willMessage.getBytes(Charset.forName("UTF-8"));
         this.willMessage = ByteBufAllocator.DEFAULT.buffer(payload.length);
         this.willMessage.writeBytes(payload);
         this.willQoSLevel = willQosLevel;
         this.willRetain = willRetain;
         this.willTopic = willTopic;
      }

      session.getConnection().setConnected(true);
      session.start();
      session.getProtocolHandler().sendConnack(MqttConnectReturnCode.CONNECTION_ACCEPTED);
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
                                                         session.getProtocolManager().getPrefixes());
      return (ServerSessionImpl) serverSession;
   }

   synchronized void disconnect(boolean failure) {
      if (session == null || session.getStopped()) {
         return;
      }

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
            if (clientId != null) {
               CONNECTED_CLIENTS.remove(clientId);
            }
         }
      }
   }

   private MQTTSessionState getSessionState(String clientId) throws InterruptedException {
      /* [MQTT-3.1.2-6] If CleanSession is set to 1, the Client and Server MUST discard any previous Session and
       * start a new one  This Session lasts as long as the Network Connection. State data associated with this Session
       * MUST NOT be reused in any subsequent Session */

      /* [MQTT-3.1.2-4] Attach an existing session if one exists (if cleanSession flag is false) otherwise create
      a new one. */
      MQTTSessionState state = MQTTSession.SESSIONS.get(clientId);
      if (state != null) {
         return state;
      } else {
         state = new MQTTSessionState(clientId);
         MQTTSession.SESSIONS.put(clientId, state);
         return state;
      }
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
      } else if (!CONNECTED_CLIENTS.add(clientId)) {
         // ^^^ If the client ID is not unique (i.e. it has already registered) then do not accept it.


         // [MQTT-3.1.3-9] Return ID Rejected if server rejects the client ID
         return null;
      }
      return clientId;
   }
}
