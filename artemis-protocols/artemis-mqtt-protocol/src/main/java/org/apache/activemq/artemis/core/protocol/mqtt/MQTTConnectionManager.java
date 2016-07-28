/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.core.protocol.mqtt;

import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ServerMessage;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.core.server.impl.ServerSessionImpl;
import org.apache.activemq.artemis.utils.ConcurrentHashSet;
import org.apache.activemq.artemis.utils.UUIDGenerator;

import java.util.Set;
import java.util.UUID;

/**
 * MQTTConnectionMananager is responsible for handle Connect and Disconnect packets and any resulting behaviour of these
 * events.
 */
public class MQTTConnectionManager {

   private MQTTSession session;

   //TODO Read in a list of existing client IDs from stored Sessions.
   public static Set<String> CONNECTED_CLIENTS = new ConcurrentHashSet<>();

   private MQTTLogger log = MQTTLogger.LOGGER;

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
         session.getProtocolHandler().disconnect();
         return;
      }

      session.setSessionState(getSessionState(clientId, cleanSession));

      ServerSessionImpl serverSession = createServerSession(username, password);
      serverSession.start();

      session.setServerSession(serverSession);

      if (will) {
         ServerMessage w = MQTTUtil.createServerMessageFromString(session, willMessage, willTopic, willQosLevel, willRetain);
         session.getSessionState().setWillMessage(w);
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

      ServerSession serverSession = server.createSession(id, username, password,
                                                         ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                                         session.getConnection(), MQTTUtil.SESSION_AUTO_COMMIT_SENDS,
                                                         MQTTUtil.SESSION_AUTO_COMMIT_ACKS, MQTTUtil.SESSION_PREACKNOWLEDGE,
                                                         MQTTUtil.SESSION_XA, null, session.getSessionCallback(),
                                                         MQTTUtil.SESSION_AUTO_CREATE_QUEUE);
      return (ServerSessionImpl) serverSession;
   }

   void disconnect() {
      if (session == null) {
         return;
      }
      try {
         if (session.getSessionState() != null) {
            String clientId = session.getSessionState().getClientId();
            if (clientId != null)
               CONNECTED_CLIENTS.remove(clientId);

            if (session.getState().isWill()) {
               session.getConnectionManager().sendWill();
            }
         }
         session.stop();
         session.getConnection().disconnect(false);
         session.getConnection().destroy();
      }
      catch (Exception e) {
         /* FIXME Failure during disconnect would leave the session state in an unrecoverable state.  We should handle
         errors more gracefully.
          */
         log.error("Error disconnecting client: " + e.getMessage());
      }
   }

   private void sendWill() throws Exception {
      session.getServerSession().send(session.getSessionState().getWillMessage(), true);
      session.getSessionState().deleteWillMessage();
   }

   private MQTTSessionState getSessionState(String clientId, boolean cleanSession) throws InterruptedException {
      synchronized (MQTTSession.SESSIONS) {
         /* [MQTT-3.1.2-6] If CleanSession is set to 1, the Client and Server MUST discard any previous Session and
          * start a new one  This Session lasts as long as the Network Connection. State data associated with this Session
          * MUST NOT be reused in any subsequent Session */
         if (cleanSession) {
            MQTTSession.SESSIONS.remove(clientId);
            return new MQTTSessionState(clientId);
         }
         else {
            /* [MQTT-3.1.2-4] Attach an existing session if one exists (if cleanSession flag is false) otherwise create
            a new one. */
            MQTTSessionState state = MQTTSession.SESSIONS.get(clientId);
            if (state != null) {
               // TODO Add a count down latch for handling wait during attached session state.
               while (state.getAttached()) {
                  Thread.sleep(1000);
               }
               return state;
            }
            else {
               state = new MQTTSessionState(clientId);
               MQTTSession.SESSIONS.put(clientId, state);
               return state;
            }
         }
      }
   }

   private String validateClientId(String clientId, boolean cleanSession) {
      if (clientId == null || clientId.isEmpty()) {
         // [MQTT-3.1.3-7] [MQTT-3.1.3-6] If client does not specify a client ID and clean session is set to 1 create it.
         if (cleanSession) {
            clientId = UUID.randomUUID().toString();
         }
         else {
            // [MQTT-3.1.3-8] Return ID rejected and disconnect if clean session = false and client id is null
            return null;
         }
      }
      // If the client ID is not unique (i.e. it has already registered) then do not accept it.
      else if (!CONNECTED_CLIENTS.add(clientId)) {
         // [MQTT-3.1.3-9] Return ID Rejected if server rejects the client ID
         return null;
      }
      return clientId;
   }
}
