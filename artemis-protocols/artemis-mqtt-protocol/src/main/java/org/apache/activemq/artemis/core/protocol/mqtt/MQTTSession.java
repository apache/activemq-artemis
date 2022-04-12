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

import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.apache.activemq.artemis.core.config.WildcardConfiguration;
import org.apache.activemq.artemis.core.persistence.CoreMessageObjectPools;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.impl.ServerSessionImpl;
import org.apache.activemq.artemis.spi.core.protocol.SessionCallback;
import org.jboss.logging.Logger;

public class MQTTSession {

   private static final Logger logger = Logger.getLogger(MQTTSession.class);

   private final String id = UUID.randomUUID().toString();

   private MQTTProtocolHandler protocolHandler;

   private MQTTSubscriptionManager subscriptionManager;

   private MQTTSessionCallback sessionCallback;

   private ServerSessionImpl serverSession;

   private ServerSessionImpl internalServerSession;

   private MQTTPublishManager mqttPublishManager;

   private MQTTConnectionManager mqttConnectionManager;

   private MQTTRetainMessageManager retainMessageManager;

   private MQTTConnection connection;

   protected MQTTSessionState state;

   private boolean stopped = false;

   private MQTTProtocolManager protocolManager;

   private boolean clean;

   private WildcardConfiguration wildcardConfiguration;

   private CoreMessageObjectPools coreMessageObjectPools = new CoreMessageObjectPools();

   private MQTTVersion version = null;

   private boolean usingServerKeepAlive = false;

   public MQTTSession(MQTTProtocolHandler protocolHandler,
                      MQTTConnection connection,
                      MQTTProtocolManager protocolManager,
                      WildcardConfiguration wildcardConfiguration) throws Exception {
      this.protocolHandler = protocolHandler;
      this.protocolManager = protocolManager;
      this.wildcardConfiguration = wildcardConfiguration;

      this.connection = connection;

      mqttConnectionManager = new MQTTConnectionManager(this);
      mqttPublishManager = new MQTTPublishManager(this, protocolManager.isCloseMqttConnectionOnPublishAuthorizationFailure());
      sessionCallback = new MQTTSessionCallback(this, connection);
      subscriptionManager = new MQTTSubscriptionManager(this);
      retainMessageManager = new MQTTRetainMessageManager(this);

      state = MQTTSessionState.DEFAULT;

      logger.debugf("MQTT session created: %s", id);
   }

   // Called after the client has Connected.
   synchronized void start() throws Exception {
      mqttPublishManager.start();
      subscriptionManager.start();
      stopped = false;
   }

   synchronized void stop(boolean failure) throws Exception {
      state.setFailed(failure);

      if (!stopped) {
         protocolHandler.stop();
         subscriptionManager.stop();
         mqttPublishManager.stop();

         if (serverSession != null) {
            serverSession.stop();
            serverSession.close(false);
         }

         if (internalServerSession != null) {
            internalServerSession.stop();
            internalServerSession.close(false);
         }

         if (state != null) {
            state.setAttached(false);
            state.setDisconnectedTime(System.currentTimeMillis());
         }

         if (getVersion() == MQTTVersion.MQTT_5) {
            if (state.getClientSessionExpiryInterval() == 0) {
               if (state.isWill() && failure) {
                  // If the session expires the will message must be sent no matter the will delay
                  sendWillMessage();
               }
               clean();
               protocolManager.removeSessionState(connection.getClientID());
            } else {
               state.setDisconnectedTime(System.currentTimeMillis());
            }
         } else {
            if (state.isWill() && failure) {
               sendWillMessage();
            }
            if (isClean()) {
               clean();
               protocolManager.removeSessionState(connection.getClientID());
            }
         }
      }
      stopped = true;
   }

   boolean getStopped() {
      return stopped;
   }

   boolean isClean() {
      return clean;
   }

   void setClean(boolean clean) {
      this.clean = clean;
   }

   MQTTPublishManager getMqttPublishManager() {
      return mqttPublishManager;
   }

   MQTTSessionState getState() {
      return state;
   }

   MQTTConnectionManager getConnectionManager() {
      return mqttConnectionManager;
   }

   ServerSessionImpl getServerSession() {
      return serverSession;
   }

   ServerSessionImpl getInternalServerSession() {
      return internalServerSession;
   }

   ActiveMQServer getServer() {
      return protocolHandler.getServer();
   }

   MQTTSubscriptionManager getSubscriptionManager() {
      return subscriptionManager;
   }

   MQTTProtocolHandler getProtocolHandler() {
      return protocolHandler;
   }

   SessionCallback getSessionCallback() {
      return sessionCallback;
   }

   void setServerSession(ServerSessionImpl serverSession, ServerSessionImpl internalServerSession) {
      this.serverSession = serverSession;
      this.internalServerSession = internalServerSession;
   }

   void setSessionState(MQTTSessionState state) {
      this.state = state;
      this.state.setAttached(true);
      this.state.setDisconnectedTime(0);
      this.state.setSession(this);
   }

   MQTTRetainMessageManager getRetainMessageManager() {
      return retainMessageManager;
   }

   MQTTConnection getConnection() {
      return connection;
   }

   MQTTProtocolManager getProtocolManager() {
      return protocolManager;
   }

   void clean() throws Exception {
      subscriptionManager.clean();
      mqttPublishManager.clean();
      state.clear();
   }

   public WildcardConfiguration getWildcardConfiguration() {
      return wildcardConfiguration;
   }

   public void setWildcardConfiguration(WildcardConfiguration wildcardConfiguration) {
      this.wildcardConfiguration = wildcardConfiguration;
   }

   public CoreMessageObjectPools getCoreMessageObjectPools() {
      return coreMessageObjectPools;
   }

   public void setVersion(MQTTVersion version) {
      this.version = version;
   }

   public MQTTVersion getVersion() {
      return this.version;
   }

   public boolean isUsingServerKeepAlive() {
      return usingServerKeepAlive;
   }

   public void setUsingServerKeepAlive(boolean usingServerKeepAlive) {
      this.usingServerKeepAlive = usingServerKeepAlive;
   }

   public void sendWillMessage() {
      try {
         MqttProperties properties;
         if (state.getWillUserProperties() == null) {
            properties = MqttProperties.NO_PROPERTIES;
         } else {
            properties = new MqttProperties();
            for (MqttProperties.MqttProperty userProperty : state.getWillUserProperties()) {
               properties.add(userProperty);
            }
         }
         MqttPublishMessage publishMessage = MqttMessageBuilders.publish()
            .messageId(0)
            .qos(MqttQoS.valueOf(state.getWillQoSLevel()))
            .retained(state.isWillRetain())
            .topicName(state.getWillTopic())
            .payload(state.getWillMessage())
            .properties(properties)
            .build();
         logger.debugf("%s sending will message: %s", this, publishMessage);
         getMqttPublishManager().sendToQueue(publishMessage, true);
         state.setWillSent(true);
         state.setWillMessage(null);
      } catch (Exception e) {
         MQTTLogger.LOGGER.errorSendingWillMessage(e);
      }
   }

   @Override
   public String toString() {
      return "MQTTSession[coreSessionId: " + (serverSession != null ? serverSession.getName() : "null") + "]";
   }
}
