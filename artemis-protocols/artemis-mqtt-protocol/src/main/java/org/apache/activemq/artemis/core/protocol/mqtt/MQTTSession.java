/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.core.protocol.mqtt;

import java.util.UUID;

import io.netty.buffer.EmptyByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.apache.activemq.artemis.api.core.ActiveMQSecurityException;
import org.apache.activemq.artemis.core.config.WildcardConfiguration;
import org.apache.activemq.artemis.core.persistence.CoreMessageObjectPools;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.impl.ServerSessionImpl;
import org.apache.activemq.artemis.spi.core.protocol.SessionCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.invoke.MethodHandles;

public class MQTTSession {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

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

   private MQTTStateManager stateManager;

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
      this.stateManager = protocolManager.getStateManager();
      this.wildcardConfiguration = wildcardConfiguration;

      this.connection = connection;

      mqttConnectionManager = new MQTTConnectionManager(this);
      mqttPublishManager = new MQTTPublishManager(this, protocolManager.isCloseMqttConnectionOnPublishAuthorizationFailure());
      sessionCallback = new MQTTSessionCallback(this, connection);
      subscriptionManager = new MQTTSubscriptionManager(this, stateManager);
      retainMessageManager = new MQTTRetainMessageManager(this);

      state = MQTTSessionState.DEFAULT;

      logger.debug("MQTT session created: {}", id);
   }

   /*
    * This method is only called by MQTTConnectionManager.connect
    * which is synchronized with MQTTConnectionManager.disconnect
    */
   void start() throws Exception {
      mqttPublishManager.start();
      subscriptionManager.start();
      stopped = false;
   }

   /*
    * This method is only called by MQTTConnectionManager.disconnect
    * which is synchronized with MQTTConnectionManager.connect
    */
   void stop(boolean failure) throws Exception {
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

         state.setAttached(false);
         state.setDisconnectedTime(System.currentTimeMillis());
         state.clearTopicAliases();

         if (getVersion() == MQTTVersion.MQTT_5) {
            if (state.getClientSessionExpiryInterval() == 0) {
               if (state.isWill() && failure) {
                  // If the session expires the will message must be sent no matter the will delay
                  sendWillMessage();
               }
               clean(false);
               stateManager.removeSessionState(connection.getClientID());
            }
         } else {
            if (state.isWill() && failure) {
               sendWillMessage();
            }
            if (isClean()) {
               clean(false);
               stateManager.removeSessionState(connection.getClientID());
            }
         }
      }
      stopped = true;
   }

   boolean getStopped() {
      return stopped;
   }

   void setStopped(boolean stopped) {
      this.stopped = stopped;
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

   MQTTStateManager getStateManager() {
      return stateManager;
   }

   void clean(boolean enforceSecurity) throws Exception {
      subscriptionManager.clean(enforceSecurity);
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
      if (state.getWillStatus() == MQTTSessionState.WillStatus.NOT_SENT) {
         try {
            state.setWillStatus(MQTTSessionState.WillStatus.SENDING);
            MqttProperties properties;
            if (state.getWillUserProperties() == null) {
               properties = MqttProperties.NO_PROPERTIES;
            } else {
               properties = new MqttProperties();
               for (MqttProperties.MqttProperty userProperty : state.getWillUserProperties()) {
                  properties.add(userProperty);
               }
            }
            MqttPublishMessage publishMessage = MqttMessageBuilders.publish().messageId(0).qos(MqttQoS.valueOf(state.getWillQoSLevel())).retained(state.isWillRetain()).topicName(state.getWillTopic()).payload(state.getWillMessage() == null ? new EmptyByteBuf(PooledByteBufAllocator.DEFAULT) : state.getWillMessage()).properties(properties).build();
            logger.debug("{} sending will message: {}", this, publishMessage);
            getMqttPublishManager().sendToQueue(publishMessage, true);
            state.setWillStatus(MQTTSessionState.WillStatus.SENT);
            state.setWillMessage(null);
         } catch (ActiveMQSecurityException e) {
            state.setWillStatus(MQTTSessionState.WillStatus.NOT_SENT);
            MQTTLogger.LOGGER.authorizationFailureSendingWillMessage(e.getMessage());
         } catch (Exception e) {
            state.setWillStatus(MQTTSessionState.WillStatus.NOT_SENT);
            MQTTLogger.LOGGER.errorSendingWillMessage(e);
         }
      }
   }

   @Override
   public String toString() {
      return "MQTTSession[" +
         "coreSessionId: " + (serverSession != null ? serverSession.getName() : "null") +
         ", clientId: " + state.getClientId() +
         "]";
   }
}
