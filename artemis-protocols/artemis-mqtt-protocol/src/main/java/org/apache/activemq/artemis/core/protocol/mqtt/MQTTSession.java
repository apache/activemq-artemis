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
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.activemq.artemis.core.config.WildcardConfiguration;
import org.apache.activemq.artemis.core.message.impl.CoreMessageObjectPools;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.impl.ServerSessionImpl;
import org.apache.activemq.artemis.spi.core.protocol.SessionCallback;

public class MQTTSession {

   static Map<String, MQTTSessionState> SESSIONS = new ConcurrentHashMap<>();

   private final String id = UUID.randomUUID().toString();

   private MQTTProtocolHandler protocolHandler;

   private MQTTSubscriptionManager subscriptionManager;

   private MQTTSessionCallback sessionCallback;

   private ServerSessionImpl serverSession;

   private MQTTPublishManager mqttPublishManager;

   private MQTTConnectionManager mqttConnectionManager;

   private MQTTRetainMessageManager retainMessageManager;

   private MQTTConnection connection;

   protected MQTTSessionState state;

   private boolean stopped = false;

   private MQTTLogger log = MQTTLogger.LOGGER;

   private MQTTProtocolManager protocolManager;

   private boolean isClean;

   private WildcardConfiguration wildcardConfiguration;

   private CoreMessageObjectPools coreMessageObjectPools = new CoreMessageObjectPools();

   public MQTTSession(MQTTProtocolHandler protocolHandler,
                      MQTTConnection connection,
                      MQTTProtocolManager protocolManager,
                      WildcardConfiguration wildcardConfiguration) throws Exception {
      this.protocolHandler = protocolHandler;
      this.protocolManager = protocolManager;
      this.wildcardConfiguration = wildcardConfiguration;

      this.connection = connection;

      mqttConnectionManager = new MQTTConnectionManager(this);
      mqttPublishManager = new MQTTPublishManager(this);
      sessionCallback = new MQTTSessionCallback(this, connection);
      subscriptionManager = new MQTTSubscriptionManager(this);
      retainMessageManager = new MQTTRetainMessageManager(this);

      log.debug("SESSION CREATED: " + id);
   }

   // Called after the client has Connected.
   synchronized void start() throws Exception {
      mqttPublishManager.start();
      subscriptionManager.start();
      stopped = false;
   }

   // TODO ensure resources are cleaned up for GC.
   synchronized void stop() throws Exception {
      if (!stopped) {
         protocolHandler.stop();
         subscriptionManager.stop();
         mqttPublishManager.stop();

         if (serverSession != null) {
            serverSession.stop();
            serverSession.close(false);
         }

         if (state != null) {
            state.setAttached(false);
         }

         if (isClean()) {
            clean();
         }
      }
      stopped = true;
   }

   boolean getStopped() {
      return stopped;
   }

   boolean isClean() {
      return isClean;
   }

   void setIsClean(boolean isClean) throws Exception {
      this.isClean = isClean;
      if (isClean) {
         clean();
      }
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

   MQTTSessionState getSessionState() {
      return state;
   }

   ServerSessionImpl getServerSession() {
      return serverSession;
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

   void setServerSession(ServerSessionImpl serverSession) {
      this.serverSession = serverSession;
   }

   void setSessionState(MQTTSessionState state) {
      this.state = state;
      state.setAttached(true);
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
}
