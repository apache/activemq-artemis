/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.mqtt;

import javax.security.auth.Subject;
import java.util.Map;
import java.util.Set;

import org.apache.activemq.artemis.core.protocol.mqtt.MQTTProtocolManager;
import org.apache.activemq.artemis.core.protocol.mqtt.MQTTSessionState;
import org.apache.activemq.artemis.core.remoting.impl.AbstractAcceptor;
import org.apache.activemq.artemis.core.security.CheckType;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.spi.core.protocol.ProtocolManager;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.remoting.Acceptor;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager5;
import org.apache.activemq.artemis.tests.util.RandomUtil;
import org.apache.activemq.artemis.tests.util.Wait;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.junit.Test;

public class MQTTSecurityManagerTest extends MQTTTestSupport {

   private String clientID = "new-" + RandomUtil.randomString();

   @Override
   public boolean isSecurityEnabled() {
      return true;
   }

   @Override
   public void configureBroker() throws Exception {
      super.configureBroker();
      server.setSecurityManager(new ActiveMQSecurityManager5() {
         @Override
         public Subject authenticate(String user,
                                     String password,
                                     RemotingConnection remotingConnection,
                                     String securityDomain) {
            remotingConnection.setClientID(clientID);
            System.out.println("Setting: " + clientID);
            return new Subject();
         }

         @Override
         public boolean authorize(Subject subject, Set<Role> roles, CheckType checkType, String address) {
            return true;
         }

         @Override
         public boolean validateUser(String user, String password) {
            return true;
         }

         @Override
         public boolean validateUserAndRole(String user, String password, Set<Role> roles, CheckType checkType) {
            return true;
         }
      });
      server.getConfiguration().setAuthenticationCacheSize(0);
      server.getConfiguration().setAuthorizationCacheSize(0);
   }

   @Test(timeout = 30000)
   public void testSecurityManagerModifyClientID() throws Exception {
      BlockingConnection connection = null;
      try {
         MQTT mqtt = createMQTTConnection(RandomUtil.randomString(), true);
         mqtt.setUserName(fullUser);
         mqtt.setPassword(fullPass);
         mqtt.setConnectAttemptsMax(1);
         connection = mqtt.blockingConnection();
         connection.connect();
         BlockingConnection finalConnection = connection;
         assertTrue("Should be connected", Wait.waitFor(() -> finalConnection.isConnected(), 5000, 100));
         Map<String, MQTTSessionState> sessionStates = null;
         Acceptor acceptor = server.getRemotingService().getAcceptor("MQTT");
         if (acceptor instanceof AbstractAcceptor) {
            ProtocolManager protocolManager = ((AbstractAcceptor) acceptor).getProtocolMap().get("MQTT");
            if (protocolManager instanceof MQTTProtocolManager) {
               sessionStates = ((MQTTProtocolManager) protocolManager).getSessionStates();
            }
         }
         assertEquals(1, sessionStates.size());
         assertTrue(sessionStates.keySet().contains(clientID));
         for (MQTTSessionState state : sessionStates.values()) {
            assertEquals(clientID, state.getClientId());
         }
      } finally {
         if (connection != null && connection.isConnected()) connection.disconnect();
      }
   }

   @Test(timeout = 30000)
   public void testSecurityManagerModifyClientIDAndStealConnection() throws Exception {
      BlockingConnection connection1 = null;
      BlockingConnection connection2 = null;
      final String CLIENT_ID = "old-" + RandomUtil.randomString();
      try {
         MQTT mqtt = createMQTTConnection(CLIENT_ID, true);
         mqtt.setUserName(fullUser);
         mqtt.setPassword(fullPass);
         mqtt.setConnectAttemptsMax(1);
         connection1 = mqtt.blockingConnection();
         connection1.connect();
         final BlockingConnection finalConnection = connection1;
         assertTrue("Should be connected", Wait.waitFor(() -> finalConnection.isConnected(), 5000, 100));
         Map<String, MQTTSessionState> sessionStates = null;
         Acceptor acceptor = server.getRemotingService().getAcceptor("MQTT");
         if (acceptor instanceof AbstractAcceptor) {
            ProtocolManager protocolManager = ((AbstractAcceptor) acceptor).getProtocolMap().get("MQTT");
            if (protocolManager instanceof MQTTProtocolManager) {
               sessionStates = ((MQTTProtocolManager) protocolManager).getSessionStates();
            }
         }
         assertEquals(1, sessionStates.size());
         assertTrue(sessionStates.keySet().contains(clientID));
         for (MQTTSessionState state : sessionStates.values()) {
            assertEquals(clientID, state.getClientId());
         }

         connection2 = mqtt.blockingConnection();
         connection2.connect();
         final BlockingConnection finalConnection2 = connection2;
         assertTrue("Should be connected", Wait.waitFor(() -> finalConnection2.isConnected(), 5000, 100));
         Wait.assertFalse(() -> finalConnection.isConnected(), 5000, 100);
         assertEquals(1, sessionStates.size());
         assertTrue(sessionStates.keySet().contains(clientID));
         for (MQTTSessionState state : sessionStates.values()) {
            assertEquals(clientID, state.getClientId());
         }
      } finally {
         if (connection1 != null && connection1.isConnected()) connection1.disconnect();
      }
   }
}
