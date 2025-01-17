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
package org.apache.activemq.artemis.tests.integration.mqtt5.spec.controlpackets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import javax.security.auth.Subject;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.Optional;
import java.util.Set;

import org.apache.activemq.artemis.core.protocol.mqtt.MQTTReasonCodes;
import org.apache.activemq.artemis.spi.core.security.jaas.RolePrincipal;
import org.apache.activemq.artemis.spi.core.security.jaas.UserPrincipal;
import org.apache.activemq.artemis.tests.integration.mqtt5.MQTT5TestSupport;
import org.apache.activemq.artemis.tests.util.RandomUtil;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptionsBuilder;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class ConnectTestsWithSecurity extends MQTT5TestSupport {

   @Override
   public boolean isSecurityEnabled() {
      return true;
   }

   /*
    * [MQTT-3.1.4-2] The Server MAY check that the contents of the CONNECT packet meet any further restrictions and
    * SHOULD perform authentication and authorization checks. If any of these checks fail, it MUST close the Network
    * Connection.
    */
   @Test
   @Timeout(DEFAULT_TIMEOUT_SEC)
   public void testAuthenticationFailureWithBadCredentials() throws Exception {
      testAuthentication(new MqttConnectionOptionsBuilder()
                            .username(RandomUtil.randomString())
                            .password(RandomUtil.randomString().getBytes(StandardCharsets.UTF_8))
                            .build());
   }

   /*
    * [MQTT-3.1.4-2] The Server MAY check that the contents of the CONNECT packet meet any further restrictions and
    * SHOULD perform authentication and authorization checks. If any of these checks fail, it MUST close the Network
    * Connection.
    */
   @Test
   @Timeout(DEFAULT_TIMEOUT_SEC)
   public void testAuthenticationFailureWithNoCredentials() throws Exception {
      testAuthentication(new MqttConnectionOptionsBuilder().build());
   }

   private void testAuthentication(MqttConnectionOptions options) throws Exception {
      final String CLIENT_ID = RandomUtil.randomString();

      MqttClient client = createPahoClient(CLIENT_ID);
      try {
         client.connect(options);
         fail("Connecting should have failed with a security problem");
      } catch (MqttException e) {
         assertEquals(MQTTReasonCodes.BAD_USER_NAME_OR_PASSWORD, (byte) e.getReasonCode());
      } catch (Exception e) {
         fail("Should have thrown an MqttException");
      }
      assertFalse(client.isConnected());
   }

   @Test
   @Timeout(DEFAULT_TIMEOUT_SEC)
   public void testAuthenticationSuccess() throws Exception {
      final String CLIENT_ID = RandomUtil.randomString();
      MqttConnectionOptions options = new MqttConnectionOptionsBuilder()
         .username(fullUser)
         .password(fullPass.getBytes(StandardCharsets.UTF_8))
         .build();
      MqttClient client = createPahoClient(CLIENT_ID);
      try {
         client.connect(options);
      } catch (Exception e) {
         fail("Should not have thrown an Exception");
      }
      assertTrue(client.isConnected());
      client.disconnect();
   }

   /*
    * With security the will identity MUST not be null.
    */
   @Test
   @Timeout(DEFAULT_TIMEOUT_SEC)
   void testWillIdentityDefined() throws Exception {
      final String CLIENT_ID = RandomUtil.randomString();
      final byte[] WILL = RandomUtil.randomBytes();

      MqttConnectionOptions options = new MqttConnectionOptionsBuilder()
         .username(fullUser)
         .password(fullPass.getBytes(StandardCharsets.UTF_8))
         .will("/topic/foo", new MqttMessage(WILL))
         .build();
      MqttClient client = createPahoClient(CLIENT_ID);
      client.connect(options);

      Subject willIdentity = getSessionStates().get(CLIENT_ID).getWillIdentity();
      assertNotNull(willIdentity);
      Set<Principal> principals = willIdentity.getPrincipals();
      assertEquals(2, principals.size());

      Optional<UserPrincipal> user = getFrom(principals, UserPrincipal.class);
      assertTrue(user.isPresent());
      assertEquals(fullUser, user.get().getName());

      Optional<RolePrincipal> role = getFrom(principals, RolePrincipal.class);
      assertTrue(role.isPresent());
      assertEquals("full", role.get().getName());

      client.disconnect();
   }

   private <T> Optional<T> getFrom(Set<Principal> principals, Class<T> clazz) {
      return principals.stream().filter(clazz::isInstance).map(clazz::cast).findFirst();
   }
}
