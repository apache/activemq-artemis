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

package org.apache.activemq.artemis.tests.integration.mqtt5.spec.controlpackets;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.core.protocol.mqtt.MQTTReasonCodes;
import org.apache.activemq.artemis.tests.integration.mqtt5.MQTT5TestSupport;
import org.apache.activemq.artemis.tests.util.RandomUtil;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptionsBuilder;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.jboss.logging.Logger;
import org.junit.Test;

public class PublishTestsWithSecurity extends MQTT5TestSupport {

   private static final Logger log = Logger.getLogger(PublishTestsWithSecurity.class);

   public PublishTestsWithSecurity(String protocol) {
      super(protocol);
   }

   @Override
   public boolean isSecurityEnabled() {
      return true;
   }

   @Test(timeout = DEFAULT_TIMEOUT)
   public void testAuthorizationFailure() throws Exception {
      final String CLIENT_ID = "publisher";
      MqttConnectionOptions options = new MqttConnectionOptionsBuilder()
         .username(noprivUser)
         .password(noprivPass.getBytes(StandardCharsets.UTF_8))
         .build();
      MqttClient client = createPahoClient(CLIENT_ID);
      client.connect(options);

      try {
         client.publish("/foo", new byte[0], 2, false);
         fail("Publishing should have failed with a security problem");
      } catch (MqttException e) {
         assertEquals(MQTTReasonCodes.NOT_AUTHORIZED, (byte) e.getReasonCode());
      } catch (Exception e) {
         fail("Should have thrown an MqttException");
      }

      assertFalse(client.isConnected());
   }

   @Test(timeout = DEFAULT_TIMEOUT)
   public void testAuthorizationSuccess() throws Exception {
      final String CLIENT_ID = "publisher";
      MqttConnectionOptions options = new MqttConnectionOptionsBuilder()
         .username(fullUser)
         .password(fullPass.getBytes(StandardCharsets.UTF_8))
         .build();
      MqttClient client = createPahoClient(CLIENT_ID);
      client.connect(options);

      try {
         client.publish("/foo", new byte[0], 2, false);
      } catch (MqttException e) {
         fail("Publishing should not have failed with a security problem");
      } catch (Exception e) {
         fail("Should have thrown an MqttException");
      }

      client.isConnected();
   }

   @Test(timeout = DEFAULT_TIMEOUT)
   public void testWillAuthorizationSuccess() throws Exception {
      internalTestWillAuthorization(fullUser, fullPass, true);
   }

   @Test(timeout = DEFAULT_TIMEOUT)
   public void testWillAuthorizationFailure() throws Exception {
      internalTestWillAuthorization(noprivUser, noprivPass, false);
   }

   private void internalTestWillAuthorization(String username, String password, boolean succeed) throws Exception {
      final byte[] WILL = RandomUtil.randomBytes();
      final String TOPIC = RandomUtil.randomString();

      // consumer of the will message
      MqttClient client1 = createPahoClient("willConsumer");
      CountDownLatch latch = new CountDownLatch(1);
      client1.setCallback(new DefaultMqttCallback() {
         @Override
         public void messageArrived(String topic, MqttMessage message) {
            latch.countDown();
         }
      });
      MqttConnectionOptions options = new MqttConnectionOptionsBuilder()
         .username(fullUser)
         .password(fullPass.getBytes(StandardCharsets.UTF_8))
         .build();
      client1.connect(options);
      client1.subscribe(TOPIC, 1);

      // consumer to generate the will
      MqttClient client2 = createPahoClient("willGenerator");
      options = new MqttConnectionOptionsBuilder()
         .username(username)
         .password(password.getBytes(StandardCharsets.UTF_8))
         .will(TOPIC, new MqttMessage(WILL))
         .build();
      client2.connect(options);
      client2.disconnectForcibly(0, 0, false);

      assertEquals(succeed, latch.await(2, TimeUnit.SECONDS));
   }
}
