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

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.core.protocol.mqtt.MQTTSessionState;
import org.apache.activemq.artemis.tests.integration.mqtt5.MQTT5TestSupport;
import org.apache.activemq.artemis.tests.util.RandomUtil;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptionsBuilder;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DisconnectTestsWithSecurity extends MQTT5TestSupport {

   @Override
   public boolean isSecurityEnabled() {
      return true;
   }

   @Test
   @Timeout(DEFAULT_TIMEOUT_SEC)
   void testWillIdentityRemovedOnSend() throws Exception {
      final String willSenderId = RandomUtil.randomString();
      CountDownLatch latch = new CountDownLatch(1);
      MqttClient willConsumer = createConnectedWillConsumer(latch);
      MqttClient willSender = createConnectedWillSender(willSenderId);

      MQTTSessionState state = getSessionStates().get(willSenderId);
      assertNotNull(state);
      assertNotNull(state.getWillIdentity());
      willSender.disconnectForcibly(0, 0, false);

      assertTrue(latch.await(2, TimeUnit.SECONDS));
      assertNull(state.getWillIdentity());

      willConsumer.disconnect();
   }

   private MqttClient createConnectedWillSender(String clientId) throws MqttException {
      MqttClient willSender = createPahoClient(clientId);
      MqttProperties willMessageProperties = new MqttProperties();
      willMessageProperties.setWillDelayInterval(1L);
      MqttConnectionOptions options = new MqttConnectionOptionsBuilder()
         .sessionExpiryInterval(5L)
         .username(fullUser)
         .password(fullPass.getBytes(StandardCharsets.UTF_8))
         .will("/topic/foo", new MqttMessage(RandomUtil.randomBytes()))
         .build();
      options.setWillMessageProperties(willMessageProperties);
      willSender.connect(options);
      return willSender;
   }

   private MqttClient createConnectedWillConsumer(CountDownLatch latch) throws MqttException {
      MqttClient willConsumer = createPahoClient(RandomUtil.randomString());
      willConsumer.setCallback(new LatchedMqttCallback(latch));
      MqttConnectionOptions options = new MqttConnectionOptionsBuilder()
         .username(fullUser)
         .password(fullPass.getBytes(StandardCharsets.UTF_8))
         .build();
      willConsumer.connect(options);
      willConsumer.subscribe("/topic/foo", 1);
      return willConsumer;
   }
}
