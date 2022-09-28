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

import org.apache.activemq.artemis.core.protocol.mqtt.MQTTReasonCodes;
import org.apache.activemq.artemis.tests.integration.mqtt5.MQTT5TestSupport;
import org.apache.activemq.artemis.tests.util.RandomUtil;
import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptionsBuilder;
import org.eclipse.paho.mqttv5.common.MqttSubscription;
import org.junit.Test;

public class SubscribeTestsWithSecurity extends MQTT5TestSupport {

   public SubscribeTestsWithSecurity(String protocol) {
      super(protocol);
   }

   @Override
   public boolean isSecurityEnabled() {
      return true;
   }

   @Test(timeout = DEFAULT_TIMEOUT)
   public void testAuthorizationFailure() throws Exception {
      final String CLIENT_ID = "consumer";
      final int SUBSCRIPTION_COUNT = 10;
      MqttConnectionOptions options = new MqttConnectionOptionsBuilder()
         .username(noprivUser)
         .password(noprivPass.getBytes(StandardCharsets.UTF_8))
         .build();
      MqttClient client = createPahoClient(CLIENT_ID);
      client.connect(options);

      MqttSubscription[] subscriptions = new MqttSubscription[SUBSCRIPTION_COUNT];
      for (int i = 0; i < SUBSCRIPTION_COUNT; i++) {
         MqttSubscription subscription = new MqttSubscription(RandomUtil.randomString(), RandomUtil.randomInterval(0, 3));
         subscriptions[i] = subscription;
      }

      IMqttToken token = client.subscribe(subscriptions);
      int[] reasonCodes = token.getResponse().getReasonCodes();
      assertEquals(SUBSCRIPTION_COUNT, reasonCodes.length);
      for (int reasonCode : reasonCodes) {
         assertEquals(MQTTReasonCodes.NOT_AUTHORIZED, (byte) reasonCode);
      }
   }

   @Test(timeout = DEFAULT_TIMEOUT)
   public void testAuthorizationSuccess() throws Exception {
      final String CLIENT_ID = "consumer";
      final int SUBSCRIPTION_COUNT = 10;
      MqttConnectionOptions options = new MqttConnectionOptionsBuilder()
         .username(fullUser)
         .password(fullPass.getBytes(StandardCharsets.UTF_8))
         .build();
      MqttClient client = createPahoClient(CLIENT_ID);
      client.connect(options);

      MqttSubscription[] subscriptions = new MqttSubscription[SUBSCRIPTION_COUNT];
      int[] requestedQos = new int[SUBSCRIPTION_COUNT];
      for (int i = 0; i < SUBSCRIPTION_COUNT; i++) {
         requestedQos[i] = RandomUtil.randomInterval(0, 3);
         MqttSubscription subscription = new MqttSubscription(RandomUtil.randomString(), requestedQos[i]);
         subscriptions[i] = subscription;
      }

      IMqttToken token = client.subscribe(subscriptions);
      int[] reasonCodes = token.getResponse().getReasonCodes();
      assertEquals(SUBSCRIPTION_COUNT, reasonCodes.length);
      for (int i = 0; i < reasonCodes.length; i++) {
         assertEquals(requestedQos[i], reasonCodes[i]);
      }

      client.disconnect();
   }
}
