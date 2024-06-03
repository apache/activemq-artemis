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
package org.apache.activemq.artemis.tests.integration.mqtt5.ssl;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.tests.extensions.parameterized.ParameterizedTestExtension;
import org.apache.activemq.artemis.tests.extensions.parameterized.Parameters;
import org.apache.activemq.artemis.tests.integration.mqtt5.MQTT5TestSupport;
import org.apache.activemq.artemis.tests.util.RandomUtil;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class BasicSslTests extends MQTT5TestSupport {

   protected String protocol;

   public BasicSslTests(String protocol) {
      this.protocol = protocol;
   }

   @Parameters(name = "protocol={0}")
   public static Collection<Object[]> getParams() {
      return Arrays.asList(new Object[][] {
         {SSL},
         {WSS}
      });
   }

   @Override
   public boolean isUseSsl() {
      return true;
   }

   @TestTemplate
   @Timeout(value = DEFAULT_TIMEOUT, unit = TimeUnit.MILLISECONDS)
   public void testSimpleSendReceive() throws Exception {
      String topic = RandomUtil.randomString();
      byte[] body = RandomUtil.randomBytes(32);

      CountDownLatch latch = new CountDownLatch(1);
      MqttClient subscriber = createPahoClient(protocol,"subscriber");
      subscriber.connect(getSslMqttConnectOptions());
      subscriber.setCallback(new DefaultMqttCallback() {
         @Override
         public void messageArrived(String topic, MqttMessage message) {
            assertEqualsByteArrays(body, message.getPayload());
            latch.countDown();
         }
      });
      subscriber.subscribe(topic, AT_LEAST_ONCE);

      MqttClient producer = createPahoClient(protocol,"producer");
      producer.connect(getSslMqttConnectOptions());
      producer.publish(topic, body, 1, false);
      assertTrue(latch.await(500, TimeUnit.MILLISECONDS));
   }
}
