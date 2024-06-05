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

import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.apache.activemq.artemis.tests.extensions.parameterized.ParameterizedTestExtension;
import org.apache.activemq.artemis.tests.extensions.parameterized.Parameters;
import org.apache.activemq.artemis.tests.integration.mqtt5.MQTT5TestSupport;
import org.apache.activemq.artemis.tests.util.RandomUtil;
import org.apache.activemq.artemis.utils.Wait;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class CertificateAuthenticationSslTests extends MQTT5TestSupport {

   static {
      String path = System.getProperty("java.security.auth.login.config");
      if (path == null) {
         URL resource = CertificateAuthenticationSslTests.class.getClassLoader().getResource("login.config");
         if (resource != null) {
            path = resource.getFile();
            System.setProperty("java.security.auth.login.config", path);
         }
      }
   }

   protected String protocol;

   public CertificateAuthenticationSslTests(String protocol) {
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

   @Override
   public boolean isMutualSsl() {
      return true;
   }

   @Override
   public boolean isSecurityEnabled() {
      return true;
   }

   @Override
   protected void configureBrokerSecurity(ActiveMQServer server) {
      server.setSecurityManager(new ActiveMQJAASSecurityManager("CertLogin"));
      server.getConfiguration().setSecurityEnabled(true);
      HashSet<Role> roles = new HashSet<>();
      roles.add(new Role("programmers", true, true, true, false, false, false, false, false, true, true, false, false));
      server.getConfiguration().putSecurityRoles("#", roles);
   }

   /*
    * Basic mutual SSL test with certificate-based authentication
    */
   @TestTemplate
   @Timeout(DEFAULT_TIMEOUT_SEC)
   public void testSimpleSendReceive() throws Exception {
      final String topic = RandomUtil.randomString();
      final String clientId = "subscriber";
      byte[] body = RandomUtil.randomBytes(32);

      CountDownLatch latch = new CountDownLatch(1);
      MqttClient subscriber = createPahoClient(protocol, clientId);
      subscriber.connect(getSslMqttConnectOptions());
      subscriber.setCallback(new DefaultMqttCallback() {
         @Override
         public void messageArrived(String topic, MqttMessage message) {
            assertEqualsByteArrays(body, message.getPayload());
            latch.countDown();
         }
      });
      subscriber.subscribe(topic, AT_LEAST_ONCE);

      Wait.assertTrue(() -> getSubscriptionQueue(topic, clientId) != null, 2000, 100);
      Wait.assertEquals(1, () -> getSubscriptionQueue(topic, clientId).getConsumerCount(), 2000, 100);

      MqttClient producer = createPahoClient(protocol,"producer");
      producer.connect(getSslMqttConnectOptions());
      producer.publish(topic, body, 1, false);
      assertTrue(latch.await(500, TimeUnit.MILLISECONDS));
   }
}
