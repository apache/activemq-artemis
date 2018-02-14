/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.mqtt.imported;

import java.util.concurrent.TimeUnit;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.config.CoreAddressConfiguration;
import org.apache.activemq.artemis.core.config.CoreQueueConfiguration;
import org.apache.activemq.artemis.core.config.WildcardConfiguration;
import org.apache.activemq.artemis.core.server.cluster.impl.MessageLoadBalancingType;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.integration.cluster.distribution.ClusterTestBase;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.junit.Test;

public class MqttClusterRemoteSubscribeTest extends ClusterTestBase {

   @Override
   protected boolean isResolveProtocols() {
      return true;
   }

   public boolean isNetty() {
      return true;
   }

   @Test
   public void unsubscribeRemoteQueue() throws Exception {
      final String TOPIC = "test/1/some/la";

      setupServers(TOPIC);

      startServers(0, 1);

      BlockingConnection connection1 = null;
      BlockingConnection connection2 = null;
      try {

         connection1 = retrieveMQTTConnection("tcp://localhost:61616");
         connection2 = retrieveMQTTConnection("tcp://localhost:61617");
         // Subscribe to topics
         Topic[] topics = {new Topic(TOPIC, QoS.AT_MOST_ONCE)};
         connection1.subscribe(topics);
         connection2.subscribe(topics);

         waitForBindings(0, TOPIC, 1, 1, true);
         waitForBindings(1, TOPIC, 1, 1, true);

         waitForBindings(0, TOPIC, 1, 1, false);
         waitForBindings(1, TOPIC, 1, 1, false);


         // Publish Messages
         String payload1 = "This is message 1";
         String payload2 = "This is message 2";
         String payload3 = "This is message 3";

         connection1.publish(TOPIC, payload1.getBytes(), QoS.AT_LEAST_ONCE, false);
         connection1.publish(TOPIC, payload2.getBytes(), QoS.AT_MOST_ONCE, false);
         connection1.publish(TOPIC, payload3.getBytes(), QoS.AT_MOST_ONCE, false);


         Message message1 = connection1.receive(5, TimeUnit.SECONDS);
         message1.ack();
         Message message2 = connection2.receive(5, TimeUnit.SECONDS);
         message2.ack();
         Message message3 = connection1.receive(5, TimeUnit.SECONDS);
         message3.ack();

         assertEquals(payload1, new String(message1.getPayload()));
         assertEquals(payload2, new String(message2.getPayload()));
         assertEquals(payload3, new String(message3.getPayload()));


         connection2.unsubscribe(new String[]{TOPIC});

         connection1.publish(TOPIC, payload1.getBytes(), QoS.AT_LEAST_ONCE, false);
         connection1.publish(TOPIC, payload2.getBytes(), QoS.AT_MOST_ONCE, false);
         connection1.publish(TOPIC, payload3.getBytes(), QoS.AT_MOST_ONCE, false);

         Message message11 = connection1.receive(5, TimeUnit.SECONDS);
         message11.ack();
         Message message21 = connection1.receive(5, TimeUnit.SECONDS);
         message21.ack();
         Message message31 = connection1.receive(5, TimeUnit.SECONDS);
         message31.ack();


         String message11String = new String(message11.getPayload());
         String message21String = new String(message21.getPayload());
         String message31String = new String(message31.getPayload());
         assertTrue(payload1.equals(message11String) || payload1.equals(message21String) || payload1.equals(message31String) );
         assertTrue(payload2.equals(message11String) || payload2.equals(message21String) || payload2.equals(message31String) );
         assertTrue(payload3.equals(message11String) || payload3.equals(message21String) || payload3.equals(message31String) );


      } finally {
         String[] topics = new String[]{TOPIC};
         if (connection1 != null) {
            connection1.unsubscribe(topics);
            connection1.disconnect();
         }
         if (connection2 != null) {
            connection2.unsubscribe(topics);
            connection2.disconnect();
         }
      }

   }

   @Test
   public void unsubscribeRemoteQueueWildCard() throws Exception {
      final String TOPIC = "test/+/some/#";

      setupServers(TOPIC);

      startServers(0, 1);

      BlockingConnection connection1 = null;
      BlockingConnection connection2 = null;
      try {

         connection1 = retrieveMQTTConnection("tcp://localhost:61616");
         connection2 = retrieveMQTTConnection("tcp://localhost:61617");
         // Subscribe to topics
         Topic[] topics = {new Topic(TOPIC, QoS.AT_MOST_ONCE)};
         connection1.subscribe(topics);
         connection2.subscribe(topics);

         waitForBindings(0, TOPIC, 1, 1, true);
         waitForBindings(1, TOPIC, 1, 1, true);

         waitForBindings(0, TOPIC, 1, 1, false);
         waitForBindings(1, TOPIC, 1, 1, false);


         // Publish Messages
         String payload1 = "This is message 1";
         String payload2 = "This is message 2";
         String payload3 = "This is message 3";

         connection1.publish("test/1/some/la", payload1.getBytes(), QoS.AT_LEAST_ONCE, false);
         connection1.publish("test/1/some/la", payload2.getBytes(), QoS.AT_MOST_ONCE, false);
         connection1.publish("test/1/some/la", payload3.getBytes(), QoS.AT_MOST_ONCE, false);


         Message message1 = connection1.receive(5, TimeUnit.SECONDS);
         message1.ack();
         Message message2 = connection2.receive(5, TimeUnit.SECONDS);
         message2.ack();
         Message message3 = connection1.receive(5, TimeUnit.SECONDS);
         message3.ack();

         assertEquals(payload1, new String(message1.getPayload()));
         assertEquals(payload2, new String(message2.getPayload()));
         assertEquals(payload3, new String(message3.getPayload()));


         connection2.unsubscribe(new String[]{TOPIC});

         connection1.publish("test/1/some/la", payload1.getBytes(), QoS.AT_LEAST_ONCE, false);
         connection1.publish("test/1/some/la", payload2.getBytes(), QoS.AT_MOST_ONCE, false);
         connection1.publish("test/1/some/la", payload3.getBytes(), QoS.AT_MOST_ONCE, false);

         Message message11 = connection1.receive(5, TimeUnit.SECONDS);
         message11.ack();
         Message message21 = connection1.receive(5, TimeUnit.SECONDS);
         message21.ack();
         Message message31 = connection1.receive(5, TimeUnit.SECONDS);
         message31.ack();

         String message11String = new String(message11.getPayload());
         String message21String = new String(message21.getPayload());
         String message31String = new String(message31.getPayload());

         assertTrue(payload1.equals(message11String) || payload1.equals(message21String) || payload1.equals(message31String));
         assertTrue(payload2.equals(message11String) || payload2.equals(message21String) || payload2.equals(message31String));
         assertTrue(payload3.equals(message11String) || payload3.equals(message21String) || payload3.equals(message31String));


      } finally {
         String[] topics = new String[]{TOPIC};
         if (connection1 != null) {
            connection1.unsubscribe(topics);
            connection1.disconnect();
         }
         if (connection2 != null) {
            connection2.unsubscribe(topics);
            connection2.disconnect();
         }
      }

   }

   @Test
   public void unsubscribeRemoteQueueMultipleSubscriptions() throws Exception {
      final String TOPIC = "test/1/some/la";
      final String TOPIC2 = "sample";

      setupServers(TOPIC);

      startServers(0, 1);

      BlockingConnection connection1 = null;
      BlockingConnection connection2 = null;
      try {

         connection1 = retrieveMQTTConnection("tcp://localhost:61616");
         connection2 = retrieveMQTTConnection("tcp://localhost:61617");
         // Subscribe to topics
         connection1.subscribe(new Topic[]{new Topic(TOPIC, QoS.AT_MOST_ONCE)});
         connection2.subscribe(new Topic[]{new Topic(TOPIC, QoS.AT_MOST_ONCE), new Topic(TOPIC2, QoS.AT_MOST_ONCE)});

         waitForBindings(0, TOPIC, 1, 1, true);
         waitForBindings(1, TOPIC, 1, 1, true);

         waitForBindings(0, TOPIC, 1, 1, false);
         waitForBindings(1, TOPIC, 1, 1, false);



         // Publish Messages
         String payload1 = "This is message 1";
         String payload2 = "This is message 2";
         String payload3 = "This is message 3";
         String payload4 = "This is message 4";

         connection1.publish(TOPIC, payload1.getBytes(), QoS.AT_LEAST_ONCE, false);
         connection1.publish(TOPIC, payload2.getBytes(), QoS.AT_MOST_ONCE, false);
         connection1.publish(TOPIC, payload3.getBytes(), QoS.AT_MOST_ONCE, false);
         connection1.publish(TOPIC2, payload4.getBytes(), QoS.AT_MOST_ONCE, false);


         Message message1 = connection1.receive(5, TimeUnit.SECONDS);
         message1.ack();
         Message message2 = connection2.receive(5, TimeUnit.SECONDS);
         message2.ack();
         Message message3 = connection1.receive(5, TimeUnit.SECONDS);
         message3.ack();
         Message message4 = connection2.receive(5, TimeUnit.SECONDS);
         message4.ack();

         assertEquals(payload1, new String(message1.getPayload()));
         assertEquals(payload2, new String(message2.getPayload()));
         assertEquals(payload3, new String(message3.getPayload()));
         assertEquals(payload4, new String(message4.getPayload()));

         connection2.unsubscribe(new String[]{TOPIC});

         connection1.publish(TOPIC, payload1.getBytes(), QoS.AT_LEAST_ONCE, false);
         connection1.publish(TOPIC, payload2.getBytes(), QoS.AT_MOST_ONCE, false);
         connection1.publish(TOPIC, payload3.getBytes(), QoS.AT_MOST_ONCE, false);
         connection1.publish(TOPIC2, payload4.getBytes(), QoS.AT_MOST_ONCE, false);

         Message message11 = connection1.receive(5, TimeUnit.SECONDS);
         message11.ack();
         Message message21 = connection1.receive(5, TimeUnit.SECONDS);
         message21.ack();
         Message message31 = connection1.receive(5, TimeUnit.SECONDS);
         message31.ack();
         Message message41 = connection2.receive(5, TimeUnit.SECONDS);
         message41.ack();

         String message11String = new String(message31.getPayload());
         String message21String = new String(message21.getPayload());
         String message31String = new String(message11.getPayload());
         assertTrue(payload1.equals(message11String) || payload1.equals(message21String) || payload1.equals(message31String));
         assertTrue(payload2.equals(message11String) || payload2.equals(message21String) || payload2.equals(message31String));
         assertTrue(payload3.equals(message11String) || payload3.equals(message21String) || payload3.equals(message31String));
         assertEquals(payload4, new String(message41.getPayload()));


      } finally {
         if (connection1 != null) {
            connection1.unsubscribe(new String[]{TOPIC});
            connection1.disconnect();
         }
         if (connection2 != null) {
            connection2.unsubscribe(new String[]{TOPIC, TOPIC2});
            connection2.disconnect();
         }
      }

   }

   @Test
   public void unsubscribeExistingQueue() throws Exception {
      final String TOPIC = "test/1/some/la";

      setupServers(TOPIC);

      startServers(0, 1);
      BlockingConnection connection1 = null;
      BlockingConnection connection2 = null;
      BlockingConnection connection3 = null;
      try {

         connection1 = retrieveMQTTConnection("tcp://localhost:61616");
         connection2 = retrieveMQTTConnection("tcp://localhost:61617");
         connection3 = retrieveMQTTConnection("tcp://localhost:61617");
         // Subscribe to topics
         Topic[] topics = {new Topic(TOPIC, QoS.AT_MOST_ONCE)};
         connection1.subscribe(topics);
         connection2.subscribe(topics);
         connection3.subscribe(topics);


         waitForBindings(0, TOPIC, 1, 1, true);
         waitForBindings(1, TOPIC, 1, 2, true);

         waitForBindings(0, TOPIC, 1, 2, false);
         waitForBindings(1, TOPIC, 1, 1, false);


         // Publish Messages
         String payload1 = "This is message 1";
         String payload2 = "This is message 2";
         String payload3 = "This is message 3";
         String payload4 = "This is message 4";

         connection1.publish(TOPIC, payload1.getBytes(), QoS.AT_LEAST_ONCE, false);
         connection1.publish(TOPIC, payload2.getBytes(), QoS.AT_MOST_ONCE, false);
         connection1.publish(TOPIC, payload3.getBytes(), QoS.AT_MOST_ONCE, false);
         connection1.publish(TOPIC, payload4.getBytes(), QoS.AT_MOST_ONCE, false);

         Message message1 = connection1.receive(5, TimeUnit.SECONDS);
         message1.ack();
         Message message2 = connection2.receive(5, TimeUnit.SECONDS);
         message2.ack();
         Message message3 = connection1.receive(5, TimeUnit.SECONDS);
         message3.ack();
         Message message4 = connection3.receive(5, TimeUnit.SECONDS);
         message4.ack();

         assertEquals(payload1, new String(message1.getPayload()));
         assertEquals(payload2, new String(message2.getPayload()));
         assertEquals(payload3, new String(message3.getPayload()));
         assertEquals(payload4, new String(message4.getPayload()));


         connection2.unsubscribe(new String[]{TOPIC});

         connection1.publish(TOPIC, payload1.getBytes(), QoS.AT_LEAST_ONCE, false);
         connection1.publish(TOPIC, payload2.getBytes(), QoS.AT_MOST_ONCE, false);
         connection1.publish(TOPIC, payload3.getBytes(), QoS.AT_MOST_ONCE, false);

         Message message11 = connection1.receive(5, TimeUnit.SECONDS);
         message11.ack();
         Message message21 = connection3.receive(5, TimeUnit.SECONDS);
         message21.ack();
         Message message31 = connection1.receive(5, TimeUnit.SECONDS);
         message31.ack();

         String message11String = new String(message11.getPayload());
         String message21String = new String(message21.getPayload());
         String message31String = new String(message31.getPayload());
         assertTrue(payload1.equals(message11String) || payload1.equals(message21String) || payload1.equals(message31String));
         assertTrue(payload2.equals(message11String) || payload2.equals(message21String) || payload2.equals(message31String));
         assertTrue(payload3.equals(message11String) || payload3.equals(message21String) || payload3.equals(message31String));

      } finally {
         String[] topics = new String[]{TOPIC};
         if (connection1 != null) {
            connection1.unsubscribe(topics);
            connection1.disconnect();
         }
         if (connection2 != null) {
            connection2.unsubscribe(topics);
            connection2.disconnect();
         }
         if (connection3 != null) {
            connection3.unsubscribe(topics);
            connection3.disconnect();
         }

      }

   }

   private static BlockingConnection retrieveMQTTConnection(String host) throws Exception {
      MQTT mqtt = new MQTT();
      mqtt.setHost(host);
      BlockingConnection connection = mqtt.blockingConnection();
      connection.connect();
      return connection;
   }

   private void setupServers(String TOPIC) throws Exception {
      WildcardConfiguration wildcardConfiguration = createWildCardConfiguration();
      CoreAddressConfiguration coreAddressConfiguration = createAddressConfiguration(TOPIC);
      AddressSettings addressSettings = createAddressSettings();


      setupServer(0, false, isNetty());
      servers[0].getConfiguration().setWildCardConfiguration(wildcardConfiguration);
      servers[0].getConfiguration().addAddressConfiguration(coreAddressConfiguration);
      servers[0].getConfiguration().addAddressesSetting("#", addressSettings);
      setupServer(1, false, isNetty());
      servers[1].getConfiguration().setWildCardConfiguration(wildcardConfiguration);
      servers[1].getConfiguration().addAddressConfiguration(coreAddressConfiguration);
      servers[1].getConfiguration().addAddressesSetting("#", addressSettings);

      setupClusterConnection("cluster0", "", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 0, 1);
      setupClusterConnection("cluster1", "", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 1, 0);
   }

   private AddressSettings createAddressSettings() {
      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setRedistributionDelay(0);
      addressSettings.setDefaultAddressRoutingType(RoutingType.ANYCAST);
      return addressSettings;
   }

   private CoreAddressConfiguration createAddressConfiguration(String TOPIC) {
      CoreAddressConfiguration coreAddressConfiguration = new CoreAddressConfiguration();
      coreAddressConfiguration.addRoutingType(RoutingType.ANYCAST);
      coreAddressConfiguration.setName(TOPIC);
      CoreQueueConfiguration coreQueueConfiguration = new CoreQueueConfiguration();
      coreQueueConfiguration.setName(TOPIC);
      coreQueueConfiguration.setAddress(TOPIC);
      coreQueueConfiguration.setRoutingType(RoutingType.ANYCAST);
      coreAddressConfiguration.addQueueConfiguration(coreQueueConfiguration);
      return coreAddressConfiguration;
   }

   private WildcardConfiguration createWildCardConfiguration() {
      WildcardConfiguration wildcardConfiguration = new WildcardConfiguration();
      wildcardConfiguration.setAnyWords('#');
      wildcardConfiguration.setDelimiter('/');
      wildcardConfiguration.setRoutingEnabled(true);
      wildcardConfiguration.setSingleWord('+');
      return wildcardConfiguration;
   }

}
