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
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.FileDeploymentManager;
import org.apache.activemq.artemis.core.config.impl.FileConfiguration;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.junit.Test;

public class MqttClusterRemoteSubscribeTest extends ActiveMQTestBase {

   @Test
   public void unsubscribeRemoteQueue() throws Exception {
      ActiveMQServerImpl server1 = initServer("mqttClusterRemoteSubscribe/broker1.xml", "broker1Unsubscribe");
      ActiveMQServerImpl server2 = initServer("mqttClusterRemoteSubscribe/broker2.xml", "broker2Unsubscribe");
      BlockingConnection connection1 = null;
      BlockingConnection connection2 = null;
      try {
         server1.start();
         server2.start();

         while (!server1.isStarted() || !server2.isStarted()) {
            Thread.sleep(50);
         }

         connection1 = retrieveMQTTConnection("tcp://localhost:1883");
         connection2 = retrieveMQTTConnection("tcp://localhost:1884");
         // Subscribe to topics
         Topic[] topics = {new Topic("test/1/some/la", QoS.AT_MOST_ONCE)};
         connection1.subscribe(topics);
         connection2.subscribe(topics);


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


         connection2.unsubscribe(new String[]{"test/1/some/la"});

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
         assertTrue(payload1.equals(message11String) || payload1.equals(message21String) || payload1.equals(message31String) );
         assertTrue(payload2.equals(message11String) || payload2.equals(message21String) || payload2.equals(message31String) );
         assertTrue(payload3.equals(message11String) || payload3.equals(message21String) || payload3.equals(message31String) );


      } finally {
         if (connection1 != null) {
            connection1.disconnect();
         }
         if (connection2 != null) {
            connection2.disconnect();
         }
         if (server2.isStarted()) {
            server2.stop();
         }
         if (server1.isStarted()) {
            server1.stop();
         }
      }

   }

   @Test
   public void unsubscribeRemoteQueueWildCard() throws Exception {
      ActiveMQServerImpl server1 = initServer("mqttClusterRemoteSubscribe/broker1.xml", "broker1Unsubscribe");
      ActiveMQServerImpl server2 = initServer("mqttClusterRemoteSubscribe/broker2.xml", "broker2Unsubscribe");
      BlockingConnection connection1 = null;
      BlockingConnection connection2 = null;
      try {
         server1.start();
         server2.start();

         while (!server1.isStarted() || !server2.isStarted()) {
            Thread.sleep(50);
         }

         connection1 = retrieveMQTTConnection("tcp://localhost:1883");
         connection2 = retrieveMQTTConnection("tcp://localhost:1884");
         // Subscribe to topics
         Topic[] topics = {new Topic("test/+/some/#", QoS.AT_MOST_ONCE)};
         connection1.subscribe(topics);
         connection2.subscribe(topics);


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


         connection2.unsubscribe(new String[]{"test/+/some/#"});

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
         if (connection1 != null) {
            connection1.disconnect();
         }
         if (connection2 != null) {
            connection2.disconnect();
         }
         if (server2.isStarted()) {
            server2.stop();
         }
         if (server1.isStarted()) {
            server1.stop();
         }
      }

   }

   @Test
   public void unsubscribeRemoteQueueMultipleSubscriptions() throws Exception {
      ActiveMQServerImpl server1 = initServer("mqttClusterRemoteSubscribe/broker1.xml", "broker1Unsubscribe");
      ActiveMQServerImpl server2 = initServer("mqttClusterRemoteSubscribe/broker2.xml", "broker2Unsubscribe");
      BlockingConnection connection1 = null;
      BlockingConnection connection2 = null;
      try {
         server1.start();
         server2.start();

         while (!server1.isStarted() || !server2.isStarted()) {
            Thread.sleep(50);
         }

         connection1 = retrieveMQTTConnection("tcp://localhost:1883");
         connection2 = retrieveMQTTConnection("tcp://localhost:1884");
         // Subscribe to topics
         connection1.subscribe(new Topic[]{new Topic("test/1/some/la", QoS.AT_MOST_ONCE)});
         connection2.subscribe(new Topic[]{new Topic("test/1/some/la", QoS.AT_MOST_ONCE), new Topic("sample", QoS.AT_MOST_ONCE)});


         // Publish Messages
         String payload1 = "This is message 1";
         String payload2 = "This is message 2";
         String payload3 = "This is message 3";
         String payload4 = "This is message 4";

         connection1.publish("test/1/some/la", payload1.getBytes(), QoS.AT_LEAST_ONCE, false);
         connection1.publish("test/1/some/la", payload2.getBytes(), QoS.AT_MOST_ONCE, false);
         connection1.publish("test/1/some/la", payload3.getBytes(), QoS.AT_MOST_ONCE, false);
         connection1.publish("sample", payload4.getBytes(), QoS.AT_MOST_ONCE, false);


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

         connection2.unsubscribe(new String[]{"test/1/some/la"});

         connection1.publish("test/1/some/la", payload1.getBytes(), QoS.AT_LEAST_ONCE, false);
         connection1.publish("test/1/some/la", payload2.getBytes(), QoS.AT_MOST_ONCE, false);
         connection1.publish("test/1/some/la", payload3.getBytes(), QoS.AT_MOST_ONCE, false);
         connection1.publish("sample", payload4.getBytes(), QoS.AT_MOST_ONCE, false);

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
            connection1.disconnect();
         }
         if (connection2 != null) {
            connection2.disconnect();
         }
         if (server2.isStarted()) {
            server2.stop();
         }
         if (server1.isStarted()) {
            server1.stop();
         }
      }

   }

   @Test
   public void unsubscribeExistingQueue() throws Exception {
      ActiveMQServerImpl server1 = initServer("mqttClusterRemoteSubscribe/broker1.xml", "broker1Unsubscribe");
      ActiveMQServerImpl server2 = initServer("mqttClusterRemoteSubscribe/broker2.xml", "broker2Unsubscribe");
      BlockingConnection connection1 = null;
      BlockingConnection connection2 = null;
      BlockingConnection connection3 = null;
      try {
         server1.start();
         server2.start();

         while (!server1.isStarted() || !server2.isStarted()) {
            Thread.sleep(50);
         }

         connection1 = retrieveMQTTConnection("tcp://localhost:1883");
         connection2 = retrieveMQTTConnection("tcp://localhost:1884");
         connection3 = retrieveMQTTConnection("tcp://localhost:1884");
         // Subscribe to topics
         Topic[] topics = {new Topic("test/1/some/la", QoS.AT_MOST_ONCE)};
         connection1.subscribe(topics);
         connection2.subscribe(topics);
         connection3.subscribe(topics);


         // Publish Messages
         String payload1 = "This is message 1";
         String payload2 = "This is message 2";
         String payload3 = "This is message 3";
         String payload4 = "This is message 4";

         connection1.publish("test/1/some/la", payload1.getBytes(), QoS.AT_LEAST_ONCE, false);
         connection1.publish("test/1/some/la", payload2.getBytes(), QoS.AT_MOST_ONCE, false);
         connection1.publish("test/1/some/la", payload3.getBytes(), QoS.AT_MOST_ONCE, false);
         connection1.publish("test/1/some/la", payload4.getBytes(), QoS.AT_MOST_ONCE, false);

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


         connection2.unsubscribe(new String[]{"test/1/some/la"});

         connection1.publish("test/1/some/la", payload1.getBytes(), QoS.AT_LEAST_ONCE, false);
         connection1.publish("test/1/some/la", payload2.getBytes(), QoS.AT_MOST_ONCE, false);
         connection1.publish("test/1/some/la", payload3.getBytes(), QoS.AT_MOST_ONCE, false);

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
         if (connection1 != null) {
            connection1.disconnect();
         }
         if (connection2 != null) {
            connection2.disconnect();
         }
         if (connection3 != null) {
            connection3.disconnect();
         }
         if (server2.isStarted()) {
            server2.stop();
         }
         if (server1.isStarted()) {
            server1.stop();
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

   private ActiveMQServerImpl initServer(String configFile, String name) throws Exception {
      Configuration configuration = createConfiguration(configFile, name);
      return new ActiveMQServerImpl(configuration);
   }

   protected Configuration createConfiguration(String fileName, String name) throws Exception {
      FileConfiguration fc = new FileConfiguration();
      FileDeploymentManager deploymentManager = new FileDeploymentManager(fileName);
      deploymentManager.addDeployable(fc);

      deploymentManager.readConfiguration();

      // we need this otherwise the data folder will be located under activemq-server and not on the temporary directory
      fc.setPagingDirectory(getTestDir() + "/" + name + "/" + fc.getPagingDirectory());
      fc.setLargeMessagesDirectory(getTestDir() + "/" + name + "/" + fc.getLargeMessagesDirectory());
      fc.setJournalDirectory(getTestDir() + "/" + name + "/" + fc.getJournalDirectory());
      fc.setBindingsDirectory(getTestDir() + "/" + name + "/" + fc.getBindingsDirectory());

      return fc;
   }
}
