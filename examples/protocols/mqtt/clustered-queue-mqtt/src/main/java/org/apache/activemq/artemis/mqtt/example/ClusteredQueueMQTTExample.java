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
package org.apache.activemq.artemis.mqtt.example;

import java.util.concurrent.TimeUnit;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;

/**
 * A simple example that demonstrates an MQTT subscription on different nodes of the cluster.
 */
public class ClusteredQueueMQTTExample {

   public static void main(final String[] args) throws Exception {
      // Create a new MQTT connection to the broker.  We are not setting the client ID.  The broker will pick one for us.
      System.out.println("Connecting to Artemis using MQTT");
      BlockingConnection connection1 = retrieveMQTTConnection("tcp://localhost:1883");
      System.out.println("Connected to Artemis 1");
      BlockingConnection connection2 = retrieveMQTTConnection("tcp://localhost:1884");
      System.out.println("Connected to Artemis 2");

      // Subscribe to topics
      Topic[] topics = {new Topic("test/+/some/#", QoS.AT_MOST_ONCE)};
      connection1.subscribe(topics);
      connection2.subscribe(topics);
      System.out.println("Subscribed to topics.");

      // Publish Messages
      String payload1 = "This is message 1";
      String payload2 = "This is message 2";
      String payload3 = "This is message 3";

      connection1.publish("test/1/some/la", payload1.getBytes(), QoS.AT_LEAST_ONCE, false);
      connection1.publish("test/1/some/la", payload2.getBytes(), QoS.AT_MOST_ONCE, false);
      connection1.publish("test/1/some/la", payload3.getBytes(), QoS.AT_MOST_ONCE, false);
      System.out.println("Sent messages.");

      Message message1 = connection1.receive(5, TimeUnit.SECONDS);
      Message message2 = connection1.receive(5, TimeUnit.SECONDS);
      Message message3 = connection1.receive(5, TimeUnit.SECONDS);
      Message message4 = connection2.receive(5, TimeUnit.SECONDS);
      Message message5 = connection2.receive(5, TimeUnit.SECONDS);
      Message message6 = connection2.receive(5, TimeUnit.SECONDS);
      System.out.println("Received messages.");

      System.out.println("Broker 1: " + new String(message1.getPayload()));
      System.out.println("Broker 1: " + new String(message2.getPayload()));
      System.out.println("Broker 1: " + new String(message3.getPayload()));
      System.out.println("Broker 2: " + new String(message4.getPayload()));
      System.out.println("Broker 2: " + new String(message5.getPayload()));
      System.out.println("Broker 2: " + new String(message6.getPayload()));
   }

   private static BlockingConnection retrieveMQTTConnection(String host) throws Exception {
      MQTT mqtt = new MQTT();
      mqtt.setHost(host);
      mqtt.setUserName("admin");
      mqtt.setPassword("admin");
      BlockingConnection connection = mqtt.blockingConnection();
      connection.connect();
      return connection;
   }

}
