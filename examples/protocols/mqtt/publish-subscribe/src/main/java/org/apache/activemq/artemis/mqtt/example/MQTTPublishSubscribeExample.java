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
 * A simple MQTT publish and subscribe example.
 */
public class MQTTPublishSubscribeExample {

   public static void main(final String[] args) throws Exception {
      // Create a new MQTT connection to the broker.  We are not setting the client ID.  The broker will pick one for us.
      System.out.println("Connecting to Artemis using MQTT");
      MQTT mqtt = new MQTT();
      mqtt.setHost("tcp://localhost:1883");
      BlockingConnection connection = mqtt.blockingConnection();
      connection.connect();
      System.out.println("Connected to Artemis");

      // Subscribe to topics
      Topic[] topics = {new Topic("mqtt/example/publish", QoS.AT_LEAST_ONCE), new Topic("test/#", QoS.EXACTLY_ONCE), new Topic("foo/+/bar", QoS.AT_LEAST_ONCE)};
      connection.subscribe(topics);
      System.out.println("Subscribed to topics.");

      // Publish Messages
      String payload1 = "This is message 1";
      String payload2 = "This is message 2";
      String payload3 = "This is message 3";

      connection.publish("mqtt/example/publish", payload1.getBytes(), QoS.AT_LEAST_ONCE, false);
      connection.publish("test/test", payload2.getBytes(), QoS.AT_MOST_ONCE, false);
      connection.publish("foo/1/bar", payload3.getBytes(), QoS.AT_MOST_ONCE, false);
      System.out.println("Sent messages.");

      Message message1 = connection.receive(5, TimeUnit.SECONDS);
      Message message2 = connection.receive(5, TimeUnit.SECONDS);
      Message message3 = connection.receive(5, TimeUnit.SECONDS);
      System.out.println("Received messages.");

      System.out.println(new String(message1.getPayload()));
      System.out.println(new String(message2.getPayload()));
      System.out.println(new String(message3.getPayload()));
   }
}
