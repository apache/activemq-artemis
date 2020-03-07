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

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;

/**
 * A simple example that shows how to implement and use interceptors with ActiveMQ Artemis with the MQTT protocol.
 */
public class InterceptorExample {
   public static void main(final String[] args) throws Exception {

      System.out.println("Connecting to Artemis using MQTT");
      MQTT mqtt = new MQTT();
      mqtt.setHost("tcp://localhost:1883");

      BlockingConnection connection = mqtt.blockingConnection();
      connection.connect();

      System.out.println("Connected to Artemis");

      // Subscribe to a topic
      Topic[] topics = {new Topic("mqtt/example/interceptor", QoS.EXACTLY_ONCE)};
      connection.subscribe(topics);
      System.out.println("Subscribed to topics.");

      // Publish message
      String payload1 = "This is message 1";

      connection.publish("mqtt/example/interceptor", payload1.getBytes(), QoS.EXACTLY_ONCE, false);

      System.out.println("Sent message");

      // Receive the sent message
      Message message1 = connection.receive(5, TimeUnit.SECONDS);

      String messagePayload = new String(message1.getPayload(), StandardCharsets.UTF_8);

      System.out.println("Received message: " + messagePayload);
   }
}
