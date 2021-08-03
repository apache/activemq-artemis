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
package org.apache.activemq.artemis.jms.example;

import javax.net.ssl.SSLException;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.core.remoting.impl.ssl.SSLSupport;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;

public class MqttCrlEnabledExample {

   public static void main(final String[] args) throws Exception {
      boolean exception = false;
      try {
         callBroker("server-ca-truststore.jks", "securepass", "other-client-keystore.jks", "securepass");
      } catch (SSLException e) {
         exception = true;
      }
      if (!exception) {
         throw new RuntimeException("The connection should be revoked");
      }
      callBroker("server-ca-truststore.jks", "securepass", "client-keystore.jks", "securepass");
   }

   private static void callBroker(String truststorePath, String truststorePass, String keystorePath, String keystorePass) throws Exception {
      BlockingConnection connection = null;

      try {
         connection = retrieveMQTTConnection("ssl://localhost:1883", truststorePath, truststorePass, keystorePath, keystorePass);
         // Subscribe to topics
         Topic[] topics = {new Topic("test/+/some/#", QoS.AT_MOST_ONCE)};
         connection.subscribe(topics);

         // Publish Messages
         String payload = "This is message 1";

         connection.publish("test/1/some/la", payload.getBytes(), QoS.AT_LEAST_ONCE, false);

         Message message = connection.receive(5, TimeUnit.SECONDS);
         System.out.println("Message received: " + new String(message.getPayload()));

      } catch (Exception e) {
         throw e;
      } finally {
         if (connection != null) {
            connection.disconnect();
         }
      }
   }

   private static BlockingConnection retrieveMQTTConnection(String host, String truststorePath, String truststorePass, String keystorePath, String keystorePass) throws Exception {
      MQTT mqtt = new MQTT();
      mqtt.setConnectAttemptsMax(0);
      mqtt.setReconnectAttemptsMax(0);
      mqtt.setHost(host);
      mqtt.setSslContext(new SSLSupport()
                            .setKeystorePath(keystorePath)
                            .setKeystorePassword(keystorePass)
                            .setTruststorePath(truststorePath)
                            .setTruststorePassword(truststorePass)
                            .createContext());
      mqtt.setCleanSession(true);

      BlockingConnection connection = mqtt.blockingConnection();
      connection.connect();
      return connection;
   }

}