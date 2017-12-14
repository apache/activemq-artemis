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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.security.ProtectionDomain;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;

/**
 * A simple JMS Queue example that uses dual broker authentication mechanisms for SSL and non-SSL connections.
 */
public class MqttCrlEnabledExample {

   public static File basedir() throws IOException {
      ProtectionDomain protectionDomain = MqttCrlEnabledExample.class.getProtectionDomain();
      return new File(new File(protectionDomain.getCodeSource().getLocation().getPath()), "../..").getCanonicalFile();
   }


   public static void main(final String[] args) throws Exception {
      String basedir = basedir().getPath();
      boolean exception = false;
      try {
         callBroker(basedir + "/src/main/resources/activemq/client0/truststore.jks", "changeit", basedir + "/src/main/resources/activemq/client0/client_revoked.jks", "changeit");
      } catch (SSLException e) {
         exception = true;
      }
      if (!exception) {
         throw new RuntimeException("The connection should be revoked");
      }
      callBroker(basedir + "/src/main/resources/activemq/client1/truststore.jks", "changeit", basedir + "/src/main/resources/activemq/client1/client_not_revoked.jks", "changeit");
   }

   private static void callBroker(String truststorePath, String truststorePass, String keystorePath, String keystorePass) throws Exception {
      BlockingConnection connection1 = null;
      try {

         connection1 = retrieveMQTTConnection("ssl://localhost:1883", truststorePath, truststorePass, keystorePath, keystorePass);
         // Subscribe to topics
         Topic[] topics = {new Topic("test/+/some/#", QoS.AT_MOST_ONCE)};
         connection1.subscribe(topics);

         // Publish Messages
         String payload1 = "This is message 1";

         connection1.publish("test/1/some/la", payload1.getBytes(), QoS.AT_LEAST_ONCE, false);

         Message message1 = connection1.receive(5, TimeUnit.SECONDS);
         System.out.println("Message received: " + new String(message1.getPayload()));

      } catch (Exception e) {
         e.printStackTrace();
         throw e;
      } finally {
         if (connection1 != null) {
            connection1.disconnect();
         }
      }
   }

   public static TrustManager[] getTrustManager(String truststorePath, String truststorePass) throws Exception {
      TrustManager[] trustStoreManagers = null;
      KeyStore trustedCertStore = KeyStore.getInstance("jks");

      trustedCertStore.load(new FileInputStream(truststorePath), truststorePass.toCharArray());
      TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());

      tmf.init(trustedCertStore);
      trustStoreManagers = tmf.getTrustManagers();
      return trustStoreManagers;
   }

   public static KeyManager[] getKeyManager(String keystorePath, String keystorePass) throws Exception {
      KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      KeyStore ks = KeyStore.getInstance("jks");
      KeyManager[] keystoreManagers = null;

      byte[] sslCert = loadClientCredential(keystorePath);

      if (sslCert != null && sslCert.length > 0) {
         ByteArrayInputStream bin = new ByteArrayInputStream(sslCert);
         ks.load(bin, keystorePass.toCharArray());
         kmf.init(ks, keystorePass.toCharArray());
         keystoreManagers = kmf.getKeyManagers();
      }
      return keystoreManagers;
   }

   private static byte[] loadClientCredential(String fileName) throws IOException {
      if (fileName == null) {
         return null;
      }
      FileInputStream in = new FileInputStream(fileName);
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      byte[] buf = new byte[512];
      int i = in.read(buf);
      while (i > 0) {
         out.write(buf, 0, i);
         i = in.read(buf);
      }
      in.close();
      return out.toByteArray();
   }


   private static BlockingConnection retrieveMQTTConnection(String host, String truststorePath, String truststorePass, String keystorePath, String keystorePass) throws Exception {

      MQTT mqtt = new MQTT();
      mqtt.setConnectAttemptsMax(1);
      mqtt.setReconnectAttemptsMax(0);
      mqtt.setHost(host);
      mqtt.setUserName("consumer");
      mqtt.setPassword("activemq");
      SSLContext sslContext = SSLContext.getInstance("TLS");
      sslContext.init(getKeyManager(keystorePath, keystorePass), getTrustManager(truststorePath, truststorePass), null);
      mqtt.setSslContext(sslContext);

      mqtt.setCleanSession(true);


      BlockingConnection connection = mqtt.blockingConnection();
      connection.connect();
      connection.disconnect();
      connection = mqtt.blockingConnection();
      connection.connect();
      return connection;
   }

}