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
package org.apache.activemq.artemis.tests.integration.mqtt.imported;

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
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.FileDeploymentManager;
import org.apache.activemq.artemis.core.config.impl.FileConfiguration;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyAcceptorFactory;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.junit.Test;

public class MQTTSecurityCRLTest extends ActiveMQTestBase {


   protected String fullUser = "user";
   protected String fullPass = "pass";


   public File basedir() throws IOException {
      ProtectionDomain protectionDomain = getClass().getProtectionDomain();
      return new File(new File(protectionDomain.getCodeSource().getLocation().getPath()), "../..").getCanonicalFile();
   }

   @Test(expected = SSLException.class)
   public void crlRevokedTest() throws Exception {

      ActiveMQServer server1 = initServer("mqttCrl/server/broker.xml", "broker");
      BlockingConnection connection1 = null;
      try {
         server1.start();

         while (!server1.isStarted()) {
            Thread.sleep(50);
         }

         String basedir = basedir().getPath() + "/src/test/resources/mqttCrl/client0/";
         connection1 = retrieveMQTTConnection("ssl://localhost:1883", basedir + "truststore.jks", "changeit", basedir + "client_revoked.jks", "changeit");

         // Subscribe to topics
         Topic[] topics = {new Topic("test/+/some/#", QoS.AT_MOST_ONCE)};
         connection1.subscribe(topics);

         // Publish Messages
         String payload1 = "This is message 1";

         connection1.publish("test/1/some/la", payload1.getBytes(), QoS.AT_LEAST_ONCE, false);

         Message message1 = connection1.receive(5, TimeUnit.SECONDS);

         assertEquals(payload1, new String(message1.getPayload()));

      }finally {
         if (connection1 != null) {
            connection1.disconnect();
         }
         if (server1.isStarted()) {
            server1.stop();
         }
      }

   }

   @Test
   public void crlNotRevokedTest() throws Exception {

      ActiveMQServer server1 = initServer("mqttCrl/server/broker.xml", "broker");
      BlockingConnection connection1 = null;
      try {
         server1.start();

         while (!server1.isStarted()) {
            Thread.sleep(50);
         }

         String basedir = basedir().getPath() + "/src/test/resources/mqttCrl/client1/";
         connection1 = retrieveMQTTConnection("ssl://localhost:1883", basedir + "truststore.jks", "changeit", basedir + "client_not_revoked.jks", "changeit");

         // Subscribe to topics
         Topic[] topics = {new Topic("test/+/some/#", QoS.AT_MOST_ONCE)};
         connection1.subscribe(topics);

         // Publish Messages
         String payload1 = "This is message 1";

         connection1.publish("test/1/some/la", payload1.getBytes(), QoS.AT_LEAST_ONCE, false);

         Message message1 = connection1.receive(5, TimeUnit.SECONDS);

         assertEquals(payload1, new String(message1.getPayload()));

      } finally {
         if (connection1 != null) {
            connection1.disconnect();
         }
         if (server1.isStarted()) {
            server1.stop();
         }
      }

   }

   protected void configureBrokerSecurity(ActiveMQServer server) {
      ActiveMQJAASSecurityManager securityManager = (ActiveMQJAASSecurityManager) server.getSecurityManager();

      // User additions
      securityManager.getConfiguration().addUser(fullUser, fullPass);
      securityManager.getConfiguration().addRole(fullUser, "full");
   }


   private ActiveMQServer initServer(String configFile, String name) throws Exception {
      Configuration configuration = createConfiguration(configFile, name);


      ActiveMQServer server = createServer(true, configuration);
      configureBrokerSecurity(server);
      return server;
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

      addMqttTransportConfiguration(fc);

      return fc;
   }

   private void addMqttTransportConfiguration(FileConfiguration fc) throws IOException {
      String basedir = basedir().getPath() + "/src/test/resources/mqttCrl/server/";
      TransportConfiguration transportConfiguration = new TransportConfiguration(NettyAcceptorFactory.class.getCanonicalName(), null, "mqtt", null);

      transportConfiguration.getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      transportConfiguration.getParams().put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, basedir + "truststore.jks");
      transportConfiguration.getParams().put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, "changeit");
      transportConfiguration.getParams().put(TransportConstants.KEYSTORE_PATH_PROP_NAME, basedir + "keystore1.jks");
      transportConfiguration.getParams().put(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME, "changeit");
      transportConfiguration.getParams().put(TransportConstants.CRL_PATH_PROP_NAME, basedir + "root.crl.pem");
      transportConfiguration.getParams().put(TransportConstants.NEED_CLIENT_AUTH_PROP_NAME, "true");
      transportConfiguration.getParams().put(TransportConstants.PORT_PROP_NAME, "1883");
      transportConfiguration.getParams().put(TransportConstants.HOST_PROP_NAME, "localhost");
      transportConfiguration.getParams().put(TransportConstants.PROTOCOLS_PROP_NAME, "MQTT");

      fc.getAcceptorConfigurations().add(transportConfiguration);
   }

   public static TrustManager[] getTrustManager(String truststorePath, String truststorePass) throws Exception {
      KeyStore trustedCertStore = KeyStore.getInstance("jks");

      trustedCertStore.load(new FileInputStream(truststorePath), truststorePass.toCharArray());
      TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());

      tmf.init(trustedCertStore);
      return tmf.getTrustManagers();
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

   private BlockingConnection retrieveMQTTConnection(String host, String truststorePath, String truststorePass, String keystorePath, String keystorePass) throws Exception {

      MQTT mqtt = new MQTT();
      mqtt.setConnectAttemptsMax(1);
      mqtt.setReconnectAttemptsMax(0);
      mqtt.setHost(host);
      mqtt.setUserName(fullUser);
      mqtt.setPassword(fullPass);
      SSLContext sslContext = SSLContext.getInstance("TLS");
      sslContext.init(getKeyManager(keystorePath, keystorePass), getTrustManager(truststorePath, truststorePass), null);
      mqtt.setSslContext(sslContext);

      BlockingConnection connection = mqtt.blockingConnection();
      connection.connect();
      return connection;
   }


}
