/**
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

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.File;
import java.io.IOException;
import java.security.ProtectionDomain;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Tracer;
import org.fusesource.mqtt.codec.MQTTFrame;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MQTTTestSupport extends ActiveMQTestBase {

   private ActiveMQServer server;

   private static final Logger LOG = LoggerFactory.getLogger(MQTTTestSupport.class);

   protected int port = 1883;
   protected ActiveMQConnectionFactory cf;
   protected LinkedList<Throwable> exceptions = new LinkedList<Throwable>();
   protected boolean persistent;
   protected String protocolConfig;
   protected String protocolScheme;
   protected boolean useSSL;

   public static final int AT_MOST_ONCE = 0;
   public static final int AT_LEAST_ONCE = 1;
   public static final int EXACTLY_ONCE = 2;

   @Rule
   public TestName name = new TestName();

   public MQTTTestSupport() {
      this.protocolScheme = "mqtt";
      this.useSSL = false;
      cf = new ActiveMQConnectionFactory(false, new TransportConfiguration(ActiveMQTestBase.NETTY_CONNECTOR_FACTORY));
   }

   public File basedir() throws IOException {
      ProtectionDomain protectionDomain = getClass().getProtectionDomain();
      return new File(new File(protectionDomain.getCodeSource().getLocation().getPath()), "../..").getCanonicalFile();
   }

   public MQTTTestSupport(String connectorScheme, boolean useSSL) {
      this.protocolScheme = connectorScheme;
      this.useSSL = useSSL;
   }

   public String getName() {
      return name.getMethodName();
   }

   @Before
   public void setUp() throws Exception {
      String basedir = basedir().getPath();
      System.setProperty("javax.net.ssl.trustStore", basedir + "/src/test/resources/client.keystore");
      System.setProperty("javax.net.ssl.trustStorePassword", "password");
      System.setProperty("javax.net.ssl.trustStoreType", "jks");
      System.setProperty("javax.net.ssl.keyStore", basedir + "/src/test/resources/server.keystore");
      System.setProperty("javax.net.ssl.keyStorePassword", "password");
      System.setProperty("javax.net.ssl.keyStoreType", "jks");

      exceptions.clear();
      startBroker();
   }

   @After
   public void tearDown() throws Exception {
      System.clearProperty("javax.net.ssl.trustStore");
      System.clearProperty("javax.net.ssl.trustStorePassword");
      System.clearProperty("javax.net.ssl.trustStoreType");
      System.clearProperty("javax.net.ssl.keyStore");
      System.clearProperty("javax.net.ssl.keyStorePassword");
      System.clearProperty("javax.net.ssl.keyStoreType");
      stopBroker();
   }

   public void startBroker() throws Exception {
      // TODO Add SSL
      super.setUp();
      server = createServer(true, true);
      addCoreConnector();
      addMQTTConnector();
      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setMaxSizeBytes(999999999);
      server.getAddressSettingsRepository().addMatch("#", addressSettings);
      server.start();
      server.waitForActivation(10, TimeUnit.SECONDS);
   }

   protected void addCoreConnector() throws Exception {
      // Overrides of this method can add additional configuration options or add multiple
      // MQTT transport connectors as needed, the port variable is always supposed to be
      // assigned the primary MQTT connector's port.

      HashMap<String, Object> params = new HashMap<String, Object>();
      params.put(TransportConstants.PORT_PROP_NAME, "" + 5445);
      params.put(TransportConstants.PROTOCOLS_PROP_NAME, "CORE");
      TransportConfiguration transportConfiguration = new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, params);
      server.getConfiguration().getAcceptorConfigurations().add(transportConfiguration);

      LOG.info("Added connector {} to broker", getProtocolScheme());
   }

   protected void addMQTTConnector() throws Exception {
      // Overrides of this method can add additional configuration options or add multiple
      // MQTT transport connectors as needed, the port variable is always supposed to be
      // assigned the primary MQTT connector's port.

      HashMap<String, Object> params = new HashMap<String, Object>();
      params.put(TransportConstants.PORT_PROP_NAME, "" + port);
      params.put(TransportConstants.PROTOCOLS_PROP_NAME, "MQTT");
      TransportConfiguration transportConfiguration = new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, params);
      server.getConfiguration().getAcceptorConfigurations().add(transportConfiguration);

      LOG.info("Added connector {} to broker", getProtocolScheme());
   }

   public void stopBroker() throws Exception {
      if (server.isStarted()) {
         server.stop();
         server = null;
      }
   }

   protected String getQueueName() {
      return getClass().getName() + "." + name.getMethodName();
   }

   protected String getTopicName() {
      return getClass().getName() + "." + name.getMethodName();
   }

   /**
    * Initialize an MQTTClientProvider instance.  By default this method uses the port that's
    * assigned to be the TCP based port using the base version of addMQTTConnector.  A subclass
    * can either change the value of port or override this method to assign the correct port.
    *
    * @param provider the MQTTClientProvider instance to initialize.
    * @throws Exception if an error occurs during initialization.
    */
   protected void initializeConnection(MQTTClientProvider provider) throws Exception {
      if (!isUseSSL()) {
         provider.connect("tcp://localhost:" + port);
      }
      else {
         SSLContext ctx = SSLContext.getInstance("TLS");
         ctx.init(new KeyManager[0], new TrustManager[]{new DefaultTrustManager()}, new SecureRandom());
         provider.setSslContext(ctx);
         provider.connect("ssl://localhost:" + port);
      }
   }

   public String getProtocolScheme() {
      return protocolScheme;
   }

   public void setProtocolScheme(String scheme) {
      this.protocolScheme = scheme;
   }

   public boolean isUseSSL() {
      return this.useSSL;
   }

   public void setUseSSL(boolean useSSL) {
      this.useSSL = useSSL;
   }

   public boolean isPersistent() {
      return persistent;
   }

   public int getPort() {
      return this.port;
   }

   public boolean isSchedulerSupportEnabled() {
      return false;
   }

   protected interface Task {

      void run() throws Exception;
   }

   protected void within(int time, TimeUnit unit, Task task) throws InterruptedException {
      long timeMS = unit.toMillis(time);
      long deadline = System.currentTimeMillis() + timeMS;
      while (true) {
         try {
            task.run();
            return;
         }
         catch (Throwable e) {
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
               if (e instanceof RuntimeException) {
                  throw (RuntimeException) e;
               }
               if (e instanceof Error) {
                  throw (Error) e;
               }
               throw new RuntimeException(e);
            }
            Thread.sleep(Math.min(timeMS / 10, remaining));
         }
      }
   }

   protected MQTTClientProvider getMQTTClientProvider() {
      return new FuseMQTTClientProvider();
   }

   protected MQTT createMQTTConnection() throws Exception {
      MQTT client = createMQTTConnection(null, false);
      client.setVersion("3.1.1");
      return client;
   }

   protected MQTT createMQTTConnection(String clientId, boolean clean) throws Exception {
      if (isUseSSL()) {
         return createMQTTSslConnection(clientId, clean);
      }
      else {
         return createMQTTTcpConnection(clientId, clean);
      }
   }

   private MQTT createMQTTTcpConnection(String clientId, boolean clean) throws Exception {
      MQTT mqtt = new MQTT();
      mqtt.setConnectAttemptsMax(1);
      mqtt.setReconnectAttemptsMax(0);
      mqtt.setTracer(createTracer());
      mqtt.setVersion("3.1.1");
      if (clientId != null) {
         mqtt.setClientId(clientId);
      }
      mqtt.setCleanSession(clean);
      mqtt.setHost("localhost", port);
      return mqtt;
   }

   private MQTT createMQTTSslConnection(String clientId, boolean clean) throws Exception {
      MQTT mqtt = new MQTT();
      mqtt.setConnectAttemptsMax(1);
      mqtt.setReconnectAttemptsMax(0);
      mqtt.setTracer(createTracer());
      mqtt.setHost("ssl://localhost:" + port);
      if (clientId != null) {
         mqtt.setClientId(clientId);
      }
      mqtt.setCleanSession(clean);

      SSLContext ctx = SSLContext.getInstance("TLS");
      ctx.init(new KeyManager[0], new TrustManager[]{new DefaultTrustManager()}, new SecureRandom());
      mqtt.setSslContext(ctx);
      return mqtt;
   }

   protected Tracer createTracer() {
      return new Tracer() {
         @Override
         public void onReceive(MQTTFrame frame) {
            LOG.info("Client Received:\n" + frame);
         }

         @Override
         public void onSend(MQTTFrame frame) {
            LOG.info("Client Sent:\n" + frame);
         }

         @Override
         public void debug(String message, Object... args) {
            LOG.info(String.format(message, args));
         }
      };
   }

   static class DefaultTrustManager implements X509TrustManager {

      @Override
      public void checkClientTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
      }

      @Override
      public void checkServerTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
      }

      @Override
      public X509Certificate[] getAcceptedIssuers() {
         return new X509Certificate[0];
      }
   }
}
