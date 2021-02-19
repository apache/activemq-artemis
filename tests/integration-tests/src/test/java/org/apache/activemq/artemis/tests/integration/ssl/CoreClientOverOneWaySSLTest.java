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
package org.apache.activemq.artemis.tests.integration.ssl;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.artemis.api.core.ActiveMQConnectionTimedOutException;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQNotConnectedException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyAcceptor;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.remoting.impl.ssl.SSLSupport;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.DefaultSensitiveStringCodec;
import org.apache.activemq.artemis.utils.PasswordMaskingUtil;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(value = Parameterized.class)
public class CoreClientOverOneWaySSLTest extends ActiveMQTestBase {
   String suffix = "";

   @Parameterized.Parameters(name = "storeProvider={0}, storeType={1}")
   public static Collection getParameters() {
      return Arrays.asList(new Object[][]{
         {TransportConstants.DEFAULT_KEYSTORE_PROVIDER, TransportConstants.DEFAULT_KEYSTORE_TYPE},
         {"SunJCE", "JCEKS"},
         {"SUN", "JKS"},
         {"SunJSSE", "PKCS12"}
      });
   }

   public CoreClientOverOneWaySSLTest(String storeProvider, String storeType) {
      this.storeProvider = storeProvider;
      this.storeType = storeType;
      suffix = storeType.toLowerCase();
      // keytool expects PKCS12 stores to use the extension "p12"
      if (storeType.equals("PKCS12")) {
         suffix = "p12";
      }
      SERVER_SIDE_KEYSTORE = "server-side-keystore." + suffix;
      CLIENT_SIDE_TRUSTSTORE = "client-side-truststore." + suffix;
   }

   public static final SimpleString QUEUE = new SimpleString("QueueOverSSL");

   /**
    * These artifacts are required for testing 1-way SSL
    *
    * Commands to create the JKS artifacts:
    * keytool -genkey -keystore server-side-keystore.jks -storepass secureexample -keypass secureexample -dname "CN=ActiveMQ Artemis Server, OU=Artemis, O=ActiveMQ, L=AMQ, S=AMQ, C=AMQ" -keyalg RSA
    * keytool -export -keystore server-side-keystore.jks -file activemq-jks.cer -storepass secureexample
    * keytool -import -keystore client-side-truststore.jks -file activemq-jks.cer -storepass secureexample -keypass secureexample -noprompt
    *
    * keytool -genkey -keystore other-server-side-keystore.jks -storepass secureexample -keypass secureexample -dname "CN=Other ActiveMQ Artemis Server, OU=Artemis, O=ActiveMQ, L=AMQ, S=AMQ, C=AMQ" -keyalg RSA
    * keytool -export -keystore other-server-side-keystore.jks -file activemq-jks.cer -storepass secureexample
    * keytool -import -keystore other-client-side-truststore.jks -file activemq-jks.cer -storepass secureexample -keypass secureexample -noprompt
    *
    * keytool -genkey -keystore verified-server-side-keystore.jks -storepass secureexample -keypass secureexample -dname "CN=localhost, OU=Artemis, O=ActiveMQ, L=AMQ, S=AMQ, C=AMQ" -keyalg RSA
    * keytool -export -keystore verified-server-side-keystore.jks -file activemq-jks.cer -storepass secureexample
    * keytool -import -keystore verified-client-side-truststore.jks -file activemq-jks.cer -storepass secureexample -keypass secureexample -noprompt
    *
    * Commands to create the JCEKS artifacts:
    * keytool -genkey -keystore server-side-keystore.jceks -storetype JCEKS -storepass secureexample -keypass secureexample -dname "CN=ActiveMQ Artemis Server, OU=Artemis, O=ActiveMQ, L=AMQ, S=AMQ, C=AMQ" -keyalg RSA
    * keytool -export -keystore server-side-keystore.jceks -file activemq-jceks.cer -storetype jceks -storepass secureexample
    * keytool -import -keystore client-side-truststore.jceks -storetype JCEKS -file activemq-jceks.cer -storepass secureexample -keypass secureexample -noprompt
    *
    * keytool -genkey -keystore other-server-side-keystore.jceks -storetype JCEKS -storepass secureexample -keypass secureexample -dname "CN=Other ActiveMQ Artemis Server, OU=Artemis, O=ActiveMQ, L=AMQ, S=AMQ, C=AMQ" -keyalg RSA
    * keytool -export -keystore other-server-side-keystore.jceks -file activemq-jceks.cer -storetype jceks -storepass secureexample
    * keytool -import -keystore other-client-side-truststore.jceks -storetype JCEKS -file activemq-jceks.cer -storepass secureexample -keypass secureexample -noprompt
    *
    * keytool -genkey -keystore verified-server-side-keystore.jceks -storetype JCEKS -storepass secureexample -keypass secureexample -dname "CN=localhost, OU=Artemis, O=ActiveMQ, L=AMQ, S=AMQ, C=AMQ" -keyalg RSA
    * keytool -export -keystore verified-server-side-keystore.jceks -file activemq-jceks.cer -storetype jceks -storepass secureexample
    * keytool -import -keystore verified-client-side-truststore.jceks -storetype JCEKS -file activemq-jceks.cer -storepass secureexample -keypass secureexample -noprompt
    *
    * Commands to create the PKCS12 artifacts:
    * keytool -genkey -keystore server-side-keystore.p12 -storetype PKCS12 -storepass secureexample -keypass secureexample -dname "CN=ActiveMQ Artemis Server, OU=Artemis, O=ActiveMQ, L=AMQ, S=AMQ, C=AMQ" -keyalg RSA
    * keytool -export -keystore server-side-keystore.p12 -file activemq-p12.cer -storetype PKCS12 -storepass secureexample
    * keytool -import -keystore client-side-truststore.p12 -storetype PKCS12 -file activemq-p12.cer -storepass secureexample -keypass secureexample -noprompt
    *
    * keytool -genkey -keystore other-server-side-keystore.p12 -storetype PKCS12 -storepass secureexample -keypass secureexample -dname "CN=Other ActiveMQ Artemis Server, OU=Artemis, O=ActiveMQ, L=AMQ, S=AMQ, C=AMQ" -keyalg RSA
    * keytool -export -keystore other-server-side-keystore.p12 -file activemq-p12.cer -storetype PKCS12 -storepass secureexample
    * keytool -import -keystore other-client-side-truststore.p12 -storetype PKCS12 -file activemq-p12.cer -storepass secureexample -keypass secureexample -noprompt
    *
    * keytool -genkey -keystore verified-server-side-keystore.p12 -storetype PKCS12 -storepass secureexample -keypass secureexample -dname "CN=localhost, OU=Artemis, O=ActiveMQ, L=AMQ, S=AMQ, C=AMQ" -keyalg RSA
    * keytool -export -keystore verified-server-side-keystore.p12 -file activemq-p12.cer -storetype PKCS12 -storepass secureexample
    * keytool -import -keystore verified-client-side-truststore.p12 -storetype PKCS12 -file activemq-p12.cer -storepass secureexample -keypass secureexample -noprompt
    */
   private String storeProvider;
   private String storeType;
   private String SERVER_SIDE_KEYSTORE;
   private String CLIENT_SIDE_TRUSTSTORE;
   private final String PASSWORD = "secureexample";

   private ActiveMQServer server;

   private TransportConfiguration tc;

   @Test
   public void testOneWaySSL() throws Exception {
      createCustomSslServer();
      String text = RandomUtil.randomString();

      tc.getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PROVIDER_PROP_NAME, storeProvider);
      tc.getParams().put(TransportConstants.TRUSTSTORE_TYPE_PROP_NAME, storeType);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, CLIENT_SIDE_TRUSTSTORE);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, PASSWORD);

      ServerLocator locator = addServerLocator(ActiveMQClient.createServerLocatorWithoutHA(tc));
      ClientSessionFactory sf = addSessionFactory(createSessionFactory(locator));
      ClientSession session = addClientSession(sf.createSession(false, true, true));
      session.createQueue(new QueueConfiguration(CoreClientOverOneWaySSLTest.QUEUE).setDurable(false));
      ClientProducer producer = addClientProducer(session.createProducer(CoreClientOverOneWaySSLTest.QUEUE));

      ClientMessage message = createTextMessage(session, text);
      producer.send(message);

      ClientConsumer consumer = addClientConsumer(session.createConsumer(CoreClientOverOneWaySSLTest.QUEUE));
      session.start();

      ClientMessage m = consumer.receive(1000);
      Assert.assertNotNull(m);
      Assert.assertEquals(text, m.getBodyBuffer().readString());
   }

   @Test
   public void testOneWaySSLwithSNI() throws Exception {
      createCustomSslServer("myhost\\.com");
      String text = RandomUtil.randomString();

      tc.getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PROVIDER_PROP_NAME, storeProvider);
      tc.getParams().put(TransportConstants.TRUSTSTORE_TYPE_PROP_NAME, storeType);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, CLIENT_SIDE_TRUSTSTORE);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, PASSWORD);
      tc.getParams().put(TransportConstants.SNIHOST_PROP_NAME, "myhost.com");

      ServerLocator locator = addServerLocator(ActiveMQClient.createServerLocatorWithoutHA(tc));
      ClientSessionFactory sf = addSessionFactory(createSessionFactory(locator));
      ClientSession session = addClientSession(sf.createSession(false, true, true));
      session.createQueue(new QueueConfiguration(CoreClientOverOneWaySSLTest.QUEUE).setDurable(false));
      ClientProducer producer = addClientProducer(session.createProducer(CoreClientOverOneWaySSLTest.QUEUE));

      ClientMessage message = createTextMessage(session, text);
      producer.send(message);

      ClientConsumer consumer = addClientConsumer(session.createConsumer(CoreClientOverOneWaySSLTest.QUEUE));
      session.start();

      ClientMessage m = consumer.receive(1000);
      Assert.assertNotNull(m);
      Assert.assertEquals(text, m.getBodyBuffer().readString());
   }

   @Test
   public void testOneWaySSLwithSNINegative() throws Exception {
      createCustomSslServer("myhost\\.com");

      tc.getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PROVIDER_PROP_NAME, storeProvider);
      tc.getParams().put(TransportConstants.TRUSTSTORE_TYPE_PROP_NAME, storeType);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, CLIENT_SIDE_TRUSTSTORE);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, PASSWORD);
      tc.getParams().put(TransportConstants.SNIHOST_PROP_NAME, "badhost.com");

      ServerLocator locator = addServerLocator(ActiveMQClient.createServerLocatorWithoutHA(tc));
      try {
         ClientSessionFactory sf = addSessionFactory(createSessionFactory(locator));
         fail("Should have failed due to unrecognized SNI host name");
      } catch (Exception e) {
         // ignore
      }
   }

   @Test
   public void testOneWaySSLwithSNINegativeAndURL() throws Exception {
      createCustomSslServer("myhost\\.com");

      ServerLocator locator = addServerLocator(ActiveMQClient.createServerLocator("tcp://127.0.0.1:61616?" +
                                                                                     TransportConstants.SSL_ENABLED_PROP_NAME + "=true;" +
                                                                                     TransportConstants.TRUSTSTORE_PROVIDER_PROP_NAME + "=" + storeProvider + ";" +
                                                                                     TransportConstants.TRUSTSTORE_TYPE_PROP_NAME + "=" + storeType + ";" +
                                                                                     TransportConstants.TRUSTSTORE_PATH_PROP_NAME + "=" + CLIENT_SIDE_TRUSTSTORE + ";" +
                                                                                     TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME + "=" + PASSWORD + ";" +
                                                                                     TransportConstants.SNIHOST_PROP_NAME + "=badhost.com"));

      try {
         ClientSessionFactory sf = addSessionFactory(createSessionFactory(locator));
         fail("Should have failed due to unrecognized SNI host name");
      } catch (Exception e) {
         // ignore
      }
   }

   @Test
   public void testOneWaySSLwithSNIOnlyOnTheClient() throws Exception {
      createCustomSslServer();
      String text = RandomUtil.randomString();

      tc.getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PROVIDER_PROP_NAME, storeProvider);
      tc.getParams().put(TransportConstants.TRUSTSTORE_TYPE_PROP_NAME, storeType);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, CLIENT_SIDE_TRUSTSTORE);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, PASSWORD);
      tc.getParams().put(TransportConstants.SNIHOST_PROP_NAME, "myhost.com");

      ServerLocator locator = addServerLocator(ActiveMQClient.createServerLocatorWithoutHA(tc));
      ClientSessionFactory sf = addSessionFactory(createSessionFactory(locator));
      ClientSession session = addClientSession(sf.createSession(false, true, true));
      session.createQueue(new QueueConfiguration(CoreClientOverOneWaySSLTest.QUEUE).setDurable(false));
      ClientProducer producer = addClientProducer(session.createProducer(CoreClientOverOneWaySSLTest.QUEUE));

      ClientMessage message = createTextMessage(session, text);
      producer.send(message);

      ClientConsumer consumer = addClientConsumer(session.createConsumer(CoreClientOverOneWaySSLTest.QUEUE));
      session.start();

      ClientMessage m = consumer.receive(1000);
      Assert.assertNotNull(m);
      Assert.assertEquals(text, m.getBodyBuffer().readString());
   }

   @Test
   public void testOneWaySSLwithSNIOnlyOnTheBroker() throws Exception {
      createCustomSslServer("myhost\\.com");
      String text = RandomUtil.randomString();

      tc.getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PROVIDER_PROP_NAME, storeProvider);
      tc.getParams().put(TransportConstants.TRUSTSTORE_TYPE_PROP_NAME, storeType);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, CLIENT_SIDE_TRUSTSTORE);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, PASSWORD);

      ServerLocator locator = addServerLocator(ActiveMQClient.createServerLocatorWithoutHA(tc));
      ClientSessionFactory sf = addSessionFactory(createSessionFactory(locator));
      ClientSession session = addClientSession(sf.createSession(false, true, true));
      session.createQueue(new QueueConfiguration(CoreClientOverOneWaySSLTest.QUEUE).setDurable(false));
      ClientProducer producer = addClientProducer(session.createProducer(CoreClientOverOneWaySSLTest.QUEUE));

      ClientMessage message = createTextMessage(session, text);
      producer.send(message);

      ClientConsumer consumer = addClientConsumer(session.createConsumer(CoreClientOverOneWaySSLTest.QUEUE));
      session.start();

      ClientMessage m = consumer.receive(1000);
      Assert.assertNotNull(m);
      Assert.assertEquals(text, m.getBodyBuffer().readString());
   }

   @Test
   public void testOneWaySSLwithTrustManagerPlugin() throws Exception {
      createCustomSslServer(null, null, false, null, TestTrustManagerFactoryPlugin.class.getName());
      String text = RandomUtil.randomString();

      tc.getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PROVIDER_PROP_NAME, storeProvider);
      tc.getParams().put(TransportConstants.TRUSTSTORE_TYPE_PROP_NAME, storeType);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, CLIENT_SIDE_TRUSTSTORE);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, PASSWORD);

      ServerLocator locator = addServerLocator(ActiveMQClient.createServerLocatorWithoutHA(tc));
      ClientSessionFactory sf = addSessionFactory(createSessionFactory(locator));

      assertTrue(TestTrustManagerFactoryPlugin.triggered.get());

      ClientSession session = addClientSession(sf.createSession(false, true, true));
      session.createQueue(new QueueConfiguration(CoreClientOverOneWaySSLTest.QUEUE).setDurable(false));
      ClientProducer producer = addClientProducer(session.createProducer(CoreClientOverOneWaySSLTest.QUEUE));

      ClientMessage message = createTextMessage(session, text);
      producer.send(message);

      ClientConsumer consumer = addClientConsumer(session.createConsumer(CoreClientOverOneWaySSLTest.QUEUE));
      session.start();

      ClientMessage m = consumer.receive(1000);
      Assert.assertNotNull(m);
      Assert.assertEquals(text, m.getBodyBuffer().readString());
   }

   @Test
   public void testOneWaySSLwithURL() throws Exception {
      createCustomSslServer();
      String text = RandomUtil.randomString();

      String url = "tcp://127.0.0.1:61616?sslEnabled=true;trustStorePath=" + CLIENT_SIDE_TRUSTSTORE + ";trustStorePassword=" + PASSWORD;
      if (!storeType.equals(TransportConstants.DEFAULT_TRUSTSTORE_TYPE)) {
         url += ";trustStoreProvider=" + storeProvider;
      }
      if (storeProvider != null && !storeProvider.equals(TransportConstants.DEFAULT_TRUSTSTORE_PROVIDER)) {
         url += ";trustStoreType=" + storeType;
      }
      ServerLocator locator = addServerLocator(ActiveMQClient.createServerLocator(url));

      ClientSessionFactory sf = addSessionFactory(createSessionFactory(locator));
      ClientSession session = addClientSession(sf.createSession(false, true, true));
      session.createQueue(new QueueConfiguration(CoreClientOverOneWaySSLTest.QUEUE).setDurable(false));
      ClientProducer producer = addClientProducer(session.createProducer(CoreClientOverOneWaySSLTest.QUEUE));

      ClientMessage message = createTextMessage(session, text);
      producer.send(message);

      ClientConsumer consumer = addClientConsumer(session.createConsumer(CoreClientOverOneWaySSLTest.QUEUE));
      session.start();

      ClientMessage m = consumer.receive(1000);
      Assert.assertNotNull(m);
      Assert.assertEquals(text, m.getBodyBuffer().readString());
   }

   @Test
   public void testOneWaySSLwithURLandMaskedPasswordProperty() throws Exception {
      createCustomSslServer();
      String text = RandomUtil.randomString();

      DefaultSensitiveStringCodec codec = PasswordMaskingUtil.getDefaultCodec();
      Map<String, String> params = new HashMap<>();
      codec.init(params);

      String masked = codec.encode(PASSWORD);
      String url = "tcp://127.0.0.1:61616?sslEnabled=true;trustStorePath=" + CLIENT_SIDE_TRUSTSTORE + ";trustStorePassword=" + masked + ";activemq.usemaskedpassword=true";
      if (!storeType.equals(TransportConstants.DEFAULT_TRUSTSTORE_TYPE)) {
         url += ";trustStoreProvider=" + storeProvider;
      }
      if (storeProvider != null && !storeProvider.equals(TransportConstants.DEFAULT_TRUSTSTORE_PROVIDER)) {
         url += ";trustStoreType=" + storeType;
      }
      ServerLocator locator = addServerLocator(ActiveMQClient.createServerLocator(url));
      ClientSessionFactory sf = addSessionFactory(createSessionFactory(locator));
      ClientSession session = addClientSession(sf.createSession(false, true, true));
      session.createQueue(new QueueConfiguration(CoreClientOverOneWaySSLTest.QUEUE).setDurable(false));
      ClientProducer producer = addClientProducer(session.createProducer(CoreClientOverOneWaySSLTest.QUEUE));

      ClientMessage message = createTextMessage(session, text);
      producer.send(message);

      ClientConsumer consumer = addClientConsumer(session.createConsumer(CoreClientOverOneWaySSLTest.QUEUE));
      session.start();

      ClientMessage m = consumer.receive(1000);
      Assert.assertNotNull(m);
      Assert.assertEquals(text, m.getBodyBuffer().readString());
   }

   @Test
   public void testOneWaySSLwithURLandMaskedPasswordENCSyntax() throws Exception {
      createCustomSslServer();
      String text = RandomUtil.randomString();

      DefaultSensitiveStringCodec codec = PasswordMaskingUtil.getDefaultCodec();
      Map<String, String> params = new HashMap<>();
      codec.init(params);

      String masked = codec.encode(PASSWORD);

      String url = "tcp://127.0.0.1:61616?sslEnabled=true;trustStorePath=" + CLIENT_SIDE_TRUSTSTORE + ";trustStorePassword=ENC(" + masked + ")";
      if (!storeType.equals(TransportConstants.DEFAULT_TRUSTSTORE_TYPE)) {
         url += ";trustStoreProvider=" + storeProvider;
      }
      if (storeProvider != null && !storeProvider.equals(TransportConstants.DEFAULT_TRUSTSTORE_PROVIDER)) {
         url += ";trustStoreType=" + storeType;
      }
      ServerLocator locator = addServerLocator(ActiveMQClient.createServerLocator(url));
//      ServerLocator locator = addServerLocator(ActiveMQClient.createServerLocator("tcp://127.0.0.1:61616?sslEnabled=true;trustStoreProvider=" + storeProvider + ";trustStoreType=" + storeType + ";trustStorePath=" + CLIENT_SIDE_TRUSTSTORE + ";trustStorePassword=ENC(" + masked + ")"));
      ClientSessionFactory sf = addSessionFactory(createSessionFactory(locator));
      ClientSession session = addClientSession(sf.createSession(false, true, true));
      session.createQueue(new QueueConfiguration(CoreClientOverOneWaySSLTest.QUEUE).setDurable(false));
      ClientProducer producer = addClientProducer(session.createProducer(CoreClientOverOneWaySSLTest.QUEUE));

      ClientMessage message = createTextMessage(session, text);
      producer.send(message);

      ClientConsumer consumer = addClientConsumer(session.createConsumer(CoreClientOverOneWaySSLTest.QUEUE));
      session.start();

      ClientMessage m = consumer.receive(1000);
      Assert.assertNotNull(m);
      Assert.assertEquals(text, m.getBodyBuffer().readString());
   }

   @Test
   public void testOneWaySSLUsingDefaultSslContext() throws Exception {
      createCustomSslServer();
      String text = RandomUtil.randomString();

      tc.getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      tc.getParams().put(TransportConstants.USE_DEFAULT_SSL_CONTEXT_PROP_NAME, true);

      SSLContext.setDefault(new SSLSupport()
                               .setTruststoreProvider(storeProvider)
                               .setTruststoreType(storeType)
                               .setTruststorePath(CLIENT_SIDE_TRUSTSTORE)
                               .setTruststorePassword(PASSWORD)
                               .createContext());

      ServerLocator locator = addServerLocator(ActiveMQClient.createServerLocatorWithoutHA(tc));
      ClientSessionFactory sf = addSessionFactory(createSessionFactory(locator));
      ClientSession session = addClientSession(sf.createSession(false, true, true));
      session.createQueue(new QueueConfiguration(CoreClientOverOneWaySSLTest.QUEUE).setDurable(false));
      ClientProducer producer = addClientProducer(session.createProducer(CoreClientOverOneWaySSLTest.QUEUE));

      ClientMessage message = createTextMessage(session, text);
      producer.send(message);

      ClientConsumer consumer = addClientConsumer(session.createConsumer(CoreClientOverOneWaySSLTest.QUEUE));
      session.start();

      ClientMessage m = consumer.receive(1000);
      Assert.assertNotNull(m);
      Assert.assertEquals(text, m.getBodyBuffer().readString());
   }

   @Test
   public void testOneWaySSLVerifyHost() throws Exception {
      createCustomSslServer(true);
      String text = RandomUtil.randomString();

      tc.getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PROVIDER_PROP_NAME, storeProvider);
      tc.getParams().put(TransportConstants.TRUSTSTORE_TYPE_PROP_NAME, storeType);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, "verified-" + CLIENT_SIDE_TRUSTSTORE);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, PASSWORD);
      tc.getParams().put(TransportConstants.VERIFY_HOST_PROP_NAME, true);

      ServerLocator locator = addServerLocator(ActiveMQClient.createServerLocatorWithoutHA(tc));
      ClientSessionFactory sf = addSessionFactory(createSessionFactory(locator));
      ClientSession session = addClientSession(sf.createSession(false, true, true));
      session.createQueue(new QueueConfiguration(CoreClientOverOneWaySSLTest.QUEUE).setDurable(false));
      ClientProducer producer = addClientProducer(session.createProducer(CoreClientOverOneWaySSLTest.QUEUE));

      ClientMessage message = createTextMessage(session, text);
      producer.send(message);

      ClientConsumer consumer = addClientConsumer(session.createConsumer(CoreClientOverOneWaySSLTest.QUEUE));
      session.start();

      Message m = consumer.receive(1000);
      Assert.assertNotNull(m);
      Assert.assertEquals(text, m.getBodyBuffer().readString());
   }

   @Test
   public void testOneWaySSLVerifyHostNegative() throws Exception {
      createCustomSslServer();
      String text = RandomUtil.randomString();

      tc.getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PROVIDER_PROP_NAME, storeProvider);
      tc.getParams().put(TransportConstants.TRUSTSTORE_TYPE_PROP_NAME, storeType);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, CLIENT_SIDE_TRUSTSTORE);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, PASSWORD);
      tc.getParams().put(TransportConstants.VERIFY_HOST_PROP_NAME, true);

      ServerLocator locator = addServerLocator(ActiveMQClient.createServerLocatorWithoutHA(tc));

      try {
         ClientSessionFactory sf = addSessionFactory(createSessionFactory(locator));
         fail("Creating a session here should fail due to a certificate with a CN that doesn't match the host name.");
      } catch (Exception e) {
         // ignore
      }
   }

   @Test
   public void testOneWaySSLReloaded() throws Exception {
      createCustomSslServer();
      server.createQueue(new QueueConfiguration(CoreClientOverOneWaySSLTest.QUEUE).setRoutingType(RoutingType.ANYCAST).setDurable(false));
      String text = RandomUtil.randomString();

      // create a valid SSL connection and keep it for use later
      tc.getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PROVIDER_PROP_NAME, storeProvider);
      tc.getParams().put(TransportConstants.TRUSTSTORE_TYPE_PROP_NAME, storeType);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, CLIENT_SIDE_TRUSTSTORE);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, PASSWORD);

      ServerLocator existingLocator = addServerLocator(ActiveMQClient.createServerLocatorWithoutHA(tc));
      existingLocator.setCallTimeout(3000);
      ClientSessionFactory existingSessionFactory = addSessionFactory(createSessionFactory(existingLocator));
      ClientSession existingSession = addClientSession(existingSessionFactory.createSession(false, true, true));
      ClientConsumer existingConsumer = addClientConsumer(existingSession.createConsumer(CoreClientOverOneWaySSLTest.QUEUE));

      // create an invalid SSL connection
      tc.getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PROVIDER_PROP_NAME, storeProvider);
      tc.getParams().put(TransportConstants.TRUSTSTORE_TYPE_PROP_NAME, storeType);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, "other-client-side-truststore." + suffix);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, PASSWORD);

      ServerLocator locator = addServerLocator(ActiveMQClient.createServerLocatorWithoutHA(tc)).setCallTimeout(3000);
      try {
         addSessionFactory(createSessionFactory(locator));
         fail("Creating session here should fail due to SSL handshake problems.");
      } catch (Exception e) {
         // ignore
      }

      // reload the acceptor to reload the SSL stores
      NettyAcceptor acceptor = (NettyAcceptor) server.getRemotingService().getAcceptor("nettySSL");
      acceptor.setKeyStorePath("other-server-side-keystore." + suffix);
      acceptor.reload();

      // create a session with the locator which failed previously proving that the SSL stores have been reloaded
      ClientSessionFactory sf = addSessionFactory(createSessionFactory(locator));
      ClientSession session = addClientSession(sf.createSession(false, true, true));
      ClientProducer producer = addClientProducer(session.createProducer(CoreClientOverOneWaySSLTest.QUEUE));

      ClientMessage message = createTextMessage(session, text);
      producer.send(message);
      producer.send(message);

      ClientConsumer consumer = addClientConsumer(session.createConsumer(CoreClientOverOneWaySSLTest.QUEUE));
      session.start();
      Message m = consumer.receive(1000);
      Assert.assertNotNull(m);
      Assert.assertEquals(text, m.getBodyBuffer().readString());
      consumer.close();

      // use the existing connection to prove it wasn't lost when the acceptor was reloaded
      existingSession.start();
      m = existingConsumer.receive(1000);
      Assert.assertNotNull(m);
      Assert.assertEquals(text, m.getBodyBuffer().readString());
   }

   @Test
   public void testOneWaySSLWithBadClientCipherSuite() throws Exception {
      createCustomSslServer();
      tc.getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PROVIDER_PROP_NAME, storeProvider);
      tc.getParams().put(TransportConstants.TRUSTSTORE_TYPE_PROP_NAME, storeType);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, CLIENT_SIDE_TRUSTSTORE);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, PASSWORD);
      tc.getParams().put(TransportConstants.ENABLED_CIPHER_SUITES_PROP_NAME, "myBadCipherSuite");

      ServerLocator locator = addServerLocator(ActiveMQClient.createServerLocatorWithoutHA(tc));
      try {
         createSessionFactory(locator);
         Assert.fail();
      } catch (ActiveMQNotConnectedException e) {
         Assert.assertTrue(true);
      }
   }

   @Test
   public void testOneWaySSLWithBadServerCipherSuite() throws Exception {
      createCustomSslServer("myBadCipherSuite", null);
      tc.getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PROVIDER_PROP_NAME, storeProvider);
      tc.getParams().put(TransportConstants.TRUSTSTORE_TYPE_PROP_NAME, storeType);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, CLIENT_SIDE_TRUSTSTORE);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, PASSWORD);

      ServerLocator locator = addServerLocator(ActiveMQClient.createServerLocatorWithoutHA(tc));
      try {
         createSessionFactory(locator);
         Assert.fail();
      } catch (ActiveMQNotConnectedException e) {
         Assert.assertTrue(true);
      }
   }

   @Test
   public void testOneWaySSLWithMismatchedCipherSuites() throws Exception {
      createCustomSslServer(getEnabledCipherSuites()[0], "TLSv1.2");
      tc.getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PROVIDER_PROP_NAME, storeProvider);
      tc.getParams().put(TransportConstants.TRUSTSTORE_TYPE_PROP_NAME, storeType);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, CLIENT_SIDE_TRUSTSTORE);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, PASSWORD);
      tc.getParams().put(TransportConstants.ENABLED_CIPHER_SUITES_PROP_NAME, getEnabledCipherSuites()[1]);
      tc.getParams().put(TransportConstants.ENABLED_PROTOCOLS_PROP_NAME, "TLSv1.2");

      ServerLocator locator = addServerLocator(ActiveMQClient.createServerLocatorWithoutHA(tc));
      try {
         createSessionFactory(locator);
         Assert.fail();
      } catch (ActiveMQNotConnectedException e) {
         Assert.assertTrue(true);
      }
   }

   @Test
   public void testOneWaySSLWithBadClientProtocol() throws Exception {
      createCustomSslServer();
      tc.getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PROVIDER_PROP_NAME, storeProvider);
      tc.getParams().put(TransportConstants.TRUSTSTORE_TYPE_PROP_NAME, storeType);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, CLIENT_SIDE_TRUSTSTORE);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, PASSWORD);
      tc.getParams().put(TransportConstants.ENABLED_PROTOCOLS_PROP_NAME, "myBadProtocol");

      ServerLocator locator = addServerLocator(ActiveMQClient.createServerLocatorWithoutHA(tc));
      try {
         createSessionFactory(locator);
         Assert.fail();
      } catch (ActiveMQNotConnectedException e) {
         Assert.assertTrue(true);
      }
   }

   @Test
   public void testOneWaySSLWithBadServerProtocol() throws Exception {
      createCustomSslServer(null, "myBadProtocol");
      tc.getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PROVIDER_PROP_NAME, storeProvider);
      tc.getParams().put(TransportConstants.TRUSTSTORE_TYPE_PROP_NAME, storeType);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, CLIENT_SIDE_TRUSTSTORE);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, PASSWORD);

      ServerLocator locator = addServerLocator(ActiveMQClient.createServerLocatorWithoutHA(tc));
      try {
         createSessionFactory(locator);
         Assert.fail();
      } catch (ActiveMQNotConnectedException e) {
         Assert.assertTrue(true);
      }
   }

   @Test
   public void testOneWaySSLWithMismatchedProtocols() throws Exception {
      createCustomSslServer(null, "TLSv1");
      tc.getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PROVIDER_PROP_NAME, storeProvider);
      tc.getParams().put(TransportConstants.TRUSTSTORE_TYPE_PROP_NAME, storeType);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, CLIENT_SIDE_TRUSTSTORE);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, PASSWORD);
      tc.getParams().put(TransportConstants.ENABLED_PROTOCOLS_PROP_NAME, "TLSv1.2");

      ServerLocator locator = addServerLocator(ActiveMQClient.createServerLocatorWithoutHA(tc));
      try {
         createSessionFactory(locator);
         Assert.fail();
      } catch (ActiveMQNotConnectedException e) {
         Assert.assertTrue(true);
      }
   }

   @Test
   // http://www.oracle.com/technetwork/topics/security/poodlecve-2014-3566-2339408.html
   public void testPOODLE() throws Exception {
      createCustomSslServer(null, "SSLv3");
      tc.getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PROVIDER_PROP_NAME, storeProvider);
      tc.getParams().put(TransportConstants.TRUSTSTORE_TYPE_PROP_NAME, storeType);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, CLIENT_SIDE_TRUSTSTORE);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, PASSWORD);
      tc.getParams().put(TransportConstants.ENABLED_PROTOCOLS_PROP_NAME, "SSLv3");

      ServerLocator locator = addServerLocator(ActiveMQClient.createServerLocatorWithoutHA(tc));
      try {
         createSessionFactory(locator);
         Assert.fail();
      } catch (ActiveMQNotConnectedException e) {
         Assert.assertTrue(true);
      }
   }

   @Test
   public void testOneWaySSLWithGoodClientCipherSuite() throws Exception {
      createCustomSslServer();
      String text = RandomUtil.randomString();

      tc.getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PROVIDER_PROP_NAME, storeProvider);
      tc.getParams().put(TransportConstants.TRUSTSTORE_TYPE_PROP_NAME, storeType);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, CLIENT_SIDE_TRUSTSTORE);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, PASSWORD);
      tc.getParams().put(TransportConstants.ENABLED_CIPHER_SUITES_PROP_NAME, getSuitableCipherSuite());
      tc.getParams().put(TransportConstants.ENABLED_PROTOCOLS_PROP_NAME, "TLSv1.2");

      ServerLocator locator = addServerLocator(ActiveMQClient.createServerLocatorWithoutHA(tc));
      ClientSessionFactory sf = null;
      try {
         sf = createSessionFactory(locator);
      } catch (ActiveMQNotConnectedException e) {
         Assert.fail();
      }

      ClientSession session = sf.createSession(false, true, true);
      session.createQueue(new QueueConfiguration(CoreClientOverOneWaySSLTest.QUEUE).setDurable(false));
      ClientProducer producer = session.createProducer(CoreClientOverOneWaySSLTest.QUEUE);

      ClientMessage message = createTextMessage(session, text);
      producer.send(message);

      ClientConsumer consumer = session.createConsumer(CoreClientOverOneWaySSLTest.QUEUE);
      session.start();

      Message m = consumer.receive(1000);
      Assert.assertNotNull(m);
      Assert.assertEquals(text, m.getBodyBuffer().readString());
   }

   @Test
   public void testOneWaySSLWithGoodServerCipherSuite() throws Exception {
      createCustomSslServer(getSuitableCipherSuite(), null);
      String text = RandomUtil.randomString();

      tc.getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PROVIDER_PROP_NAME, storeProvider);
      tc.getParams().put(TransportConstants.TRUSTSTORE_TYPE_PROP_NAME, storeType);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, CLIENT_SIDE_TRUSTSTORE);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, PASSWORD);
      tc.getParams().put(TransportConstants.ENABLED_PROTOCOLS_PROP_NAME, "TLSv1.2");

      ServerLocator locator = addServerLocator(ActiveMQClient.createServerLocatorWithoutHA(tc));
      ClientSessionFactory sf = null;
      try {
         sf = createSessionFactory(locator);
      } catch (ActiveMQNotConnectedException e) {
         Assert.fail();
      }

      ClientSession session = sf.createSession(false, true, true);
      session.createQueue(new QueueConfiguration(CoreClientOverOneWaySSLTest.QUEUE).setDurable(false));
      ClientProducer producer = session.createProducer(CoreClientOverOneWaySSLTest.QUEUE);

      ClientMessage message = createTextMessage(session, text);
      producer.send(message);

      ClientConsumer consumer = session.createConsumer(CoreClientOverOneWaySSLTest.QUEUE);
      session.start();

      Message m = consumer.receive(1000);
      Assert.assertNotNull(m);
      Assert.assertEquals(text, m.getBodyBuffer().readString());
   }

   @Test
   public void testOneWaySSLWithGoodClientProtocol() throws Exception {
      createCustomSslServer();
      String text = RandomUtil.randomString();

      tc.getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PROVIDER_PROP_NAME, storeProvider);
      tc.getParams().put(TransportConstants.TRUSTSTORE_TYPE_PROP_NAME, storeType);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, CLIENT_SIDE_TRUSTSTORE);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, PASSWORD);
      tc.getParams().put(TransportConstants.ENABLED_PROTOCOLS_PROP_NAME, "TLSv1");

      ServerLocator locator = addServerLocator(ActiveMQClient.createServerLocatorWithoutHA(tc));
      ClientSessionFactory sf = null;
      try {
         sf = createSessionFactory(locator);
         Assert.assertTrue(true);
      } catch (ActiveMQNotConnectedException e) {
         Assert.fail();
      }

      ClientSession session = sf.createSession(false, true, true);
      session.createQueue(new QueueConfiguration(CoreClientOverOneWaySSLTest.QUEUE).setDurable(false));
      ClientProducer producer = session.createProducer(CoreClientOverOneWaySSLTest.QUEUE);

      ClientMessage message = createTextMessage(session, text);
      producer.send(message);

      ClientConsumer consumer = session.createConsumer(CoreClientOverOneWaySSLTest.QUEUE);
      session.start();

      Message m = consumer.receive(1000);
      Assert.assertNotNull(m);
      Assert.assertEquals(text, m.getBodyBuffer().readString());
   }

   @Test
   public void testOneWaySSLWithGoodServerProtocol() throws Exception {
      createCustomSslServer(null, "TLSv1");
      String text = RandomUtil.randomString();

      tc.getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PROVIDER_PROP_NAME, storeProvider);
      tc.getParams().put(TransportConstants.TRUSTSTORE_TYPE_PROP_NAME, storeType);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, CLIENT_SIDE_TRUSTSTORE);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, PASSWORD);

      ServerLocator locator = addServerLocator(ActiveMQClient.createServerLocatorWithoutHA(tc));
      ClientSessionFactory sf = null;
      try {
         sf = createSessionFactory(locator);
         Assert.assertTrue(true);
      } catch (ActiveMQNotConnectedException e) {
         Assert.fail();
      }

      ClientSession session = sf.createSession(false, true, true);
      session.createQueue(new QueueConfiguration(CoreClientOverOneWaySSLTest.QUEUE).setDurable(false));
      ClientProducer producer = session.createProducer(CoreClientOverOneWaySSLTest.QUEUE);

      ClientMessage message = createTextMessage(session, text);
      producer.send(message);

      ClientConsumer consumer = session.createConsumer(CoreClientOverOneWaySSLTest.QUEUE);
      session.start();

      Message m = consumer.receive(1000);
      Assert.assertNotNull(m);
      Assert.assertEquals(text, m.getBodyBuffer().readString());
   }

   public String getSuitableCipherSuite() throws Exception {
      String result = "";

      String[] suites = getEnabledCipherSuites();

      /**
       * The JKS certs are generated using Java keytool using RSA and not ECDSA but the JVM prefers ECDSA over RSA so we have
       * to look through the cipher suites until we find one that's suitable for us.
       * If the JVM running this test is version 7 from Oracle then this cipher suite will will almost certainly require
       * TLSv1.2 (which is not enabled on the client by default).
       * See http://docs.oracle.com/javase/8/docs/technotes/guides/security/SunProviders.html#SunJSSEProvider for the
       * preferred cipher suites.
       */

      /**
       * JCEKS is essentially the same story as JKS
       */
      for (int i = 0; i < suites.length; i++) {
         String suite = suites[i];
         if ((storeType.equals("JCEKS") && suite.contains("RSA") && !suite.contains("ECDH_")) || (!storeType.equals("JCEKS") && !suite.contains("ECDSA") && suite.contains("RSA"))) {
            result = suite;
            break;
         }
      }

      return result;
   }

   public String[] getEnabledCipherSuites() throws Exception {
      SSLContext context = new SSLSupport()
         .setKeystoreProvider(storeProvider)
         .setKeystoreType(storeType)
         .setKeystorePath(SERVER_SIDE_KEYSTORE)
         .setKeystorePassword(PASSWORD)
         .setTruststoreProvider(storeProvider)
         .setTruststoreType(storeType)
         .setTruststorePath(CLIENT_SIDE_TRUSTSTORE)
         .setTruststorePassword(PASSWORD)
         .createContext();
      SSLEngine engine = context.createSSLEngine();
      return engine.getEnabledCipherSuites();
   }

   @Test
   public void testOneWaySSLWithoutTrustStore() throws Exception {
      createCustomSslServer();
      tc.getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, true);

      ServerLocator locator = addServerLocator(ActiveMQClient.createServerLocatorWithoutHA(tc));
      try {
         createSessionFactory(locator);
         Assert.fail();
      } catch (ActiveMQNotConnectedException se) {
         //ok
      } catch (ActiveMQException e) {
         fail("Invalid Exception type:" + e.getType());
      }
   }

   @Test
   public void testOneWaySSLWithIncorrectTrustStorePassword() throws Exception {
      createCustomSslServer();
      tc.getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PROVIDER_PROP_NAME, storeProvider);
      tc.getParams().put(TransportConstants.TRUSTSTORE_TYPE_PROP_NAME, storeType);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, CLIENT_SIDE_TRUSTSTORE);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, "invalid password");

      ServerLocator locator = addServerLocator(ActiveMQClient.createServerLocatorWithoutHA(tc));
      try {
         ClientSessionFactory sf = createSessionFactory(locator);
         Assert.fail();
      } catch (ActiveMQNotConnectedException se) {
         //ok
      } catch (ActiveMQException e) {
         fail("Invalid Exception type:" + e.getType());
      }
   }

   @Test
   public void testOneWaySSLWithIncorrectTrustStorePath() throws Exception {
      createCustomSslServer();
      tc.getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, "incorrect path");
      tc.getParams().put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, PASSWORD);

      ServerLocator locator = addServerLocator(ActiveMQClient.createServerLocatorWithoutHA(tc));
      try {
         ClientSessionFactory sf = createSessionFactory(locator);
         Assert.fail();
      } catch (ActiveMQNotConnectedException se) {
         //ok
      } catch (ActiveMQException e) {
         fail("Invalid Exception type:" + e.getType());
      }
   }

   // see https://jira.jboss.org/jira/browse/HORNETQ-234
   @Test
   public void testPlainConnectionToSSLEndpoint() throws Exception {
      createCustomSslServer();
      tc.getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, false);

      ServerLocator locator = addServerLocator(ActiveMQClient.createServerLocatorWithoutHA(tc)).setCallTimeout(2000);
      try {
         createSessionFactory(locator);
         fail("expecting exception");
      } catch (ActiveMQNotConnectedException se) {
         //ok
      } catch (ActiveMQConnectionTimedOutException ctoe) {
         //ok
      } catch (ActiveMQException e) {
         fail("Invalid Exception type:" + e.getType());
      }
   }

   // Package protected ---------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
   }

   private void createCustomSslServer() throws Exception {
      createCustomSslServer(null, null);
   }

   private void createCustomSslServer(String cipherSuites, String protocols) throws Exception {
      createCustomSslServer(cipherSuites, protocols, false, null);
   }

   private void createCustomSslServer(String sniHost) throws Exception {
      createCustomSslServer(null, null, false, sniHost);
   }

   private void createCustomSslServer(boolean useVerifiedKeystore) throws Exception {
      createCustomSslServer(null, null, useVerifiedKeystore, null);
   }

   private void createCustomSslServer(String cipherSuites,
                                      String protocols,
                                      boolean useVerifiedKeystore,
                                      String sniHost) throws Exception {
      createCustomSslServer(cipherSuites, protocols, useVerifiedKeystore, sniHost, null);
   }

   private void createCustomSslServer(String cipherSuites,
                                      String protocols,
                                      boolean useVerifiedKeystore,
                                      String sniHost,
                                      String trustManagerFactoryPlugin) throws Exception {
      Map<String, Object> params = new HashMap<>();
      params.put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      params.put(TransportConstants.KEYSTORE_PROVIDER_PROP_NAME, storeProvider);
      params.put(TransportConstants.KEYSTORE_TYPE_PROP_NAME, storeType);

      if (sniHost != null) {
         params.put(TransportConstants.SNIHOST_PROP_NAME, sniHost);
      }

      if (useVerifiedKeystore) {
         params.put(TransportConstants.KEYSTORE_PATH_PROP_NAME, "verified-" + SERVER_SIDE_KEYSTORE);
      } else {
         params.put(TransportConstants.KEYSTORE_PATH_PROP_NAME, SERVER_SIDE_KEYSTORE);
      }
      params.put(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME, PASSWORD);
      params.put(TransportConstants.HOST_PROP_NAME, "localhost");

      if (cipherSuites != null) {
         params.put(TransportConstants.ENABLED_CIPHER_SUITES_PROP_NAME, cipherSuites);
      }

      if (protocols != null) {
         params.put(TransportConstants.ENABLED_PROTOCOLS_PROP_NAME, protocols);
      }

      if (trustManagerFactoryPlugin != null) {
         params.put(TransportConstants.TRUST_MANAGER_FACTORY_PLUGIN_PROP_NAME, trustManagerFactoryPlugin);
      }

      ConfigurationImpl config = createBasicConfig().addAcceptorConfiguration(new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, params, "nettySSL"));
      server = createServer(false, config);
      server.start();
      waitForServerToStart(server);
      tc = new TransportConfiguration(NETTY_CONNECTOR_FACTORY);
   }
}
