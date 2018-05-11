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

import io.netty.handler.ssl.SslHandler;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQNotConnectedException;
import org.apache.activemq.artemis.api.core.Interceptor;
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
import org.apache.activemq.artemis.core.protocol.core.Packet;
import org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyAcceptor;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnection;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.net.ssl.SSLPeerUnverifiedException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Testing connection where client and server are running OpenSSL TLS
 */
@RunWith(value = Parameterized.class)
public class CoreClientOverTwoWayOpenSSLTest extends ActiveMQTestBase {

   @Parameterized.Parameters(name = "storeType={0}")
   public static Collection getParameters() {
      return Arrays.asList(new Object[][]{{"JCEKS"}, {"JKS"}});
   }

   public CoreClientOverTwoWayOpenSSLTest(String storeType) {
      this.storeType = storeType;
      SERVER_SIDE_KEYSTORE = "openssl-server-side-keystore." + storeType.toLowerCase();
      SERVER_SIDE_TRUSTSTORE = "openssl-server-side-truststore." + storeType.toLowerCase();
      CLIENT_SIDE_TRUSTSTORE = "openssl-client-side-truststore." + storeType.toLowerCase();
      CLIENT_SIDE_KEYSTORE = "openssl-client-side-keystore." + storeType.toLowerCase();
   }

   public static final SimpleString QUEUE = new SimpleString("QueueOverSSL");

   /**
    * These artifacts are required for testing 2-way SSL with open SSL - note the EC key and ECDSA signature to comply with what OpenSSL offers
    *
    * Commands to create the JKS artifacts:
    * keytool -genkey -keystore openssl-client-side-keystore.jks -storepass secureexample -keypass secureexample -dname "CN=ActiveMQ Artemis Client, OU=Artemis, O=ActiveMQ, L=AMQ, S=AMQ, C=AMQ" -keyalg EC -sigalg SHA256withECDSA
    * keytool -export -keystore openssl-client-side-keystore.jks -file activemq-jks.cer -storepass secureexample
    * keytool -import -keystore openssl-server-side-truststore.jks -file activemq-jks.cer -storepass secureexample -keypass secureexample -noprompt
    *
    * keytool -genkey -keystore openssl-server-side-keystore.jks -storepass secureexample -keypass secureexample -dname "CN=ActiveMQ Artemis Server, OU=Artemis, O=ActiveMQ, L=AMQ, S=AMQ, C=AMQ" -keyalg EC -sigalg SHA256withECDSA
    * keytool -export -keystore openssl-server-side-keystore.jks -file activemq-jks.cer -storepass secureexample
    * keytool -import -keystore openssl-client-side-truststore.jks -file activemq-jks.cer -storepass secureexample -keypass secureexample -noprompt
    *
    * keytool -genkey -keystore verified-openssl-client-side-keystore.jks -storepass secureexample -keypass secureexample -dname "CN=localhost, OU=Artemis, O=ActiveMQ, L=AMQ, S=AMQ, C=AMQ" -keyalg EC -sigalg SHA256withECDSA
    * keytool -export -keystore verified-openssl-client-side-keystore.jks -file activemq-jks.cer -storepass secureexample
    * keytool -import -keystore verified-openssl-server-side-truststore.jks -file activemq-jks.cer -storepass secureexample -keypass secureexample -noprompt
    *
    * Commands to create the JCEKS artifacts:
    * keytool -genkey -keystore openssl-client-side-keystore.jceks -storetype JCEKS -storepass secureexample -keypass secureexample -dname "CN=ActiveMQ Artemis Client, OU=Artemis, O=ActiveMQ, L=AMQ, S=AMQ, C=AMQ" -keyalg EC  -sigalg SHA256withECDSA
    * keytool -export -keystore openssl-client-side-keystore.jceks -file activemq-jceks.cer -storetype jceks -storepass secureexample
    * keytool -import -keystore openssl-server-side-truststore.jceks -storetype JCEKS -file activemq-jceks.cer -storepass secureexample -keypass secureexample -noprompt
    *
    * keytool -genkey -keystore openssl-server-side-keystore.jceks -storetype JCEKS -storepass secureexample -keypass secureexample -dname "CN=ActiveMQ Artemis Server, OU=Artemis, O=ActiveMQ, L=AMQ, S=AMQ, C=AMQ" -keyalg EC  -sigalg SHA256withECDSA
    * keytool -export -keystore openssl-server-side-keystore.jceks -file activemq-jceks.cer -storetype jceks -storepass secureexample
    * keytool -import -keystore openssl-client-side-truststore.jceks -storetype JCEKS -file activemq-jceks.cer -storepass secureexample -keypass secureexample -noprompt
    *
    * keytool -genkey -keystore verified-openssl-client-side-keystore.jceks -storetype JCEKS -storepass secureexample -keypass secureexample -dname "CN=localhost, OU=Artemis, O=ActiveMQ, L=AMQ, S=AMQ, C=AMQ" -keyalg EC -sigalg SHA256withECDSA
    * keytool -export -keystore verified-openssl-client-side-keystore.jceks -file activemq-jceks.cer -storetype jceks -storepass secureexample
    * keytool -import -keystore verified-openssl-server-side-truststore.jceks -storetype JCEKS -file activemq-jceks.cer -storepass secureexample -keypass secureexample -noprompt
    *
    */

   private String storeType;
   private String SERVER_SIDE_KEYSTORE;
   private String SERVER_SIDE_TRUSTSTORE;
   private String CLIENT_SIDE_TRUSTSTORE;
   private String CLIENT_SIDE_KEYSTORE;
   private final String PASSWORD = "secureexample";

   private ActiveMQServer server;

   private TransportConfiguration tc;

   private class MyInterceptor implements Interceptor {

      @Override
      public boolean intercept(final Packet packet, final RemotingConnection connection) throws ActiveMQException {
         if (packet.getType() == PacketImpl.SESS_SEND) {
            try {
               if (connection.getTransportConnection() instanceof NettyConnection) {
                  System.out.println("Passed through....");
                  NettyConnection nettyConnection = (NettyConnection) connection.getTransportConnection();
                  SslHandler sslHandler = (SslHandler) nettyConnection.getChannel().pipeline().get("ssl");
                  Assert.assertNotNull(sslHandler);
                  Assert.assertNotNull(sslHandler.engine().getSession());
                  Assert.assertNotNull(sslHandler.engine().getSession().getPeerCertificateChain());
               }
            } catch (SSLPeerUnverifiedException e) {
               Assert.fail(e.getMessage());
            }
         }
         return true;
      }
   }

   @Test
   public void testTwoWaySSL() throws Exception {
      String text = RandomUtil.randomString();

      tc.getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      tc.getParams().put(TransportConstants.SSL_PROVIDER, TransportConstants.OPENSSL_PROVIDER);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PROVIDER_PROP_NAME, storeType);
      tc.getParams().put(TransportConstants.KEYSTORE_PROVIDER_PROP_NAME, storeType);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, CLIENT_SIDE_TRUSTSTORE);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, PASSWORD);
      tc.getParams().put(TransportConstants.KEYSTORE_PATH_PROP_NAME, CLIENT_SIDE_KEYSTORE);
      tc.getParams().put(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME, PASSWORD);
      //tc.getParams().put(TransportConstants.ENABLED_CIPHER_SUITES_PROP_NAME, "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256");

      server.getRemotingService().addIncomingInterceptor(new MyInterceptor());

      ServerLocator locator = addServerLocator(ActiveMQClient.createServerLocatorWithoutHA(tc));
      ClientSessionFactory sf = createSessionFactory(locator);
      ClientSession session = sf.createSession(false, true, true);
      session.createQueue(CoreClientOverTwoWayOpenSSLTest.QUEUE, CoreClientOverTwoWayOpenSSLTest.QUEUE, false);
      ClientProducer producer = session.createProducer(CoreClientOverTwoWayOpenSSLTest.QUEUE);

      ClientMessage message = createTextMessage(session, text);
      producer.send(message);

      ClientConsumer consumer = session.createConsumer(CoreClientOverTwoWayOpenSSLTest.QUEUE);
      session.start();

      ClientMessage m = consumer.receive(1000);
      Assert.assertNotNull(m);
      Assert.assertEquals(text, m.getBodyBuffer().readString());
   }

   @Test
   public void testTwoWaySSLVerifyClientHost() throws Exception {
      NettyAcceptor acceptor = (NettyAcceptor) server.getRemotingService().getAcceptor("nettySSL");
      acceptor.getConfiguration().put(TransportConstants.VERIFY_HOST_PROP_NAME, true);
      acceptor.getConfiguration().put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, "verified-" + SERVER_SIDE_TRUSTSTORE);
      server.getRemotingService().stop(false);
      server.getRemotingService().start();
      server.getRemotingService().startAcceptors();

      String text = RandomUtil.randomString();

      tc.getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      tc.getParams().put(TransportConstants.SSL_PROVIDER, TransportConstants.OPENSSL_PROVIDER);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PROVIDER_PROP_NAME, storeType);
      tc.getParams().put(TransportConstants.KEYSTORE_PROVIDER_PROP_NAME, storeType);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, CLIENT_SIDE_TRUSTSTORE);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, PASSWORD);
      tc.getParams().put(TransportConstants.KEYSTORE_PATH_PROP_NAME, "verified-" + CLIENT_SIDE_KEYSTORE);
      tc.getParams().put(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME, PASSWORD);

      server.getRemotingService().addIncomingInterceptor(new MyInterceptor());

      ServerLocator locator = addServerLocator(ActiveMQClient.createServerLocatorWithoutHA(tc));
      ClientSessionFactory sf = createSessionFactory(locator);
      ClientSession session = sf.createSession(false, true, true);
      session.createQueue(CoreClientOverTwoWayOpenSSLTest.QUEUE, CoreClientOverTwoWayOpenSSLTest.QUEUE, false);
      ClientProducer producer = session.createProducer(CoreClientOverTwoWayOpenSSLTest.QUEUE);

      ClientMessage message = createTextMessage(session, text);
      producer.send(message);

      ClientConsumer consumer = session.createConsumer(CoreClientOverTwoWayOpenSSLTest.QUEUE);
      session.start();

      ClientMessage m = consumer.receive(1000);
      Assert.assertNotNull(m);
      Assert.assertEquals(text, m.getBodyBuffer().readString());
   }

   @Test
   public void testTwoWaySSLVerifyClientHostNegative() throws Exception {
      NettyAcceptor acceptor = (NettyAcceptor) server.getRemotingService().getAcceptor("nettySSL");
      acceptor.getConfiguration().put(TransportConstants.VERIFY_HOST_PROP_NAME, true);
      server.getRemotingService().stop(false);
      server.getRemotingService().start();
      server.getRemotingService().startAcceptors();

      tc.getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      tc.getParams().put(TransportConstants.SSL_PROVIDER, TransportConstants.OPENSSL_PROVIDER);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PROVIDER_PROP_NAME, storeType);
      tc.getParams().put(TransportConstants.KEYSTORE_PROVIDER_PROP_NAME, storeType);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, CLIENT_SIDE_TRUSTSTORE);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, PASSWORD);
      tc.getParams().put(TransportConstants.KEYSTORE_PATH_PROP_NAME, CLIENT_SIDE_KEYSTORE);
      tc.getParams().put(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME, PASSWORD);

      server.getRemotingService().addIncomingInterceptor(new MyInterceptor());

      ServerLocator locator = addServerLocator(ActiveMQClient.createServerLocatorWithoutHA(tc));
      try {
         ClientSessionFactory sf = createSessionFactory(locator);
         fail("Creating a session here should fail due to a certificate with a CN that doesn't match the host name.");
      } catch (ActiveMQNotConnectedException se) {
         // ignore
      }
   }

   @Test
   public void testTwoWaySSLVerifyClientTrustAllTrue() throws Exception {
      NettyAcceptor acceptor = (NettyAcceptor) server.getRemotingService().getAcceptor("nettySSL");
      acceptor.getConfiguration().put(TransportConstants.NEED_CLIENT_AUTH_PROP_NAME, true);
      server.getRemotingService().stop(false);
      server.getRemotingService().start();
      server.getRemotingService().startAcceptors();

      //Set trust all so this should work even with no trust store set
      tc.getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      tc.getParams().put(TransportConstants.SSL_PROVIDER, TransportConstants.OPENSSL_PROVIDER);
      tc.getParams().put(TransportConstants.TRUST_ALL_PROP_NAME, true);
      tc.getParams().put(TransportConstants.KEYSTORE_PROVIDER_PROP_NAME, storeType);
      tc.getParams().put(TransportConstants.KEYSTORE_PATH_PROP_NAME, CLIENT_SIDE_KEYSTORE);
      tc.getParams().put(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME, PASSWORD);

      server.getRemotingService().addIncomingInterceptor(new MyInterceptor());

      ServerLocator locator = addServerLocator(ActiveMQClient.createServerLocatorWithoutHA(tc));
      ClientSessionFactory sf = createSessionFactory(locator);
      sf.close();
   }

   @Test
   public void testTwoWaySSLVerifyClientTrustAllTrueByURI() throws Exception {
      NettyAcceptor acceptor = (NettyAcceptor) server.getRemotingService().getAcceptor("nettySSL");
      acceptor.getConfiguration().put(TransportConstants.NEED_CLIENT_AUTH_PROP_NAME, true);
      server.getRemotingService().stop(false);
      server.getRemotingService().start();
      server.getRemotingService().startAcceptors();

      //Set trust all so this should work even with no trust store set
      StringBuilder uri = new StringBuilder("tcp://" + tc.getParams().get(TransportConstants.HOST_PROP_NAME).toString()
            + ":" + tc.getParams().get(TransportConstants.PORT_PROP_NAME).toString());

      uri.append("?").append(TransportConstants.SSL_ENABLED_PROP_NAME).append("=true");
      uri.append("&").append(TransportConstants.SSL_PROVIDER).append("=").append(TransportConstants.OPENSSL_PROVIDER);
      uri.append("&").append(TransportConstants.TRUST_ALL_PROP_NAME).append("=true");
      uri.append("&").append(TransportConstants.KEYSTORE_PROVIDER_PROP_NAME).append("=").append(storeType);
      uri.append("&").append(TransportConstants.KEYSTORE_PATH_PROP_NAME).append("=").append(CLIENT_SIDE_KEYSTORE);
      uri.append("&").append(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME).append("=").append(PASSWORD);

      server.getRemotingService().addIncomingInterceptor(new MyInterceptor());

      ServerLocator locator = addServerLocator(ActiveMQClient.createServerLocator(uri.toString()));
      ClientSessionFactory sf = createSessionFactory(locator);
      sf.close();
   }

   @Test
   public void testTwoWaySSLVerifyClientTrustAllFalse() throws Exception {
      NettyAcceptor acceptor = (NettyAcceptor) server.getRemotingService().getAcceptor("nettySSL");
      acceptor.getConfiguration().put(TransportConstants.NEED_CLIENT_AUTH_PROP_NAME, true);
      server.getRemotingService().stop(false);
      server.getRemotingService().start();
      server.getRemotingService().startAcceptors();

      //Trust all defaults to false so this should fail with no trust store set
      tc.getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      tc.getParams().put(TransportConstants.KEYSTORE_PROVIDER_PROP_NAME, storeType);
      tc.getParams().put(TransportConstants.KEYSTORE_PATH_PROP_NAME, CLIENT_SIDE_KEYSTORE);
      tc.getParams().put(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME, PASSWORD);

      server.getRemotingService().addIncomingInterceptor(new MyInterceptor());

      ServerLocator locator = addServerLocator(ActiveMQClient.createServerLocatorWithoutHA(tc));
      try {
         ClientSessionFactory sf = createSessionFactory(locator);
         fail("Creating a session here should fail due to no trust store being set");
      } catch (ActiveMQNotConnectedException se) {
         // ignore
      }
   }

   @Test
   public void testTwoWaySSLWithoutClientKeyStore() throws Exception {
      tc.getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PROVIDER_PROP_NAME, storeType);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, CLIENT_SIDE_TRUSTSTORE);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, PASSWORD);

      ServerLocator locator = addServerLocator(ActiveMQClient.createServerLocatorWithoutHA(tc));
      try {
         createSessionFactory(locator);
         Assert.fail();
      } catch (ActiveMQNotConnectedException se) {
         //ok
      } catch (ActiveMQException e) {
         Assert.fail("Invalid Exception type:" + e.getType());
      }
   }

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      Map<String, Object> params = new HashMap<>();
      params.put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      params.put(TransportConstants.SSL_PROVIDER, TransportConstants.OPENSSL_PROVIDER);
      params.put(TransportConstants.KEYSTORE_PATH_PROP_NAME, SERVER_SIDE_KEYSTORE);
      params.put(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME, PASSWORD);
      params.put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, SERVER_SIDE_TRUSTSTORE);
      params.put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, PASSWORD);
      params.put(TransportConstants.TRUSTSTORE_PROVIDER_PROP_NAME, storeType);
      params.put(TransportConstants.KEYSTORE_PROVIDER_PROP_NAME, storeType);
      params.put(TransportConstants.NEED_CLIENT_AUTH_PROP_NAME, true);
      ConfigurationImpl config = createBasicConfig().addAcceptorConfiguration(new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, params, "nettySSL"));
      server = createServer(false, config);
      server.start();
      waitForServerToStart(server);
      tc = new TransportConfiguration(NETTY_CONNECTOR_FACTORY);
   }
}
