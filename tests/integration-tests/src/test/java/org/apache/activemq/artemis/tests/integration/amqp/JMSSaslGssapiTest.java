/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.amqp;

import javax.jms.Connection;
import javax.jms.JMSSecurityException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.File;
import java.net.URI;
import java.net.URL;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnector;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.protocol.amqp.broker.ProtonProtocolManagerFactory;
import org.apache.activemq.artemis.protocol.amqp.client.AMQPClientConnectionFactory;
import org.apache.activemq.artemis.protocol.amqp.client.ProtonClientConnectionManager;
import org.apache.activemq.artemis.protocol.amqp.client.ProtonClientProtocolManager;
import org.apache.activemq.artemis.protocol.amqp.proton.handler.EventHandler;
import org.apache.activemq.artemis.protocol.amqp.proton.handler.ProtonHandler;
import org.apache.activemq.artemis.protocol.amqp.sasl.ClientSASL;
import org.apache.activemq.artemis.protocol.amqp.sasl.ClientSASLFactory;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.sasl.GssapiMechanism;
import org.apache.qpid.proton.amqp.Symbol;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class JMSSaslGssapiTest extends JMSClientTestSupport {

   static {
      String path = System.getProperty("java.security.auth.login.config");
      if (path == null) {
         URL resource = JMSSaslGssapiTest.class.getClassLoader().getResource("login.config");
         if (resource != null) {
            path = resource.getFile();
            System.setProperty("java.security.auth.login.config", path);
         }
      }
   }
   MiniKdc kdc = null;
   private final boolean debug = false;

   @Before
   public void setUpKerberos() throws Exception {
      kdc = new MiniKdc(MiniKdc.createConf(), temporaryFolder.newFolder("kdc"));
      kdc.start();

      // hard coded match, default_keytab_name in minikdc-krb5.conf template
      File userKeyTab = new File("target/test.krb5.keytab");
      kdc.createPrincipal(userKeyTab, "client", "amqp/localhost");

      if (debug) {
         for (java.util.logging.Logger logger : new java.util.logging.Logger[] {java.util.logging.Logger.getLogger("javax.security.sasl"), java.util.logging.Logger.getLogger("org.apache.qpid.proton")}) {
            logger.setLevel(java.util.logging.Level.FINEST);
            logger.addHandler(new java.util.logging.ConsoleHandler());
            for (java.util.logging.Handler handler : logger.getHandlers()) {
               handler.setLevel(java.util.logging.Level.FINEST);
            }
         }
      }
   }

   @After
   public void stopKerberos() throws Exception {
      if (kdc != null) {
         kdc.stop();
      }
   }

   @Override
   protected boolean isSecurityEnabled() {
      return true;
   }

   @Override
   protected void configureBrokerSecurity(ActiveMQServer server) {
      server.getConfiguration().setSecurityEnabled(isSecurityEnabled());
      ActiveMQJAASSecurityManager securityManager = (ActiveMQJAASSecurityManager) server.getSecurityManager();
      securityManager.setConfigurationName("Krb5Plus");
      securityManager.setConfiguration(null);

      final String roleName = "ALLOW_ALL";
      Role role = new Role(roleName, true, true, true, true, true, true, true, true, true, true);
      Set<Role> roles = new HashSet<>();
      roles.add(role);
      server.getSecurityRepository().addMatch(getQueueName().toString(), roles);

   }

   @Override
   protected String getJmsConnectionURIOptions() {
      return "amqp.saslMechanisms=GSSAPI";
   }

   @Override
   protected URI getBrokerQpidJMSConnectionURI() {

      try {
         int port = AMQP_PORT;

         // match the sasl.service <the host name>
         String uri = "amqp://localhost:" + port;

         if (!getJmsConnectionURIOptions().isEmpty()) {
            uri = uri + "?" + getJmsConnectionURIOptions();
         }

         return new URI(uri);
      } catch (Exception e) {
         throw new RuntimeException();
      }
   }

   @Override
   protected void configureAMQPAcceptorParameters(Map<String, Object> params) {
      params.put("saslMechanisms", "GSSAPI");
      params.put("saslLoginConfigScope", "amqp-sasl-gssapi");
   }

   @Test(timeout = 600000)
   public void testConnection() throws Exception {
      Connection connection = createConnection("client", null);
      connection.start();

      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         javax.jms.Queue queue = session.createQueue(getQueueName());
         MessageConsumer consumer = session.createConsumer(queue);
         MessageProducer producer = session.createProducer(queue);

         final String text = RandomUtil.randomString();
         producer.send(session.createTextMessage(text));

         TextMessage m = (TextMessage) consumer.receive(1000);
         assertNotNull(m);
         assertEquals(text, m.getText());

      } finally {
         connection.close();
      }
   }

   @Test(timeout = 600000)
   public void testSaslPlainConnectionDenied() throws Exception {

      JmsConnectionFactory factory = new JmsConnectionFactory(new URI("amqp://localhost:" + AMQP_PORT + "?amqp.saslMechanisms=PLAIN"));
      try {
         factory.createConnection("plain", "secret");
         fail("Expect sasl failure");
      } catch (JMSSecurityException expected) {
         assertTrue(expected.getMessage().contains("SASL"));
      }
   }

   @Test
   public void testOutboundWithSlowMech() throws Exception {
      final Map<String, Object> config = new LinkedHashMap<>(); config.put(TransportConstants.HOST_PROP_NAME, "localhost");
      config.put(TransportConstants.PORT_PROP_NAME, String.valueOf(AMQP_PORT));
      final ClientSASLFactory clientSASLFactory = new ClientSASLFactory() {
         @Override
         public ClientSASL chooseMechanism(String[] availableMechanims) {
            GssapiMechanism gssapiMechanism = new GssapiMechanism();
            return new ClientSASL() {
               @Override
               public String getName() {
                  return gssapiMechanism.getName();
               }

               @Override
               public byte[] getInitialResponse() {
                  gssapiMechanism.setUsername("client");
                  gssapiMechanism.setServerName("localhost");
                  try {
                     return gssapiMechanism.getInitialResponse();
                  } catch (Exception e) {
                     e.printStackTrace();
                  }
                  return new byte[0];
               }

               @Override
               public byte[] getResponse(byte[] challenge) {
                  try {
                     // simulate a slow client
                     TimeUnit.SECONDS.sleep(4);
                  } catch (InterruptedException e) {
                     e.printStackTrace();
                  }
                  try {
                     return gssapiMechanism.getChallengeResponse(challenge);
                  } catch (Exception e) {
                     e.printStackTrace();
                  }
                  return new byte[0];
               }
            };
         }
      };

      final AtomicBoolean connectionOpened = new AtomicBoolean();
      final AtomicBoolean authFailed = new AtomicBoolean();

      EventHandler eventHandler = new EventHandler() {
         @Override
         public void onRemoteOpen(org.apache.qpid.proton.engine.Connection connection) throws Exception {
            connectionOpened.set(true);
         }

         @Override
         public void onAuthFailed(ProtonHandler protonHandler, org.apache.qpid.proton.engine.Connection connection) {
            authFailed.set(true);
         }
      };

      ProtonClientConnectionManager lifeCycleListener = new ProtonClientConnectionManager(new AMQPClientConnectionFactory(server, "myid", Collections.singletonMap(Symbol.getSymbol("myprop"), "propvalue"), 5000), Optional.of(eventHandler), clientSASLFactory);
      ProtonClientProtocolManager protocolManager = new ProtonClientProtocolManager(new ProtonProtocolManagerFactory(), server);
      NettyConnector connector = new NettyConnector(config, lifeCycleListener, lifeCycleListener, server.getExecutorFactory().getExecutor(), server.getExecutorFactory().getExecutor(), server.getScheduledPool(), protocolManager);
      connector.start();
      connector.createConnection();

      try {
         Wait.assertEquals(1, server::getConnectionCount);
         Wait.assertTrue(connectionOpened::get);
         Wait.assertFalse(authFailed::get);

         lifeCycleListener.stop();

         Wait.assertEquals(0, server::getConnectionCount);
      } finally {
         lifeCycleListener.stop();
      }
   }
}
