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
package org.apache.activemq.artemis.tests.integration.security;

import javax.jms.Session;
import javax.security.cert.X509Certificate;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.lang.management.ManagementFactory;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQSslConnectionFactory;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.api.core.ActiveMQSecurityException;
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
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMConnection;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.security.CheckType;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.settings.HierarchicalRepository;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager2;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager3;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.CreateMessage;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class SecurityTest extends ActiveMQTestBase {

   static {
      String path = System.getProperty("java.security.auth.login.config");
      if (path == null) {
         URL resource = SecurityTest.class.getClassLoader().getResource("login.config");
         if (resource != null) {
            path = resource.getFile();
            System.setProperty("java.security.auth.login.config", path);
         }
      }
   }

   /*
    * create session tests
    */
   private static final String addressA = "addressA";

   private static final String queueA = "queueA";

   private ServerLocator locator;

   private Configuration configuration;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      locator = createInVMNonHALocator();
   }

   @Test
   public void testJAASSecurityManagerAuthentication() throws Exception {
      ActiveMQJAASSecurityManager securityManager = new ActiveMQJAASSecurityManager("PropertiesLogin");
      ActiveMQServer server = addServer(ActiveMQServers.newActiveMQServer(createDefaultInVMConfig().setSecurityEnabled(true), ManagementFactory.getPlatformMBeanServer(), securityManager, false));
      server.start();
      ClientSessionFactory cf = createSessionFactory(locator);

      try {
         ClientSession session = cf.createSession("first", "secret", false, true, true, false, 0);
         session.close();
      } catch (ActiveMQException e) {
         e.printStackTrace();
         Assert.fail("should not throw exception");
      }
   }

   @Test
   public void testJAASSecurityManagerAuthenticationWithValidateUser() throws Exception {
      ActiveMQJAASSecurityManager securityManager = new ActiveMQJAASSecurityManager("PropertiesLogin");
      ActiveMQServer server = addServer(ActiveMQServers.newActiveMQServer(createDefaultInVMConfig().setSecurityEnabled(true), ManagementFactory.getPlatformMBeanServer(), securityManager, false));
      server.getConfiguration().setPopulateValidatedUser(true);
      server.start();
      Role role = new Role("programmers", true, true, true, true, true, true, true, true, true, true);
      Set<Role> roles = new HashSet<>();
      roles.add(role);
      server.getSecurityRepository().addMatch("#", roles);
      ClientSessionFactory cf = createSessionFactory(locator);

      try {
         ClientSession session = cf.createSession("first", "secret", false, true, true, false, 0);
         server.createQueue(SimpleString.toSimpleString("address"), RoutingType.ANYCAST, SimpleString.toSimpleString("queue"), null, true, false);
         ClientProducer producer = session.createProducer("address");
         producer.send(session.createMessage(true));
         session.commit();
         producer.close();
         ClientConsumer consumer = session.createConsumer("queue");
         session.start();
         ClientMessage message = consumer.receive(1000);
         assertNotNull(message);
         assertEquals("first", message.getValidatedUserID());
         session.close();
      } catch (ActiveMQException e) {
         e.printStackTrace();
         Assert.fail("should not throw exception");
      }
   }

   @Test
   public void testJAASSecurityManagerAuthenticationWithCerts() throws Exception {
      testJAASSecurityManagerAuthenticationWithCerts(TransportConstants.NEED_CLIENT_AUTH_PROP_NAME);
   }

   @Test
   public void testJAASSecurityManagerAuthenticationWithCertsWantClientAuth() throws Exception {
      testJAASSecurityManagerAuthenticationWithCerts(TransportConstants.WANT_CLIENT_AUTH_PROP_NAME);
   }

   protected void testJAASSecurityManagerAuthenticationWithCerts(String clientAuthPropName) throws Exception {
      ActiveMQJAASSecurityManager securityManager = new ActiveMQJAASSecurityManager("CertLogin");
      ActiveMQServer server = addServer(ActiveMQServers.newActiveMQServer(createDefaultInVMConfig().setSecurityEnabled(true), ManagementFactory.getPlatformMBeanServer(), securityManager, false));

      Map<String, Object> params = new HashMap<>();
      params.put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      params.put(TransportConstants.KEYSTORE_PATH_PROP_NAME, "server-side-keystore.jks");
      params.put(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME, "secureexample");
      params.put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, "server-side-truststore.jks");
      params.put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, "secureexample");
      params.put(clientAuthPropName, true);

      server.getConfiguration().addAcceptorConfiguration(new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, params));

      server.start();

      TransportConfiguration tc = new TransportConfiguration(NETTY_CONNECTOR_FACTORY);
      tc.getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, "client-side-truststore.jks");
      tc.getParams().put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, "secureexample");
      tc.getParams().put(TransportConstants.KEYSTORE_PATH_PROP_NAME, "client-side-keystore.jks");
      tc.getParams().put(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME, "secureexample");
      ServerLocator locator = addServerLocator(ActiveMQClient.createServerLocatorWithoutHA(tc));
      ClientSessionFactory cf = createSessionFactory(locator);

      try {
         ClientSession session = cf.createSession();
         session.close();
      } catch (ActiveMQException e) {
         e.printStackTrace();
         Assert.fail("should not throw exception");
      }
   }

   @Test
   public void testJAASSecurityManagerAuthenticationWithCertsAndOpenWire() throws Exception {
      ActiveMQJAASSecurityManager securityManager = new ActiveMQJAASSecurityManager("CertLogin");
      ActiveMQServer server = addServer(ActiveMQServers.newActiveMQServer(createDefaultInVMConfig().setSecurityEnabled(true), ManagementFactory.getPlatformMBeanServer(), securityManager, false));

      Map<String, Object> params = new HashMap<>();
      params.put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      params.put(TransportConstants.KEYSTORE_PATH_PROP_NAME, "server-side-keystore.jks");
      params.put(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME, "secureexample");
      params.put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, "server-side-truststore.jks");
      params.put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, "secureexample");
      params.put(TransportConstants.NEED_CLIENT_AUTH_PROP_NAME, true);

      server.getConfiguration().addAcceptorConfiguration(new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, params));

      server.start();

      ActiveMQSslConnectionFactory factory = new ActiveMQSslConnectionFactory("ssl://localhost:61616");
      factory.setTrustStore("client-side-truststore.jks");
      factory.setTrustStorePassword("secureexample");
      factory.setKeyStore("client-side-keystore.jks");
      factory.setKeyStorePassword("secureexample");

      try (ActiveMQConnection connection = (ActiveMQConnection) factory.createConnection()) {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         session.close();
      } catch (Throwable e) {
         e.printStackTrace();
         Assert.fail("should not throw exception");
      }
   }

   @Test
   public void testJAASSecurityManagerAuthenticationBadPassword() throws Exception {
      ActiveMQJAASSecurityManager securityManager = new ActiveMQJAASSecurityManager("PropertiesLogin");
      ActiveMQServer server = addServer(ActiveMQServers.newActiveMQServer(createDefaultInVMConfig().setSecurityEnabled(true), ManagementFactory.getPlatformMBeanServer(), securityManager, false));
      server.start();
      ClientSessionFactory cf = createSessionFactory(locator);

      try {
         cf.createSession("first", "badpassword", false, true, true, false, 0);
         Assert.fail("should throw exception here");
      } catch (Exception e) {
         // ignore
      }
   }

   /**
    * This test requires a client-side certificate that will be trusted by the server but whose dname will be rejected
    * by the CertLogin login module. I created this cert with the follow commands:
    *
    * keytool -genkey -keystore bad-client-side-keystore.jks -storepass secureexample -keypass secureexample -dname "CN=Bad Client, OU=Artemis, O=ActiveMQ, L=AMQ, S=AMQ, C=AMQ"
    * keytool -export -keystore bad-client-side-keystore.jks -file activemq-jks.cer -storepass secureexample
    * keytool -import -keystore server-side-truststore.jks -file activemq-jks.cer -storepass secureexample -keypass secureexample -noprompt -alias bad
    */
   @Test
   public void testJAASSecurityManagerAuthenticationWithBadClientCert() throws Exception {
      ActiveMQJAASSecurityManager securityManager = new ActiveMQJAASSecurityManager("CertLogin");
      ActiveMQServer server = addServer(ActiveMQServers.newActiveMQServer(createDefaultInVMConfig().setSecurityEnabled(true), ManagementFactory.getPlatformMBeanServer(), securityManager, false));

      Map<String, Object> params = new HashMap<>();
      params.put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      params.put(TransportConstants.KEYSTORE_PATH_PROP_NAME, "server-side-keystore.jks");
      params.put(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME, "secureexample");
      params.put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, "server-side-truststore.jks");
      params.put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, "secureexample");
      params.put(TransportConstants.NEED_CLIENT_AUTH_PROP_NAME, true);

      server.getConfiguration().addAcceptorConfiguration(new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, params));

      server.start();

      TransportConfiguration tc = new TransportConfiguration(NETTY_CONNECTOR_FACTORY);
      tc.getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, "client-side-truststore.jks");
      tc.getParams().put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, "secureexample");
      tc.getParams().put(TransportConstants.KEYSTORE_PATH_PROP_NAME, "bad-client-side-keystore.jks");
      tc.getParams().put(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME, "secureexample");
      ServerLocator locator = addServerLocator(ActiveMQClient.createServerLocatorWithoutHA(tc));
      ClientSessionFactory cf = createSessionFactory(locator);

      try {
         cf.createSession();
         fail("Creating session here should fail due to authentication error.");
      } catch (ActiveMQException e) {
         assertTrue(e.getType() == ActiveMQExceptionType.SECURITY_EXCEPTION);
      }
   }

   @Test
   public void testJAASSecurityManagerAuthenticationGuest() throws Exception {
      ActiveMQJAASSecurityManager securityManager = new ActiveMQJAASSecurityManager("GuestLogin");
      ActiveMQServer server = addServer(ActiveMQServers.newActiveMQServer(createDefaultInVMConfig().setSecurityEnabled(true), ManagementFactory.getPlatformMBeanServer(), securityManager, false));
      server.start();
      ClientSessionFactory cf = createSessionFactory(locator);

      try {
         ClientSession session = cf.createSession("first", "secret", false, true, true, false, 0);
         session.close();
      } catch (ActiveMQException e) {
         e.printStackTrace();
         Assert.fail("should not throw exception");
      }
   }

   @Test
   public void testJAASSecurityManagerAuthorizationNegative() throws Exception {
      final SimpleString ADDRESS = new SimpleString("address");
      final SimpleString DURABLE_QUEUE = new SimpleString("durableQueue");
      final SimpleString NON_DURABLE_QUEUE = new SimpleString("nonDurableQueue");

      ActiveMQJAASSecurityManager securityManager = new ActiveMQJAASSecurityManager("PropertiesLogin");
      ActiveMQServer server = addServer(ActiveMQServers.newActiveMQServer(createDefaultInVMConfig().setSecurityEnabled(true), ManagementFactory.getPlatformMBeanServer(), securityManager, false));
      Set<Role> roles = new HashSet<>();
      roles.add(new Role("programmers", false, false, false, false, false, false, false, false, false, false));
      server.getConfiguration().putSecurityRoles("#", roles);
      server.start();
      server.addAddressInfo(new AddressInfo(ADDRESS, RoutingType.ANYCAST));
      server.createQueue(ADDRESS, RoutingType.ANYCAST, DURABLE_QUEUE, null, true, false);
      server.createQueue(ADDRESS, RoutingType.ANYCAST, NON_DURABLE_QUEUE, null, false, false);

      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession session = addClientSession(cf.createSession("first", "secret", false, true, true, false, 0));

      // CREATE_DURABLE_QUEUE
      try {
         session.createQueue(ADDRESS, DURABLE_QUEUE, true);
         Assert.fail("should throw exception here");
      } catch (ActiveMQException e) {
         assertTrue(e.getMessage().contains("User: first does not have permission='CREATE_DURABLE_QUEUE' for queue durableQueue on address address"));
      }

      // DELETE_DURABLE_QUEUE
      try {
         session.deleteQueue(DURABLE_QUEUE);
         Assert.fail("should throw exception here");
      } catch (ActiveMQException e) {
         assertTrue(e.getMessage().contains("User: first does not have permission='DELETE_DURABLE_QUEUE' for queue durableQueue on address address"));
      }

      // CREATE_NON_DURABLE_QUEUE
      try {
         session.createQueue(ADDRESS, NON_DURABLE_QUEUE, false);
         Assert.fail("should throw exception here");
      } catch (ActiveMQException e) {
         assertTrue(e.getMessage().contains("User: first does not have permission='CREATE_NON_DURABLE_QUEUE' for queue nonDurableQueue on address address"));
      }

      // DELETE_NON_DURABLE_QUEUE
      try {
         session.deleteQueue(NON_DURABLE_QUEUE);
         Assert.fail("should throw exception here");
      } catch (ActiveMQException e) {
         assertTrue(e.getMessage().contains("User: first does not have permission='DELETE_NON_DURABLE_QUEUE' for queue nonDurableQueue on address address"));
      }

      // PRODUCE
      try {
         ClientProducer producer = session.createProducer(ADDRESS);
         producer.send(session.createMessage(true));
         Assert.fail("should throw exception here");
      } catch (ActiveMQException e) {
         assertTrue(e.getMessage().contains("User: first does not have permission='SEND' on address address"));
      }

      // CONSUME
      try {
         ClientConsumer consumer = session.createConsumer(DURABLE_QUEUE);
         Assert.fail("should throw exception here");
      } catch (ActiveMQException e) {
         assertTrue(e.getMessage().contains("User: first does not have permission='CONSUME' for queue durableQueue on address address"));
      }

      // MANAGE
      try {
         ClientProducer producer = session.createProducer(server.getConfiguration().getManagementAddress());
         producer.send(session.createMessage(true));
         Assert.fail("should throw exception here");
      } catch (ActiveMQException e) {
         assertTrue(e.getMessage().contains("User: first does not have permission='MANAGE' on address activemq.management"));
      }

      // BROWSE
      try {
         ClientConsumer browser = session.createConsumer(DURABLE_QUEUE, true);
         Assert.fail("should throw exception here");
      } catch (ActiveMQException e) {
         assertTrue(e.getMessage().contains("User: first does not have permission='BROWSE' for queue durableQueue on address address"));
      }
   }

   @Test
   public void testJAASSecurityManagerAuthorizationSameAddressDifferentQueues() throws Exception {
      final SimpleString ADDRESS = new SimpleString("address");
      final SimpleString QUEUE_A = new SimpleString("a");
      final SimpleString QUEUE_B = new SimpleString("b");

      ActiveMQJAASSecurityManager securityManager = new ActiveMQJAASSecurityManager("PropertiesLogin");
      ActiveMQServer server = addServer(ActiveMQServers.newActiveMQServer(createDefaultInVMConfig().setSecurityEnabled(true), ManagementFactory.getPlatformMBeanServer(), securityManager, false));
      Set<Role> aRoles = new HashSet<>();
      aRoles.add(new Role(QUEUE_A.toString(), false, true, false, false, false, false, false, false, false, false));
      server.getConfiguration().putSecurityRoles(ADDRESS.concat(".").concat(QUEUE_A).toString(), aRoles);
      Set<Role> bRoles = new HashSet<>();
      bRoles.add(new Role(QUEUE_B.toString(), false, true, false, false, false, false, false, false, false, false));
      server.getConfiguration().putSecurityRoles(ADDRESS.concat(".").concat(QUEUE_B).toString(), bRoles);
      server.start();
      server.addAddressInfo(new AddressInfo(ADDRESS, RoutingType.ANYCAST));
      server.createQueue(ADDRESS, RoutingType.ANYCAST, QUEUE_A, null, true, false);
      server.createQueue(ADDRESS, RoutingType.ANYCAST, QUEUE_B, null, true, false);

      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession aSession = addClientSession(cf.createSession("a", "a", false, true, true, false, 0));
      ClientSession bSession = addClientSession(cf.createSession("b", "b", false, true, true, false, 0));

      // client A CONSUME from queue A
      try {
         ClientConsumer consumer = aSession.createConsumer(QUEUE_A);
      } catch (ActiveMQException e) {
         e.printStackTrace();
         Assert.fail("should not throw exception here");
      }

      // client B CONSUME from queue A
      try {
         ClientConsumer consumer = bSession.createConsumer(QUEUE_A);
         Assert.fail("should throw exception here");
      } catch (ActiveMQException e) {
         assertTrue(e instanceof ActiveMQSecurityException);
      }

      // client B CONSUME from queue B
      try {
         ClientConsumer consumer = bSession.createConsumer(QUEUE_B);
      } catch (ActiveMQException e) {
         e.printStackTrace();
         Assert.fail("should not throw exception here");
      }

      // client A CONSUME from queue B
      try {
         ClientConsumer consumer = aSession.createConsumer(QUEUE_B);
         Assert.fail("should throw exception here");
      } catch (ActiveMQException e) {
         assertTrue(e instanceof ActiveMQSecurityException);
      }
   }

   @Test
   public void testJAASSecurityManagerAuthorizationNegativeWithCerts() throws Exception {
      final SimpleString ADDRESS = new SimpleString("address");
      final SimpleString DURABLE_QUEUE = new SimpleString("durableQueue");
      final SimpleString NON_DURABLE_QUEUE = new SimpleString("nonDurableQueue");

      ActiveMQJAASSecurityManager securityManager = new ActiveMQJAASSecurityManager("CertLogin");
      ActiveMQServer server = addServer(ActiveMQServers.newActiveMQServer(createDefaultInVMConfig().setSecurityEnabled(true), ManagementFactory.getPlatformMBeanServer(), securityManager, false));

      Map<String, Object> params = new HashMap<>();
      params.put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      params.put(TransportConstants.KEYSTORE_PATH_PROP_NAME, "server-side-keystore.jks");
      params.put(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME, "secureexample");
      params.put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, "server-side-truststore.jks");
      params.put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, "secureexample");
      params.put(TransportConstants.NEED_CLIENT_AUTH_PROP_NAME, true);

      server.getConfiguration().addAcceptorConfiguration(new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, params));

      Set<Role> roles = new HashSet<>();
      roles.add(new Role("programmers", false, false, false, false, false, false, false, false, false, false));
      server.getConfiguration().putSecurityRoles("#", roles);

      server.start();

      TransportConfiguration tc = new TransportConfiguration(NETTY_CONNECTOR_FACTORY);
      tc.getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, "client-side-truststore.jks");
      tc.getParams().put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, "secureexample");
      tc.getParams().put(TransportConstants.KEYSTORE_PATH_PROP_NAME, "client-side-keystore.jks");
      tc.getParams().put(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME, "secureexample");
      ServerLocator locator = addServerLocator(ActiveMQClient.createServerLocatorWithoutHA(tc));
      ClientSessionFactory cf = createSessionFactory(locator);

      server.addAddressInfo(new AddressInfo(ADDRESS, RoutingType.ANYCAST));
      server.createQueue(ADDRESS, RoutingType.ANYCAST, DURABLE_QUEUE, null, true, false);
      server.createQueue(ADDRESS, RoutingType.ANYCAST, NON_DURABLE_QUEUE, null, false, false);

      ClientSession session = addClientSession(cf.createSession());

      // CREATE_DURABLE_QUEUE
      try {
         session.createQueue(ADDRESS, DURABLE_QUEUE, true);
         Assert.fail("should throw exception here");
      } catch (ActiveMQException e) {
         // ignore
      }

      // DELETE_DURABLE_QUEUE
      try {
         session.deleteQueue(DURABLE_QUEUE);
         Assert.fail("should throw exception here");
      } catch (ActiveMQException e) {
         // ignore
      }

      // CREATE_NON_DURABLE_QUEUE
      try {
         session.createQueue(ADDRESS, NON_DURABLE_QUEUE, false);
         Assert.fail("should throw exception here");
      } catch (ActiveMQException e) {
         // ignore
      }

      // DELETE_NON_DURABLE_QUEUE
      try {
         session.deleteQueue(NON_DURABLE_QUEUE);
         Assert.fail("should throw exception here");
      } catch (ActiveMQException e) {
         // ignore
      }

      // PRODUCE
      try {
         ClientProducer producer = session.createProducer(ADDRESS);
         producer.send(session.createMessage(true));
         Assert.fail("should throw exception here");
      } catch (ActiveMQException e) {
         // ignore
      }

      // CONSUME
      try {
         ClientConsumer consumer = session.createConsumer(DURABLE_QUEUE);
         Assert.fail("should throw exception here");
      } catch (ActiveMQException e) {
         // ignore
      }

      // MANAGE
      try {
         ClientProducer producer = session.createProducer(server.getConfiguration().getManagementAddress());
         producer.send(session.createMessage(true));
         Assert.fail("should throw exception here");
      } catch (ActiveMQException e) {
         // ignore
      }

      // BROWSE
      try {
         ClientConsumer browser = session.createConsumer(DURABLE_QUEUE, true);
         Assert.fail("should throw exception here");
      } catch (ActiveMQException e) {
         // ignore
      }
   }

   @Test
   public void testJAASSecurityManagerAuthorizationPositive() throws Exception {
      final SimpleString ADDRESS = new SimpleString("address");
      final SimpleString DURABLE_QUEUE = new SimpleString("durableQueue");
      final SimpleString NON_DURABLE_QUEUE = new SimpleString("nonDurableQueue");

      ActiveMQJAASSecurityManager securityManager = new ActiveMQJAASSecurityManager("PropertiesLogin");
      ActiveMQServer server = addServer(ActiveMQServers.newActiveMQServer(createDefaultInVMConfig().setSecurityEnabled(true), ManagementFactory.getPlatformMBeanServer(), securityManager, false));
      Set<Role> roles = new HashSet<>();
      roles.add(new Role("programmers", true, true, true, true, true, true, true, true, true, true));
      server.getConfiguration().putSecurityRoles("#", roles);
      server.start();

      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession session = addClientSession(cf.createSession("first", "secret", false, true, true, false, 0));

      // CREATE_DURABLE_QUEUE
      try {
         session.createQueue(ADDRESS, DURABLE_QUEUE, true);
      } catch (ActiveMQException e) {
         Assert.fail("should not throw exception here");
      }

      // DELETE_DURABLE_QUEUE
      try {
         session.deleteQueue(DURABLE_QUEUE);
      } catch (ActiveMQException e) {
         Assert.fail("should not throw exception here");
      }

      // CREATE_NON_DURABLE_QUEUE
      try {
         session.createQueue(ADDRESS, NON_DURABLE_QUEUE, false);
      } catch (ActiveMQException e) {
         Assert.fail("should not throw exception here");
      }

      // DELETE_NON_DURABLE_QUEUE
      try {
         session.deleteQueue(NON_DURABLE_QUEUE);
      } catch (ActiveMQException e) {
         Assert.fail("should not throw exception here");
      }

      session.createQueue(ADDRESS, DURABLE_QUEUE, true);

      // PRODUCE
      try {
         ClientProducer producer = session.createProducer(ADDRESS);
         producer.send(session.createMessage(true));
      } catch (ActiveMQException e) {
         Assert.fail("should not throw exception here");
      }

      // CONSUME
      try {
         session.createConsumer(DURABLE_QUEUE);
      } catch (ActiveMQException e) {
         Assert.fail("should not throw exception here");
      }

      // MANAGE
      try {
         ClientProducer producer = session.createProducer(server.getConfiguration().getManagementAddress());
         producer.send(session.createMessage(true));
      } catch (ActiveMQException e) {
         Assert.fail("should not throw exception here");
      }

      // BROWSE
      try {
         session.createConsumer(DURABLE_QUEUE, true);
      } catch (ActiveMQException e) {
         Assert.fail("should not throw exception here");
      }
   }

   @Test
   public void testJAASSecurityManagerAuthorizationPositiveWithCerts() throws Exception {
      testJAASSecurityManagerAuthorizationPositiveWithCerts(TransportConstants.NEED_CLIENT_AUTH_PROP_NAME);
   }

   @Test
   public void testJAASSecurityManagerAuthorizationPositiveWithCertsWantClientAuth() throws Exception {
      testJAASSecurityManagerAuthorizationPositiveWithCerts(TransportConstants.WANT_CLIENT_AUTH_PROP_NAME);
   }

   protected void testJAASSecurityManagerAuthorizationPositiveWithCerts(String clientAuthPropName) throws Exception {
      final SimpleString ADDRESS = new SimpleString("address");
      final SimpleString DURABLE_QUEUE = new SimpleString("durableQueue");
      final SimpleString NON_DURABLE_QUEUE = new SimpleString("nonDurableQueue");

      ActiveMQJAASSecurityManager securityManager = new ActiveMQJAASSecurityManager("CertLogin");
      ActiveMQServer server = addServer(ActiveMQServers.newActiveMQServer(createDefaultInVMConfig().setSecurityEnabled(true), ManagementFactory.getPlatformMBeanServer(), securityManager, false));

      Map<String, Object> params = new HashMap<>();
      params.put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      params.put(TransportConstants.KEYSTORE_PATH_PROP_NAME, "server-side-keystore.jks");
      params.put(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME, "secureexample");
      params.put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, "server-side-truststore.jks");
      params.put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, "secureexample");
      params.put(clientAuthPropName, true);

      server.getConfiguration().addAcceptorConfiguration(new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, params));

      Set<Role> roles = new HashSet<>();
      roles.add(new Role("programmers", true, true, true, true, true, true, true, true, true, true));
      server.getConfiguration().putSecurityRoles("#", roles);
      server.start();

      TransportConfiguration tc = new TransportConfiguration(NETTY_CONNECTOR_FACTORY);
      tc.getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      tc.getParams().put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, "client-side-truststore.jks");
      tc.getParams().put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, "secureexample");
      tc.getParams().put(TransportConstants.KEYSTORE_PATH_PROP_NAME, "client-side-keystore.jks");
      tc.getParams().put(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME, "secureexample");
      ServerLocator locator = addServerLocator(ActiveMQClient.createServerLocatorWithoutHA(tc));
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession session = addClientSession(cf.createSession());

      // CREATE_DURABLE_QUEUE
      try {
         session.createQueue(ADDRESS, DURABLE_QUEUE, true);
      } catch (ActiveMQException e) {
         Assert.fail("should not throw exception here");
      }

      // DELETE_DURABLE_QUEUE
      try {
         session.deleteQueue(DURABLE_QUEUE);
      } catch (ActiveMQException e) {
         Assert.fail("should not throw exception here");
      }

      // CREATE_NON_DURABLE_QUEUE
      try {
         session.createQueue(ADDRESS, NON_DURABLE_QUEUE, false);
      } catch (ActiveMQException e) {
         Assert.fail("should not throw exception here");
      }

      // DELETE_NON_DURABLE_QUEUE
      try {
         session.deleteQueue(NON_DURABLE_QUEUE);
      } catch (ActiveMQException e) {
         Assert.fail("should not throw exception here");
      }

      session.createQueue(ADDRESS, DURABLE_QUEUE, true);

      // PRODUCE
      try {
         ClientProducer producer = session.createProducer(ADDRESS);
         producer.send(session.createMessage(true));
      } catch (ActiveMQException e) {
         Assert.fail("should not throw exception here");
      }

      // CONSUME
      try {
         session.createConsumer(DURABLE_QUEUE);
      } catch (ActiveMQException e) {
         Assert.fail("should not throw exception here");
      }

      // MANAGE
      try {
         ClientProducer producer = session.createProducer(server.getConfiguration().getManagementAddress());
         producer.send(session.createMessage(true));
      } catch (ActiveMQException e) {
         Assert.fail("should not throw exception here");
      }

      // BROWSE
      try {
         session.createConsumer(DURABLE_QUEUE, true);
      } catch (ActiveMQException e) {
         Assert.fail("should not throw exception here");
      }
   }

   @Test
   public void testJAASSecurityManagerAuthorizationPositiveGuest() throws Exception {
      final SimpleString ADDRESS = new SimpleString("address");
      final SimpleString DURABLE_QUEUE = new SimpleString("durableQueue");
      final SimpleString NON_DURABLE_QUEUE = new SimpleString("nonDurableQueue");

      ActiveMQJAASSecurityManager securityManager = new ActiveMQJAASSecurityManager("GuestLogin");
      ActiveMQServer server = addServer(ActiveMQServers.newActiveMQServer(createDefaultInVMConfig().setSecurityEnabled(true), ManagementFactory.getPlatformMBeanServer(), securityManager, false));
      Set<Role> roles = new HashSet<>();
      roles.add(new Role("bar", true, true, true, true, true, true, true, false, true, true));
      server.getConfiguration().putSecurityRoles("#", roles);
      server.start();

      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession session = addClientSession(cf.createSession("junk", "junk", false, true, true, false, 0));

      // CREATE_DURABLE_QUEUE
      try {
         session.createQueue(ADDRESS, DURABLE_QUEUE, true);
      } catch (ActiveMQException e) {
         e.printStackTrace();
         Assert.fail("should not throw exception here");
      }

      // DELETE_DURABLE_QUEUE
      try {
         session.deleteQueue(DURABLE_QUEUE);
      } catch (ActiveMQException e) {
         Assert.fail("should not throw exception here");
      }

      // CREATE_NON_DURABLE_QUEUE
      try {
         session.createQueue(ADDRESS, NON_DURABLE_QUEUE, false);
      } catch (ActiveMQException e) {
         Assert.fail("should not throw exception here");
      }

      // DELETE_NON_DURABLE_QUEUE
      try {
         session.deleteQueue(NON_DURABLE_QUEUE);
      } catch (ActiveMQException e) {
         Assert.fail("should not throw exception here");
      }

      session.createQueue(ADDRESS, DURABLE_QUEUE, true);

      // PRODUCE
      try {
         ClientProducer producer = session.createProducer(ADDRESS);
         producer.send(session.createMessage(true));
      } catch (ActiveMQException e) {
         Assert.fail("should not throw exception here");
      }

      // CONSUME
      try {
         session.createConsumer(DURABLE_QUEUE);
      } catch (ActiveMQException e) {
         Assert.fail("should not throw exception here");
      }

      // MANAGE
      try {
         ClientProducer producer = session.createProducer(server.getConfiguration().getManagementAddress());
         producer.send(session.createMessage(true));
      } catch (ActiveMQException e) {
         Assert.fail("should not throw exception here");
      }
   }

   @Test
   public void testCreateSessionWithNullUserPass() throws Exception {
      ActiveMQServer server = createServer();
      ActiveMQJAASSecurityManager securityManager = (ActiveMQJAASSecurityManager) server.getSecurityManager();
      securityManager.getConfiguration().addUser("guest", "guest");
      securityManager.getConfiguration().setDefaultUser("guest");
      server.start();
      ClientSessionFactory cf = createSessionFactory(locator);

      try {
         ClientSession session = cf.createSession(false, true, true);

         session.close();
      } catch (ActiveMQException e) {
         Assert.fail("should not throw exception");
      }
   }

   /**
    * @return
    * @throws Exception
    */
   private ActiveMQServer createServer() throws Exception {
      configuration = createDefaultInVMConfig().setSecurityEnabled(true);
      ActiveMQServer server = createServer(false, configuration);
      return server;
   }

   @Test
   public void testCreateSessionWithNullUserPassNoGuest() throws Exception {
      ActiveMQServer server = createServer();
      server.start();
      ClientSessionFactory cf = createSessionFactory(locator);
      try {
         cf.createSession(false, true, true);
         Assert.fail("should throw exception");
      } catch (ActiveMQSecurityException se) {
         //ok
      } catch (ActiveMQException e) {
         fail("Invalid Exception type:" + e.getType());
      }
   }

   @Test
   public void testCreateSessionWithCorrectUserWrongPass() throws Exception {
      ActiveMQServer server = createServer();
      ActiveMQJAASSecurityManager securityManager = (ActiveMQJAASSecurityManager) server.getSecurityManager();
      securityManager.getConfiguration().addUser("newuser", "apass");
      server.start();
      ClientSessionFactory cf = createSessionFactory(locator);

      try {
         cf.createSession("newuser", "awrongpass", false, true, true, false, -1);
         Assert.fail("should not throw exception");
      } catch (ActiveMQSecurityException se) {
         //ok
      } catch (ActiveMQException e) {
         fail("Invalid Exception type:" + e.getType());
      }
   }

   @Test
   public void testCreateSessionWithCorrectUserCorrectPass() throws Exception {
      ActiveMQServer server = createServer();
      ActiveMQJAASSecurityManager securityManager = (ActiveMQJAASSecurityManager) server.getSecurityManager();
      securityManager.getConfiguration().addUser("newuser", "apass");
      server.start();
      ClientSessionFactory cf = createSessionFactory(locator);

      try {
         ClientSession session = cf.createSession("newuser", "apass", false, true, true, false, -1);

         session.close();
      } catch (ActiveMQException e) {
         Assert.fail("should not throw exception");
      }
   }

   @Test
   public void testCreateDurableQueueWithRole() throws Exception {
      ActiveMQServer server = createServer();
      server.start();
      HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
      ActiveMQJAASSecurityManager securityManager = (ActiveMQJAASSecurityManager) server.getSecurityManager();
      securityManager.getConfiguration().addUser("auser", "pass");
      Role role = new Role("arole", false, false, true, false, false, false, false, false, false, false);
      Set<Role> roles = new HashSet<>();
      roles.add(role);
      securityRepository.addMatch(SecurityTest.addressA, roles);
      securityManager.getConfiguration().addRole("auser", "arole");
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
      session.createQueue(SecurityTest.addressA, SecurityTest.queueA, true);
      session.close();
   }

   @Test
   public void testCreateDurableQueueWithoutRole() throws Exception {
      ActiveMQServer server = createServer();

      server.start();
      HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
      ActiveMQJAASSecurityManager securityManager = (ActiveMQJAASSecurityManager) server.getSecurityManager();
      securityManager.getConfiguration().addUser("auser", "pass");
      Role role = new Role("arole", false, false, false, false, false, false, false, false, false, false);
      Set<Role> roles = new HashSet<>();
      roles.add(role);
      securityRepository.addMatch(SecurityTest.addressA, roles);
      securityManager.getConfiguration().addRole("auser", "arole");
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
      try {
         session.createQueue(SecurityTest.addressA, SecurityTest.queueA, true);
         Assert.fail("should throw exception");
      } catch (ActiveMQSecurityException se) {
         //ok
      } catch (ActiveMQException e) {
         fail("Invalid Exception type:" + e.getType());
      }
      session.close();
   }

   @Test
   public void testDeleteDurableQueueWithRole() throws Exception {
      ActiveMQServer server = createServer();
      server.start();
      HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
      ActiveMQJAASSecurityManager securityManager = (ActiveMQJAASSecurityManager) server.getSecurityManager();
      securityManager.getConfiguration().addUser("auser", "pass");
      Role role = new Role("arole", false, false, true, true, false, false, false, false, false, true);
      Set<Role> roles = new HashSet<>();
      roles.add(role);
      securityRepository.addMatch(SecurityTest.addressA, roles);
      securityManager.getConfiguration().addRole("auser", "arole");
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
      session.createQueue(SecurityTest.addressA, SecurityTest.queueA, true);
      session.deleteQueue(SecurityTest.queueA);
      session.close();
   }

   @Test
   public void testDeleteDurableQueueWithoutRole() throws Exception {
      ActiveMQServer server = createServer();
      server.start();
      HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
      ActiveMQJAASSecurityManager securityManager = (ActiveMQJAASSecurityManager) server.getSecurityManager();
      securityManager.getConfiguration().addUser("auser", "pass");
      Role role = new Role("arole", false, false, true, false, false, false, false, false, false, false);
      Set<Role> roles = new HashSet<>();
      roles.add(role);
      securityRepository.addMatch(SecurityTest.addressA, roles);
      securityManager.getConfiguration().addRole("auser", "arole");
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
      session.createQueue(SecurityTest.addressA, SecurityTest.queueA, true);
      try {
         session.deleteQueue(SecurityTest.queueA);
         Assert.fail("should throw exception");
      } catch (ActiveMQSecurityException se) {
         //ok
      } catch (ActiveMQException e) {
         fail("Invalid Exception type:" + e.getType());
      }
      session.close();
   }

   @Test
   public void testCreateTempQueueWithRole() throws Exception {
      ActiveMQServer server = createServer();

      server.start();
      HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
      ActiveMQJAASSecurityManager securityManager = (ActiveMQJAASSecurityManager) server.getSecurityManager();
      securityManager.getConfiguration().addUser("auser", "pass");
      Role role = new Role("arole", false, false, false, false, true, false, false, false, false, false);
      Set<Role> roles = new HashSet<>();
      roles.add(role);
      securityRepository.addMatch(SecurityTest.addressA, roles);
      securityManager.getConfiguration().addRole("auser", "arole");
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
      session.createQueue(SecurityTest.addressA, SecurityTest.queueA, false);
      session.close();
   }

   @Test
   public void testCreateTempQueueWithoutRole() throws Exception {
      ActiveMQServer server = createServer();

      server.start();
      HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
      ActiveMQJAASSecurityManager securityManager = (ActiveMQJAASSecurityManager) server.getSecurityManager();
      securityManager.getConfiguration().addUser("auser", "pass");
      Role role = new Role("arole", false, false, false, false, false, false, false, false, false, false);
      Set<Role> roles = new HashSet<>();
      roles.add(role);
      securityRepository.addMatch(SecurityTest.addressA, roles);
      securityManager.getConfiguration().addRole("auser", "arole");
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
      try {
         session.createQueue(SecurityTest.addressA, SecurityTest.queueA, false);
         Assert.fail("should throw exception");
      } catch (ActiveMQSecurityException se) {
         //ok
      } catch (ActiveMQException e) {
         fail("Invalid Exception type:" + e.getType());
      }
      session.close();
   }

   @Test
   public void testDeleteTempQueueWithRole() throws Exception {
      ActiveMQServer server = createServer();
      server.start();
      HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
      ActiveMQJAASSecurityManager securityManager = (ActiveMQJAASSecurityManager) server.getSecurityManager();
      securityManager.getConfiguration().addUser("auser", "pass");
      Role role = new Role("arole", false, false, false, false, true, true, false, false, false, true);
      Set<Role> roles = new HashSet<>();
      roles.add(role);
      securityRepository.addMatch(SecurityTest.addressA, roles);
      securityManager.getConfiguration().addRole("auser", "arole");
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
      session.createQueue(SecurityTest.addressA, SecurityTest.queueA, false);
      session.deleteQueue(SecurityTest.queueA);
      session.close();
   }

   @Test
   public void testDeleteTempQueueWithoutRole() throws Exception {
      ActiveMQServer server = createServer();
      server.start();
      HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
      ActiveMQJAASSecurityManager securityManager = (ActiveMQJAASSecurityManager) server.getSecurityManager();
      securityManager.getConfiguration().addUser("auser", "pass");
      Role role = new Role("arole", false, false, false, false, true, false, false, false, false, false);
      Set<Role> roles = new HashSet<>();
      roles.add(role);
      securityRepository.addMatch(SecurityTest.addressA, roles);
      securityManager.getConfiguration().addRole("auser", "arole");
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
      session.createQueue(SecurityTest.addressA, SecurityTest.queueA, false);
      try {
         session.deleteQueue(SecurityTest.queueA);
         Assert.fail("should throw exception");
      } catch (ActiveMQSecurityException se) {
         //ok
      } catch (ActiveMQException e) {
         fail("Invalid Exception type:" + e.getType());
      }
      session.close();
   }

   @Test
   public void testSendWithRole() throws Exception {
      ActiveMQServer server = createServer();

      server.start();

      HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();

      ActiveMQJAASSecurityManager securityManager = (ActiveMQJAASSecurityManager) server.getSecurityManager();

      securityManager.getConfiguration().addUser("auser", "pass");

      Role role = new Role("arole", true, true, true, false, false, false, false, false, false, false);

      Set<Role> roles = new HashSet<>();

      roles.add(role);

      securityRepository.addMatch(SecurityTest.addressA, roles);

      securityManager.getConfiguration().addRole("auser", "arole");

      locator.setBlockOnNonDurableSend(true);

      ClientSessionFactory cf = createSessionFactory(locator);

      ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);

      session.createQueue(SecurityTest.addressA, SecurityTest.queueA, true);

      ClientProducer cp = session.createProducer(SecurityTest.addressA);

      cp.send(session.createMessage(false));

      session.start();

      ClientConsumer cons = session.createConsumer(queueA);

      ClientMessage receivedMessage = cons.receive(5000);

      assertNotNull(receivedMessage);

      receivedMessage.acknowledge();

      role = new Role("arole", false, false, true, false, false, false, false, false, false, false);

      roles = new HashSet<>();

      roles.add(role);

      // This was added to validate https://issues.jboss.org/browse/SOA-3363
      securityRepository.addMatch(SecurityTest.addressA, roles);
      boolean failed = false;
      try {
         cp.send(session.createMessage(true));
      } catch (ActiveMQException e) {
         failed = true;
      }
      // This was added to validate https://issues.jboss.org/browse/SOA-3363 ^^^^^

      assertTrue("Failure expected on send after removing the match", failed);
   }

   @Test
   public void testSendWithoutRole() throws Exception {
      ActiveMQServer server = createServer();

      server.start();
      HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
      ActiveMQJAASSecurityManager securityManager = (ActiveMQJAASSecurityManager) server.getSecurityManager();
      securityManager.getConfiguration().addUser("auser", "pass");
      Role role = new Role("arole", false, false, true, false, false, false, false, false, false, false);
      Set<Role> roles = new HashSet<>();
      roles.add(role);
      securityRepository.addMatch(SecurityTest.addressA, roles);
      securityManager.getConfiguration().addRole("auser", "arole");
      locator.setBlockOnNonDurableSend(true);
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
      session.createQueue(SecurityTest.addressA, SecurityTest.queueA, true);
      ClientProducer cp = session.createProducer(SecurityTest.addressA);
      try {
         cp.send(session.createMessage(false));
      } catch (ActiveMQSecurityException se) {
         //ok
      } catch (ActiveMQException e) {
         fail("Invalid Exception type:" + e.getType());
      }
      session.close();
   }

   @Test
   public void testNonBlockSendWithoutRole() throws Exception {
      ActiveMQServer server = createServer();

      server.start();
      HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
      ActiveMQJAASSecurityManager securityManager = (ActiveMQJAASSecurityManager) server.getSecurityManager();
      securityManager.getConfiguration().addUser("auser", "pass");
      Role role = new Role("arole", false, false, true, false, false, false, false, false, false, false);
      Set<Role> roles = new HashSet<>();
      roles.add(role);
      securityRepository.addMatch(SecurityTest.addressA, roles);
      securityManager.getConfiguration().addRole("auser", "arole");
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
      session.createQueue(SecurityTest.addressA, SecurityTest.queueA, true);
      ClientProducer cp = session.createProducer(SecurityTest.addressA);
      cp.send(session.createMessage(false));
      session.close();

      Queue binding = (Queue) server.getPostOffice().getBinding(new SimpleString(SecurityTest.queueA)).getBindable();
      Assert.assertEquals(0, getMessageCount(binding));
   }

   @Test
   public void testCreateConsumerWithRole() throws Exception {
      ActiveMQServer server = createServer();
      server.start();
      HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
      ActiveMQJAASSecurityManager securityManager = (ActiveMQJAASSecurityManager) server.getSecurityManager();
      securityManager.getConfiguration().addUser("auser", "pass");
      securityManager.getConfiguration().addUser("guest", "guest");
      securityManager.getConfiguration().addRole("guest", "guest");
      securityManager.getConfiguration().setDefaultUser("guest");
      Role role = new Role("arole", false, true, false, false, false, false, false, false, false, false);
      Role sendRole = new Role("guest", true, false, true, false, false, false, false, false, false, false);
      Set<Role> roles = new HashSet<>();
      roles.add(sendRole);
      roles.add(role);
      securityRepository.addMatch(SecurityTest.addressA, roles);
      securityManager.getConfiguration().addRole("auser", "arole");
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession senSession = cf.createSession(false, true, true);
      ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
      senSession.createQueue(SecurityTest.addressA, SecurityTest.queueA, true);
      ClientProducer cp = senSession.createProducer(SecurityTest.addressA);
      cp.send(session.createMessage(false));
      session.createConsumer(SecurityTest.queueA);
      session.close();
      senSession.close();
   }

   @Test
   public void testCreateConsumerWithoutRole() throws Exception {
      ActiveMQServer server = createServer();
      server.start();
      HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
      ActiveMQJAASSecurityManager securityManager = (ActiveMQJAASSecurityManager) server.getSecurityManager();
      securityManager.getConfiguration().addUser("auser", "pass");
      securityManager.getConfiguration().addUser("guest", "guest");
      securityManager.getConfiguration().addRole("guest", "guest");
      securityManager.getConfiguration().setDefaultUser("guest");
      Role role = new Role("arole", false, false, false, false, false, false, false, false, false, false);
      Role sendRole = new Role("guest", true, false, true, false, false, false, false, false, false, false);
      Set<Role> roles = new HashSet<>();
      roles.add(sendRole);
      roles.add(role);
      securityRepository.addMatch(SecurityTest.addressA, roles);
      securityManager.getConfiguration().addRole("auser", "arole");
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession senSession = cf.createSession(false, true, true);
      ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
      senSession.createQueue(SecurityTest.addressA, SecurityTest.queueA, true);
      ClientProducer cp = senSession.createProducer(SecurityTest.addressA);
      cp.send(session.createMessage(false));
      try {
         session.createConsumer(SecurityTest.queueA);
      } catch (ActiveMQSecurityException se) {
         //ok
      } catch (ActiveMQException e) {
         fail("Invalid Exception type:" + e.getType());
      }
      session.close();
      senSession.close();
   }

   @Test
   public void testSendMessageUpdateRoleCached() throws Exception {
      Configuration configuration = createDefaultInVMConfig().setSecurityEnabled(true).setSecurityInvalidationInterval(10000);
      ActiveMQServer server = createServer(false, configuration);
      server.start();
      HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
      ActiveMQJAASSecurityManager securityManager = (ActiveMQJAASSecurityManager) server.getSecurityManager();
      securityManager.getConfiguration().addUser("auser", "pass");
      securityManager.getConfiguration().addUser("guest", "guest");
      securityManager.getConfiguration().addRole("guest", "guest");
      securityManager.getConfiguration().setDefaultUser("guest");
      Role role = new Role("arole", false, false, false, false, false, false, false, false, false, false);
      Role sendRole = new Role("guest", true, false, true, false, false, false, false, false, false, false);
      Role receiveRole = new Role("receiver", false, true, false, false, false, false, false, false, false, false);
      Set<Role> roles = new HashSet<>();
      roles.add(sendRole);
      roles.add(role);
      roles.add(receiveRole);
      securityRepository.addMatch(SecurityTest.addressA, roles);
      securityManager.getConfiguration().addRole("auser", "arole");
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession senSession = cf.createSession(false, true, true);
      ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
      senSession.createQueue(SecurityTest.addressA, SecurityTest.queueA, true);
      ClientProducer cp = senSession.createProducer(SecurityTest.addressA);
      cp.send(session.createMessage(false));
      try {
         session.createConsumer(SecurityTest.queueA);
      } catch (ActiveMQSecurityException se) {
         //ok
      } catch (ActiveMQException e) {
         fail("Invalid Exception type:" + e.getType());
      }

      securityManager.getConfiguration().addRole("auser", "receiver");

      session.createConsumer(SecurityTest.queueA);

      // Removing the Role... the check should be cached, so the next createConsumer shouldn't fail
      securityManager.getConfiguration().removeRole("auser", "receiver");

      session.createConsumer(SecurityTest.queueA);

      session.close();

      senSession.close();
   }

   @Test
   public void testSendMessageUpdateRoleCached2() throws Exception {
      Configuration configuration = createDefaultInVMConfig().setSecurityEnabled(true).setSecurityInvalidationInterval(0);
      ActiveMQServer server = createServer(false, configuration);

      server.start();
      HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
      ActiveMQJAASSecurityManager securityManager = (ActiveMQJAASSecurityManager) server.getSecurityManager();
      securityManager.getConfiguration().addUser("auser", "pass");
      securityManager.getConfiguration().addUser("guest", "guest");
      securityManager.getConfiguration().addRole("guest", "guest");
      securityManager.getConfiguration().setDefaultUser("guest");
      Role role = new Role("arole", false, false, false, false, false, false, false, false, false, false);
      Role sendRole = new Role("guest", true, false, true, false, false, false, false, false, false, false);
      Role receiveRole = new Role("receiver", false, true, false, false, false, false, false, false, false, false);
      Set<Role> roles = new HashSet<>();
      roles.add(sendRole);
      roles.add(role);
      roles.add(receiveRole);
      securityRepository.addMatch(SecurityTest.addressA, roles);
      securityManager.getConfiguration().addRole("auser", "arole");
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession senSession = cf.createSession(false, true, true);
      ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
      senSession.createQueue(SecurityTest.addressA, SecurityTest.queueA, true);
      ClientProducer cp = senSession.createProducer(SecurityTest.addressA);
      cp.send(session.createMessage(false));
      try {
         session.createConsumer(SecurityTest.queueA);
      } catch (ActiveMQSecurityException se) {
         //ok
      } catch (ActiveMQException e) {
         fail("Invalid Exception type:" + e.getType());
      }

      securityManager.getConfiguration().addRole("auser", "receiver");

      session.createConsumer(SecurityTest.queueA);

      // Removing the Role... the check should be cached... but we used
      // setSecurityInvalidationInterval(0), so the
      // next createConsumer should fail
      securityManager.getConfiguration().removeRole("auser", "receiver");

      try {
         session.createConsumer(SecurityTest.queueA);
      } catch (ActiveMQSecurityException se) {
         //ok
      } catch (ActiveMQException e) {
         fail("Invalid Exception type:" + e.getType());
      }

      session.close();

      senSession.close();
   }

   @Test
   public void testSendMessageUpdateSender() throws Exception {
      Configuration configuration = createDefaultInVMConfig().setSecurityEnabled(true).setSecurityInvalidationInterval(-1);
      ActiveMQServer server = createServer(false, configuration);
      server.start();
      HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
      ActiveMQJAASSecurityManager securityManager = (ActiveMQJAASSecurityManager) server.getSecurityManager();
      securityManager.getConfiguration().addUser("auser", "pass");
      securityManager.getConfiguration().addUser("guest", "guest");
      securityManager.getConfiguration().addRole("guest", "guest");
      securityManager.getConfiguration().setDefaultUser("guest");
      Role role = new Role("arole", false, false, false, false, false, false, false, false, false, false);
      System.out.println("guest:" + role);
      Role sendRole = new Role("guest", true, false, true, false, false, false, false, false, false, false);
      System.out.println("guest:" + sendRole);
      Role receiveRole = new Role("receiver", false, true, false, false, false, false, false, false, false, false);
      System.out.println("guest:" + receiveRole);
      Set<Role> roles = new HashSet<>();
      roles.add(sendRole);
      roles.add(role);
      roles.add(receiveRole);
      securityRepository.addMatch(SecurityTest.addressA, roles);
      securityManager.getConfiguration().addRole("auser", "arole");
      ClientSessionFactory cf = createSessionFactory(locator);

      ClientSession senSession = cf.createSession(false, true, true);
      ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
      senSession.createQueue(SecurityTest.addressA, SecurityTest.queueA, true);
      ClientProducer cp = senSession.createProducer(SecurityTest.addressA);
      cp.send(session.createMessage(false));
      try {
         session.createConsumer(SecurityTest.queueA);
      } catch (ActiveMQSecurityException se) {
         //ok
      } catch (ActiveMQException e) {
         fail("Invalid Exception type:" + e.getType());
      }

      securityManager.getConfiguration().addRole("auser", "receiver");

      session.createConsumer(SecurityTest.queueA);

      // Removing the Role... the check should be cached... but we used
      // setSecurityInvalidationInterval(0), so the
      // next createConsumer should fail
      securityManager.getConfiguration().removeRole("auser", "guest");

      ClientSession sendingSession = cf.createSession("auser", "pass", false, false, false, false, 0);
      ClientProducer prod = sendingSession.createProducer(SecurityTest.addressA);
      prod.send(CreateMessage.createTextMessage(sendingSession, "Test", true));
      prod.send(CreateMessage.createTextMessage(sendingSession, "Test", true));
      try {
         sendingSession.commit();
         Assert.fail("Expected exception");
      } catch (ActiveMQException e) {
         // I would expect the commit to fail, since there were failures registered
      }

      sendingSession.close();

      Xid xid = newXID();

      sendingSession = cf.createSession("auser", "pass", true, false, false, false, 0);
      sendingSession.start(xid, XAResource.TMNOFLAGS);

      prod = sendingSession.createProducer(SecurityTest.addressA);
      prod.send(CreateMessage.createTextMessage(sendingSession, "Test", true));
      prod.send(CreateMessage.createTextMessage(sendingSession, "Test", true));
      sendingSession.end(xid, XAResource.TMSUCCESS);

      try {
         sendingSession.prepare(xid);
         Assert.fail("Exception was expected");
      } catch (Exception e) {
         e.printStackTrace();
      }

      // A prepare shouldn't mark any recoverable resources
      Xid[] xids = sendingSession.recover(XAResource.TMSTARTRSCAN);
      Assert.assertEquals(0, xids.length);

      session.close();

      senSession.close();

      sendingSession.close();
   }

   @Test
   public void testSendManagementWithRole() throws Exception {
      ActiveMQServer server = createServer();

      server.start();
      HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
      ActiveMQJAASSecurityManager securityManager = (ActiveMQJAASSecurityManager) server.getSecurityManager();
      securityManager.getConfiguration().addUser("auser", "pass");
      Role role = new Role("arole", false, false, false, false, false, false, true, false, false, false);
      Set<Role> roles = new HashSet<>();
      roles.add(role);
      securityRepository.addMatch(configuration.getManagementAddress().toString(), roles);
      securityManager.getConfiguration().addRole("auser", "arole");
      locator.setBlockOnNonDurableSend(true);
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
      ClientProducer cp = session.createProducer(configuration.getManagementAddress());
      cp.send(session.createMessage(false));
      session.close();
   }

   @Test
   public void testSendManagementWithoutRole() throws Exception {
      ActiveMQServer server = createServer();

      server.start();
      HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
      ActiveMQJAASSecurityManager securityManager = (ActiveMQJAASSecurityManager) server.getSecurityManager();
      securityManager.getConfiguration().addUser("auser", "pass");
      Role role = new Role("arole", false, false, true, false, false, false, false, false, false, false);
      Set<Role> roles = new HashSet<>();
      roles.add(role);
      securityRepository.addMatch(configuration.getManagementAddress().toString(), roles);
      securityManager.getConfiguration().addRole("auser", "arole");
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
      session.createQueue(configuration.getManagementAddress().toString(), SecurityTest.queueA, true);
      ClientProducer cp = session.createProducer(configuration.getManagementAddress());
      cp.send(session.createMessage(false));
      try {
         cp.send(session.createMessage(false));
      } catch (ActiveMQSecurityException se) {
         //ok
      } catch (ActiveMQException e) {
         fail("Invalid Exception type:" + e.getType());
      }
      session.close();

   }

   @Test
   public void testNonBlockSendManagementWithoutRole() throws Exception {
      ActiveMQServer server = createServer();

      server.start();
      HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
      ActiveMQJAASSecurityManager securityManager = (ActiveMQJAASSecurityManager) server.getSecurityManager();
      securityManager.getConfiguration().addUser("auser", "pass");
      Role role = new Role("arole", false, false, true, false, false, false, false, false, false, false);
      Set<Role> roles = new HashSet<>();
      roles.add(role);
      securityRepository.addMatch(configuration.getManagementAddress().toString(), roles);
      securityManager.getConfiguration().addRole("auser", "arole");
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
      session.createQueue(configuration.getManagementAddress().toString(), SecurityTest.queueA, true);
      ClientProducer cp = session.createProducer(configuration.getManagementAddress());
      cp.send(session.createMessage(false));
      session.close();

      Queue binding = (Queue) server.getPostOffice().getBinding(new SimpleString(SecurityTest.queueA)).getBindable();
      Assert.assertEquals(0, getMessageCount(binding));

   }

   @Test
   public void testComplexRoles() throws Exception {
      ActiveMQServer server = createServer();
      server.start();
      ActiveMQJAASSecurityManager securityManager = (ActiveMQJAASSecurityManager) server.getSecurityManager();
      securityManager.getConfiguration().addUser("all", "all");
      securityManager.getConfiguration().addUser("bill", "activemq");
      securityManager.getConfiguration().addUser("andrew", "activemq1");
      securityManager.getConfiguration().addUser("frank", "activemq2");
      securityManager.getConfiguration().addUser("sam", "activemq3");
      securityManager.getConfiguration().addRole("all", "all");
      securityManager.getConfiguration().addRole("bill", "user");
      securityManager.getConfiguration().addRole("andrew", "europe-user");
      securityManager.getConfiguration().addRole("andrew", "user");
      securityManager.getConfiguration().addRole("frank", "us-user");
      securityManager.getConfiguration().addRole("frank", "news-user");
      securityManager.getConfiguration().addRole("frank", "user");
      securityManager.getConfiguration().addRole("sam", "news-user");
      securityManager.getConfiguration().addRole("sam", "user");
      Role all = new Role("all", true, true, true, true, true, true, true, true, true, true);
      HierarchicalRepository<Set<Role>> repository = server.getSecurityRepository();
      Set<Role> add = new HashSet<>();
      add.add(new Role("user", true, true, true, true, true, true, false, true, true, true));
      add.add(all);
      repository.addMatch("#", add);
      Set<Role> add1 = new HashSet<>();
      add1.add(all);
      add1.add(new Role("user", false, false, true, true, true, true, false, true, true, true));
      add1.add(new Role("europe-user", true, false, false, false, false, false, false, true, true, true));
      add1.add(new Role("news-user", false, true, false, false, false, false, false, true, true, true));
      repository.addMatch("news.europe.#", add1);
      Set<Role> add2 = new HashSet<>();
      add2.add(all);
      add2.add(new Role("user", false, false, true, true, true, true, false, true, true, true));
      add2.add(new Role("us-user", true, false, false, false, false, false, false, true, true, true));
      add2.add(new Role("news-user", false, true, false, false, false, false, false, true, true, true));
      repository.addMatch("news.us.#", add2);
      ClientSession billConnection = null;
      ClientSession andrewConnection = null;
      ClientSession frankConnection = null;
      ClientSession samConnection = null;
      locator.setBlockOnNonDurableSend(true).setBlockOnDurableSend(true);
      ClientSessionFactory factory = createSessionFactory(locator);

      ClientSession adminSession = factory.createSession("all", "all", false, true, true, false, -1);
      String genericQueueName = "genericQueue";
      adminSession.createQueue(genericQueueName, genericQueueName, false);
      String eurQueueName = "news.europe.europeQueue";
      adminSession.createQueue(eurQueueName, eurQueueName, false);
      String usQueueName = "news.us.usQueue";
      adminSession.createQueue(usQueueName, usQueueName, false);
      // Step 4. Try to create a JMS Connection without user/password. It will fail.
      try {
         factory.createSession(false, true, true);
         Assert.fail("should throw exception");
      } catch (ActiveMQSecurityException se) {
         //ok
      } catch (ActiveMQException e) {
         fail("Invalid Exception type:" + e.getType());
      }

      // Step 5. bill tries to make a connection using wrong password
      try {
         billConnection = factory.createSession("bill", "activemq1", false, true, true, false, -1);
         Assert.fail("should throw exception");
      } catch (ActiveMQSecurityException se) {
         //ok
      } catch (ActiveMQException e) {
         fail("Invalid Exception type:" + e.getType());
      }

      // Step 6. bill makes a good connection.
      billConnection = factory.createSession("bill", "activemq", false, true, true, false, -1);

      // Step 7. andrew makes a good connection.
      andrewConnection = factory.createSession("andrew", "activemq1", false, true, true, false, -1);

      // Step 8. frank makes a good connection.
      frankConnection = factory.createSession("frank", "activemq2", false, true, true, false, -1);

      // Step 9. sam makes a good connection.
      samConnection = factory.createSession("sam", "activemq3", false, true, true, false, -1);

      checkUserSendAndReceive(genericQueueName, billConnection);
      checkUserSendAndReceive(genericQueueName, andrewConnection);
      checkUserSendAndReceive(genericQueueName, frankConnection);
      checkUserSendAndReceive(genericQueueName, samConnection);

      // Step 11. Check permissions on news.europe.europeTopic for bill: can't send and can't
      // receive
      checkUserNoSendNoReceive(eurQueueName, billConnection, adminSession);

      // Step 12. Check permissions on news.europe.europeTopic for andrew: can send but can't
      // receive
      checkUserSendNoReceive(eurQueueName, andrewConnection);

      // Step 13. Check permissions on news.europe.europeTopic for frank: can't send but can
      // receive
      checkUserReceiveNoSend(eurQueueName, frankConnection, adminSession);

      // Step 14. Check permissions on news.europe.europeTopic for sam: can't send but can
      // receive
      checkUserReceiveNoSend(eurQueueName, samConnection, adminSession);

      // Step 15. Check permissions on news.us.usTopic for bill: can't send and can't receive
      checkUserNoSendNoReceive(usQueueName, billConnection, adminSession);

      // Step 16. Check permissions on news.us.usTopic for andrew: can't send and can't receive
      checkUserNoSendNoReceive(usQueueName, andrewConnection, adminSession);

      // Step 17. Check permissions on news.us.usTopic for frank: can both send and receive
      checkUserSendAndReceive(usQueueName, frankConnection);

      // Step 18. Check permissions on news.us.usTopic for same: can't send but can receive
      checkUserReceiveNoSend(usQueueName, samConnection, adminSession);

      billConnection.close();

      andrewConnection.close();

      frankConnection.close();

      samConnection.close();

      adminSession.close();

   }

   @Test
   @Ignore
   public void testComplexRoles2() throws Exception {
      ActiveMQServer server = createServer();
      server.start();
      ActiveMQJAASSecurityManager securityManager = (ActiveMQJAASSecurityManager) server.getSecurityManager();
      securityManager.getConfiguration().addUser("all", "all");
      securityManager.getConfiguration().addUser("bill", "activemq");
      securityManager.getConfiguration().addUser("andrew", "activemq1");
      securityManager.getConfiguration().addUser("frank", "activemq2");
      securityManager.getConfiguration().addUser("sam", "activemq3");
      securityManager.getConfiguration().addRole("all", "all");
      securityManager.getConfiguration().addRole("bill", "user");
      securityManager.getConfiguration().addRole("andrew", "europe-user");
      securityManager.getConfiguration().addRole("andrew", "user");
      securityManager.getConfiguration().addRole("frank", "us-user");
      securityManager.getConfiguration().addRole("frank", "news-user");
      securityManager.getConfiguration().addRole("frank", "user");
      securityManager.getConfiguration().addRole("sam", "news-user");
      securityManager.getConfiguration().addRole("sam", "user");
      Role all = new Role("all", true, true, true, true, true, true, true, true, true, true);
      HierarchicalRepository<Set<Role>> repository = server.getSecurityRepository();
      Set<Role> add = new HashSet<>();
      add.add(new Role("user", true, true, true, true, true, true, false, true, true, true));
      add.add(all);
      repository.addMatch("#", add);
      Set<Role> add1 = new HashSet<>();
      add1.add(all);
      add1.add(new Role("user", false, false, true, true, true, true, false, true, true, true));
      add1.add(new Role("europe-user", true, false, false, false, false, false, false, true, true, true));
      add1.add(new Role("news-user", false, true, false, false, false, false, false, true, true, true));
      repository.addMatch("news.europe.#", add1);
      Set<Role> add2 = new HashSet<>();
      add2.add(all);
      add2.add(new Role("user", false, false, true, true, true, true, false, true, true, true));
      add2.add(new Role("us-user", true, false, false, false, false, false, false, true, true, true));
      add2.add(new Role("news-user", false, true, false, false, false, false, false, true, true, true));
      repository.addMatch("news.us.#", add2);
      ClientSession billConnection = null;
      ClientSession andrewConnection = null;
      ClientSession frankConnection = null;
      ClientSession samConnection = null;
      locator.setBlockOnNonDurableSend(true).setBlockOnDurableSend(true);
      ClientSessionFactory factory = createSessionFactory(locator);

      ClientSession adminSession = factory.createSession("all", "all", false, true, true, false, -1);
      String genericQueueName = "genericQueue";
      adminSession.createQueue(genericQueueName, genericQueueName, false);
      String eurQueueName = "news.europe.europeQueue";
      adminSession.createQueue(eurQueueName, eurQueueName, false);
      String usQueueName = "news.us.usQueue";
      adminSession.createQueue(usQueueName, usQueueName, false);
      // Step 4. Try to create a JMS Connection without user/password. It will fail.
      try {
         factory.createSession(false, true, true);
         Assert.fail("should throw exception");
      } catch (ActiveMQSecurityException se) {
         //ok
      } catch (ActiveMQException e) {
         fail("Invalid Exception type:" + e.getType());
      }

      // Step 5. bill tries to make a connection using wrong password
      try {
         billConnection = factory.createSession("bill", "activemq1", false, true, true, false, -1);
         Assert.fail("should throw exception");
      } catch (ActiveMQSecurityException se) {
         //ok
      } catch (ActiveMQException e) {
         fail("Invalid Exception type:" + e.getType());
      }

      // Step 6. bill makes a good connection.
      billConnection = factory.createSession("bill", "activemq", false, true, true, false, -1);

      // Step 7. andrew makes a good connection.
      andrewConnection = factory.createSession("andrew", "activemq1", false, true, true, false, -1);

      // Step 8. frank makes a good connection.
      frankConnection = factory.createSession("frank", "activemq2", false, true, true, false, -1);

      // Step 9. sam makes a good connection.
      samConnection = factory.createSession("sam", "activemq3", false, true, true, false, -1);

      checkUserSendAndReceive(genericQueueName, billConnection);
      checkUserSendAndReceive(genericQueueName, andrewConnection);
      checkUserSendAndReceive(genericQueueName, frankConnection);
      checkUserSendAndReceive(genericQueueName, samConnection);

      // Step 11. Check permissions on news.europe.europeTopic for bill: can't send and can't
      // receive
      checkUserNoSendNoReceive(eurQueueName, billConnection, adminSession);

      // Step 12. Check permissions on news.europe.europeTopic for andrew: can send but can't
      // receive
      checkUserSendNoReceive(eurQueueName, andrewConnection);

      // Step 13. Check permissions on news.europe.europeTopic for frank: can't send but can
      // receive
      checkUserReceiveNoSend(eurQueueName, frankConnection, adminSession);

      // Step 14. Check permissions on news.europe.europeTopic for sam: can't send but can
      // receive
      checkUserReceiveNoSend(eurQueueName, samConnection, adminSession);

      // Step 15. Check permissions on news.us.usTopic for bill: can't send and can't receive
      checkUserNoSendNoReceive(usQueueName, billConnection, adminSession);

      // Step 16. Check permissions on news.us.usTopic for andrew: can't send and can't receive
      checkUserNoSendNoReceive(usQueueName, andrewConnection, adminSession);

      // Step 17. Check permissions on news.us.usTopic for frank: can both send and receive
      checkUserSendAndReceive(usQueueName, frankConnection);

      // Step 18. Check permissions on news.us.usTopic for same: can't send but can receive
      checkUserReceiveNoSend(usQueueName, samConnection, adminSession);

   }

   @Test
   public void testCustomSecurityManager() throws Exception {
      final Configuration configuration = createDefaultInVMConfig().setSecurityEnabled(true);
      final ActiveMQSecurityManager customSecurityManager = new ActiveMQSecurityManager() {
         @Override
         public boolean validateUser(final String username, final String password) {
            return (username.equals("foo") || username.equals("bar") || username.equals("all")) && password.equals("frobnicate");
         }

         @Override
         public boolean validateUserAndRole(final String username,
                                            final String password,
                                            final Set<Role> requiredRoles,
                                            final CheckType checkType) {

            if ((username.equals("foo") || username.equals("bar") || username.equals("all")) && password.equals("frobnicate")) {

               if (username.equals("all")) {
                  return true;
               } else if (username.equals("foo")) {
                  return checkType == CheckType.CONSUME || checkType == CheckType.CREATE_NON_DURABLE_QUEUE;
               } else if (username.equals("bar")) {
                  return checkType == CheckType.SEND || checkType == CheckType.CREATE_NON_DURABLE_QUEUE;
               } else {
                  return false;
               }
            } else {
               return false;
            }
         }
      };
      final ActiveMQServer server = addServer(new ActiveMQServerImpl(configuration, customSecurityManager));
      server.start();

      final ServerLocator locator = createInVMNonHALocator();
      locator.setBlockOnNonDurableSend(true).setBlockOnDurableSend(true);
      final ClientSessionFactory factory = createSessionFactory(locator);
      ClientSession adminSession = factory.createSession("all", "frobnicate", false, true, true, false, -1);

      final String queueName = "test.queue";
      adminSession.createQueue(queueName, queueName, false);

      // Wrong user name
      try {
         factory.createSession("baz", "frobnicate", false, true, true, false, -1);
         Assert.fail("should throw exception");
      } catch (ActiveMQSecurityException se) {
         //ok
      } catch (ActiveMQException e) {
         fail("Invalid Exception type:" + e.getType());
      }

      // Wrong password
      try {
         factory.createSession("foo", "xxx", false, true, true, false, -1);
         Assert.fail("should throw exception");
      } catch (ActiveMQSecurityException se) {
         //ok
      } catch (ActiveMQException e) {
         fail("Invalid Exception type:" + e.getType());
      }

      // Correct user and password, allowed to send but not receive
      {
         final ClientSession session = factory.createSession("foo", "frobnicate", false, true, true, false, -1);
         checkUserReceiveNoSend(queueName, session, adminSession);
      }

      // Correct user and password, allowed to receive but not send
      {
         final ClientSession session = factory.createSession("bar", "frobnicate", false, true, true, false, -1);
         checkUserSendNoReceive(queueName, session);
      }

   }

   @Test
   public void testCustomSecurityManager2() throws Exception {
      final Configuration configuration = createDefaultInVMConfig().setSecurityEnabled(true);
      final ActiveMQSecurityManager customSecurityManager = new ActiveMQSecurityManager2() {
         @Override
         public boolean validateUser(final String username, final String password) {
            fail("Unexpected call to overridden method");
            return false;
         }

         @Override
         public boolean validateUser(final String username,
                                     final String password,
                                     final X509Certificate[] certificates) {
            return (username.equals("foo") || username.equals("bar") || username.equals("all")) && password.equals("frobnicate");
         }

         @Override
         public boolean validateUserAndRole(final String username,
                                            final String password,
                                            final Set<Role> requiredRoles,
                                            final CheckType checkType) {

            fail("Unexpected call to overridden method");
            return false;
         }

         @Override
         public boolean validateUserAndRole(final String username,
                                            final String password,
                                            final Set<Role> requiredRoles,
                                            final CheckType checkType,
                                            final String address,
                                            final RemotingConnection connection) {

            if (!(connection.getTransportConnection() instanceof InVMConnection)) {
               return false;
            }

            if ((username.equals("foo") || username.equals("bar") || username.equals("all")) && password.equals("frobnicate")) {

               if (username.equals("all")) {
                  return true;
               } else if (username.equals("foo")) {
                  return address.equals("test.queue") && checkType == CheckType.CONSUME;
               } else if (username.equals("bar")) {
                  return address.equals("test.queue") && checkType == CheckType.SEND;
               } else {
                  return false;
               }
            } else {
               return false;
            }
         }
      };
      final ActiveMQServer server = addServer(new ActiveMQServerImpl(configuration, customSecurityManager));
      server.start();

      final ServerLocator locator = createInVMNonHALocator();
      locator.setBlockOnNonDurableSend(true).setBlockOnDurableSend(true);
      final ClientSessionFactory factory = createSessionFactory(locator);
      ClientSession adminSession = factory.createSession("all", "frobnicate", false, true, true, false, -1);

      final String queueName = "test.queue";
      adminSession.createQueue(queueName, queueName, false);

      final String otherQueueName = "other.queue";
      adminSession.createQueue(otherQueueName, otherQueueName, false);

      // Wrong user name
      try {
         factory.createSession("baz", "frobnicate", false, true, true, false, -1);
         Assert.fail("should throw exception");
      } catch (ActiveMQSecurityException se) {
         //ok
      } catch (ActiveMQException e) {
         fail("Invalid Exception type:" + e.getType());
      }

      // Wrong password
      try {
         factory.createSession("foo", "xxx", false, true, true, false, -1);
         Assert.fail("should throw exception");
      } catch (ActiveMQSecurityException se) {
         //ok
      } catch (ActiveMQException e) {
         fail("Invalid Exception type:" + e.getType());
      }

      // Correct user and password, wrong queue for sending
      try {
         final ClientSession session = factory.createSession("foo", "frobnicate", false, true, true, false, -1);
         checkUserReceiveNoSend(otherQueueName, session, adminSession);
         Assert.fail("should throw exception");
      } catch (ActiveMQSecurityException se) {
         //ok
      } catch (ActiveMQException e) {
         fail("Invalid Exception type:" + e.getType());
      }

      // Correct user and password, wrong queue for receiving
      try {
         final ClientSession session = factory.createSession("foo", "frobnicate", false, true, true, false, -1);
         checkUserReceiveNoSend(otherQueueName, session, adminSession);
         Assert.fail("should throw exception");
      } catch (ActiveMQSecurityException se) {
         //ok
      } catch (ActiveMQException e) {
         fail("Invalid Exception type:" + e.getType());
      }

      // Correct user and password, allowed to send but not receive
      {
         final ClientSession session = factory.createSession("foo", "frobnicate", false, true, true, false, -1);
         checkUserReceiveNoSend(queueName, session, adminSession);
      }

      // Correct user and password, allowed to receive but not send
      {
         final ClientSession session = factory.createSession("bar", "frobnicate", false, true, true, false, -1);
         checkUserSendNoReceive(queueName, session);
      }
   }

   @Test
   public void testCustomSecurityManager3() throws Exception {
      final Configuration configuration = createDefaultInVMConfig().setSecurityEnabled(true);
      final ActiveMQSecurityManager customSecurityManager = new ActiveMQSecurityManager3() {
         @Override
         public boolean validateUser(final String username, final String password) {
            fail("Unexpected call to overridden method");
            return false;
         }

         @Override
         public String validateUser(final String username,
                                    final String password,
                                    final RemotingConnection remotingConnection) {
            if ((username.equals("foo") || username.equals("bar") || username.equals("all")) && password.equals("frobnicate")) {
               return username;
            } else {
               return null;
            }
         }

         @Override
         public boolean validateUserAndRole(final String username,
                                            final String password,
                                            final Set<Role> requiredRoles,
                                            final CheckType checkType) {

            fail("Unexpected call to overridden method");
            return false;
         }

         @Override
         public String validateUserAndRole(final String username,
                                           final String password,
                                           final Set<Role> requiredRoles,
                                           final CheckType checkType,
                                           final String address,
                                           final RemotingConnection connection) {

            if (!(connection.getTransportConnection() instanceof InVMConnection)) {
               return null;
            }

            if ((username.equals("foo") || username.equals("bar") || username.equals("all")) && password.equals("frobnicate")) {

               if (username.equals("all")) {
                  return username;
               } else if (username.equals("foo")) {
                  if (address.equals("test.queue") && checkType == CheckType.CONSUME)
                     return username;
                  else
                     return null;
               } else if (username.equals("bar")) {
                  if (address.equals("test.queue") && checkType == CheckType.SEND)
                     return username;
                  else
                     return null;
               } else {
                  return null;
               }
            } else {
               return null;
            }
         }
      };
      final ActiveMQServer server = addServer(new ActiveMQServerImpl(configuration, customSecurityManager));
      server.start();

      final ServerLocator locator = createInVMNonHALocator();
      locator.setBlockOnNonDurableSend(true).setBlockOnDurableSend(true);
      final ClientSessionFactory factory = createSessionFactory(locator);
      ClientSession adminSession = factory.createSession("all", "frobnicate", false, true, true, false, -1);

      final String queueName = "test.queue";
      adminSession.createQueue(queueName, queueName, false);

      final String otherQueueName = "other.queue";
      adminSession.createQueue(otherQueueName, otherQueueName, false);

      // Wrong user name
      try {
         factory.createSession("baz", "frobnicate", false, true, true, false, -1);
         Assert.fail("should throw exception");
      } catch (ActiveMQSecurityException se) {
         //ok
      } catch (ActiveMQException e) {
         fail("Invalid Exception type:" + e.getType());
      }

      // Wrong password
      try {
         factory.createSession("foo", "xxx", false, true, true, false, -1);
         Assert.fail("should throw exception");
      } catch (ActiveMQSecurityException se) {
         //ok
      } catch (ActiveMQException e) {
         fail("Invalid Exception type:" + e.getType());
      }

      // Correct user and password, wrong queue for sending
      try {
         final ClientSession session = factory.createSession("foo", "frobnicate", false, true, true, false, -1);
         checkUserReceiveNoSend(otherQueueName, session, adminSession);
         Assert.fail("should throw exception");
      } catch (ActiveMQSecurityException se) {
         //ok
      } catch (ActiveMQException e) {
         fail("Invalid Exception type:" + e.getType());
      }

      // Correct user and password, wrong queue for receiving
      try {
         final ClientSession session = factory.createSession("foo", "frobnicate", false, true, true, false, -1);
         checkUserReceiveNoSend(otherQueueName, session, adminSession);
         Assert.fail("should throw exception");
      } catch (ActiveMQSecurityException se) {
         //ok
      } catch (ActiveMQException e) {
         fail("Invalid Exception type:" + e.getType());
      }

      // Correct user and password, allowed to send but not receive
      {
         final ClientSession session = factory.createSession("foo", "frobnicate", false, true, true, false, -1);
         checkUserReceiveNoSend(queueName, session, adminSession);
      }

      // Correct user and password, allowed to receive but not send
      {
         final ClientSession session = factory.createSession("bar", "frobnicate", false, true, true, false, -1);
         checkUserSendNoReceive(queueName, session);
      }
   }

   // Check the user connection has both send and receive permissions on the queue
   private void checkUserSendAndReceive(final String genericQueueName,
                                        final ClientSession connection) throws Exception {
      connection.start();
      try {
         ClientProducer prod = connection.createProducer(genericQueueName);
         ClientConsumer con = connection.createConsumer(genericQueueName);
         ClientMessage m = connection.createMessage(false);
         prod.send(m);
         ClientMessage rec = con.receive(1000);
         Assert.assertNotNull(rec);
         rec.acknowledge();
      } finally {
         connection.stop();
      }
   }

   // Check the user can receive message but cannot send message.
   private void checkUserReceiveNoSend(final String queue,
                                       final ClientSession connection,
                                       final ClientSession sendingConn) throws Exception {
      connection.start();
      try {
         ClientProducer prod = connection.createProducer(queue);
         ClientMessage m = connection.createMessage(false);
         try {
            prod.send(m);
            Assert.fail("should throw exception");
         } catch (ActiveMQException e) {
            // pass
         }

         prod = sendingConn.createProducer(queue);
         prod.send(m);
         ClientConsumer con = connection.createConsumer(queue);
         ClientMessage rec = con.receive(1000);
         Assert.assertNotNull(rec);
         rec.acknowledge();
      } finally {
         connection.stop();
      }
   }

   private void checkUserNoSendNoReceive(final String queue,
                                         final ClientSession connection,
                                         final ClientSession sendingConn) throws Exception {
      connection.start();
      try {
         ClientProducer prod = connection.createProducer(queue);
         ClientMessage m = connection.createMessage(false);
         try {
            prod.send(m);
            Assert.fail("should throw exception");
         } catch (ActiveMQException e) {
            // pass
         }

         prod = sendingConn.createProducer(queue);
         prod.send(m);

         try {
            connection.createConsumer(queue);
            Assert.fail("should throw exception");
         } catch (ActiveMQException e) {
            // pass
         }
      } finally {
         connection.stop();
      }
   }

   // Check the user can send message but cannot receive message
   private void checkUserSendNoReceive(final String queue, final ClientSession connection) throws Exception {
      ClientProducer prod = connection.createProducer(queue);
      ClientMessage m = connection.createMessage(false);
      prod.send(m);

      try {
         connection.createConsumer(queue);
         Assert.fail("should throw exception");
      } catch (ActiveMQSecurityException se) {
         //ok
      } catch (ActiveMQException e) {
         fail("Invalid Exception type:" + e.getType());
      }
   }
}
