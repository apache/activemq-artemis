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

import org.apache.activemq.artemis.api.core.ActiveMQSecurityException;
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
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.settings.HierarchicalRepository;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.apache.activemq.artemis.utils.RetryRule;
import org.apache.hadoop.minikdc.MiniKdc;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Investigate this as part of https://issues.apache.org/jira/browse/ARTEMIS-3038
 * This test will fail with any recent JDK (post 2014).
 * @throws Exception
 */
@Ignore
public class CoreClientOverOneWaySSLKerb5Test extends ActiveMQTestBase {

   @Rule
   public RetryRule retryRule = new RetryRule(2);

   public static final SimpleString QUEUE = new SimpleString("QueueOverKrb5SSL");
   public static final String CLIENT_PRINCIPAL = "client";
   public static final String SNI_HOST = "sni.host";
   public static final String SERVICE_PRINCIPAL = "host/" + SNI_HOST;

   static {
      String path = System.getProperty("java.security.auth.login.config");
      if (path == null) {
         URL resource = CoreClientOverOneWaySSLKerb5Test.class.getClassLoader().getResource("login.config");
         if (resource != null) {
            path = resource.getFile();
            System.setProperty("java.security.auth.login.config", path);
         }
      }
   }

   private MiniKdc kdc;
   private ActiveMQServer server;

   private TransportConfiguration tc;
   private TransportConfiguration inVMTc;
   private String userPrincipal;

   /**
    * Investigate this as part of https://issues.apache.org/jira/browse/ARTEMIS-3038
    * @throws Exception
    */
   @Ignore // Ignored because of http://www.oracle.com/technetwork/topics/security/poodlecve-2014-3566-2339408.html
   @Test
   public void testOneWaySSLWithGoodClientCipherSuite() throws Exception {

      // hard coded match, default_keytab_name in minikdc-krb5.conf template
      File userKeyTab = new File("target/test.krb5.keytab");
      kdc.createPrincipal(userKeyTab, CLIENT_PRINCIPAL, SERVICE_PRINCIPAL);

      createCustomSslServer();

      tc.getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      tc.getParams().put(TransportConstants.ENABLED_CIPHER_SUITES_PROP_NAME, getSuitableCipherSuite());
      tc.getParams().put(TransportConstants.SNIHOST_PROP_NAME, SNI_HOST); // static service name rather than dynamic machine name
      tc.getParams().put(TransportConstants.SSL_KRB5_CONFIG_PROP_NAME, "core-tls-krb5-client");
      final ServerLocator locator = addServerLocator(ActiveMQClient.createServerLocatorWithoutHA(tc));

      ClientSessionFactory sf = null;
      try {
         sf = createSessionFactory(locator);
         ClientSession session = sf.createSession(false, true, true);
         session.createQueue(new QueueConfiguration(CoreClientOverOneWaySSLKerb5Test.QUEUE).setRoutingType(RoutingType.ANYCAST));
         ClientProducer producer = session.createProducer(CoreClientOverOneWaySSLKerb5Test.QUEUE);

         final String text = RandomUtil.randomString();
         ClientMessage message = createTextMessage(session, text);
         producer.send(message);

         ClientConsumer consumer = session.createConsumer(CoreClientOverOneWaySSLKerb5Test.QUEUE);
         session.start();

         ClientMessage m = consumer.receive(1000);
         Assert.assertNotNull(m);
         Assert.assertEquals(text, m.getReadOnlyBodyBuffer().readString());
         System.err.println("m:" + m + ", user:" + m.getValidatedUserID());
         Assert.assertNotNull("got validated user", m.getValidatedUserID());
         Assert.assertTrue("krb id in validated user", m.getValidatedUserID().contains(CLIENT_PRINCIPAL));

      } finally {
         if (sf != null) {
            sf.close();
         }
         locator.close();
      }

      // validate only ssl creds work, try and fake the principal w/o ssl
      final ServerLocator inVmLocator = addServerLocator(ActiveMQClient.createServerLocatorWithoutHA(inVMTc));
      ClientSessionFactory inVmSf = null;
      try {
         inVmSf = createSessionFactory(inVmLocator);
         inVmSf.createSession(userPrincipal, "", false, false, false, false, 10);

         fail("supposed to throw exception");
      } catch (ActiveMQSecurityException e) {
         // expected
      } finally {
         if (inVmSf != null) {
            inVmSf.close();
         }
         inVmLocator.close();
      }
   }


   public String getSuitableCipherSuite() throws Exception {
      return "TLS_KRB5_WITH_3DES_EDE_CBC_SHA";
   }


   // Package protected ---------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      kdc = new MiniKdc(MiniKdc.createConf(), temporaryFolder.newFolder("kdc"));
      kdc.start();
   }

   @Override
   @After
   public void tearDown() throws Exception {
      try {
         kdc.stop();
      } finally {
         super.tearDown();
      }
   }

   private void createCustomSslServer() throws Exception {
      Map<String, Object> params = new HashMap<>();

      params.put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      params.put(TransportConstants.ENABLED_CIPHER_SUITES_PROP_NAME, getSuitableCipherSuite());
      params.put(TransportConstants.SSL_KRB5_CONFIG_PROP_NAME, "core-tls-krb5-server");

      ConfigurationImpl config = createBasicConfig().addAcceptorConfiguration(new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, params, "nettySSL"));
      config.setPopulateValidatedUser(true); // so we can verify the kerb5 id is present
      config.setSecurityEnabled(true);

      config.addAcceptorConfiguration(new TransportConfiguration(INVM_ACCEPTOR_FACTORY));

      ActiveMQSecurityManager securityManager = new ActiveMQJAASSecurityManager("Krb5Plus");
      server = addServer(ActiveMQServers.newActiveMQServer(config, ManagementFactory.getPlatformMBeanServer(), securityManager, false));
      HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();


      final String roleName = "ALLOW_ALL";
      Role role = new Role(roleName, true, true, true, true, true, true, true, true, true, true);
      Set<Role> roles = new HashSet<>();
      roles.add(role);
      securityRepository.addMatch(QUEUE.toString(), roles);

      server.start();
      waitForServerToStart(server);

      // note kerberos user does not exist on the broker save as a role member in dual-authentication-roles.properties
      userPrincipal = CLIENT_PRINCIPAL + "@" + kdc.getRealm();

      tc = new TransportConfiguration(NETTY_CONNECTOR_FACTORY);
      inVMTc = new TransportConfiguration(INVM_CONNECTOR_FACTORY);
   }
}
