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

import javax.naming.Context;
import javax.naming.NameClassPair;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import java.io.File;
import java.lang.management.ManagementFactory;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMAcceptorFactory;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMConnectorFactory;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.server.impl.LegacyLDAPSecuritySettingPlugin;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.core.annotations.ApplyLdifFiles;
import org.apache.directory.server.core.integ.AbstractLdapTestUnit;
import org.apache.directory.server.core.integ.FrameworkRunner;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

@RunWith(FrameworkRunner.class)
@CreateLdapServer(transports = {@CreateTransport(protocol = "LDAP", port = 1024)})
@ApplyLdifFiles("AMQauth.ldif")
public class LegacyLDAPSecuritySettingPluginListenerTest extends AbstractLdapTestUnit {

   static {
      String path = System.getProperty("java.security.auth.login.config");
      if (path == null) {
         URL resource = LegacyLDAPSecuritySettingPluginListenerTest.class.getClassLoader().getResource("login.config");
         if (resource != null) {
            path = resource.getFile();
            System.setProperty("java.security.auth.login.config", path);
         }
      }
   }

   private ServerLocator locator;
   ActiveMQServer server;

   public static final String TARGET_TMP = "./target/tmp";
   private static final String PRINCIPAL = "uid=admin,ou=system";
   private static final String CREDENTIALS = "secret";

   public LegacyLDAPSecuritySettingPluginListenerTest() {
      File parent = new File(TARGET_TMP);
      parent.mkdirs();
      temporaryFolder = new TemporaryFolder(parent);
   }

   @Rule
   public TemporaryFolder temporaryFolder;
   private String testDir;

   @Before
   public void setUp() throws Exception {
      locator = ActiveMQClient.createServerLocatorWithoutHA(new TransportConfiguration(InVMConnectorFactory.class.getCanonicalName()));
      testDir = temporaryFolder.getRoot().getAbsolutePath();

      LegacyLDAPSecuritySettingPlugin legacyLDAPSecuritySettingPlugin = new LegacyLDAPSecuritySettingPlugin();
      Map<String, String> map = new HashMap<>();
      map.put(LegacyLDAPSecuritySettingPlugin.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
      map.put(LegacyLDAPSecuritySettingPlugin.CONNECTION_URL, "ldap://localhost:1024");
      map.put(LegacyLDAPSecuritySettingPlugin.CONNECTION_USERNAME, "uid=admin,ou=system");
      map.put(LegacyLDAPSecuritySettingPlugin.CONNECTION_PASSWORD, "secret");
      map.put(LegacyLDAPSecuritySettingPlugin.CONNECTION_PROTOCOL, "s");
      map.put(LegacyLDAPSecuritySettingPlugin.AUTHENTICATION, "simple");
      map.put(LegacyLDAPSecuritySettingPlugin.ENABLE_LISTENER, "true");
      legacyLDAPSecuritySettingPlugin.init(map);

      ActiveMQJAASSecurityManager securityManager = new ActiveMQJAASSecurityManager("LDAPLogin");
      Configuration configuration = new ConfigurationImpl().setSecurityEnabled(true).addAcceptorConfiguration(new TransportConfiguration(InVMAcceptorFactory.class.getCanonicalName())).setJournalDirectory(ActiveMQTestBase.getJournalDir(testDir, 0, false)).setBindingsDirectory(ActiveMQTestBase.getBindingsDir(testDir, 0, false)).setPagingDirectory(ActiveMQTestBase.getPageDir(testDir, 0, false)).setLargeMessagesDirectory(ActiveMQTestBase.getLargeMessagesDir(testDir, 0, false)).setPersistenceEnabled(false).addSecuritySettingPlugin(legacyLDAPSecuritySettingPlugin);

      server = ActiveMQServers.newActiveMQServer(configuration, ManagementFactory.getPlatformMBeanServer(), securityManager, false);
   }

   @After
   public void tearDown() throws Exception {
      locator.close();
      server.stop();
   }

   @Test
   public void testRunning() throws Exception {
      DirContext ctx = getContext();

      HashSet<String> set = new HashSet<>();

      NamingEnumeration<NameClassPair> list = ctx.list("ou=system");

      while (list.hasMore()) {
         NameClassPair ncp = list.next();
         set.add(ncp.getName());
      }

      Assert.assertTrue(set.contains("uid=admin"));
      Assert.assertTrue(set.contains("ou=users"));
      Assert.assertTrue(set.contains("ou=groups"));
      Assert.assertTrue(set.contains("ou=configuration"));
      Assert.assertTrue(set.contains("prefNodeName=sysPrefRoot"));
   }

   private DirContext getContext() throws NamingException {
      Hashtable<String, String> env = new Hashtable<>();
      env.put(Context.PROVIDER_URL, "ldap://localhost:1024");
      env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
      env.put(Context.SECURITY_AUTHENTICATION, "simple");
      env.put(Context.SECURITY_PRINCIPAL, PRINCIPAL);
      env.put(Context.SECURITY_CREDENTIALS, CREDENTIALS);
      return new InitialDirContext(env);
   }

   @Test
   public void testProducerPermissionUpdate() throws Exception {
      server.getConfiguration().setSecurityInvalidationInterval(0);
      server.start();
      ClientSessionFactory cf = locator.createSessionFactory();
      String name = "queue1";
      ClientSession session = cf.createSession("first", "secret", false, true, true, false, 0);
      ClientSession session2 = cf.createSession("second", "secret", false, true, true, false, 0);
      session.createQueue(SimpleString.toSimpleString(name), SimpleString.toSimpleString(name));
      ClientProducer producer = session.createProducer();
      ClientProducer producer2 = session2.createProducer();
      producer.send(name, session.createMessage(true));

      try {
         producer2.send(name, session.createMessage(true));
         Assert.fail("Sending here should fail due to the original security data.");
      } catch (ActiveMQException e) {
         // ok
      }

      DirContext ctx = getContext();
      BasicAttributes basicAttributes = new BasicAttributes();
      basicAttributes.put("uniquemember", "uid=role2");
      ctx.modifyAttributes("cn=write,uid=queue1,ou=queues,ou=destinations,o=ActiveMQ,ou=system", DirContext.REPLACE_ATTRIBUTE, basicAttributes);
      ctx.close();

      producer2.send(name, session.createMessage(true));

      try {
         producer.send(name, session.createMessage(true));
         Assert.fail("Sending here should fail due to the modified security data.");
      } catch (ActiveMQException e) {
         // ok
      }

      cf.close();
   }

   @Test
   public void testConsumerPermissionUpdate() throws Exception {
      server.getConfiguration().setSecurityInvalidationInterval(0);
      server.start();
      ClientSessionFactory cf = locator.createSessionFactory();
      String queue = "queue1";
      ClientSession session = cf.createSession("first", "secret", false, true, true, false, 0);
      ClientSession session2 = cf.createSession("second", "secret", false, true, true, false, 0);
      session.createQueue(SimpleString.toSimpleString(queue), SimpleString.toSimpleString(queue));
      ClientConsumer consumer = session.createConsumer(queue);
      consumer.receiveImmediate();
      consumer.close();
      ClientConsumer consumer2 = null;

      try {
         session2.createConsumer(queue);
         Assert.fail("Consuming here should fail due to the original security data.");
      } catch (ActiveMQException e) {
         // ok
      }

      DirContext ctx = getContext();
      BasicAttributes basicAttributes = new BasicAttributes();
      basicAttributes.put("uniquemember", "uid=role2");
      ctx.modifyAttributes("cn=read,uid=queue1,ou=queues,ou=destinations,o=ActiveMQ,ou=system", DirContext.REPLACE_ATTRIBUTE, basicAttributes);
      ctx.close();

      consumer2 = session2.createConsumer(queue);
      consumer2.receiveImmediate();
      consumer2.close();

      try {
         session.createConsumer(queue);
         Assert.fail("Sending here should fail due to the modified security data.");
      } catch (ActiveMQException e) {
         // ok
      }

      cf.close();
   }

   @Test
   public void testNewConsumerPermission() throws Exception {
      server.getConfiguration().setSecurityInvalidationInterval(0);
      server.start();
      String queue = "queue2";
      server.createQueue(SimpleString.toSimpleString(queue), RoutingType.ANYCAST, SimpleString.toSimpleString(queue), null, false, false);
      ClientSessionFactory cf = locator.createSessionFactory();
      ClientSession session = cf.createSession("first", "secret", false, true, true, false, 0);
      ClientConsumer consumer;

      try {
         session.createConsumer(queue);
         Assert.fail("Consuming here should fail due to the original security data.");
      } catch (ActiveMQException e) {
         // ok
      }

      DirContext ctx = getContext();
      BasicAttributes basicAttributes = new BasicAttributes();
      basicAttributes.put("uniquemember", "uid=role1");
      Attribute objclass = new BasicAttribute("objectclass");
      objclass.add("top");
      objclass.add("groupOfUniqueNames");
      basicAttributes.put(objclass);
      ctx.bind("cn=read,uid=" + queue + ",ou=queues,ou=destinations,o=ActiveMQ,ou=system", null, basicAttributes);

      consumer = session.createConsumer(queue);
      consumer.receiveImmediate();

      ctx.unbind("cn=read,uid=" + queue + ",ou=queues,ou=destinations,o=ActiveMQ,ou=system");
      ctx.close();

      try {
         session.createConsumer(queue);
         Assert.fail("Consuming here should fail due to the modified security data.");
      } catch (ActiveMQException e) {
         // ok
      }

      cf.close();
   }

   @Test
   public void testNewProducerPermission() throws Exception {
      server.getConfiguration().setSecurityInvalidationInterval(0);
      server.start();
      String queue = "queue2";
      server.createQueue(SimpleString.toSimpleString(queue), RoutingType.ANYCAST, SimpleString.toSimpleString(queue), null, false, false);
      ClientSessionFactory cf = locator.createSessionFactory();
      ClientSession session = cf.createSession("first", "secret", false, true, true, false, 0);
      ClientProducer producer = session.createProducer(SimpleString.toSimpleString(queue));

      try {
         producer.send(session.createMessage(true));
         Assert.fail("Producing here should fail due to the original security data.");
      } catch (ActiveMQException e) {
         // ok
      }

      DirContext ctx = getContext();
      BasicAttributes basicAttributes = new BasicAttributes();
      basicAttributes.put("uniquemember", "uid=role1");
      Attribute objclass = new BasicAttribute("objectclass");
      objclass.add("top");
      objclass.add("groupOfUniqueNames");
      basicAttributes.put(objclass);
      ctx.bind("cn=write,uid=" + queue + ",ou=queues,ou=destinations,o=ActiveMQ,ou=system", null, basicAttributes);

      producer.send(session.createMessage(true));

      ctx.unbind("cn=write,uid=" + queue + ",ou=queues,ou=destinations,o=ActiveMQ,ou=system");
      ctx.close();

      try {
         producer.send(session.createMessage(true));
         Assert.fail("Producing here should fail due to the modified security data.");
      } catch (ActiveMQException e) {
         // ok
      }

      cf.close();
   }
}
