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
import java.util.Hashtable;
import java.util.Map;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.WildcardConfiguration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMAcceptorFactory;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMConnectorFactory;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.core.server.impl.LegacyLDAPSecuritySettingPlugin;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.core.annotations.ApplyLdifFiles;
import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.annotations.CreatePartition;
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
@CreateDS(name = "myDS",
   partitions = {
      @CreatePartition(name = "test", suffix = "dc=example,dc=com")
   })
@CreateLdapServer(transports = {@CreateTransport(protocol = "LDAP", port = 1024)})
@ApplyLdifFiles("AMQauth3.ldif")
public class LegacyLDAPSecuritySettingPluginListenerTest2 extends AbstractLdapTestUnit {

   static {
      String path = System.getProperty("java.security.auth.login.config");
      if (path == null) {
         URL resource = LegacyLDAPSecuritySettingPluginListenerTest2.class.getClassLoader().getResource("login.config");
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

   public LegacyLDAPSecuritySettingPluginListenerTest2() {
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
      map.put(LegacyLDAPSecuritySettingPlugin.DESTINATION_BASE, "ou=destinations,ou=ActiveMQ,dc=example,dc=com");
      map.put(LegacyLDAPSecuritySettingPlugin.MAP_ADMIN_TO_MANAGE, "true");
      legacyLDAPSecuritySettingPlugin.init(map);

      ActiveMQJAASSecurityManager securityManager = new ActiveMQJAASSecurityManager("LDAPLogin3");
      WildcardConfiguration wildcardConfiguration = new WildcardConfiguration();
      wildcardConfiguration.setAnyWords('$');
      Configuration configuration = new ConfigurationImpl().setSecurityEnabled(true).addAcceptorConfiguration(new TransportConfiguration(InVMAcceptorFactory.class.getCanonicalName())).setJournalDirectory(ActiveMQTestBase.getJournalDir(testDir, 0, false)).setBindingsDirectory(ActiveMQTestBase.getBindingsDir(testDir, 0, false)).setPagingDirectory(ActiveMQTestBase.getPageDir(testDir, 0, false)).setLargeMessagesDirectory(ActiveMQTestBase.getLargeMessagesDir(testDir, 0, false)).setPersistenceEnabled(false).addSecuritySettingPlugin(legacyLDAPSecuritySettingPlugin).setWildCardConfiguration(wildcardConfiguration);

      server = ActiveMQServers.newActiveMQServer(configuration, ManagementFactory.getPlatformMBeanServer(), securityManager, false);
   }

   @After
   public void tearDown() throws Exception {
      locator.close();
      server.stop();
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
   public void testNewUserAndRoleWithWildcard() throws Exception {
      server.getConfiguration().setSecurityInvalidationInterval(0);
      server.start();
      ClientSessionFactory cf = locator.createSessionFactory();

      // authz should succeed
      try {
         ClientSession session = cf.createSession("user1", "secret", false, true, true, false, 0);
         ClientProducer producer = session.createProducer("project1.test");
         producer.send(session.createMessage(true));
      } catch (ActiveMQException e) {
         e.printStackTrace();
         Assert.fail("Should NOT fail");
      }

      // authz should fail
      try {
         ClientSession session = cf.createSession("user1", "secret", false, true, true, false, 0);
         ClientProducer producer = session.createProducer("project2.test");
         producer.send(session.createMessage(true));
         Assert.fail("Sending message here should fail!");
      } catch (ActiveMQException e) {
         Assert.assertTrue(e.getMessage().contains("229032")); // authorization exception
      }

      { // add new user - user3
         DirContext ctx = getContext();
         BasicAttributes basicAttributes = new BasicAttributes();
         basicAttributes.put("userPassword", "secret");
         Attribute objclass = new BasicAttribute("objectclass");
         objclass.add("top");
         objclass.add("person");
         objclass.add("organizationalPerson");
         objclass.add("inetOrgPerson");
         basicAttributes.put(objclass);
         Attribute cn = new BasicAttribute("cn");
         cn.add("user3");
         basicAttributes.put(cn);
         Attribute sn = new BasicAttribute("sn");
         sn.add("user3");
         basicAttributes.put(sn);
         ctx.bind("uid=user3,ou=users,dc=example,dc=com", null, basicAttributes);
      }

      { // add new role - team3
         DirContext ctx = getContext();
         BasicAttributes basicAttributes = new BasicAttributes();
         basicAttributes.put("uniqueMember", "uid=user3,ou=users,dc=example,dc=com");
         Attribute objclass = new BasicAttribute("objectclass");
         objclass.add("top");
         objclass.add("groupOfUniqueNames");
         basicAttributes.put(objclass);
         ctx.bind("cn=team3,ou=roles,dc=example,dc=com", null, basicAttributes);
      }

      { // add new security match - project3.$
         DirContext ctx = getContext();
         BasicAttributes basicAttributes = new BasicAttributes();
         Attribute objclass = new BasicAttribute("objectclass");
         objclass.add("top");
         objclass.add("applicationProcess");
         basicAttributes.put(objclass);
         ctx.bind("cn=project3.$,ou=queues,ou=destinations,ou=ActiveMQ,dc=example,dc=com", null, basicAttributes);
      }

      { // add write permission for match
         DirContext ctx = getContext();
         BasicAttributes basicAttributes = new BasicAttributes();
         basicAttributes.put("uniquemember", "cn=team3,ou=roles,dc=example,dc=com");
         Attribute objclass = new BasicAttribute("objectclass");
         objclass.add("top");
         objclass.add("groupOfUniqueNames");
         basicAttributes.put(objclass);
         ctx.bind("cn=write,cn=project3.$,ou=queues,ou=destinations,ou=ActiveMQ,dc=example,dc=com", null, basicAttributes);
      }

      // authz should succeed
      try {
         ClientSession session = cf.createSession("user3", "secret", false, true, true, false, 0);
         ClientProducer producer = session.createProducer("project3.test");
         producer.send(session.createMessage(true));
      } catch (ActiveMQException e) {
         e.printStackTrace();
         Assert.fail("Should NOT fail");
      }

      // authz should fail
      try {
         ClientSession session = cf.createSession("user3", "secret", false, true, true, false, 0);
         ClientProducer producer = session.createProducer("project7.test");
         producer.send(session.createMessage(true));
         Assert.fail("Sending message here should fail!");
      } catch (ActiveMQException e) {
         Assert.assertTrue(e.getMessage().contains("229032")); // authorization exception
      }
   }
}
