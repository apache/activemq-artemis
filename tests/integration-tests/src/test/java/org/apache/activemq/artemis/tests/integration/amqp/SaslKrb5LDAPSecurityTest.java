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
package org.apache.activemq.artemis.tests.integration.amqp;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.NameClassPair;
import javax.naming.NamingEnumeration;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.JavaVersionUtil;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.directory.api.ldap.model.constants.SupportedSaslMechanisms;
import org.apache.directory.api.ldap.model.entry.DefaultEntry;
import org.apache.directory.api.ldap.model.entry.Entry;
import org.apache.directory.api.ldap.model.filter.PresenceNode;
import org.apache.directory.api.ldap.model.ldif.LdifEntry;
import org.apache.directory.api.ldap.model.ldif.LdifReader;
import org.apache.directory.api.ldap.model.ldif.LdifUtils;
import org.apache.directory.api.ldap.model.message.AliasDerefMode;
import org.apache.directory.api.ldap.model.message.SearchScope;
import org.apache.directory.api.ldap.model.name.Dn;
import org.apache.directory.server.annotations.CreateKdcServer;
import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.annotations.SaslMechanism;
import org.apache.directory.server.core.annotations.ApplyLdifFiles;
import org.apache.directory.server.core.annotations.ContextEntry;
import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.annotations.CreateIndex;
import org.apache.directory.server.core.annotations.CreatePartition;
import org.apache.directory.server.core.api.filtering.EntryFilteringCursor;
import org.apache.directory.server.core.integ.AbstractLdapTestUnit;
import org.apache.directory.server.core.integ.FrameworkRunner;
import org.apache.directory.server.core.kerberos.KeyDerivationInterceptor;
import org.apache.directory.server.kerberos.shared.crypto.encryption.KerberosKeyFactory;
import org.apache.directory.server.kerberos.shared.keytab.Keytab;
import org.apache.directory.server.kerberos.shared.keytab.KeytabEntry;
import org.apache.directory.server.ldap.handlers.sasl.gssapi.GssapiMechanismHandler;
import org.apache.directory.shared.kerberos.KerberosTime;
import org.apache.directory.shared.kerberos.codec.types.EncryptionType;
import org.apache.directory.shared.kerberos.components.EncryptionKey;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.activemq.artemis.tests.util.ActiveMQTestBase.NETTY_ACCEPTOR_FACTORY;

@RunWith(FrameworkRunner.class)
@CreateDS(name = "Example",
   partitions = {@CreatePartition(name = "example", suffix = "dc=example,dc=com",
      contextEntry = @ContextEntry(entryLdif = "dn: dc=example,dc=com\n" + "dc: example\n" + "objectClass: top\n" + "objectClass: domain\n\n"),
      indexes = {@CreateIndex(attribute = "objectClass"), @CreateIndex(attribute = "dc"), @CreateIndex(attribute = "ou")})},
      additionalInterceptors = { KeyDerivationInterceptor.class })

@CreateLdapServer(transports = {@CreateTransport(protocol = "LDAP", port = 1024)},
   saslHost = "localhost",
   saslPrincipal = "ldap/localhost@EXAMPLE.COM",
   saslMechanisms = {@SaslMechanism(name = SupportedSaslMechanisms.GSSAPI, implClass = GssapiMechanismHandler.class)})

@CreateKdcServer(transports = {@CreateTransport(protocol = "TCP", port = 0)})
@ApplyLdifFiles("SaslKrb5LDAPSecurityTest.ldif")
public class SaslKrb5LDAPSecurityTest extends AbstractLdapTestUnit {
   private static final org.jboss.logging.Logger log = org.jboss.logging.Logger.getLogger(SaslKrb5LDAPSecurityTest.class);

   protected static final Logger LOG = LoggerFactory.getLogger(SaslKrb5LDAPSecurityTest.class);
   public static final String QUEUE_NAME = "some_queue";

   static {
      String path = System.getProperty("java.security.auth.login.config");
      if (path == null) {
         URL resource = SaslKrb5LDAPSecurityTest.class.getClassLoader().getResource("login.config");
         if (resource != null) {
            path = resource.getFile();
            System.setProperty("java.security.auth.login.config", path);
         }
      }
   }

   ActiveMQServer server;

   public static final String TARGET_TMP = "./target/tmp";
   private static final String PRINCIPAL = "uid=admin,ou=system";
   private static final String CREDENTIALS = "secret";
   private final boolean debug = false;

   public SaslKrb5LDAPSecurityTest() {
      File parent = new File(TARGET_TMP);
      parent.mkdirs();
      temporaryFolder = new TemporaryFolder(parent);
   }

   @Rule
   public TemporaryFolder temporaryFolder;
   private String testDir;

   @BeforeClass
   public static void checkAssumptions() throws Exception {
      Assume.assumeTrue("Test only runs on JDK 8", JavaVersionUtil.isJava8());
   }

   @Before
   public void setUp() throws Exception {

      if (debug) {
         initLogging();
      }

      testDir = temporaryFolder.getRoot().getAbsolutePath();

      // hard coded match, default_keytab_name in minikdc-krb5.conf template
      File userKeyTab = new File("target/test.krb5.keytab");
      createPrincipal(userKeyTab, "client", "amqp/localhost", "ldap/localhost");

      if (debug) {
         dumpLdapContents();
      }

      rewriteKerb5Conf();
   }

   private void createArtemisServer(String securityConfigScope) {
      ActiveMQJAASSecurityManager securityManager = new ActiveMQJAASSecurityManager(securityConfigScope);
      HashMap<String, Object> params = new HashMap<>();
      params.put(TransportConstants.PORT_PROP_NAME, String.valueOf(5672));
      params.put(TransportConstants.PROTOCOLS_PROP_NAME, "AMQP");

      HashMap<String, Object> amqpParams = new HashMap<>();
      amqpParams.put("saslMechanisms", "GSSAPI");
      amqpParams.put("saslLoginConfigScope", "amqp-sasl-gssapi");

      Configuration configuration = new ConfigurationImpl().setSecurityEnabled(true).addAcceptorConfiguration(new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, params, "netty-amqp", amqpParams)).setJournalDirectory(ActiveMQTestBase.getJournalDir(testDir, 0, false)).setBindingsDirectory(ActiveMQTestBase.getBindingsDir(testDir, 0, false)).setPagingDirectory(ActiveMQTestBase.getPageDir(testDir, 0, false)).setLargeMessagesDirectory(ActiveMQTestBase.getLargeMessagesDir(testDir, 0, false));
      server = ActiveMQServers.newActiveMQServer(configuration, ManagementFactory.getPlatformMBeanServer(), securityManager, false);
   }

   private void rewriteKerb5Conf() throws Exception {
      StringBuilder sb = new StringBuilder();
      InputStream is2 = this.getClass().getClassLoader().getResourceAsStream("minikdc-krb5.conf");

      BufferedReader r = null;
      try {
         r = new BufferedReader(new InputStreamReader(is2, StandardCharsets.UTF_8));
         String line = r.readLine();

         while (line != null) {
            sb.append(line).append("{3}");
            line = r.readLine();
         }
      } finally {
         IOUtils.closeQuietly(r);
         IOUtils.closeQuietly(is2);
      }

      InetSocketAddress addr =
         (InetSocketAddress)kdcServer.getTransports()[0].getAcceptor().getLocalAddress();
      int port = addr.getPort();
      File krb5conf = new File(testDir, "krb5.conf").getAbsoluteFile();
      FileUtils.writeStringToFile(krb5conf, MessageFormat.format(sb.toString(), getRealm(), "localhost", Integer.toString(port), System.getProperty("line.separator")));
      System.setProperty("java.security.krb5.conf", krb5conf.getAbsolutePath());

      System.setProperty("sun.security.krb5.debug", "true");

      // refresh the config
      Class<?> classRef;
      if (System.getProperty("java.vendor").contains("IBM")) {
         classRef = Class.forName("com.ibm.security.krb5.internal.Config");
      } else {
         classRef = Class.forName("sun.security.krb5.Config");
      }
      Method refreshMethod = classRef.getMethod("refresh", new Class[0]);
      refreshMethod.invoke(classRef, new Object[0]);

      LOG.debug("krb5.conf to: {}", krb5conf.getAbsolutePath());
   }

   private void dumpLdapContents() throws Exception {
      EntryFilteringCursor cursor = getService().getAdminSession().search(new Dn("ou=system"), SearchScope.SUBTREE, new PresenceNode("ObjectClass"), AliasDerefMode.DEREF_ALWAYS);
      String st = "";

      while (cursor.next()) {
         Entry entry = cursor.get();
         String ss = LdifUtils.convertToLdif(entry);
         st += ss + "\n";
      }
      log.debug(st);

      cursor = getService().getAdminSession().search(new Dn("dc=example,dc=com"), SearchScope.SUBTREE, new PresenceNode("ObjectClass"), AliasDerefMode.DEREF_ALWAYS);
      st = "";

      while (cursor.next()) {
         Entry entry = cursor.get();
         String ss = LdifUtils.convertToLdif(entry);
         st += ss + "\n";
      }
      log.debug(st);
   }

   private void initLogging() {
      java.util.logging.Logger logger = java.util.logging.Logger.getLogger("javax.security.sasl");
      logger.setLevel(java.util.logging.Level.FINEST);
      logger.addHandler(new java.util.logging.ConsoleHandler());
      for (java.util.logging.Handler handler: logger.getHandlers()) {
         handler.setLevel(java.util.logging.Level.FINEST);
      }
   }

   public synchronized void createPrincipal(String principal, String password) throws Exception {
      String baseDn = getKdcServer().getSearchBaseDn();
      String content = "dn: uid=" + principal + "," + baseDn + "\n" + "objectClass: top\n" + "objectClass: person\n" + "objectClass: inetOrgPerson\n" + "objectClass: krb5principal\n"
         + "objectClass: krb5kdcentry\n" + "cn: " + principal + "\n" + "sn: " + principal + "\n"
         + "uid: " + principal + "\n" + "userPassword: " + password + "\n"
         // using businessCategory as a proxy for memberoOf attribute pending: https://issues.apache.org/jira/browse/DIRSERVER-1844
         + "businessCategory: " + "cn=admins,ou=system" + "\n"
         + "businessCategory: " + "cn=bees,ou=system" + "\n"
         + "krb5PrincipalName: " + principal + "@" + getRealm() + "\n"
         + "krb5KeyVersionNumber: 0";

      for (LdifEntry ldifEntry : new LdifReader(new StringReader(content))) {
         service.getAdminSession().add(new DefaultEntry(service.getSchemaManager(), ldifEntry.getEntry()));
      }
   }

   public void createPrincipal(File keytabFile, String... principals) throws Exception {
      String generatedPassword = "notSecret!";
      Keytab keytab = new Keytab();
      List<KeytabEntry> entries = new ArrayList<>();
      for (String principal : principals) {
         createPrincipal(principal, generatedPassword);
         principal = principal + "@" + getRealm();
         KerberosTime timestamp = new KerberosTime();
         for (Map.Entry<EncryptionType, EncryptionKey> entry : KerberosKeyFactory.getKerberosKeys(principal, generatedPassword).entrySet()) {
            EncryptionKey ekey = entry.getValue();
            byte keyVersion = (byte) ekey.getKeyVersion();
            entries.add(new KeytabEntry(principal, 1L, timestamp, keyVersion, ekey));
         }
      }
      keytab.setEntries(entries);
      keytab.write(keytabFile);
   }

   private String getRealm() {
      return getKdcServer().getConfig().getPrimaryRealm();
   }

   @After
   public void tearDown() throws Exception {
      if (server != null) {
         server.stop();
      }
   }

   @Test
   public void testRunning() throws Exception {
      Hashtable<String, String> env = new Hashtable<>();
      env.put(Context.PROVIDER_URL, "ldap://localhost:1024");
      env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
      env.put(Context.SECURITY_AUTHENTICATION, "simple");
      env.put(Context.SECURITY_PRINCIPAL, PRINCIPAL);
      env.put(Context.SECURITY_CREDENTIALS, CREDENTIALS);
      DirContext ctx = new InitialDirContext(env);

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

      ctx.close();
   }

   @Test
   public void testSaslGssapiLdapAuth() throws Exception {

      final Hashtable<String, String> env = new Hashtable<>();
      env.put(Context.PROVIDER_URL, "ldap://localhost:1024");
      env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
      env.put(Context.SECURITY_AUTHENTICATION, "GSSAPI");

      LoginContext loginContext = new LoginContext("broker-sasl-gssapi");
      loginContext.login();
      try {
         Subject.doAs(loginContext.getSubject(), (PrivilegedExceptionAction<Object>) () -> {

            HashSet<String> set = new HashSet<>();

            DirContext ctx = new InitialDirContext(env);
            NamingEnumeration<NameClassPair> list = ctx.list("ou=system");

            while (list.hasMore()) {
               NameClassPair ncp = list.next();
               set.add(ncp.getName());
            }

            Assert.assertTrue(set.contains("uid=first"));
            Assert.assertTrue(set.contains("cn=users"));
            Assert.assertTrue(set.contains("ou=configuration"));
            Assert.assertTrue(set.contains("prefNodeName=sysPrefRoot"));

            ctx.close();
            return null;

         });
      } catch (PrivilegedActionException e) {
         throw e.getException();
      }
   }

   @Test
   public void testJAASSecurityManagerAuthorizationPositive() throws Exception {
      dotestJAASSecurityManagerAuthorizationPositive("Krb5PlusLdap", "admins");
   }

   @Test
   public void testJAASSecurityManagerAuthorizationPositiveMemberOf() throws Exception {
      // using businessCategory as a proxy for memberoOf attribute pending: https://issues.apache.org/jira/browse/DIRSERVER-1844
      dotestJAASSecurityManagerAuthorizationPositive("Krb5PlusLdapMemberOf", "bees");
   }

   @Test
   public void testJAASSecurityManagerAuthorizationPositiveNoRoleName() throws Exception {
      dotestJAASSecurityManagerAuthorizationPositive("Krb5PlusLdapNoRoleName", "cn=admins,ou=system");
   }

   @Test
   public void testJAASSecurityManagerAuthorizationPositiveMemberOfNoRoleName() throws Exception {
      dotestJAASSecurityManagerAuthorizationPositive("Krb5PlusLdapMemberOfNoRoleName", "cn=bees,ou=system");
   }

   public void dotestJAASSecurityManagerAuthorizationPositive(String jaasConfigScope, String artemisRoleName) throws Exception {

      createArtemisServer(jaasConfigScope);

      Set<Role> roles = new HashSet<>();
      roles.add(new Role(artemisRoleName, true, true, true, true, true, true, true, true, true, true));
      server.getConfiguration().putSecurityRoles(QUEUE_NAME, roles);
      server.start();

      JmsConnectionFactory jmsConnectionFactory = new JmsConnectionFactory("amqp://localhost:5672?amqp.saslMechanisms=GSSAPI");
      Connection connection = jmsConnectionFactory.createConnection("client", null);

      try {
         connection.start();

         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         javax.jms.Queue queue = session.createQueue(QUEUE_NAME);

         // PRODUCE
         final String text = RandomUtil.randomString();

         try {
            MessageProducer producer = session.createProducer(queue);
            producer.send(session.createTextMessage(text));
         } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("should not throw exception here");
         }

         // CONSUME
         try {
            MessageConsumer consumer = session.createConsumer(queue);
            TextMessage m = (TextMessage) consumer.receive(1000);
            Assert.assertNotNull(m);
            Assert.assertEquals(text, m.getText());
         } catch (Exception e) {
            Assert.fail("should not throw exception here");
         }
      } finally {
         connection.close();
      }
   }
}
