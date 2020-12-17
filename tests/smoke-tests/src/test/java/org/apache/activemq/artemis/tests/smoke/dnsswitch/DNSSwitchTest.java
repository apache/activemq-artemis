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

package org.apache.activemq.artemis.tests.smoke.dnsswitch;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.MBeanServerInvocationHandler;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.management.ActiveMQServerControl;
import org.apache.activemq.artemis.api.core.management.ObjectNameBuilder;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.tests.smoke.common.SmokeTestBase;
import org.apache.activemq.artemis.util.ServerUtil;
import org.apache.activemq.artemis.utils.SpawnedVMSupport;
import org.apache.activemq.artemis.utils.Wait;
import org.apache.activemq.artemis.utils.network.NetUtil;
import org.apache.activemq.artemis.utils.network.NetUtilResource;
import org.jboss.logging.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

/**
 * Validating connection retry scenarios where the DNS had changes
 */
public class DNSSwitchTest extends SmokeTestBase {

   private static boolean USING_SPAWN = true;
   public static final File ETC_HOSTS = new File("/etc/hosts");

   private static File ETC_BACKUP;

   private static final String JMX_SERVER_HOSTNAME = "localhost";
   private static final int JMX_SERVER_PORT_0 = 10099;
   private static final int JMX_SERVER_PORT_1 = 10199;

   static String liveURI = "service:jmx:rmi:///jndi/rmi://" + JMX_SERVER_HOSTNAME + ":" + JMX_SERVER_PORT_0 + "/jmxrmi";
   static String backupURI = "service:jmx:rmi:///jndi/rmi://" + JMX_SERVER_HOSTNAME + ":" + JMX_SERVER_PORT_1 + "/jmxrmi";

   static ObjectNameBuilder liveNameBuilder = ObjectNameBuilder.create(ActiveMQDefaultConfiguration.getDefaultJmxDomain(), "live", true);
   static ObjectNameBuilder backupNameBuilder = ObjectNameBuilder.create(ActiveMQDefaultConfiguration.getDefaultJmxDomain(), "backup", true);

   // This is a more intrusive option to use with JDK 8
   // Instead of using a separate jdk hsots, which is not supported on jdk8,
   // with this option set to true we would use the original /etc/hosts
   private static boolean USE_ETC_HOSTS = System.getProperty("java.version").startsWith("1.8");

   private static final Logger logger = Logger.getLogger(DNSSwitchTest.class);

   private static final String SERVER_NAME_0 = "dnsswitch";
   private static final String SERVER_NAME_1 = "dnsswitch2";
   private static final String SERVER_STANDARD = "standard";
   private static final String SERVER_LIVE = "dnsswitch-replicated-main";
   private static final String SERVER_LIVE_NORETRYDNS = "dnsswitch-replicated-main-noretrydns";
   private static final String SERVER_BACKUP = "dnsswitch-replicated-backup";

   private static final String SERVER_LIVE_PING = "dnsswitch-replicated-main-withping";
   private static final String SERVER_BACKUP_PING = "dnsswitch-replicated-backup-withping";

   // 192.0.2.0 is reserved for documentation (and testing on this case).
   private static final String FIRST_IP = "192.0.2.0";
   private static final String SECOND_IP = "192.0.3.0";
   private static final String THIRD_IP = "192.0.3.0";
   private static final String FOURTH_IP = "192.0.4.0";

   private static final String INVALID_IP = "203.0.113.0";

   private static String serverLocation;

   @Rule
   public NetUtilResource netUtilResource = new NetUtilResource();

   private static JMXConnector newJMXFactory(String uri) throws Throwable {
      return JMXConnectorFactory.connect(new JMXServiceURL(uri));
   }

   private static ActiveMQServerControl getServerControl(String uri,
                                                         ObjectNameBuilder builder,
                                                         long timeout) throws Throwable {
      long expireLoop = System.currentTimeMillis() + timeout;
      Throwable lastException = null;
      do {
         try {
            JMXConnector connector = newJMXFactory(uri);

            ActiveMQServerControl serverControl = MBeanServerInvocationHandler.newProxyInstance(connector.getMBeanServerConnection(), builder.getActiveMQServerObjectName(), ActiveMQServerControl.class, false);
            serverControl.isActive(); // making one call to make sure it's working
            return serverControl;
         } catch (Throwable e) {
            lastException = e;
            Thread.sleep(500);
         }
      }
      while (expireLoop > System.currentTimeMillis());

      throw lastException;
   }

   @BeforeClass
   public static void beforeClassMethod() throws Exception {
      if (USE_ETC_HOSTS) {
         if (!ETC_HOSTS.canWrite()) {
            System.out.println("If you want to run this test, you must do 'sudo chmod 666 " + ETC_HOSTS);
         }
         Assume.assumeTrue("If you want to run this test, you must do 'sudo chmod 666 " + ETC_HOSTS + "'", ETC_HOSTS.canWrite());
      }
      serverLocation = getServerLocation(SERVER_NAME_0);
      // Before anything we must copy the jave security and change what we need for no cache
      // this will be used to spawn new tests
      generateNoCacheSecurity(serverLocation);
      generateNoRetrySecurity(serverLocation);
      if (USE_ETC_HOSTS) {
         Assert.assertTrue("If you want to run this test, you must do 'sudo chmod 666 " + ETC_HOSTS + "'", ETC_HOSTS.canWrite());
         File tmpDirectory = new File(System.getProperty("java.io.tmpdir"));
         ETC_BACKUP = new File(tmpDirectory, "etcHostsBackup");

         if (!ETC_BACKUP.exists()) {
            Files.copy(ETC_HOSTS.toPath(), ETC_BACKUP.toPath(), StandardCopyOption.COPY_ATTRIBUTES);
         }
      }
      NetUtil.failIfNotSudo();
   }

   private static File getETCBackup() {

      if (ETC_BACKUP == null) {
         File tmpDirectory = new File(System.getProperty("java.io.tmpdir"));
         ETC_BACKUP = new File(tmpDirectory, "etcHostsBackup");
      }

      Assert.assertTrue(ETC_BACKUP.exists());

      return ETC_BACKUP;
   }

   @AfterClass
   public static void afterClassMethod() throws Exception {

      if (USE_ETC_HOSTS && ETC_BACKUP != null) {
         Assert.assertTrue(ETC_BACKUP.exists());
         try {
            recoverETCHosts();
         } finally {
            ETC_BACKUP.delete();
            ETC_BACKUP = null;
         }

      }

   }

   private static void recoverETCHosts() throws IOException {
      // it seems silly to use a FileInputStream / FileOutputStream to copy
      // a file these days, but on this case we have authorization to write on the file
      // but not to replace the file in any way.
      // So, Files.copy is not acceptable.
      // I could use a library that was doing the same here
      // but I didn't bother about it and simply went to the simplest way possible
      FileInputStream inputStream = new FileInputStream(getETCBackup());
      FileOutputStream outputStream = new FileOutputStream(ETC_HOSTS);
      byte[] buffer = new byte[4 * 1024];
      int bytes;
      try {
         while ((bytes = inputStream.read(buffer)) > 0) {
            outputStream.write(buffer, 0, bytes);
         }
      } finally {
         inputStream.close();
         outputStream.close();
      }
   }

   private static void generateNoCacheSecurity(String serverLocation) throws Exception {
      File outputSecurity = new File(serverLocation + File.separator + "etc" + File.separator + "zerocache.security");
      generateSecurity(outputSecurity, "networkaddress.cache.ttl", "0", "networkaddress.cache.negative.ttl", "0");
   }

   private static void generateNoRetrySecurity(String serverLocation) throws Exception {
      File outputSecurity = new File(serverLocation + File.separator + "etc" + File.separator + "noretrydns.security");
      generateSecurity(outputSecurity, "networkaddress.cache.ttl", "-1", "networkaddress.cache.negative.ttl", "-1");
   }

   private static void generateSecurity(File outputSecurity, String... overrideParameters) throws IOException {

      Assert.assertTrue("You must send pairs as overrideParameters", overrideParameters.length % 2 == 0);


      String javaVersion = System.getProperty("java.version");

      File security;

      if (javaVersion.startsWith("1.8")) {
         security = new File(System.getProperty("java.home") + File.separator + "lib" + File.separator + "security" + File.separator + "java.security");
      } else {
         security = new File(System.getProperty("java.home") + File.separator + "conf" + File.separator + "security" + File.separator + "java.security");
      }

      Properties securityProperties = new Properties();
      securityProperties.load(new FileInputStream(security));

      for (int i = 0; i < overrideParameters.length; i += 2) {
         securityProperties.setProperty(overrideParameters[i], overrideParameters[i + 1]);
      }

      securityProperties.store(new FileOutputStream(outputSecurity), "# generated by DNSSwitchTest");
   }

   private static final String hostsFile = System.getProperty("jdk.net.hosts.file");

   @Before
   public void before() throws Exception {
      cleanupData(SERVER_NAME_0);
      cleanupData(SERVER_NAME_1);
      cleanupData(SERVER_STANDARD);
      cleanupData(SERVER_LIVE);
      cleanupData(SERVER_LIVE_NORETRYDNS);
      cleanupData(SERVER_BACKUP);
      cleanupData(SERVER_LIVE_PING);
      cleanupData(SERVER_BACKUP_PING);
   }

   @Test
   public void testBackupRedefinition() throws Throwable {
      spawnRun(serverLocation, "testBackupRedefinition", getServerLocation(SERVER_LIVE), getServerLocation(SERVER_BACKUP));
   }

   public static void testBackupRedefinition(String[] args) throws Throwable {
      NetUtil.netUp(FIRST_IP, "lo:first");
      NetUtil.netUp(SECOND_IP, "lo:second");
      saveConf(hostsFile, FIRST_IP, "FIRST", SECOND_IP, "SECOND");

      Process serverLive = null;
      Process serverBackup = null;

      try {
         serverLive = ServerUtil.startServer(args[1], "live", "tcp://FIRST:61616", 30_000);
         serverBackup = ServerUtil.startServer(args[2], "backup", "tcp://SECOND:61716", 0);

         connectAndWaitBackup();
         saveConf(hostsFile, FIRST_IP, "FIRST", THIRD_IP, "SECOND");

         NetUtil.netDown(SECOND_IP, "lo:second", true);
         serverBackup.destroyForcibly();

         Thread.sleep(1000); // wait some time at least until a reconnection is in place

         saveConf(hostsFile, FIRST_IP, "FIRST", THIRD_IP, "SECOND");

         // waitForTopology(connectionFactory.getServerLocator().getTopology(), 60_000, 1, 0);

         serverBackup = ServerUtil.startServer(args[2], "backup", "tcp://SECOND:61716", 0);

         ActiveMQServerControl backupControl = getServerControl(backupURI, backupNameBuilder, 20_000);
         Wait.assertTrue(backupControl::isStarted);
         Wait.assertTrue(backupControl::isReplicaSync);

         connectAndWaitBackup();

         ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://FIRST:61616?ha=true");
         Assert.assertTrue(connectionFactory.getServerLocator().isHA());
         Connection connection = connectionFactory.createConnection();
         waitForTopology(connectionFactory.getServerLocator().getTopology(), 60_000, 1, 1);

         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

         Queue queue = session.createQueue("test");

         MessageProducer producer = session.createProducer(queue);

         producer.send(session.createTextMessage("hello"));
         session.commit();

         NetUtil.netUp(THIRD_IP, "lo:third");
         serverLive.destroyForcibly();

         Wait.assertTrue(backupControl::isActive);

         MessageConsumer consumer = null;
         int errors = 0;
         while (true) {
            try {
               consumer = session.createConsumer(queue);
               connection.start();
               TextMessage message = (TextMessage) consumer.receive(5000);
               Assert.assertNotNull(message);
               Assert.assertEquals("hello", message.getText());
               session.commit();
               break;
            } catch (Exception e) {
               e.printStackTrace();
               errors++;
               Assert.assertTrue(errors < 20); // I would accept one or two errors, but the code must connect itself
               connection.close();
               connectionFactory = new ActiveMQConnectionFactory("tcp://SECOND:61716?ha=true");
               connection = connectionFactory.createConnection();
               connection.start();
               session = connection.createSession(true, Session.SESSION_TRANSACTED);
            }
         }

      } finally {
         if (serverLive != null) {
            serverLive.destroyForcibly();
         }
         if (serverBackup != null) {
            serverBackup.destroyForcibly();
         }

      }

   }


   @Test
   public void testBackupRedefinition2() throws Throwable {
      spawnRun(serverLocation, "testBackupRedefinition2", getServerLocation(SERVER_LIVE), getServerLocation(SERVER_BACKUP));
   }

   public static void testBackupRedefinition2(String[] args) throws Throwable {
      NetUtil.netUp(FIRST_IP, "lo:first");
      NetUtil.netUp(SECOND_IP, "lo:second");
      NetUtil.netUp(THIRD_IP, "lo:third");
      saveConf(hostsFile, FIRST_IP, "FIRST", SECOND_IP, "SECOND");

      Process serverLive = null;
      Process serverBackup = null;

      try {
         serverLive = ServerUtil.startServer(args[1], "live", "tcp://FIRST:61616", 30_000);
         serverBackup = ServerUtil.startServer(args[2], "backup", "tcp://SECOND:61716", 0);

         connectAndWaitBackup();
         saveConf(hostsFile, FIRST_IP, "FIRST", THIRD_IP, "SECOND");

         NetUtil.netDown(SECOND_IP, "lo:second", true);
         serverBackup.destroyForcibly();

         Thread.sleep(1000); // wait some time at least until a reconnection is in place

         saveConf(hostsFile, FIRST_IP, "FIRST", THIRD_IP, "SECOND");

         // waitForTopology(connectionFactory.getServerLocator().getTopology(), 60_000, 1, 0);

         serverBackup = ServerUtil.startServer(args[2], "backup", "tcp://SECOND:61716", 0);

         ActiveMQServerControl backupControl = getServerControl(backupURI, backupNameBuilder, 20_000);
         Wait.assertTrue(backupControl::isStarted);
         Wait.assertTrue(backupControl::isReplicaSync);


         saveConf(hostsFile, FIRST_IP, "FIRST", SECOND_IP, "SECOND");
         serverBackup.destroyForcibly();
         serverBackup = ServerUtil.startServer(args[2], "backup", "tcp://SECOND:61716", 0);
         connectAndWaitBackup();

         backupControl = getServerControl(backupURI, backupNameBuilder, 20_000);
         Wait.assertTrue(backupControl::isStarted);
         Wait.assertTrue(backupControl::isReplicaSync);

         ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://FIRST:61616?ha=true");
         Assert.assertTrue(connectionFactory.getServerLocator().isHA());
         Connection connection = connectionFactory.createConnection();
         waitForTopology(connectionFactory.getServerLocator().getTopology(), 60_000, 1, 1);

         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

         Queue queue = session.createQueue("test");

         MessageProducer producer = session.createProducer(queue);

         producer.send(session.createTextMessage("hello"));
         session.commit();

         NetUtil.netUp(THIRD_IP, "lo:third");
         serverLive.destroyForcibly();

         Wait.assertTrue(backupControl::isActive);

         MessageConsumer consumer = null;
         int errors = 0;
         while (true) {
            try {
               consumer = session.createConsumer(queue);
               connection.start();
               TextMessage message = (TextMessage) consumer.receive(5000);
               Assert.assertNotNull(message);
               Assert.assertEquals("hello", message.getText());
               session.commit();
               break;
            } catch (Exception e) {
               e.printStackTrace();
               errors++;
               Assert.assertTrue(errors < 20); // I would accept one or two errors, but the code must connect itself
               connection.close();
               connectionFactory = new ActiveMQConnectionFactory("tcp://SECOND:61716?ha=true");
               connection = connectionFactory.createConnection();
               connection.start();
               session = connection.createSession(true, Session.SESSION_TRANSACTED);
            }
         }

      } finally {
         if (serverBackup != null) {
            serverBackup.destroyForcibly();
         }
         if (serverLive != null) {
            serverLive.destroyForcibly();
         }
      }

   }


   @Test
   public void testBackupRedefinition3() throws Throwable {
      spawnRun(serverLocation, "testBackupRedefinition2", getServerLocation(SERVER_LIVE), getServerLocation(SERVER_BACKUP));
   }

   public static void testBackupRedefinition3(String[] args) throws Throwable {
      NetUtil.netUp(FIRST_IP, "lo:first");
      NetUtil.netUp(SECOND_IP, "lo:second");
      NetUtil.netUp(THIRD_IP, "lo:third");
      saveConf(hostsFile, FIRST_IP, "FIRST", SECOND_IP, "SECOND");

      Process serverLive = null;
      Process serverBackup = null;

      try {
         serverLive = ServerUtil.startServer(args[1], "live", "tcp://FIRST:61616", 30_000);
         serverBackup = ServerUtil.startServer(args[2], "backup", "tcp://SECOND:61716", 0);

         connectAndWaitBackup();
         saveConf(hostsFile, FIRST_IP, "FIRST", THIRD_IP, "SECOND");

         NetUtil.netDown(SECOND_IP, "lo:second", true);
         serverBackup.destroyForcibly();

         Thread.sleep(1000); // wait some time at least until a reconnection is in place

         saveConf(hostsFile, FIRST_IP, "FIRST", THIRD_IP, "SECOND");

         // waitForTopology(connectionFactory.getServerLocator().getTopology(), 60_000, 1, 0);

         serverBackup = ServerUtil.startServer(args[2], "backup", "tcp://SECOND:61716", 0);

         ActiveMQServerControl backupControl = getServerControl(backupURI, backupNameBuilder, 20_000);
         Wait.assertTrue(backupControl::isStarted);
         Wait.assertTrue(backupControl::isReplicaSync);


         saveConf(hostsFile, FIRST_IP, "FIRST", SECOND_IP, "SECOND");
         serverBackup.destroyForcibly();
         serverBackup = ServerUtil.startServer(args[2], "backup", "tcp://SECOND:61716", 0);
         connectAndWaitBackup();

         backupControl = getServerControl(backupURI, backupNameBuilder, 20_000);
         Wait.assertTrue(backupControl::isStarted);
         Wait.assertTrue(backupControl::isReplicaSync);

         ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://FIRST:61616?ha=true");
         Assert.assertTrue(connectionFactory.getServerLocator().isHA());
         Connection connection = connectionFactory.createConnection();
         waitForTopology(connectionFactory.getServerLocator().getTopology(), 60_000, 1, 1);

         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

         Queue queue = session.createQueue("test");

         MessageProducer producer = session.createProducer(queue);

         producer.send(session.createTextMessage("hello"));
         session.commit();

         NetUtil.netUp(THIRD_IP, "lo:third");
         serverLive.destroyForcibly();

         Wait.assertTrue(backupControl::isActive);

         MessageConsumer consumer = null;
         int errors = 0;
         while (true) {
            try {
               consumer = session.createConsumer(queue);
               connection.start();
               TextMessage message = (TextMessage) consumer.receive(5000);
               Assert.assertNotNull(message);
               Assert.assertEquals("hello", message.getText());
               session.commit();
               break;
            } catch (Exception e) {
               e.printStackTrace();
               errors++;
               Assert.assertTrue(errors < 20); // I would accept one or two errors, but the code must connect itself
               connection.close();
               connectionFactory = new ActiveMQConnectionFactory("tcp://SECOND:61616?ha=true");
               connection = connectionFactory.createConnection();
               connection.start();
               session = connection.createSession(true, Session.SESSION_TRANSACTED);
            }
         }

      } finally {
         if (serverBackup != null) {
            serverBackup.destroyForcibly();
         }
         if (serverLive != null) {
            serverLive.destroyForcibly();
         }


      }

   }


   @Test
   public void testCantReachBack() throws Throwable {
      spawnRun(serverLocation, "testCantReachBack", getServerLocation(SERVER_LIVE_NORETRYDNS), getServerLocation(SERVER_BACKUP));
   }

   public static void testCantReachBack(String[] args) throws Throwable {
      NetUtil.netUp(FIRST_IP, "lo:first");
      NetUtil.netUp(SECOND_IP, "lo:second");

      // notice there's no THIRD_IP anywhere
      saveConf(hostsFile, FIRST_IP, "FIRST", THIRD_IP, "SECOND");

      Process serverLive = null;
      Process serverBackup = null;

      try {
         serverLive = ServerUtil.startServer(args[1], "live", "tcp://FIRST:61616", 30_000);
         ActiveMQServerControl liveControl = getServerControl(liveURI, liveNameBuilder, 20_000);

         Wait.assertTrue(liveControl::isStarted);

         // notice the first server does not know about this server at all
         saveConf(hostsFile, FIRST_IP, "FIRST", SECOND_IP, "SECOND");
         serverBackup = ServerUtil.startServer(args[2], "backup", "tcp://SECOND:61716", 0);
         ActiveMQServerControl backupControl = getServerControl(backupURI, backupNameBuilder, 20_000);

         Wait.assertTrue(backupControl::isStarted);
         Wait.assertTrue(backupControl::isReplicaSync);

         connectAndWaitBackup();

      } finally {
         if (serverBackup != null) {
            serverBackup.destroyForcibly();
         }
         if (serverLive != null) {
            serverLive.destroyForcibly();
         }
      }

   }


   @Test
   public void testWithPing() throws Throwable {
      spawnRun(serverLocation, "testWithPing", getServerLocation(SERVER_LIVE_PING), getServerLocation(SERVER_BACKUP_PING));
   }

   public static void testWithPing(String[] args) throws Throwable {
      NetUtil.netUp(FIRST_IP, "lo:first");
      NetUtil.netUp(SECOND_IP, "lo:second");
      NetUtil.netUp(THIRD_IP, "lo:third");

      // notice there's no THIRD_IP anywhere
      saveConf(hostsFile, FIRST_IP, "FIRST", SECOND_IP, "SECOND", THIRD_IP, "PINGPLACE");

      Process serverLive = null;
      Process serverBackup = null;

      try {
         serverLive = ServerUtil.startServer(args[1], "live", "tcp://FIRST:61616", 30_000);
         ActiveMQServerControl liveControl = getServerControl(liveURI, liveNameBuilder, 20_000);

         Wait.assertTrue(liveControl::isStarted);

         // notice the first server does not know about this server at all
         serverBackup = ServerUtil.startServer(args[2], "backup", "tcp://SECOND:61716", 0);
         ActiveMQServerControl backupControl = getServerControl(backupURI, backupNameBuilder, 20_000);

         Wait.assertTrue(backupControl::isStarted);
         Wait.assertTrue(backupControl::isReplicaSync);
         // Removing PINGPLACE from DNS
         saveConf(hostsFile, FIRST_IP, "FIRST", SECOND_IP, "SECOND", INVALID_IP, "PINGPLACE");

         Wait.assertFalse(liveControl::isStarted);

         serverBackup.destroyForcibly();


         //Thread.sleep(10_000);
         serverLive.destroyForcibly();
         serverLive = ServerUtil.startServer(args[1], "live", "tcp://FIRST:61616", 0);

         Thread.sleep(1_000);


         logger.debug("going to re-enable ping");
         // Enable the address just for ping now
         saveConf(hostsFile, THIRD_IP, "PINGPLACE");
         liveControl = getServerControl(liveURI, liveNameBuilder, 20_000);
         Wait.assertTrue(liveControl::isStarted);

         // Waiting some time as to the retry logic to kick in
         Thread.sleep(5_000);

         // the backup will know about the live, but live doesn't have a direct DNS to backup.. lets see what happens
         saveConf(hostsFile, FIRST_IP, "FIRST", THIRD_IP, "PINGPLACE");

         boolean ok = false;
         for (int i = 0; i < 5; i++) {
            serverBackup = ServerUtil.startServer(args[2], "backup", "tcp://SECOND:61716", 0);
            backupControl = getServerControl(backupURI, backupNameBuilder, 20_000);
            Wait.assertTrue(backupControl::isStarted);
            if (!Wait.waitFor(backupControl::isReplicaSync, 5000, 100)) {
               serverBackup.destroyForcibly();
            } else {
               ok = true;
               break;
            }
         }

         Assert.assertTrue(ok);

      } finally {
         if (serverBackup != null) {
            serverBackup.destroyForcibly();
         }
         if (serverLive != null) {
            serverLive.destroyForcibly();
         }


      }

   }

   @Test
   public void testWithoutPingKill() throws Throwable {
      spawnRun(serverLocation, "testWithoutPing", getServerLocation(SERVER_LIVE), getServerLocation(SERVER_BACKUP), "1");
   }

   @Test
   public void testWithoutPingRestart() throws Throwable {
      spawnRun(serverLocation, "testWithoutPing", getServerLocation(SERVER_LIVE), getServerLocation(SERVER_BACKUP), "0");
   }
   /**
    * arg[0] = constant "testWithoutPing" to be used on reflection through main(String arg[])
    * arg[1] = serverlive
    * arg[2] = server backup
    * arg[3] = 1 | 0 (kill the backup = 1, stop the backup = 0);
    * @param args
    * @throws Throwable
    */
   public static void testWithoutPing(String[] args) throws Throwable {
      boolean killTheBackup = Integer.parseInt(args[3]) == 1;
      NetUtil.netUp(FIRST_IP, "lo:first");
      NetUtil.netUp(SECOND_IP, "lo:second");

      // notice there's no THIRD_IP anywhere
      saveConf(hostsFile, FIRST_IP, "FIRST", SECOND_IP, "SECOND");

      Process serverLive = null;
      Process serverBackup = null;

      try {
         serverLive = ServerUtil.startServer(args[1], "live", "tcp://FIRST:61616", 0);
         ActiveMQServerControl liveControl = getServerControl(liveURI, liveNameBuilder, 20_000);

         Wait.assertTrue(liveControl::isStarted);

         // notice the first server does not know about this server at all
         serverBackup = ServerUtil.startServer(args[2], "backup", "tcp://SECOND:61716", 0);
         ActiveMQServerControl backupControl = getServerControl(backupURI, backupNameBuilder, 20_000);

         Wait.assertTrue(backupControl::isStarted);
         Wait.assertTrue(backupControl::isReplicaSync);

         logger.debug("shutdown the Network now");

         // this will remove all the DNS information
         // I need the pingers to stop responding.
         // That will only happen if I stop both devices on Linux.
         // On mac that works regardless
         NetUtil.netDown(FIRST_IP, "lo:first", false);
         NetUtil.netDown(SECOND_IP, "lo:second", false);
         saveConf(hostsFile);

         Wait.assertTrue(backupControl::isActive);

         logger.debug("Starting the network");

         NetUtil.netUp(FIRST_IP, "lo:first");
         NetUtil.netUp(SECOND_IP, "lo:second");
         saveConf(hostsFile, FIRST_IP, "FIRST", SECOND_IP, "SECOND");

         // I must wait some time for the backup to have a chance to retry here
         Thread.sleep(2000);

         logger.debug("Going down now");

         System.out.println("*******************************************************************************************************************************");
         System.out.println("Forcing backup down and restarting it");
         System.out.println("*******************************************************************************************************************************");

         if (killTheBackup) {
            serverBackup.destroyForcibly();
         } else {
            String serverLocation = args[2];
            stopServerWithFile(serverLocation);
            Assert.assertTrue(serverBackup.waitFor(10, TimeUnit.SECONDS));
         }

         cleanupData(SERVER_BACKUP);

         serverBackup = ServerUtil.startServer(args[2], "backup", "tcp://SECOND:61716", 0);
         backupControl = getServerControl(backupURI, backupNameBuilder, 20_000);
         Wait.assertTrue(backupControl::isStarted);
         Wait.assertTrue(backupControl::isReplicaSync);
      } finally {
         if (serverBackup != null) {
            serverBackup.destroyForcibly();
         }
         if (serverLive != null) {
            serverLive.destroyForcibly();
         }


      }

   }


   private static void connectAndWaitBackup() throws Exception {
      ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://FIRST:61616?ha=true");
      Assert.assertTrue(connectionFactory.getServerLocator().isHA());
      Connection connection = connectionFactory.createConnection();
      waitForTopology(connectionFactory.getServerLocator().getTopology(), 60_000, 1, 1);
      connection.close();
   }

   @Test
   public void testFailoverDifferentIPRedefinition() throws Throwable {

      spawnRun(serverLocation, "testFailoverDifferentIPRedefinition", serverLocation, getServerLocation(SERVER_NAME_1));
   }

   public static void testFailoverDifferentIPRedefinition(String[] arg) throws Throwable {
      NetUtil.netUp(FIRST_IP);
      NetUtil.netUp(SECOND_IP);

      saveConf(hostsFile, FIRST_IP, "test");
      Process server = null;
      Process server2 = null;
      try {
         server = ServerUtil.startServer(arg[1], "original-server", "tcp://test:61616", 5000);

         ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://test:61616?initialConnectAttempts=500&retryInterval=100&connect-timeout-millis=100&reconnectAttempts=500&connectionTTL=1000");

         Connection connection = factory.createConnection();

         server.destroyForcibly();
         NetUtil.netDown(FIRST_IP, true);

         CountDownLatch latchConnect = new CountDownLatch(1);
         AtomicInteger errors = new AtomicInteger(0);

         Thread connecting = new Thread(() -> {
            try {
               latchConnect.countDown();
               Connection connection2 = factory.createConnection();
               connection2.close();
            } catch (Exception e) {
               e.printStackTrace();
               errors.incrementAndGet();
            }
         });

         connecting.start();

         Assert.assertTrue(latchConnect.await(5, TimeUnit.SECONDS));

         Thread.sleep(500);

         server = ServerUtil.startServer(arg[2], "new-server", "tcp://" + SECOND_IP + ":61616", 5000);

         saveConf(hostsFile, SECOND_IP, "test");
         connecting.join(5000);
         Assert.assertFalse(connecting.isAlive());
         Assert.assertEquals(0, errors.get());
      } finally {
         if (server != null)
            server.destroyForcibly();
         if (server2 != null)
            server2.destroyForcibly();
      }

   }

   @Test
   public void testInitialConnector() throws Throwable {
      ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://test:61616?initialConnectAttempts=500&retryInterval=100&connect-timeout-millis=100");
      startServer(SERVER_STANDARD, 0, 30000);

      String location = getServerLocation(SERVER_NAME_0);

      spawnRun(location, "testInitialConnector");
      // If you eed to debug the test, comment out spawnRun, and call the method directly
      // you will need to add roperties on the JDK for that
      // Add the properties you need
      // testInitialConnector("testInitialConnector", location);
   }

   // called with reflection
   public static void testInitialConnector(String... arg) throws Throwable {
      saveConf(hostsFile, "192.0.0.3", "test");
      validateIP("test", "192.0.0.3");

      AtomicInteger errors = new AtomicInteger(0);

      CountDownLatch initialConnectTried = new CountDownLatch(1);

      ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://test:61616?initialConnectAttempts=500&retryInterval=100&connect-timeout-millis=100");
      Thread connecting = new Thread(() -> {
         try {
            initialConnectTried.countDown();
            Connection connection = factory.createConnection();
            connection.close();
         } catch (Exception e) {
            e.printStackTrace();
            errors.incrementAndGet();
         }
      });

      connecting.start();

      Assert.assertTrue(initialConnectTried.await(10, TimeUnit.SECONDS));

      Thread.sleep(1000);

      saveConf(hostsFile, "127.0.0.1", "test");
      validateIP("test", "127.0.0.1");
      connecting.join(10_000);

      Connection connection = factory.createConnection();
      connection.close();

      Assert.assertEquals(0, errors.get());

      Assert.assertFalse(connecting.isAlive());
   }

   // This test is just validating the DNS is not being cached on the separte VM
   @Test
   public void testSimpleResolution() throws Throwable {
      spawnRun(serverLocation, "testSimpleResolution");
   }

   // called with reflection
   public static void testSimpleResolution(String[] arg) throws Throwable {
      // This is just to validate the DNS hosts is picking up the right host
      saveConf(hostsFile, "192.0.0.1", "test");
      validateIP("test", "192.0.0.1");

      // and it should not cache anything if the right properties are in place
      saveConf(hostsFile, "192.0.0.3", "test");
      validateIP("test", "192.0.0.3");
   }

   /**
    * it will continue the test on a spwned VM with the properties we need for this test
    */
   private void spawnRun(String location, String... args) throws Throwable {
      // We have to run part of the test on a separate VM, as we need VM settings to tweak the DNS

      String securityProperties = System.getProperty("java.security.properties");

      if (securityProperties != null && securityProperties.equals(location + "/etc/zerocache.security")) {
         logger.info("No need to spawn a VM, the zerocache is already in place");
         System.setProperty("artemis.config.location", location);
         USING_SPAWN = false;
         main(args);
      } else {

         securityProperties = "-Djava.security.properties=" + location + "/etc/zerocache.security";
         String hostProperties = "-Djdk.net.hosts.file=" + location + "/etc/hosts.conf";
         String configLocation = "-Dartemis.config.location=" + location;
         String temporaryLocation = "-Djava.io.tmpdir=" + System.getProperty("java.io.tmpdir");

         logger.info("if you would like to run without Spawn for debugging purposes, add these properties to your VM arguments on this test: " + securityProperties + " " + hostProperties);
         Process p = SpawnedVMSupport.spawnVM(DNSSwitchTest.class.getName(), new String[]{securityProperties, hostProperties, configLocation, temporaryLocation}, args);
         addProcess(p);
         Assert.assertEquals(1, p.waitFor());
      }
   }

   public static void saveConf(String fileName, String... hostDefinition) throws Exception {
      if (USE_ETC_HOSTS) {
         recoverETCHosts();
         saveConf(ETC_HOSTS, true, hostDefinition);
      } else {
         saveConf(new File(fileName), false, hostDefinition);
      }
   }

   public static void saveConf(File fileName, boolean append, String... hostDefinition) throws Exception {
      PrintWriter writer = new PrintWriter(new FileOutputStream(fileName, append));
      Assert.assertTrue("you must send pairs", hostDefinition.length % 2 == 0);

      if (USE_ETC_HOSTS) {
         writer.println();
         writer.println("# this was generated by DNSSwitchTest. Make sure you recover from your backup");
      }

      for (int i = 0; i < hostDefinition.length; i += 2) {
         writer.println(hostDefinition[i] + "                  " + hostDefinition[i + 1]);
      }
      writer.close();
   }

   private static void validateIP(String host, String ip) {
      InetSocketAddress inetSocketAddress;
      inetSocketAddress = new InetSocketAddress(host, 8080);
      // And this is to validate no cache
      Assert.assertEquals(ip, inetSocketAddress.getAddress().getHostAddress());
   }

   // This main method will be used with spawnRun to continue the test on a separate VM
   public static void main(String[] arg) throws Throwable {
      try {

         String methodName = arg[0];

         Method methodReflection = DNSSwitchTest.class.getMethod(methodName, arg.getClass());
         methodReflection.invoke(null, new Object[]{arg});

         if (USING_SPAWN) {
            NetUtil.cleanup();
            System.exit(1);
         }
      } catch (InvocationTargetException e) {
         e.getCause().printStackTrace();
         if (USING_SPAWN) {
            NetUtil.cleanup();
            System.exit(2);
         } else {
            throw e;
         }
      } catch (Throwable e) {
         e.printStackTrace();
         if (USING_SPAWN) {
            NetUtil.cleanup();
            System.exit(2);
         } else {
            throw e;
         }
      }
   }

}
