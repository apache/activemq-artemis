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
package org.apache.activemq.cli.test;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.cli.Artemis;
import org.apache.activemq.artemis.cli.CLIException;
import org.apache.activemq.artemis.cli.commands.ActionContext;
import org.apache.activemq.artemis.cli.commands.Create;
import org.apache.activemq.artemis.cli.commands.Mask;
import org.apache.activemq.artemis.cli.commands.queue.StatQueue;
import org.apache.activemq.artemis.cli.commands.Run;
import org.apache.activemq.artemis.cli.commands.user.AddUser;
import org.apache.activemq.artemis.cli.commands.user.ListUser;
import org.apache.activemq.artemis.cli.commands.user.RemoveUser;
import org.apache.activemq.artemis.cli.commands.user.ResetUser;
import org.apache.activemq.artemis.cli.commands.util.SyncCalculation;
import org.apache.activemq.artemis.core.client.impl.ServerLocatorImpl;
import org.apache.activemq.artemis.core.config.FileDeploymentManager;
import org.apache.activemq.artemis.core.config.impl.FileConfiguration;
import org.apache.activemq.artemis.core.server.JournalType;
import org.apache.activemq.artemis.jlibaio.LibaioContext;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQDestination;
import org.apache.activemq.artemis.utils.DefaultSensitiveStringCodec;
import org.apache.activemq.artemis.utils.HashProcessor;
import org.apache.activemq.artemis.utils.PasswordMaskingUtil;
import org.apache.activemq.artemis.utils.StringUtil;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test to validate that the CLI doesn't throw improper exceptions when invoked.
 */
public class ArtemisTest extends CliTestBase {

   @Before
   @Override
   public void setup() throws Exception {
      setupAuth();
      super.setup();
   }

   public void setupAuth() throws Exception {
      setupAuth(temporaryFolder.getRoot());
   }

   public void setupAuth(File folder) throws Exception {
      System.setProperty("java.security.auth.login.config", folder.getAbsolutePath() + "/etc/login.config");
   }

   @Test
   public void invalidCliDoesntThrowException() {
      testCli("--silent", "create");
   }

   @Test
   public void invalidPathDoesntThrowException() {
      if (isWindows()) {
         testCli("create", "zzzzz:/rawr", "--silent");
      } else {
         testCli("create", "/rawr", "--silent");
      }
   }

   @Test
   public void testSupportsLibaio() throws Exception {
      Create x = new Create();
      x.setInstance(new File("/tmp/foo"));
      x.supportsLibaio();
   }

   @Test
   public void testSync() throws Exception {
      int writes = 2;
      int tries = 5;
      long totalAvg = SyncCalculation.syncTest(temporaryFolder.getRoot(), 4096, writes, tries, true, true, true, "file.tmp", 1, JournalType.NIO);
      System.out.println();
      System.out.println("TotalAvg = " + totalAvg);
      long nanoTime = SyncCalculation.toNanos(totalAvg, writes, false);
      System.out.println("nanoTime avg = " + nanoTime);
      assertEquals(0, LibaioContext.getTotalMaxIO());

   }

   @Test
   public void testSimpleCreate() throws Exception {
      //instance1: default using http
      File instance1 = new File(temporaryFolder.getRoot(), "instance1");
      Artemis.main("create", instance1.getAbsolutePath(), "--silent", "--no-fsync", "--no-autotune");
   }


   @Test
   public void testCreateDB() throws Exception {
      File instance1 = new File(temporaryFolder.getRoot(), "instance1");
      Artemis.internalExecute("create", instance1.getAbsolutePath(), "--silent", "--jdbc");
   }


   @Test
   public void testSimpleCreateMapped() throws Throwable {
      try {
         //instance1: default using http
         File instance1 = new File(temporaryFolder.getRoot(), "instance1");
         Artemis.main("create", instance1.getAbsolutePath(), "--silent", "--mapped", "--no-autotune");
      } catch (Throwable e) {
         e.printStackTrace();
         throw e;
      }
   }

   @Test
   public void testWebConfig() throws Exception {
      setupAuth();
      Run.setEmbedded(true);
      //instance1: default using http
      File instance1 = new File(temporaryFolder.getRoot(), "instance1");
      Artemis.main("create", instance1.getAbsolutePath(), "--silent", "--no-fsync", "--no-autotune");
      File bootstrapFile = new File(new File(instance1, "etc"), "bootstrap.xml");
      assertTrue(bootstrapFile.exists());
      Document config = parseXml(bootstrapFile);
      Element webElem = (Element) config.getElementsByTagName("web").item(0);

      String bindAttr = webElem.getAttribute("bind");
      String bindStr = "http://" + Create.HTTP_HOST + ":" + Create.HTTP_PORT;

      assertEquals(bindAttr, bindStr);
      //no any of those
      assertFalse(webElem.hasAttribute("keyStorePath"));
      assertFalse(webElem.hasAttribute("keyStorePassword"));
      assertFalse(webElem.hasAttribute("clientAuth"));
      assertFalse(webElem.hasAttribute("trustStorePath"));
      assertFalse(webElem.hasAttribute("trustStorePassword"));

      //instance2: https
      File instance2 = new File(temporaryFolder.getRoot(), "instance2");
      Artemis.main("create", instance2.getAbsolutePath(), "--silent", "--ssl-key", "etc/keystore", "--ssl-key-password", "password1", "--no-fsync", "--no-autotune");
      bootstrapFile = new File(new File(instance2, "etc"), "bootstrap.xml");
      assertTrue(bootstrapFile.exists());
      config = parseXml(bootstrapFile);
      webElem = (Element) config.getElementsByTagName("web").item(0);

      bindAttr = webElem.getAttribute("bind");
      bindStr = "https://localhost:" + Create.HTTP_PORT;
      assertEquals(bindAttr, bindStr);

      String keyStr = webElem.getAttribute("keyStorePath");
      assertEquals("etc/keystore", keyStr);
      String keyPass = webElem.getAttribute("keyStorePassword");
      assertEquals("password1", keyPass);

      assertFalse(webElem.hasAttribute("clientAuth"));
      assertFalse(webElem.hasAttribute("trustStorePath"));
      assertFalse(webElem.hasAttribute("trustStorePassword"));

      //instance3: https with clientAuth
      File instance3 = new File(temporaryFolder.getRoot(), "instance3");
      Artemis.main("create", instance3.getAbsolutePath(), "--silent", "--ssl-key", "etc/keystore", "--ssl-key-password", "password1", "--use-client-auth", "--ssl-trust", "etc/truststore", "--ssl-trust-password", "password2", "--no-fsync", "--no-autotune");
      bootstrapFile = new File(new File(instance3, "etc"), "bootstrap.xml");
      assertTrue(bootstrapFile.exists());

      byte[] contents = Files.readAllBytes(bootstrapFile.toPath());
      String cfgText = new String(contents);
      System.out.println("confg: " + cfgText);

      config = parseXml(bootstrapFile);
      webElem = (Element) config.getElementsByTagName("web").item(0);

      bindAttr = webElem.getAttribute("bind");
      bindStr = "https://localhost:" + Create.HTTP_PORT;
      assertEquals(bindAttr, bindStr);

      keyStr = webElem.getAttribute("keyStorePath");
      assertEquals("etc/keystore", keyStr);
      keyPass = webElem.getAttribute("keyStorePassword");
      assertEquals("password1", keyPass);

      String clientAuthAttr = webElem.getAttribute("clientAuth");
      assertEquals("true", clientAuthAttr);
      String trustPathAttr = webElem.getAttribute("trustStorePath");
      assertEquals("etc/truststore", trustPathAttr);
      String trustPass = webElem.getAttribute("trustStorePassword");
      assertEquals("password2", trustPass);
   }

   @Test
   public void testUserCommand() throws Exception {
      Run.setEmbedded(true);
      File instance1 = new File(temporaryFolder.getRoot(), "instance_user");
      System.setProperty("java.security.auth.login.config", instance1.getAbsolutePath() + "/etc/login.config");
      Artemis.main("create", instance1.getAbsolutePath(), "--silent", "--no-autotune");
      System.setProperty("artemis.instance", instance1.getAbsolutePath());

      File userFile = new File(instance1.getAbsolutePath() + "/etc/artemis-users.properties");
      File roleFile = new File(instance1.getAbsolutePath() + "/etc/artemis-roles.properties");

      ListUser listCmd = new ListUser();
      TestActionContext context = new TestActionContext();
      listCmd.execute(context);

      String result = context.getStdout();
      System.out.println("output1:\n" + result);

      //default only one user admin with role amq
      assertTrue(result.contains("\"admin\"(amq)"));
      checkRole("admin", roleFile, "amq");

      //add a simple user
      AddUser addCmd = new AddUser();
      addCmd.setUsername("guest");
      addCmd.setPassword("guest123");
      addCmd.setRole("admin");
      addCmd.execute(new TestActionContext());

      //verify use list cmd
      context = new TestActionContext();
      listCmd.execute(context);
      result = context.getStdout();
      System.out.println("output2:\n" + result);

      assertTrue(result.contains("\"admin\"(amq)"));
      assertTrue(result.contains("\"guest\"(admin)"));

      checkRole("guest", roleFile, "admin");
      assertTrue(checkPassword("guest", "guest123", userFile));

      //add a user with 2 roles
      addCmd = new AddUser();
      addCmd.setUsername("scott");
      addCmd.setPassword("tiger");
      addCmd.setRole("admin,operator");
      addCmd.execute(ActionContext.system());

      //verify
      context = new TestActionContext();
      listCmd.execute(context);
      result = context.getStdout();
      System.out.println("output3:\n" + result);

      assertTrue(result.contains("\"admin\"(amq)"));
      assertTrue(result.contains("\"guest\"(admin)"));
      assertTrue(result.contains("\"scott\"(admin,operator)"));

      checkRole("scott", roleFile, "admin", "operator");
      assertTrue(checkPassword("scott", "tiger", userFile));

      //add an existing user
      addCmd = new AddUser();
      addCmd.setUsername("scott");
      addCmd.setPassword("password");
      addCmd.setRole("visitor");
      try {
         addCmd.execute(ActionContext.system());
         fail("should throw an exception if adding a existing user");
      } catch (IllegalArgumentException expected) {
      }

      //check existing users are intact
      context = new TestActionContext();
      listCmd.execute(context);
      result = context.getStdout();
      System.out.println("output4:\n" + result);

      assertTrue(result.contains("\"admin\"(amq)"));
      assertTrue(result.contains("\"guest\"(admin)"));
      assertTrue(result.contains("\"scott\"(admin,operator)"));

      //remove a user
      RemoveUser rmCmd = new RemoveUser();
      rmCmd.setUsername("guest");
      rmCmd.execute(ActionContext.system());

      //check
      context = new TestActionContext();
      listCmd.execute(context);
      result = context.getStdout();
      System.out.println("output5:\n" + result);

      assertTrue(result.contains("\"admin\"(amq)"));
      assertFalse(result.contains("\"guest\"(admin)"));
      assertTrue(result.contains("\"scott\"(admin,operator)") || result.contains("\"scott\"(operator,admin)"));
      assertTrue(result.contains("Total: 2"));

      //remove another
      rmCmd = new RemoveUser();
      rmCmd.setUsername("scott");
      rmCmd.execute(ActionContext.system());

      //check
      context = new TestActionContext();
      listCmd.execute(context);
      result = context.getStdout();
      System.out.println("output6:\n" + result);

      assertTrue(result.contains("\"admin\"(amq)"));
      assertFalse(result.contains("\"guest\"(admin)"));
      assertFalse(result.contains("\"scott\"(admin,operator)") || result.contains("\"scott\"(operator,admin)"));
      assertTrue(result.contains("Total: 1"));

      //remove non-exist
      rmCmd = new RemoveUser();
      rmCmd.setUsername("alien");
      try {
         rmCmd.execute(ActionContext.system());
         fail("should throw exception when removing a non-existing user");
      } catch (IllegalArgumentException expected) {
      }

      //check
      context = new TestActionContext();
      listCmd.execute(context);
      result = context.getStdout();
      System.out.println("output7:\n" + result);
      assertTrue(result.contains("\"admin\"(amq)"));
      assertTrue(result.contains("Total: 1"));

      //now remove last
      rmCmd = new RemoveUser();
      rmCmd.setUsername("admin");
      rmCmd.execute(ActionContext.system());

      //check
      context = new TestActionContext();
      listCmd.execute(context);
      result = context.getStdout();
      System.out.println("output8:\n" + result);

      assertTrue(result.contains("Total: 0"));
   }

   @Test
   public void testUserCommandReset() throws Exception {
      Run.setEmbedded(true);
      File instance1 = new File(temporaryFolder.getRoot(), "instance_user");
      System.setProperty("java.security.auth.login.config", instance1.getAbsolutePath() + "/etc/login.config");
      Artemis.main("create", instance1.getAbsolutePath(), "--silent", "--no-autotune");
      System.setProperty("artemis.instance", instance1.getAbsolutePath());

      File userFile = new File(instance1.getAbsolutePath() + "/etc/artemis-users.properties");
      File roleFile = new File(instance1.getAbsolutePath() + "/etc/artemis-roles.properties");

      ListUser listCmd = new ListUser();
      TestActionContext context = new TestActionContext();
      listCmd.execute(context);

      String result = context.getStdout();
      System.out.println("output1:\n" + result);

      //default only one user admin with role amq
      assertTrue(result.contains("\"admin\"(amq)"));

      //remove a user
      RemoveUser rmCmd = new RemoveUser();
      rmCmd.setUsername("admin");
      rmCmd.execute(ActionContext.system());

      //check
      context = new TestActionContext();
      listCmd.execute(context);
      result = context.getStdout();
      System.out.println("output8:\n" + result);

      assertTrue(result.contains("Total: 0"));

      //add some users
      AddUser addCmd = new AddUser();
      addCmd.setUsername("guest");
      addCmd.setPassword("guest123");
      addCmd.setRole("admin");
      addCmd.execute(new TestActionContext());

      addCmd.setUsername("user1");
      addCmd.setPassword("password1");
      addCmd.setRole("admin,manager");
      addCmd.execute(new TestActionContext());
      assertTrue(checkPassword("user1", "password1", userFile));

      addCmd.setUsername("user2");
      addCmd.setPassword("password2");
      addCmd.setRole("admin,manager,master");
      addCmd.execute(new TestActionContext());

      addCmd.setUsername("user3");
      addCmd.setPassword("password3");
      addCmd.setRole("system,master");
      addCmd.execute(new TestActionContext());

      //verify use list cmd
      context = new TestActionContext();
      listCmd.execute(context);
      result = context.getStdout();
      System.out.println("output2:\n" + result);

      assertTrue(result.contains("Total: 4"));
      assertTrue(result.contains("\"guest\"(admin)"));
      assertTrue(result.contains("\"user1\"(admin,manager)"));
      assertTrue(result.contains("\"user2\"(admin,manager,master)"));
      assertTrue(result.contains("\"user3\"(master,system)"));

      checkRole("user1", roleFile, "admin", "manager");

      //reset password
      context = new TestActionContext();
      ResetUser resetCommand = new ResetUser();
      resetCommand.setUsername("user1");
      resetCommand.setPassword("newpassword1");
      resetCommand.execute(context);

      checkRole("user1", roleFile, "admin", "manager");
      assertFalse(checkPassword("user1", "password1", userFile));
      assertTrue(checkPassword("user1", "newpassword1", userFile));

      //reset role
      resetCommand.setUsername("user2");
      resetCommand.setRole("manager,master,operator");
      resetCommand.execute(new TestActionContext());

      checkRole("user2", roleFile, "manager", "master", "operator");

      //reset both
      resetCommand.setUsername("user3");
      resetCommand.setPassword("newpassword3");
      resetCommand.setRole("admin,system");
      resetCommand.execute(new ActionContext());

      checkRole("user3", roleFile, "admin", "system");
      assertTrue(checkPassword("user3", "newpassword3", userFile));
   }

   @Test
   public void testMaskCommand() throws Exception {

      String password1 = "password";
      String encrypt1 = "3a34fd21b82bf2a822fa49a8d8fa115d";
      String newKey = "artemisfun";
      String encrypt2 = "-2b8e3b47950b9b481a6f3100968e42e9";


      TestActionContext context = new TestActionContext();
      Mask mask = new Mask();
      mask.setPassword(password1);

      String result = (String) mask.execute(context);
      System.out.println(context.getStdout());
      assertEquals(encrypt1, result);

      context = new TestActionContext();
      mask = new Mask();
      mask.setPassword(password1);
      mask.setHash(true);
      result = (String) mask.execute(context);
      System.out.println(context.getStdout());
      DefaultSensitiveStringCodec codec = mask.getCodec();
      codec.verify(password1.toCharArray(), result);

      context = new TestActionContext();
      mask = new Mask();
      mask.setPassword(password1);
      mask.setKey(newKey);
      result = (String) mask.execute(context);
      System.out.println(context.getStdout());
      assertEquals(encrypt2, result);
   }

   @Test
   public void testSimpleRun() throws Exception {
      testSimpleRun("server");
   }

   @Test
   public void testWeirdCharacter() throws Exception {
      testSimpleRun("test%26%26x86_6");
   }


   @Test
   public void testSpaces() throws Exception {
      testSimpleRun("with space");
   }


   @Test
   public void testPerfJournal() throws Exception {
      File instanceFolder = temporaryFolder.newFolder("server1");
      setupAuth(instanceFolder);

      Run.setEmbedded(true);
      Artemis.main("create", instanceFolder.getAbsolutePath(), "--force", "--silent", "--no-web", "--no-autotune", "--require-login");
      System.setProperty("artemis.instance", instanceFolder.getAbsolutePath());

      Artemis.main("perf-journal", "--journal-type", "NIO", "--writes", "5", "--tries", "1");

   }


   public void testSimpleRun(String folderName) throws Exception {
      File instanceFolder = temporaryFolder.newFolder(folderName);

      setupAuth(instanceFolder);
      String queues = "q1,q2";
      String addresses = "a1,a2";


      // This is usually set when run from the command line via artemis.profile
      Run.setEmbedded(true);
      Artemis.main("create", instanceFolder.getAbsolutePath(), "--force", "--silent", "--no-web", "--queues", queues, "--addresses", addresses, "--no-autotune", "--require-login");
      System.setProperty("artemis.instance", instanceFolder.getAbsolutePath());


      try {
         // Some exceptions may happen on the initialization, but they should be ok on start the basic core protocol
         Artemis.internalExecute("run");

         try (ServerLocator locator = ServerLocatorImpl.newLocator("tcp://localhost:61616");
              ClientSessionFactory factory = locator.createSessionFactory();
              ClientSession coreSession = factory.createSession("admin", "admin", false, true, true, false, 0)) {
            for (String str : queues.split(",")) {
               ClientSession.QueueQuery queryResult = coreSession.queueQuery(SimpleString.toSimpleString(str));
               assertTrue("Couldn't find queue " + str, queryResult.isExists());
            }
            for (String str : addresses.split(",")) {
               ClientSession.AddressQuery queryResult = coreSession.addressQuery(SimpleString.toSimpleString(str));
               assertTrue("Couldn't find address " + str, queryResult.isExists());
            }
         }

         try {
            Artemis.internalExecute("data", "print");
            Assert.fail("Exception expected");
         } catch (CLIException expected) {
         }
         Artemis.internalExecute("data", "print", "--f");

         assertEquals(Integer.valueOf(100), Artemis.internalExecute("producer", "--message-count", "100", "--user", "admin", "--password", "admin"));
         assertEquals(Integer.valueOf(100), Artemis.internalExecute("consumer", "--break-on-null", "--receive-timeout", "100", "--user", "admin", "--password", "admin"));
         assertEquals(Integer.valueOf(10), Artemis.internalExecute("producer", "--text-size", "500", "--message-count", "10", "--user", "admin", "--password", "admin"));
         assertEquals(Integer.valueOf(10), Artemis.internalExecute("consumer", "--break-on-null", "--receive-timeout", "100", "--user", "admin", "--password", "admin"));
         assertEquals(Integer.valueOf(10), Artemis.internalExecute("producer", "--message-size", "500", "--message-count", "10", "--user", "admin", "--password", "admin"));
         assertEquals(Integer.valueOf(10), Artemis.internalExecute("consumer", "--break-on-null", "--receive-timeout", "100", "--user", "admin", "--password", "admin"));

         ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("tcp://localhost:61616");
         Connection connection = cf.createConnection("admin", "admin");
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         MessageProducer producer = session.createProducer(ActiveMQDestination.createDestination("queue://TEST", ActiveMQDestination.TYPE.QUEUE));

         TextMessage message = session.createTextMessage("Banana");
         message.setStringProperty("fruit", "banana");
         producer.send(message);

         for (int i = 0; i < 100; i++) {
            message = session.createTextMessage("orange");
            message.setStringProperty("fruit", "orange");
            producer.send(message);
         }
         session.commit();

         connection.close();
         cf.close();

         assertEquals(Integer.valueOf(1), Artemis.internalExecute("browser", "--txt-size", "50", "--filter", "fruit='banana'", "--user", "admin", "--password", "admin"));

         assertEquals(Integer.valueOf(100), Artemis.internalExecute("browser", "--txt-size", "50", "--filter", "fruit='orange'", "--user", "admin", "--password", "admin"));

         assertEquals(Integer.valueOf(101), Artemis.internalExecute("browser", "--txt-size", "50", "--user", "admin", "--password", "admin"));

         // should only receive 10 messages on browse as I'm setting messageCount=10
         assertEquals(Integer.valueOf(10), Artemis.internalExecute("browser", "--txt-size", "50", "--message-count", "10", "--user", "admin", "--password", "admin"));

         // Nothing was consumed until here as it was only browsing, check it's receiving again
         assertEquals(Integer.valueOf(1), Artemis.internalExecute("consumer", "--txt-size", "50", "--break-on-null", "--receive-timeout", "100", "--filter", "fruit='banana'", "--user", "admin", "--password", "admin"));

         // Checking it was acked before
         assertEquals(Integer.valueOf(100), Artemis.internalExecute("consumer", "--txt-size", "50", "--break-on-null", "--receive-timeout", "100", "--user", "admin", "--password", "admin"));
      } finally {
         stopServer();
      }
   }


   @Test
   public void testPing() throws Exception {
      File instanceFolder = temporaryFolder.newFolder("pingTest");

      setupAuth(instanceFolder);
      String queues = "q1,t2";
      String topics = "t1,t2";

      // This is usually set when run from the command line via artemis.profile
      Run.setEmbedded(true);
      Artemis.main("create", instanceFolder.getAbsolutePath(), "--force", "--silent", "--no-web", "--queues", queues, "--addresses", topics, "--no-autotune", "--require-login", "--ping", "127.0.0.1", "--no-autotune");
      System.setProperty("artemis.instance", instanceFolder.getAbsolutePath());

      FileConfiguration fc = new FileConfiguration();
      FileDeploymentManager deploymentManager = new FileDeploymentManager(new File(instanceFolder, "./etc/broker.xml").toURI().toString());
      deploymentManager.addDeployable(fc);
      deploymentManager.readConfiguration();

      Assert.assertEquals("127.0.0.1", fc.getNetworkCheckList());

   }

   @Test
   public void testQstat() throws Exception {

      File instanceQstat = new File(temporaryFolder.getRoot(), "instanceQStat");
      setupAuth(instanceQstat);
      Run.setEmbedded(true);
      Artemis.main("create", instanceQstat.getAbsolutePath(), "--silent", "--no-fsync", "--no-autotune", "--no-web", "--require-login");
      System.setProperty("artemis.instance", instanceQstat.getAbsolutePath());
      Artemis.internalExecute("run");

      try (ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("tcp://localhost:61616"); Connection connection = cf.createConnection("admin", "admin");) {

         //set up some queues with messages and consumers
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         connection.start();
         sendMessages(session, "Test1", 15);
         sendMessages(session, "Test11", 1);
         sendMessages(session, "Test12", 1);
         sendMessages(session, "Test20", 20);
         MessageConsumer consumer = session.createConsumer(ActiveMQDestination.createDestination("queue://Test1", ActiveMQDestination.TYPE.QUEUE));
         MessageConsumer consumer2 = session.createConsumer(ActiveMQDestination.createDestination("queue://Test1", ActiveMQDestination.TYPE.QUEUE));

         for (int i = 0; i < 5; i++) {
            Message message = consumer.receive(100);
         }

         //check all queues containing "Test1" are displayed
         TestActionContext context = new TestActionContext();
         StatQueue statQueue = new StatQueue();
         statQueue.setUser("admin");
         statQueue.setPassword("admin");
         statQueue.setQueueName("Test1");
         statQueue.execute(context);
         ArrayList<String> lines = getOutputLines(context, false);
         // Header line + 3 queues
         Assert.assertEquals("rows returned using queueName=Test1", 4, lines.size());

         //check all queues are displayed when no Filter set
         context = new TestActionContext();
         statQueue = new StatQueue();
         statQueue.setUser("admin");
         statQueue.setPassword("admin");
         statQueue.execute(context);
         lines = getOutputLines(context, false);
         // Header line + 4 queues (at least - possibly other infra queues as well)
         Assert.assertTrue("rows returned filtering no name ", 5 <= lines.size());

         //check all queues containing "Test1" are displayed using Filter field NAME
         context = new TestActionContext();
         statQueue = new StatQueue();
         statQueue.setUser("admin");
         statQueue.setPassword("admin");
         statQueue.setFieldName("NAME");
         statQueue.setOperationName("CONTAINS");
         statQueue.setValue("Test1");
         statQueue.execute(context);
         lines = getOutputLines(context, false);
         // Header line + 3 queues
         Assert.assertEquals("rows returned filtering by NAME ", 4, lines.size());

         //check only queue named "Test1" is displayed using Filter field NAME and operation EQUALS
         context = new TestActionContext();
         statQueue = new StatQueue();
         statQueue.setUser("admin");
         statQueue.setPassword("admin");
         statQueue.setFieldName("NAME");
         statQueue.setOperationName("EQUALS");
         statQueue.setValue("Test1");
         statQueue.execute(context);
         lines = getOutputLines(context, false);
         //Header line + 1 queue only
         Assert.assertEquals("rows returned filtering by NAME operation EQUALS", 2, lines.size());
         //verify contents of queue stat line is correct
         String queueTest1 = lines.get(1);
         String[] parts = queueTest1.split("\\|");
         Assert.assertEquals("queue name", "Test1", parts[1].trim());
         Assert.assertEquals("address name", "Test1", parts[2].trim());
         Assert.assertEquals("Consumer count", "2", parts[3].trim());
         Assert.assertEquals("Message count", "10", parts[4].trim());
         Assert.assertEquals("Added count", "15", parts[5].trim());
         Assert.assertEquals("Delivering count", "10", parts[6].trim());
         Assert.assertEquals("Acked count", "5", parts[7].trim());

         //check all queues containing address "Test1" are displayed using Filter field ADDRESS
         context = new TestActionContext();
         statQueue = new StatQueue();
         statQueue.setUser("admin");
         statQueue.setPassword("admin");
         statQueue.setFieldName("ADDRESS");
         statQueue.setOperationName("CONTAINS");
         statQueue.setValue("Test1");
         statQueue.execute(context);
         lines = getOutputLines(context, false);
         // Header line + 3 queues
         Assert.assertEquals("rows returned filtering by ADDRESS", 4, lines.size());

         //check all queues containing address "Test1" are displayed using Filter field MESSAGE_COUNT
         context = new TestActionContext();
         statQueue = new StatQueue();
         statQueue.setUser("admin");
         statQueue.setPassword("admin");
         statQueue.setFieldName("MESSAGE_COUNT");
         statQueue.setOperationName("CONTAINS");
         statQueue.setValue("10");
         statQueue.execute(context);
         lines = getOutputLines(context, false);

         // Header line + 0 queues
         Assert.assertEquals("rows returned filtering by MESSAGE_COUNT", 1, lines.size());

         //check all queues containing address "Test1" are displayed using Filter field MESSAGE_ADDED
         context = new TestActionContext();
         statQueue = new StatQueue();
         statQueue.setUser("admin");
         statQueue.setPassword("admin");
         statQueue.setFieldName("MESSAGES_ADDED");
         statQueue.setOperationName("CONTAINS");
         statQueue.setValue("20");
         statQueue.execute(context);
         lines = getOutputLines(context, false);
         // Header line + 0 queues
         Assert.assertEquals("rows returned filtering by MESSAGES_ADDED", 1, lines.size());

         //check  queues with greater_than 19 MESSAGE_ADDED  displayed
         context = new TestActionContext();
         statQueue = new StatQueue();
         statQueue.setUser("admin");
         statQueue.setPassword("admin");
         statQueue.setFieldName("MESSAGES_ADDED");
         statQueue.setOperationName("GREATER_THAN");
         statQueue.setValue("19");
         statQueue.execute(context);
         lines = getOutputLines(context, false);

         // Header line + 1 queues
         Assert.assertEquals("rows returned filtering by MESSAGES_ADDED", 2, lines.size());
         String[] columns = lines.get(1).split("\\|");
         Assert.assertEquals("queue name filtered by MESSAGES_ADDED GREATER_THAN ", "Test20", columns[2].trim());

         //check queues with less_than 2 MESSAGE_ADDED displayed
         context = new TestActionContext();
         statQueue = new StatQueue();
         statQueue.setUser("admin");
         statQueue.setPassword("admin");
         statQueue.setFieldName("MESSAGES_ADDED");
         statQueue.setOperationName("LESS_THAN");
         statQueue.setValue("2");
         statQueue.execute(context);
         lines = getOutputLines(context, false);

         // Header line + "at least" 2 queues
         Assert.assertTrue("rows returned filtering by MESSAGES_ADDED LESS_THAN", 2 <= lines.size());

         //walk the result returned and the specific destinations are not part of the output
         for (String line : lines) {
            columns = line.split("\\|");
            Assert.assertNotEquals("ensure Test20 is not part of returned result", "Test20", columns[2].trim());
            Assert.assertNotEquals("ensure Test1 is not part of returned result", "Test1", columns[2].trim());
         }

         //check all queues containing address "Test1" are displayed using Filter field DELIVERING_COUNT
         context = new TestActionContext();
         statQueue = new StatQueue();
         statQueue.setUser("admin");
         statQueue.setPassword("admin");
         statQueue.setFieldName("DELIVERING_COUNT");
         statQueue.setOperationName("EQUALS");
         statQueue.setValue("10");
         statQueue.execute(context);
         lines = getOutputLines(context, false);
         columns = lines.get(1).split("\\|");
         // Header line + 1 queues
         Assert.assertEquals("rows returned filtering by DELIVERING_COUNT", 2, lines.size());
         Assert.assertEquals("queue name filtered by DELIVERING_COUNT ", "Test1", columns[2].trim());

         //check all queues containing address "Test1" are displayed using Filter field CONSUMER_COUNT
         context = new TestActionContext();
         statQueue = new StatQueue();
         statQueue.setUser("admin");
         statQueue.setPassword("admin");
         statQueue.setFieldName("CONSUMER_COUNT");
         statQueue.setOperationName("EQUALS");
         statQueue.setValue("2");
         statQueue.execute(context);
         lines = getOutputLines(context, false);
         columns = lines.get(1).split("\\|");
         // Header line + 1 queues
         Assert.assertEquals("rows returned filtering by CONSUMER_COUNT ", 2, lines.size());
         Assert.assertEquals("queue name filtered by CONSUMER_COUNT ", "Test1", columns[2].trim());

         //check all queues containing address "Test1" are displayed using Filter field MESSAGE_ACKED
         context = new TestActionContext();
         statQueue = new StatQueue();
         statQueue.setUser("admin");
         statQueue.setPassword("admin");
         statQueue.setFieldName("MESSAGES_ACKED");
         statQueue.setOperationName("EQUALS");
         statQueue.setValue("5");
         statQueue.execute(context);
         lines = getOutputLines(context, false);
         columns = lines.get(1).split("\\|");
         // Header line + 1 queues
         Assert.assertEquals("rows returned filtering by MESSAGE_ACKED ", 2, lines.size());
         Assert.assertEquals("queue name filtered by MESSAGE_ACKED", "Test1", columns[2].trim());

         //check no queues  are displayed when name does not match
         context = new TestActionContext();
         statQueue = new StatQueue();
         statQueue.setUser("admin");
         statQueue.setPassword("admin");
         statQueue.setQueueName("no_queue_name");
         statQueue.execute(context);
         lines = getOutputLines(context, false);
         // Header line + 0 queues
         Assert.assertEquals("rows returned by queueName for no Matching queue ", 1, lines.size());

         //check maxrows is taking effect"
         context = new TestActionContext();
         statQueue = new StatQueue();
         statQueue.setUser("admin");
         statQueue.setPassword("admin");
         statQueue.setQueueName("Test1");
         statQueue.setMaxRows(1);
         statQueue.execute(context);
         lines = getOutputLines(context, false);
         // Header line + 1 queue only
         Assert.assertEquals("rows returned by maxRows=1", 2, lines.size());

      } finally {
         stopServer();
      }

   }

   @Test
   public void testQstatErrors() throws Exception {

      File instanceQstat = new File(temporaryFolder.getRoot(), "instanceQStatErrors");
      setupAuth(instanceQstat);
      Run.setEmbedded(true);
      Artemis.main("create", instanceQstat.getAbsolutePath(), "--silent", "--no-fsync", "--no-autotune", "--no-web", "--require-login");
      System.setProperty("artemis.instance", instanceQstat.getAbsolutePath());
      Artemis.internalExecute("run");
      try {

         //check err when FIELD wrong"
         TestActionContext context = new TestActionContext();
         StatQueue statQueue = new StatQueue();
         statQueue.setUser("admin");
         statQueue.setPassword("admin");
         statQueue.setFieldName("WRONG_FILE");
         statQueue.setOperationName("EQUALS");
         statQueue.setValue("5");
         statQueue.execute(context);
         ArrayList<String> lines = getOutputLines(context, false);
         // Header line + 0 queue
         Assert.assertEquals("No stdout for wrong FIELD", 0, lines.size());

         lines = getOutputLines(context, true);
         // 1 error line
         Assert.assertEquals("stderr for wrong FIELD", 1, lines.size());
         Assert.assertTrue("'FIELD incorrect' error messsage", lines.get(0).contains("'--field' must be set to one of the following"));

         //Test err when OPERATION wrong
         context = new TestActionContext();
         statQueue = new StatQueue();
         statQueue.setUser("admin");
         statQueue.setPassword("admin");
         statQueue.setFieldName("MESSAGE_COUNT");
         statQueue.setOperationName("WRONG_OPERATION");
         statQueue.setValue("5");
         statQueue.execute(context);
         lines = getOutputLines(context, false);
         // Header line + 0 queue
         Assert.assertEquals("No stdout for wrong OPERATION", 0, lines.size());
         lines = getOutputLines(context, true);
         // 1 error line
         Assert.assertEquals("stderr for wrong OPERATION", 1, lines.size());
         Assert.assertTrue("'OPERATION incorrect' error message", lines.get(0).contains("'--operation' must be set to one of the following"));

         //Test err when queueName and field set together
         context = new TestActionContext();
         statQueue = new StatQueue();
         statQueue.setUser("admin");
         statQueue.setPassword("admin");
         statQueue.setQueueName("DLQ");
         statQueue.setFieldName("MESSAGE_COUNT");
         statQueue.setOperationName("CONTAINS");
         statQueue.setValue("5");
         statQueue.execute(context);
         lines = getOutputLines(context, false);
         // Header line + 0 queue
         Assert.assertEquals("No stdout for --field and --queueName both set", 0, lines.size());

         lines = getOutputLines(context, true);
         // 1 error line
         Assert.assertEquals("stderr for  --field and --queueName both set", 1, lines.size());
         Assert.assertTrue("field and queueName error message", lines.get(0).contains("'--field' and '--queueName' cannot be specified together"));

         //Test err when field set but no value
         context = new TestActionContext();
         statQueue = new StatQueue();
         statQueue.setUser("admin");
         statQueue.setPassword("admin");
         statQueue.setFieldName("MESSAGE_COUNT");
         statQueue.setOperationName("CONTAINS");
         statQueue.execute(context);
         lines = getOutputLines(context, false);
         // Header line + 0 queue
         Assert.assertEquals("No stdout for --field set BUT no --value", 0, lines.size());
         lines = getOutputLines(context, true);
         // 1 error line
         Assert.assertEquals("stderr for --field set BUT no --value", 1, lines.size());
         Assert.assertTrue("NO VALUE error message", lines.get(0).contains("'--value' needs to be set when '--field' is specified"));

         //Test err when field set but no operation
         context = new TestActionContext();
         statQueue = new StatQueue();
         statQueue.setUser("admin");
         statQueue.setPassword("admin");
         statQueue.setFieldName("MESSAGE_COUNT");
         statQueue.setValue("5");
         statQueue.execute(context);
         lines = getOutputLines(context, false);
         // Header line + 0 queue
         Assert.assertEquals("No stdout for --field set BUT no --operation", 0, lines.size());
         lines = getOutputLines(context, true);
         // 1 error line
         Assert.assertEquals("stderr for --field set BUT no --operation", 1, lines.size());
         Assert.assertTrue("OPERATION incorrect error message", lines.get(0).contains("'--operation' must be set when '--field' is specified "));

      } finally {
         stopServer();
      }

   }

   //read individual lines from byteStream
   private ArrayList<String> getOutputLines(TestActionContext context, boolean errorOutput) throws IOException {
      byte[] bytes;

      if (errorOutput) {
         bytes = context.getStdErrBytes();
      } else {
         bytes = context.getStdoutBytes();
      }
      BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(bytes)));
      ArrayList<String> lines = new ArrayList<>();

      String currentLine = bufferedReader.readLine();
      while (currentLine != null) {
         lines.add(currentLine);
         currentLine = bufferedReader.readLine();
      }

      return lines;
   }

   private void sendMessages(Session session, String queueName, int messageCount) throws JMSException {
      MessageProducer producer = session.createProducer(ActiveMQDestination.createDestination("queue://" + queueName, ActiveMQDestination.TYPE.QUEUE));

      TextMessage message = session.createTextMessage("sample message");
      for (int i = 0; i < messageCount; i++) {
         producer.send(message);
      }
   }

   private void testCli(String... args) {
      try {
         Artemis.main(args);
      } catch (Exception e) {
         e.printStackTrace();
         fail("Exception caught " + e.getMessage());
      }
   }

   public boolean isWindows() {
      return System.getProperty("os.name", "null").toLowerCase().indexOf("win") >= 0;
   }

   private void stopServer() throws Exception {
      Artemis.internalExecute("stop");
      assertTrue(Run.latchRunning.await(5, TimeUnit.SECONDS));
      assertEquals(0, LibaioContext.getTotalMaxIO());
   }

   private static Document parseXml(File xmlFile) throws ParserConfigurationException, IOException, SAXException {
      DocumentBuilderFactory domFactory = DocumentBuilderFactory.newInstance();
      DocumentBuilder domBuilder = domFactory.newDocumentBuilder();
      return domBuilder.parse(xmlFile);
   }

   private void checkRole(String user, File roleFile, String... roles) throws Exception {
      Configurations configs = new Configurations();
      FileBasedConfigurationBuilder<PropertiesConfiguration> roleBuilder = configs.propertiesBuilder(roleFile);
      PropertiesConfiguration roleConfig = roleBuilder.getConfiguration();

      for (String r : roles) {
         String storedUsers = (String) roleConfig.getProperty(r);

         System.out.println("users in role: " + r + " ; " + storedUsers);
         List<String> userList = StringUtil.splitStringList(storedUsers, ",");
         assertTrue(userList.contains(user));
      }
   }

   private boolean checkPassword(String user, String password, File userFile) throws Exception {
      Configurations configs = new Configurations();
      FileBasedConfigurationBuilder<PropertiesConfiguration> userBuilder = configs.propertiesBuilder(userFile);
      PropertiesConfiguration userConfig = userBuilder.getConfiguration();
      String storedPassword = (String) userConfig.getProperty(user);
      HashProcessor processor = PasswordMaskingUtil.getHashProcessor(storedPassword);
      return processor.compare(password.toCharArray(), storedPassword);
   }

}
