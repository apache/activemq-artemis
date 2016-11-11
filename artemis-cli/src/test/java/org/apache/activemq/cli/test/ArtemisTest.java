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
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.cli.Artemis;
import org.apache.activemq.artemis.cli.CLIException;
import org.apache.activemq.artemis.cli.commands.ActionContext;
import org.apache.activemq.artemis.cli.commands.Create;
import org.apache.activemq.artemis.cli.commands.Mask;
import org.apache.activemq.artemis.cli.commands.Run;
import org.apache.activemq.artemis.cli.commands.tools.LockAbstract;
import org.apache.activemq.artemis.cli.commands.user.AddUser;
import org.apache.activemq.artemis.cli.commands.user.ListUser;
import org.apache.activemq.artemis.cli.commands.user.RemoveUser;
import org.apache.activemq.artemis.cli.commands.user.ResetUser;
import org.apache.activemq.artemis.cli.commands.util.SyncCalculation;
import org.apache.activemq.artemis.core.client.impl.ServerLocatorImpl;
import org.apache.activemq.artemis.core.config.FileDeploymentManager;
import org.apache.activemq.artemis.core.config.impl.FileConfiguration;
import org.apache.activemq.artemis.jlibaio.LibaioContext;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQDestination;
import org.apache.activemq.artemis.spi.core.security.jaas.PropertiesLoader;
import org.apache.activemq.artemis.utils.DefaultSensitiveStringCodec;
import org.apache.activemq.artemis.utils.ThreadLeakCheckRule;
import org.apache.activemq.artemis.utils.HashProcessor;
import org.apache.activemq.artemis.utils.PasswordMaskingUtil;
import org.apache.activemq.artemis.utils.StringUtil;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
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
public class ArtemisTest {

   @Rule
   public TemporaryFolder temporaryFolder;

   @Rule
   public ThreadLeakCheckRule leakCheckRule = new ThreadLeakCheckRule();

   private String original = System.getProperty("java.security.auth.login.config");

   public ArtemisTest() {
      File parent = new File("./target/tmp");
      parent.mkdirs();
      temporaryFolder = new TemporaryFolder(parent);
   }

   @Before
   public void setup() throws Exception {
      setupAuth();
      Run.setEmbedded(true);
      PropertiesLoader.resetUsersAndGroupsCache();
   }

   public void setupAuth() throws Exception {
      setupAuth(temporaryFolder.getRoot());
   }

   public void setupAuth(File folder) throws Exception {
      System.setProperty("java.security.auth.login.config", folder.getAbsolutePath() + "/etc/login.config");
   }

   @After
   public void cleanup() {
      ActiveMQClient.clearThreadPools();
      System.clearProperty("artemis.instance");
      Run.setEmbedded(false);

      if (original == null) {
         System.clearProperty("java.security.auth.login.config");
      } else {
         System.setProperty("java.security.auth.login.config", original);
      }

      LockAbstract.unlock();
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
      int writes = 20;
      int tries = 10;
      long totalAvg = SyncCalculation.syncTest(temporaryFolder.getRoot(), 4096, writes, tries, true, true, true);
      System.out.println();
      System.out.println("TotalAvg = " + totalAvg);
      long nanoTime = SyncCalculation.toNanos(totalAvg, writes);
      System.out.println("nanoTime avg = " + nanoTime);
      assertEquals(0, LibaioContext.getTotalMaxIO());

   }

   @Test
   public void testWebConfig() throws Exception {
      setupAuth();
      Run.setEmbedded(true);
      //instance1: default using http
      File instance1 = new File(temporaryFolder.getRoot(), "instance1");
      Artemis.main("create", instance1.getAbsolutePath(), "--silent", "--no-fsync");
      File bootstrapFile = new File(new File(instance1, "etc"), "bootstrap.xml");
      assertTrue(bootstrapFile.exists());
      Document config = parseXml(bootstrapFile);
      Element webElem = (Element) config.getElementsByTagName("web").item(0);

      String bindAttr = webElem.getAttribute("bind");
      String bindStr = "http://localhost:" + Create.HTTP_PORT;

      assertEquals(bindAttr, bindStr);
      //no any of those
      assertFalse(webElem.hasAttribute("keyStorePath"));
      assertFalse(webElem.hasAttribute("keyStorePassword"));
      assertFalse(webElem.hasAttribute("clientAuth"));
      assertFalse(webElem.hasAttribute("trustStorePath"));
      assertFalse(webElem.hasAttribute("trustStorePassword"));

      //instance2: https
      File instance2 = new File(temporaryFolder.getRoot(), "instance2");
      Artemis.main("create", instance2.getAbsolutePath(), "--silent", "--ssl-key", "etc/keystore", "--ssl-key-password", "password1", "--no-fsync");
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
      Artemis.main("create", instance3.getAbsolutePath(), "--silent", "--ssl-key", "etc/keystore", "--ssl-key-password", "password1", "--use-client-auth", "--ssl-trust", "etc/truststore", "--ssl-trust-password", "password2", "--no-fsync");
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
      Artemis.main("create", instance1.getAbsolutePath(), "--silent");
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
      Artemis.main("create", instance1.getAbsolutePath(), "--silent");
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


   public void testSimpleRun(String folderName) throws Exception {
      File instanceFolder = temporaryFolder.newFolder(folderName);

      setupAuth(instanceFolder);
      String queues = "q1,t2";
      String topics = "t1,t2";


      // This is usually set when run from the command line via artemis.profile
      Run.setEmbedded(false);
      Artemis.main("create", instanceFolder.getAbsolutePath(), "--force", "--silent", "--no-web", "--queues", queues, "--topics", topics, "--no-autotune", "--require-login");
      System.setProperty("artemis.instance", instanceFolder.getAbsolutePath());

      // Some exceptions may happen on the initialization, but they should be ok on start the basic core protocol
      Artemis.internalExecute("run");

      Artemis.main("queue", "create", "--name", "q1", "--address", "q1", "--user", "admin", "--password", "admin");
      Artemis.main("queue", "create", "--name", "t2", "--address", "t2", "--user", "admin", "--password", "admin");

      try {
         try (ServerLocator locator = ServerLocatorImpl.newLocator("tcp://localhost:61616");
              ClientSessionFactory factory = locator.createSessionFactory();
              ClientSession coreSession = factory.createSession("admin", "admin", false, true, true, false, 0)) {
            for (String str : queues.split(",")) {
               ClientSession.QueueQuery queryResult = coreSession.queueQuery(SimpleString.toSimpleString(str));
               assertTrue("Couldn't find queue " + str, queryResult.isExists());
            }
            for (String str : topics.split(",")) {
               ClientSession.QueueQuery queryResult = coreSession.queueQuery(SimpleString.toSimpleString(str));
               assertTrue("Couldn't find topic " + str, queryResult.isExists());
            }
         }

         try {
            Artemis.internalExecute("data", "print");
            Assert.fail("Exception expected");
         } catch (CLIException expected) {
         }
         Artemis.internalExecute("data", "print", "--f");

         assertEquals(Integer.valueOf(100), Artemis.internalExecute("producer", "--message-count", "100", "--verbose", "--user", "admin", "--password", "admin"));
         assertEquals(Integer.valueOf(100), Artemis.internalExecute("consumer", "--verbose", "--break-on-null", "--receive-timeout", "100", "--user", "admin", "--password", "admin"));

         ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("tcp://localhost:61616");
         Connection connection = cf.createConnection("admin", "admin");
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         MessageProducer producer = session.createProducer(ActiveMQDestination.createDestination("queue://TEST", ActiveMQDestination.QUEUE_TYPE));

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

         assertEquals(Integer.valueOf(1), Artemis.internalExecute("browser", "--txt-size", "50", "--verbose", "--filter", "fruit='banana'", "--user", "admin", "--password", "admin"));

         assertEquals(Integer.valueOf(100), Artemis.internalExecute("browser", "--txt-size", "50", "--verbose", "--filter", "fruit='orange'", "--user", "admin", "--password", "admin"));

         assertEquals(Integer.valueOf(101), Artemis.internalExecute("browser", "--txt-size", "50", "--verbose", "--user", "admin", "--password", "admin"));

         // should only receive 10 messages on browse as I'm setting messageCount=10
         assertEquals(Integer.valueOf(10), Artemis.internalExecute("browser", "--txt-size", "50", "--verbose", "--message-count", "10", "--user", "admin", "--password", "admin"));

         // Nothing was consumed until here as it was only browsing, check it's receiving again
         assertEquals(Integer.valueOf(1), Artemis.internalExecute("consumer", "--txt-size", "50", "--verbose", "--break-on-null", "--receive-timeout", "100", "--filter", "fruit='banana'", "--user", "admin", "--password", "admin"));

         // Checking it was acked before
         assertEquals(Integer.valueOf(100), Artemis.internalExecute("consumer", "--txt-size", "50", "--verbose", "--break-on-null", "--receive-timeout", "100", "--user", "admin", "--password", "admin"));
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
      Artemis.main("create", instanceFolder.getAbsolutePath(), "--force", "--silent", "--no-web", "--queues", queues, "--topics", topics, "--no-autotune", "--require-login", "--ping", "127.0.0.1");
      System.setProperty("artemis.instance", instanceFolder.getAbsolutePath());

      FileConfiguration fc = new FileConfiguration();
      FileDeploymentManager deploymentManager = new FileDeploymentManager(new File(instanceFolder, "./etc/broker.xml").toURI().toString());
      deploymentManager.addDeployable(fc);
      deploymentManager.readConfiguration();

      Assert.assertEquals("127.0.0.1", fc.getNetworkCheckList());

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
