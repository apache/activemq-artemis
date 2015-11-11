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
import java.io.File;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.cli.Artemis;
import org.apache.activemq.artemis.cli.commands.Configurable;
import org.apache.activemq.artemis.cli.commands.Create;
import org.apache.activemq.artemis.cli.commands.Run;
import org.apache.activemq.artemis.cli.commands.util.SyncCalculation;
import org.apache.activemq.artemis.core.client.impl.ServerLocatorImpl;
import org.apache.activemq.artemis.jlibaio.LibaioContext;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQDestination;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test to validate that the CLI doesn't throw improper exceptions when invoked.
 */
public class ArtemisTest {

   @Rule
   public TemporaryFolder temporaryFolder;

   private String original = System.getProperty("java.security.auth.login.config");

   public ArtemisTest() {
      File parent = new File("./target/tmp");
      parent.mkdirs();
      temporaryFolder = new TemporaryFolder(parent);
   }

   @Before
   public void setup() {
      System.setProperty("java.security.auth.login.config", temporaryFolder.getRoot().getAbsolutePath() + "/etc/login.config");
   }

   @After
   public void cleanup() {
      System.clearProperty("artemis.instance");
      Run.setEmbedded(false);

      if (original == null) {
         System.clearProperty("java.security.auth.login.config");
      }
      else {
         System.setProperty("java.security.auth.login.config", original);
      }

      Configurable.unlock();
   }

   @Test
   public void invalidCliDoesntThrowException() {
      testCli("--silent", "create");
   }

   @Test
   public void invalidPathDoesntThrowException() {
      if (isWindows()) {
         testCli("create", "zzzzz:/rawr", "--silent");
      }
      else {
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
      long totalAvg = SyncCalculation.syncTest(temporaryFolder.getRoot(), 4096, writes, tries, true, true);
      System.out.println();
      System.out.println("TotalAvg = " + totalAvg);
      long nanoTime = SyncCalculation.toNanos(totalAvg, writes);
      System.out.println("nanoTime avg = " + nanoTime);
      Assert.assertEquals(0, LibaioContext.getTotalMaxIO());

   }

   @Test
   public void testSimpleRun() throws Exception {
      String queues = "q1,t2";
      String topics = "t1,t2";

      // This is usually set when run from the command line via artemis.profile
      Run.setEmbedded(true);
      Artemis.main("create", temporaryFolder.getRoot().getAbsolutePath(), "--force", "--silent", "--no-web", "--queues", queues, "--topics", topics, "--no-autotune", "--require-login");
      System.setProperty("artemis.instance", temporaryFolder.getRoot().getAbsolutePath());
      // Some exceptions may happen on the initialization, but they should be ok on start the basic core protocol
      Artemis.internalExecute("run");

      try {
         try (ServerLocator locator = ServerLocatorImpl.newLocator("tcp://localhost:61616");
              ClientSessionFactory factory = locator.createSessionFactory();
              ClientSession coreSession = factory.createSession("admin", "admin", false, true, true, false, 0)) {
            for (String str : queues.split(",")) {
               ClientSession.QueueQuery queryResult = coreSession.queueQuery(SimpleString.toSimpleString("jms.queue." + str));
               Assert.assertTrue("Couldn't find queue " + str, queryResult.isExists());
            }
            for (String str : topics.split(",")) {
               ClientSession.QueueQuery queryResult = coreSession.queueQuery(SimpleString.toSimpleString("jms.topic." + str));
               Assert.assertTrue("Couldn't find topic " + str, queryResult.isExists());
            }
         }

         Assert.assertEquals(Integer.valueOf(100), Artemis.internalExecute("producer", "--message-count", "100", "--verbose", "--user", "admin", "--password", "admin"));
         Assert.assertEquals(Integer.valueOf(100), Artemis.internalExecute("consumer", "--verbose", "--break-on-null", "--receive-timeout", "100", "--user", "admin", "--password", "admin"));

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

         Assert.assertEquals(Integer.valueOf(1), Artemis.internalExecute("browser", "--txt-size", "50", "--verbose", "--filter", "fruit='banana'", "--user", "admin", "--password", "admin"));

         Assert.assertEquals(Integer.valueOf(100), Artemis.internalExecute("browser", "--txt-size", "50", "--verbose", "--filter", "fruit='orange'", "--user", "admin", "--password", "admin"));

         Assert.assertEquals(Integer.valueOf(101), Artemis.internalExecute("browser", "--txt-size", "50", "--verbose", "--user", "admin", "--password", "admin"));

         // should only receive 10 messages on browse as I'm setting messageCount=10
         Assert.assertEquals(Integer.valueOf(10), Artemis.internalExecute("browser", "--txt-size", "50", "--verbose", "--message-count", "10", "--user", "admin", "--password", "admin"));

         // Nothing was consumed until here as it was only browsing, check it's receiving again
         Assert.assertEquals(Integer.valueOf(1), Artemis.internalExecute("consumer", "--txt-size", "50", "--verbose", "--break-on-null", "--receive-timeout", "100", "--filter", "fruit='banana'", "--user", "admin", "--password", "admin"));

         // Checking it was acked before
         Assert.assertEquals(Integer.valueOf(100), Artemis.internalExecute("consumer", "--txt-size", "50", "--verbose", "--break-on-null", "--receive-timeout", "100", "--user", "admin", "--password", "admin"));
      }
      finally {
         stopServer();
      }
   }

   @Test
   public void testAnonymousAutoCreate() throws Exception {
      // This is usually set when run from the command line via artemis.profile

      Run.setEmbedded(true);
      Artemis.main("create", temporaryFolder.getRoot().getAbsolutePath(), "--force", "--silent", "--no-web", "--no-autotune", "--allow-anonymous", "--user", "a", "--password", "a", "--role", "a");
      System.setProperty("artemis.instance", temporaryFolder.getRoot().getAbsolutePath());
      // Some exceptions may happen on the initialization, but they should be ok on start the basic core protocol
      Artemis.internalExecute("run");

      try {
         Assert.assertEquals(Integer.valueOf(100), Artemis.internalExecute("producer", "--message-count", "100"));
         Assert.assertEquals(Integer.valueOf(100), Artemis.internalExecute("consumer", "--message-count", "100"));
      }
      finally {
         stopServer();
      }
   }

   private void testCli(String... args) {
      try {
         Artemis.main(args);
      }
      catch (Exception e) {
         e.printStackTrace();
         Assert.fail("Exception caught " + e.getMessage());
      }
   }

   public boolean isWindows() {
      return System.getProperty("os.name", "null").toLowerCase().indexOf("win") >= 0;
   }

   private void stopServer() throws Exception {
      Artemis.internalExecute("stop");
      Assert.assertTrue(Run.latchRunning.await(5, TimeUnit.SECONDS));
      Assert.assertEquals(0, LibaioContext.getTotalMaxIO());
   }


}
