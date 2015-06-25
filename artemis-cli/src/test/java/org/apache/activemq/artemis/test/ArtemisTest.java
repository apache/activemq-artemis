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
package org.apache.activemq.artemis.test;

import javax.jms.Connection;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.File;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.cli.Artemis;
import org.apache.activemq.artemis.cli.commands.Run;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQDestination;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test to validate that the CLI doesn't throw improper exceptions when invoked.
 */
public class ArtemisTest
{
   @Rule
   public TemporaryFolder temporaryFolder;

   public ArtemisTest()
   {
      File parent = new File("./target/tmp");
      parent.mkdirs();
      temporaryFolder = new TemporaryFolder(parent);
   }


   @After
   public void cleanup()
   {
      System.clearProperty("artemis.instance");
      Run.setEmbedded(false);
   }

   @Test
   public void invalidCliDoesntThrowException()
   {
      testCli("create");
   }

   @Test
   public void invalidPathDoesntThrowException()
   {
      testCli("create","/rawr");
   }

   @Test
   public void testSimpleRun() throws Exception
   {
      Run.setEmbedded(true);
      Artemis.main("create", temporaryFolder.getRoot().getAbsolutePath(), "--force", "--silent-input", "--no-web");
      System.setProperty("artemis.instance", temporaryFolder.getRoot().getAbsolutePath());
      // Some exceptions may happen on the initialization, but they should be ok on start the basic core protocol
      Artemis.main("run");
      Assert.assertEquals(Integer.valueOf(70), Artemis.execute("produce", "--txSize", "50", "--messageCount", "70", "--verbose"));
      Assert.assertEquals(Integer.valueOf(70), Artemis.execute("consume", "--txSize", "50", "--verbose", "--breakOnNull", "--receiveTimeout", "100"));

      ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("tcp://localhost:61616");
      Connection connection = cf.createConnection();
      Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
      MessageProducer producer = session.createProducer(ActiveMQDestination.createDestination("queue://TEST", ActiveMQDestination.QUEUE_TYPE));

      TextMessage message = session.createTextMessage("Banana");
      message.setStringProperty("fruit", "banana");
      producer.send(message);

      for (int i = 0; i < 100; i++)
      {
         message = session.createTextMessage("orange");
         message.setStringProperty("fruit", "orange");
         producer.send(message);
      }
      session.commit();

      connection.close();
      cf.close();

      Assert.assertEquals(Integer.valueOf(1), Artemis.execute("browse", "--txSize", "50", "--verbose", "--filter", "fruit='banana'"));

      Assert.assertEquals(Integer.valueOf(100), Artemis.execute("browse", "--txSize", "50", "--verbose", "--filter", "fruit='orange'"));

      Assert.assertEquals(Integer.valueOf(101), Artemis.execute("browse", "--txSize", "50", "--verbose"));

      // should only receive 10 messages on browse as I'm setting messageCount=10
      Assert.assertEquals(Integer.valueOf(10), Artemis.execute("browse", "--txSize", "50", "--verbose", "--messageCount", "10"));

      // Nothing was consumed until here as it was only browsing, check it's receiving again
      Assert.assertEquals(Integer.valueOf(1), Artemis.execute("consume", "--txSize", "50", "--verbose", "--breakOnNull", "--receiveTimeout", "100", "--filter", "fruit='banana'"));

      // Checking it was acked before
      Assert.assertEquals(Integer.valueOf(100), Artemis.execute("consume", "--txSize", "50", "--verbose",  "--breakOnNull", "--receiveTimeout", "100"));

      Artemis.execute("stop");
      Assert.assertTrue(Run.latchRunning.await(5, TimeUnit.SECONDS));

   }

   private void testCli(String... args)
   {
      try
      {
         Artemis.main(args);
      }
      catch (Exception e)
      {
         e.printStackTrace();
         Assert.fail("Exception caught " + e.getMessage());
      }
   }
}
