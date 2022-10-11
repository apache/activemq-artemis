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

package org.apache.activemq.artemis.tests.soak.paging;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.paging.impl.PagingStoreImpl;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.logs.AssertionLoggerHandler;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.artemis.utils.SpawnedVMSupport;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PageCleanup should still be able to perform it well.
 * */
public class MegaCleanerPagingTest extends ActiveMQTestBase {

   // set this to true to have the test to be called directly
   // useful for debugging, keeping it false
   private static final boolean DIRECT_CALL = false;

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private static final int OK = 35; // Abitrary code the spawn must return if ok.

   @Test
   public void testCleanup() throws Throwable {
      testCleanup(false);
   }

   @Test
   public void testCleanupMidstream() throws Throwable {
      testCleanup(true);
   }

   public void testCleanup(boolean midstream) throws Throwable {

      if (DIRECT_CALL) {
         internalTest(midstream);
      } else {
         // Using a spawn to limit memory consumption to the test
         Process process = SpawnedVMSupport.spawnVM(MegaCleanerPagingTest.class.getName(), new String[]{"-Xmx512M"}, getTestDir(), "" + midstream);
         logger.debug("process PID {}", process.pid());
         Assert.assertTrue(process.waitFor(10, TimeUnit.MINUTES));
         Assert.assertEquals(OK, process.exitValue());
      }
   }

   // I am using a separate VM to limit memory..
   // and the test will pass the parameters needed for the JUnit Class to be able to proceed
   public static void main(String[] arg) {
      try {
         MegaCleanerPagingTest megaCleanerPagingTest = new MegaCleanerPagingTest();
         megaCleanerPagingTest.setTestDir(arg[0]);
         boolean midstream = Boolean.parseBoolean(arg[1]);
         megaCleanerPagingTest.internalTest(midstream);
         System.exit(OK);
      } catch (Throwable e) {
         e.printStackTrace();
         logger.warn(e.getMessage(), e);
         System.exit(-1);
      }
   }

   public void internalTest(boolean midstream) throws Throwable {
      ActiveMQServer server = createServer(true, true);
      server.getConfiguration().clearAddressSettings();
      server.getConfiguration().addAddressSetting("#", new AddressSettings().setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE).setMaxSizeMessages(1000).setPageSizeBytes(10 * 1024 * 1024));
      server.start();

      // if I didn't limit the memory on this test, the NUMBER_OF_MESSAGES would have to be something huge such as 500_000, and that could be a moving target
      // if more memory is set to the JUNIT Runner.
      // This is the main reason we limit memory on this test
      int NUMBER_OF_MESSAGES = 50_000;

      String queueName = "testPageAndDepage";

      server.addAddressInfo(new AddressInfo(queueName).addRoutingType(RoutingType.ANYCAST).setAutoCreated(false));
      server.createQueue(new QueueConfiguration(queueName).setRoutingType(RoutingType.ANYCAST));

      ConnectionFactory factory = CFUtil.createConnectionFactory("core", "tcp://localhost:61616");
      Connection connection = factory.createConnection();
      Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
      Queue queue = session.createQueue(queueName);
      MessageProducer producer = session.createProducer(queue);


      org.apache.activemq.artemis.core.server.Queue serverQueue = server.locateQueue(queueName);
      Assert.assertNotNull(serverQueue);
      serverQueue.getPagingStore().startPaging();

      ConnectionFactory cf = CFUtil.createConnectionFactory("core", "tcp://localhost:61616?consumerWindowSize=0");
      Assert.assertEquals(0, ((ActiveMQConnectionFactory)cf).getServerLocator().getConsumerWindowSize());
      Connection slowConnection  = cf.createConnection();
      Session slowSession = slowConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      Queue slowQueue = slowSession.createQueue(queueName);
      MessageProducer slowProducer = slowSession.createProducer(slowQueue);
      if (midstream) {
         slowProducer.send(session.createTextMessage("slow"));
      }
      slowConnection.start();
      MessageConsumer slowConsumer = slowSession.createConsumer(slowQueue);
      TextMessage slowMessage;

      if (midstream) {
         slowMessage = (TextMessage) slowConsumer.receive(5000);
         Assert.assertNotNull(slowMessage);
         Assert.assertEquals("slow", slowMessage.getText());
      } else {
         slowMessage = (TextMessage) slowConsumer.receiveNoWait();
         Assert.assertNull(slowMessage);
      }


      final int SIZE = 10 * 1024;
      for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
         producer.send(session.createTextMessage(createBuffer(i, SIZE)));
         if (i % 1000 == 0) {
            logger.debug("sent {} messages", i);
            session.commit();
         }
      }
      session.commit();


      PagingStoreImpl store = (PagingStoreImpl) server.getPagingManager().getPageStore(SimpleString.toSimpleString(queueName));
      store.disableCleanup();

      MessageConsumer consumer = session.createConsumer(queue);
      connection.start();

      for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
         TextMessage message = (TextMessage) consumer.receive(5000);
         Assert.assertNotNull(message);
         Assert.assertEquals(createBuffer(i, SIZE), message.getText());

         if (i % 1000 == 0) {
            logger.debug("received {} messages", i);
            session.commit();
         }
      }
      session.commit();
      Assert.assertNull(consumer.receiveNoWait());
      connection.close();
      slowConnection.close();

      AssertionLoggerHandler.startCapture();
      runAfter(AssertionLoggerHandler::stopCapture);
      store.enableCleanup();
      store.getCursorProvider().scheduleCleanup();
      if (!midstream) {
         Wait.assertFalse(store::isPaging);
      }
      server.stop();
      Assert.assertFalse(AssertionLoggerHandler.findText("AMQ222023")); // error associated with OME
      Assert.assertFalse(AssertionLoggerHandler.findText("AMQ222010")); // critical IO Error
   }


   String createBuffer(int msgNumber, int size) {
      StringBuffer buffer = new StringBuffer();
      buffer.append("message " + msgNumber + " ");
      while (buffer.length() < size) {
         buffer.append(" Lorem Ipsum Whatever it's saying in there... ");
      }
      return buffer.toString();
   }
}