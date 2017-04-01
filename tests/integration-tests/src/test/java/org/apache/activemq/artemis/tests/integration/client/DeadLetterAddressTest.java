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
package org.apache.activemq.artemis.tests.integration.client;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.sun.management.UnixOperatingSystemMXBean;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.MessageHandler;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.integration.IntegrationTestLogger;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

public class DeadLetterAddressTest extends ActiveMQTestBase {

   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   private ActiveMQServer server;

   private ClientSession clientSession;
   private ServerLocator locator;

   @Test
   public void testBasicSend() throws Exception {
      SimpleString dla = new SimpleString("DLA");
      SimpleString qName = new SimpleString("q1");
      SimpleString adName = new SimpleString("ad1");
      AddressSettings addressSettings = new AddressSettings().setMaxDeliveryAttempts(1).setDeadLetterAddress(dla);
      server.getAddressSettingsRepository().addMatch(adName.toString(), addressSettings);
      SimpleString dlq = new SimpleString("DLQ1");
      clientSession.createQueue(dla, dlq, null, false);
      clientSession.createQueue(adName, qName, null, false);
      ClientProducer producer = clientSession.createProducer(adName);
      producer.send(createTextMessage(clientSession, "heyho!"));
      clientSession.start();
      ClientConsumer clientConsumer = clientSession.createConsumer(qName);
      ClientMessage m = clientConsumer.receive(500);
      m.acknowledge();
      Assert.assertNotNull(m);
      Assert.assertEquals(m.getBodyBuffer().readString(), "heyho!");
      // force a cancel
      clientSession.rollback();
      m = clientConsumer.receiveImmediate();
      Assert.assertNull(m);
      clientConsumer.close();
      clientConsumer = clientSession.createConsumer(dlq);
      m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      assertEquals("q1", m.getStringProperty(Message.HDR_ORIGINAL_QUEUE));
      assertEquals("ad1", m.getStringProperty(Message.HDR_ORIGINAL_ADDRESS));
      Assert.assertEquals(m.getBodyBuffer().readString(), "heyho!");
   }

   @Test
   public void testLargeMessageFileLeak() throws Exception {
      OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();

      // only run this on *nix systems which will have the com.sun.management.UnixOperatingSystemMXBean (needed to check open file count)
      Assume.assumeTrue(os instanceof UnixOperatingSystemMXBean);

      long fdBaseline = ((UnixOperatingSystemMXBean) os).getOpenFileDescriptorCount();
      final int SIZE = 2 * 1024;
      SimpleString dla = new SimpleString("DLA");
      SimpleString qName = new SimpleString("q1");
      SimpleString adName = new SimpleString("ad1");

      AddressSettings addressSettings = new AddressSettings().setMaxDeliveryAttempts(1).setDeadLetterAddress(dla);
      server.getAddressSettingsRepository().addMatch(adName.toString(), addressSettings);
      SimpleString dlq = new SimpleString("DLQ1");
      clientSession.createQueue(dla, dlq, null, false);
      clientSession.createQueue(adName, qName, null, false);
      for (int i = 0; i < 10; i++) {
         ClientProducer producer = clientSession.createProducer(adName);
         ClientMessage clientFile = clientSession.createMessage(true);
         clientFile.setBodyInputStream(ActiveMQTestBase.createFakeLargeStream(SIZE));
         producer.send(clientFile);
         clientSession.start();
         ClientConsumer clientConsumer = clientSession.createConsumer(qName);
         ClientMessage m = clientConsumer.receive(500);
         m.acknowledge();
         Assert.assertNotNull(m);

         // force a cancel
         clientSession.rollback();
         m = clientConsumer.receiveImmediate();
         Assert.assertNull(m);
         clientConsumer.close();
      }
      assertTrue("File descriptors are leaking", ((UnixOperatingSystemMXBean) os).getOpenFileDescriptorCount() - fdBaseline <= 0);
   }

   // HORNETQ- 1084
   @Test
   public void testBasicSendWithDLAButNoBinding() throws Exception {
      SimpleString dla = new SimpleString("DLA");
      SimpleString qName = new SimpleString("q1");
      AddressSettings addressSettings = new AddressSettings().setMaxDeliveryAttempts(1).setDeadLetterAddress(dla);
      server.getAddressSettingsRepository().addMatch(qName.toString(), addressSettings);
      //SimpleString dlq = new SimpleString("DLQ1");
      //clientSession.createQueue(dla, dlq, null, false);
      clientSession.createQueue(qName, qName, null, false);
      ClientProducer producer = clientSession.createProducer(qName);
      producer.send(createTextMessage(clientSession, "heyho!"));
      clientSession.start();
      ClientConsumer clientConsumer = clientSession.createConsumer(qName);
      ClientMessage m = clientConsumer.receive(500);
      m.acknowledge();
      Assert.assertNotNull(m);
      Assert.assertEquals(m.getBodyBuffer().readString(), "heyho!");
      // force a cancel
      clientSession.rollback();
      m = clientConsumer.receiveImmediate();
      Assert.assertNull(m);
      clientConsumer.close();
      Queue q = (Queue) server.getPostOffice().getBinding(qName).getBindable();
      Assert.assertEquals(0, q.getDeliveringCount());
   }

   @Test
   public void testBasicSend2times() throws Exception {
      SimpleString dla = new SimpleString("DLA");
      SimpleString qName = new SimpleString("q1");
      AddressSettings addressSettings = new AddressSettings().setMaxDeliveryAttempts(2).setDeadLetterAddress(dla);
      server.getAddressSettingsRepository().addMatch(qName.toString(), addressSettings);
      SimpleString dlq = new SimpleString("DLQ1");
      clientSession.createQueue(dla, dlq, null, false);
      clientSession.createQueue(qName, qName, null, false);
      ClientProducer producer = clientSession.createProducer(qName);
      producer.send(createTextMessage(clientSession, "heyho!"));
      clientSession.start();
      ClientConsumer clientConsumer = clientSession.createConsumer(qName);
      ClientMessage m = clientConsumer.receive(5000);
      m.acknowledge();
      Assert.assertNotNull(m);
      Assert.assertEquals(m.getBodyBuffer().readString(), "heyho!");
      // force a cancel
      clientSession.rollback();
      clientSession.start();
      m = clientConsumer.receive(5000);
      m.acknowledge();
      Assert.assertNotNull(m);
      Assert.assertEquals(m.getBodyBuffer().readString(), "heyho!");
      // force a cancel
      clientSession.rollback();
      m = clientConsumer.receiveImmediate();
      Assert.assertNull(m);
      clientConsumer.close();
      clientConsumer = clientSession.createConsumer(dlq);
      m = clientConsumer.receive(5000);
      Assert.assertNotNull(m);
      Assert.assertEquals(m.getBodyBuffer().readString(), "heyho!");
   }

   @Test
   public void testReceiveWithListeners() throws Exception {
      SimpleString dla = new SimpleString("DLA");
      SimpleString qName = new SimpleString("q1");
      AddressSettings addressSettings = new AddressSettings().setMaxDeliveryAttempts(2).setDeadLetterAddress(dla);
      server.getAddressSettingsRepository().addMatch(qName.toString(), addressSettings);
      SimpleString dlq = new SimpleString("DLQ1");
      clientSession.createQueue(dla, dlq, null, false);
      clientSession.createQueue(qName, qName, null, false);
      ClientProducer producer = clientSession.createProducer(qName);
      producer.send(createTextMessage(clientSession, "heyho!"));
      ClientConsumer clientConsumer = clientSession.createConsumer(qName);
      final CountDownLatch latch = new CountDownLatch(2);
      TestHandler handler = new TestHandler(latch, clientSession);
      clientConsumer.setMessageHandler(handler);
      clientSession.start();
      assertTrue(latch.await(5, TimeUnit.SECONDS));
      assertEquals(handler.count, 2);
      clientConsumer = clientSession.createConsumer(dlq);
      Message m = clientConsumer.receive(5000);
      Assert.assertNotNull(m);
      Assert.assertEquals(m.getBodyBuffer().readString(), "heyho!");
   }

   class TestHandler implements MessageHandler {

      private final CountDownLatch latch;
      int count = 0;

      private final ClientSession clientSession;

      TestHandler(CountDownLatch latch, ClientSession clientSession) {
         this.latch = latch;
         this.clientSession = clientSession;
      }

      @Override
      public void onMessage(ClientMessage message) {
         count++;
         latch.countDown();
         try {
            clientSession.rollback(true);
         } catch (ActiveMQException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
         }
         throw new RuntimeException();
      }
   }

   @Test
   public void testBasicSendToMultipleQueues() throws Exception {
      SimpleString dla = new SimpleString("DLA");
      SimpleString qName = new SimpleString("q1");
      AddressSettings addressSettings = new AddressSettings().setMaxDeliveryAttempts(1).setDeadLetterAddress(dla);
      server.getAddressSettingsRepository().addMatch(qName.toString(), addressSettings);
      SimpleString dlq = new SimpleString("DLQ1");
      SimpleString dlq2 = new SimpleString("DLQ2");
      clientSession.createQueue(dla, dlq, null, false);
      clientSession.createQueue(dla, dlq2, null, false);
      clientSession.createQueue(qName, qName, null, false);
      ClientProducer producer = clientSession.createProducer(qName);
      producer.send(createTextMessage(clientSession, "heyho!"));
      clientSession.start();
      ClientConsumer clientConsumer = clientSession.createConsumer(qName);
      ClientMessage m = clientConsumer.receive(500);
      m.acknowledge();
      Assert.assertNotNull(m);
      Assert.assertEquals(m.getBodyBuffer().readString(), "heyho!");
      // force a cancel
      clientSession.rollback();
      m = clientConsumer.receiveImmediate();
      Assert.assertNull(m);
      clientConsumer.close();
      clientConsumer = clientSession.createConsumer(dlq);
      m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      m.acknowledge();
      Assert.assertEquals(m.getBodyBuffer().readString(), "heyho!");
      clientConsumer.close();
      clientConsumer = clientSession.createConsumer(dlq2);
      m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      m.acknowledge();
      Assert.assertEquals(m.getBodyBuffer().readString(), "heyho!");
      clientConsumer.close();
   }

   @Test
   public void testBasicSendToNoQueue() throws Exception {
      SimpleString qName = new SimpleString("q1");
      AddressSettings addressSettings = new AddressSettings().setMaxDeliveryAttempts(1);
      server.getAddressSettingsRepository().addMatch(qName.toString(), addressSettings);
      clientSession.createQueue(qName, qName, null, false);
      ClientProducer producer = clientSession.createProducer(qName);
      producer.send(createTextMessage(clientSession, "heyho!"));
      clientSession.start();
      ClientConsumer clientConsumer = clientSession.createConsumer(qName);
      ClientMessage m = clientConsumer.receive(500);
      m.acknowledge();
      Assert.assertNotNull(m);
      Assert.assertEquals(m.getBodyBuffer().readString(), "heyho!");
      // force a cancel
      clientSession.rollback();
      m = clientConsumer.receiveImmediate();
      Assert.assertNull(m);
      clientConsumer.close();
   }

   @Test
   public void testHeadersSet() throws Exception {
      final int MAX_DELIVERIES = 16;
      final int NUM_MESSAGES = 5;
      SimpleString dla = new SimpleString("DLA");
      SimpleString qName = new SimpleString("q1");
      AddressSettings addressSettings = new AddressSettings().setMaxDeliveryAttempts(MAX_DELIVERIES).setDeadLetterAddress(dla);
      server.getAddressSettingsRepository().addMatch(qName.toString(), addressSettings);
      SimpleString dlq = new SimpleString("DLQ1");
      clientSession.createQueue(dla, dlq, null, false);
      clientSession.createQueue(qName, qName, null, false);
      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory sessionFactory = createSessionFactory(locator);
      ClientSession sendSession = sessionFactory.createSession(false, true, true);
      ClientProducer producer = sendSession.createProducer(qName);
      Map<String, Long> origIds = new HashMap<>();

      for (int i = 0; i < NUM_MESSAGES; i++) {
         ClientMessage tm = createTextMessage(clientSession, "Message:" + i);
         producer.send(tm);
      }

      ClientConsumer clientConsumer = clientSession.createConsumer(qName);
      clientSession.start();

      for (int i = 0; i < MAX_DELIVERIES; i++) {
         for (int j = 0; j < NUM_MESSAGES; j++) {
            ClientMessage tm = clientConsumer.receive(1000);

            Assert.assertNotNull(tm);
            tm.acknowledge();
            if (i == 0) {
               origIds.put("Message:" + j, tm.getMessageID());
            }
            Assert.assertEquals("Message:" + j, tm.getBodyBuffer().readString());
         }
         clientSession.rollback();
      }

      long timeout = System.currentTimeMillis() + 5000;

      // DLA transfer is asynchronous fired on the rollback
      while (System.currentTimeMillis() < timeout && getMessageCount(((Queue) server.getPostOffice().getBinding(qName).getBindable())) != 0) {
         Thread.sleep(1);
      }

      Assert.assertEquals(0, getMessageCount(((Queue) server.getPostOffice().getBinding(qName).getBindable())));
      ClientMessage m = clientConsumer.receiveImmediate();
      Assert.assertNull(m);
      // All the messages should now be in the DLQ

      ClientConsumer cc3 = clientSession.createConsumer(dlq);

      for (int i = 0; i < NUM_MESSAGES; i++) {
         ClientMessage tm = cc3.receive(1000);

         Assert.assertNotNull(tm);

         String text = tm.getBodyBuffer().readString();
         Assert.assertEquals("Message:" + i, text);

         // Check the headers
         SimpleString origDest = (SimpleString) tm.getObjectProperty(Message.HDR_ORIGINAL_ADDRESS);

         Long origMessageId = (Long) tm.getObjectProperty(Message.HDR_ORIG_MESSAGE_ID);

         Assert.assertEquals(qName, origDest);

         Long origId = origIds.get(text);

         Assert.assertEquals(origId, origMessageId);
      }

      sendSession.close();

   }

   @Test
   public void testDeadlLetterAddressWithDefaultAddressSettings() throws Exception {
      int deliveryAttempt = 3;

      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString deadLetterAddress = RandomUtil.randomSimpleString();
      SimpleString deadLetterQueue = RandomUtil.randomSimpleString();
      AddressSettings addressSettings = new AddressSettings().setMaxDeliveryAttempts(deliveryAttempt).setDeadLetterAddress(deadLetterAddress);
      server.getAddressSettingsRepository().setDefault(addressSettings);

      clientSession.createQueue(address, queue, false);
      clientSession.createQueue(deadLetterAddress, deadLetterQueue, false);

      ClientProducer producer = clientSession.createProducer(address);
      ClientMessage clientMessage = createTextMessage(clientSession, "heyho!");
      producer.send(clientMessage);

      clientSession.start();
      ClientConsumer clientConsumer = clientSession.createConsumer(queue);
      for (int i = 0; i < deliveryAttempt; i++) {
         ClientMessage m = clientConsumer.receive(500);
         Assert.assertNotNull(m);
         DeadLetterAddressTest.log.info("i is " + i);
         DeadLetterAddressTest.log.info("delivery cout is " + m.getDeliveryCount());
         Assert.assertEquals(i + 1, m.getDeliveryCount());
         m.acknowledge();
         clientSession.rollback();
      }
      ClientMessage m = clientConsumer.receive(500);
      Assert.assertNull("not expecting a message", m);
      clientConsumer.close();

      clientConsumer = clientSession.createConsumer(deadLetterQueue);
      m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals(m.getBodyBuffer().readString(), "heyho!");
   }

   @Test
   public void testDeadlLetterAddressWithWildcardAddressSettings() throws Exception {
      int deliveryAttempt = 3;

      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString deadLetterAddress = RandomUtil.randomSimpleString();
      SimpleString deadLetterQueue = RandomUtil.randomSimpleString();
      AddressSettings addressSettings = new AddressSettings().setMaxDeliveryAttempts(deliveryAttempt).setDeadLetterAddress(deadLetterAddress);
      server.getAddressSettingsRepository().addMatch("*", addressSettings);

      clientSession.createQueue(address, queue, false);
      clientSession.createQueue(deadLetterAddress, deadLetterQueue, false);

      ClientProducer producer = clientSession.createProducer(address);
      ClientMessage clientMessage = createTextMessage(clientSession, "heyho!");
      producer.send(clientMessage);

      clientSession.start();
      ClientConsumer clientConsumer = clientSession.createConsumer(queue);
      for (int i = 0; i < deliveryAttempt; i++) {
         ClientMessage m = clientConsumer.receive(500);
         Assert.assertNotNull(m);
         Assert.assertEquals(i + 1, m.getDeliveryCount());
         m.acknowledge();
         clientSession.rollback();
      }
      ClientMessage m = clientConsumer.receiveImmediate();
      Assert.assertNull(m);
      clientConsumer.close();

      clientConsumer = clientSession.createConsumer(deadLetterQueue);
      m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals(m.getBodyBuffer().readString(), "heyho!");
   }

   @Test
   public void testDeadLetterAddressWithOverridenSublevelAddressSettings() throws Exception {
      int defaultDeliveryAttempt = 3;
      int specificeDeliveryAttempt = defaultDeliveryAttempt + 1;

      SimpleString address = new SimpleString("prefix.address");
      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString defaultDeadLetterAddress = RandomUtil.randomSimpleString();
      SimpleString defaultDeadLetterQueue = RandomUtil.randomSimpleString();
      SimpleString specificDeadLetterAddress = RandomUtil.randomSimpleString();
      SimpleString specificDeadLetterQueue = RandomUtil.randomSimpleString();

      AddressSettings defaultAddressSettings = new AddressSettings().setMaxDeliveryAttempts(defaultDeliveryAttempt).setDeadLetterAddress(defaultDeadLetterAddress);
      server.getAddressSettingsRepository().addMatch("*", defaultAddressSettings);
      AddressSettings specificAddressSettings = new AddressSettings().setMaxDeliveryAttempts(specificeDeliveryAttempt).setDeadLetterAddress(specificDeadLetterAddress);
      server.getAddressSettingsRepository().addMatch(address.toString(), specificAddressSettings);

      clientSession.createQueue(address, queue, false);
      clientSession.createQueue(defaultDeadLetterAddress, defaultDeadLetterQueue, false);
      clientSession.createQueue(specificDeadLetterAddress, specificDeadLetterQueue, false);

      ClientProducer producer = clientSession.createProducer(address);
      ClientMessage clientMessage = createTextMessage(clientSession, "heyho!");
      producer.send(clientMessage);

      clientSession.start();
      ClientConsumer clientConsumer = clientSession.createConsumer(queue);
      ClientConsumer defaultDeadLetterConsumer = clientSession.createConsumer(defaultDeadLetterQueue);
      ClientConsumer specificDeadLetterConsumer = clientSession.createConsumer(specificDeadLetterQueue);

      for (int i = 0; i < defaultDeliveryAttempt; i++) {
         ClientMessage m = clientConsumer.receive(500);
         Assert.assertNotNull(m);
         Assert.assertEquals(i + 1, m.getDeliveryCount());
         m.acknowledge();
         clientSession.rollback();
      }

      Assert.assertNull(defaultDeadLetterConsumer.receiveImmediate());
      Assert.assertNull(specificDeadLetterConsumer.receiveImmediate());

      // one more redelivery attempt:
      ClientMessage m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals(specificeDeliveryAttempt, m.getDeliveryCount());
      m.acknowledge();
      clientSession.rollback();

      Assert.assertNull(defaultDeadLetterConsumer.receiveImmediate());
      Assert.assertNotNull(specificDeadLetterConsumer.receive(500));
   }

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      server = addServer(ActiveMQServers.newActiveMQServer(createDefaultInVMConfig(), true));
      server.start();
      // then we create a client as normal
      locator = createInVMNonHALocator();
      locator.setMinLargeMessageSize(1024);
      ClientSessionFactory sessionFactory = createSessionFactory(locator);
      clientSession = addClientSession(sessionFactory.createSession(false, true, false));
   }
}
