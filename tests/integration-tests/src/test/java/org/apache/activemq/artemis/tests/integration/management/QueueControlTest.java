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
package org.apache.activemq.artemis.tests.integration.management;

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
import org.apache.activemq.artemis.api.core.management.ActiveMQServerControl;
import org.apache.activemq.artemis.api.core.management.CoreNotificationType;
import org.apache.activemq.artemis.api.core.management.DayCounterInfo;
import org.apache.activemq.artemis.api.core.management.MessageCounterInfo;
import org.apache.activemq.artemis.api.core.management.ObjectNameBuilder;
import org.apache.activemq.artemis.api.core.management.QueueControl;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.message.impl.MessageImpl;
import org.apache.activemq.artemis.core.messagecounter.impl.MessageCounterManagerImpl;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.integration.jms.server.management.JMSUtil;
import org.apache.activemq.artemis.tests.util.RandomUtil;
import org.apache.activemq.artemis.utils.json.JSONArray;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.management.Notification;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class QueueControlTest extends ManagementTestBase {

   private ActiveMQServer server;
   private ClientSession session;
   private ServerLocator locator;

   @Test
   public void testAttributes() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString filter = new SimpleString("color = 'blue'");
      boolean durable = RandomUtil.randomBoolean();

      session.createQueue(address, queue, filter, durable);

      QueueControl queueControl = createManagementControl(address, queue);
      Assert.assertEquals(queue.toString(), queueControl.getName());
      Assert.assertEquals(address.toString(), queueControl.getAddress());
      Assert.assertEquals(filter.toString(), queueControl.getFilter());
      Assert.assertEquals(durable, queueControl.isDurable());
      Assert.assertEquals(false, queueControl.isTemporary());

      session.deleteQueue(queue);
   }

   @Test
   public void testGetNullFilter() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, null, false);

      QueueControl queueControl = createManagementControl(address, queue);
      Assert.assertEquals(queue.toString(), queueControl.getName());
      Assert.assertEquals(null, queueControl.getFilter());

      session.deleteQueue(queue);
   }

   @Test
   public void testGetDeadLetterAddress() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();
      final SimpleString deadLetterAddress = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, null, false);

      QueueControl queueControl = createManagementControl(address, queue);
      Assert.assertNull(queueControl.getDeadLetterAddress());

      server.getAddressSettingsRepository().addMatch(address.toString(), new AddressSettings() {
         private static final long serialVersionUID = -4919035864731465338L;

         @Override
         public SimpleString getDeadLetterAddress() {
            return deadLetterAddress;
         }
      });

      Assert.assertEquals(deadLetterAddress.toString(), queueControl.getDeadLetterAddress());

      session.deleteQueue(queue);
   }

   @Test
   public void testSetDeadLetterAddress() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();
      String deadLetterAddress = RandomUtil.randomString();

      session.createQueue(address, queue, null, false);

      QueueControl queueControl = createManagementControl(address, queue);

      AddressSettings addressSettings = new AddressSettings().setDeadLetterAddress(new SimpleString(deadLetterAddress));
      server.getAddressSettingsRepository().addMatch(address.toString(), addressSettings);

      Assert.assertEquals(deadLetterAddress, queueControl.getDeadLetterAddress());

      session.deleteQueue(queue);
   }

   @Test
   public void testGetExpiryAddress() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();
      final SimpleString expiryAddress = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, null, false);

      QueueControl queueControl = createManagementControl(address, queue);
      Assert.assertNull(queueControl.getExpiryAddress());

      server.getAddressSettingsRepository().addMatch(address.toString(), new AddressSettings() {
         private static final long serialVersionUID = 6745306517827764680L;

         @Override
         public SimpleString getExpiryAddress() {
            return expiryAddress;
         }
      });

      Assert.assertEquals(expiryAddress.toString(), queueControl.getExpiryAddress());

      session.deleteQueue(queue);
   }

   @Test
   public void testSetExpiryAddress() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();
      String expiryAddress = RandomUtil.randomString();

      session.createQueue(address, queue, null, false);

      QueueControl queueControl = createManagementControl(address, queue);

      AddressSettings addressSettings = new AddressSettings().setExpiryAddress(new SimpleString(expiryAddress));
      server.getAddressSettingsRepository().addMatch(address.toString(), addressSettings);

      Assert.assertEquals(expiryAddress, queueControl.getExpiryAddress());

      Queue serverqueue = server.locateQueue(queue);
      assertEquals(expiryAddress, serverqueue.getExpiryAddress().toString());

      session.deleteQueue(queue);
   }

   @Test
   public void testGetConsumerCount() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, null, false);

      QueueControl queueControl = createManagementControl(address, queue);

      Assert.assertEquals(0, queueControl.getConsumerCount());

      ClientConsumer consumer = session.createConsumer(queue);
      Assert.assertEquals(1, queueControl.getConsumerCount());

      consumer.close();
      Assert.assertEquals(0, queueControl.getConsumerCount());

      session.deleteQueue(queue);
   }

   @Test
   public void testGetConsumerJSON() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, null, false);

      QueueControl queueControl = createManagementControl(address, queue);

      Assert.assertEquals(0, queueControl.getConsumerCount());

      ClientConsumer consumer = session.createConsumer(queue);
      Assert.assertEquals(1, queueControl.getConsumerCount());

      System.out.println("Consumers: " + queueControl.listConsumersAsJSON());

      JSONArray obj = new JSONArray(queueControl.listConsumersAsJSON());

      assertEquals(1, obj.length());

      consumer.close();
      Assert.assertEquals(0, queueControl.getConsumerCount());

      obj = new JSONArray(queueControl.listConsumersAsJSON());

      assertEquals(0, obj.length());

      session.deleteQueue(queue);
   }

   @Test
   public void testGetMessageCount() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, null, false);

      QueueControl queueControl = createManagementControl(address, queue);
      Assert.assertEquals(0, getMessageCount(queueControl));

      ClientProducer producer = session.createProducer(address);
      producer.send(session.createMessage(false));
      Assert.assertEquals(1, getMessageCount(queueControl));

      consumeMessages(1, session, queue);

      Assert.assertEquals(0, getMessageCount(queueControl));

      session.deleteQueue(queue);
   }

   @Test
   public void testGetFirstMessage() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, null, false);

      QueueControl queueControl = createManagementControl(address, queue);
      Assert.assertEquals(0, getMessageCount(queueControl));

      // It's empty, so it's supposed to be like this
      assertEquals("[{}]", queueControl.getFirstMessageAsJSON());

      long beforeSend = System.currentTimeMillis();
      ClientProducer producer = session.createProducer(address);
      producer.send(session.createMessage(false).putStringProperty("x", "valueX").putStringProperty("y", "valueY"));

      System.out.println("first:" + queueControl.getFirstMessageAsJSON());

      long firstMessageTimestamp = queueControl.getFirstMessageTimestamp();
      System.out.println("first message timestamp: " + firstMessageTimestamp);
      assertTrue(beforeSend <= firstMessageTimestamp);
      assertTrue(firstMessageTimestamp <= System.currentTimeMillis());

      long firstMessageAge = queueControl.getFirstMessageAge();
      System.out.println("first message age: " + firstMessageAge);
      assertTrue(firstMessageAge <= (System.currentTimeMillis() - firstMessageTimestamp));

      session.deleteQueue(queue);
   }

   @Test
   public void testGetMessagesAdded() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, null, false);

      QueueControl queueControl = createManagementControl(address, queue);
      Assert.assertEquals(0, getMessagesAdded(queueControl));

      ClientProducer producer = session.createProducer(address);
      producer.send(session.createMessage(false));
      Assert.assertEquals(1, getMessagesAdded(queueControl));
      producer.send(session.createMessage(false));
      Assert.assertEquals(2, getMessagesAdded(queueControl));

      consumeMessages(2, session, queue);

      Assert.assertEquals(2, getMessagesAdded(queueControl));

      session.deleteQueue(queue);
   }

   @Test
   public void testGetMessagesAcknowledged() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, null, false);

      QueueControl queueControl = createManagementControl(address, queue);
      Assert.assertEquals(0, queueControl.getMessagesAcknowledged());

      ClientProducer producer = session.createProducer(address);
      producer.send(session.createMessage(false));
      consumeMessages(1, session, queue);
      Assert.assertEquals(1, queueControl.getMessagesAcknowledged());
      producer.send(session.createMessage(false));
      consumeMessages(1, session, queue);
      Assert.assertEquals(2, queueControl.getMessagesAcknowledged());

      //      ManagementTestBase.consumeMessages(2, session, queue);

      //      Assert.assertEquals(2, getMessagesAdded(queueControl));

      session.deleteQueue(queue);
   }

   @Test
   public void testGetScheduledCount() throws Exception {
      long delay = 500;
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, null, false);

      QueueControl queueControl = createManagementControl(address, queue);
      Assert.assertEquals(0, queueControl.getScheduledCount());

      ClientProducer producer = session.createProducer(address);
      ClientMessage message = session.createMessage(false);
      message.putLongProperty(Message.HDR_SCHEDULED_DELIVERY_TIME, System.currentTimeMillis() + delay);
      producer.send(message);

      long timeout = System.currentTimeMillis() + 5000;
      while (timeout > System.currentTimeMillis() && queueControl.getScheduledCount() != 1) {
         Thread.sleep(100);
      }

      Assert.assertEquals(1, queueControl.getScheduledCount());
      consumeMessages(0, session, queue);

      Thread.sleep(delay * 2);

      Assert.assertEquals(0, queueControl.getScheduledCount());
      consumeMessages(1, session, queue);

      session.deleteQueue(queue);
   }

   //https://issues.jboss.org/browse/HORNETQ-1231
   @Test
   public void testListDeliveringMessagesWithRASession() throws Exception {
      ServerLocator locator1 = createInVMNonHALocator().setBlockOnNonDurableSend(true).setConsumerWindowSize(10240).setAckBatchSize(0);
      ClientSessionFactory sf = locator1.createSessionFactory();
      final ClientSession transSession = sf.createSession(false, true, false);
      ClientConsumer consumer = null;
      SimpleString queue = null;
      int numMsg = 10;

      try {
         // a session from RA does this
         transSession.addMetaData("resource-adapter", "inbound");
         transSession.addMetaData("jms-session", "");

         SimpleString address = RandomUtil.randomSimpleString();
         queue = RandomUtil.randomSimpleString();

         transSession.createQueue(address, queue, null, false);

         final QueueControl queueControl = createManagementControl(address, queue);

         ClientProducer producer = transSession.createProducer(address);

         for (int i = 0; i < numMsg; i++) {
            ClientMessage message = transSession.createMessage(false);
            message.putIntProperty(new SimpleString("seqno"), i);
            producer.send(message);
         }

         consumer = transSession.createConsumer(queue);
         transSession.start();

         /**
          * the following latches are used to make sure that
          *
          * 1. the first call on queueControl happens after the
          * first message arrived at the message handler.
          *
          * 2. the message handler wait on the first message until
          * the queueControl returns the right/wrong result.
          *
          * 3. the test exits after all messages are received.
          *
          */
         final CountDownLatch latch1 = new CountDownLatch(1);
         final CountDownLatch latch2 = new CountDownLatch(1);
         final CountDownLatch latch3 = new CountDownLatch(10);

         consumer.setMessageHandler(new MessageHandler() {

            @Override
            public void onMessage(ClientMessage message) {
               try {
                  message.acknowledge();
               }
               catch (ActiveMQException e1) {
                  e1.printStackTrace();
               }
               latch1.countDown();
               try {
                  latch2.await(10, TimeUnit.SECONDS);
               }
               catch (InterruptedException e) {
                  e.printStackTrace();
               }
               latch3.countDown();
            }
         });

         latch1.await(10, TimeUnit.SECONDS);
         //now we know the ack of the message is sent but to make sure
         //the server has received it, we try 5 times
         int n = 0;
         for (int i = 0; i < 5; i++) {
            Thread.sleep(1000);
            String jsonStr = queueControl.listDeliveringMessagesAsJSON();

            n = countOccurrencesOf(jsonStr, "seqno");

            if (n == numMsg) {
               break;
            }
         }

         assertEquals(numMsg, n);

         latch2.countDown();

         latch3.await(10, TimeUnit.SECONDS);

         transSession.commit();
      }
      finally {
         consumer.close();
         transSession.deleteQueue(queue);
         transSession.close();
         locator1.close();
      }
   }

   @Test
   public void testListDeliveringMessages() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();
      int intValue = RandomUtil.randomInt();
      session.createQueue(address, queue, null, false);

      Queue srvqueue = server.locateQueue(queue);

      QueueControl queueControl = createManagementControl(address, queue);

      ClientProducer producer = session.createProducer(address);
      ClientMessage message = session.createMessage(false);
      message.putIntProperty(new SimpleString("key"), intValue);
      producer.send(message);
      producer.send(session.createMessage(false));

      ClientConsumer consumer = session.createConsumer(queue);
      session.start();
      ClientMessage msgRec = consumer.receive(5000);
      assertNotNull(msgRec);
      assertEquals(msgRec.getIntProperty("key").intValue(), intValue);

      ClientSessionFactory sf2 = createSessionFactory(locator);
      ClientSession session2 = sf2.createSession(false, true, false);
      ClientConsumer consumer2 = session2.createConsumer(queue);
      session2.start();
      ClientMessage msgRec2 = consumer2.receive(5000);
      assertNotNull(msgRec2);

      assertEquals(2, srvqueue.getDeliveringCount());
      assertEquals(2, srvqueue.getConsumerCount());

      System.out.println(queueControl.listDeliveringMessagesAsJSON());

      Map<String, Map<String, Object>[]> deliveringMap = queueControl.listDeliveringMessages();
      assertEquals(2, deliveringMap.size());

      consumer.close();
      consumer2.close();

      session.deleteQueue(queue);
   }

   @Test
   public void testListScheduledMessages() throws Exception {
      long delay = 2000;
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();
      int intValue = RandomUtil.randomInt();
      session.createQueue(address, queue, null, false);

      QueueControl queueControl = createManagementControl(address, queue);

      ClientProducer producer = session.createProducer(address);
      ClientMessage message = session.createMessage(false);
      message.putLongProperty(Message.HDR_SCHEDULED_DELIVERY_TIME, System.currentTimeMillis() + delay);
      message.putIntProperty(new SimpleString("key"), intValue);
      producer.send(message);
      // unscheduled message
      producer.send(session.createMessage(false));

      Map<String, Object>[] messages = queueControl.listScheduledMessages();
      Assert.assertEquals(1, messages.length);
      Assert.assertEquals(intValue, ((Number) messages[0].get("key")).intValue());

      Thread.sleep(delay + 500);

      messages = queueControl.listScheduledMessages();
      Assert.assertEquals(0, messages.length);

      consumeMessages(2, session, queue);

      session.deleteQueue(queue);
   }

   @Test
   public void testListScheduledMessagesAsJSON() throws Exception {
      long delay = 2000;
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();
      int intValue = RandomUtil.randomInt();
      session.createQueue(address, queue, null, false);

      QueueControl queueControl = createManagementControl(address, queue);

      ClientProducer producer = session.createProducer(address);
      ClientMessage message = session.createMessage(false);
      message.putLongProperty(Message.HDR_SCHEDULED_DELIVERY_TIME, System.currentTimeMillis() + delay);
      message.putIntProperty(new SimpleString("key"), intValue);
      producer.send(message);
      // unscheduled message
      producer.send(session.createMessage(false));

      String jsonString = queueControl.listScheduledMessagesAsJSON();
      Assert.assertNotNull(jsonString);
      JSONArray array = new JSONArray(jsonString);
      Assert.assertEquals(1, array.length());
      Assert.assertEquals(intValue, array.getJSONObject(0).get("key"));

      Thread.sleep(delay + 500);

      jsonString = queueControl.listScheduledMessagesAsJSON();
      Assert.assertNotNull(jsonString);
      array = new JSONArray(jsonString);
      Assert.assertEquals(0, array.length());

      consumeMessages(2, session, queue);

      session.deleteQueue(queue);
   }

   @Test
   public void testGetDeliveringCount() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, null, false);

      ClientProducer producer = session.createProducer(address);
      producer.send(session.createMessage(false));

      QueueControl queueControl = createManagementControl(address, queue);
      Assert.assertEquals(0, queueControl.getDeliveringCount());

      ClientConsumer consumer = session.createConsumer(queue);
      ClientMessage message = consumer.receive(500);
      Assert.assertNotNull(message);
      Assert.assertEquals(1, queueControl.getDeliveringCount());

      message.acknowledge();
      session.commit();
      Assert.assertEquals(0, queueControl.getDeliveringCount());

      consumer.close();
      session.deleteQueue(queue);
   }

   @Test
   public void testListMessagesAsJSONWithNullFilter() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();
      int intValue = RandomUtil.randomInt();
      session.createQueue(address, queue, null, false);

      QueueControl queueControl = createManagementControl(address, queue);

      ClientProducer producer = session.createProducer(address);
      ClientMessage message = session.createMessage(false);
      message.putIntProperty(new SimpleString("key"), intValue);
      producer.send(message);

      String jsonString = queueControl.listMessagesAsJSON(null);
      Assert.assertNotNull(jsonString);
      JSONArray array = new JSONArray(jsonString);
      Assert.assertEquals(1, array.length());
      Assert.assertEquals(intValue, array.getJSONObject(0).get("key"));

      consumeMessages(1, session, queue);

      jsonString = queueControl.listMessagesAsJSON(null);
      Assert.assertNotNull(jsonString);
      array = new JSONArray(jsonString);
      Assert.assertEquals(0, array.length());

      session.deleteQueue(queue);
   }

   @Test
   public void testListMessagesWithFilter() throws Exception {
      SimpleString key = new SimpleString("key");
      long matchingValue = RandomUtil.randomLong();
      long unmatchingValue = matchingValue + 1;
      String filter = key + " =" + matchingValue;

      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, null, false);
      QueueControl queueControl = createManagementControl(address, queue);

      ClientProducer producer = session.createProducer(address);
      ClientMessage matchingMessage = session.createMessage(false);
      matchingMessage.putLongProperty(key, matchingValue);
      producer.send(matchingMessage);
      ClientMessage unmatchingMessage = session.createMessage(false);
      unmatchingMessage.putLongProperty(key, unmatchingValue);
      producer.send(unmatchingMessage);

      Map<String, Object>[] messages = queueControl.listMessages(filter);
      Assert.assertEquals(1, messages.length);
      Assert.assertEquals(matchingValue, messages[0].get("key"));

      consumeMessages(2, session, queue);

      messages = queueControl.listMessages(filter);
      Assert.assertEquals(0, messages.length);

      session.deleteQueue(queue);
   }

   @Test
   public void testListMessagesWithNullFilter() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, null, false);
      QueueControl queueControl = createManagementControl(address, queue);

      ClientProducer producer = session.createProducer(address);
      producer.send(session.createMessage(false));
      producer.send(session.createMessage(false));

      Map<String, Object>[] messages = queueControl.listMessages(null);
      Assert.assertEquals(2, messages.length);

      consumeMessages(2, session, queue);

      messages = queueControl.listMessages(null);
      Assert.assertEquals(0, messages.length);

      session.deleteQueue(queue);
   }

   @Test
   public void testListMessagesWithEmptyFilter() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, null, false);
      QueueControl queueControl = createManagementControl(address, queue);

      ClientProducer producer = session.createProducer(address);
      producer.send(session.createMessage(false));
      producer.send(session.createMessage(false));

      Map<String, Object>[] messages = queueControl.listMessages("");
      Assert.assertEquals(2, messages.length);

      consumeMessages(2, session, queue);

      messages = queueControl.listMessages("");
      Assert.assertEquals(0, messages.length);

      session.deleteQueue(queue);
   }

   @Test
   public void testListMessagesAsJSONWithFilter() throws Exception {
      SimpleString key = new SimpleString("key");
      long matchingValue = RandomUtil.randomLong();
      long unmatchingValue = matchingValue + 1;
      String filter = key + " =" + matchingValue;

      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, null, false);
      QueueControl queueControl = createManagementControl(address, queue);

      ClientProducer producer = session.createProducer(address);
      ClientMessage matchingMessage = session.createMessage(false);
      matchingMessage.putLongProperty(key, matchingValue);
      producer.send(matchingMessage);
      ClientMessage unmatchingMessage = session.createMessage(false);
      unmatchingMessage.putLongProperty(key, unmatchingValue);
      producer.send(unmatchingMessage);

      String jsonString = queueControl.listMessagesAsJSON(filter);
      Assert.assertNotNull(jsonString);
      JSONArray array = new JSONArray(jsonString);
      Assert.assertEquals(1, array.length());
      Assert.assertEquals(matchingValue, array.getJSONObject(0).get("key"));

      consumeMessages(2, session, queue);

      jsonString = queueControl.listMessagesAsJSON(filter);
      Assert.assertNotNull(jsonString);
      array = new JSONArray(jsonString);
      Assert.assertEquals(0, array.length());

      session.deleteQueue(queue);
   }

   /**
    * <ol>
    * <li>send a message to queue</li>
    * <li>move all messages from queue to otherQueue using management method</li>
    * <li>check there is no message to consume from queue</li>
    * <li>consume the message from otherQueue</li>
    * </ol>
    */
   @Test
   public void testMoveMessages() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString otherAddress = RandomUtil.randomSimpleString();
      SimpleString otherQueue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, null, false);
      session.createQueue(otherAddress, otherQueue, null, false);
      ClientProducer producer = session.createProducer(address);

      // send on queue
      ClientMessage message = session.createMessage(false);
      SimpleString key = RandomUtil.randomSimpleString();
      long value = RandomUtil.randomLong();
      message.putLongProperty(key, value);
      producer.send(message);

      QueueControl queueControl = createManagementControl(address, queue);
      Assert.assertEquals(1, getMessageCount(queueControl));

      // moved all messages to otherQueue
      int movedMessagesCount = queueControl.moveMessages(null, otherQueue.toString());
      Assert.assertEquals(1, movedMessagesCount);
      Assert.assertEquals(0, getMessageCount(queueControl));

      // check there is no message to consume from queue
      consumeMessages(0, session, queue);

      // consume the message from otherQueue
      ClientConsumer otherConsumer = session.createConsumer(otherQueue);
      ClientMessage m = otherConsumer.receive(500);
      Assert.assertEquals(value, m.getObjectProperty(key));

      m.acknowledge();

      session.deleteQueue(queue);
      otherConsumer.close();
      session.deleteQueue(otherQueue);
   }

   @Test
   public void testMoveMessagesToUnknownQueue() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString unknownQueue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, null, false);
      ClientProducer producer = session.createProducer(address);

      // send on queue
      ClientMessage message = session.createMessage(false);
      SimpleString key = RandomUtil.randomSimpleString();
      long value = RandomUtil.randomLong();
      message.putLongProperty(key, value);
      producer.send(message);

      QueueControl queueControl = createManagementControl(address, queue);
      Assert.assertEquals(1, getMessageCount(queueControl));

      // moved all messages to unknown queue
      try {
         queueControl.moveMessages(null, unknownQueue.toString());
         Assert.fail("operation must fail if the other queue does not exist");
      }
      catch (Exception e) {
      }
      Assert.assertEquals(1, getMessageCount(queueControl));

      consumeMessages(1, session, queue);

      session.deleteQueue(queue);
   }

   /**
    * <ol>
    * <li>send 2 message to queue</li>
    * <li>move messages from queue to otherQueue using management method <em>with filter</em></li>
    * <li>consume the message which <strong>did not</strong> matches the filter from queue</li>
    * <li>consume the message which <strong>did</strong> matches the filter from otherQueue</li>
    * </ol>
    */

   @Test
   public void testMoveMessagesWithFilter() throws Exception {
      SimpleString key = new SimpleString("key");
      long matchingValue = RandomUtil.randomLong();
      long unmatchingValue = matchingValue + 1;

      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString otherAddress = RandomUtil.randomSimpleString();
      SimpleString otherQueue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, null, false);
      session.createQueue(otherAddress, otherQueue, null, false);
      ClientProducer producer = session.createProducer(address);

      // send on queue
      ClientMessage matchingMessage = session.createMessage(false);
      matchingMessage.putLongProperty(key, matchingValue);
      producer.send(matchingMessage);
      ClientMessage unmatchingMessage = session.createMessage(false);
      unmatchingMessage.putLongProperty(key, unmatchingValue);
      producer.send(unmatchingMessage);

      QueueControl queueControl = createManagementControl(address, queue);
      Assert.assertEquals(2, getMessageCount(queueControl));

      // moved matching messages to otherQueue
      int movedMatchedMessagesCount = queueControl.moveMessages(key + " =" + matchingValue, otherQueue.toString());
      Assert.assertEquals(1, movedMatchedMessagesCount);
      Assert.assertEquals(1, getMessageCount(queueControl));

      // consume the unmatched message from queue
      ClientConsumer consumer = session.createConsumer(queue);
      ClientMessage m = consumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals(unmatchingValue, m.getObjectProperty(key));

      // consume the matched message from otherQueue
      ClientConsumer otherConsumer = session.createConsumer(otherQueue);
      m = otherConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals(matchingValue, m.getObjectProperty(key));

      m.acknowledge();

      consumer.close();
      session.deleteQueue(queue);
      otherConsumer.close();
      session.deleteQueue(otherQueue);
   }

   @Test
   public void testMoveMessage() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString otherAddress = RandomUtil.randomSimpleString();
      SimpleString otherQueue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, null, false);
      session.createQueue(otherAddress, otherQueue, null, false);
      ClientProducer producer = session.createProducer(address);

      // send 2 messages on queue
      producer.send(session.createMessage(false));
      producer.send(session.createMessage(false));

      QueueControl queueControl = createManagementControl(address, queue);
      QueueControl otherQueueControl = createManagementControl(otherAddress, otherQueue);
      Assert.assertEquals(2, getMessageCount(queueControl));
      Assert.assertEquals(0, getMessageCount(otherQueueControl));

      // the message IDs are set on the server
      Map<String, Object>[] messages = queueControl.listMessages(null);
      Assert.assertEquals(2, messages.length);
      long messageID = (Long) messages[0].get("messageID");

      boolean moved = queueControl.moveMessage(messageID, otherQueue.toString());
      Assert.assertTrue(moved);
      Assert.assertEquals(1, getMessageCount(queueControl));
      Assert.assertEquals(1, getMessageCount(otherQueueControl));

      consumeMessages(1, session, queue);
      consumeMessages(1, session, otherQueue);

      session.deleteQueue(queue);
      session.deleteQueue(otherQueue);
   }

   @Test
   public void testMoveMessageToUnknownQueue() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString unknownQueue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, null, false);
      ClientProducer producer = session.createProducer(address);

      // send 2 messages on queue
      producer.send(session.createMessage(false));

      QueueControl queueControl = createManagementControl(address, queue);
      Assert.assertEquals(1, getMessageCount(queueControl));

      // the message IDs are set on the server
      Map<String, Object>[] messages = queueControl.listMessages(null);
      Assert.assertEquals(1, messages.length);
      long messageID = (Long) messages[0].get("messageID");

      // moved all messages to unknown queue
      try {
         queueControl.moveMessage(messageID, unknownQueue.toString());
         Assert.fail("operation must fail if the other queue does not exist");
      }
      catch (Exception e) {
      }
      Assert.assertEquals(1, getMessageCount(queueControl));

      consumeMessages(1, session, queue);

      session.deleteQueue(queue);
   }

   /**
    * <ol>
    * <li>send 2 message to queue</li>
    * <li>remove messages from queue using management method <em>with filter</em></li>
    * <li>check there is only one message to consume from queue</li>
    * </ol>
    */

   @Test
   public void testRemoveMessages() throws Exception {
      SimpleString key = new SimpleString("key");
      long matchingValue = RandomUtil.randomLong();
      long unmatchingValue = matchingValue + 1;

      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, null, false);
      ClientProducer producer = session.createProducer(address);

      // send on queue
      ClientMessage matchingMessage = session.createMessage(false);
      matchingMessage.putLongProperty(key, matchingValue);
      producer.send(matchingMessage);
      ClientMessage unmatchingMessage = session.createMessage(false);
      unmatchingMessage.putLongProperty(key, unmatchingValue);
      producer.send(unmatchingMessage);

      QueueControl queueControl = createManagementControl(address, queue);
      Assert.assertEquals(2, getMessageCount(queueControl));

      // removed matching messages to otherQueue
      int removedMatchedMessagesCount = queueControl.removeMessages(key + " =" + matchingValue);
      Assert.assertEquals(1, removedMatchedMessagesCount);
      Assert.assertEquals(1, getMessageCount(queueControl));

      // consume the unmatched message from queue
      ClientConsumer consumer = session.createConsumer(queue);
      ClientMessage m = consumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals(unmatchingValue, m.getObjectProperty(key));

      m.acknowledge();

      // check there is no other message to consume:
      m = consumer.receiveImmediate();
      Assert.assertNull(m);

      consumer.close();
      session.deleteQueue(queue);
   }

   @Test
   public void testRemoveMessagesWithLimit() throws Exception {
      SimpleString key = new SimpleString("key");
      long matchingValue = RandomUtil.randomLong();
      long unmatchingValue = matchingValue + 1;

      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, null, false);
      ClientProducer producer = session.createProducer(address);

      // send on queue
      ClientMessage matchingMessage = session.createMessage(false);
      matchingMessage.putLongProperty(key, matchingValue);
      producer.send(matchingMessage);
      ClientMessage unmatchingMessage = session.createMessage(false);
      unmatchingMessage.putLongProperty(key, unmatchingValue);
      producer.send(unmatchingMessage);

      QueueControl queueControl = createManagementControl(address, queue);
      Assert.assertEquals(2, getMessageCount(queueControl));

      // removed matching messages to otherQueue
      int removedMatchedMessagesCount = queueControl.removeMessages(5, key + " =" + matchingValue);
      Assert.assertEquals(1, removedMatchedMessagesCount);
      Assert.assertEquals(1, getMessageCount(queueControl));

      // consume the unmatched message from queue
      ClientConsumer consumer = session.createConsumer(queue);
      ClientMessage m = consumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals(unmatchingValue, m.getObjectProperty(key));

      m.acknowledge();

      // check there is no other message to consume:
      m = consumer.receiveImmediate();
      Assert.assertNull(m);

      consumer.close();
      session.deleteQueue(queue);
   }

   @Test
   public void testRemoveMessagesWithNullFilter() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, null, false);
      ClientProducer producer = session.createProducer(address);

      // send on queue
      producer.send(session.createMessage(false));
      producer.send(session.createMessage(false));

      QueueControl queueControl = createManagementControl(address, queue);
      Assert.assertEquals(2, getMessageCount(queueControl));

      // removed matching messages to otherQueue
      int removedMatchedMessagesCount = queueControl.removeMessages(null);
      Assert.assertEquals(2, removedMatchedMessagesCount);
      Assert.assertEquals(0, getMessageCount(queueControl));

      session.deleteQueue(queue);
   }

   @Test
   public void testRemoveMessagesWithEmptyFilter() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, null, false);
      ClientProducer producer = session.createProducer(address);

      // send on queue
      producer.send(session.createMessage(false));
      producer.send(session.createMessage(false));

      QueueControl queueControl = createManagementControl(address, queue);
      Assert.assertEquals(2, getMessageCount(queueControl));

      // removed matching messages to otherQueue
      int removedMatchedMessagesCount = queueControl.removeMessages("");
      Assert.assertEquals(2, removedMatchedMessagesCount);
      Assert.assertEquals(0, getMessageCount(queueControl));

      session.deleteQueue(queue);
   }

   @Test
   public void testRemoveMessage() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, null, false);
      ClientProducer producer = session.createProducer(address);

      // send 2 messages on queue
      producer.send(session.createMessage(false));
      producer.send(session.createMessage(false));

      QueueControl queueControl = createManagementControl(address, queue);
      Assert.assertEquals(2, getMessageCount(queueControl));

      // the message IDs are set on the server
      Map<String, Object>[] messages = queueControl.listMessages(null);
      Assert.assertEquals(2, messages.length);
      long messageID = (Long) messages[0].get("messageID");

      // delete 1st message
      boolean deleted = queueControl.removeMessage(messageID);
      Assert.assertTrue(deleted);
      Assert.assertEquals(1, getMessageCount(queueControl));

      // check there is a single message to consume from queue
      consumeMessages(1, session, queue);

      session.deleteQueue(queue);
   }

   @Test
   public void testRemoveScheduledMessage() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, null, false);
      ClientProducer producer = session.createProducer(address);

      // send 2 messages on queue, both scheduled
      long timeout = System.currentTimeMillis() + 5000;
      ClientMessage m1 = session.createMessage(true);
      m1.putLongProperty(MessageImpl.HDR_SCHEDULED_DELIVERY_TIME, timeout);
      producer.send(m1);
      ClientMessage m2 = session.createMessage(true);
      m2.putLongProperty(MessageImpl.HDR_SCHEDULED_DELIVERY_TIME, timeout);
      producer.send(m2);

      QueueControl queueControl = createManagementControl(address, queue);
      Assert.assertEquals(2, queueControl.getScheduledCount());

      // the message IDs are set on the server
      Map<String, Object>[] messages = queueControl.listScheduledMessages();
      Assert.assertEquals(2, messages.length);
      long messageID = (Long) messages[0].get("messageID");

      // delete 1st message
      boolean deleted = queueControl.removeMessage(messageID);
      Assert.assertTrue(deleted);
      Assert.assertEquals(1, queueControl.getScheduledCount());

      // check there is a single message to consume from queue
      while (timeout > System.currentTimeMillis() && queueControl.getScheduledCount() == 1) {
         Thread.sleep(100);
      }

      consumeMessages(1, session, queue);

      session.deleteQueue(queue);
   }

   @Test
   public void testRemoveMessage2() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, null, false);
      ClientProducer producer = session.createProducer(address);

      // send messages on queue

      for (int i = 0; i < 100; i++) {

         ClientMessage msg = session.createMessage(false);
         msg.putIntProperty("count", i);
         producer.send(msg);
      }

      ClientConsumer cons = session.createConsumer(queue);
      session.start();
      LinkedList<ClientMessage> msgs = new LinkedList<ClientMessage>();
      for (int i = 0; i < 50; i++) {
         ClientMessage msg = cons.receive(1000);
         msgs.add(msg);
      }

      QueueControl queueControl = createManagementControl(address, queue);
      Assert.assertEquals(100, getMessageCount(queueControl));

      // the message IDs are set on the server
      Map<String, Object>[] messages = queueControl.listMessages(null);
      Assert.assertEquals(50, messages.length);
      assertEquals(50, ((Number) messages[0].get("count")).intValue());
      long messageID = (Long) messages[0].get("messageID");

      // delete 1st message
      boolean deleted = queueControl.removeMessage(messageID);
      Assert.assertTrue(deleted);
      Assert.assertEquals(99, getMessageCount(queueControl));

      cons.close();

      // check there is a single message to consume from queue
      consumeMessages(99, session, queue);

      session.deleteQueue(queue);
   }

   @Test
   public void testCountMessagesWithFilter() throws Exception {
      SimpleString key = new SimpleString("key");
      long matchingValue = RandomUtil.randomLong();
      long unmatchingValue = matchingValue + 1;

      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, null, false);
      ClientProducer producer = session.createProducer(address);

      // send on queue
      ClientMessage matchingMessage = session.createMessage(false);
      matchingMessage.putLongProperty(key, matchingValue);
      ClientMessage unmatchingMessage = session.createMessage(false);
      unmatchingMessage.putLongProperty(key, unmatchingValue);
      producer.send(matchingMessage);
      producer.send(unmatchingMessage);
      producer.send(matchingMessage);

      QueueControl queueControl = createManagementControl(address, queue);
      Assert.assertEquals(3, getMessageCount(queueControl));

      Assert.assertEquals(2, queueControl.countMessages(key + " =" + matchingValue));
      Assert.assertEquals(1, queueControl.countMessages(key + " =" + unmatchingValue));

      session.deleteQueue(queue);
   }

   @Test
   public void testCountMessagesWithInvalidFilter() throws Exception {
      SimpleString key = new SimpleString("key");
      String matchingValue = "MATCH";
      String nonMatchingValue = "DIFFERENT";

      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, null, false);
      ClientProducer producer = session.createProducer(address);

      // send on queue
      for (int i = 0; i < 100; i++) {
         ClientMessage msg = session.createMessage(false);
         msg.putStringProperty(key, SimpleString.toSimpleString(matchingValue));
         producer.send(msg);
      }

      for (int i = 0; i < 10; i++) {
         ClientMessage msg = session.createMessage(false);
         msg.putStringProperty(key, SimpleString.toSimpleString(nonMatchingValue));
         producer.send(msg);
      }

      // this is just to guarantee a round trip and avoid in transit messages, so they are all accounted for
      session.commit();

      ClientConsumer consumer = session.createConsumer(queue, SimpleString.toSimpleString("nonExistentProperty like \'%Temp/88\'"));

      session.start();

      assertNull(consumer.receiveImmediate());

      QueueControl queueControl = createManagementControl(address, queue);
      Assert.assertEquals(110, getMessageCount(queueControl));

      Assert.assertEquals(0, queueControl.countMessages("nonExistentProperty like \'%Temp/88\'"));

      Assert.assertEquals(100, queueControl.countMessages(key + "=\'" + matchingValue + "\'"));
      Assert.assertEquals(10, queueControl.countMessages(key + " = \'" + nonMatchingValue + "\'"));

      consumer.close();

      session.deleteQueue(queue);
   }

   @Test
   public void testExpireMessagesWithFilter() throws Exception {
      SimpleString key = new SimpleString("key");
      long matchingValue = RandomUtil.randomLong();
      long unmatchingValue = matchingValue + 1;

      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, null, false);
      ClientProducer producer = session.createProducer(address);

      // send on queue
      ClientMessage matchingMessage = session.createMessage(false);
      matchingMessage.putLongProperty(key, matchingValue);
      producer.send(matchingMessage);
      ClientMessage unmatchingMessage = session.createMessage(false);
      unmatchingMessage.putLongProperty(key, unmatchingValue);
      producer.send(unmatchingMessage);

      QueueControl queueControl = createManagementControl(address, queue);
      Assert.assertEquals(2, getMessageCount(queueControl));

      int expiredMessagesCount = queueControl.expireMessages(key + " =" + matchingValue);
      Assert.assertEquals(1, expiredMessagesCount);
      Assert.assertEquals(1, getMessageCount(queueControl));

      // consume the unmatched message from queue
      ClientConsumer consumer = session.createConsumer(queue);
      ClientMessage m = consumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals(unmatchingValue, m.getObjectProperty(key));

      m.acknowledge();

      // check there is no other message to consume:
      m = consumer.receiveImmediate();
      Assert.assertNull(m);

      consumer.close();
      session.deleteQueue(queue);
      session.close();
   }

   @Test
   public void testExpireMessage() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString expiryAddress = RandomUtil.randomSimpleString();
      SimpleString expiryQueue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, null, false);
      session.createQueue(expiryAddress, expiryQueue, null, false);
      ClientProducer producer = session.createProducer(address);

      // send on queue
      producer.send(session.createMessage(false));

      QueueControl queueControl = createManagementControl(address, queue);
      QueueControl expiryQueueControl = createManagementControl(expiryAddress, expiryQueue);
      Assert.assertEquals(1, getMessageCount(queueControl));
      Assert.assertEquals(0, getMessageCount(expiryQueueControl));

      // the message IDs are set on the server
      Map<String, Object>[] messages = queueControl.listMessages(null);
      Assert.assertEquals(1, messages.length);
      long messageID = (Long) messages[0].get("messageID");

      AddressSettings addressSettings = new AddressSettings().setExpiryAddress(expiryAddress);
      server.getAddressSettingsRepository().addMatch(address.toString(), addressSettings);

      boolean expired = queueControl.expireMessage(messageID);
      Assert.assertTrue(expired);
      Assert.assertEquals(0, getMessageCount(queueControl));
      Assert.assertEquals(1, getMessageCount(expiryQueueControl));

      consumeMessages(0, session, queue);
      consumeMessages(1, session, expiryQueue);

      session.deleteQueue(queue);
      session.deleteQueue(expiryQueue);
      session.close();
   }

   @Test
   public void testSendMessageToDeadLetterAddress() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString deadLetterAddress = RandomUtil.randomSimpleString();
      SimpleString deadLetterQueue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, null, false);
      session.createQueue(deadLetterAddress, deadLetterQueue, null, false);
      ClientProducer producer = session.createProducer(address);

      // send 2 messages on queue
      producer.send(session.createMessage(false));
      producer.send(session.createMessage(false));

      QueueControl queueControl = createManagementControl(address, queue);
      QueueControl deadLetterQueueControl = createManagementControl(deadLetterAddress, deadLetterQueue);
      Assert.assertEquals(2, getMessageCount(queueControl));

      // the message IDs are set on the server
      Map<String, Object>[] messages = queueControl.listMessages(null);
      Assert.assertEquals(2, messages.length);
      long messageID = (Long) messages[0].get("messageID");

      AddressSettings addressSettings = new AddressSettings().setDeadLetterAddress(deadLetterAddress);
      server.getAddressSettingsRepository().addMatch(address.toString(), addressSettings);

      Assert.assertEquals(0, getMessageCount(deadLetterQueueControl));
      boolean movedToDeadLetterAddress = queueControl.sendMessageToDeadLetterAddress(messageID);
      Assert.assertTrue(movedToDeadLetterAddress);
      Assert.assertEquals(1, getMessageCount(queueControl));
      Assert.assertEquals(1, getMessageCount(deadLetterQueueControl));

      // check there is a single message to consume from queue
      consumeMessages(1, session, queue);

      // check there is a single message to consume from deadletter queue
      consumeMessages(1, session, deadLetterQueue);

      session.deleteQueue(queue);
      session.deleteQueue(deadLetterQueue);
   }

   @Test
   public void testChangeMessagePriority() throws Exception {
      byte originalPriority = (byte) 1;
      byte newPriority = (byte) 8;

      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, null, false);
      ClientProducer producer = session.createProducer(address);

      ClientMessage message = session.createMessage(false);
      message.setPriority(originalPriority);
      producer.send(message);

      QueueControl queueControl = createManagementControl(address, queue);
      Assert.assertEquals(1, getMessageCount(queueControl));

      // the message IDs are set on the server
      Map<String, Object>[] messages = queueControl.listMessages(null);
      Assert.assertEquals(1, messages.length);
      long messageID = (Long) messages[0].get("messageID");

      boolean priorityChanged = queueControl.changeMessagePriority(messageID, newPriority);
      Assert.assertTrue(priorityChanged);

      ClientConsumer consumer = session.createConsumer(queue);
      ClientMessage m = consumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals(newPriority, m.getPriority());

      consumer.close();
      session.deleteQueue(queue);
   }

   @Test
   public void testChangeMessagePriorityWithInvalidValue() throws Exception {
      byte invalidPriority = (byte) 23;

      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, null, false);
      ClientProducer producer = session.createProducer(address);

      ClientMessage message = session.createMessage(false);
      producer.send(message);

      QueueControl queueControl = createManagementControl(address, queue);
      Assert.assertEquals(1, getMessageCount(queueControl));

      // the message IDs are set on the server
      Map<String, Object>[] messages = queueControl.listMessages(null);
      Assert.assertEquals(1, messages.length);
      long messageID = (Long) messages[0].get("messageID");

      try {
         queueControl.changeMessagePriority(messageID, invalidPriority);
         Assert.fail("operation fails when priority value is < 0 or > 9");
      }
      catch (Exception e) {
      }

      ClientConsumer consumer = session.createConsumer(queue);
      ClientMessage m = consumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertTrue(invalidPriority != m.getPriority());

      consumer.close();
      session.deleteQueue(queue);
   }

   @Test
   public void testListMessageCounter() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, null, false);
      QueueControl queueControl = createManagementControl(address, queue);

      ActiveMQServerControl serverControl = ManagementControlHelper.createActiveMQServerControl(mbeanServer);
      serverControl.enableMessageCounters();
      serverControl.setMessageCounterSamplePeriod(MessageCounterManagerImpl.MIN_SAMPLE_PERIOD);

      String jsonString = queueControl.listMessageCounter();
      MessageCounterInfo info = MessageCounterInfo.fromJSON(jsonString);

      Assert.assertEquals(0, info.getDepth());
      Assert.assertEquals(0, info.getCount());

      ClientProducer producer = session.createProducer(address);
      producer.send(session.createMessage(false));

      Thread.sleep(MessageCounterManagerImpl.MIN_SAMPLE_PERIOD * 2);
      jsonString = queueControl.listMessageCounter();
      info = MessageCounterInfo.fromJSON(jsonString);
      Assert.assertEquals(1, info.getDepth());
      Assert.assertEquals(1, info.getDepthDelta());
      Assert.assertEquals(1, info.getCount());
      Assert.assertEquals(1, info.getCountDelta());

      producer.send(session.createMessage(false));

      Thread.sleep(MessageCounterManagerImpl.MIN_SAMPLE_PERIOD * 2);
      jsonString = queueControl.listMessageCounter();
      info = MessageCounterInfo.fromJSON(jsonString);
      Assert.assertEquals(2, info.getDepth());
      Assert.assertEquals(1, info.getDepthDelta());
      Assert.assertEquals(2, info.getCount());
      Assert.assertEquals(1, info.getCountDelta());

      consumeMessages(2, session, queue);

      Thread.sleep(MessageCounterManagerImpl.MIN_SAMPLE_PERIOD * 2);
      jsonString = queueControl.listMessageCounter();
      info = MessageCounterInfo.fromJSON(jsonString);
      Assert.assertEquals(0, info.getDepth());
      Assert.assertEquals(-2, info.getDepthDelta());
      Assert.assertEquals(2, info.getCount());
      Assert.assertEquals(0, info.getCountDelta());

      session.deleteQueue(queue);
   }

   @Test
   public void testResetMessageCounter() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, null, false);
      QueueControl queueControl = createManagementControl(address, queue);

      ActiveMQServerControl serverControl = ManagementControlHelper.createActiveMQServerControl(mbeanServer);
      serverControl.enableMessageCounters();
      serverControl.setMessageCounterSamplePeriod(MessageCounterManagerImpl.MIN_SAMPLE_PERIOD);

      String jsonString = queueControl.listMessageCounter();
      MessageCounterInfo info = MessageCounterInfo.fromJSON(jsonString);

      Assert.assertEquals(0, info.getDepth());
      Assert.assertEquals(0, info.getCount());

      ClientProducer producer = session.createProducer(address);
      producer.send(session.createMessage(false));

      Thread.sleep(MessageCounterManagerImpl.MIN_SAMPLE_PERIOD * 2);
      jsonString = queueControl.listMessageCounter();
      info = MessageCounterInfo.fromJSON(jsonString);
      Assert.assertEquals(1, info.getDepth());
      Assert.assertEquals(1, info.getDepthDelta());
      Assert.assertEquals(1, info.getCount());
      Assert.assertEquals(1, info.getCountDelta());

      consumeMessages(1, session, queue);

      Thread.sleep(MessageCounterManagerImpl.MIN_SAMPLE_PERIOD * 2);
      jsonString = queueControl.listMessageCounter();
      info = MessageCounterInfo.fromJSON(jsonString);
      Assert.assertEquals(0, info.getDepth());
      Assert.assertEquals(-1, info.getDepthDelta());
      Assert.assertEquals(1, info.getCount());
      Assert.assertEquals(0, info.getCountDelta());

      queueControl.resetMessageCounter();

      Thread.sleep(MessageCounterManagerImpl.MIN_SAMPLE_PERIOD * 2);
      jsonString = queueControl.listMessageCounter();
      info = MessageCounterInfo.fromJSON(jsonString);
      Assert.assertEquals(0, info.getDepth());
      Assert.assertEquals(0, info.getDepthDelta());
      Assert.assertEquals(0, info.getCount());
      Assert.assertEquals(0, info.getCountDelta());

      session.deleteQueue(queue);
   }

   @Test
   public void testListMessageCounterAsHTML() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, null, false);
      QueueControl queueControl = createManagementControl(address, queue);

      String history = queueControl.listMessageCounterAsHTML();
      Assert.assertNotNull(history);

      session.deleteQueue(queue);
   }

   @Test
   public void testListMessageCounterHistory() throws Exception {
      long counterPeriod = 1000;
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, null, false);
      QueueControl queueControl = createManagementControl(address, queue);

      ActiveMQServerControl serverControl = ManagementControlHelper.createActiveMQServerControl(mbeanServer);
      serverControl.enableMessageCounters();
      serverControl.setMessageCounterSamplePeriod(counterPeriod);

      String jsonString = queueControl.listMessageCounterHistory();
      DayCounterInfo[] infos = DayCounterInfo.fromJSON(jsonString);
      Assert.assertEquals(1, infos.length);

      session.deleteQueue(queue);
   }

   @Test
   public void testListMessageCounterHistoryAsHTML() throws Exception {
      long counterPeriod = 1000;
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, null, false);
      QueueControl queueControl = createManagementControl(address, queue);

      ActiveMQServerControl serverControl = ManagementControlHelper.createActiveMQServerControl(mbeanServer);
      serverControl.enableMessageCounters();
      serverControl.setMessageCounterSamplePeriod(counterPeriod);

      String history = queueControl.listMessageCounterHistoryAsHTML();
      Assert.assertNotNull(history);

      session.deleteQueue(queue);

   }

   @Test
   public void testMoveMessagesBack() throws Exception {
      server.createQueue(new SimpleString("q1"), new SimpleString("q1"), null, true, false);
      server.createQueue(new SimpleString("q2"), new SimpleString("q2"), null, true, false);

      ServerLocator locator = createInVMNonHALocator();

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(true, true);

      ClientProducer prod1 = session.createProducer("q1");

      for (int i = 0; i < 10; i++) {
         ClientMessage msg = session.createMessage(true);

         msg.putStringProperty(Message.HDR_DUPLICATE_DETECTION_ID, new SimpleString("dupl-" + i));

         prod1.send(msg);
      }

      session.commit();

      ClientConsumer consumer = session.createConsumer("q1", true);
      session.start();

      assertNotNull(consumer.receive(5000));
      consumer.close();

      QueueControl q1Control = ManagementControlHelper.createQueueControl(new SimpleString("q1"), new SimpleString("q1"), mbeanServer);

      QueueControl q2Control = ManagementControlHelper.createQueueControl(new SimpleString("q2"), new SimpleString("q2"), mbeanServer);

      assertEquals(10, q1Control.moveMessages(null, "q2"));

      consumer = session.createConsumer("q2", true);

      assertNotNull(consumer.receive(500));

      consumer.close();

      q2Control.moveMessages(null, "q1", false);

      session.start();
      consumer = session.createConsumer("q1");

      for (int i = 0; i < 10; i++) {
         ClientMessage msg = consumer.receive(5000);
         assertNotNull(msg);
         msg.acknowledge();
      }

      consumer.close();

      session.deleteQueue("q1");

      session.deleteQueue("q2");

      session.close();

      locator.close();

   }

   @Test
   public void testMoveMessagesBack2() throws Exception {
      server.createQueue(new SimpleString("q1"), new SimpleString("q1"), null, true, false);
      server.createQueue(new SimpleString("q2"), new SimpleString("q2"), null, true, false);

      ServerLocator locator = createInVMNonHALocator();

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(true, true);

      ClientProducer prod1 = session.createProducer("q1");

      int NUMBER_OF_MSGS = 10;

      for (int i = 0; i < NUMBER_OF_MSGS; i++) {
         ClientMessage msg = session.createMessage(true);

         msg.putStringProperty(Message.HDR_DUPLICATE_DETECTION_ID, new SimpleString("dupl-" + i));

         prod1.send(msg);
      }

      session.commit();

      ClientConsumer consumer = session.createConsumer("q1", true);
      session.start();

      assertNotNull(consumer.receive(5000));
      consumer.close();

      QueueControl q1Control = ManagementControlHelper.createQueueControl(new SimpleString("q1"), new SimpleString("q1"), mbeanServer);

      QueueControl q2Control = ManagementControlHelper.createQueueControl(new SimpleString("q2"), new SimpleString("q2"), mbeanServer);

      assertEquals(NUMBER_OF_MSGS, q1Control.moveMessages(null, "q2"));

      long[] messageIDs = new long[NUMBER_OF_MSGS];

      consumer = session.createConsumer("q2", true);

      for (int i = 0; i < NUMBER_OF_MSGS; i++) {
         ClientMessage msg = consumer.receive(5000);
         assertNotNull(msg);
         messageIDs[i] = msg.getMessageID();
      }

      assertNull(consumer.receiveImmediate());

      consumer.close();

      for (int i = 0; i < NUMBER_OF_MSGS; i++) {
         q2Control.moveMessage(messageIDs[i], "q1");
      }

      session.start();
      consumer = session.createConsumer("q1");

      for (int i = 0; i < NUMBER_OF_MSGS; i++) {
         ClientMessage msg = consumer.receive(5000);
         assertNotNull(msg);
         msg.acknowledge();
      }

      consumer.close();

      session.deleteQueue("q1");

      session.deleteQueue("q2");

      session.close();
   }

   @Test
   public void testPauseAndResume() {
      long counterPeriod = 1000;
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      try {
         session.createQueue(address, queue, null, false);
         QueueControl queueControl = createManagementControl(address, queue);

         ActiveMQServerControl serverControl = ManagementControlHelper.createActiveMQServerControl(mbeanServer);
         serverControl.enableMessageCounters();
         serverControl.setMessageCounterSamplePeriod(counterPeriod);
         Assert.assertFalse(queueControl.isPaused());
         queueControl.pause();
         Assert.assertTrue(queueControl.isPaused());
         queueControl.resume();
         Assert.assertFalse(queueControl.isPaused());
      }
      catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
   }

   @Test
   public void testResetMessagesAdded() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, null, false);

      QueueControl queueControl = createManagementControl(address, queue);
      Assert.assertEquals(0, getMessagesAdded(queueControl));

      ClientProducer producer = session.createProducer(address);
      producer.send(session.createMessage(false));
      Assert.assertEquals(1, getMessagesAdded(queueControl));
      producer.send(session.createMessage(false));
      Assert.assertEquals(2, getMessagesAdded(queueControl));

      consumeMessages(2, session, queue);

      Assert.assertEquals(2, getMessagesAdded(queueControl));

      queueControl.resetMessagesAdded();

      Assert.assertEquals(0, getMessagesAdded(queueControl));

      session.deleteQueue(queue);
   }

   @Test
   public void testResetMessagesAcknowledged() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, null, false);

      QueueControl queueControl = createManagementControl(address, queue);
      Assert.assertEquals(0, queueControl.getMessagesAcknowledged());

      ClientProducer producer = session.createProducer(address);
      producer.send(session.createMessage(false));
      consumeMessages(1, session, queue);
      Assert.assertEquals(1, queueControl.getMessagesAcknowledged());
      producer.send(session.createMessage(false));
      consumeMessages(1, session, queue);
      Assert.assertEquals(2, queueControl.getMessagesAcknowledged());

      queueControl.resetMessagesAcknowledged();

      Assert.assertEquals(0, queueControl.getMessagesAcknowledged());

      session.deleteQueue(queue);
   }

   //make sure notifications are always received no matter whether
   //a Queue is created via QueueControl or by JMSServerManager directly.
   @Test
   public void testCreateQueueNotification() throws Exception {
      JMSUtil.JMXListener listener = new JMSUtil.JMXListener();
      this.mbeanServer.addNotificationListener(ObjectNameBuilder.DEFAULT.getActiveMQServerObjectName(), listener, null, null);

      SimpleString testQueueName = new SimpleString("newQueue");
      String testQueueName2 = "newQueue2";
      this.server.createQueue(testQueueName, testQueueName, null, false, false);

      Notification notif = listener.getNotification();

      System.out.println("got notif: " + notif);
      assertEquals(CoreNotificationType.BINDING_ADDED.toString(), notif.getType());

      this.server.destroyQueue(testQueueName);

      notif = listener.getNotification();
      System.out.println("got notif: " + notif);
      assertEquals(CoreNotificationType.BINDING_REMOVED.toString(), notif.getType());

      ActiveMQServerControl control = ManagementControlHelper.createActiveMQServerControl(mbeanServer);

      control.createQueue(testQueueName2, testQueueName2);

      notif = listener.getNotification();
      System.out.println("got notif: " + notif);
      assertEquals(CoreNotificationType.BINDING_ADDED.toString(), notif.getType());

      control.destroyQueue(testQueueName2);

      notif = listener.getNotification();
      System.out.println("got notif: " + notif);
      assertEquals(CoreNotificationType.BINDING_REMOVED.toString(), notif.getType());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      Configuration conf = createDefaultInVMConfig().setJMXManagementEnabled(true);
      server = addServer(ActiveMQServers.newActiveMQServer(conf, mbeanServer, false));

      server.start();

      locator = createInVMNonHALocator().setBlockOnNonDurableSend(true).setConsumerWindowSize(0);
      ClientSessionFactory sf = createSessionFactory(locator);
      session = sf.createSession(false, true, false);
      session.start();
   }

   protected QueueControl createManagementControl(final SimpleString address,
                                                  final SimpleString queue) throws Exception {
      QueueControl queueControl = ManagementControlHelper.createQueueControl(address, queue, mbeanServer);

      return queueControl;
   }
}
