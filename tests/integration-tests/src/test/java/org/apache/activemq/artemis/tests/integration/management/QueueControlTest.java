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

import static org.apache.activemq.artemis.core.management.impl.openmbean.CompositeDataConstants.BODY;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.management.Notification;
import javax.management.openmbean.CompositeData;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.JsonUtil;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.RoutingType;
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
import org.apache.activemq.artemis.core.config.DivertConfiguration;
import org.apache.activemq.artemis.core.messagecounter.impl.MessageCounterManagerImpl;
import org.apache.activemq.artemis.core.paging.impl.PagingManagerImpl;
import org.apache.activemq.artemis.core.postoffice.impl.LocalQueueBinding;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.impl.QueueImpl;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.junit.Wait;
import org.apache.activemq.artemis.tests.integration.jms.server.management.JMSUtil;
import org.apache.activemq.artemis.utils.Base64;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(value = Parameterized.class)
public class QueueControlTest extends ManagementTestBase {

   private ActiveMQServer server;
   private ClientSession session;
   private ServerLocator locator;
   private final boolean durable;

   @Parameterized.Parameters(name = "durable={0}")
   public static Collection<Object[]> getParams() {
      return Arrays.asList(new Object[][] {{true}, {false}});
   }


   /**
    * @param durable
    */
   public QueueControlTest(boolean durable) {
      super();
      this.durable = durable;
   }


   @Test
   public void testAttributes() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString filter = new SimpleString("color = 'blue'");

      session.createQueue(address, RoutingType.MULTICAST, queue, filter, durable);

      QueueControl queueControl = createManagementControl(address, queue);
      Assert.assertEquals(queue.toString(), queueControl.getName());
      Assert.assertEquals(address.toString(), queueControl.getAddress());
      Assert.assertEquals(filter.toString(), queueControl.getFilter());
      Assert.assertEquals(durable, queueControl.isDurable());
      Assert.assertFalse(queueControl.isTemporary());

      session.deleteQueue(queue);
   }

   @Test
   public void testGetNullFilter() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);

      QueueControl queueControl = createManagementControl(address, queue);
      Assert.assertEquals(queue.toString(), queueControl.getName());
      Assert.assertNull(queueControl.getFilter());

      session.deleteQueue(queue);
   }

   @Test
   public void testGetDeadLetterAddress() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();
      final SimpleString deadLetterAddress = RandomUtil.randomSimpleString();

      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);

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

      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);

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

      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);

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

      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);

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

      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);

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

      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);

      QueueControl queueControl = createManagementControl(address, queue);

      Assert.assertEquals(0, queueControl.getConsumerCount());

      ClientConsumer consumer = session.createConsumer(queue);
      Assert.assertEquals(1, queueControl.getConsumerCount());

      System.out.println("Consumers: " + queueControl.listConsumersAsJSON());

      JsonArray obj = JsonUtil.readJsonArray(queueControl.listConsumersAsJSON());

      assertEquals(1, obj.size());

      consumer.close();
      Assert.assertEquals(0, queueControl.getConsumerCount());

      obj = JsonUtil.readJsonArray(queueControl.listConsumersAsJSON());

      assertEquals(0, obj.size());

      session.deleteQueue(queue);
   }

   @Test
   public void testGetMessageCount() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);

      QueueControl queueControl = createManagementControl(address, queue);
      Assert.assertEquals(0, getMessageCount(queueControl));

      ClientProducer producer = session.createProducer(address);
      producer.send(session.createMessage(durable));
      assertMessageMetrics(queueControl, 1, durable);

      consumeMessages(1, session, queue);

      assertMessageMetrics(queueControl, 0, durable);

      session.deleteQueue(queue);
   }

   @Test
   public void testGetFirstMessage() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);

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

      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);

      QueueControl queueControl = createManagementControl(address, queue);
      Assert.assertEquals(0, getMessagesAdded(queueControl));

      ClientProducer producer = session.createProducer(address);
      producer.send(session.createMessage(durable));
      Assert.assertEquals(1, getMessagesAdded(queueControl));
      producer.send(session.createMessage(durable));
      Assert.assertEquals(2, getMessagesAdded(queueControl));

      consumeMessages(2, session, queue);

      Assert.assertEquals(2, getMessagesAdded(queueControl));

      session.deleteQueue(queue);
   }

   @Test
   public void testGetMessagesAcknowledged() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, RoutingType.MULTICAST, queue, null, false);

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

      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);

      QueueControl queueControl = createManagementControl(address, queue);
      Assert.assertEquals(0, queueControl.getScheduledCount());

      ClientProducer producer = session.createProducer(address);
      ClientMessage message = session.createMessage(durable);
      message.putLongProperty(Message.HDR_SCHEDULED_DELIVERY_TIME, System.currentTimeMillis() + delay);
      producer.send(message);

      long timeout = System.currentTimeMillis() + 5000;
      while (timeout > System.currentTimeMillis() && queueControl.getScheduledCount() != 1) {
         Thread.sleep(100);
      }

      assertScheduledMetrics(queueControl, 1, durable);
      assertMessageMetrics(queueControl, 1, durable);

      consumeMessages(0, session, queue);

      Thread.sleep(delay * 2);

      Assert.assertEquals(0, queueControl.getScheduledCount());
      consumeMessages(1, session, queue);
      assertMessageMetrics(queueControl, 0, durable);
      assertScheduledMetrics(queueControl, 0, durable);

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

         transSession.createQueue(address, RoutingType.MULTICAST, queue, null, durable);

         final QueueControl queueControl = createManagementControl(address, queue);

         ClientProducer producer = transSession.createProducer(address);

         for (int i = 0; i < numMsg; i++) {
            ClientMessage message = transSession.createMessage(durable);
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
               } catch (ActiveMQException e1) {
                  e1.printStackTrace();
               }
               latch1.countDown();
               try {
                  latch2.await(10, TimeUnit.SECONDS);
               } catch (InterruptedException e) {
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
      } finally {
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
      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);

      Queue srvqueue = server.locateQueue(queue);

      QueueControl queueControl = createManagementControl(address, queue);

      ClientProducer producer = session.createProducer(address);
      ClientMessage message = session.createMessage(durable);
      message.putIntProperty(new SimpleString("key"), intValue);
      producer.send(message);
      producer.send(session.createMessage(durable));

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
      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);

      QueueControl queueControl = createManagementControl(address, queue);

      ClientProducer producer = session.createProducer(address);
      ClientMessage message = session.createMessage(durable);
      message.putLongProperty(Message.HDR_SCHEDULED_DELIVERY_TIME, System.currentTimeMillis() + delay);
      message.putIntProperty(new SimpleString("key"), intValue);
      producer.send(message);
      // unscheduled message
      producer.send(session.createMessage(durable));

      Map<String, Object>[] messages = queueControl.listScheduledMessages();
      Assert.assertEquals(1, messages.length);
      assertScheduledMetrics(queueControl, 1, durable);

      Assert.assertEquals(intValue, Integer.parseInt((messages[0].get("key")).toString()));

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
      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);

      QueueControl queueControl = createManagementControl(address, queue);

      ClientProducer producer = session.createProducer(address);
      ClientMessage message = session.createMessage(false);
      message.putLongProperty(Message.HDR_SCHEDULED_DELIVERY_TIME, System.currentTimeMillis() + delay);
      message.putIntProperty(new SimpleString("key"), intValue);
      producer.send(message);
      // unscheduled message
      producer.send(session.createMessage(durable));

      String jsonString = queueControl.listScheduledMessagesAsJSON();
      Assert.assertNotNull(jsonString);
      JsonArray array = JsonUtil.readJsonArray(jsonString);
      Assert.assertEquals(1, array.size());
      int i = Integer.parseInt(array.getJsonObject(0).get("key").toString().replaceAll("\"", ""));
      Assert.assertEquals(intValue, i);

      Thread.sleep(delay + 500);

      jsonString = queueControl.listScheduledMessagesAsJSON();
      Assert.assertNotNull(jsonString);
      array = JsonUtil.readJsonArray(jsonString);
      Assert.assertEquals(0, array.size());

      consumeMessages(2, session, queue);

      session.deleteQueue(queue);
   }

   @Test
   public void testGetDeliveringCount() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);

      ClientProducer producer = session.createProducer(address);
      producer.send(session.createMessage(durable));

      QueueControl queueControl = createManagementControl(address, queue);
      Assert.assertEquals(0, queueControl.getDeliveringCount());

      ClientConsumer consumer = session.createConsumer(queue);
      ClientMessage message = consumer.receive(500);
      Assert.assertNotNull(message);
      assertDeliveringMetrics(queueControl, 1, durable);

      message.acknowledge();
      session.commit();
      assertDeliveringMetrics(queueControl, 0, durable);

      consumer.close();
      session.deleteQueue(queue);
   }

   @Test
   public void testMessagesAddedAndMessagesAcknowledged() throws Exception {
      final int THREAD_COUNT = 5;
      final int MSG_COUNT = 1000;

      CountDownLatch producerCountDown = new CountDownLatch(THREAD_COUNT);
      CountDownLatch consumerCountDown = new CountDownLatch(THREAD_COUNT);

      ExecutorService producerExecutor = Executors.newFixedThreadPool(THREAD_COUNT);
      ExecutorService consumerExecutor = Executors.newFixedThreadPool(THREAD_COUNT);

      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      try {
         session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);

         for (int i = 0; i < THREAD_COUNT; i++) {
            producerExecutor.submit(() -> {
               try (ClientSessionFactory sf = locator.createSessionFactory(); ClientSession session = sf.createSession(false, true, false); ClientProducer producer = session.createProducer(address)) {
                  for (int j = 0; j < MSG_COUNT; j++) {
                     producer.send(session.createMessage(false));
                     Thread.sleep(5);
                  }
                  producerCountDown.countDown();
               } catch (Exception e) {
                  e.printStackTrace();
               }
            });
         }

         for (int i = 0; i < THREAD_COUNT; i++) {
            consumerExecutor.submit(() -> {
               try (ClientSessionFactory sf = locator.createSessionFactory(); ClientSession session = sf.createSession(false, true, false); ClientConsumer consumer = session.createConsumer(queue)) {
                  session.start();
                  for (int j = 0; j < MSG_COUNT; j++) {
                     ClientMessage message = consumer.receive(500);
                     Assert.assertNotNull(message);
                     message.acknowledge();
                  }
                  session.commit();
                  consumerCountDown.countDown();
               } catch (Exception e) {
                  e.printStackTrace();
               }
            });
         }

         producerCountDown.await(30, TimeUnit.SECONDS);
         consumerCountDown.await(30, TimeUnit.SECONDS);

         QueueControl queueControl = createManagementControl(address, queue, RoutingType.MULTICAST);
         Thread.sleep(200);
         Assert.assertEquals(0, queueControl.getMessageCount());
         Assert.assertEquals(0, queueControl.getConsumerCount());
         Assert.assertEquals(0, queueControl.getDeliveringCount());
         Assert.assertEquals(THREAD_COUNT * MSG_COUNT, queueControl.getMessagesAdded());
         Assert.assertEquals(THREAD_COUNT * MSG_COUNT, queueControl.getMessagesAcknowledged());

         session.deleteQueue(queue);
      } finally {
         shutdownExecutor(producerExecutor);
         shutdownExecutor(consumerExecutor);
      }
   }

   private void shutdownExecutor(ExecutorService executor) {
      try {
         executor.shutdown();
         executor.awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
      } finally {
         executor.shutdownNow();
      }
   }

   @Test
   public void testListMessagesAsJSONWithNullFilter() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();
      int intValue = RandomUtil.randomInt();
      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);

      QueueControl queueControl = createManagementControl(address, queue);

      ClientProducer producer = session.createProducer(address);
      ClientMessage message = session.createMessage(durable);
      message.putIntProperty(new SimpleString("key"), intValue);
      producer.send(message);

      String jsonString = queueControl.listMessagesAsJSON(null);
      Assert.assertNotNull(jsonString);
      JsonArray array = JsonUtil.readJsonArray(jsonString);
      Assert.assertEquals(1, array.size());

      long l = Long.parseLong(array.getJsonObject(0).get("key").toString().replaceAll("\"", ""));
      Assert.assertEquals(intValue, l);

      consumeMessages(1, session, queue);

      jsonString = queueControl.listMessagesAsJSON(null);
      Assert.assertNotNull(jsonString);
      array = JsonUtil.readJsonArray(jsonString);
      Assert.assertEquals(0, array.size());

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

      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);
      QueueControl queueControl = createManagementControl(address, queue);

      ClientProducer producer = session.createProducer(address);
      ClientMessage matchingMessage = session.createMessage(durable);
      matchingMessage.putLongProperty(key, matchingValue);
      producer.send(matchingMessage);
      ClientMessage unmatchingMessage = session.createMessage(durable);
      unmatchingMessage.putLongProperty(key, unmatchingValue);
      producer.send(unmatchingMessage);

      assertMessageMetrics(queueControl, 2, durable);
      Map<String, Object>[] messages = queueControl.listMessages(filter);
      Assert.assertEquals(1, messages.length);
      Assert.assertEquals(matchingValue, Long.parseLong(messages[0].get("key").toString()));

      consumeMessages(2, session, queue);

      messages = queueControl.listMessages(filter);
      Assert.assertEquals(0, messages.length);
      assertMessageMetrics(queueControl, 0, durable);

      session.deleteQueue(queue);
   }

   @Test
   public void testListMessagesWithNullFilter() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);
      QueueControl queueControl = createManagementControl(address, queue);

      ClientProducer producer = session.createProducer(address);
      producer.send(session.createMessage(durable));
      producer.send(session.createMessage(durable));

      Wait.assertEquals(2, () -> queueControl.listMessages(null).length);

      consumeMessages(2, session, queue);

      Wait.assertEquals(0, () -> queueControl.listMessages(null).length);

      session.deleteQueue(queue);
   }

   @Test
   public void testListMessagesWithEmptyFilter() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);
      QueueControl queueControl = createManagementControl(address, queue);

      ClientProducer producer = session.createProducer(address);
      producer.send(session.createMessage(durable));
      producer.send(session.createMessage(durable));

      Wait.assertEquals(2, () -> queueControl.listMessages("").length);

      consumeMessages(2, session, queue);

      Wait.assertEquals(0, () -> queueControl.listMessages("").length);

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

      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);
      QueueControl queueControl = createManagementControl(address, queue);

      ClientProducer producer = session.createProducer(address);
      ClientMessage matchingMessage = session.createMessage(durable);
      matchingMessage.putLongProperty(key, matchingValue);
      producer.send(matchingMessage);
      ClientMessage unmatchingMessage = session.createMessage(durable);
      unmatchingMessage.putLongProperty(key, unmatchingValue);
      producer.send(unmatchingMessage);

      String jsonString = queueControl.listMessagesAsJSON(filter);
      Assert.assertNotNull(jsonString);
      JsonArray array = JsonUtil.readJsonArray(jsonString);
      Assert.assertEquals(1, array.size());

      long l = Long.parseLong(array.getJsonObject(0).get("key").toString().replaceAll("\"", ""));
      Assert.assertEquals(matchingValue, l);

      consumeMessages(2, session, queue);

      jsonString = queueControl.listMessagesAsJSON(filter);
      Assert.assertNotNull(jsonString);
      array = JsonUtil.readJsonArray(jsonString);
      Assert.assertEquals(0, array.size());

      session.deleteQueue(queue);
   }

   /**
    * Test retry - get a message from DLQ and put on original queue.
    */
   @Test
   public void testRetryMessage() throws Exception {
      final SimpleString dla = new SimpleString("DLA");
      final SimpleString qName = new SimpleString("q1");
      final SimpleString adName = new SimpleString("ad1");
      final SimpleString dlq = new SimpleString("DLQ1");
      final String sampleText = "Put me on DLQ";

      AddressSettings addressSettings = new AddressSettings().setMaxDeliveryAttempts(1).setDeadLetterAddress(dla);
      server.getAddressSettingsRepository().addMatch(adName.toString(), addressSettings);

      session.createQueue(dla, RoutingType.MULTICAST, dlq, null, durable);
      session.createQueue(adName, RoutingType.MULTICAST, qName, null, durable);

      // Send message to queue.
      ClientProducer producer = session.createProducer(adName);
      producer.send(createTextMessage(session, sampleText));
      session.start();

      ClientConsumer clientConsumer = session.createConsumer(qName);
      ClientMessage clientMessage = clientConsumer.receive(500);
      clientMessage.acknowledge();
      Assert.assertNotNull(clientMessage);

      Assert.assertEquals(clientMessage.getBodyBuffer().readString(), sampleText);

      // force a rollback to DLQ
      session.rollback();
      clientMessage = clientConsumer.receiveImmediate();
      Assert.assertNull(clientMessage);

      QueueControl queueControl = createManagementControl(dla, dlq);
      assertMessageMetrics(queueControl, 1, durable);
      final long messageID = getFirstMessageId(queueControl);

      // Retry the message - i.e. it should go from DLQ to original Queue.
      Assert.assertTrue(queueControl.retryMessage(messageID));

      // Assert DLQ is empty...
      Assert.assertEquals(0, getMessageCount(queueControl));
      assertMessageMetrics(queueControl, 0, durable);

      // .. and that the message is now on the original queue once more.
      clientMessage = clientConsumer.receive(500);
      clientMessage.acknowledge();
      Assert.assertNotNull(clientMessage);

      Assert.assertEquals(sampleText, clientMessage.getBodyBuffer().readString());

      clientConsumer.close();
   }

   /**
    * Test retry - get a diverted message from DLQ and put on original queue.
    */
   @Test
   public void testRetryDivertedMessage() throws Exception {
      final SimpleString dla = new SimpleString("DLA");
      final SimpleString dlq = new SimpleString("DLQ");
      final SimpleString forwardingQueue = new SimpleString("forwardingQueue");
      final SimpleString forwardingAddress = new SimpleString("forwardingAddress");
      final SimpleString myTopic = new SimpleString("myTopic");
      final String sampleText = "Put me on DLQ";

      AddressSettings addressSettings = new AddressSettings().setMaxDeliveryAttempts(1).setDeadLetterAddress(dla);
      server.getAddressSettingsRepository().addMatch(forwardingAddress.toString(), addressSettings);

      // create target queue, DLQ and source topic
      session.createQueue(dla, RoutingType.MULTICAST, dlq, null, durable);
      session.createQueue(forwardingAddress, RoutingType.MULTICAST, forwardingQueue, null, durable);
      session.createAddress(myTopic, RoutingType.MULTICAST, false);

      DivertConfiguration divert = new DivertConfiguration().setName("local-divert")
            .setRoutingName("some-name").setAddress(myTopic.toString())
            .setForwardingAddress(forwardingAddress.toString()).setExclusive(false);
      server.deployDivert(divert);

      // Send message to topic.
      ClientProducer producer = session.createProducer(myTopic);
      producer.send(createTextMessage(session, sampleText));
      session.start();

      ClientConsumer clientConsumer = session.createConsumer(forwardingQueue);
      ClientMessage clientMessage = clientConsumer.receive(500);
      clientMessage.acknowledge();
      Assert.assertNotNull(clientMessage);

      Assert.assertEquals(clientMessage.getBodyBuffer().readString(), sampleText);

      // force a rollback to DLQ
      session.rollback();
      clientMessage = clientConsumer.receiveImmediate();
      Assert.assertNull(clientMessage);

      QueueControl queueControl = createManagementControl(dla, dlq, RoutingType.MULTICAST);
      assertMessageMetrics(queueControl, 1, durable);

      final long messageID = getFirstMessageId(queueControl);

      // Retry the message - i.e. it should go from DLQ to original Queue.
      Assert.assertTrue(queueControl.retryMessage(messageID));

      // Assert DLQ is empty...
      assertMessageMetrics(queueControl, 0, durable);

      // .. and that the message is now on the original queue once more.
      clientMessage = clientConsumer.receive(500);
      Assert.assertNotNull(clientMessage); // fails because of AMQ222196 !!!
      clientMessage.acknowledge();

      Assert.assertEquals(sampleText, clientMessage.getBodyBuffer().readString());

      clientConsumer.close();
   }

   /**
    * Test retry multiple messages from  DLQ to original queue.
    */
   @Test
   public void testRetryMultipleMessages() throws Exception {
      final SimpleString dla = new SimpleString("DLA");
      final SimpleString qName = new SimpleString("q1");
      final SimpleString adName = new SimpleString("ad1");
      final SimpleString dlq = new SimpleString("DLQ1");
      final String sampleText = "Put me on DLQ";
      final int numMessagesToTest = 10;

      AddressSettings addressSettings = new AddressSettings().setMaxDeliveryAttempts(1).setDeadLetterAddress(dla);
      server.getAddressSettingsRepository().addMatch(adName.toString(), addressSettings);

      session.createQueue(dla, RoutingType.MULTICAST, dlq, null, durable);
      session.createQueue(adName, RoutingType.MULTICAST, qName, null, durable);

      // Send message to queue.
      ClientProducer producer = session.createProducer(adName);
      for (int i = 0; i < numMessagesToTest; i++) {
         producer.send(createTextMessage(session, sampleText));
      }

      session.start();

      final LocalQueueBinding binding = (LocalQueueBinding) server.getPostOffice().getBinding(qName);
      Queue q = binding.getQueue();
      final LocalQueueBinding binding2 = (LocalQueueBinding) server.getPostOffice().getBinding(dlq);
      Queue q2 = binding2.getQueue();

      Field queueMemorySizeField = QueueImpl.class.getDeclaredField("queueMemorySize");
      queueMemorySizeField.setAccessible(true);

      //Get memory size counters to verify
      AtomicInteger queueMemorySize1 = (AtomicInteger) queueMemorySizeField.get(q);
      AtomicInteger queueMemorySize2 = (AtomicInteger) queueMemorySizeField.get(q2);

      //Verify that original queue has a memory size greater than 0 and DLQ is 0
      assertTrue(queueMemorySize1.get() > 0);
      assertEquals(0, queueMemorySize2.get());

      // Read and rollback all messages to DLQ
      ClientConsumer clientConsumer = session.createConsumer(qName);
      for (int i = 0; i < numMessagesToTest; i++) {
         ClientMessage clientMessage = clientConsumer.receive(500);
         clientMessage.acknowledge();
         Assert.assertNotNull(clientMessage);
         Assert.assertEquals(clientMessage.getBodyBuffer().readString(), sampleText);
         session.rollback();
      }

      Assert.assertNull(clientConsumer.receiveImmediate());

      //Verify that original queue has a memory size of 0 and DLQ is greater than 0 after rollback
      assertEquals(0, queueMemorySize1.get());
      assertTrue(queueMemorySize2.get() > 0);

      QueueControl dlqQueueControl = createManagementControl(dla, dlq);
      assertMessageMetrics(dlqQueueControl, numMessagesToTest, durable);

      // Retry all messages - i.e. they should go from DLQ to original Queue.
      Assert.assertEquals(numMessagesToTest, dlqQueueControl.retryMessages());

      // Assert DLQ is empty...
      assertMessageMetrics(dlqQueueControl, 0, durable);

      //Verify that original queue has a memory size of greater than 0 and DLQ is 0 after move
      assertTrue(queueMemorySize1.get() > 0);
      assertEquals(0, queueMemorySize2.get());

      // .. and that the messages is now on the original queue once more.
      for (int i = 0; i < numMessagesToTest; i++) {
         ClientMessage clientMessage = clientConsumer.receive(500);
         clientMessage.acknowledge();
         Assert.assertNotNull(clientMessage);
         Assert.assertEquals(clientMessage.getBodyBuffer().readString(), sampleText);
      }

      clientConsumer.close();

      //Verify that original queue and DLQ have a memory size of 0
      assertEquals(0, queueMemorySize1.get());
      assertEquals(0, queueMemorySize2.get());
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

      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);
      session.createQueue(otherAddress, RoutingType.MULTICAST, otherQueue, null, durable);
      ClientProducer producer = session.createProducer(address);

      // send on queue
      ClientMessage message = session.createMessage(durable);
      SimpleString key = RandomUtil.randomSimpleString();
      long value = RandomUtil.randomLong();
      message.putLongProperty(key, value);
      producer.send(message);

      final LocalQueueBinding binding = (LocalQueueBinding) server.getPostOffice().getBinding(queue);
      Queue q = binding.getQueue();
      Field queueMemorySizeField = QueueImpl.class.getDeclaredField("queueMemorySize");
      queueMemorySizeField.setAccessible(true);

      //Get memory size counters to verify
      AtomicInteger queueMemorySize = (AtomicInteger) queueMemorySizeField.get(q);

      QueueControl queueControl = createManagementControl(address, queue);
      assertMessageMetrics(queueControl, 1, durable);

      //verify memory usage is greater than 0
      Assert.assertTrue(queueMemorySize.get() > 0);

      // moved all messages to otherQueue
      int movedMessagesCount = queueControl.moveMessages(null, otherQueue.toString());
      Assert.assertEquals(1, movedMessagesCount);
      assertMessageMetrics(queueControl, 0, durable);

      //verify memory usage is 0 after move
      Assert.assertEquals(0, queueMemorySize.get());

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
   public void testMoveMessages2() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queueA = new SimpleString("A");
      SimpleString queueB = new SimpleString("B");
      SimpleString queueC = new SimpleString("C");

      server.createQueue(address, RoutingType.MULTICAST, queueA, null, durable, false);
      server.createQueue(address, RoutingType.MULTICAST, queueB, null, durable, false);
      server.createQueue(address, RoutingType.MULTICAST, queueC, null, durable, false);


      QueueControl queueControlA = createManagementControl(address, queueA);
      QueueControl queueControlB = createManagementControl(address, queueB);
      QueueControl queueControlC = createManagementControl(address, queueC);

      // send two messages on queueA

      queueControlA.sendMessage(new HashMap<String, String>(), Message.BYTES_TYPE, Base64.encodeBytes("theBody".getBytes()), true, "myUser", "myPassword");
      queueControlA.sendMessage(new HashMap<String, String>(), Message.BYTES_TYPE, Base64.encodeBytes("theBody2".getBytes()), true, "myUser", "myPassword");

      Wait.assertEquals(2, () -> getMessageCount(queueControlA));
      Wait.assertEquals(0, () -> getMessageCount(queueControlB));
      Wait.assertEquals(0, () -> getMessageCount(queueControlC));

      // move 2 messages from queueA to queueB
      queueControlA.moveMessages(null, queueB.toString());
      Thread.sleep(500);
      Wait.assertEquals(0, () -> getMessageCount(queueControlA));
      Wait.assertEquals(2, () -> getMessageCount(queueControlB));

      // move 1 message to queueC
      queueControlA.sendMessage(new HashMap<String, String>(), Message.BYTES_TYPE, Base64.encodeBytes("theBody3".getBytes()), true, "myUser", "myPassword");
      Wait.assertEquals(1, () -> getMessageCount(queueControlA));
      queueControlA.moveMessages(null, queueC.toString());
      Wait.assertEquals(1, () -> getMessageCount(queueControlC));
      Wait.assertEquals(0, () -> getMessageCount(queueControlA));

      //move all messages back to A
      queueControlB.moveMessages(null, queueA.toString());
      Wait.assertEquals(2, () -> getMessageCount(queueControlA));
      Wait.assertEquals(0, () -> getMessageCount(queueControlB));

      queueControlC.moveMessages(null, queueA.toString());
      Wait.assertEquals(3, () -> getMessageCount(queueControlA));
      Wait.assertEquals(0, () -> getMessageCount(queueControlC));

      // consume the message from queueA
      ClientConsumer consumer = session.createConsumer(queueA);
      ClientMessage m1 = consumer.receive(500);
      ClientMessage m2 = consumer.receive(500);
      ClientMessage m3 = consumer.receive(500);

      m1.acknowledge();
      m2.acknowledge();
      m3.acknowledge();

      consumer.close();
      session.deleteQueue(queueA);
      session.deleteQueue(queueB);
      session.deleteQueue(queueC);

   }

   @Test
   public void testMoveMessagesToUnknownQueue() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString unknownQueue = RandomUtil.randomSimpleString();

      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);
      ClientProducer producer = session.createProducer(address);

      // send on queue
      ClientMessage message = session.createMessage(durable);
      SimpleString key = RandomUtil.randomSimpleString();
      long value = RandomUtil.randomLong();
      message.putLongProperty(key, value);
      producer.send(message);

      QueueControl queueControl = createManagementControl(address, queue);
      assertMessageMetrics(queueControl, 1, durable);

      // moved all messages to unknown queue
      try {
         queueControl.moveMessages(null, unknownQueue.toString());
         Assert.fail("operation must fail if the other queue does not exist");
      } catch (Exception e) {
      }
      Assert.assertEquals(1, getMessageCount(queueControl));
      assertMessageMetrics(queueControl, 1, durable);

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

      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);
      session.createQueue(otherAddress, RoutingType.MULTICAST,otherQueue, null, durable);
      ClientProducer producer = session.createProducer(address);

      // send on queue
      ClientMessage matchingMessage = session.createMessage(durable);
      matchingMessage.putLongProperty(key, matchingValue);
      producer.send(matchingMessage);
      ClientMessage unmatchingMessage = session.createMessage(durable);
      unmatchingMessage.putLongProperty(key, unmatchingValue);
      producer.send(unmatchingMessage);

      QueueControl queueControl = createManagementControl(address, queue);
      assertMessageMetrics(queueControl, 2, durable);

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

      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);
      session.createQueue(otherAddress, RoutingType.MULTICAST, otherQueue, null, durable);
      ClientProducer producer = session.createProducer(address);

      // send 2 messages on queue
      producer.send(session.createMessage(durable));
      producer.send(session.createMessage(durable));

      QueueControl queueControl = createManagementControl(address, queue);
      QueueControl otherQueueControl = createManagementControl(otherAddress, otherQueue);
      assertMessageMetrics(queueControl, 2, durable);
      assertMessageMetrics(otherQueueControl, 0, durable);

      // the message IDs are set on the server
      Map<String, Object>[] messages = queueControl.listMessages(null);
      Assert.assertEquals(2, messages.length);
      long messageID = (Long) messages[0].get("messageID");

      boolean moved = queueControl.moveMessage(messageID, otherQueue.toString());
      Assert.assertTrue(moved);
      assertMessageMetrics(queueControl, 1, durable);
      assertMessageMetrics(otherQueueControl, 1, durable);

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

      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);
      ClientProducer producer = session.createProducer(address);

      // send 2 messages on queue
      producer.send(session.createMessage(durable));

      QueueControl queueControl = createManagementControl(address, queue);
      assertMessageMetrics(queueControl, 1, durable);

      // the message IDs are set on the server
      Map<String, Object>[] messages = queueControl.listMessages(null);
      Assert.assertEquals(1, messages.length);
      long messageID = (Long) messages[0].get("messageID");

      // moved all messages to unknown queue
      try {
         queueControl.moveMessage(messageID, unknownQueue.toString());
         Assert.fail("operation must fail if the other queue does not exist");
      } catch (Exception e) {
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

      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);
      ClientProducer producer = session.createProducer(address);

      // send on queue
      ClientMessage matchingMessage = session.createMessage(durable);
      matchingMessage.putLongProperty(key, matchingValue);
      producer.send(matchingMessage);
      ClientMessage unmatchingMessage = session.createMessage(durable);
      unmatchingMessage.putLongProperty(key, unmatchingValue);
      producer.send(unmatchingMessage);

      QueueControl queueControl = createManagementControl(address, queue);
      assertMessageMetrics(queueControl, 2, durable);

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

      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);
      ClientProducer producer = session.createProducer(address);

      // send on queue
      ClientMessage matchingMessage = session.createMessage(durable);
      matchingMessage.putLongProperty(key, matchingValue);
      producer.send(matchingMessage);
      ClientMessage unmatchingMessage = session.createMessage(durable);
      unmatchingMessage.putLongProperty(key, unmatchingValue);
      producer.send(unmatchingMessage);

      QueueControl queueControl = createManagementControl(address, queue);
      assertMessageMetrics(queueControl, 2, durable);

      // removed matching messages to otherQueue
      int removedMatchedMessagesCount = queueControl.removeMessages(5, key + " =" + matchingValue);
      Assert.assertEquals(1, removedMatchedMessagesCount);
      assertMessageMetrics(queueControl, 1, durable);

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

      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);
      ClientProducer producer = session.createProducer(address);

      // send on queue
      producer.send(session.createMessage(durable));
      producer.send(session.createMessage(durable));

      QueueControl queueControl = createManagementControl(address, queue);
      assertMessageMetrics(queueControl, 2, durable);

      // removed matching messages to otherQueue
      int removedMatchedMessagesCount = queueControl.removeMessages(null);
      Assert.assertEquals(2, removedMatchedMessagesCount);
      assertMessageMetrics(queueControl, 0, durable);

      session.deleteQueue(queue);
   }

   @Test
   public void testRemoveAllMessages() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);
      ClientProducer producer = session.createProducer(address);

      // send on queue
      producer.send(session.createMessage(durable));
      producer.send(session.createMessage(durable));

      QueueControl queueControl = createManagementControl(address, queue);
      assertMessageMetrics(queueControl, 2, durable);

      // removed matching messages to otherQueue
      int removedMatchedMessagesCount = queueControl.removeAllMessages();
      Assert.assertEquals(2, removedMatchedMessagesCount);
      assertMessageMetrics(queueControl, 0, durable);

      session.deleteQueue(queue);
   }

   @Test
   public void testRemoveAllWithPagingMode() throws Exception {

      final int MESSAGE_SIZE = 1024 * 3; // 3k

      // reset maxSize for Paging mode
      Field maxSizField = PagingManagerImpl.class.getDeclaredField("maxSize");
      maxSizField.setAccessible(true);
      maxSizField.setLong(server.getPagingManager(), 10240);
      clearDataRecreateServerDirs();

      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queueName = RandomUtil.randomSimpleString();

      session.createQueue(address, RoutingType.MULTICAST, queueName, null, durable);

      Queue queue = server.locateQueue(queueName);
      Assert.assertFalse(queue.getPageSubscription().isPaging());

      ClientProducer producer = session.createProducer(address);

      byte[] body = new byte[MESSAGE_SIZE];

      ByteBuffer bb = ByteBuffer.wrap(body);

      for (int j = 1; j <= MESSAGE_SIZE; j++) {
         bb.put(getSamplebyte(j));
      }

      final int numberOfMessages = 8000;
      ClientMessage message;
      for (int i = 0; i < numberOfMessages; i++) {
         message = session.createMessage(true);

         ActiveMQBuffer bodyLocal = message.getBodyBuffer();

         bodyLocal.writeBytes(body);

         producer.send(message);
      }

      Assert.assertTrue(queue.getPageSubscription().isPaging());

      QueueControl queueControl = createManagementControl(address, queueName);
      assertMessageMetrics(queueControl, numberOfMessages, durable);
      int removedMatchedMessagesCount = queueControl.removeAllMessages();
      Assert.assertEquals(numberOfMessages, removedMatchedMessagesCount);
      assertMessageMetrics(queueControl, 0, durable);

      Field queueMemoprySizeField = QueueImpl.class.getDeclaredField("queueMemorySize");
      queueMemoprySizeField.setAccessible(true);
      AtomicInteger queueMemorySize = (AtomicInteger) queueMemoprySizeField.get(queue);
      Assert.assertEquals(0, queueMemorySize.get());

      session.deleteQueue(queueName);
   }

   @Test
   public void testRemoveMessagesWithEmptyFilter() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);
      ClientProducer producer = session.createProducer(address);

      // send on queue
      producer.send(session.createMessage(durable));
      producer.send(session.createMessage(durable));

      QueueControl queueControl = createManagementControl(address, queue);
      assertMessageMetrics(queueControl, 2, durable);

      // removed matching messages to otherQueue
      int removedMatchedMessagesCount = queueControl.removeMessages("");
      Assert.assertEquals(2, removedMatchedMessagesCount);
      assertMessageMetrics(queueControl, 0, durable);

      session.deleteQueue(queue);
   }

   @Test
   public void testRemoveMessage() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);
      ClientProducer producer = session.createProducer(address);

      // send 2 messages on queue
      producer.send(session.createMessage(durable));
      producer.send(session.createMessage(durable));

      QueueControl queueControl = createManagementControl(address, queue);
      assertMessageMetrics(queueControl, 2, durable);

      // the message IDs are set on the server
      Map<String, Object>[] messages = queueControl.listMessages(null);
      Assert.assertEquals(2, messages.length);
      long messageID = (Long) messages[0].get("messageID");

      // delete 1st message
      boolean deleted = queueControl.removeMessage(messageID);
      Assert.assertTrue(deleted);
      assertMessageMetrics(queueControl, 1, durable);

      // check there is a single message to consume from queue
      consumeMessages(1, session, queue);

      session.deleteQueue(queue);
   }

   @Test
   public void testRemoveScheduledMessage() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);
      ClientProducer producer = session.createProducer(address);

      // send 2 messages on queue, both scheduled
      long timeout = System.currentTimeMillis() + 5000;
      ClientMessage m1 = session.createMessage(durable);
      m1.putLongProperty(Message.HDR_SCHEDULED_DELIVERY_TIME, timeout);
      producer.send(m1);
      ClientMessage m2 = session.createMessage(durable);
      m2.putLongProperty(Message.HDR_SCHEDULED_DELIVERY_TIME, timeout);
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
      assertScheduledMetrics(queueControl, 1, durable);

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

      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);
      ClientProducer producer = session.createProducer(address);

      // send messages on queue

      for (int i = 0; i < 100; i++) {

         ClientMessage msg = session.createMessage(durable);
         msg.putIntProperty("count", i);
         producer.send(msg);
      }

      ClientConsumer cons = session.createConsumer(queue);
      session.start();
      LinkedList<ClientMessage> msgs = new LinkedList<>();
      for (int i = 0; i < 50; i++) {
         ClientMessage msg = cons.receive(1000);
         msgs.add(msg);
      }

      QueueControl queueControl = createManagementControl(address, queue);
      assertMessageMetrics(queueControl, 100, durable);

      // the message IDs are set on the server
      Map<String, Object>[] messages = queueControl.listMessages(null);
      Assert.assertEquals(50, messages.length);
      int i = Integer.parseInt((messages[0].get("count")).toString());
      assertEquals(50, i);
      long messageID = (Long) messages[0].get("messageID");

      // delete 1st message
      boolean deleted = queueControl.removeMessage(messageID);
      Assert.assertTrue(deleted);
      assertMessageMetrics(queueControl, 99, durable);

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

      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);
      ClientProducer producer = session.createProducer(address);

      // send on queue
      ClientMessage matchingMessage = session.createMessage(durable);
      matchingMessage.putLongProperty(key, matchingValue);
      ClientMessage unmatchingMessage = session.createMessage(durable);
      unmatchingMessage.putLongProperty(key, unmatchingValue);
      producer.send(matchingMessage);
      producer.send(unmatchingMessage);
      producer.send(matchingMessage);

      QueueControl queueControl = createManagementControl(address, queue);
      Assert.assertEquals(3, getMessageCount(queueControl));
      assertMessageMetrics(queueControl, 3, durable);

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

      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);
      ClientProducer producer = session.createProducer(address);

      // send on queue
      for (int i = 0; i < 100; i++) {
         ClientMessage msg = session.createMessage(durable);
         msg.putStringProperty(key, SimpleString.toSimpleString(matchingValue));
         producer.send(msg);
      }

      for (int i = 0; i < 10; i++) {
         ClientMessage msg = session.createMessage(durable);
         msg.putStringProperty(key, SimpleString.toSimpleString(nonMatchingValue));
         producer.send(msg);
      }

      // this is just to guarantee a round trip and avoid in transit messages, so they are all accounted for
      session.commit();

      ClientConsumer consumer = session.createConsumer(queue, SimpleString.toSimpleString("nonExistentProperty like \'%Temp/88\'"));

      session.start();

      assertNull(consumer.receiveImmediate());

      QueueControl queueControl = createManagementControl(address, queue);
      assertMessageMetrics(queueControl, 110, durable);

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

      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);
      ClientProducer producer = session.createProducer(address);

      // send on queue
      ClientMessage matchingMessage = session.createMessage(durable);
      matchingMessage.putLongProperty(key, matchingValue);
      producer.send(matchingMessage);
      ClientMessage unmatchingMessage = session.createMessage(durable);
      unmatchingMessage.putLongProperty(key, unmatchingValue);
      producer.send(unmatchingMessage);

      QueueControl queueControl = createManagementControl(address, queue);
      Assert.assertEquals(2, getMessageCount(queueControl));

      int expiredMessagesCount = queueControl.expireMessages(key + " =" + matchingValue);
      Assert.assertEquals(1, expiredMessagesCount);
      assertMessageMetrics(queueControl, 1, durable);

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

      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);
      session.createQueue(expiryAddress, RoutingType.MULTICAST, expiryQueue, null, durable);
      ClientProducer producer = session.createProducer(address);

      // send on queue
      producer.send(session.createMessage(durable));

      QueueControl queueControl = createManagementControl(address, queue);
      QueueControl expiryQueueControl = createManagementControl(expiryAddress, expiryQueue);
      assertMessageMetrics(queueControl, 1, durable);
      assertMessageMetrics(expiryQueueControl, 0, durable);

      // the message IDs are set on the server
      Map<String, Object>[] messages = queueControl.listMessages(null);
      Assert.assertEquals(1, messages.length);
      long messageID = (Long) messages[0].get("messageID");

      AddressSettings addressSettings = new AddressSettings().setExpiryAddress(expiryAddress);
      server.getAddressSettingsRepository().addMatch(address.toString(), addressSettings);

      boolean expired = queueControl.expireMessage(messageID);
      Assert.assertTrue(expired);
      Thread.sleep(200);
      assertMessageMetrics(queueControl, 0, durable);
      assertMessageMetrics(expiryQueueControl, 1, durable);

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

      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);
      session.createQueue(deadLetterAddress, RoutingType.MULTICAST, deadLetterQueue, null, durable);
      ClientProducer producer = session.createProducer(address);

      // send 2 messages on queue
      producer.send(session.createMessage(durable));
      producer.send(session.createMessage(durable));

      QueueControl queueControl = createManagementControl(address, queue);
      QueueControl deadLetterQueueControl = createManagementControl(deadLetterAddress, deadLetterQueue);
      assertMessageMetrics(queueControl, 2, durable);

      // the message IDs are set on the server
      Map<String, Object>[] messages = queueControl.listMessages(null);
      Assert.assertEquals(2, messages.length);
      long messageID = (Long) messages[0].get("messageID");

      AddressSettings addressSettings = new AddressSettings().setDeadLetterAddress(deadLetterAddress);
      server.getAddressSettingsRepository().addMatch(address.toString(), addressSettings);

      Assert.assertEquals(0, getMessageCount(deadLetterQueueControl));
      boolean movedToDeadLetterAddress = queueControl.sendMessageToDeadLetterAddress(messageID);
      Assert.assertTrue(movedToDeadLetterAddress);
      assertMessageMetrics(queueControl, 1, durable);
      Thread.sleep(200);
      assertMessageMetrics(deadLetterQueueControl, 1, durable);

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

      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);
      ClientProducer producer = session.createProducer(address);

      ClientMessage message = session.createMessage(durable);
      message.setPriority(originalPriority);
      producer.send(message);

      QueueControl queueControl = createManagementControl(address, queue);
      Wait.assertEquals(1, () -> getMessageCount(queueControl));

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

      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);
      ClientProducer producer = session.createProducer(address);

      ClientMessage message = session.createMessage(durable);
      producer.send(message);

      QueueControl queueControl = createManagementControl(address, queue);
      Wait.assertEquals(1, () -> getMessageCount(queueControl));

      // the message IDs are set on the server
      Map<String, Object>[] messages = queueControl.listMessages(null);
      Assert.assertEquals(1, messages.length);
      long messageID = (Long) messages[0].get("messageID");

      try {
         queueControl.changeMessagePriority(messageID, invalidPriority);
         Assert.fail("operation fails when priority value is < 0 or > 9");
      } catch (Exception e) {
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

      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);
      QueueControl queueControl = createManagementControl(address, queue);

      ActiveMQServerControl serverControl = ManagementControlHelper.createActiveMQServerControl(mbeanServer);
      serverControl.enableMessageCounters();
      serverControl.setMessageCounterSamplePeriod(100);

      String jsonString = queueControl.listMessageCounter();
      MessageCounterInfo info = MessageCounterInfo.fromJSON(jsonString);

      Assert.assertEquals(0, info.getDepth());
      Assert.assertEquals(0, info.getCount());

      ClientProducer producer = session.createProducer(address);
      producer.send(session.createMessage(durable));

      Thread.sleep(200);
      jsonString = queueControl.listMessageCounter();
      info = MessageCounterInfo.fromJSON(jsonString);
      Assert.assertEquals(1, info.getDepth());
      Assert.assertEquals(1, info.getDepthDelta());
      Assert.assertEquals(1, info.getCount());
      Assert.assertEquals(1, info.getCountDelta());

      producer.send(session.createMessage(durable));

      Thread.sleep(200);
      jsonString = queueControl.listMessageCounter();
      info = MessageCounterInfo.fromJSON(jsonString);
      Assert.assertEquals(2, info.getDepth());
      Assert.assertEquals(1, info.getDepthDelta());
      Assert.assertEquals(2, info.getCount());
      Assert.assertEquals(1, info.getCountDelta());

      consumeMessages(2, session, queue);

      Thread.sleep(200);
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

      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);
      QueueControl queueControl = createManagementControl(address, queue);

      ActiveMQServerControl serverControl = ManagementControlHelper.createActiveMQServerControl(mbeanServer);
      serverControl.enableMessageCounters();
      serverControl.setMessageCounterSamplePeriod(MessageCounterManagerImpl.MIN_SAMPLE_PERIOD);

      String jsonString = queueControl.listMessageCounter();
      MessageCounterInfo info = MessageCounterInfo.fromJSON(jsonString);

      Assert.assertEquals(0, info.getDepth());
      Assert.assertEquals(0, info.getCount());

      ClientProducer producer = session.createProducer(address);
      producer.send(session.createMessage(durable));

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

      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);
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

      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);
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

      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);
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
      server.createQueue(new SimpleString("q1"), RoutingType.MULTICAST, new SimpleString("q1"), null, durable, false);
      server.createQueue(new SimpleString("q2"), RoutingType.MULTICAST, new SimpleString("q2"), null, durable, false);

      ServerLocator locator = createInVMNonHALocator();

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(true, true);

      ClientProducer prod1 = session.createProducer("q1");

      for (int i = 0; i < 10; i++) {
         ClientMessage msg = session.createMessage(durable);

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
      server.createQueue(new SimpleString("q1"), RoutingType.MULTICAST, new SimpleString("q1"), null, durable, false);
      server.createQueue(new SimpleString("q2"), RoutingType.MULTICAST, new SimpleString("q2"), null, durable, false);

      ServerLocator locator = createInVMNonHALocator();

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(true, true);

      ClientProducer prod1 = session.createProducer("q1");

      int NUMBER_OF_MSGS = 10;

      for (int i = 0; i < NUMBER_OF_MSGS; i++) {
         ClientMessage msg = session.createMessage(durable);

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
         session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);
         QueueControl queueControl = createManagementControl(address, queue);

         ActiveMQServerControl serverControl = ManagementControlHelper.createActiveMQServerControl(mbeanServer);
         serverControl.enableMessageCounters();
         serverControl.setMessageCounterSamplePeriod(counterPeriod);
         Assert.assertFalse(queueControl.isPaused());
         queueControl.pause();
         Assert.assertTrue(queueControl.isPaused());
         queueControl.resume();
         Assert.assertFalse(queueControl.isPaused());
      } catch (Exception e) {
         e.printStackTrace();
      }
   }

   @Test
   public void testResetMessagesAdded() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);

      QueueControl queueControl = createManagementControl(address, queue);
      Assert.assertEquals(0, getMessagesAdded(queueControl));

      ClientProducer producer = session.createProducer(address);
      producer.send(session.createMessage(durable));
      Wait.assertEquals(1, () -> getMessagesAdded(queueControl));
      producer.send(session.createMessage(durable));
      Wait.assertEquals(2, () -> getMessagesAdded(queueControl));

      consumeMessages(2, session, queue);

      Wait.assertEquals(2, () -> getMessagesAdded(queueControl));

      queueControl.resetMessagesAdded();

      Wait.assertEquals(0, () -> getMessagesAdded(queueControl));

      session.deleteQueue(queue);
   }

   @Test
   public void testResetMessagesAcknowledged() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);

      QueueControl queueControl = createManagementControl(address, queue);
      Assert.assertEquals(0, queueControl.getMessagesAcknowledged());

      ClientProducer producer = session.createProducer(address);
      producer.send(session.createMessage(durable));
      consumeMessages(1, session, queue);
      Assert.assertEquals(1, queueControl.getMessagesAcknowledged());
      producer.send(session.createMessage(durable));
      consumeMessages(1, session, queue);
      Assert.assertEquals(2, queueControl.getMessagesAcknowledged());

      queueControl.resetMessagesAcknowledged();

      Assert.assertEquals(0, queueControl.getMessagesAcknowledged());

      session.deleteQueue(queue);
   }

   @Test
   public void testResetMessagesExpired() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);

      QueueControl queueControl = createManagementControl(address, queue);
      Assert.assertEquals(0, queueControl.getMessagesExpired());

      ClientProducer producer = session.createProducer(address);
      ClientMessage message = session.createMessage(durable);
      producer.send(message);

      // the message IDs are set on the server
      Map<String, Object>[] messages;
      Wait.assertEquals(1, () -> queueControl.listMessages(null).length);
      messages = queueControl.listMessages(null);
      long messageID = (Long) messages[0].get("messageID");

      queueControl.expireMessage(messageID);
      Assert.assertEquals(1, queueControl.getMessagesExpired());

      message = session.createMessage(durable);
      producer.send(message);

      Queue serverqueue = server.locateQueue(queue);

      Wait.assertEquals(1, serverqueue::getMessageCount);

      // the message IDs are set on the server
      messages = queueControl.listMessages(null);
      Assert.assertEquals(1, messages.length);
      messageID = (Long) messages[0].get("messageID");

      queueControl.expireMessage(messageID);
      Assert.assertEquals(2, queueControl.getMessagesExpired());

      queueControl.resetMessagesExpired();

      Assert.assertEquals(0, queueControl.getMessagesExpired());

      session.deleteQueue(queue);
   }

   @Test
   public void testResetMessagesKilled() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);

      QueueControl queueControl = createManagementControl(address, queue);
      Assert.assertEquals(0, queueControl.getMessagesExpired());

      ClientProducer producer = session.createProducer(address);
      ClientMessage message = session.createMessage(durable);
      producer.send(message);

      Wait.assertEquals(1, queueControl::getMessageCount);
      // the message IDs are set on the server
      Map<String, Object>[] messages = queueControl.listMessages(null);
      Assert.assertEquals(1, messages.length);
      long messageID = (Long) messages[0].get("messageID");

      queueControl.sendMessageToDeadLetterAddress(messageID);
      Assert.assertEquals(1, queueControl.getMessagesKilled());
      assertMessageMetrics(queueControl, 0, durable);

      message = session.createMessage(false);
      producer.send(message);

      // send to DLA the old-fashioned way
      ClientConsumer consumer = session.createConsumer(queue);
      for (int i = 0; i < server.getAddressSettingsRepository().getMatch(queue.toString()).getMaxDeliveryAttempts(); i++) {
         message = consumer.receive(500);
         assertNotNull(message);
         message.acknowledge();
         session.rollback();
      }

      consumer.close();

      Assert.assertEquals(2, queueControl.getMessagesKilled());

      queueControl.resetMessagesKilled();

      Assert.assertEquals(0, queueControl.getMessagesKilled());

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
      this.server.createQueue(testQueueName, RoutingType.MULTICAST, testQueueName, null, durable, false);

      Notification notif = listener.getNotification();

      System.out.println("got notif: " + notif);
      assertEquals(CoreNotificationType.BINDING_ADDED.toString(), notif.getType());

      this.server.destroyQueue(testQueueName);

      notif = listener.getNotification();
      System.out.println("got notif: " + notif);
      assertEquals(CoreNotificationType.BINDING_REMOVED.toString(), notif.getType());

      ActiveMQServerControl control = ManagementControlHelper.createActiveMQServerControl(mbeanServer);

      control.createQueue(testQueueName2, testQueueName2, RoutingType.MULTICAST.toString());

      notif = listener.getNotification();
      System.out.println("got notif: " + notif);
      assertEquals(CoreNotificationType.BINDING_ADDED.toString(), notif.getType());

      control.destroyQueue(testQueueName2);

      notif = listener.getNotification();
      System.out.println("got notif: " + notif);
      assertEquals(CoreNotificationType.BINDING_REMOVED.toString(), notif.getType());
   }

   @Test
   public void testSendMessage() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);

      QueueControl queueControl = createManagementControl(address, queue);

      queueControl.sendMessage(new HashMap<String, String>(), Message.BYTES_TYPE, Base64.encodeBytes("theBody".getBytes()), true, "myUser", "myPassword");
      queueControl.sendMessage(null, Message.BYTES_TYPE, Base64.encodeBytes("theBody".getBytes()), true, "myUser", "myPassword");

      Wait.assertEquals(2, () -> getMessageCount(queueControl));

      // the message IDs are set on the server
      CompositeData[] browse = queueControl.browse(null);

      Assert.assertEquals(2, browse.length);

      byte[] body = (byte[]) browse[0].get(BODY);

      Assert.assertNotNull(body);

      Assert.assertEquals(new String(body), "theBody");

      body = (byte[]) browse[1].get(BODY);

      Assert.assertNotNull(body);

      Assert.assertEquals(new String(body), "theBody");
   }

   @Test
   public void testResetGroups() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);

      QueueControl queueControl = createManagementControl(address, queue);

      ClientConsumer consumer = session.createConsumer(queue);
      Assert.assertEquals(1, queueControl.getConsumerCount());
      consumer.setMessageHandler(new MessageHandler() {
         @Override
         public void onMessage(ClientMessage message) {
            System.out.println(message);
         }
      });
      session.start();

      ClientProducer producer = session.createProducer(address);
      producer.send(session.createMessage(durable).putStringProperty(Message.HDR_GROUP_ID, "group1"));
      producer.send(session.createMessage(durable).putStringProperty(Message.HDR_GROUP_ID, "group2"));
      producer.send(session.createMessage(durable).putStringProperty(Message.HDR_GROUP_ID, "group3"));

      Wait.assertEquals(3, () -> getGroupCount(queueControl));

      queueControl.resetGroup("group1");

      Wait.assertEquals(2, () -> getGroupCount(queueControl));

      producer.send(session.createMessage(durable).putStringProperty(Message.HDR_GROUP_ID, "group1"));

      Wait.assertEquals(3, () -> getGroupCount(queueControl));

      queueControl.resetAllGroups();

      Wait.assertEquals(0, () -> getGroupCount(queueControl));

      consumer.close();
      session.deleteQueue(queue);
   }

   @Test
   public void testGetScheduledCountOnRemove() throws Exception {
      long delay = Integer.MAX_VALUE;
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, RoutingType.MULTICAST, queue, null, durable);

      QueueControl queueControl = createManagementControl(address, queue);
      Assert.assertEquals(0, queueControl.getScheduledCount());

      Field queueMemorySizeField = QueueImpl.class.getDeclaredField("queueMemorySize");
      queueMemorySizeField.setAccessible(true);
      final LocalQueueBinding binding = (LocalQueueBinding) server.getPostOffice().getBinding(queue);
      Queue q = binding.getQueue();
      AtomicInteger queueMemorySize1 = (AtomicInteger) queueMemorySizeField.get(q);
      assertEquals(0, queueMemorySize1.get());

      ClientProducer producer = session.createProducer(address);
      ClientMessage message = session.createMessage(durable);
      message.putLongProperty(Message.HDR_SCHEDULED_DELIVERY_TIME, System.currentTimeMillis() + delay);
      producer.send(message);

      queueControl.removeAllMessages();

      Assert.assertEquals(0, queueControl.getMessageCount());

      //Verify that original queue has a memory size of 0
      assertEquals(0, queueMemorySize1.get());

      session.deleteQueue(queue);
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

   protected long getFirstMessageId(final QueueControl queueControl) throws Exception {
      JsonArray array = JsonUtil.readJsonArray(queueControl.getFirstMessageAsJSON());
      JsonObject object = (JsonObject) array.get(0);
      return object.getJsonNumber("messageID").longValue();
   }

   protected void assertMessageMetrics(final QueueControl queueControl, long messageCount, boolean durable) throws Exception {
      assertMetrics(queueControl, messageCount, durable, queueControl::getMessageCount,
            queueControl::getPersistentSize, queueControl::getDurableMessageCount, queueControl::getDurablePersistentSize);
   }

   protected void assertScheduledMetrics(final QueueControl queueControl, long messageCount, boolean durable) throws Exception {
      assertMetrics(queueControl, messageCount, durable, queueControl::getScheduledCount,
            queueControl::getScheduledSize, queueControl::getDurableScheduledCount, queueControl::getDurableScheduledSize);
   }

   protected void assertDeliveringMetrics(final QueueControl queueControl, long messageCount, boolean durable) throws Exception {
      assertMetrics(queueControl, messageCount, durable, queueControl::getDeliveringCount,
            queueControl::getDeliveringSize, queueControl::getDurableDeliveringCount, queueControl::getDurableDeliveringSize);
   }

   protected void assertMetrics(final QueueControl queueControl, long messageCount, boolean durable,
                                Supplier<Number> count, Supplier<Number> size,
                                Supplier<Number> durableCount, Supplier<Number> durableSize) throws Exception {

      //make sure count stat equals message count
      Assert.assertTrue(Wait.waitFor(() -> count.get().longValue() == messageCount, 3 * 1000, 100));

      if (messageCount > 0) {
         //verify size stat greater than 0
         Assert.assertTrue(Wait.waitFor(() -> size.get().longValue() > 0, 3 * 1000, 100));

         //If durable then make sure durable count and size are correct
         if (durable) {
            Wait.assertEquals(messageCount, () -> durableCount.get().longValue(), 3 * 1000, 100);
            Assert.assertTrue(Wait.waitFor(() -> durableSize.get().longValue() > 0, 3 * 1000, 100));
         } else {
            Wait.assertEquals(0L, () -> durableCount.get().longValue(), 3 * 1000, 100);
            Wait.assertEquals(0L, () -> durableSize.get().longValue(), 3 * 1000, 100);
         }
      } else {
         Wait.assertEquals(0L, () -> count.get().longValue(), 3 * 1000, 100);
         Wait.assertEquals(0L, () -> durableCount.get().longValue(), 3 * 1000, 100);
         Wait.assertEquals(0L, () -> size.get().longValue(), 3 * 1000, 100);
         Wait.assertEquals(0L, () -> durableSize.get().longValue(), 3 * 1000, 100);
      }
   }
}
