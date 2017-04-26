/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.amqp;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.policy.JmsDefaultPrefetchPolicy;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JMSMessageConsumerTest extends JMSClientTestSupport {

   protected static final Logger LOG = LoggerFactory.getLogger(JMSMessageConsumerTest.class);

   @Test(timeout = 60000)
   public void testSelector() throws Exception {
      Connection connection = createConnection();

      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         javax.jms.Queue queue = session.createQueue(getQueueName());
         MessageProducer producer = session.createProducer(queue);

         TextMessage message = session.createTextMessage();
         message.setText("msg:0");
         producer.send(message);
         message = session.createTextMessage();
         message.setText("msg:1");
         message.setStringProperty("color", "RED");
         producer.send(message);

         connection.start();

         MessageConsumer messageConsumer = session.createConsumer(queue, "color = 'RED'");
         TextMessage m = (TextMessage) messageConsumer.receive(5000);
         assertNotNull(m);
         assertEquals("msg:1", m.getText());
         assertEquals(m.getStringProperty("color"), "RED");
      } finally {
         connection.close();
      }
   }

   @SuppressWarnings("rawtypes")
   @Test(timeout = 30000)
   public void testSelectorsWithJMSType() throws Exception {
      Connection connection = createConnection();

      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         javax.jms.Queue queue = session.createQueue(getQueueName());
         MessageProducer p = session.createProducer(queue);

         TextMessage message = session.createTextMessage();
         message.setText("text");
         p.send(message, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);

         TextMessage message2 = session.createTextMessage();
         String type = "myJMSType";
         message2.setJMSType(type);
         message2.setText("text + type");
         p.send(message2, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);

         QueueBrowser browser = session.createBrowser(queue);
         Enumeration enumeration = browser.getEnumeration();
         int count = 0;
         while (enumeration.hasMoreElements()) {
            Message m = (Message) enumeration.nextElement();
            assertTrue(m instanceof TextMessage);
            count++;
         }

         assertEquals(2, count);

         MessageConsumer consumer = session.createConsumer(queue, "JMSType = '" + type + "'");
         Message msg = consumer.receive(2000);
         assertNotNull(msg);
         assertTrue(msg instanceof TextMessage);
         assertEquals("Unexpected JMSType value", type, msg.getJMSType());
         assertEquals("Unexpected message content", "text + type", ((TextMessage) msg).getText());
      } finally {
         connection.close();
      }
   }

   @SuppressWarnings("rawtypes")
   @Test(timeout = 30000)
   public void testSelectorsWithJMSPriority() throws Exception {
      Connection connection = createConnection();

      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         javax.jms.Queue queue = session.createQueue(getQueueName());
         MessageProducer p = session.createProducer(queue);

         TextMessage message = session.createTextMessage();
         message.setText("hello");
         p.send(message, DeliveryMode.PERSISTENT, 5, 0);

         message = session.createTextMessage();
         message.setText("hello + 9");
         p.send(message, DeliveryMode.PERSISTENT, 9, 0);

         QueueBrowser browser = session.createBrowser(queue);
         Enumeration enumeration = browser.getEnumeration();
         int count = 0;
         while (enumeration.hasMoreElements()) {
            Message m = (Message) enumeration.nextElement();
            assertTrue(m instanceof TextMessage);
            count++;
         }

         assertEquals(2, count);

         MessageConsumer consumer = session.createConsumer(queue, "JMSPriority > 8");
         Message msg = consumer.receive(2000);
         assertNotNull(msg);
         assertTrue(msg instanceof TextMessage);
         assertEquals("hello + 9", ((TextMessage) msg).getText());
      } finally {
         connection.close();
      }
   }

   @Test(timeout = 60000)
   public void testJMSSelectorFiltersJMSMessageID() throws Exception {
      Connection connection = createConnection();

      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         javax.jms.Queue queue = session.createQueue(getQueueName());
         MessageProducer producer = session.createProducer(queue);

         // Send one to receive
         TextMessage message = session.createTextMessage();
         producer.send(message);

         // Send another to filter
         producer.send(session.createTextMessage());

         connection.start();

         // First one should make it through
         MessageConsumer messageConsumer = session.createConsumer(queue, "JMSMessageID = '" + message.getJMSMessageID() + "'");
         TextMessage m = (TextMessage) messageConsumer.receive(5000);
         assertNotNull(m);
         assertEquals(message.getJMSMessageID(), m.getJMSMessageID());

         // The second one should not be received.
         assertNull(messageConsumer.receive(1000));
      } finally {
         connection.close();
      }
   }

   @Test(timeout = 60000)
   public void testZeroPrefetchWithTwoConsumers() throws Exception {
      JmsConnection connection = (JmsConnection) createConnection();
      ((JmsDefaultPrefetchPolicy) connection.getPrefetchPolicy()).setAll(0);
      connection.start();

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      javax.jms.Queue queue = session.createQueue(getQueueName());

      MessageProducer producer = session.createProducer(queue);
      producer.send(session.createTextMessage("Msg1"));
      producer.send(session.createTextMessage("Msg2"));

      // now lets receive it
      MessageConsumer consumer1 = session.createConsumer(queue);
      MessageConsumer consumer2 = session.createConsumer(queue);
      TextMessage answer = (TextMessage) consumer1.receive(5000);
      assertNotNull(answer);
      assertEquals("Should have received a message!", answer.getText(), "Msg1");
      answer = (TextMessage) consumer2.receive(5000);
      assertNotNull(answer);
      assertEquals("Should have received a message!", answer.getText(), "Msg2");

      answer = (TextMessage) consumer2.receiveNoWait();
      assertNull("Should have not received a message!", answer);
   }

   @Test(timeout = 30000)
   public void testProduceAndConsumeLargeNumbersOfTopicMessagesClientAck() throws Exception {
      doTestProduceAndConsumeLargeNumbersOfMessages(true, Session.CLIENT_ACKNOWLEDGE);
   }

   @Test(timeout = 30000)
   public void testProduceAndConsumeLargeNumbersOfQueueMessagesClientAck() throws Exception {
      doTestProduceAndConsumeLargeNumbersOfMessages(false, Session.CLIENT_ACKNOWLEDGE);
   }

   @Test(timeout = 30000)
   public void testProduceAndConsumeLargeNumbersOfTopicMessagesAutoAck() throws Exception {
      doTestProduceAndConsumeLargeNumbersOfMessages(true, Session.AUTO_ACKNOWLEDGE);
   }

   @Test(timeout = 30000)
   public void testProduceAndConsumeLargeNumbersOfQueueMessagesAutoAck() throws Exception {
      doTestProduceAndConsumeLargeNumbersOfMessages(false, Session.AUTO_ACKNOWLEDGE);
   }

   public void doTestProduceAndConsumeLargeNumbersOfMessages(boolean topic, int ackMode) throws Exception {

      final int MSG_COUNT = 1000;
      final CountDownLatch done = new CountDownLatch(MSG_COUNT);

      JmsConnection connection = (JmsConnection) createConnection();
      connection.setForceAsyncSend(true);
      connection.start();

      Session session = connection.createSession(false, ackMode);
      final Destination destination;
      if (topic) {
         destination = session.createTopic(getTopicName());
      } else {
         destination = session.createQueue(getQueueName());
      }

      MessageConsumer consumer = session.createConsumer(destination);
      consumer.setMessageListener(new MessageListener() {

         @Override
         public void onMessage(Message message) {
            try {
               message.acknowledge();
               done.countDown();
            } catch (JMSException ex) {
               LOG.info("Caught exception.", ex);
            }
         }
      });

      MessageProducer producer = session.createProducer(destination);

      TextMessage textMessage = session.createTextMessage();
      textMessage.setText("messageText");

      for (int i = 0; i < MSG_COUNT; i++) {
         producer.send(textMessage);
      }

      assertTrue("Did not receive all messages: " + MSG_COUNT, done.await(15, TimeUnit.SECONDS));
   }

   @Test(timeout = 60000)
   public void testPrefetchedMessagesAreNotConsumedOnConsumerClose() throws Exception {
      final int NUM_MESSAGES = 10;

      Connection connection = createConnection();

      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         javax.jms.Queue queue = session.createQueue(getQueueName());
         MessageProducer producer = session.createProducer(queue);

         byte[] bytes = new byte[2048];
         new Random().nextBytes(bytes);
         for (int i = 0; i < NUM_MESSAGES; i++) {
            TextMessage message = session.createTextMessage();
            message.setText("msg:" + i);
            producer.send(message);
         }

         connection.close();

         Queue queueView = getProxyToQueue(getQueueName());
         assertTrue("Not all messages were enqueud", Wait.waitFor(() -> queueView.getMessageCount() == NUM_MESSAGES));

         // Create a consumer and prefetch the messages
         connection = createConnection();
         session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer consumer = session.createConsumer(queue);

         Thread.sleep(100);

         consumer.close();
         connection.close();

         assertTrue("Not all messages were enqueud", Wait.waitFor(() -> queueView.getMessageCount() == NUM_MESSAGES));
      } finally {
         connection.close();
      }
   }

   @Test(timeout = 60000)
   public void testMessagesReceivedInParallel() throws Throwable {
      final int numMessages = 50000;
      long time = System.currentTimeMillis();

      final ArrayList<Throwable> exceptions = new ArrayList<>();

      Thread t = new Thread(new Runnable() {
         @Override
         public void run() {
            Connection connectionConsumer = null;
            try {
               connectionConsumer = createConnection();
               connectionConsumer.start();
               Session sessionConsumer = connectionConsumer.createSession(false, Session.AUTO_ACKNOWLEDGE);
               final javax.jms.Queue queue = sessionConsumer.createQueue(getQueueName());
               final MessageConsumer consumer = sessionConsumer.createConsumer(queue);

               long n = 0;
               int count = numMessages;
               while (count > 0) {
                  try {
                     if (++n % 1000 == 0) {
                        System.out.println("received " + n + " messages");
                     }

                     Message m = consumer.receive(5000);
                     Assert.assertNotNull("Could not receive message count=" + count + " on consumer", m);
                     count--;
                  } catch (JMSException e) {
                     e.printStackTrace();
                     break;
                  }
               }
            } catch (Throwable e) {
               exceptions.add(e);
               e.printStackTrace();
            } finally {
               try {
                  connectionConsumer.close();
               } catch (Throwable ignored) {
                  // NO OP
               }
            }
         }
      });

      Connection connection = createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      javax.jms.Queue queue = session.createQueue(getQueueName());

      t.start();

      MessageProducer p = session.createProducer(queue);
      p.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      for (int i = 0; i < numMessages; i++) {
         BytesMessage message = session.createBytesMessage();
         message.writeUTF("Hello world!!!!" + i);
         message.setIntProperty("count", i);
         p.send(message);
      }

      // Wait for the consumer thread to completely read the Queue
      t.join();

      if (!exceptions.isEmpty()) {
         throw exceptions.get(0);
      }

      Queue queueView = getProxyToQueue(getQueueName());

      connection.close();
      assertTrue("Not all messages consumed", Wait.waitFor(() -> queueView.getMessageCount() == 0));

      long taken = (System.currentTimeMillis() - time);
      System.out.println("Microbenchamrk ran in " + taken + " milliseconds, sending/receiving " + numMessages);

      double messagesPerSecond = ((double) numMessages / (double) taken) * 1000;

      System.out.println(((int) messagesPerSecond) + " messages per second");
   }

   @Test(timeout = 60000)
   public void testClientAckMessages() throws Exception {
      final int numMessages = 10;

      Connection connection = createConnection();

      try {
         long time = System.currentTimeMillis();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         javax.jms.Queue queue = session.createQueue(getQueueName());
         MessageProducer producer = session.createProducer(queue);

         byte[] bytes = new byte[2048];
         new Random().nextBytes(bytes);
         for (int i = 0; i < numMessages; i++) {
            TextMessage message = session.createTextMessage();
            message.setText("msg:" + i);
            producer.send(message);
         }
         connection.close();
         Queue queueView = getProxyToQueue(getQueueName());

         assertTrue("Not all messages enqueued", Wait.waitFor(() -> queueView.getMessageCount() == numMessages));

         // Now create a new connection and receive and acknowledge
         connection = createConnection();
         session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
         MessageConsumer consumer = session.createConsumer(queue);

         for (int i = 0; i < numMessages; i++) {
            Message msg = consumer.receive(5000);
            if (msg == null) {
               System.out.println("ProtonTest.testManyMessages");
            }
            Assert.assertNotNull("" + i, msg);
            Assert.assertTrue("" + msg, msg instanceof TextMessage);
            String text = ((TextMessage) msg).getText();
            // System.out.println("text = " + text);
            Assert.assertEquals(text, "msg:" + i);
            msg.acknowledge();
         }

         consumer.close();
         connection.close();

         // Wait for Acks to be processed and message removed from queue.
         Thread.sleep(500);

         assertTrue("Not all messages consumed", Wait.waitFor(() -> queueView.getMessageCount() == 0));
         long taken = (System.currentTimeMillis() - time) / 1000;
         System.out.println("taken = " + taken);
      } finally {
         connection.close();
      }
   }

   @Test(timeout = 240000)
   public void testTimedOutWaitingForWriteLogOnConsumer() throws Throwable {
      String name = "exampleQueue1";

      final int numMessages = 40;

      Connection connection = createConnection();
      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         javax.jms.Queue queue = session.createQueue(name);
         MessageProducer producer = session.createProducer(queue);
         for (int i = 0; i < numMessages; i++) {
            TextMessage message = session.createTextMessage();
            message.setText("Message temporary");
            producer.send(message);
         }
         producer.close();
         session.close();

         for (int i = 0; i < numMessages; i++) {
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            queue = session.createQueue(name);
            MessageConsumer c = session.createConsumer(queue);
            c.receive(1000);
            producer.close();
            session.close();
         }

         session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         queue = session.createQueue(name);
         MessageConsumer c = session.createConsumer(queue);
         for (int i = 0; i < numMessages; i++) {
            c.receive(1000);
         }
         producer.close();
         session.close();
      } finally {
         connection.close();
      }
   }
}
