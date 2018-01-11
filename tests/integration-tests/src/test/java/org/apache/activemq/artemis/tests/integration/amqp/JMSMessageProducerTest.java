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

import java.util.Random;
import java.util.UUID;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.junit.Assert;
import org.junit.Test;

public class JMSMessageProducerTest extends JMSClientTestSupport {

   @Test(timeout = 30000)
   public void testAnonymousProducer() throws Exception {
      Connection connection = createConnection();

      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue queue1 = session.createQueue(getQueueName(1));
         Queue queue2 = session.createQueue(getQueueName(2));
         MessageProducer p = session.createProducer(null);

         TextMessage message = session.createTextMessage();
         message.setText("hello");
         p.send(queue1, message);
         p.send(queue2, message);

         {
            MessageConsumer consumer = session.createConsumer(queue1);
            Message msg = consumer.receive(2000);
            assertNotNull(msg);
            assertTrue(msg instanceof TextMessage);
            consumer.close();
         }
         {
            MessageConsumer consumer = session.createConsumer(queue2);
            Message msg = consumer.receive(2000);
            assertNotNull(msg);
            assertTrue(msg instanceof TextMessage);
            consumer.close();
         }
      } finally {
         connection.close();
      }
   }

   @Test(timeout = 30000)
   public void testAnonymousProducerWithAutoCreation() throws Exception {
      Connection connection = createConnection();

      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Topic topic = session.createTopic(UUID.randomUUID().toString());
         MessageProducer p = session.createProducer(null);

         TextMessage message = session.createTextMessage();
         message.setText("hello");
         // this will auto-create the address
         p.send(topic, message);

         {
            MessageConsumer consumer = session.createConsumer(topic);
            p.send(topic, message);
            Message msg = consumer.receive(2000);
            assertNotNull(msg);
            assertTrue(msg instanceof TextMessage);
            consumer.close();
         }
      } finally {
         connection.close();
      }
   }

   @Test(timeout = 60000)
   public void testAnonymousProducerAcrossManyDestinations() throws Exception {
      Connection connection = createConnection();

      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = session.createProducer(null);

         for (int i = 0; i < getPrecreatedQueueSize(); i++) {
            javax.jms.Queue target = session.createQueue(getQueueName(i));
            TextMessage message = session.createTextMessage("message for " + target.getQueueName());
            p.send(target, message);
         }

         connection.start();

         MessageConsumer messageConsumer = session.createConsumer(session.createQueue(getQueueName()));
         Message m = messageConsumer.receive(200);
         Assert.assertNull(m);

         for (int i = 0; i < getPrecreatedQueueSize(); i++) {
            javax.jms.Queue target = session.createQueue(getQueueName(i));
            MessageConsumer consumer = session.createConsumer(target);
            TextMessage tm = (TextMessage) consumer.receive(2000);
            assertNotNull(tm);
            assertEquals("message for " + target.getQueueName(), tm.getText());
            consumer.close();
         }
      } finally {
         connection.close();
      }
   }

   @Test(timeout = 60000)
   public void testSendingBigMessage() throws Exception {
      Connection connection = createConnection();

      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue queue = session.createQueue(getQueueName());
         MessageProducer sender = session.createProducer(queue);

         String body = createMessage(10240);
         sender.send(session.createTextMessage(body));
         connection.start();

         MessageConsumer consumer = session.createConsumer(queue);
         TextMessage m = (TextMessage) consumer.receive(5000);

         assertEquals(body, m.getText());
      } finally {
         if (connection != null) {
            connection.close();
         }
      }
   }

   @Test(timeout = 60000)
   public void testSendWithTimeToLiveExpiresToDLQ() throws Exception {
      Connection connection = createConnection();

      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue queue = session.createQueue(getQueueName());

         MessageProducer sender = session.createProducer(queue);
         sender.setTimeToLive(1);

         Message message = session.createMessage();
         sender.send(message);
         connection.start();

         MessageConsumer consumer = session.createConsumer(session.createQueue(getDeadLetterAddress()));
         Message m = consumer.receive(10000);
         assertNotNull(m);
         consumer.close();

         consumer = session.createConsumer(queue);
         m = consumer.receiveNoWait();
         assertNull(m);
         consumer.close();
      } finally {
         if (connection != null) {
            connection.close();
         }
      }
   }

   @Test(timeout = 60000)
   public void testReplyToUsingQueue() throws Throwable {
      Connection connection = createConnection();

      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         TemporaryQueue queue = session.createTemporaryQueue();
         MessageProducer p = session.createProducer(queue);

         TextMessage message = session.createTextMessage();
         message.setText("Message temporary");
         message.setJMSReplyTo(session.createQueue(getQueueName()));
         p.send(message);

         MessageConsumer cons = session.createConsumer(queue);
         connection.start();

         message = (TextMessage) cons.receive(5000);
         assertNotNull(message);
         Destination jmsReplyTo = message.getJMSReplyTo();
         assertNotNull(jmsReplyTo);
         assertNotNull(message);
      } finally {
         connection.close();
      }
   }

   @Test(timeout = 60000)
   public void testReplyToUsingTempQueue() throws Throwable {
      Connection connection = createConnection();

      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         TemporaryQueue queue = session.createTemporaryQueue();
         MessageProducer p = session.createProducer(queue);

         TextMessage message = session.createTextMessage();
         message.setText("Message temporary");
         message.setJMSReplyTo(session.createTemporaryQueue());
         p.send(message);

         MessageConsumer cons = session.createConsumer(queue);
         connection.start();

         message = (TextMessage) cons.receive(5000);
         Destination jmsReplyTo = message.getJMSReplyTo();
         assertNotNull(jmsReplyTo);
         assertNotNull(message);
      } finally {
         connection.close();
      }
   }

   private static String createMessage(int messageSize) {
      final String AB = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
      Random rnd = new Random();
      StringBuilder sb = new StringBuilder(messageSize);
      for (int j = 0; j < messageSize; j++) {
         sb.append(AB.charAt(rnd.nextInt(AB.length())));
      }
      String body = sb.toString();
      return body;
   }
}
