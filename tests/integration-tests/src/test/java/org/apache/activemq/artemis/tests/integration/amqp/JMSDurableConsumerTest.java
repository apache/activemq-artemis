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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.tests.util.Wait;
import org.junit.Test;

public class JMSDurableConsumerTest extends JMSClientTestSupport {

   @Override
   protected String getConfiguredProtocols() {
      return "AMQP,OPENWIRE,CORE";
   }

   @Test(timeout = 30000)
   public void testDurableConsumerAsync() throws Exception {
      final CountDownLatch latch = new CountDownLatch(1);
      final AtomicReference<Message> received = new AtomicReference<>();
      String durableClientId = getTopicName() + "-ClientId";

      Connection connection = createConnection(durableClientId);

      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Topic topic = session.createTopic(getTopicName());
         MessageConsumer consumer = session.createDurableSubscriber(topic, "DurbaleTopic");
         consumer.setMessageListener(new MessageListener() {

            @Override
            public void onMessage(Message message) {
               received.set(message);
               latch.countDown();
            }
         });

         MessageProducer producer = session.createProducer(topic);
         producer.setDeliveryMode(DeliveryMode.PERSISTENT);
         connection.start();

         TextMessage message = session.createTextMessage();
         message.setText("hello");
         producer.send(message);

         assertTrue(latch.await(10, TimeUnit.SECONDS));
         assertNotNull("Should have received a message by now.", received.get());
         assertTrue("Should be an instance of TextMessage", received.get() instanceof TextMessage);
      } finally {
         connection.close();
      }
   }

   @Test(timeout = 30000)
   public void testDurableConsumerSync() throws Exception {
      String durableClientId = getTopicName() + "-ClientId";

      Connection connection = createConnection(durableClientId);
      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Topic topic = session.createTopic(getTopicName());
         final MessageConsumer consumer = session.createDurableSubscriber(topic, "DurbaleTopic");
         MessageProducer producer = session.createProducer(topic);
         producer.setDeliveryMode(DeliveryMode.PERSISTENT);
         connection.start();

         TextMessage message = session.createTextMessage();
         message.setText("hello");
         producer.send(message);

         final AtomicReference<Message> msg = new AtomicReference<>();
         assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
               msg.set(consumer.receiveNoWait());
               return msg.get() != null;
            }
         }, TimeUnit.SECONDS.toMillis(25), TimeUnit.MILLISECONDS.toMillis(200)));

         assertNotNull("Should have received a message by now.", msg.get());
         assertTrue("Should be an instance of TextMessage", msg.get() instanceof TextMessage);
      } finally {
         connection.close();
      }
   }

   @Test(timeout = 30000)
   public void testDurableConsumerUnsubscribe() throws Exception {
      String durableClientId = getTopicName() + "-ClientId";

      Connection connection = createConnection(durableClientId);
      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Topic topic = session.createTopic(getTopicName());
         MessageConsumer consumer = session.createDurableSubscriber(topic, "DurbaleTopic");

         assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
               return server.getTotalConsumerCount() == 1;
            }
         }, TimeUnit.SECONDS.toMillis(20), TimeUnit.MILLISECONDS.toMillis(250)));

         consumer.close();

         assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
               return server.getTotalConsumerCount() == 0;
            }
         }, TimeUnit.SECONDS.toMillis(20), TimeUnit.MILLISECONDS.toMillis(250)));

         session.unsubscribe("DurbaleTopic");
         assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
               return server.getTotalConsumerCount() == 0;
            }
         }, TimeUnit.SECONDS.toMillis(20), TimeUnit.MILLISECONDS.toMillis(250)));
      } finally {
         connection.close();
      }
   }

   @Test(timeout = 30000)
   public void testDurableConsumerUnsubscribeWhileNoSubscription() throws Exception {
      Connection connection = createConnection();

      try {
         connection.start();

         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         assertTrue(Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisfied() throws Exception {
               return server.getTotalConsumerCount() == 0;
            }
         }, TimeUnit.SECONDS.toMillis(20), TimeUnit.MILLISECONDS.toMillis(250)));

         try {
            session.unsubscribe("DurbaleTopic");
            fail("Should have thrown as subscription is in use.");
         } catch (JMSException ex) {
         }
      } finally {
         connection.close();
      }
   }

   @Test(timeout = 30000)
   public void testDurableConsumerUnsubscribeWhileActive() throws Exception {
      String durableClientId = getTopicName() + "-ClientId";

      Connection connection = createConnection(durableClientId);
      try {
         connection.start();

         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Topic topic = session.createTopic(getTopicName());
         MessageConsumer consumer = session.createDurableSubscriber(topic, "DurbaleTopic");

         assertNotNull(consumer);
         assertNull(consumer.receive(10));

         try {
            session.unsubscribe("DurbaleTopic");
            fail("Should have thrown as subscription is in use.");
         } catch (JMSException ex) {
         }
      } finally {
         connection.close();
      }
   }


   private void testDurableConsumer(Connection connection1, Connection connection2) throws JMSException {
      try {
         Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Topic topic = session1.createTopic(getTopicName());
         final MessageConsumer consumer1 = session1.createDurableConsumer(topic, "Durable");

         MessageProducer producer = session1.createProducer(topic);
         producer.setDeliveryMode(DeliveryMode.PERSISTENT);
         connection1.start();

         TextMessage message = session1.createTextMessage();
         message.setText("hello");
         producer.send(message);

         Message message1 = consumer1.receive(100);

         assertNotNull("Should have received a message by now.", message1);
         assertTrue("Should be an instance of TextMessage", message1 instanceof TextMessage);

         consumer1.close();

         producer.send(message);

         Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Topic topic2 = session2.createTopic(getTopicName());
         final MessageConsumer consumer2 = session2.createDurableConsumer(topic2, "Durable");
         connection2.start();
         Message message2 = consumer2.receive(100);

         System.out.println((server.getPostOffice().listQueuesForAddress(new SimpleString(getTopicName()))));

         assertNotNull("Should have received a message by now.", message2);
         assertTrue("Should be an instance of TextMessage", message2 instanceof TextMessage);

      } catch (Exception e) {
         e.printStackTrace();
      } finally {
         connection1.close();
         connection2.close();
      }
   }

   @Test(timeout = 30000)
   public void testSharedConsumerWithArtemisClientAndAMQPClient() throws Exception {

      Connection connection = createCoreConnection("clientId"); //CORE
      Connection connection2 = createConnection("clientId"); //AMQP

      testDurableConsumer(connection, connection2);

   }

}
