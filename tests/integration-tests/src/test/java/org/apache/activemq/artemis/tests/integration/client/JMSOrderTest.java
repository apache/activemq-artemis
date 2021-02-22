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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.tests.util.JMSTestBase;
import org.apache.activemq.artemis.tests.util.Wait;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.activemq.artemis.tests.util.CFUtil.createConnectionFactory;

@RunWith(value = Parameterized.class)
public class JMSOrderTest extends JMSTestBase {

   String protocol;

   ConnectionFactory protocolCF;

   public JMSOrderTest(String protocol) {
      this.protocol = protocol;
   }

   @Before
   public void setupCF() {
      protocolCF = createConnectionFactory(protocol, "tcp://localhost:61616");
   }

   @Parameterized.Parameters(name = "protocol={0}")
   public static Collection getParameters() {
      return Arrays.asList(new Object[][]{{"AMQP"}, {"OPENWIRE"}, {"CORE"}});
   }

   protected void sendToAmqQueue(int count) throws Exception {
      Connection activemqConnection = protocolCF.createConnection();
      Session amqSession = activemqConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue amqTestQueue = amqSession.createQueue(name.getMethodName());
      sendMessages(activemqConnection, amqTestQueue, count);
      activemqConnection.close();
   }

   public void sendMessages(Connection connection, Destination destination, int count) throws Exception {
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer p = session.createProducer(destination);

      for (int i = 1; i <= count; i++) {
         TextMessage message = session.createTextMessage();
         message.setText("TextMessage: " + i);
         message.setIntProperty("nr", i);
         p.send(message);
      }

      session.close();

   }

   @Test(timeout = 60000)
   public void testReceiveSomeThenRollback() throws Exception {
      Connection connection = protocolCF.createConnection();
      try {
         connection.start();

         int totalCount = 5;
         int consumeBeforeRollback = 2;

         sendToAmqQueue(totalCount);

         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         Queue queue = session.createQueue(name.getMethodName());
         MessageConsumer consumer = session.createConsumer(queue);

         for (int i = 1; i <= consumeBeforeRollback; i++) {
            Message message = consumer.receive(3000);
            assertNotNull(message);
            assertEquals("Unexpected message number", i, message.getIntProperty("nr"));
         }

         session.rollback();

         // Consume again.. the previously consumed messages should get delivered
         // again after the rollback and then the remainder should follow
         List<Integer> messageNumbers = new ArrayList<>();
         for (int i = 1; i <= totalCount; i++) {
            Message message = consumer.receive(3000);
            assertNotNull("Failed to receive message: " + i, message);
            int msgNum = message.getIntProperty("nr");
            messageNumbers.add(msgNum);
         }

         session.commit();

         assertEquals("Unexpected size of list", totalCount, messageNumbers.size());
         for (int i = 0; i < messageNumbers.size(); i++) {
            assertEquals("Unexpected order of messages: " + messageNumbers, Integer.valueOf(i + 1), messageNumbers.get(i));
         }
      } finally {
         connection.close();
      }

   }

   @Test(timeout = 60000)
   public void testReceiveSomeThenClose() throws Exception {
      Connection connection = protocolCF.createConnection();
      try {
         connection.start();

         int totalCount = 5;
         int consumeBeforeRollback = 2;

         sendToAmqQueue(totalCount);

         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         Queue queue = session.createQueue(name.getMethodName());
         MessageConsumer consumer = session.createConsumer(queue);

         for (int i = 1; i <= consumeBeforeRollback; i++) {
            Message message = consumer.receive(3000);
            assertNotNull(message);
            assertEquals("Unexpected message number", i, message.getIntProperty("nr"));
         }

         session.close();

         session = connection.createSession(true, Session.SESSION_TRANSACTED);
         queue = session.createQueue(name.getMethodName());
         consumer = session.createConsumer(queue);

         // Consume again.. the previously consumed messages should get delivered
         // again after the rollback and then the remainder should follow
         List<Integer> messageNumbers = new ArrayList<>();
         for (int i = 1; i <= totalCount; i++) {
            Message message = consumer.receive(3000);
            assertNotNull("Failed to receive message: " + i, message);
            int msgNum = message.getIntProperty("nr");
            messageNumbers.add(msgNum);
         }

         session.commit();

         assertEquals("Unexpected size of list", totalCount, messageNumbers.size());
         for (int i = 0; i < messageNumbers.size(); i++) {
            assertEquals("Unexpected order of messages: " + messageNumbers, Integer.valueOf(i + 1), messageNumbers.get(i));
         }
      } finally {
         connection.close();
      }

   }

   @Test
   public void testMultipleConsumersRollback() throws Exception {
      internalMultipleConsumers(true);
   }

   @Test
   public void testMultipleConsumersClose() throws Exception {
      internalMultipleConsumers(false);
   }

   private void internalMultipleConsumers(final boolean rollback) throws Exception {


      org.apache.activemq.artemis.core.server.Queue serverQueue = server.createQueue(new QueueConfiguration(getName()).setRoutingType(RoutingType.ANYCAST).setDurable(false));

      int numberOfMessages = 100;
      int numberOfConsumers = 3;

      ConnectionFactory factory = CFUtil.createConnectionFactory(protocol, "tcp://localhost:61616");
      final javax.jms.Queue jmsQueue;

      try (Connection connection = factory.createConnection()) {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         jmsQueue = session.createQueue(getName());
         MessageProducer producer = session.createProducer(jmsQueue);

         for (int i = 0; i < numberOfMessages; i++) {
            TextMessage message = session.createTextMessage("test " + i);
            message.setIntProperty("i", i);
            producer.send(message);
         }
      }

      Wait.assertEquals(numberOfMessages, serverQueue::getMessageCount);

      AtomicBoolean running = new AtomicBoolean(true);
      AtomicInteger errors = new AtomicInteger(0);
      Runnable r = () -> {
         try (Connection c = factory.createConnection()) {
            Session s = c.createSession(true, Session.SESSION_TRANSACTED);
            MessageConsumer cs = s.createConsumer(jmsQueue);
            c.start();
            int rollbacks = 0;
            while (running.get()) {
               TextMessage txt = (TextMessage)cs.receive(500);
               if (txt != null) {
                  if (rollback) {
                     s.rollback();
                     rollbacks++;

                     if (rollbacks >= 3) {
                        break;
                     }
                  }
               } else {
                  return;
               }
            }
         } catch (Throwable e) {
            e.printStackTrace();
            errors.incrementAndGet();
            running.set(false);
         }
      };

      Thread[] threads = new Thread[numberOfConsumers];

      for (int i = 0; i < numberOfConsumers; i++) {
         threads[i] = new Thread(r, "consumer " + i);
         threads[i].start();
      }

      for (Thread t : threads) {
         t.join();
      }

      Assert.assertEquals(0, errors.get());

      Wait.assertEquals(numberOfMessages, serverQueue::getMessageCount);

      try (Connection c = factory.createConnection()) {
         Session s = c.createSession(true, Session.SESSION_TRANSACTED);
         MessageConsumer cs = s.createConsumer(jmsQueue);
         c.start();

         for (int i = 0; i < numberOfMessages; i++) {
            TextMessage message = (TextMessage) cs.receive(1000);
            Assert.assertNotNull(message);
            Assert.assertEquals(i, message.getIntProperty("i"));
         }

         Assert.assertNull(cs.receiveNoWait());
      }

   }


}
