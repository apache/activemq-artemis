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
package org.apache.activemq.artemis.tests.integration.jms.consumer;

import javax.jms.Connection;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSProducer;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.Enumeration;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSConstants;
import org.apache.activemq.artemis.api.jms.JMSFactoryType;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.tests.integration.IntegrationTestLogger;
import org.apache.activemq.artemis.tests.util.JMSTestBase;
import org.apache.activemq.artemis.utils.ReusableLatch;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JmsConsumerTest extends JMSTestBase {

   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   private static final String Q_NAME = "ConsumerTestQueue";

   private static final String T_NAME = "ConsumerTestTopic";

   private static final String T2_NAME = "ConsumerTestTopic2";

   private javax.jms.Queue jBossQueue;
   private javax.jms.Topic topic;
   private javax.jms.Topic topic2;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      topic = ActiveMQJMSClient.createTopic(T_NAME);
      topic2 = ActiveMQJMSClient.createTopic(T2_NAME);

      jmsServer.createQueue(false, JmsConsumerTest.Q_NAME, null, true, JmsConsumerTest.Q_NAME);
      jmsServer.createTopic(true, T_NAME, "/topic/" + T_NAME);
      jmsServer.createTopic(true, T2_NAME, "/topic/" + T2_NAME);
      cf = ActiveMQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, new TransportConfiguration(INVM_CONNECTOR_FACTORY));
   }

   @Test
   public void testTransactionalSessionRollback() throws Exception {
      conn = cf.createConnection();
      Session sess = conn.createSession(true, Session.SESSION_TRANSACTED);

      MessageProducer prod = sess.createProducer(topic);
      MessageConsumer cons = sess.createConsumer(topic);

      conn.start();

      TextMessage msg1 = sess.createTextMessage("m1");
      TextMessage msg2 = sess.createTextMessage("m2");
      TextMessage msg3 = sess.createTextMessage("m3");

      prod.send(msg1);
      sess.commit();

      prod.send(msg2);
      sess.rollback();

      prod.send(msg3);
      sess.commit();

      TextMessage m1 = (TextMessage) cons.receive(2000);
      Assert.assertNotNull(m1);
      Assert.assertEquals("m1", m1.getText());

      TextMessage m2 = (TextMessage) cons.receive(2000);
      Assert.assertNotNull(m2);
      Assert.assertEquals("m3", m2.getText());

      TextMessage m3 = (TextMessage) cons.receive(2000);
      Assert.assertNull("m3 should be null", m3);

      System.out.println("received m1: " + m1.getText());
      System.out.println("received m2: " + m2.getText());
      System.out.println("received m3: " + m3);
      sess.commit();
   }

   @Test
   public void testPreCommitAcks() throws Exception {
      conn = cf.createConnection();
      Session session = conn.createSession(false, ActiveMQJMSConstants.PRE_ACKNOWLEDGE);
      jBossQueue = ActiveMQJMSClient.createQueue(JmsConsumerTest.Q_NAME);
      MessageProducer producer = session.createProducer(jBossQueue);
      MessageConsumer consumer = session.createConsumer(jBossQueue);
      int noOfMessages = 100;
      for (int i = 0; i < noOfMessages; i++) {
         producer.send(session.createTextMessage("m" + i));
      }

      conn.start();
      for (int i = 0; i < noOfMessages; i++) {
         Message m = consumer.receive(500);
         Assert.assertNotNull(m);
      }

      SimpleString queueName = new SimpleString(JmsConsumerTest.Q_NAME);
      Assert.assertEquals(0, getMessageCount((Queue) server.getPostOffice().getBinding(queueName).getBindable()));
      Assert.assertEquals(0, getMessageCount((Queue) server.getPostOffice().getBinding(queueName).getBindable()));
   }

   @Test
   public void testIndividualACK() throws Exception {
      Connection conn = cf.createConnection();
      Session session = conn.createSession(false, ActiveMQJMSConstants.INDIVIDUAL_ACKNOWLEDGE);
      jBossQueue = ActiveMQJMSClient.createQueue(JmsConsumerTest.Q_NAME);
      MessageProducer producer = session.createProducer(jBossQueue);
      MessageConsumer consumer = session.createConsumer(jBossQueue);
      int noOfMessages = 100;
      for (int i = 0; i < noOfMessages; i++) {
         producer.send(session.createTextMessage("m" + i));
      }

      conn.start();

      // Consume even numbers first
      for (int i = 0; i < noOfMessages; i++) {
         Message m = consumer.receive(500);
         Assert.assertNotNull(m);
         if (i % 2 == 0) {
            m.acknowledge();
         }
      }

      session.close();

      session = conn.createSession(false, ActiveMQJMSConstants.INDIVIDUAL_ACKNOWLEDGE);

      consumer = session.createConsumer(jBossQueue);

      // Consume odd numbers first
      for (int i = 0; i < noOfMessages; i++) {
         if (i % 2 == 0) {
            continue;
         }

         TextMessage m = (TextMessage) consumer.receive(1000);
         Assert.assertNotNull(m);
         m.acknowledge();
         Assert.assertEquals("m" + i, m.getText());
      }

      SimpleString queueName = new SimpleString(JmsConsumerTest.Q_NAME);
      Assert.assertEquals(0, ((Queue) server.getPostOffice().getBinding(queueName).getBindable()).getDeliveringCount());
      Assert.assertEquals(0, getMessageCount((Queue) server.getPostOffice().getBinding(queueName).getBindable()));
      conn.close();
   }

   @Test
   public void testIndividualACKMessageConsumer() throws Exception {
      Connection conn = cf.createConnection();
      Session session = conn.createSession(false, ActiveMQJMSConstants.INDIVIDUAL_ACKNOWLEDGE);
      jBossQueue = ActiveMQJMSClient.createQueue(JmsConsumerTest.Q_NAME);
      MessageProducer producer = session.createProducer(jBossQueue);
      MessageConsumer consumer = session.createConsumer(jBossQueue);
      int noOfMessages = 100;
      for (int i = 0; i < noOfMessages; i++) {
         producer.setPriority(2);
         producer.send(session.createTextMessage("m" + i));
      }

      conn.start();

      final AtomicInteger errors = new AtomicInteger(0);
      final ReusableLatch latch = new ReusableLatch();
      latch.setCount(noOfMessages);

      class MessageAckEven implements MessageListener {

         int count = 0;

         @Override
         public void onMessage(Message msg) {
            try {
               TextMessage txtmsg = (TextMessage) msg;
               if (!txtmsg.getText().equals("m" + count)) {

                  errors.incrementAndGet();
               }

               if (count % 2 == 0) {
                  msg.acknowledge();
               }

               count++;
            } catch (Exception e) {
               errors.incrementAndGet();
            } finally {
               latch.countDown();
            }
         }

      }

      consumer.setMessageListener(new MessageAckEven());

      Assert.assertTrue(latch.await(5000));

      session.close();

      session = conn.createSession(false, ActiveMQJMSConstants.INDIVIDUAL_ACKNOWLEDGE);

      consumer = session.createConsumer(jBossQueue);

      // Consume odd numbers first
      for (int i = 0; i < noOfMessages; i++) {
         if (i % 2 == 0) {
            continue;
         }

         TextMessage m = (TextMessage) consumer.receive(1000);
         Assert.assertNotNull(m);
         m.acknowledge();
         Assert.assertEquals("m" + i, m.getText());
      }

      SimpleString queueName = new SimpleString(JmsConsumerTest.Q_NAME);
      Assert.assertEquals(0, ((Queue) server.getPostOffice().getBinding(queueName).getBindable()).getDeliveringCount());
      Assert.assertEquals(0, getMessageCount((Queue) server.getPostOffice().getBinding(queueName).getBindable()));
      conn.close();
   }

   @Test
   public void testPreCommitAcksSetOnConnectionFactory() throws Exception {
      ((ActiveMQConnectionFactory) cf).setPreAcknowledge(true);
      conn = cf.createConnection();

      Session session = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      jBossQueue = ActiveMQJMSClient.createQueue(JmsConsumerTest.Q_NAME);
      MessageProducer producer = session.createProducer(jBossQueue);
      MessageConsumer consumer = session.createConsumer(jBossQueue);
      int noOfMessages = 100;
      for (int i = 0; i < noOfMessages; i++) {
         producer.send(session.createTextMessage("m" + i));
      }

      conn.start();
      for (int i = 0; i < noOfMessages; i++) {
         Message m = consumer.receive(500);
         Assert.assertNotNull(m);
      }

      // Messages should all have been acked since we set pre ack on the cf
      SimpleString queueName = new SimpleString(JmsConsumerTest.Q_NAME);
      Assert.assertEquals(0, ((Queue) server.getPostOffice().getBinding(queueName).getBindable()).getDeliveringCount());
      Assert.assertEquals(0, getMessageCount((Queue) server.getPostOffice().getBinding(queueName).getBindable()));
   }

   @Test
   public void testPreCommitAcksWithMessageExpiry() throws Exception {
      JmsConsumerTest.log.info("starting test");

      conn = cf.createConnection();
      Session session = conn.createSession(false, ActiveMQJMSConstants.PRE_ACKNOWLEDGE);
      jBossQueue = ActiveMQJMSClient.createQueue(JmsConsumerTest.Q_NAME);
      MessageProducer producer = session.createProducer(jBossQueue);
      MessageConsumer consumer = session.createConsumer(jBossQueue);
      int noOfMessages = 1000;
      for (int i = 0; i < noOfMessages; i++) {
         TextMessage textMessage = session.createTextMessage("m" + i);
         producer.setTimeToLive(1);
         producer.send(textMessage);
      }

      Thread.sleep(2);

      conn.start();

      Message m = consumer.receiveNoWait();
      Assert.assertNull(m);

      // Asserting delivering count is zero is bogus since messages might still be being delivered and expired at this
      // point
      // which can cause delivering count to flip to 1

   }

   @Test
   public void testPreCommitAcksWithMessageExpirySetOnConnectionFactory() throws Exception {
      ((ActiveMQConnectionFactory) cf).setPreAcknowledge(true);
      conn = cf.createConnection();
      Session session = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      jBossQueue = ActiveMQJMSClient.createQueue(JmsConsumerTest.Q_NAME);
      MessageProducer producer = session.createProducer(jBossQueue);
      MessageConsumer consumer = session.createConsumer(jBossQueue);
      int noOfMessages = 1000;
      for (int i = 0; i < noOfMessages; i++) {
         TextMessage textMessage = session.createTextMessage("m" + i);
         producer.setTimeToLive(1);
         producer.send(textMessage);
      }

      Thread.sleep(2);

      conn.start();
      Message m = consumer.receiveNoWait();
      Assert.assertNull(m);

      // Asserting delivering count is zero is bogus since messages might still be being delivered and expired at this
      // point
      // which can cause delivering count to flip to 1
   }

   @Test
   public void testBrowserAndConsumerSimultaneous() throws Exception {
      ((ActiveMQConnectionFactory) cf).setConsumerWindowSize(0);
      conn = cf.createConnection();

      Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      jBossQueue = ActiveMQJMSClient.createQueue(JmsConsumerTest.Q_NAME);
      MessageProducer producer = session.createProducer(jBossQueue);

      QueueBrowser browser = session.createBrowser(jBossQueue);
      Enumeration enumMessages = browser.getEnumeration();

      MessageConsumer consumer = session.createConsumer(jBossQueue);
      int noOfMessages = 10;
      for (int i = 0; i < noOfMessages; i++) {
         TextMessage textMessage = session.createTextMessage("m" + i);
         textMessage.setIntProperty("i", i);
         producer.send(textMessage);
      }

      conn.start();
      for (int i = 0; i < noOfMessages; i++) {
         TextMessage msg = (TextMessage) enumMessages.nextElement();
         Assert.assertNotNull(msg);
         Assert.assertEquals(i, msg.getIntProperty("i"));

         conn.start();
         TextMessage recvMessage = (TextMessage) consumer.receiveNoWait();
         Assert.assertNotNull(recvMessage);
         conn.stop();
         Assert.assertEquals(i, msg.getIntProperty("i"));
      }

      Assert.assertNull(consumer.receiveNoWait());
      Assert.assertFalse(enumMessages.hasMoreElements());

      conn.close();

      // Asserting delivering count is zero is bogus since messages might still be being delivered and expired at this
      // point
      // which can cause delivering count to flip to 1
   }

   @Test
   public void testBrowserAndConsumerSimultaneousDifferentConnections() throws Exception {
      ((ActiveMQConnectionFactory) cf).setConsumerWindowSize(0);
      conn = cf.createConnection();

      Connection connConsumer = cf.createConnection();
      Session sessionConsumer = connConsumer.createSession(false, Session.AUTO_ACKNOWLEDGE);

      Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      jBossQueue = ActiveMQJMSClient.createQueue(JmsConsumerTest.Q_NAME);
      MessageProducer producer = session.createProducer(jBossQueue);
      MessageConsumer consumer = sessionConsumer.createConsumer(jBossQueue);
      int noOfMessages = 1000;
      for (int i = 0; i < noOfMessages; i++) {
         TextMessage textMessage = session.createTextMessage("m" + i);
         textMessage.setIntProperty("i", i);
         producer.send(textMessage);
      }

      connConsumer.start();

      QueueBrowser browser = session.createBrowser(jBossQueue);
      Enumeration enumMessages = browser.getEnumeration();

      for (int i = 0; i < noOfMessages; i++) {
         TextMessage msg = (TextMessage) enumMessages.nextElement();
         Assert.assertNotNull(msg);
         Assert.assertEquals(i, msg.getIntProperty("i"));

         TextMessage recvMessage = (TextMessage) consumer.receiveNoWait();
         Assert.assertNotNull(recvMessage);
         Assert.assertEquals(i, msg.getIntProperty("i"));
      }

      Message m = consumer.receiveNoWait();
      Assert.assertFalse(enumMessages.hasMoreElements());
      Assert.assertNull(m);

      conn.close();
   }

   @Test
   public void testBrowserOnly() throws Exception {
      ((ActiveMQConnectionFactory) cf).setConsumerWindowSize(0);
      conn = cf.createConnection();

      Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      jBossQueue = ActiveMQJMSClient.createQueue(JmsConsumerTest.Q_NAME);
      MessageProducer producer = session.createProducer(jBossQueue);
      int noOfMessages = 10;
      for (int i = 0; i < noOfMessages; i++) {
         TextMessage textMessage = session.createTextMessage("m" + i);
         textMessage.setIntProperty("i", i);
         producer.send(textMessage);
      }

      QueueBrowser browser = session.createBrowser(jBossQueue);
      Enumeration enumMessages = browser.getEnumeration();

      for (int i = 0; i < noOfMessages; i++) {
         Assert.assertTrue(enumMessages.hasMoreElements());
         TextMessage msg = (TextMessage) enumMessages.nextElement();
         Assert.assertNotNull(msg);
         Assert.assertEquals(i, msg.getIntProperty("i"));

      }

      Assert.assertFalse(enumMessages.hasMoreElements());

      conn.close();

      // Asserting delivering count is zero is bogus since messages might still be being delivered and expired at this
      // point
      // which can cause delivering count to flip to 1
   }

   @Test
   public void testClearExceptionListener() throws Exception {
      conn = cf.createConnection();
      Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      jBossQueue = ActiveMQJMSClient.createQueue(JmsConsumerTest.Q_NAME);
      MessageConsumer consumer = session.createConsumer(jBossQueue);
      consumer.setMessageListener(new MessageListener() {
         @Override
         public void onMessage(final Message msg) {
         }
      });

      consumer.setMessageListener(null);
      consumer.receiveNoWait();
   }

   @Test
   public void testCantReceiveWhenListenerIsSet() throws Exception {
      conn = cf.createConnection();
      Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      jBossQueue = ActiveMQJMSClient.createQueue(JmsConsumerTest.Q_NAME);
      MessageConsumer consumer = session.createConsumer(jBossQueue);
      consumer.setMessageListener(new MessageListener() {
         @Override
         public void onMessage(final Message msg) {
         }
      });

      try {
         consumer.receiveNoWait();
         Assert.fail("Should throw exception");
      } catch (JMSException e) {
         // Ok
      }
   }

   @Test
   public void testSharedConsumer() throws Exception {
      conn = cf.createConnection();
      conn.start();
      Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      topic = ActiveMQJMSClient.createTopic(T_NAME);

      MessageConsumer cons = session.createSharedConsumer(topic, "test1");

      MessageProducer producer = session.createProducer(topic);

      producer.send(session.createTextMessage("test"));

      TextMessage txt = (TextMessage) cons.receive(5000);

      Assert.assertNotNull(txt);
   }

   @Test
   public void testSharedDurableConsumer() throws Exception {
      conn = cf.createConnection();
      conn.start();
      Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      topic = ActiveMQJMSClient.createTopic(T_NAME);

      MessageConsumer cons = session.createSharedDurableConsumer(topic, "test1");

      MessageProducer producer = session.createProducer(topic);

      producer.send(session.createTextMessage("test"));

      TextMessage txt = (TextMessage) cons.receive(5000);

      Assert.assertNotNull(txt);
   }

   @Test
   public void testSharedDurableConsumerWithClientID() throws Exception {
      conn = cf.createConnection();
      conn.setClientID("C1");
      conn.start();
      Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      Connection conn2 = cf.createConnection();
      conn2.setClientID("C2");
      Session session2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);

      {
         Connection conn3 = cf.createConnection();

         boolean exception = false;
         try {
            conn3.setClientID("C2");
         } catch (Exception e) {
            exception = true;
         }

         Assert.assertTrue(exception);
         conn3.close();
      }

      topic = ActiveMQJMSClient.createTopic(T_NAME);

      MessageConsumer cons = session.createSharedDurableConsumer(topic, "test1");

      MessageProducer producer = session.createProducer(topic);

      producer.send(session.createTextMessage("test"));

      TextMessage txt = (TextMessage) cons.receive(5000);

      Assert.assertNotNull(txt);
   }

   @Test
   public void testValidateExceptionsThroughSharedConsumers() throws Exception {
      conn = cf.createConnection();
      conn.setClientID("C1");
      conn.start();
      Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Connection conn2 = cf.createConnection();
      conn2.setClientID("C2");

      MessageConsumer cons = session.createSharedConsumer(topic, "cons1");
      boolean exceptionHappened = false;
      try {
         MessageConsumer cons2Error = session.createSharedConsumer(topic2, "cons1");
      } catch (JMSException e) {
         exceptionHappened = true;
      }

      Assert.assertTrue(exceptionHappened);

      MessageProducer producer = session.createProducer(topic2);

      // This is durable, different than the one on topic... So it should go through
      MessageConsumer cons2 = session.createSharedDurableConsumer(topic2, "cons1");

      conn.start();

      producer.send(session.createTextMessage("hello!"));

      TextMessage msg = (TextMessage) cons2.receive(5000);
      Assert.assertNotNull(msg);

      exceptionHappened = false;
      try {
         session.unsubscribe("cons1");
      } catch (JMSException e) {
         exceptionHappened = true;
      }

      Assert.assertTrue(exceptionHappened);
      cons2.close();
      conn.close();
      conn2.close();

   }

   @Test
   public void testUnsubscribeDurable() throws Exception {
      conn = cf.createConnection();
      conn.setClientID("C1");
      conn.start();
      Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      MessageConsumer cons = session.createSharedDurableConsumer(topic, "c1");

      MessageProducer prod = session.createProducer(topic);

      for (int i = 0; i < 100; i++) {
         prod.send(session.createTextMessage("msg" + i));
      }

      Assert.assertNotNull(cons.receive(5000));

      cons.close();

      session.unsubscribe("c1");

      cons = session.createSharedDurableConsumer(topic, "c1");

      // it should be null since the queue was deleted through unsubscribe
      Assert.assertNull(cons.receiveNoWait());
   }

   @Test
   public void testShareDurable() throws Exception {
      ((ActiveMQConnectionFactory) cf).setConsumerWindowSize(0);
      conn = cf.createConnection();
      conn.start();
      Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session session2 = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      MessageConsumer cons = session.createSharedDurableConsumer(topic, "c1");
      MessageConsumer cons2 = session2.createSharedDurableConsumer(topic, "c1");

      MessageProducer prod = session.createProducer(topic);

      for (int i = 0; i < 100; i++) {
         prod.send(session.createTextMessage("msg" + i));
      }

      for (int i = 0; i < 50; i++) {
         Message msg = cons.receive(5000);
         Assert.assertNotNull(msg);
         msg = cons2.receive(5000);
         Assert.assertNotNull(msg);
      }

      Assert.assertNull(cons.receiveNoWait());
      Assert.assertNull(cons2.receiveNoWait());

      cons.close();

      boolean exceptionHappened = false;

      try {
         session.unsubscribe("c1");
      } catch (JMSException e) {
         exceptionHappened = true;
      }

      Assert.assertTrue(exceptionHappened);

      cons2.close();

      for (int i = 0; i < 100; i++) {
         prod.send(session.createTextMessage("msg" + i));
      }

      session.unsubscribe("c1");

      cons = session.createSharedDurableConsumer(topic, "c1");

      // it should be null since the queue was deleted through unsubscribe
      Assert.assertNull(cons.receiveNoWait());
   }

   @Test
   public void testShareDuraleWithJMSContext() throws Exception {
      ((ActiveMQConnectionFactory) cf).setConsumerWindowSize(0);
      JMSContext conn = cf.createContext(JMSContext.AUTO_ACKNOWLEDGE);

      JMSConsumer consumer = conn.createSharedDurableConsumer(topic, "c1");

      JMSProducer producer = conn.createProducer();

      for (int i = 0; i < 100; i++) {
         producer.setProperty("count", i).send(topic, "test" + i);
      }

      JMSContext conn2 = conn.createContext(JMSContext.AUTO_ACKNOWLEDGE);
      JMSConsumer consumer2 = conn2.createSharedDurableConsumer(topic, "c1");

      for (int i = 0; i < 50; i++) {
         String txt = consumer.receiveBody(String.class, 5000);
         System.out.println("TXT:" + txt);
         Assert.assertNotNull(txt);

         txt = consumer.receiveBody(String.class, 5000);
         System.out.println("TXT:" + txt);
         Assert.assertNotNull(txt);
      }

      Assert.assertNull(consumer.receiveNoWait());
      Assert.assertNull(consumer2.receiveNoWait());

      boolean exceptionHappened = false;

      try {
         conn.unsubscribe("c1");
      } catch (Exception e) {
         e.printStackTrace();
         exceptionHappened = true;
      }

      Assert.assertTrue(exceptionHappened);

      consumer.close();
      consumer2.close();
      conn2.close();

      conn.unsubscribe("c1");

   }

   @Test
   public void defaultAutoCreatedQueueConfigTest() throws Exception {
      final String queueName = "q1";

      server.getAddressSettingsRepository()
            .addMatch(queueName, new AddressSettings()
               .setDefaultMaxConsumers(5)
               .setDefaultDeleteOnNoConsumers(true));

      Connection connection = cf.createConnection();

      Session session = connection.createSession();

      session.createConsumer(session.createQueue(queueName));

      org.apache.activemq.artemis.core.server.Queue  queue = server.locateQueue(SimpleString.toSimpleString(queueName));

      assertEquals(5, queue.getMaxConsumers());
      assertEquals(true, queue.isDeleteOnNoConsumers());

      connection.close();
   }
}
