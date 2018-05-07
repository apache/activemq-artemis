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
package org.apache.activemq.artemis.tests.integration.openwire;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;
import javax.jms.XAConnection;
import javax.jms.XASession;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.postoffice.impl.LocalQueueBinding;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.core.transaction.impl.XidImpl;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class SimpleOpenWireTest extends BasicOpenWireTest {

   private final String testString = "simple test string";
   private final String testProp = "BASE_DATE";
   private final String propValue = "2017-11-01";

   @Override
   @Before
   public void setUp() throws Exception {
      //this system property is used to construct the executor in
      //org.apache.activemq.transport.AbstractInactivityMonitor.createExecutor()
      //and affects the pool's shutdown time. (default is 30 sec)
      //set it to 2 to make tests shutdown quicker.
      System.setProperty("org.apache.activemq.transport.AbstractInactivityMonitor.keepAliveTime", "2");
      this.realStore = true;
      super.setUp();
   }

   @Test
   public void testSimple() throws Exception {
      Connection connection = factory.createConnection();

      Collection<Session> sessions = new LinkedList<>();

      for (int i = 0; i < 10; i++) {
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         sessions.add(session);
      }

      connection.close();
   }

   @Test
   public void testTransactionalSimple() throws Exception {
      try (Connection connection = factory.createConnection()) {

         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         Queue queue = session.createQueue(queueName);
         System.out.println("Queue:" + queue);
         MessageProducer producer = session.createProducer(queue);
         MessageConsumer consumer = session.createConsumer(queue);
         producer.send(session.createTextMessage("test"));
         session.commit();

         Assert.assertNull(consumer.receive(100));
         connection.start();

         TextMessage message = (TextMessage) consumer.receive(5000);
         Assert.assertEquals("test", message.getText());

         Assert.assertNotNull(message);

         message.acknowledge();
      }
   }

   @Test
   public void testSendEmpty() throws Exception {
      try (Connection connection = factory.createConnection()) {

         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue queue = session.createQueue(queueName);
         System.out.println("Queue:" + queue);
         MessageProducer producer = session.createProducer(queue);
         MessageConsumer consumer = session.createConsumer(queue);
         producer.send(session.createTextMessage());

         Assert.assertNull(consumer.receive(100));
         connection.start();

         TextMessage message = (TextMessage) consumer.receive(5000);

         Assert.assertNotNull(message);

         message.acknowledge();
      }
   }

   @Test
   public void testSendNullMapMessage() throws Exception {
      try (Connection connection = factory.createConnection()) {

         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue queue = session.createQueue(queueName);
         System.out.println("Queue:" + queue);
         MessageProducer producer = session.createProducer(queue);
         MessageConsumer consumer = session.createConsumer(queue);
         producer.send(session.createMapMessage());

         Assert.assertNull(consumer.receive(100));
         connection.start();

         MapMessage message = (MapMessage) consumer.receive(5000);

         Assert.assertNotNull(message);

         message.acknowledge();
      }
   }

   @Test
   public void testSendEmptyMessages() throws Exception {
      Queue dest = new ActiveMQQueue(queueName);

      QueueSession defaultQueueSession =  connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
      QueueSender defaultSender = defaultQueueSession.createSender(dest);
      defaultSender.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      connection.start();

      Message msg = defaultQueueSession.createMessage();
      msg.setStringProperty("testName", "testSendEmptyMessages");
      defaultSender.send(msg);

      QueueReceiver queueReceiver = defaultQueueSession.createReceiver(dest);
      assertNotNull("Didn't receive message", queueReceiver.receive(1000));

      //bytes
      BytesMessage bytesMessage = defaultQueueSession.createBytesMessage();
      bytesMessage.setStringProperty("testName", "testSendEmptyMessages");
      defaultSender.send(bytesMessage);
      assertNotNull("Didn't receive message", queueReceiver.receive(1000));

      //map
      MapMessage mapMessage = defaultQueueSession.createMapMessage();
      mapMessage.setStringProperty("testName", "testSendEmptyMessages");
      defaultSender.send(mapMessage);
      assertNotNull("Didn't receive message", queueReceiver.receive(1000));

      //object
      ObjectMessage objMessage = defaultQueueSession.createObjectMessage();
      objMessage.setStringProperty("testName", "testSendEmptyMessages");
      defaultSender.send(objMessage);
      assertNotNull("Didn't receive message", queueReceiver.receive(1000));

      //stream
      StreamMessage streamMessage = defaultQueueSession.createStreamMessage();
      streamMessage.setStringProperty("testName", "testSendEmptyMessages");
      defaultSender.send(streamMessage);
      assertNotNull("Didn't receive message", queueReceiver.receive(1000));

      //text
      TextMessage textMessage = defaultQueueSession.createTextMessage();
      textMessage.setStringProperty("testName", "testSendEmptyMessages");
      defaultSender.send(textMessage);
      assertNotNull("Didn't receive message", queueReceiver.receive(1000));
   }

   @Test
   public void testXASimple() throws Exception {
      XAConnection connection = xaFactory.createXAConnection();

      Collection<Session> sessions = new LinkedList<>();

      for (int i = 0; i < 10; i++) {
         XASession session = connection.createXASession();
         session.getXAResource().start(newXID(), XAResource.TMNOFLAGS);
         sessions.add(session);
      }

      connection.close();

   }

   @Test
   public void testClientACK() throws Exception {
      try {

         Connection connection = factory.createConnection();

         Collection<Session> sessions = new LinkedList<>();

         Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
         Queue queue = session.createQueue(queueName);
         System.out.println("Queue:" + queue);
         MessageProducer producer = session.createProducer(queue);
         MessageConsumer consumer = session.createConsumer(queue);
         producer.send(session.createTextMessage("test"));

         Assert.assertNull(consumer.receive(100));
         connection.start();

         TextMessage message = (TextMessage) consumer.receive(5000);

         Assert.assertNotNull(message);

         message.acknowledge();

         connection.close();

         System.err.println("Done!!!");
      } catch (Throwable e) {
         e.printStackTrace();
      }
   }

   @Test
   public void testRollback() throws Exception {
      try (Connection connection = factory.createConnection()) {
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         Queue queue = session.createQueue(queueName);
         System.out.println("Queue:" + queue);
         MessageProducer producer = session.createProducer(queue);
         MessageConsumer consumer = session.createConsumer(queue);
         producer.send(session.createTextMessage("test"));
         producer.send(session.createTextMessage("test2"));
         connection.start();
         Assert.assertNull(consumer.receiveNoWait());
         session.rollback();
         producer.send(session.createTextMessage("test2"));
         Assert.assertNull(consumer.receiveNoWait());
         session.commit();
         TextMessage msg = (TextMessage) consumer.receive(1000);

         Assert.assertNotNull(msg);
         Assert.assertEquals("test2", msg.getText());
      }
   }

   @Test
   public void testAutoAck() throws Exception {
      Connection connection = factory.createConnection();

      Collection<Session> sessions = new LinkedList<>();

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue queue = session.createQueue(queueName);
      System.out.println("Queue:" + queue);
      MessageProducer producer = session.createProducer(queue);
      MessageConsumer consumer = session.createConsumer(queue);
      TextMessage msg = session.createTextMessage("test");
      msg.setStringProperty("abc", "testAutoACK");
      producer.send(msg);

      Assert.assertNull(consumer.receive(100));
      connection.start();

      TextMessage message = (TextMessage) consumer.receive(5000);

      Assert.assertNotNull(message);

      connection.close();

      System.err.println("Done!!!");
   }

   @Test
   public void testProducerFlowControl() throws Exception {
      ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(urlString);

      factory.setProducerWindowSize(1024 * 64);

      Connection connection = factory.createConnection();
      Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
      Queue queue = session.createQueue(queueName);
      MessageProducer producer = session.createProducer(queue);
      producer.send(session.createTextMessage("test"));

      connection.close();
   }

   @Test
   public void testCompression() throws Exception {

      Connection cconnection = null;
      Connection connection = null;
      try {
         ActiveMQConnectionFactory cfactory = new ActiveMQConnectionFactory("tcp://" + OWHOST + ":" + OWPORT + "");
         cconnection = cfactory.createConnection();
         cconnection.start();
         Session csession = cconnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue cQueue = csession.createQueue(queueName);
         MessageConsumer consumer = csession.createConsumer(cQueue);

         ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://" + OWHOST + ":" + OWPORT + "?jms.useCompression=true");
         connection = factory.createConnection();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue queue = session.createQueue(queueName);

         MessageProducer producer = session.createProducer(queue);
         producer.setDeliveryMode(DeliveryMode.PERSISTENT);

         //text
         TextMessage textMessage = session.createTextMessage();
         textMessage.setText(testString);
         TextMessage receivedMessage = sendAndReceive(textMessage, producer, consumer);

         String receivedText = receivedMessage.getText();
         assertEquals(testString, receivedText);

         //MapMessage
         MapMessage mapMessage = session.createMapMessage();
         mapMessage.setString(testProp, propValue);
         MapMessage receivedMapMessage = sendAndReceive(mapMessage, producer, consumer);
         String value = receivedMapMessage.getString(testProp);
         assertEquals(propValue, value);

         //Object
         ObjectMessage objMessage = session.createObjectMessage();
         objMessage.setObject(testString);
         ObjectMessage receivedObjMessage = sendAndReceive(objMessage, producer, consumer);
         String receivedObj = (String) receivedObjMessage.getObject();
         assertEquals(testString, receivedObj);

         //Stream
         StreamMessage streamMessage = session.createStreamMessage();
         streamMessage.writeString(testString);
         StreamMessage receivedStreamMessage = sendAndReceive(streamMessage, producer, consumer);
         String streamValue = receivedStreamMessage.readString();
         assertEquals(testString, streamValue);

         //byte
         BytesMessage byteMessage = session.createBytesMessage();
         byte[] bytes = testString.getBytes();
         byteMessage.writeBytes(bytes);

         BytesMessage receivedByteMessage = sendAndReceive(byteMessage, producer, consumer);
         long receivedBodylength = receivedByteMessage.getBodyLength();

         assertEquals("bodylength Correct", bytes.length, receivedBodylength);

         byte[] receivedBytes = new byte[(int) receivedBodylength];
         receivedByteMessage.readBytes(receivedBytes);

         String receivedString = new String(receivedBytes);
         assertEquals(testString, receivedString);

         //Message
         Message m = session.createMessage();
         sendAndReceive(m, producer, consumer);
      } finally {
         if (cconnection != null) {
            connection.close();
         }
         if (connection != null) {
            cconnection.close();
         }
      }

   }

   private <T extends Message> T sendAndReceive(T m, MessageProducer producer, MessageConsumer consumer) throws JMSException {
      m.setStringProperty(testProp, propValue);
      producer.send(m);
      T receivedMessage = (T) consumer.receive(1000);
      String receivedProp = receivedMessage.getStringProperty(testProp);
      assertEquals(propValue, receivedProp);
      return receivedMessage;
   }

   @Test
   public void testSimpleQueue() throws Exception {
      connection.start();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      System.out.println("creating queue: " + queueName);
      Destination dest = new ActiveMQQueue(queueName);

      System.out.println("creating producer...");
      MessageProducer producer = session.createProducer(dest);

      final int num = 1;
      final String msgBase = "MfromAMQ-";
      for (int i = 0; i < num; i++) {
         TextMessage msg = session.createTextMessage("MfromAMQ-" + i);
         producer.send(msg);
         System.out.println("sent: ");
      }

      //receive
      MessageConsumer consumer = session.createConsumer(dest);

      System.out.println("receiving messages...");
      for (int i = 0; i < num; i++) {
         TextMessage msg = (TextMessage) consumer.receive(5000);
         System.out.println("received: " + msg);
         String content = msg.getText();
         System.out.println("content: " + content);
         assertEquals(msgBase + i, content);
      }

      assertNull(consumer.receive(1000));

      session.close();
   }

   @Test
   public void testSendReceiveDifferentEncoding() throws Exception {
      connection.start();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      System.out.println("creating queue: " + queueName);
      Destination dest = new ActiveMQQueue(queueName);

      System.out.println("creating producer...");
      MessageProducer producer = session.createProducer(dest);

      final int num = 10;
      final String msgBase = "MfromAMQ-";
      for (int i = 0; i < num; i++) {
         TextMessage msg = session.createTextMessage(msgBase + i);
         producer.send(msg);
         System.out.println("sent: ");
      }

      //receive loose
      ActiveMQConnection looseConn = (ActiveMQConnection) looseFactory.createConnection();
      try {
         looseConn.start();
         Session looseSession = looseConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer looseConsumer = looseSession.createConsumer(dest);

         System.out.println("receiving messages...");
         for (int i = 0; i < num; i++) {
            TextMessage msg = (TextMessage) looseConsumer.receive(5000);
            System.out.println("received: " + msg);
            String content = msg.getText();
            System.out.println("content: " + content);
            assertEquals(msgBase + i, content);
         }

         assertNull(looseConsumer.receive(1000));
         looseConsumer.close();

         //now reverse

         MessageProducer looseProducer = looseSession.createProducer(dest);
         for (int i = 0; i < num; i++) {
            TextMessage msg = looseSession.createTextMessage(msgBase + i);
            looseProducer.send(msg);
            System.out.println("sent: ");
         }

         MessageConsumer consumer = session.createConsumer(dest);
         System.out.println("receiving messages...");
         for (int i = 0; i < num; i++) {
            TextMessage msg = (TextMessage) consumer.receive(5000);
            System.out.println("received: " + msg);
            assertNotNull(msg);
            String content = msg.getText();
            System.out.println("content: " + content);
            assertEquals(msgBase + i, content);
         }

         assertNull(consumer.receive(1000));

         session.close();
         looseSession.close();
      } finally {
         looseConn.close();
      }
   }

   @Test
   @Ignore("ignored for now")
   public void testKeepAlive() throws Exception {
      connection.start();

      Thread.sleep(30000);

      connection.createSession(false, 1);
   }

   @Test
   public void testSimpleTopic() throws Exception {
      connection.start();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      System.out.println("creating queue: " + topicName);
      Destination dest = new ActiveMQTopic(topicName);

      MessageConsumer consumer1 = session.createConsumer(dest);
      MessageConsumer consumer2 = session.createConsumer(dest);

      MessageProducer producer = session.createProducer(dest);

      final int num = 1;
      final String msgBase = "MfromAMQ-";
      for (int i = 0; i < num; i++) {
         TextMessage msg = session.createTextMessage("MfromAMQ-" + i);
         producer.send(msg);
         System.out.println("Sent a message");
      }

      //receive
      System.out.println("receiving messages...");
      for (int i = 0; i < num; i++) {
         TextMessage msg = (TextMessage) consumer1.receive(5000);
         System.out.println("received: " + msg);
         String content = msg.getText();
         assertEquals(msgBase + i, content);
      }

      assertNull(consumer1.receive(500));

      System.out.println("receiving messages...");
      for (int i = 0; i < num; i++) {
         TextMessage msg = (TextMessage) consumer2.receive(5000);
         System.out.println("received: " + msg);
         String content = msg.getText();
         assertEquals(msgBase + i, content);
      }

      assertNull(consumer2.receive(500));
      session.close();
   }

   @Test
   public void testTopicNoLocal() throws Exception {
      connection.start();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      System.out.println("creating queue: " + topicName);
      Destination dest = new ActiveMQTopic(topicName);

      MessageConsumer nolocalConsumer = session.createConsumer(dest, null, true);
      MessageConsumer consumer = session.createConsumer(dest, null, false);
      MessageConsumer selectorConsumer  = session.createConsumer(dest,"TESTKEY = 'test'", false);

      MessageProducer producer = session.createProducer(dest);

      final String body1 = "MfromAMQ-1";
      final String body2 = "MfromAMQ-2";
      TextMessage msg = session.createTextMessage(body1);
      producer.send(msg);

      msg = session.createTextMessage(body2);
      msg.setStringProperty("TESTKEY", "test");
      producer.send(msg);

      //receive nolocal
      TextMessage receivedMsg = (TextMessage) nolocalConsumer.receive(1000);
      assertNull("nolocal consumer got: " + receivedMsg, receivedMsg);

      //receive normal consumer
      receivedMsg = (TextMessage) consumer.receive(1000);
      assertNotNull(receivedMsg);
      assertEquals(body1, receivedMsg.getText());

      receivedMsg = (TextMessage) consumer.receive(1000);
      assertNotNull(receivedMsg);
      assertEquals(body2, receivedMsg.getText());

      assertNull(consumer.receiveNoWait());

      //selector should only receive one
      receivedMsg = (TextMessage) selectorConsumer.receive(1000);
      assertNotNull(receivedMsg);
      assertEquals(body2, receivedMsg.getText());
      assertEquals("test", receivedMsg.getStringProperty("TESTKEY"));

      assertNull(selectorConsumer.receiveNoWait());

      //send from another connection
      Connection anotherConn = this.factory.createConnection();
      try {
         anotherConn.start();

         Session anotherSession = anotherConn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer anotherProducer = anotherSession.createProducer(dest);
         TextMessage anotherMsg = anotherSession.createTextMessage(body1);
         anotherProducer.send(anotherMsg);

         assertNotNull(consumer.receive(1000));
         assertNull(selectorConsumer.receive(1000));
         assertNotNull(nolocalConsumer.receive(1000));
      } finally {
         anotherConn.close();
      }

      session.close();
   }

   @Test
   public void testTopicNoLocalDurable() throws Exception {
      connection.setClientID("forNoLocal-1");
      connection.start();
      TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);

      System.out.println("creating queue: " + topicName);
      Topic dest = new ActiveMQTopic(topicName);

      MessageConsumer nolocalConsumer = session.createDurableSubscriber(dest, "nolocal-subscriber1", "", true);
      MessageConsumer consumer = session.createDurableSubscriber(dest, "normal-subscriber", null, false);
      MessageConsumer selectorConsumer = session.createDurableSubscriber(dest, "selector-subscriber", "TESTKEY = 'test'", false);

      MessageProducer producer = session.createProducer(dest);

      final String body1 = "MfromAMQ-1";
      final String body2 = "MfromAMQ-2";
      TextMessage msg = session.createTextMessage(body1);
      producer.send(msg);

      msg = session.createTextMessage(body2);
      msg.setStringProperty("TESTKEY", "test");
      producer.send(msg);

      //receive nolocal
      TextMessage receivedMsg = (TextMessage) nolocalConsumer.receive(1000);
      assertNull("nolocal consumer got: " + receivedMsg, receivedMsg);

      //receive normal consumer
      receivedMsg = (TextMessage) consumer.receive(1000);
      assertNotNull(receivedMsg);
      assertEquals(body1, receivedMsg.getText());

      receivedMsg = (TextMessage) consumer.receive(1000);
      assertNotNull(receivedMsg);
      assertEquals(body2, receivedMsg.getText());

      assertNull(consumer.receiveNoWait());

      //selector should only receive one
      receivedMsg = (TextMessage) selectorConsumer.receive(1000);
      assertNotNull(receivedMsg);
      assertEquals(body2, receivedMsg.getText());
      assertEquals("test", receivedMsg.getStringProperty("TESTKEY"));

      assertNull(selectorConsumer.receiveNoWait());

      //send from another connection
      Connection anotherConn = this.factory.createConnection();
      try {
         anotherConn.start();

         Session anotherSession = anotherConn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer anotherProducer = anotherSession.createProducer(dest);
         TextMessage anotherMsg = anotherSession.createTextMessage(body1);
         anotherProducer.send(anotherMsg);

         assertNotNull(consumer.receive(1000));
         assertNull(selectorConsumer.receive(1000));
         assertNotNull(nolocalConsumer.receive(1000));
      } finally {
         anotherConn.close();
      }

      session.close();
   }

   @Test
   public void testTempTopicDelete() throws Exception {
      connection.start();
      TopicSession topicSession = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);

      TemporaryTopic tempTopic = topicSession.createTemporaryTopic();

      ActiveMQConnection newConn = (ActiveMQConnection) factory.createConnection();

      try {
         TopicSession newTopicSession = newConn.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
         TopicPublisher publisher = newTopicSession.createPublisher(tempTopic);

         // need to wait here because the ActiveMQ client's temp destination map is updated asynchronously, not waiting can introduce a race
         assertTrue(Wait.waitFor(() -> newConn.activeTempDestinations.size() == 1, 2000, 100));

         TextMessage msg = newTopicSession.createTextMessage("Test Message");

         publisher.publish(msg);

         try {
            TopicSubscriber consumer = newTopicSession.createSubscriber(tempTopic);
            fail("should have gotten exception but got consumer: " + consumer);
         } catch (JMSException ex) {
            //correct
         }

         connection.close();

         try {
            Message newMsg = newTopicSession.createMessage();
            publisher.publish(newMsg);
         } catch (JMSException e) {
            //ok
         }

      } finally {
         newConn.close();
      }
   }

   @Test
   public void testTempQueueDelete() throws Exception {
      connection.start();
      QueueSession queueSession = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);

      TemporaryQueue tempQueue = queueSession.createTemporaryQueue();

      ActiveMQConnection newConn = (ActiveMQConnection) factory.createConnection();
      try {
         QueueSession newQueueSession = newConn.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
         QueueSender queueSender = newQueueSession.createSender(tempQueue);

         Message msg = queueSession.createMessage();
         queueSender.send(msg);

         try {
            QueueReceiver consumer = newQueueSession.createReceiver(tempQueue);
            fail("should have gotten exception but got consumer: " + consumer);
         } catch (JMSException ex) {
            //correct
         }

         connection.close();

         try {
            Message newMsg = newQueueSession.createMessage();
            queueSender.send(newMsg);
         } catch (JMSException e) {
            //ok
         }

      } finally {
         newConn.close();
      }
   }

   @Test
   public void testSimpleTempTopic() throws Exception {
      connection.start();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      System.out.println("creating temp topic");
      TemporaryTopic tempTopic = session.createTemporaryTopic();

      System.out.println("create consumer 1");
      MessageConsumer consumer1 = session.createConsumer(tempTopic);
      System.out.println("create consumer 2");
      MessageConsumer consumer2 = session.createConsumer(tempTopic);

      System.out.println("create producer");
      MessageProducer producer = session.createProducer(tempTopic);

      System.out.println("sending messages");
      final int num = 1;
      final String msgBase = "MfromAMQ-";
      for (int i = 0; i < num; i++) {
         TextMessage msg = session.createTextMessage("MfromAMQ-" + i);
         producer.send(msg);
         System.out.println("Sent a message");
      }

      //receive
      System.out.println("receiving messages...");
      for (int i = 0; i < num; i++) {
         TextMessage msg = (TextMessage) consumer1.receive(5000);
         System.out.println("received: " + msg);
         String content = msg.getText();
         assertEquals(msgBase + i, content);
      }

      assertNull(consumer1.receive(500));

      System.out.println("receiving messages...");
      for (int i = 0; i < num; i++) {
         TextMessage msg = (TextMessage) consumer2.receive(5000);
         System.out.println("received: " + msg);
         String content = msg.getText();
         assertEquals(msgBase + i, content);
      }

      assertNull(consumer2.receive(500));
      session.close();
   }

   @Test
   public void testSimpleTempQueue() throws Exception {
      AddressSettings addressSetting = new AddressSettings();
      addressSetting.setAutoCreateQueues(true);
      addressSetting.setAutoCreateAddresses(true);

      String address = "#";
      server.getAddressSettingsRepository().addMatch(address, addressSetting);

      connection.start();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      System.out.println("creating temp queue");
      TemporaryQueue tempQueue = session.createTemporaryQueue();

      System.out.println("create consumer 1");
      MessageConsumer consumer1 = session.createConsumer(tempQueue);

      System.out.println("create producer");
      MessageProducer producer = session.createProducer(tempQueue);

      System.out.println("sending messages");
      final int num = 1;
      final String msgBase = "MfromAMQ-";
      for (int i = 0; i < num; i++) {
         TextMessage msg = session.createTextMessage("MfromAMQ-" + i);
         producer.send(msg);
         System.out.println("Sent a message");
      }

      //receive
      System.out.println("receiving messages...");
      for (int i = 0; i < num; i++) {
         TextMessage msg = (TextMessage) consumer1.receive(5000);
         System.out.println("received: " + msg);
         String content = msg.getText();
         assertEquals(msgBase + i, content);
      }

      assertNull(consumer1.receive(500));
      session.close();
   }

   @Test
   public void testInvalidDestinationExceptionWhenNoQueueExistsOnCreateProducer() throws Exception {
      connection.start();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue queue = session.createQueue("foo");

      try {
         session.createProducer(queue);
         fail("Should have thrown an exception creating a producer here");
      } catch (JMSException expected) {
      }
      session.close();
   }

   @Test
   public void testAutoDestinationCreationOnProducerSend() throws JMSException {
      AddressSettings addressSetting = new AddressSettings();
      addressSetting.setAutoCreateQueues(true);
      addressSetting.setAutoCreateAddresses(true);

      String address = "foo";
      server.getAddressSettingsRepository().addMatch(address, addressSetting);

      connection.start();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      TextMessage message = session.createTextMessage("bar");
      Queue queue = new ActiveMQQueue(address);

      MessageProducer producer = session.createProducer(null);
      producer.send(queue, message);

      MessageConsumer consumer = session.createConsumer(queue);
      TextMessage message1 = (TextMessage) consumer.receive(1000);
      assertTrue(message1.getText().equals(message.getText()));
   }

   @Test
   public void testAutoDestinationCreationAndDeletionOnConsumer() throws Exception {
      AddressSettings addressSetting = new AddressSettings();
      addressSetting.setAutoCreateQueues(true);
      addressSetting.setAutoCreateAddresses(true);
      addressSetting.setAutoDeleteQueues(true);
      addressSetting.setAutoDeleteAddresses(true);

      String address = "foo";
      server.getAddressSettingsRepository().addMatch(address, addressSetting);

      connection.start();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      TextMessage message = session.createTextMessage("bar");
      Queue queue = new ActiveMQQueue(address);

      MessageConsumer consumer = session.createConsumer(queue);

      assertTrue(Wait.waitFor(() -> (server.locateQueue(SimpleString.toSimpleString("foo")) != null), 2000, 100));
      assertTrue(Wait.waitFor(() -> (server.getAddressInfo(SimpleString.toSimpleString("foo")) != null), 2000, 100));

      MessageProducer producer = session.createProducer(null);
      producer.send(queue, message);

      TextMessage message1 = (TextMessage) consumer.receive(1000);
      assertTrue(message1.getText().equals(message.getText()));

      assertNotNull(server.locateQueue(SimpleString.toSimpleString("foo")));

      consumer.close();
      connection.close();

      assertTrue(Wait.waitFor(() -> (server.locateQueue(SimpleString.toSimpleString("foo")) == null), 2000, 100));
      assertTrue(Wait.waitFor(() -> (server.getAddressInfo(SimpleString.toSimpleString("foo")) == null), 2000, 100));
   }

   @Test
   public void testAutoDestinationNoCreationOnConsumer() throws JMSException {
      AddressSettings addressSetting = new AddressSettings();
      addressSetting.setAutoCreateQueues(false);

      String address = "foo";
      server.getAddressSettingsRepository().addMatch(address, addressSetting);

      connection.start();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      TextMessage message = session.createTextMessage("bar");
      Queue queue = new ActiveMQQueue(address);

      try {
         MessageConsumer consumer = session.createConsumer(queue);
         fail("supposed to throw an exception here");
      } catch (JMSException e) {

      }
   }

   @Test
   public void testFailoverTransportReconnect() throws Exception {
      Connection exConn = null;

      try {
         String urlString = "failover:(tcp://" + OWHOST + ":" + OWPORT + ")";
         ActiveMQConnectionFactory exFact = new ActiveMQConnectionFactory(urlString);

         Queue queue = new ActiveMQQueue(durableQueueName);

         exConn = exFact.createConnection();
         exConn.start();

         Session session = exConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer messageProducer = session.createProducer(queue);
         messageProducer.send(session.createTextMessage("Test"));

         MessageConsumer consumer = session.createConsumer(queue);
         assertNotNull(consumer.receive(5000));

         server.stop();
         Thread.sleep(3000);

         server.start();
         server.waitForActivation(10, TimeUnit.SECONDS);

         messageProducer.send(session.createTextMessage("Test2"));
         assertNotNull(consumer.receive(5000));
      } finally {
         if (exConn != null) {
            exConn.close();
         }
      }
   }

   /**
    * This is the example shipped with the distribution
    *
    * @throws Exception
    */
   @Test
   public void testOpenWireExample() throws Exception {
      Connection exConn = null;

      SimpleString durableQueue = new SimpleString("exampleQueue");
      this.server.createQueue(durableQueue, RoutingType.ANYCAST, durableQueue, null, true, false, -1, false, true);

      try {
         ActiveMQConnectionFactory exFact = new ActiveMQConnectionFactory();

         Queue queue = new ActiveMQQueue(durableQueueName);

         exConn = exFact.createConnection();

         exConn.start();

         Session session = exConn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer producer = session.createProducer(queue);

         TextMessage message = session.createTextMessage("This is a text message");

         producer.send(message);

         MessageConsumer messageConsumer = session.createConsumer(queue);

         TextMessage messageReceived = (TextMessage) messageConsumer.receive(5000);

         assertEquals("This is a text message", messageReceived.getText());
      } finally {
         if (exConn != null) {
            exConn.close();
         }
      }

   }

   /**
    * This is the example shipped with the distribution
    *
    * @throws Exception
    */
   @Test
   public void testMultipleConsumers() throws Exception {
      Connection exConn = null;

      SimpleString durableQueue = new SimpleString("exampleQueue");
      this.server.createQueue(durableQueue, RoutingType.ANYCAST, durableQueue, null, true, false, -1, false, true);

      try {
         ActiveMQConnectionFactory exFact = new ActiveMQConnectionFactory();

         Queue queue = new ActiveMQQueue(durableQueueName);

         exConn = exFact.createConnection();

         exConn.start();

         Session session = exConn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer producer = session.createProducer(queue);

         TextMessage message = session.createTextMessage("This is a text message");

         producer.send(message);

         MessageConsumer messageConsumer = session.createConsumer(queue);

         TextMessage messageReceived = (TextMessage) messageConsumer.receive(5000);

         assertEquals("This is a text message", messageReceived.getText());
      } finally {
         if (exConn != null) {
            exConn.close();
         }
      }

   }

   @Test
   public void testMixedOpenWireExample() throws Exception {
      Connection openConn = null;

      SimpleString durableQueue = new SimpleString("exampleQueue");
      this.server.createQueue(durableQueue, RoutingType.ANYCAST, durableQueue, null, true, false, -1, false, true);

      ActiveMQConnectionFactory openCF = new ActiveMQConnectionFactory();

      Queue queue = new ActiveMQQueue("exampleQueue");

      openConn = openCF.createConnection();

      openConn.start();

      Session openSession = openConn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      MessageProducer producer = openSession.createProducer(queue);

      TextMessage message = openSession.createTextMessage("This is a text message");

      producer.send(message);

      org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory artemisCF = new org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory();

      Connection artemisConn = artemisCF.createConnection();
      Session artemisSession = artemisConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      artemisConn.start();
      MessageConsumer messageConsumer = artemisSession.createConsumer(artemisSession.createQueue("exampleQueue"));

      TextMessage messageReceived = (TextMessage) messageConsumer.receive(5000);

      assertEquals("This is a text message", messageReceived.getText());

      openConn.close();
      artemisConn.close();

   }

   // simple test sending openwire, consuming core
   @Test
   public void testMixedOpenWireExample2() throws Exception {
      Connection conn1 = null;

      SimpleString durableQueue = new SimpleString("exampleQueue");
      this.server.createQueue(durableQueue, RoutingType.ANYCAST, durableQueue, null, true, false, -1, false, true);

      Queue queue = ActiveMQJMSClient.createQueue("exampleQueue");

      org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory artemisCF = new org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory();

      conn1 = artemisCF.createConnection();

      conn1.start();

      Session session1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer producer = session1.createProducer(queue);
      for (int i = 0; i < 10; i++) {
         TextMessage message = session1.createTextMessage("This is a text message");
         producer.send(message);
      }

      ActiveMQConnectionFactory openCF = new ActiveMQConnectionFactory();

      Connection conn2 = openCF.createConnection();
      Session sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
      conn2.start();
      MessageConsumer messageConsumer = sess2.createConsumer(sess2.createQueue("exampleQueue"));

      for (int i = 0; i < 10; i++) {
         TextMessage messageReceived = (TextMessage) messageConsumer.receive(5000);
         assertEquals("This is a text message", messageReceived.getText());
      }

      conn1.close();
      conn2.close();
   }

   @Test
   public void testXAConsumer() throws Exception {
      Queue queue;
      try (Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE)) {
         queue = session.createQueue(queueName);
         System.out.println("Queue:" + queue);
         MessageProducer producer = session.createProducer(queue);
         for (int i = 0; i < 10; i++) {
            TextMessage msg = session.createTextMessage("test" + i);
            msg.setStringProperty("myobj", "test" + i);
            producer.send(msg);
         }
         session.close();
      }

      try (XAConnection xaconnection = xaFactory.createXAConnection()) {
         Xid xid = newXID();

         XASession session = xaconnection.createXASession();
         session.getXAResource().start(xid, XAResource.TMNOFLAGS);
         MessageConsumer consumer = session.createConsumer(queue);
         xaconnection.start();
         for (int i = 0; i < 5; i++) {
            TextMessage message = (TextMessage) consumer.receive(5000);
            Assert.assertNotNull(message);
            Assert.assertEquals("test" + i, message.getText());
         }
         session.getXAResource().end(xid, XAResource.TMSUCCESS);
         session.getXAResource().rollback(xid);
         consumer.close();
         xaconnection.close();
      }

      try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
         connection.start();
         MessageConsumer consumer = session.createConsumer(queue);
         for (int i = 0; i < 10; i++) {
            TextMessage message = (TextMessage) consumer.receive(5000);
            Assert.assertNotNull(message);
            //            Assert.assertEquals("test" + i, message.getText());
            System.out.println("Message " + message.getText());
         }
         checkDuplicate(consumer);
         System.out.println("Queue:" + queue);
         session.close();
      }

      System.err.println("Done!!!");
   }

   @Test
   public void testXASameConsumerRollback() throws Exception {
      Queue queue;
      try (Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE)) {
         queue = session.createQueue(queueName);
         System.out.println("Queue:" + queue);
         MessageProducer producer = session.createProducer(queue);
         for (int i = 0; i < 10; i++) {
            TextMessage msg = session.createTextMessage("test" + i);
            msg.setStringProperty("myobj", "test" + i);
            producer.send(msg);
         }
         session.close();
      }

      try (XAConnection xaconnection = xaFactory.createXAConnection()) {
         Xid xid = newXID();

         XASession session = xaconnection.createXASession();
         session.getXAResource().start(xid, XAResource.TMNOFLAGS);
         MessageConsumer consumer = session.createConsumer(queue);
         xaconnection.start();
         for (int i = 0; i < 5; i++) {
            TextMessage message = (TextMessage) consumer.receive(5000);
            Assert.assertNotNull(message);
            Assert.assertEquals("test" + i, message.getText());
         }
         session.getXAResource().end(xid, XAResource.TMSUCCESS);
         session.getXAResource().rollback(xid);

         xid = newXID();
         session.getXAResource().start(xid, XAResource.TMNOFLAGS);

         for (int i = 0; i < 10; i++) {
            TextMessage message = (TextMessage) consumer.receive(5000);
            Assert.assertNotNull(message);
            Assert.assertEquals("test" + i, message.getText());
         }

         checkDuplicate(consumer);

         session.getXAResource().end(xid, XAResource.TMSUCCESS);
         session.getXAResource().commit(xid, true);
      }
   }

   @Test
   public void testXAPrepare() throws Exception {
      try {

         XAConnection connection = xaFactory.createXAConnection();

         XASession xasession = connection.createXASession();

         Xid xid = newXID();
         xasession.getXAResource().start(xid, XAResource.TMNOFLAGS);
         Queue queue = xasession.createQueue(queueName);
         MessageProducer producer = xasession.createProducer(queue);
         producer.send(xasession.createTextMessage("hello"));
         producer.send(xasession.createTextMessage("hello"));
         xasession.getXAResource().end(xid, XAResource.TMSUCCESS);

         xasession.getXAResource().prepare(xid);

         connection.close();

         System.err.println("Done!!!");
      } catch (Exception e) {
         e.printStackTrace();
      }
   }

   @Test
   public void testAutoSend() throws Exception {
      connection.start();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      Queue queue = session.createQueue(queueName);
      MessageConsumer consumer = session.createConsumer(queue);

      MessageProducer producer = session.createProducer(queue);
      for (int i = 0; i < 10; i++) {
         producer.send(session.createTextMessage("testXX" + i));
      }
      connection.start();

      for (int i = 0; i < 10; i++) {
         TextMessage txt = (TextMessage) consumer.receive(5000);

         Assert.assertEquals("testXX" + i, txt.getText());
      }
   }

   /*
    * This test create a consumer on a connection to consume
    * messages slowly, so the connection stay for a longer time
    * than its configured TTL without any user data (messages)
    * coming from broker side. It tests the working of
    * KeepAlive mechanism without which the test will fail.
    */
   @Test
   public void testSendReceiveUsingTtl() throws Exception {
      String brokerUri = "failover://tcp://" + OWHOST + ":" + OWPORT + "?wireFormat.maxInactivityDuration=5000&wireFormat.maxInactivityDurationInitalDelay=1000";
      ActiveMQConnectionFactory testFactory = new ActiveMQConnectionFactory(brokerUri);

      Connection sendConnection = testFactory.createConnection();
      System.out.println("created send connection: " + sendConnection);
      Connection receiveConnection = testFactory.createConnection();
      System.out.println("created receive connection: " + receiveConnection);

      try {
         final int nMsg = 10;
         final long delay = 2L;

         AsyncConsumer consumer = new AsyncConsumer(queueName, receiveConnection, Session.CLIENT_ACKNOWLEDGE, delay, nMsg);

         Session sendSession = sendConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue queue = sendSession.createQueue(queueName);

         MessageProducer producer = sendSession.createProducer(queue);
         for (int i = 0; i < nMsg; i++) {
            producer.send(sendSession.createTextMessage("testXX" + i));
         }

         consumer.waitFor(nMsg * delay * 2);
      } finally {
         sendConnection.close();
         receiveConnection.close();
      }
   }

   @Test
   public void testCommitCloseConsumerBefore() throws Exception {
      testCommitCloseConsumer(true);
   }

   @Test
   public void testCommitCloseConsumerAfter() throws Exception {
      testCommitCloseConsumer(false);
   }

   private void testCommitCloseConsumer(boolean closeBefore) throws Exception {
      connection.start();
      Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

      Queue queue = session.createQueue(queueName);
      MessageConsumer consumer = session.createConsumer(queue);

      MessageProducer producer = session.createProducer(queue);
      for (int i = 0; i < 10; i++) {
         TextMessage msg = session.createTextMessage("testXX" + i);
         msg.setStringProperty("count", "str " + i);
         producer.send(msg);
      }
      session.commit();
      connection.start();

      for (int i = 0; i < 5; i++) {
         TextMessage txt = (TextMessage) consumer.receive(5000);
         Assert.assertEquals("testXX" + i, txt.getText());
      }
      if (closeBefore) {
         consumer.close();
      }

      session.commit();

      // we're testing two scenarios.
      // closing the consumer before commit or after commit
      if (!closeBefore) {
         consumer.close();
      }

      consumer = session.createConsumer(queue);
      //      Assert.assertNull(consumer.receiveNoWait());
      for (int i = 5; i < 10; i++) {
         TextMessage txt = (TextMessage) consumer.receive(5000);
         Assert.assertEquals("testXX" + i, txt.getText());
      }

      Assert.assertNull(consumer.receiveNoWait());

   }

   @Test
   public void testRollbackWithAcked() throws Exception {
      connection.start();
      Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

      Queue queue = session.createQueue(queueName);
      MessageConsumer consumer = session.createConsumer(queue);

      MessageProducer producer = session.createProducer(queue);
      for (int i = 0; i < 10; i++) {
         TextMessage msg = session.createTextMessage("testXX" + i);
         msg.setStringProperty("count", "str " + i);
         producer.send(msg);
      }
      session.commit();
      connection.start();

      for (int i = 0; i < 5; i++) {
         TextMessage txt = (TextMessage) consumer.receive(5000);
         Assert.assertEquals("testXX" + i, txt.getText());
      }

      session.rollback();

      consumer.close();

      consumer = session.createConsumer(queue);
      //      Assert.assertNull(consumer.receiveNoWait());
      for (int i = 0; i < 10; i++) {
         TextMessage txt = (TextMessage) consumer.receive(5000);
         //         System.out.println("TXT::" + txt);
         Assert.assertNotNull(txt);
         System.out.println("TXT " + txt.getText());
         //         Assert.assertEquals("testXX" + i, txt.getText());
      }
      session.commit();

      checkDuplicate(consumer);

   }

   @Test
   public void testRollbackLocal() throws Exception {
      connection.start();
      Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

      Queue queue = session.createQueue(queueName);
      MessageConsumer consumer = session.createConsumer(queue);

      MessageProducer producer = session.createProducer(queue);
      for (int i = 0; i < 10; i++) {
         TextMessage msg = session.createTextMessage("testXX" + i);
         msg.setStringProperty("count", "str " + i);
         producer.send(msg);
      }
      session.commit();
      connection.start();

      for (int i = 0; i < 5; i++) {
         TextMessage txt = (TextMessage) consumer.receive(500);
         Assert.assertEquals("testXX" + i, txt.getText());
      }

      session.rollback();

      for (int i = 0; i < 10; i++) {
         TextMessage txt = (TextMessage) consumer.receive(5000);
         Assert.assertNotNull(txt);
         System.out.println("TXT " + txt.getText());
         Assert.assertEquals("testXX" + i, txt.getText());
      }

      checkDuplicate(consumer);

      session.commit();

   }

   private void checkDuplicate(MessageConsumer consumer) throws JMSException {
      boolean duplicatedMessages = false;
      while (true) {
         TextMessage txt = (TextMessage) consumer.receiveNoWait();
         if (txt == null) {
            break;
         } else {
            duplicatedMessages = true;
            System.out.println("received in duplicate:" + txt.getText());
         }
      }

      Assert.assertFalse("received messages in duplicate", duplicatedMessages);
   }

   @Test
   public void testIndividualAck() throws Exception {
      connection.start();
      Session session = connection.createSession(false, ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE);

      Queue queue = session.createQueue(queueName);
      MessageConsumer consumer = session.createConsumer(queue);

      MessageProducer producer = session.createProducer(queue);
      for (int i = 0; i < 10; i++) {
         TextMessage msg = session.createTextMessage("testXX" + i);
         msg.setStringProperty("count", "str " + i);
         producer.send(msg);
      }
      connection.start();

      for (int i = 0; i < 5; i++) {
         TextMessage txt = (TextMessage) consumer.receive(5000);
         if (i == 4) {
            txt.acknowledge();
         }
         Assert.assertEquals("testXX" + i, txt.getText());
      }

      consumer.close();

      consumer = session.createConsumer(queue);
      //      Assert.assertNull(consumer.receiveNoWait());
      for (int i = 0; i < 4; i++) {
         TextMessage txt = (TextMessage) consumer.receive(5000);
         txt.acknowledge();
         Assert.assertEquals("testXX" + i, txt.getText());
      }

      for (int i = 5; i < 10; i++) {
         TextMessage txt = (TextMessage) consumer.receive(5000);
         txt.acknowledge();
         Assert.assertEquals("testXX" + i, txt.getText());
      }

      checkDuplicate(consumer);

      Assert.assertNull(consumer.receiveNoWait());

   }

   @Test
   public void testCommitCloseConsumeXA() throws Exception {

      Queue queue;
      {
         connection.start();
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

         queue = session.createQueue(queueName);

         MessageProducer producer = session.createProducer(queue);
         for (int i = 0; i < 10; i++) {
            TextMessage msg = session.createTextMessage("testXX" + i);
            msg.setStringProperty("count", "str " + i);
            producer.send(msg);
         }
         session.commit();
      }

      try (XAConnection xaconnection = xaFactory.createXAConnection()) {
         xaconnection.start();

         XASession xasession = xaconnection.createXASession();
         Xid xid = newXID();
         xasession.getXAResource().start(xid, XAResource.TMNOFLAGS);
         MessageConsumer consumer = xasession.createConsumer(queue);

         for (int i = 0; i < 5; i++) {
            TextMessage txt = (TextMessage) consumer.receive(5000);
            Assert.assertEquals("testXX" + i, txt.getText());
         }

         consumer.close();

         xasession.getXAResource().end(xid, XAResource.TMSUCCESS);
         xasession.getXAResource().prepare(xid);
         xasession.getXAResource().commit(xid, false);

         xaconnection.close();
      }

      {
         connection.start();
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         try (MessageConsumer consumer = session.createConsumer(queue)) {
            for (int i = 5; i < 10; i++) {
               TextMessage txt = (TextMessage) consumer.receive(5000);
               Assert.assertEquals("testXX" + i, txt.getText());
            }
         }

      }

   }

   @Test
   public void testTempQueueSendAfterConnectionClose() throws Exception {

      Connection connection1 = null;
      final Connection connection2 = factory.createConnection();

      try {
         connection1 = factory.createConnection();
         connection1.start();
         connection2.start();

         Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue tempQueue = session1.createTemporaryQueue();

         Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = session2.createProducer(tempQueue);

         // need to wait here because the ActiveMQ client's temp destination map is updated asynchronously, not waiting can introduce a race
         assertTrue(Wait.waitFor(() -> ((ActiveMQConnection)connection2).activeTempDestinations.size() == 1, 2000, 100));

         producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         TextMessage m = session2.createTextMessage("Hello temp queue");
         producer.send(m);

         MessageConsumer consumer = session1.createConsumer(tempQueue);
         TextMessage received = (TextMessage) consumer.receive(5000);
         assertNotNull(received);
         assertEquals("Hello temp queue", received.getText());

         //close first connection, let temp queue die
         connection1.close();

         // need to wait here because the ActiveMQ client's temp destination map is updated asynchronously, not waiting can introduce a race
         assertTrue(Wait.waitFor(() -> ((ActiveMQConnection)connection2).activeTempDestinations.size() == 0, 2000, 100));

         waitForBindings(this.server, tempQueue.getQueueName(), true, 0, 0, 5000);
         //send again
         try {
            producer.send(m);
            fail("Send should fail since temp destination should not exist anymore.");
         } catch (InvalidDestinationException e) {
            //ignore
         }
      } finally {
         if (connection1 != null) {
            connection1.close();
         }
         if (connection2 != null) {
            connection2.close();
         }
      }
   }

   @Test
   public void testNotificationProperties() throws Exception {
      try (TopicConnection topicConnection = factory.createTopicConnection()) {
         TopicSession topicSession = topicConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
         Topic notificationsTopic = topicSession.createTopic("activemq.notifications");
         TopicSubscriber subscriber = topicSession.createSubscriber(notificationsTopic);
         List<Message> receivedMessages = new CopyOnWriteArrayList<>();
         subscriber.setMessageListener(receivedMessages::add);
         topicConnection.start();

         Wait.waitFor(() -> receivedMessages.size() > 0);

         Assert.assertTrue(receivedMessages.size() > 0);

         for (Message message : receivedMessages) {
            assertNotNull(message);
            assertNotNull(message.getStringProperty("_AMQ_NotifType"));
         }
      }
   }

   @Test
   public void testXAResourceCommitSuspendedNotRemoved() throws Exception {
      Queue queue = null;

      Xid xid = newXID();
      try (XAConnection xaconnection = xaFactory.createXAConnection()) {
         XASession session = xaconnection.createXASession();
         queue = session.createQueue(queueName);
         session.getXAResource().start(xid, XAResource.TMNOFLAGS);
         session.getXAResource().end(xid, XAResource.TMSUSPEND);

         XidImpl xid1 = new XidImpl(xid);
         Transaction transaction = server.getResourceManager().getTransaction(xid1);
         //amq5.x doesn't pass suspend flags to broker,
         //directly suspend the tx
         transaction.suspend();

         session.getXAResource().commit(xid, true);
      } catch (XAException ex) {
         //ignore
      } finally {
         XidImpl xid1 = new XidImpl(xid);
         Transaction transaction = server.getResourceManager().getTransaction(xid1);
         assertNotNull(transaction);
      }
   }

   @Test
   public void testXAResourceRolledBackSuspendedNotRemoved() throws Exception {
      Queue queue = null;

      Xid xid = newXID();
      try (XAConnection xaconnection = xaFactory.createXAConnection()) {
         XASession session = xaconnection.createXASession();
         queue = session.createQueue(queueName);
         session.getXAResource().start(xid, XAResource.TMNOFLAGS);
         session.getXAResource().end(xid, XAResource.TMSUSPEND);

         XidImpl xid1 = new XidImpl(xid);
         Transaction transaction = server.getResourceManager().getTransaction(xid1);
         //directly suspend the tx
         transaction.suspend();

         session.getXAResource().rollback(xid);
      } catch (XAException ex) {
        //ignore
      } finally {
         XidImpl xid1 = new XidImpl(xid);
         Transaction transaction = server.getResourceManager().getTransaction(xid1);
         assertNotNull(transaction);
      }
   }

   @Test
   public void testXAResourceCommittedRemoved() throws Exception {
      Queue queue = null;

      Xid xid = newXID();
      try (XAConnection xaconnection = xaFactory.createXAConnection()) {
         XASession session = xaconnection.createXASession();
         queue = session.createQueue(queueName);
         session.getXAResource().start(xid, XAResource.TMNOFLAGS);
         MessageProducer producer = session.createProducer(queue);
         producer.send(session.createTextMessage("xa message"));
         session.getXAResource().end(xid, XAResource.TMSUCCESS);
         session.getXAResource().commit(xid, true);
      }
      XidImpl xid1 = new XidImpl(xid);
      Transaction transaction = server.getResourceManager().getTransaction(xid1);
      assertNull(transaction);
   }

   @Test
   public void testXAResourceRolledBackRemoved() throws Exception {
      Queue queue = null;

      Xid xid = newXID();
      try (XAConnection xaconnection = xaFactory.createXAConnection()) {
         XASession session = xaconnection.createXASession();
         queue = session.createQueue(queueName);
         session.getXAResource().start(xid, XAResource.TMNOFLAGS);
         MessageProducer producer = session.createProducer(queue);
         producer.send(session.createTextMessage("xa message"));
         session.getXAResource().end(xid, XAResource.TMSUCCESS);
         session.getXAResource().rollback(xid);
      }
      XidImpl xid1 = new XidImpl(xid);
      Transaction transaction = server.getResourceManager().getTransaction(xid1);
      assertNull(transaction);
   }

   private void checkQueueEmpty(String qName) {
      PostOffice po = server.getPostOffice();
      LocalQueueBinding binding = (LocalQueueBinding) po.getBinding(SimpleString.toSimpleString(qName));
      try {
         //waiting for last ack to finish
         Thread.sleep(1000);
      } catch (InterruptedException e) {
      }
      assertEquals(0L, binding.getQueue().getMessageCount());
   }

   private class AsyncConsumer {

      private List<Message> messages = new ArrayList<>();
      private CountDownLatch latch = new CountDownLatch(1);
      private int nMsgs;
      private String queueName;

      private MessageConsumer consumer;

      AsyncConsumer(String queueName,
                    Connection receiveConnection,
                    final int ackMode,
                    final long delay,
                    final int expectedMsgs) throws JMSException {
         this.queueName = queueName;
         this.nMsgs = expectedMsgs;
         Session session = receiveConnection.createSession(false, ackMode);
         Queue queue = session.createQueue(queueName);
         consumer = session.createConsumer(queue);
         consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
               System.out.println("received : " + message);

               messages.add(message);

               if (messages.size() < expectedMsgs) {
                  //delay
                  try {
                     TimeUnit.SECONDS.sleep(delay);
                  } catch (InterruptedException e) {
                     e.printStackTrace();
                  }
               }
               if (ackMode == Session.CLIENT_ACKNOWLEDGE) {
                  try {
                     message.acknowledge();
                  } catch (JMSException e) {
                     System.err.println("Failed to acknowledge " + message);
                     e.printStackTrace();
                  }
               }
               if (messages.size() == expectedMsgs) {
                  latch.countDown();
               }
            }
         });
         receiveConnection.start();
      }

      public void waitFor(long timeout) throws TimeoutException, InterruptedException, JMSException {
         boolean result = latch.await(timeout, TimeUnit.SECONDS);
         assertTrue(result);
         //check queue empty
         checkQueueEmpty(queueName);
         //then check messages still the size and no dup.
         assertEquals(nMsgs, messages.size());
      }
   }
}
