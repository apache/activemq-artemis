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

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.XAConnection;
import javax.jms.XASession;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.postoffice.impl.LocalQueueBinding;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SimpleOpenWireTest extends BasicOpenWireTest {

   @Override
   @Before
   public void setUp() throws Exception {
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
      }
      catch (Throwable e) {
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

   //   @Test -- ignored for now
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
      AddressSettings addressSetting = new AddressSettings();
      addressSetting.setAutoCreateJmsQueues(false);

      server.getAddressSettingsRepository().addMatch("jms.queue.foo", addressSetting);

      connection.start();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue queue = session.createQueue("foo");

      try {
         session.createProducer(queue);
      }
      catch (JMSException expected) {
      }
      session.close();
   }

   @Test
   public void testAutoDestinationCreationOnProducerSend() throws JMSException {
      AddressSettings addressSetting = new AddressSettings();
      addressSetting.setAutoCreateJmsQueues(true);

      String address = "foo";
      server.getAddressSettingsRepository().addMatch("jms.queue." + address, addressSetting);

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
   public void testAutoDestinationCreationOnConsumer() throws JMSException {
      AddressSettings addressSetting = new AddressSettings();
      addressSetting.setAutoCreateJmsQueues(true);

      String address = "foo";
      server.getAddressSettingsRepository().addMatch("jms.queue." + address, addressSetting);

      connection.start();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      TextMessage message = session.createTextMessage("bar");
      Queue queue = new ActiveMQQueue(address);

      MessageConsumer consumer = session.createConsumer(queue);

      MessageProducer producer = session.createProducer(null);
      producer.send(queue, message);

      TextMessage message1 = (TextMessage) consumer.receive(1000);
      assertTrue(message1.getText().equals(message.getText()));
   }

   @Test
   public void testAutoDestinationNoCreationOnConsumer() throws JMSException {
      AddressSettings addressSetting = new AddressSettings();
      addressSetting.setAutoCreateJmsQueues(false);

      String address = "foo";
      server.getAddressSettingsRepository().addMatch("jms.queue." + address, addressSetting);

      connection.start();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      TextMessage message = session.createTextMessage("bar");
      Queue queue = new ActiveMQQueue(address);

      try {
         MessageConsumer consumer = session.createConsumer(queue);
         fail("supposed to throw an exception here");
      }
      catch (JMSException e) {

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
      }
      finally {
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

      SimpleString durableQueue = new SimpleString("jms.queue.exampleQueue");
      this.server.createQueue(durableQueue, durableQueue, null, true, false);

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
      }
      finally {
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

      SimpleString durableQueue = new SimpleString("jms.queue.exampleQueue");
      this.server.createQueue(durableQueue, durableQueue, null, true, false);

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
      }
      finally {
         if (exConn != null) {
            exConn.close();
         }
      }

   }

   @Test
   public void testMixedOpenWireExample() throws Exception {
      Connection openConn = null;

      SimpleString durableQueue = new SimpleString("jms.queue.exampleQueue");
      this.server.createQueue(durableQueue, durableQueue, null, true, false);

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

      SimpleString durableQueue = new SimpleString("jms.queue.exampleQueue");
      this.server.createQueue(durableQueue, durableQueue, null, true, false);

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
      }
      catch (Exception e) {
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
      String brokerUri = "failover://tcp://" + OWHOST + ":" + OWPORT + "?wireFormat.maxInactivityDuration=10000&wireFormat.maxInactivityDurationInitalDelay=5000";
      ActiveMQConnectionFactory testFactory = new ActiveMQConnectionFactory(brokerUri);

      Connection sendConnection = testFactory.createConnection();
      System.out.println("created send connection: " + sendConnection);
      Connection receiveConnection = testFactory.createConnection();
      System.out.println("created receive connection: " + receiveConnection);

      try {
         final int nMsg = 20;
         final long delay = 2L;

         AsyncConsumer consumer = new AsyncConsumer(queueName, receiveConnection, Session.CLIENT_ACKNOWLEDGE, delay, nMsg);

         Session sendSession = sendConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue queue = sendSession.createQueue(queueName);

         MessageProducer producer = sendSession.createProducer(queue);
         for (int i = 0; i < nMsg; i++) {
            producer.send(sendSession.createTextMessage("testXX" + i));
         }

         consumer.waitFor(nMsg * delay * 2);
      }
      finally {
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
         }
         else {
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

   private void checkQueueEmpty(String qName) {
      PostOffice po = server.getPostOffice();
      LocalQueueBinding binding = (LocalQueueBinding) po.getBinding(SimpleString.toSimpleString("jms.queue." + qName));
      try {
         //waiting for last ack to finish
         Thread.sleep(1000);
      }
      catch (InterruptedException e) {
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
                  }
                  catch (InterruptedException e) {
                     e.printStackTrace();
                  }
               }
               if (ackMode == Session.CLIENT_ACKNOWLEDGE) {
                  try {
                     message.acknowledge();
                  }
                  catch (JMSException e) {
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
