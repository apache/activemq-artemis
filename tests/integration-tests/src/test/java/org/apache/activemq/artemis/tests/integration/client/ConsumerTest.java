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

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.stream.Stream;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQIllegalStateException;
import org.apache.activemq.artemis.api.core.Interceptor;
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
import org.apache.activemq.artemis.core.client.impl.ClientConsumerInternal;
import org.apache.activemq.artemis.core.client.impl.ServerLocatorImpl;
import org.apache.activemq.artemis.core.protocol.core.Packet;
import org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.artemis.utils.ByteUtil;
import org.apache.activemq.artemis.utils.collections.ConcurrentHashSet;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(value = Parameterized.class)
public class ConsumerTest extends ActiveMQTestBase {

   @Parameterized.Parameters(name = "isNetty={0}, persistent={1}")
   public static Collection getParameters() {
      return Arrays.asList(new Object[][]{{true, true}, {false, false}, {false, true}, {true, false}});
   }

   public ConsumerTest(boolean netty, boolean durable) {
      this.netty = netty;
      this.durable = durable;
   }

   private final boolean durable;
   private final boolean netty;
   private ActiveMQServer server;

   private final SimpleString QUEUE = new SimpleString("ConsumerTestQueue");

   private ServerLocator locator;

   protected boolean isNetty() {
      return netty;
   }

   @Before
   @Override
   public void setUp() throws Exception {
      super.setUp();

      server = createServer(durable, isNetty());

      server.start();

      locator = createFactory(isNetty());
   }

   @Before
   public void createQueue() throws Exception {

      ServerLocator locator = createFactory(isNetty());

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSessionFactory sf2 = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true, true);

      server.createQueue(QUEUE, RoutingType.ANYCAST, QUEUE, null, true, false);

      session.close();

      sf.close();

      locator.close();
   }

   @Test
   public void testStressConnection() throws Exception {

      for (int i = 0; i < 10; i++) {
         ServerLocator locatorSendx = createFactory(isNetty()).setReconnectAttempts(15);
         ClientSessionFactory factoryx = locatorSendx.createSessionFactory();
         factoryx.close();
         locatorSendx.close();
      }

   }

   @Test
   public void testSimpleSend() throws Throwable {
      receive(false);
   }

   @Test
   public void testSimpleSendWithCloseConsumer() throws Throwable {
      receive(true);
   }

   private void receive(boolean cancelOnce) throws Throwable {
      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true, false);

      ClientProducer producer = session.createProducer(QUEUE);
      ClientMessage message = session.createMessage(Message.TEXT_TYPE, true, 0, System.currentTimeMillis(), (byte) 4);
      message.getBodyBuffer().writeString("hi");
      message.putStringProperty("hello", "elo");
      producer.send(message);

      session.commit();

      session.close();
      if (durable) {
         server.stop();
         server.start();
      }
      sf = createSessionFactory(locator);
      session = sf.createSession(false, true, true, false);
      ClientConsumer consumer = session.createConsumer(QUEUE);

      session.start();

      if (cancelOnce) {
         final ClientConsumerInternal consumerInternal = (ClientConsumerInternal) consumer;
         Wait.waitFor(() -> consumerInternal.getBufferSize() > 0);
         consumer.close();
         consumer = session.createConsumer(QUEUE);
      }
      ClientMessage message2 = consumer.receive(1000);

      Assert.assertNotNull(message2);

      System.out.println("Id::" + message2.getMessageID());

      System.out.println("Received " + message2);

      System.out.println("Clie:" + ByteUtil.bytesToHex(message2.getBuffer().array(), 4));

      System.out.println("String::" + message2.getReadOnlyBodyBuffer().readString());

      Assert.assertEquals("elo", message2.getStringProperty("hello"));

      Assert.assertEquals("hi", message2.getReadOnlyBodyBuffer().readString());

      Assert.assertNull(consumer.receiveImmediate());

      session.close();
   }

   @Test
   public void testSendReceiveAMQP() throws Throwable {

      if (!isNetty()) {
         // no need to run the test, there's no AMQP support
         return;
      }

      internalSend(2, 2);
   }

   @Test
   public void testSendReceiveCore() throws Throwable {

      if (!isNetty()) {
         // no need to run the test, there's no AMQP support
         return;
      }

      internalSend(1, 1);
   }

   @Test
   public void testSendAMQPReceiveCore() throws Throwable {

      if (!isNetty()) {
         // no need to run the test, there's no AMQP support
         return;
      }

      internalSend(2, 1);
   }

   @Test
   public void testAutoCreateMulticastAddress() throws Throwable {
      if (!isNetty()) {
         // no need to run the test, there's no AMQP support
         return;
      }

      assertNull(server.getAddressInfo(SimpleString.toSimpleString("topic")));

      ConnectionFactory factorySend = createFactory(2);
      Connection connection = factorySend.createConnection();

      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         javax.jms.Topic topic = session.createTopic("topic");
         MessageProducer producer = session.createProducer(topic);
         producer.setDeliveryMode(DeliveryMode.PERSISTENT);

         TextMessage msg = session.createTextMessage("hello");
         msg.setIntProperty("mycount", 0);
         producer.send(msg);
      } finally {
         connection.close();
      }

      assertNotNull(server.getAddressInfo(SimpleString.toSimpleString("topic")));
      assertEquals(RoutingType.MULTICAST, server.getAddressInfo(SimpleString.toSimpleString("topic")).getRoutingType());
      assertEquals(0, server.getTotalMessageCount());
   }

   @Test
   public void testAutoCreateCOnConsumerAMQP() throws Throwable {
      testAutoCreate(2);
   }

   @Test
   public void testAutoCreateCOnConsumerCore() throws Throwable {
      testAutoCreate(1);
   }

   @Test
   public void testAutoCreateCOnConsumerOpenWire() throws Throwable {
      testAutoCreate(3);
   }

   private void testAutoCreate(int protocol) throws Throwable {

      final SimpleString thisQueue = SimpleString.toSimpleString("ThisQueue");
      if (!isNetty()) {
         // no need to run the test, there's no AMQP support
         return;
      }

      for (int i = 0; i < 10; i++) {
         ConnectionFactory factorySend = createFactory(protocol);
         Connection connection = factorySend.createConnection();

         try {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            javax.jms.Queue queue = session.createQueue(thisQueue.toString());
            MessageProducer producer = session.createProducer(queue);

            MessageConsumer consumer = session.createConsumer(queue);
            connection.start();

            producer.send(session.createTextMessage("hello"));

            Assert.assertNotNull(consumer.receive(5000));
            consumer.close();
            session.close();
         } finally {
            connection.close();
         }

         Wait.waitFor(() -> server.getAddressInfo(thisQueue) == null, 1000, 10);
         assertNull(server.getAddressInfo(thisQueue));
         assertEquals(0, server.getTotalMessageCount());
      }
   }

   @Test
   public void testAutoDeleteAutoCreatedAddressAndQueue() throws Throwable {
      if (!isNetty()) {
         // no need to run the test, there's no AMQP support
         return;
      }

      assertNull(server.getAddressInfo(SimpleString.toSimpleString("queue")));

      ConnectionFactory factorySend = createFactory(2);
      Connection connection = factorySend.createConnection();

      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         javax.jms.Queue queue = session.createQueue("queue");
         MessageProducer producer = session.createProducer(queue);
         producer.setDeliveryMode(DeliveryMode.PERSISTENT);

         TextMessage msg = session.createTextMessage("hello");
         msg.setIntProperty("mycount", 0);
         producer.send(msg);

         connection.start();
         MessageConsumer consumer = session.createConsumer(queue);
         assertNotNull(consumer.receive(1000));
      } finally {
         connection.close();
      }

      Wait.assertTrue(() -> server.getAddressInfo(SimpleString.toSimpleString("queue")) == null);
      Wait.assertTrue(() -> server.locateQueue(SimpleString.toSimpleString("queue")) == null);
      Wait.assertEquals(0, server::getTotalMessageCount);
   }

   @Test
   public void testSendCoreReceiveAMQP() throws Throwable {

      if (!isNetty()) {
         // no need to run the test, there's no AMQP support
         return;
      }

      internalSend(1, 2);
   }

   @Test
   public void testSendOpenWireReceiveAMQP() throws Throwable {

      if (!isNetty()) {
         // no need to run the test, there's no AMQP support
         return;
      }

      internalSend(3, 2);
   }

   @Test
   public void testSendAMQPReceiveOpenWire() throws Throwable {

      if (!isNetty()) {
         // no need to run the test, there's no AMQP support
         return;
      }

      internalSend(2, 3);
   }

   @Test
   public void testOpenWireReceiveCore() throws Throwable {

      if (!isNetty()) {
         // no need to run the test, there's no AMQP support
         return;
      }

      internalSend(3, 1);
   }

   @Test
   public void testCoreReceiveOpenwire() throws Throwable {

      if (!isNetty()) {
         // no need to run the test, there's no AMQP support
         return;
      }

      internalSend(1, 3);
   }



   public static class MyTest implements Serializable {
      int i;

      public int getI() {
         return i;
      }

      public MyTest setI(int i) {
         this.i = i;
         return this;
      }
   }

   private ConnectionFactory createFactory(int protocol) {
      switch (protocol) {
         case 1: return new ActiveMQConnectionFactory();// core protocol
         case 2: return new JmsConnectionFactory("amqp://localhost:61616"); // amqp
         case 3: return new org.apache.activemq.ActiveMQConnectionFactory("tcp://localhost:61616"); // openwire
         default: return null;
      }
   }


   public void internalSimpleSend(int protocolSender, int protocolConsumer) throws Throwable {

      ConnectionFactory factorySend = createFactory(protocolSender);
      ConnectionFactory factoryConsume = protocolConsumer == protocolSender ? factorySend : createFactory(protocolConsumer);


      Connection connection = factorySend.createConnection();

      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         javax.jms.Queue queue = session.createQueue(QUEUE.toString());
         MessageProducer producer = session.createProducer(queue);
         producer.setDeliveryMode(DeliveryMode.PERSISTENT);

         TextMessage msg = session.createTextMessage("hello");
         msg.setIntProperty("mycount", 0);
         producer.send(msg);
         connection.close();

         connection = factoryConsume.createConnection();
         session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         queue = session.createQueue(QUEUE.toString());

         connection.start();

         MessageConsumer consumer = session.createConsumer(queue);

         TextMessage message = (TextMessage) consumer.receive(1000);
         Assert.assertNotNull(message);
         Assert.assertEquals(0, message.getIntProperty("mycount"));
         Assert.assertEquals("hello", message.getText());

         Wait.waitFor(() -> server.getPagingManager().getGlobalSize() == 0, 5000, 100);
         Assert.assertEquals(0, server.getPagingManager().getGlobalSize());

      } finally {
         connection.close();
      }
   }


   public void internalSend(int protocolSender, int protocolConsumer) throws Throwable {

      internalSimpleSend(protocolSender, protocolConsumer);

      ConnectionFactory factorySend = createFactory(protocolSender);
      ConnectionFactory factoryConsume = protocolConsumer == protocolSender ? factorySend : createFactory(protocolConsumer);


      Connection connection = factorySend.createConnection();

      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         javax.jms.Queue queue = session.createQueue(QUEUE.toString());
         MessageProducer producer = session.createProducer(queue);
         producer.setDeliveryMode(DeliveryMode.PERSISTENT);

         long time = System.currentTimeMillis();
         int NUMBER_OF_MESSAGES = 100;
         for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
            TextMessage msg = session.createTextMessage("hello " + i);
            msg.setIntProperty("mycount", i);
            producer.send(msg);

            ObjectMessage objectMessage = session.createObjectMessage(new MyTest().setI(i));
            producer.send(objectMessage);

            MapMessage mapMessage = session.createMapMessage();
            mapMessage.setInt("intOne", i);
            mapMessage.setString("stringOne", Integer.toString(i));
            producer.send(mapMessage);

            StreamMessage stream = session.createStreamMessage();
            stream.writeBoolean(true);
            stream.writeInt(i);
            producer.send(stream);

            BytesMessage bytes = session.createBytesMessage();
            bytes.writeUTF("string " + i);
            producer.send(bytes);
         }
         long end = System.currentTimeMillis();

         System.out.println("Time = " + (end - time));

         {
            TextMessage dummyMessage = session.createTextMessage();
            dummyMessage.setJMSType("car");
            dummyMessage.setStringProperty("color", "red");
            dummyMessage.setLongProperty("weight", 3000);
            dummyMessage.setText("testSelectorExampleFromSpecs:1");
            producer.send(dummyMessage);

            dummyMessage = session.createTextMessage();
            dummyMessage.setJMSType("car");
            dummyMessage.setStringProperty("color", "blue");
            dummyMessage.setLongProperty("weight", 3000);
            dummyMessage.setText("testSelectorExampleFromSpecs:2");
            producer.send(dummyMessage);
         }




         connection.close();

         if (this.durable) {
            server.stop();
            server.start();
         }

         connection = factoryConsume.createConnection();
         session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         queue = session.createQueue(QUEUE.toString());

         connection.start();

         MessageConsumer consumer = session.createConsumer(queue);

         for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
            TextMessage message = (TextMessage) consumer.receive(1000);
            Assert.assertNotNull(message);
            Assert.assertEquals(i, message.getIntProperty("mycount"));
            Assert.assertEquals("hello " + i, message.getText());

            ObjectMessage objectMessage = (ObjectMessage)consumer.receive(5000);
            Assert.assertNotNull(objectMessage);
            Assert.assertEquals(i, ((MyTest)objectMessage.getObject()).getI());

            MapMessage mapMessage = (MapMessage) consumer.receive(1000);
            Assert.assertNotNull(mapMessage);
            Assert.assertEquals(i, mapMessage.getInt("intOne"));
            Assert.assertEquals(Integer.toString(i), mapMessage.getString("stringOne"));

            StreamMessage stream = (StreamMessage)consumer.receive(5000);
            Assert.assertTrue(stream.readBoolean());
            Assert.assertEquals(i, stream.readInt());

            BytesMessage bytes = (BytesMessage) consumer.receive(5000);
            Assert.assertEquals("string " + i, bytes.readUTF());
         }

         consumer.close();

         consumer = session.createConsumer(queue, "JMSType = 'car' AND color = 'blue' AND weight > 2500");

         TextMessage msg = (TextMessage) consumer.receive(1000);
         Assert.assertEquals("testSelectorExampleFromSpecs:2", msg.getText());

         consumer.close();

         consumer = session.createConsumer(queue);
         msg = (TextMessage)consumer.receive(5000);
         Assert.assertNotNull(msg);

         Assert.assertNull(consumer.receiveNoWait());

         Wait.waitFor(() -> server.getPagingManager().getGlobalSize() == 0, 5000, 100);


         Assert.assertEquals(0, server.getPagingManager().getGlobalSize());

      } finally {
         connection.close();
      }
   }

   @Test
   public void testConsumerAckImmediateAutoCommitTrue() throws Exception {
      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true, true);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = createTextMessage(session, "m" + i);
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(QUEUE);
      session.start();
      for (int i = 0; i < numMessages; i++) {
         ClientMessage message2 = consumer.receive(1000);

         Assert.assertEquals("m" + i, message2.getBodyBuffer().readString());
      }
      // assert that all the messages are there and none have been acked
      Assert.assertEquals(0, ((Queue) server.getPostOffice().getBinding(QUEUE).getBindable()).getDeliveringCount());
      Assert.assertEquals(0, getMessageCount(((Queue) server.getPostOffice().getBinding(QUEUE).getBindable())));

      session.close();
   }

   @Test
   public void testConsumerAckImmediateAutoCommitFalse() throws Exception {

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, false, true);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = createTextMessage(session, "m" + i);
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(QUEUE);
      session.start();
      for (int i = 0; i < numMessages; i++) {
         ClientMessage message2 = consumer.receive(1000);

         Assert.assertEquals("m" + i, message2.getBodyBuffer().readString());
      }
      // assert that all the messages are there and none have been acked
      Assert.assertEquals(0, ((Queue) server.getPostOffice().getBinding(QUEUE).getBindable()).getDeliveringCount());
      Assert.assertEquals(0, getMessageCount(((Queue) server.getPostOffice().getBinding(QUEUE).getBindable())));

      session.close();
   }

   @Test
   public void testConsumerAckImmediateAckIgnored() throws Exception {

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true, true);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = createTextMessage(session, "m" + i);
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(QUEUE);
      session.start();
      for (int i = 0; i < numMessages; i++) {
         ClientMessage message2 = consumer.receive(1000);

         Assert.assertEquals("m" + i, message2.getBodyBuffer().readString());
         if (i < 50) {
            message2.acknowledge();
         }
      }
      // assert that all the messages are there and none have been acked
      Assert.assertEquals(0, ((Queue) server.getPostOffice().getBinding(QUEUE).getBindable()).getDeliveringCount());
      Assert.assertEquals(0, getMessageCount(((Queue) server.getPostOffice().getBinding(QUEUE).getBindable())));

      session.close();
   }

   @Test
   public void testConsumerAckImmediateCloseSession() throws Exception {

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true, true);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = createTextMessage(session, "m" + i);
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(QUEUE);
      session.start();
      for (int i = 0; i < numMessages; i++) {
         ClientMessage message2 = consumer.receive(1000);

         Assert.assertEquals("m" + i, message2.getBodyBuffer().readString());
         if (i < 50) {
            message2.acknowledge();
         }
      }
      // assert that all the messages are there and none have been acked
      Assert.assertEquals(0, ((Queue) server.getPostOffice().getBinding(QUEUE).getBindable()).getDeliveringCount());
      Assert.assertEquals(0, getMessageCount(((Queue) server.getPostOffice().getBinding(QUEUE).getBindable())));

      session.close();

      Assert.assertEquals(0, ((Queue) server.getPostOffice().getBinding(QUEUE).getBindable()).getDeliveringCount());
      Assert.assertEquals(0, getMessageCount(((Queue) server.getPostOffice().getBinding(QUEUE).getBindable())));
   }

   @Test
   public void testAcksWithSmallSendWindow() throws Exception {
      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = createTextMessage(session, "m" + i);
         producer.send(message);
      }
      session.close();
      sf.close();
      final CountDownLatch latch = new CountDownLatch(numMessages);
      server.getRemotingService().addIncomingInterceptor(new Interceptor() {
         @Override
         public boolean intercept(final Packet packet, final RemotingConnection connection) throws ActiveMQException {
            if (packet.getType() == PacketImpl.SESS_ACKNOWLEDGE) {
               latch.countDown();
            }
            return true;
         }
      });
      ServerLocator locator = createInVMNonHALocator().setConfirmationWindowSize(100).setAckBatchSize(-1);
      ClientSessionFactory sfReceive = createSessionFactory(locator);
      ClientSession sessionRec = sfReceive.createSession(false, true, true);
      ClientConsumer consumer = sessionRec.createConsumer(QUEUE);
      consumer.setMessageHandler(new MessageHandler() {
         @Override
         public void onMessage(final ClientMessage message) {
            try {
               message.acknowledge();
            } catch (ActiveMQException e) {
               e.printStackTrace();
            }
         }
      });
      sessionRec.start();
      Assert.assertTrue(latch.await(60, TimeUnit.SECONDS));
      sessionRec.close();
      locator.close();
   }

   // https://jira.jboss.org/browse/HORNETQ-410
   @Test
   public void testConsumeWithNoConsumerFlowControl() throws Exception {

      ServerLocator locator = createInVMNonHALocator();

      locator.setConsumerWindowSize(-1);

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true);

      session.start();

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = createTextMessage(session, "m" + i);
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(QUEUE);

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = consumer.receive(10000);
         assertNotNull(message);
         message.acknowledge();
      }

      session.close();
      sf.close();
      locator.close();

   }

   @Test
   public void testClearListener() throws Exception {
      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true);
      session.start();

      ClientConsumer consumer = session.createConsumer(QUEUE);

      consumer.setMessageHandler(new MessageHandler() {
         @Override
         public void onMessage(final ClientMessage msg) {
         }
      });

      consumer.setMessageHandler(null);
      consumer.receiveImmediate();

      session.close();
   }

   @Test
   public void testNoReceiveWithListener() throws Exception {
      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true);

      ClientConsumer consumer = session.createConsumer(QUEUE);

      consumer.setMessageHandler(new MessageHandler() {
         @Override
         public void onMessage(final ClientMessage msg) {
         }
      });

      try {
         consumer.receiveImmediate();
         Assert.fail("Should throw exception");
      } catch (ActiveMQIllegalStateException ise) {
         //ok
      } catch (ActiveMQException me) {
         Assert.fail("Wrong exception code");
      }

      session.close();
   }

   @Test
   public void testReceiveAndResend() throws Exception {

      final Set<Object> sessions = new ConcurrentHashSet<>();
      final AtomicInteger errors = new AtomicInteger(0);

      final SimpleString QUEUE_RESPONSE = SimpleString.toSimpleString("QUEUE_RESPONSE");

      final int numberOfSessions = 50;
      final int numberOfMessages = 10;

      final CountDownLatch latchReceive = new CountDownLatch(numberOfSessions * numberOfMessages);

      ClientSessionFactory sf = locator.createSessionFactory();
      for (int i = 0; i < numberOfSessions; i++) {

         ClientSession session = sf.createSession(false, true, true);

         sessions.add(session);

         session.createQueue(QUEUE, QUEUE.concat("" + i), null, true);

         if (i == 0) {
            session.createQueue(QUEUE_RESPONSE, QUEUE_RESPONSE);
         }

         ClientConsumer consumer = session.createConsumer(QUEUE.concat("" + i));
         sessions.add(consumer);

         {

            consumer.setMessageHandler(new MessageHandler() {
               @Override
               public void onMessage(final ClientMessage msg) {
                  try {
                     ServerLocator locatorSendx = createFactory(isNetty()).setReconnectAttempts(15);
                     ClientSessionFactory factoryx = locatorSendx.createSessionFactory();
                     ClientSession sessionSend = factoryx.createSession(true, true);

                     sessions.add(sessionSend);
                     sessions.add(locatorSendx);
                     sessions.add(factoryx);

                     final ClientProducer prod = sessionSend.createProducer(QUEUE_RESPONSE);
                     sessionSend.start();

                     sessions.add(prod);

                     msg.acknowledge();
                     prod.send(sessionSend.createMessage(true));
                     prod.close();
                     sessionSend.commit();
                     sessionSend.close();
                     factoryx.close();
                     if (Thread.currentThread().isInterrupted()) {
                        System.err.println("Netty has interrupted a thread!!!");
                        errors.incrementAndGet();
                     }

                  } catch (Throwable e) {
                     e.printStackTrace();
                     errors.incrementAndGet();
                  } finally {
                     latchReceive.countDown();
                  }
               }
            });
         }

         session.start();
      }

      Thread tCons = new Thread() {
         @Override
         public void run() {
            try {
               final ServerLocator locatorSend = createFactory(isNetty());
               final ClientSessionFactory factory = locatorSend.createSessionFactory();
               final ClientSession sessionSend = factory.createSession(true, true);
               ClientConsumer cons = sessionSend.createConsumer(QUEUE_RESPONSE);
               sessionSend.start();

               for (int i = 0; i < numberOfMessages * numberOfSessions; i++) {
                  ClientMessage msg = cons.receive(5000);
                  if (msg == null) {
                     break;
                  }
                  msg.acknowledge();
               }

               if (cons.receiveImmediate() != null) {
                  System.out.println("ERROR: Received an extra message");
                  errors.incrementAndGet();
               }
               sessionSend.close();
               factory.close();
               locatorSend.close();
            } catch (Exception e) {
               e.printStackTrace();
               errors.incrementAndGet();

            }

         }
      };

      tCons.start();

      ClientSession mainSessionSend = sf.createSession(true, true);
      ClientProducer mainProd = mainSessionSend.createProducer(QUEUE);

      for (int i = 0; i < numberOfMessages; i++) {
         mainProd.send(mainSessionSend.createMessage(true));
      }

      latchReceive.await(2, TimeUnit.MINUTES);

      tCons.join();

      sf.close();

      assertEquals("Had errors along the execution", 0, errors.get());
   }

   // https://jira.jboss.org/jira/browse/HORNETQ-111
   // Test that, on rollback credits are released for messages cleared in the buffer
   @Test
   public void testConsumerCreditsOnRollback() throws Exception {
      locator.setConsumerWindowSize(10000);

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createTransactedSession();

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      final byte[] bytes = new byte[1000];

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = session.createMessage(false);

         message.getBodyBuffer().writeBytes(bytes);

         message.putIntProperty("count", i);

         producer.send(message);
      }

      session.commit();

      ClientConsumer consumer = session.createConsumer(QUEUE);
      session.start();

      for (int i = 0; i < 110; i++) {
         ClientMessage message = consumer.receive();

         int count = message.getIntProperty("count");

         boolean redelivered = message.getDeliveryCount() > 1;

         if (count % 2 == 0 && !redelivered) {
            session.rollback();
         } else {
            session.commit();
         }
      }

      session.close();
   }

   // https://jira.jboss.org/jira/browse/HORNETQ-111
   // Test that, on rollback credits are released for messages cleared in the buffer
   @Test
   public void testInVMURI() throws Exception {
      locator.close();
      ServerLocator locator = addServerLocator(ServerLocatorImpl.newLocator("vm:/1"));
      ClientSessionFactory factory = locator.createSessionFactory();
      ClientSession session = factory.createSession();
      ClientProducer producer = session.createProducer(QUEUE);
      producer.send(session.createMessage(true));

      ClientConsumer consumer = session.createConsumer(QUEUE);
      session.start();
      Assert.assertNotNull(consumer.receiveImmediate());
      session.close();
      factory.close();

   }

   // https://jira.jboss.org/jira/browse/HORNETQ-111
   // Test that, on rollback credits are released for messages cleared in the buffer
   @Test
   public void testConsumerCreditsOnRollbackLargeMessages() throws Exception {
      locator.setConsumerWindowSize(10000).setMinLargeMessageSize(1000);

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createTransactedSession();

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      final byte[] bytes = new byte[10000];

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = session.createMessage(false);

         message.getBodyBuffer().writeBytes(bytes);

         message.putIntProperty("count", i);

         producer.send(message);
      }

      session.commit();

      ClientConsumer consumer = session.createConsumer(QUEUE);
      session.start();

      for (int i = 0; i < 110; i++) {
         ClientMessage message = consumer.receive();

         int count = message.getIntProperty("count");

         boolean redelivered = message.getDeliveryCount() > 1;

         if (count % 2 == 0 && !redelivered) {
            session.rollback();
         } else {
            session.commit();
         }
      }

      session.close();
   }

   @Test
   public void testMultipleConsumersOnSharedQueue() throws Throwable {
      if (!isNetty() || this.durable) {
         return;
      }
      final boolean durable = false;
      final long TIMEOUT_MILLIS = TimeUnit.MINUTES.toMillis(1);
      final int forks = 100;
      final int queues = forks;
      final int runs = 1;
      final int messages = 1;
      final ConnectionFactory factorySend = createFactory(1);
      final AtomicLongArray receivedMessages = new AtomicLongArray(forks);
      final Thread[] producersRunners = new Thread[forks];
      final Thread[] consumersRunners = new Thread[forks];
      //parties are forks (1 producer 1 consumer) + 1 controller in the main test thread
      final CyclicBarrier onStartRun = new CyclicBarrier((forks * 2) + 1);
      final CyclicBarrier onFinishRun = new CyclicBarrier((forks * 2) + 1);

      final int messagesSent = forks * messages;
      final AtomicInteger messagesRecieved = new AtomicInteger(0);

      for (int i = 0; i < forks; i++) {
         final int forkIndex = i;
         final String queueName = "q_" + (forkIndex % queues);
         final Thread producerRunner = new Thread(() -> {
            try (Connection connection = factorySend.createConnection()) {
               connection.start();
               try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                  final javax.jms.Queue queue = session.createQueue(queueName);
                  try (MessageProducer producer = session.createProducer(queue)) {
                     producer.setDeliveryMode(durable ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
                     for (int r = 0; r < runs; r++) {
                        onStartRun.await();
                        for (int m = 0; m < messages; m++) {
                           final BytesMessage bytesMessage = session.createBytesMessage();
                           bytesMessage.writeInt(forkIndex);
                           producer.send(bytesMessage);
                        }
                        onFinishRun.await();
                     }
                  } catch (InterruptedException | BrokenBarrierException e) {
                     e.printStackTrace();
                  }
               }
            } catch (JMSException e) {
               e.printStackTrace();
            }
         });

         producerRunner.setDaemon(true);

         final Thread consumerRunner = new Thread(() -> {
            try (Connection connection = factorySend.createConnection()) {
               connection.start();
               try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                  final javax.jms.Queue queue = session.createQueue(queueName);
                  try (MessageConsumer consumer = session.createConsumer(queue)) {
                     for (int r = 0; r < runs; r++) {
                        onStartRun.await();
                        while (messagesRecieved.get() != messagesSent) {
                           final BytesMessage receivedMessage = (BytesMessage) consumer.receive(1000);
                           if (receivedMessage != null) {
                              final int receivedConsumerIndex = receivedMessage.readInt();
                              receivedMessages.getAndIncrement(receivedConsumerIndex);
                              messagesRecieved.incrementAndGet();
                           }
                        }
                        onFinishRun.await();
                     }
                  } catch (InterruptedException e) {
                     e.printStackTrace();
                  } catch (BrokenBarrierException e) {
                     e.printStackTrace();
                  }
               }
            } catch (JMSException e) {
               e.printStackTrace();
            }
         });
         consumerRunner.setDaemon(true);
         consumersRunners[forkIndex] = consumerRunner;
         producersRunners[forkIndex] = producerRunner;
      }
      Stream.of(consumersRunners).forEach(Thread::start);
      Stream.of(producersRunners).forEach(Thread::start);
      final long messagesPerRun = (forks * messages);
      for (int r = 0; r < runs; r++) {
         onStartRun.await(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
         System.out.println("started run " + r);
         final long start = System.currentTimeMillis();
         onFinishRun.await(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
         final long elapsedMillis = System.currentTimeMillis() - start;
         System.out.println((messagesPerRun * 1000L) / elapsedMillis + " msg/sec");
      }
      Stream.of(producersRunners).forEach(runner -> {
         try {
            runner.join(TIMEOUT_MILLIS * runs);
         } catch (InterruptedException e) {
            e.printStackTrace();
         }
      });
      Stream.of(producersRunners).forEach(Thread::interrupt);
      Stream.of(consumersRunners).forEach(runner -> {
         try {
            runner.join(TIMEOUT_MILLIS * runs);
         } catch (InterruptedException e) {
            e.printStackTrace();
         }
      });
      Stream.of(consumersRunners).forEach(Thread::interrupt);
      for (int i = 0; i < forks; i++) {
         Assert.assertEquals("The consumer " + i + " must receive all the messages sent.", messages * runs, receivedMessages.get(i));
      }
   }
}
