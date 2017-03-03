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
import javax.jms.DeliveryMode;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
import org.apache.activemq.artemis.utils.ConcurrentHashSet;
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

      ClientSession session = sf.createSession(false, true, true, true);

      server.createQueue(QUEUE, RoutingType.ANYCAST, QUEUE, null, true, false);

      session.close();

      sf.close();

      locator.close();
   }

   @Test
   public void testStressConnection() throws Exception {

      for (int i = 0; i < 10; i++) {
         ServerLocator locatorSendx = createFactory(isNetty()).setReconnectAttempts(-1);
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

      session.close();
   }

   @Test
   public void testSendReceiveAMQP() throws Throwable {

      if (!isNetty()) {
         // no need to run the test, there's no AMQP support
         return;
      }

      internalSend(true);
   }

   @Test
   public void testSendReceiveCore() throws Throwable {

      if (!isNetty()) {
         // no need to run the test, there's no AMQP support
         return;
      }

      internalSend(false);
   }

   public void internalSend(boolean amqp) throws Throwable {

      ConnectionFactory factory;

      if (amqp) {
         factory = new JmsConnectionFactory("amqp://localhost:61616");
      } else {
         factory = new ActiveMQConnectionFactory();
      }


      Connection connection = factory.createConnection();

      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         javax.jms.Queue queue = session.createQueue(QUEUE.toString());
         MessageProducer producer = session.createProducer(queue);
         producer.setDeliveryMode(DeliveryMode.PERSISTENT);

         long time = System.currentTimeMillis();
         int NUMBER_OF_MESSAGES = 100;
         for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
            producer.send(session.createTextMessage("hello " + i));
         }
         long end = System.currentTimeMillis();

         System.out.println("Time = " + (end - time));

         connection.close();

         if (this.durable) {
            server.stop();
            server.start();
         }

         connection = factory.createConnection();
         session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         connection.start();

         MessageConsumer consumer = session.createConsumer(queue);

         for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
            TextMessage message = (TextMessage) consumer.receive(1000);
            Assert.assertNotNull(message);
            Assert.assertEquals("hello " + i, message.getText());
         }
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
                     ServerLocator locatorSendx = createFactory(isNetty()).setReconnectAttempts(-1);
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

}
