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
package org.apache.activemq.artemis.tests.integration.proton;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TemporaryQueue;
import javax.jms.TextMessage;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Random;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.ByteUtil;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class ProtonTest extends ActiveMQTestBase {

   // this will ensure that all tests in this class are run twice,
   // once with "true" passed to the class' constructor and once with "false"
   @Parameterized.Parameters(name = "{0}")
   public static Collection getParameters() {

      // these 3 are for comparison
      return Arrays.asList(new Object[][]{{"AMQP", 0}, {"ActiveMQ (InVM)", 1}, {"ActiveMQ (Netty)", 2}, {"AMQP_ANONYMOUS", 3}});
   }

   ConnectionFactory factory;

   private final int protocol;

   public ProtonTest(String name, int protocol) {
      this.coreAddress = "jms.queue.exampleQueue";
      this.protocol = protocol;
      if (protocol == 0 || protocol == 3) {
         this.address = coreAddress;
      }
      else {
         this.address = "exampleQueue";
      }
   }

   private ActiveMQServer server;
   private final String coreAddress;
   private final String address;
   private Connection connection;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      disableCheckThread();
      server = this.createServer(true, true);
      HashMap<String, Object> params = new HashMap<>();
      params.put(TransportConstants.PORT_PROP_NAME, "5672");
      params.put(TransportConstants.PROTOCOLS_PROP_NAME, "AMQP");
      TransportConfiguration transportConfiguration = new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, params);

      server.getConfiguration().getAcceptorConfigurations().add(transportConfiguration);
      server.start();
      server.createQueue(new SimpleString(coreAddress), new SimpleString(coreAddress), null, true, false);
      server.createQueue(new SimpleString(coreAddress + "1"), new SimpleString(coreAddress + "1"), null, true, false);
      server.createQueue(new SimpleString(coreAddress + "2"), new SimpleString(coreAddress + "2"), null, true, false);
      server.createQueue(new SimpleString(coreAddress + "3"), new SimpleString(coreAddress + "3"), null, true, false);
      server.createQueue(new SimpleString(coreAddress + "4"), new SimpleString(coreAddress + "4"), null, true, false);
      server.createQueue(new SimpleString(coreAddress + "5"), new SimpleString(coreAddress + "5"), null, true, false);
      server.createQueue(new SimpleString(coreAddress + "6"), new SimpleString(coreAddress + "6"), null, true, false);
      server.createQueue(new SimpleString(coreAddress + "7"), new SimpleString(coreAddress + "7"), null, true, false);
      server.createQueue(new SimpleString(coreAddress + "8"), new SimpleString(coreAddress + "8"), null, true, false);
      server.createQueue(new SimpleString(coreAddress + "9"), new SimpleString(coreAddress + "9"), null, true, false);
      server.createQueue(new SimpleString(coreAddress + "10"), new SimpleString(coreAddress + "10"), null, true, false);
      server.createQueue(new SimpleString("amqp_testtopic"), new SimpleString("amqp_testtopic"), null, true, false);
      server.createQueue(new SimpleString("amqp_testtopic" + "1"), new SimpleString("amqp_testtopic" + "1"), null, true, false);
      server.createQueue(new SimpleString("amqp_testtopic" + "2"), new SimpleString("amqp_testtopic" + "2"), null, true, false);
      server.createQueue(new SimpleString("amqp_testtopic" + "3"), new SimpleString("amqp_testtopic" + "3"), null, true, false);
      server.createQueue(new SimpleString("amqp_testtopic" + "4"), new SimpleString("amqp_testtopic" + "4"), null, true, false);
      server.createQueue(new SimpleString("amqp_testtopic" + "5"), new SimpleString("amqp_testtopic" + "5"), null, true, false);
      server.createQueue(new SimpleString("amqp_testtopic" + "6"), new SimpleString("amqp_testtopic" + "6"), null, true, false);
      server.createQueue(new SimpleString("amqp_testtopic" + "7"), new SimpleString("amqp_testtopic" + "7"), null, true, false);
      server.createQueue(new SimpleString("amqp_testtopic" + "8"), new SimpleString("amqp_testtopic" + "8"), null, true, false);
      server.createQueue(new SimpleString("amqp_testtopic" + "9"), new SimpleString("amqp_testtopic" + "9"), null, true, false);
      server.createQueue(new SimpleString("amqp_testtopic" + "10"), new SimpleString("amqp_testtopic" + "10"), null, true, false);
      connection = createConnection();

   }

   @Override
   @After
   public void tearDown() throws Exception {
      try {
         Thread.sleep(250);
         if (connection != null) {
            connection.close();
         }

         server.stop();
      }
      finally {
         super.tearDown();
      }
   }

   @Test
   public void testTemporaryQueue() throws Throwable {

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      TemporaryQueue queue = session.createTemporaryQueue();
      System.out.println("queue:" + queue.getQueueName());
      MessageProducer p = session.createProducer(queue);

      TextMessage message = session.createTextMessage();
      message.setText("Message temporary");
      p.send(message);

      MessageConsumer cons = session.createConsumer(queue);
      connection.start();

      message = (TextMessage) cons.receive(5000);
      Assert.assertNotNull(message);

   }


   @Test
   public void testReplyTo() throws Throwable {

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      TemporaryQueue queue = session.createTemporaryQueue();
      System.out.println("queue:" + queue.getQueueName());
      MessageProducer p = session.createProducer(queue);

      TextMessage message = session.createTextMessage();
      message.setText("Message temporary");
      message.setJMSReplyTo(createQueue(address));
      p.send(message);

      MessageConsumer cons = session.createConsumer(queue);
      connection.start();

      message = (TextMessage) cons.receive(5000);
      Destination jmsReplyTo = message.getJMSReplyTo();
      Assert.assertNotNull(jmsReplyTo);
      Assert.assertNotNull(message);

   }

   @Test
      public void testReplyToNonJMS() throws Throwable {

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      TemporaryQueue queue = session.createTemporaryQueue();
      System.out.println("queue:" + queue.getQueueName());
      MessageProducer p = session.createProducer(queue);

      TextMessage message = session.createTextMessage();
      message.setText("Message temporary");
      message.setJMSReplyTo(createQueue("someaddress"));
      p.send(message);

      MessageConsumer cons = session.createConsumer(queue);
      connection.start();

      message = (TextMessage) cons.receive(5000);
      Destination jmsReplyTo = message.getJMSReplyTo();
      Assert.assertNotNull(jmsReplyTo);
      Assert.assertNotNull(message);

   }

   /*
   // Uncomment testLoopBrowser to validate the hunging on the test
   @Test
   public void testLoopBrowser() throws Throwable
   {
      for (int i = 0 ; i < 1000; i++)
      {
         System.out.println("#test " + i);
         testBrowser();
         tearDown();
         setUp();
      }
   } */

   /**
    * This test eventually fails because of: https://issues.apache.org/jira/browse/QPID-4901
    *
    * @throws Throwable
    */
   // @Test TODO: re-enable this when we can get a version free of QPID-4901 bug
   public void testBrowser() throws Throwable {

      boolean success = false;

      for (int i = 0; i < 10; i++) {
         // As this test was hunging, we added a protection here to fail it instead.
         // it seems something on the qpid client, so this failure belongs to them and we can ignore it on
         // our side (ActiveMQ)
         success = runWithTimeout(new RunnerWithEX() {
            @Override
            public void run() throws Throwable {
               int numMessages = 50;
               javax.jms.Queue queue = createQueue(address);
               Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
               MessageProducer p = session.createProducer(queue);
               for (int i = 0; i < numMessages; i++) {
                  TextMessage message = session.createTextMessage();
                  message.setText("msg:" + i);
                  p.send(message);
               }

               connection.close();
               Queue q = (Queue) server.getPostOffice().getBinding(new SimpleString(coreAddress)).getBindable();

               connection = createConnection();
               session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

               QueueBrowser browser = session.createBrowser(queue);
               Enumeration enumeration = browser.getEnumeration();
               int count = 0;
               while (enumeration.hasMoreElements()) {
                  Message msg = (Message) enumeration.nextElement();
                  Assert.assertNotNull("" + count, msg);
                  Assert.assertTrue("" + msg, msg instanceof TextMessage);
                  String text = ((TextMessage) msg).getText();
                  Assert.assertEquals(text, "msg:" + count++);
               }
               Assert.assertEquals(count, numMessages);
               connection.close();
               Assert.assertEquals(getMessageCount(q), numMessages);
            }
         }, 1000);

         if (success) {
            break;
         }
         else {
            System.err.println("Had to make it fail!!!");
            tearDown();
            setUp();
         }
      }

      // There is a bug on the qpid client library currently, we can expect having to interrupt the thread on browsers.
      // but we can't have it on 10 iterations... something must be broken if that's the case
      Assert.assertTrue("Test had to interrupt on all occasions.. this is beyond the expected for the test", success);
   }

   @Test
   public void testConnection() throws Exception {
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      MessageConsumer cons = session.createConsumer(createQueue(address));

      org.apache.activemq.artemis.core.server.Queue serverQueue = server.locateQueue(SimpleString.toSimpleString(coreAddress));

      assertEquals(1, serverQueue.getConsumerCount());

      cons.close();

      for (int i = 0; i < 100 && serverQueue.getConsumerCount() != 0; i++) {
         Thread.sleep(500);
      }

      assertEquals(0, serverQueue.getConsumerCount());

      session.close();

   }

   @Test
   public void testMessagesSentTransactional() throws Exception {
      int numMessages = 1000;
      javax.jms.Queue queue = createQueue(address);
      Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
      MessageProducer p = session.createProducer(queue);
      byte[] bytes = new byte[2048];
      new Random().nextBytes(bytes);
      for (int i = 0; i < numMessages; i++) {
         TextMessage message = session.createTextMessage();
         message.setText("msg:" + i);
         p.send(message);
      }
      session.commit();
      connection.close();
      Queue q = (Queue) server.getPostOffice().getBinding(new SimpleString(coreAddress)).getBindable();
      for (long timeout = System.currentTimeMillis() + 5000; timeout > System.currentTimeMillis() && getMessageCount(q) != numMessages; ) {
         Thread.sleep(1);
      }
      Assert.assertEquals(numMessages, getMessageCount(q));
   }

   @Test
   public void testMessagesSentTransactionalRolledBack() throws Exception {
      int numMessages = 1;
      javax.jms.Queue queue = createQueue(address);
      Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
      MessageProducer p = session.createProducer(queue);
      byte[] bytes = new byte[2048];
      new Random().nextBytes(bytes);
      for (int i = 0; i < numMessages; i++) {
         TextMessage message = session.createTextMessage();
         message.setText("msg:" + i);
         p.send(message);
      }
      session.close();
      connection.close();
      Queue q = (Queue) server.getPostOffice().getBinding(new SimpleString(coreAddress)).getBindable();
      Assert.assertEquals(getMessageCount(q), 0);
   }

   @Test
   public void testCancelMessages() throws Exception {
      int numMessages = 10;
      long time = System.currentTimeMillis();
      javax.jms.Queue queue = createQueue(address);
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer p = session.createProducer(queue);
      byte[] bytes = new byte[2048];
      new Random().nextBytes(bytes);
      for (int i = 0; i < numMessages; i++) {
         TextMessage message = session.createTextMessage();
         message.setText("msg:" + i);
         p.send(message);
      }
      connection.close();
      Queue q = (Queue) server.getPostOffice().getBinding(new SimpleString(coreAddress)).getBindable();

      for (long timeout = System.currentTimeMillis() + 5000; timeout > System.currentTimeMillis() && getMessageCount(q) != numMessages; ) {
         Thread.sleep(1);
      }

      Assert.assertEquals(numMessages, getMessageCount(q));
      //now create a new connection and receive
      connection = createConnection();
      session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageConsumer consumer = session.createConsumer(queue);
      Thread.sleep(100);
      consumer.close();
      connection.close();
      Assert.assertEquals(numMessages, getMessageCount(q));
      long taken = (System.currentTimeMillis() - time) / 1000;
      System.out.println("taken = " + taken);
   }

   @Test
   public void testClientAckMessages() throws Exception {
      int numMessages = 10;
      long time = System.currentTimeMillis();
      javax.jms.Queue queue = createQueue(address);
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer p = session.createProducer(queue);
      byte[] bytes = new byte[2048];
      new Random().nextBytes(bytes);
      for (int i = 0; i < numMessages; i++) {
         TextMessage message = session.createTextMessage();
         message.setText("msg:" + i);
         p.send(message);
      }
      connection.close();
      Queue q = (Queue) server.getPostOffice().getBinding(new SimpleString(coreAddress)).getBindable();

      for (long timeout = System.currentTimeMillis() + 5000; timeout > System.currentTimeMillis() && getMessageCount(q) != numMessages; ) {
         Thread.sleep(1);
      }
      Assert.assertEquals(numMessages, getMessageCount(q));
      //now create a new connection and receive
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
         //System.out.println("text = " + text);
         Assert.assertEquals(text, "msg:" + i);
         msg.acknowledge();
      }

      consumer.close();
      connection.close();
      Assert.assertEquals(0, getMessageCount(q));
      long taken = (System.currentTimeMillis() - time) / 1000;
      System.out.println("taken = " + taken);

   }

   @Test
   public void testMessagesReceivedInParallel() throws Throwable {
      final int numMessages = 50000;
      long time = System.currentTimeMillis();
      final javax.jms.Queue queue = createQueue(address);

      final ArrayList<Throwable> exceptions = new ArrayList<>();

      Thread t = new Thread(new Runnable() {
         @Override
         public void run() {
            Connection connectionConsumer = null;
            try {
               // TODO the test may starve if using the same connection (dead lock maybe?)
               connectionConsumer = createConnection();
               //               connectionConsumer = connection;
               connectionConsumer.start();
               Session sessionConsumer = connectionConsumer.createSession(false, Session.AUTO_ACKNOWLEDGE);
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
                  }
                  catch (JMSException e) {
                     e.printStackTrace();
                     break;
                  }
               }
            }
            catch (Throwable e) {
               exceptions.add(e);
               e.printStackTrace();
            }
            finally {
               try {
                  // if the createconnecion wasn't commented out
                  if (connectionConsumer != connection) {
                     connectionConsumer.close();
                  }
               }
               catch (Throwable ignored) {
                  // NO OP
               }
            }
         }
      });

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      t.start();

      MessageProducer p = session.createProducer(queue);
      p.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      for (int i = 0; i < numMessages; i++) {
         BytesMessage message = session.createBytesMessage();
         message.writeUTF("Hello world!!!!" + i);
         message.setIntProperty("count", i);
         p.send(message);
      }
      t.join();

      for (Throwable e : exceptions) {
         throw e;
      }
      Queue q = (Queue) server.getPostOffice().getBinding(new SimpleString(coreAddress)).getBindable();

      connection.close();
      Assert.assertEquals(0, getMessageCount(q));

      long taken = (System.currentTimeMillis() - time);
      System.out.println("Microbenchamrk ran in " + taken + " milliseconds, sending/receiving " + numMessages);

      double messagesPerSecond = ((double) numMessages / (double) taken) * 1000;

      System.out.println(((int) messagesPerSecond) + " messages per second");

   }

   @Test
   public void testSimpleBinary() throws Throwable {
      final int numMessages = 500;
      long time = System.currentTimeMillis();
      final javax.jms.Queue queue = createQueue(address);

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      byte[] bytes = new byte[0xf + 1];
      for (int i = 0; i <= 0xf; i++) {
         bytes[i] = (byte) i;
      }

      MessageProducer p = session.createProducer(queue);
      for (int i = 0; i < numMessages; i++) {
         System.out.println("Sending " + i);
         BytesMessage message = session.createBytesMessage();

         message.writeBytes(bytes);
         message.setIntProperty("count", i);
         p.send(message);
      }

      Session sessionConsumer = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      final MessageConsumer consumer = sessionConsumer.createConsumer(queue);

      for (int i = 0; i < numMessages; i++) {
         BytesMessage m = (BytesMessage) consumer.receive(5000);
         Assert.assertNotNull("Could not receive message count=" + i + " on consumer", m);

         m.reset();

         long size = m.getBodyLength();
         byte[] bytesReceived = new byte[(int) size];
         m.readBytes(bytesReceived);

         System.out.println("Received " + ByteUtil.bytesToHex(bytesReceived, 1) + " count - " + m.getIntProperty("count"));

         Assert.assertArrayEquals(bytes, bytesReceived);
      }

      //      assertEquals(0, q.getMessageCount());
      long taken = (System.currentTimeMillis() - time) / 1000;
      System.out.println("taken = " + taken);
   }

   @Test
   public void testSimpleDefault() throws Throwable {
      final int numMessages = 500;
      long time = System.currentTimeMillis();
      final javax.jms.Queue queue = createQueue(address);

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      byte[] bytes = new byte[0xf + 1];
      for (int i = 0; i <= 0xf; i++) {
         bytes[i] = (byte) i;
      }

      MessageProducer p = session.createProducer(queue);
      for (int i = 0; i < numMessages; i++) {
         System.out.println("Sending " + i);
         Message message = session.createMessage();

         message.setIntProperty("count", i);
         p.send(message);
      }

      Session sessionConsumer = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      final MessageConsumer consumer = sessionConsumer.createConsumer(queue);

      for (int i = 0; i < numMessages; i++) {
         Message m = consumer.receive(5000);
         Assert.assertNotNull("Could not receive message count=" + i + " on consumer", m);
      }

      //      assertEquals(0, q.getMessageCount());
      long taken = (System.currentTimeMillis() - time) / 1000;
      System.out.println("taken = " + taken);
   }

   @Test
   public void testSimpleMap() throws Throwable {
      final int numMessages = 100;
      long time = System.currentTimeMillis();
      final javax.jms.Queue queue = createQueue(address);

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      MessageProducer p = session.createProducer(queue);
      for (int i = 0; i < numMessages; i++) {
         System.out.println("Sending " + i);
         MapMessage message = session.createMapMessage();

         message.setInt("i", i);
         message.setIntProperty("count", i);
         p.send(message);
      }

      Session sessionConsumer = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      final MessageConsumer consumer = sessionConsumer.createConsumer(queue);

      for (int i = 0; i < numMessages; i++) {
         MapMessage m = (MapMessage) consumer.receive(5000);
         Assert.assertNotNull("Could not receive message count=" + i + " on consumer", m);

         Assert.assertEquals(i, m.getInt("i"));
         Assert.assertEquals(i, m.getIntProperty("count"));
      }

      //      assertEquals(0, q.getMessageCount());
      long taken = (System.currentTimeMillis() - time) / 1000;
      System.out.println("taken = " + taken);
   }

   @Test
   public void testSimpleStream() throws Throwable {
      final int numMessages = 100;
      final javax.jms.Queue queue = createQueue(address);

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      MessageProducer p = session.createProducer(queue);
      for (int i = 0; i < numMessages; i++) {
         StreamMessage message = session.createStreamMessage();
         message.writeInt(i);
         message.writeBoolean(true);
         message.writeString("test");
         p.send(message);
      }

      Session sessionConsumer = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      final MessageConsumer consumer = sessionConsumer.createConsumer(queue);

      for (int i = 0; i < numMessages; i++) {
         StreamMessage m = (StreamMessage) consumer.receive(5000);
         Assert.assertNotNull("Could not receive message count=" + i + " on consumer", m);

         Assert.assertEquals(i, m.readInt());
         Assert.assertEquals(true, m.readBoolean());
         Assert.assertEquals("test", m.readString());
      }

   }

   @Test
   public void testSimpleText() throws Throwable {
      final int numMessages = 100;
      long time = System.currentTimeMillis();
      final javax.jms.Queue queue = createQueue(address);

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      MessageProducer p = session.createProducer(queue);
      for (int i = 0; i < numMessages; i++) {
         System.out.println("Sending " + i);
         TextMessage message = session.createTextMessage("text" + i);
         message.setStringProperty("text", "text" + i);
         p.send(message);
      }

      Session sessionConsumer = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      final MessageConsumer consumer = sessionConsumer.createConsumer(queue);

      for (int i = 0; i < numMessages; i++) {
         TextMessage m = (TextMessage) consumer.receive(5000);
         Assert.assertNotNull("Could not receive message count=" + i + " on consumer", m);
         Assert.assertEquals("text" + i, m.getText());
      }

      //      assertEquals(0, q.getMessageCount());
      long taken = (System.currentTimeMillis() - time) / 1000;
      System.out.println("taken = " + taken);
   }

   @Test
   public void testSimpleObject() throws Throwable {
      final int numMessages = 1;
      long time = System.currentTimeMillis();
      final javax.jms.Queue queue = createQueue(address);

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      MessageProducer p = session.createProducer(queue);
      for (int i = 0; i < numMessages; i++) {
         System.out.println("Sending " + i);
         ObjectMessage message = session.createObjectMessage(new AnythingSerializable(i));
         p.send(message);
      }

      Session sessionConsumer = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      final MessageConsumer consumer = sessionConsumer.createConsumer(queue);

      for (int i = 0; i < numMessages; i++) {
         ObjectMessage msg = (ObjectMessage) consumer.receive(5000);
         Assert.assertNotNull("Could not receive message count=" + i + " on consumer", msg);

         AnythingSerializable someSerialThing = (AnythingSerializable) msg.getObject();
         Assert.assertEquals(i, someSerialThing.getCount());
      }

      //      assertEquals(0, q.getMessageCount());
      long taken = (System.currentTimeMillis() - time) / 1000;
      System.out.println("taken = " + taken);
   }

   @Test
   public void testSelector() throws Exception {
      javax.jms.Queue queue = createQueue(address);
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer p = session.createProducer(queue);
      TextMessage message = session.createTextMessage();
      message.setText("msg:0");
      p.send(message);
      message = session.createTextMessage();
      message.setText("msg:1");
      message.setStringProperty("color", "RED");
      p.send(message);
      connection.start();
      MessageConsumer messageConsumer = session.createConsumer(queue, "color = 'RED'");
      TextMessage m = (TextMessage) messageConsumer.receive(5000);
      Assert.assertNotNull(m);
      Assert.assertEquals("msg:1", m.getText());
      Assert.assertEquals(m.getStringProperty("color"), "RED");
      connection.close();
   }

   @Test
   public void testProperties() throws Exception {
      javax.jms.Queue queue = createQueue(address);
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer p = session.createProducer(queue);
      TextMessage message = session.createTextMessage();
      message.setText("msg:0");
      message.setBooleanProperty("true", true);
      message.setBooleanProperty("false", false);
      message.setStringProperty("foo", "bar");
      message.setDoubleProperty("double", 66.6);
      message.setFloatProperty("float", 56.789f);
      message.setIntProperty("int", 8);
      message.setByteProperty("byte", (byte) 10);
      p.send(message);
      p.send(message);
      connection.start();
      MessageConsumer messageConsumer = session.createConsumer(queue);
      TextMessage m = (TextMessage) messageConsumer.receive(5000);
      Assert.assertNotNull(m);
      Assert.assertEquals("msg:0", m.getText());
      Assert.assertEquals(m.getBooleanProperty("true"), true);
      Assert.assertEquals(m.getBooleanProperty("false"), false);
      Assert.assertEquals(m.getStringProperty("foo"), "bar");
      Assert.assertEquals(m.getDoubleProperty("double"), 66.6, 0.0001);
      Assert.assertEquals(m.getFloatProperty("float"), 56.789f, 0.0001);
      Assert.assertEquals(m.getIntProperty("int"), 8);
      Assert.assertEquals(m.getByteProperty("byte"), (byte) 10);
      m = (TextMessage) messageConsumer.receive(5000);
      Assert.assertNotNull(m);
      connection.close();
   }

   private javax.jms.Queue createQueue(String address) throws Exception {
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      try {
         return session.createQueue(address);
      }
      finally {
         session.close();
      }
   }

   private javax.jms.Connection createConnection() throws JMSException {
      Connection connection;
      if (protocol == 3) {
         factory = new JmsConnectionFactory("amqp://localhost:5672");
         connection = factory.createConnection();
         connection.setExceptionListener(new ExceptionListener() {
            @Override
            public void onException(JMSException exception) {
               exception.printStackTrace();
            }
         });
         connection.start();
      }
      else if (protocol == 0) {
         factory = new JmsConnectionFactory("guest", "guest", "amqp://localhost:5672");
         connection = factory.createConnection();
         connection.setExceptionListener(new ExceptionListener() {
            @Override
            public void onException(JMSException exception) {
               exception.printStackTrace();
            }
         });
         connection.start();
      }
      else {
         TransportConfiguration transport;

         if (protocol == 1) {
            transport = new TransportConfiguration(INVM_CONNECTOR_FACTORY);
            factory = new ActiveMQConnectionFactory("vm:/0");
         }
         else {
            factory = new ActiveMQConnectionFactory();
         }

         connection = factory.createConnection("guest", "guest");
         connection.setExceptionListener(new ExceptionListener() {
            @Override
            public void onException(JMSException exception) {
               exception.printStackTrace();
            }
         });
         connection.start();
      }

      return connection;
   }

   public static class AnythingSerializable implements Serializable {

      private int count;

      public AnythingSerializable(int count) {
         this.count = count;
      }

      public int getCount() {
         return count;
      }
   }
}
