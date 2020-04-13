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
package org.apache.activemq.artemis.tests.integration.crossprotocol;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.Topic;
import java.net.URI;
import java.util.Arrays;
import java.util.UUID;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.tests.integration.openwire.OpenWireTestBase;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpSender;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.activemq.artemis.tests.util.CFUtil.createConnectionFactory;

@RunWith(Parameterized.class)
public class RequestReplyNonJMSTest extends OpenWireTestBase {

   private static final SimpleString queueName = SimpleString.toSimpleString("RequestReplyQueueTest");
   private static final SimpleString topicName = SimpleString.toSimpleString("RequestReplyTopicTest");
   private static final SimpleString replyQueue = SimpleString.toSimpleString("ReplyOnRequestReplyQueueTest");

   private final String protocolConsumer;

   private ConnectionFactory consumerCF;

   public RequestReplyNonJMSTest(String protocolConsumer) {
      this.protocolConsumer = protocolConsumer;
   }

   @Parameterized.Parameters(name = "openWireOnSender={0}")
   public static Iterable<Object[]> data() {
      return Arrays.asList(new Object[][] {
         {"OPENWIRE"},
         {"CORE"},
         {"AMQP"}
      });
   }

   @Before
   public void setupCF() {
      consumerCF = createConnectionFactory(protocolConsumer, urlString);
   }

   @Before
   public void setupQueue() throws Exception {
      Wait.assertTrue(server::isStarted);
      Wait.assertTrue(server::isActive);
      this.server.createQueue(new QueueConfiguration(queueName).setRoutingType(RoutingType.ANYCAST));
      this.server.createQueue(new QueueConfiguration(replyQueue).setRoutingType(RoutingType.ANYCAST));
      AddressInfo info = new AddressInfo(topicName, RoutingType.MULTICAST);
      this.server.addAddressInfo(info);
   }

   @Test
   public void testReplyToFromAMQPClientWithInvalidTypeAnnotation() throws Throwable {
      AmqpClient directClient = new AmqpClient(new URI("tcp://localhost:61616"), null, null);
      AmqpConnection connection = null;
      AmqpSession session = null;
      AmqpSender sender = null;
      Connection consumerConn = null;

      try {
         connection = directClient.connect(true);
         session = connection.createSession();
         sender = session.createSender(queueName.toString());

         AmqpMessage message = new AmqpMessage();
         message = new AmqpMessage();
         message.setReplyToAddress(replyQueue.toString());
         message.setMessageAnnotation("x-opt-jms-reply-to", new Byte((byte)10)); // that's invalid on the conversion, lets hope it doesn't fail
         message.setMessageId("msg-1");
         sender.send(message);

         consumerConn = consumerCF.createConnection();
         Session consumerSess = consumerConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue queue = consumerSess.createQueue(queueName.toString());
         Queue replyQueue = consumerSess.createQueue(RequestReplyNonJMSTest.replyQueue.toString());

         MessageConsumer consumer = consumerSess.createConsumer(queue);
         consumerConn.start();
         javax.jms.Message receivedMessage = consumer.receive(5000);
         Assert.assertNotNull(receivedMessage);
         Assert.assertEquals(replyQueue, receivedMessage.getJMSReplyTo());
         Assert.assertTrue(receivedMessage.getJMSReplyTo() instanceof javax.jms.Queue);

         Assert.assertNull(consumer.receiveNoWait());
      } catch (Throwable e) {
         e.printStackTrace();
         throw e;
      } finally {
         try {
            connection.close();
         } catch (Throwable e) {
            e.printStackTrace();
         }
         try {
            consumerConn.close();
         } catch (Throwable dontcare) {
            dontcare.printStackTrace();
         }
      }
   }

   @Test
   public void testReplyToFromAMQPClientWithNoTypeOrOtherAnnotations() throws Throwable {
      AmqpClient directClient = new AmqpClient(new URI("tcp://localhost:61616"), null, null);
      AmqpConnection connection = null;
      AmqpSession session = null;
      AmqpSender sender = null;
      Connection consumerConn = null;

      try {
         connection = directClient.connect(true);
         session = connection.createSession();
         sender = session.createSender(queueName.toString());

         AmqpMessage message = new AmqpMessage();
         message.setReplyToAddress(replyQueue.toString());
         message.setMessageId("msg-1");
         message.setText("Test-Message");
         sender.send(message);

         consumerConn = consumerCF.createConnection();
         Session consumerSess = consumerConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue queue = consumerSess.createQueue(queueName.toString());
         Queue replyQueue = consumerSess.createQueue(RequestReplyNonJMSTest.replyQueue.toString());

         MessageConsumer consumer = consumerSess.createConsumer(queue);
         consumerConn.start();
         javax.jms.Message receivedMessage = consumer.receive(5000);
         Assert.assertNotNull(receivedMessage);
         Assert.assertEquals(replyQueue, receivedMessage.getJMSReplyTo());
         Assert.assertTrue(receivedMessage.getJMSReplyTo() instanceof javax.jms.Queue);

         Assert.assertNull(consumer.receiveNoWait());
      } catch (Throwable e) {
         e.printStackTrace();
         throw e;
      } finally {
         try {
            connection.close();
         } catch (Throwable e) {
            e.printStackTrace();
         }
         try {
            consumerConn.close();
         } catch (Throwable dontcare) {
            dontcare.printStackTrace();
         }
      }
   }

   @Test
   public void testReplyToFromAMQPClientWithNoTypeButWithOtherAnnotations() throws Throwable {
      AmqpClient directClient = new AmqpClient(new URI("tcp://localhost:61616"), null, null);
      AmqpConnection connection = null;
      AmqpSession session = null;
      AmqpSender sender = null;
      Connection consumerConn = null;

      try {
         connection = directClient.connect(true);
         session = connection.createSession();
         sender = session.createSender(queueName.toString());

         AmqpMessage message = new AmqpMessage();
         message.setReplyToAddress(replyQueue.toString());
         message.setMessageId("msg-1");
         message.setMessageAnnotation("x-opt-not-jms-reply-to", new Byte((byte)1));
         message.setText("Test-Message");
         sender.send(message);

         consumerConn = consumerCF.createConnection();
         Session consumerSess = consumerConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue queue = consumerSess.createQueue(queueName.toString());
         Queue replyQueue = consumerSess.createQueue(RequestReplyNonJMSTest.replyQueue.toString());

         MessageConsumer consumer = consumerSess.createConsumer(queue);
         consumerConn.start();
         javax.jms.Message receivedMessage = consumer.receive(5000);
         Assert.assertNotNull(receivedMessage);
         Assert.assertEquals(replyQueue, receivedMessage.getJMSReplyTo());
         Assert.assertTrue(receivedMessage.getJMSReplyTo() instanceof javax.jms.Queue);

         Assert.assertNull(consumer.receiveNoWait());
      } catch (Throwable e) {
         e.printStackTrace();
         throw e;
      } finally {
         try {
            connection.close();
         } catch (Throwable e) {
            e.printStackTrace();
         }
         try {
            consumerConn.close();
         } catch (Throwable dontcare) {
            dontcare.printStackTrace();
         }
      }
   }

   @Test
   public void testReplyToFromAMQPClientWithQueueReplyToAddress() throws Throwable {
      AmqpClient directClient = new AmqpClient(new URI("tcp://localhost:61616"), null, null);
      AmqpConnection connection = null;
      AmqpSession session = null;
      AmqpSender sender = null;
      Connection consumerConn = null;

      try {
         connection = directClient.connect(true);
         session = connection.createSession();
         sender = session.createSender(queueName.toString());

         AmqpMessage message = new AmqpMessage();
         message.setReplyToAddress(replyQueue.toString());
         message.setMessageId("msg-1");
         message.setMessageAnnotation("x-opt-jms-reply-to", new Byte((byte)0));
         message.setText("Test-Message");
         sender.send(message);

         consumerConn = consumerCF.createConnection();
         Session consumerSess = consumerConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue queue = consumerSess.createQueue(queueName.toString());
         Queue replyQueue = consumerSess.createQueue(RequestReplyNonJMSTest.replyQueue.toString());

         MessageConsumer consumer = consumerSess.createConsumer(queue);
         consumerConn.start();
         javax.jms.Message receivedMessage = consumer.receive(5000);
         Assert.assertNotNull(receivedMessage);
         Assert.assertEquals(replyQueue, receivedMessage.getJMSReplyTo());
         Assert.assertTrue(receivedMessage.getJMSReplyTo() instanceof javax.jms.Queue);

         Assert.assertNull(consumer.receiveNoWait());
      } catch (Throwable e) {
         e.printStackTrace();
         throw e;
      } finally {
         try {
            connection.close();
         } catch (Throwable e) {
            e.printStackTrace();
         }
         try {
            consumerConn.close();
         } catch (Throwable dontcare) {
            dontcare.printStackTrace();
         }
      }
   }

   @Test
   public void testReplyToFromAMQPClientWithTopicReplyToAddress() throws Throwable {
      AmqpClient directClient = new AmqpClient(new URI("tcp://localhost:61616"), null, null);
      AmqpConnection connection = null;
      AmqpSession session = null;
      AmqpSender sender = null;
      Connection consumerConn = null;

      try {
         connection = directClient.connect(true);
         session = connection.createSession();
         sender = session.createSender(queueName.toString());

         AmqpMessage message = new AmqpMessage();
         message.setReplyToAddress(topicName.toString());
         message.setMessageId("msg-1");
         message.setMessageAnnotation("x-opt-jms-reply-to", new Byte((byte)1));
         message.setText("Test-Message");
         sender.send(message);

         consumerConn = consumerCF.createConnection();
         Session consumerSess = consumerConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue queue = consumerSess.createQueue(queueName.toString());
         Topic replyTopic = consumerSess.createTopic(RequestReplyNonJMSTest.topicName.toString());

         MessageConsumer consumer = consumerSess.createConsumer(queue);
         consumerConn.start();
         javax.jms.Message receivedMessage = consumer.receive(5000);
         Assert.assertNotNull(receivedMessage);
         Assert.assertEquals(replyTopic, receivedMessage.getJMSReplyTo());
         Assert.assertTrue(receivedMessage.getJMSReplyTo() instanceof javax.jms.Topic);

         Assert.assertNull(consumer.receiveNoWait());
      } catch (Throwable e) {
         e.printStackTrace();
         throw e;
      } finally {
         try {
            connection.close();
         } catch (Throwable e) {
            e.printStackTrace();
         }
         try {
            consumerConn.close();
         } catch (Throwable dontcare) {
            dontcare.printStackTrace();
         }
      }
   }

   @Test
   public void testReplyToFromAMQPClientWithTempTopicReplyToAddress() throws Throwable {
      AmqpClient directClient = new AmqpClient(new URI("tcp://localhost:61616"), null, null);
      AmqpConnection connection = null;
      AmqpSession session = null;
      AmqpSender sender = null;
      Connection consumerConn = null;

      try {
         connection = directClient.connect(true);
         session = connection.createSession();
         sender = session.createSender(queueName.toString());

         String replyToName = UUID.randomUUID().toString();

         AmqpMessage message = new AmqpMessage();
         message.setReplyToAddress(replyToName);
         message.setMessageId("msg-1");
         message.setMessageAnnotation("x-opt-jms-reply-to", new Byte((byte)3));
         message.setText("Test-Message");
         sender.send(message);

         consumerConn = consumerCF.createConnection();
         Session consumerSess = consumerConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue queue = consumerSess.createQueue(queueName.toString());

         MessageConsumer consumer = consumerSess.createConsumer(queue);
         consumerConn.start();
         javax.jms.Message receivedMessage = consumer.receive(5000);
         Assert.assertNotNull(receivedMessage);
         Assert.assertTrue(receivedMessage.getJMSReplyTo() instanceof javax.jms.TemporaryTopic);
         Assert.assertEquals(replyToName, ((TemporaryTopic) receivedMessage.getJMSReplyTo()).getTopicName());

         Assert.assertNull(consumer.receiveNoWait());
      } catch (Throwable e) {
         e.printStackTrace();
         throw e;
      } finally {
         try {
            connection.close();
         } catch (Throwable e) {
            e.printStackTrace();
         }
         try {
            consumerConn.close();
         } catch (Throwable dontcare) {
            dontcare.printStackTrace();
         }
      }
   }

   @Test
   public void testReplyToFromAMQPClientWithTempQueueReplyToAddress() throws Throwable {
      AmqpClient directClient = new AmqpClient(new URI("tcp://localhost:61616"), null, null);
      AmqpConnection connection = null;
      AmqpSession session = null;
      AmqpSender sender = null;
      Connection consumerConn = null;

      try {
         connection = directClient.connect(true);
         session = connection.createSession();
         sender = session.createSender(queueName.toString());

         String replyToName = UUID.randomUUID().toString();

         AmqpMessage message = new AmqpMessage();
         message.setReplyToAddress(replyToName);
         message.setMessageId("msg-1");
         message.setMessageAnnotation("x-opt-jms-reply-to", new Byte((byte)2));
         message.setText("Test-Message");
         sender.send(message);

         consumerConn = consumerCF.createConnection();
         Session consumerSess = consumerConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue queue = consumerSess.createQueue(queueName.toString());

         MessageConsumer consumer = consumerSess.createConsumer(queue);
         consumerConn.start();
         javax.jms.Message receivedMessage = consumer.receive(5000);
         Assert.assertNotNull(receivedMessage);
         Assert.assertTrue(receivedMessage.getJMSReplyTo() instanceof javax.jms.TemporaryQueue);
         Assert.assertEquals(replyToName, ((TemporaryQueue) receivedMessage.getJMSReplyTo()).getQueueName());

         Assert.assertNull(consumer.receiveNoWait());
      } catch (Throwable e) {
         e.printStackTrace();
         throw e;
      } finally {
         try {
            connection.close();
         } catch (Throwable e) {
            e.printStackTrace();
         }
         try {
            consumerConn.close();
         } catch (Throwable dontcare) {
            dontcare.printStackTrace();
         }
      }
   }
}
