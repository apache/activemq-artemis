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
package org.apache.activemq.artemis.tests.integration.amqp;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSender;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.message.impl.MessageImpl;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AmqpLargeMessageTest extends AmqpClientTestSupport {

   protected static final Logger LOG = LoggerFactory.getLogger(AmqpLargeMessageTest.class);

   private final Random rand = new Random(System.currentTimeMillis());

   private static final int FRAME_SIZE = 32767;
   private static final int PAYLOAD = 110 * 1024;

   String testQueueName = "ConnectionFrameSize";

   @Override
   protected void addConfiguration(ActiveMQServer server) {
      // Make the journal file size larger than the frame+message sizes used in the tests,
      // since it is by default for external brokers and it changes the behaviour.
      server.getConfiguration().setJournalFileSize(5 * 1024 * 1024);
   }

   @Override
   protected void configureAMQPAcceptorParameters(Map<String, Object> params) {
      params.put("maxFrameSize", FRAME_SIZE);
   }

   @Override
   protected void createAddressAndQueues(ActiveMQServer server) throws Exception {
   }

   @Override
   protected void addAdditionalAcceptors(ActiveMQServer server) throws Exception {
      server.getConfiguration().addAcceptorConfiguration("tcp", "tcp://localhost:61616");
   }

   @Test(timeout = 60000)
   public void testSendAMQPReceiveCore() throws Exception {
      server.getAddressSettingsRepository().addMatch("#", new AddressSettings().setDefaultAddressRoutingType(RoutingType.ANYCAST));

      int nMsgs = 200;

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      try {
         sendMessages(nMsgs, connection);

         int count = getMessageCount(server.getPostOffice(), testQueueName);
         assertEquals(nMsgs, count);

         ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory();
         receiveJMS(nMsgs, factory);
      } finally {
         connection.close();
      }
   }

   @Test(timeout = 60000)
   public void testSendAMQPReceiveOpenWire() throws Exception {
      server.getAddressSettingsRepository().addMatch("#", new AddressSettings().setDefaultAddressRoutingType(RoutingType.ANYCAST));

      int nMsgs = 200;

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      try {
         sendMessages(nMsgs, connection);

         int count = getMessageCount(server.getPostOffice(), testQueueName);
         assertEquals(nMsgs, count);

         ConnectionFactory factory = new org.apache.activemq.ActiveMQConnectionFactory("tcp://localhost:61616");
         receiveJMS(nMsgs, factory);
      } finally {
         connection.close();
      }
   }

   private void sendMessages(int nMsgs, AmqpConnection connection) throws Exception {
      connection.connect();

      AmqpSession session = connection.createSession();
      AmqpSender sender = session.createSender(testQueueName);

      for (int i = 0; i < nMsgs; ++i) {
         AmqpMessage message = createAmqpMessage((byte) 'A', PAYLOAD);
         message.setApplicationProperty("i", (Integer) i);
         message.setDurable(true);
         sender.send(message);
      }

      session.close();
   }

   private void receiveJMS(int nMsgs, ConnectionFactory factory) throws JMSException {
      Connection connection2 = factory.createConnection();
      Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
      connection2.start();
      MessageConsumer consumer = session2.createConsumer(session2.createQueue(testQueueName));

      for (int i = 0; i < nMsgs; i++) {
         Message message = consumer.receive(5000);
         Assert.assertNotNull(message);
         Assert.assertEquals(i, message.getIntProperty("i"));
      }

      connection2.close();
   }

   @Test(timeout = 60000)
   public void testSendAMQPReceiveAMQP() throws Exception {
      server.getAddressSettingsRepository().addMatch("#", new AddressSettings().setDefaultAddressRoutingType(RoutingType.ANYCAST));

      String testQueueName = "ConnectionFrameSize";
      int nMsgs = 200;

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      try {
         sendMessages(nMsgs, connection);

         int count = getMessageCount(server.getPostOffice(), testQueueName);
         assertEquals(nMsgs, count);

         AmqpSession session = connection.createSession();
         AmqpReceiver receiver = session.createReceiver(testQueueName);
         receiver.flow(nMsgs);

         for (int i = 0; i < nMsgs; ++i) {
            AmqpMessage message = receiver.receive(5, TimeUnit.SECONDS);
            assertNotNull("failed at " + i, message);
            MessageImpl wrapped = (MessageImpl) message.getWrappedMessage();
            if (wrapped.getBody() instanceof Data) {
               // converters can change this to AmqValue
               Data data = (Data) wrapped.getBody();
               System.out.println("received : message: " + data.getValue().getLength());
               assertEquals(PAYLOAD, data.getValue().getLength());
            }
            message.accept();
         }
         session.close();

      } finally {
         connection.close();
      }
   }

   @Test(timeout = 60000)
   public void testSendAMQPReceiveAMQPViaJMSObjectMessage() throws Exception {
      server.getAddressSettingsRepository().addMatch("#", new AddressSettings().setDefaultAddressRoutingType(RoutingType.ANYCAST));

      String testQueueName = "ConnectionFrameSize";
      int nMsgs = 1;

      ConnectionFactory factory = new JmsConnectionFactory("amqp://localhost:61616");

      sendObjectMessages(nMsgs, new JmsConnectionFactory("amqp://localhost:61616"));

      int count = getMessageCount(server.getPostOffice(), testQueueName);
      assertEquals(nMsgs, count);

      receiveJMS(nMsgs, factory);
   }

   @Test(timeout = 60000)
   public void testSendAMQPReceiveAMQPViaJMSText() throws Exception {
      server.getAddressSettingsRepository().addMatch("#", new AddressSettings().setDefaultAddressRoutingType(RoutingType.ANYCAST));

      String testQueueName = "ConnectionFrameSize";
      int nMsgs = 1;

      ConnectionFactory factory = new JmsConnectionFactory("amqp://localhost:61616");

      sendTextMessages(nMsgs, new JmsConnectionFactory("amqp://localhost:61616"));

      int count = getMessageCount(server.getPostOffice(), testQueueName);
      assertEquals(nMsgs, count);

      receiveJMS(nMsgs, factory);
   }

   @Test(timeout = 60000)
   public void testSendAMQPReceiveAMQPViaJMSBytes() throws Exception {
      server.getAddressSettingsRepository().addMatch("#", new AddressSettings().setDefaultAddressRoutingType(RoutingType.ANYCAST));

      String testQueueName = "ConnectionFrameSize";
      int nMsgs = 1;

      ConnectionFactory factory = new JmsConnectionFactory("amqp://localhost:61616");

      sendBytesMessages(nMsgs, new JmsConnectionFactory("amqp://localhost:61616"));

      int count = getMessageCount(server.getPostOffice(), testQueueName);
      assertEquals(nMsgs, count);

      receiveJMS(nMsgs, factory);
   }

   private byte[] createLargePayload(int sizeInBytes) {
      byte[] payload = new byte[sizeInBytes];
      for (int i = 0; i < sizeInBytes; i++) {
         payload[i] = (byte) rand.nextInt(256);
      }

      LOG.debug("Created buffer with size : " + sizeInBytes + " bytes");
      return payload;
   }

   @Test(timeout = 60000)
   public void testSendSmallerMessages() throws Exception {
      for (int i = 512; i <= (8 * 1024); i += 512) {
         doTestSendLargeMessage(i);
      }
   }

   @Test(timeout = 120000)
   public void testSendFixedSizedMessages() throws Exception {
      doTestSendLargeMessage(65536);
      doTestSendLargeMessage(65536 * 2);
      doTestSendLargeMessage(65536 * 4);
   }

   @Test(timeout = 120000)
   public void testSend1MBMessage() throws Exception {
      doTestSendLargeMessage(1024 * 1024);
   }

   @Ignore("Useful for performance testing")
   @Test(timeout = 120000)
   public void testSend10MBMessage() throws Exception {
      doTestSendLargeMessage(1024 * 1024 * 10);
   }

   @Ignore("Useful for performance testing")
   @Test(timeout = 120000)
   public void testSend100MBMessage() throws Exception {
      doTestSendLargeMessage(1024 * 1024 * 100);
   }

   public void doTestSendLargeMessage(int expectedSize) throws Exception {
      LOG.info("doTestSendLargeMessage called with expectedSize " + expectedSize);
      byte[] payload = createLargePayload(expectedSize);
      assertEquals(expectedSize, payload.length);

      ConnectionFactory factory = new JmsConnectionFactory("amqp://localhost:61616");
      try (Connection connection = factory.createConnection()) {

         long startTime = System.currentTimeMillis();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue queue = session.createQueue(name.getMethodName());
         MessageProducer producer = session.createProducer(queue);
         BytesMessage message = session.createBytesMessage();
         message.writeBytes(payload);
         producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

         // Set this to non-default to get a Header in the encoded message.
         producer.setPriority(4);
         producer.send(message);
         long endTime = System.currentTimeMillis();

         LOG.info("Returned from send after {} ms", endTime - startTime);
         startTime = System.currentTimeMillis();
         MessageConsumer consumer = session.createConsumer(queue);
         connection.start();

         LOG.info("Calling receive");
         Message received = consumer.receive();
         assertNotNull(received);
         assertTrue(received instanceof BytesMessage);
         BytesMessage bytesMessage = (BytesMessage) received;
         assertNotNull(bytesMessage);
         endTime = System.currentTimeMillis();

         LOG.info("Returned from receive after {} ms", endTime - startTime);
         byte[] bytesReceived = new byte[expectedSize];
         assertEquals(expectedSize, bytesMessage.readBytes(bytesReceived, expectedSize));
         assertTrue(Arrays.equals(payload, bytesReceived));
         connection.close();
      }
   }

   @Test(timeout = 60000)
   public void testReceiveRedeliveredLargeMessagesWithSessionFlowControl() throws Exception {
      server.getAddressSettingsRepository().addMatch("#", new AddressSettings().setDefaultAddressRoutingType(RoutingType.ANYCAST));

      int numMsgs = 10;
      int msgSize = 2_000_000;
      int maxFrameSize = FRAME_SIZE; // Match the brokers outgoing frame size limit to make window sizing easy
      int sessionCapacity = 2_500_000; // Restrict session to 1.x messages in flight at once, make it likely send is partial.

      byte[] payload = createLargePayload(msgSize);
      assertEquals(msgSize, payload.length);

      AmqpClient client = createAmqpClient();

      AmqpConnection connection = client.createConnection();
      connection.setMaxFrameSize(maxFrameSize);
      connection.setSessionIncomingCapacity(sessionCapacity);

      connection.connect();
      addConnection(connection);
      try {
         String testQueueName = getTestName();
         AmqpSession session = connection.createSession();
         AmqpSender sender = session.createSender(testQueueName);

         for (int i = 0; i < numMsgs; ++i) {
            AmqpMessage message = new AmqpMessage();
            message.setBytes(payload);

            sender.send(message);
         }

         Wait.assertEquals(numMsgs, () -> getMessageCount(server.getPostOffice(), testQueueName), 5000, 10);

         AmqpReceiver receiver = session.createReceiver(testQueueName);
         receiver.flow(numMsgs);

         ArrayList<AmqpMessage> messages = new ArrayList<>();
         for (int i = 0; i < numMsgs; ++i) {
            AmqpMessage message = receiver.receive(5, TimeUnit.SECONDS);
            assertNotNull("failed at " + i, message);
            messages.add(message);
         }

         for (int i = 0; i < numMsgs; ++i) {
            AmqpMessage msg = messages.get(i);
            msg.modified(true, false);
         }

         receiver.close();

         AmqpReceiver receiver2 = session.createReceiver(testQueueName);
         receiver2.flow(numMsgs);
         for (int i = 0; i < numMsgs; ++i) {
            AmqpMessage message = receiver2.receive(5, TimeUnit.SECONDS);
            assertNotNull("failed at " + i, message);

            Section body = message.getWrappedMessage().getBody();
            assertNotNull("No message body for msg " + i, body);

            assertTrue("Unexpected message body type for msg " + body.getClass(), body instanceof Data);
            assertEquals("Unexpected body content for msg", new Binary(payload, 0, payload.length), ((Data) body).getValue());

            message.accept();
         }

         session.close();

      } finally {
         connection.close();
      }
   }

   @Test(timeout = 60000)
   public void testMessageWithAmqpValueAndEmptyBinaryPreservesBody() throws Exception {
      server.getAddressSettingsRepository().addMatch("#", new AddressSettings().setDefaultAddressRoutingType(RoutingType.ANYCAST));

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      try {
         AmqpSession session = connection.createSession();
         AmqpSender sender = session.createSender(getTestName());

         AmqpMessage message = createAmqpLargeMessageWithNoBody();

         message.getWrappedMessage().setBody(new AmqpValue(new Binary(new byte[0])));

         sender.send(message);
         sender.close();

         AmqpReceiver receiver = session.createReceiver(getTestName());
         receiver.flow(1);

         AmqpMessage received = receiver.receive(5, TimeUnit.SECONDS);
         assertNotNull("failed to read large AMQP message", received);
         MessageImpl wrapped = (MessageImpl) received.getWrappedMessage();

         assertTrue(wrapped.getBody() instanceof AmqpValue);
         AmqpValue body = (AmqpValue) wrapped.getBody();
         assertTrue(body.getValue() instanceof Binary);
         Binary payload = (Binary) body.getValue();
         assertEquals(0, payload.getLength());

         received.accept();
         session.close();
      } finally {
         connection.close();
      }
   }

   @Test(timeout = 60000)
   public void testMessageWithDataAndEmptyBinaryPreservesBody() throws Exception {
      server.getAddressSettingsRepository().addMatch("#", new AddressSettings().setDefaultAddressRoutingType(RoutingType.ANYCAST));

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      try {
         AmqpSession session = connection.createSession();
         AmqpSender sender = session.createSender(getTestName());

         AmqpMessage message = createAmqpLargeMessageWithNoBody();

         message.getWrappedMessage().setBody(new Data(new Binary(new byte[0])));

         sender.send(message);
         sender.close();

         AmqpReceiver receiver = session.createReceiver(getTestName());
         receiver.flow(1);

         AmqpMessage received = receiver.receive(5, TimeUnit.SECONDS);
         assertNotNull("failed to read large AMQP message", received);
         MessageImpl wrapped = (MessageImpl) received.getWrappedMessage();

         assertTrue(wrapped.getBody() instanceof Data);
         Data body = (Data) wrapped.getBody();
         assertTrue(body.getValue() instanceof Binary);
         Binary payload = (Binary) body.getValue();
         assertEquals(0, payload.getLength());

         received.accept();
         session.close();
      } finally {
         connection.close();
      }
   }

   @Test(timeout = 60000)
   public void testMessageWithDataAndContentTypeOfTextPreservesBodyType() throws Exception {
      server.getAddressSettingsRepository().addMatch("#", new AddressSettings().setDefaultAddressRoutingType(RoutingType.ANYCAST));

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      try {
         AmqpSession session = connection.createSession();
         AmqpSender sender = session.createSender(getTestName());

         AmqpMessage message = createAmqpLargeMessageWithNoBody();

         String messageText = "This text will be in a Data Section";

         message.getWrappedMessage().setContentType("text/plain");
         message.getWrappedMessage().setBody(new Data(new Binary(messageText.getBytes(StandardCharsets.UTF_8))));

         sender.send(message);
         sender.close();

         AmqpReceiver receiver = session.createReceiver(getTestName());
         receiver.flow(1);

         AmqpMessage received = receiver.receive(10, TimeUnit.SECONDS);
         assertNotNull("failed to read large AMQP message", received);
         MessageImpl wrapped = (MessageImpl) received.getWrappedMessage();

         assertTrue(wrapped.getBody() instanceof Data);
         Data body = (Data) wrapped.getBody();
         assertTrue(body.getValue() instanceof Binary);
         Binary payload = (Binary) body.getValue();
         String reconstitutedString = new String(
            payload.getArray(), payload.getArrayOffset(), payload.getLength(), StandardCharsets.UTF_8);

         assertEquals(messageText, reconstitutedString);

         received.accept();
         session.close();
      } finally {
         connection.close();
      }
   }

   @SuppressWarnings({ "unchecked", "rawtypes" })
   @Test(timeout = 60000)
   public void testMessageWithAmqpValueListPreservesBodyType() throws Exception {
      server.getAddressSettingsRepository().addMatch("#", new AddressSettings().setDefaultAddressRoutingType(RoutingType.ANYCAST));

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      try {
         AmqpSession session = connection.createSession();
         AmqpSender sender = session.createSender(getTestName());

         AmqpMessage message = createAmqpLargeMessageWithNoBody();

         List<String> values = new ArrayList<>();
         values.add("1");
         values.add("2");
         values.add("3");

         message.getWrappedMessage().setBody(new AmqpValue(values));

         sender.send(message);
         sender.close();

         AmqpReceiver receiver = session.createReceiver(getTestName());
         receiver.flow(1);

         AmqpMessage received = receiver.receive(10, TimeUnit.SECONDS);
         assertNotNull("failed to read large AMQP message", received);
         MessageImpl wrapped = (MessageImpl) received.getWrappedMessage();

         assertTrue(wrapped.getBody() instanceof AmqpValue);
         AmqpValue body = (AmqpValue) wrapped.getBody();
         assertTrue(body.getValue() instanceof List);
         List<String> payload = (List) body.getValue();
         assertEquals(3, payload.size());

         received.accept();
         session.close();
      } finally {
         connection.close();
      }
   }

   @SuppressWarnings({ "unchecked", "rawtypes" })
   @Test(timeout = 60000)
   public void testMessageWithAmqpSequencePreservesBodyType() throws Exception {
      server.getAddressSettingsRepository().addMatch("#", new AddressSettings().setDefaultAddressRoutingType(RoutingType.ANYCAST));

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      try {
         AmqpSession session = connection.createSession();
         AmqpSender sender = session.createSender(getTestName());

         AmqpMessage message = createAmqpLargeMessageWithNoBody();

         List<String> values = new ArrayList<>();
         values.add("1");
         values.add("2");
         values.add("3");

         message.getWrappedMessage().setBody(new AmqpSequence(values));

         sender.send(message);
         sender.close();

         AmqpReceiver receiver = session.createReceiver(getTestName());
         receiver.flow(1);

         AmqpMessage received = receiver.receive(10, TimeUnit.SECONDS);
         assertNotNull("failed to read large AMQP message", received);
         MessageImpl wrapped = (MessageImpl) received.getWrappedMessage();

         assertTrue(wrapped.getBody() instanceof AmqpSequence);
         AmqpSequence body = (AmqpSequence) wrapped.getBody();
         assertTrue(body.getValue() instanceof List);
         List<String> payload = (List) body.getValue();
         assertEquals(3, payload.size());

         received.accept();
         session.close();
      } finally {
         connection.close();
      }
   }

   private void sendObjectMessages(int nMsgs, ConnectionFactory factory) throws Exception {
      try (Connection connection = factory.createConnection()) {
         Session session = connection.createSession();
         Queue queue = session.createQueue(testQueueName);
         MessageProducer producer = session.createProducer(queue);
         ObjectMessage msg = session.createObjectMessage();

         StringBuilder builder = new StringBuilder();
         for (int i = 0; i < PAYLOAD; ++i) {
            builder.append("A");
         }

         msg.setObject(builder.toString());

         for (int i = 0; i < nMsgs; ++i) {
            msg.setIntProperty("i", (Integer) i);
            producer.send(msg);
         }
      }
   }

   private void sendTextMessages(int nMsgs, ConnectionFactory factory) throws Exception {
      try (Connection connection = factory.createConnection()) {
         Session session = connection.createSession();
         Queue queue = session.createQueue(testQueueName);
         MessageProducer producer = session.createProducer(queue);
         TextMessage msg = session.createTextMessage();

         StringBuilder builder = new StringBuilder();
         for (int i = 0; i < PAYLOAD; ++i) {
            builder.append("A");
         }

         msg.setText(builder.toString());

         for (int i = 0; i < nMsgs; ++i) {
            msg.setIntProperty("i", (Integer) i);
            producer.send(msg);
         }
      }
   }

   private void sendBytesMessages(int nMsgs, ConnectionFactory factory) throws Exception {
      try (Connection connection = factory.createConnection()) {
         Session session = connection.createSession();
         Queue queue = session.createQueue(testQueueName);
         MessageProducer producer = session.createProducer(queue);
         BytesMessage msg = session.createBytesMessage();

         StringBuilder builder = new StringBuilder();
         for (int i = 0; i < PAYLOAD; ++i) {
            builder.append("A");
         }

         msg.writeBytes(builder.toString().getBytes(StandardCharsets.UTF_8));

         for (int i = 0; i < nMsgs; ++i) {
            msg.setIntProperty("i", (Integer) i);
            producer.send(msg);
         }
      }
   }

   private AmqpMessage createAmqpMessage(byte value, int payloadSize) {
      AmqpMessage message = new AmqpMessage();
      byte[] payload = new byte[payloadSize];
      for (int i = 0; i < payload.length; i++) {
         payload[i] = value;
      }
      message.setBytes(payload);
      return message;
   }

   private AmqpMessage createAmqpLargeMessageWithNoBody() {
      AmqpMessage message = new AmqpMessage();

      byte[] payload = new byte[512 * 1024];
      for (int i = 0; i < payload.length; i++) {
         payload[i] = (byte) 65;
      }

      message.setMessageAnnotation("x-opt-big-blob", new String(payload, StandardCharsets.UTF_8));

      return message;
   }
}
