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

import java.io.Serializable;
import java.util.ArrayList;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.management.AddressControl;
import org.apache.activemq.artemis.tests.integration.management.ManagementControlHelper;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.artemis.utils.Base64;
import org.apache.activemq.artemis.utils.ByteUtil;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test that various message types are handled as expected with an AMQP JMS client.
 */
public class JMSMessageTypesTest extends JMSClientTestSupport {

   final int NUM_MESSAGES = 10;

   @Override
   protected String getConfiguredProtocols() {
      return "AMQP,OPENWIRE,CORE";
   }

   @Test(timeout = 60000)
   public void testAddressControlSendMessage() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      server.createQueue(new QueueConfiguration(address).setRoutingType(RoutingType.ANYCAST));

      AddressControl addressControl = ManagementControlHelper.createAddressControl(address, mBeanServer);
      Assert.assertEquals(1, addressControl.getQueueNames().length);
      addressControl.sendMessage(null, org.apache.activemq.artemis.api.core.Message.BYTES_TYPE, Base64.encodeBytes("test".getBytes()), false, fullUser, fullPass);

      Wait.assertEquals(1, addressControl::getMessageCount);

      Connection connection = createConnection("myClientId");
      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         javax.jms.Queue queue = session.createQueue(address.toString());
         MessageConsumer consumer = session.createConsumer(queue);
         Message message = consumer.receive(5000);
         assertNotNull(message);
         byte[] buffer = new byte[(int)((BytesMessage)message).getBodyLength()];
         ((BytesMessage)message).readBytes(buffer);
         assertEquals("test", new String(buffer));
         session.close();
         connection.close();
      } finally {
         if (connection != null) {
            connection.close();
         }
      }
   }

   @Test(timeout = 60000)
   public void testAddressControlSendMessageWithText() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      server.createQueue(new QueueConfiguration(address).setRoutingType(RoutingType.ANYCAST));

      AddressControl addressControl = ManagementControlHelper.createAddressControl(address, mBeanServer);
      Assert.assertEquals(1, addressControl.getQueueNames().length);
      addressControl.sendMessage(null, org.apache.activemq.artemis.api.core.Message.TEXT_TYPE, "test", false, fullUser, fullPass);

      Wait.assertEquals(1, addressControl::getMessageCount);

      Assert.assertEquals(1, addressControl.getMessageCount());

      Connection connection = createConnection("myClientId");
      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         javax.jms.Queue queue = session.createQueue(address.toString());
         MessageConsumer consumer = session.createConsumer(queue);
         Message message = consumer.receive(500);
         assertNotNull(message);
         String text = ((TextMessage) message).getText();
         assertEquals("test", text);
         session.close();
         connection.close();
      } finally {
         if (connection != null) {
            connection.close();
         }
      }
   }

   private void testBytesMessageSendReceive(Connection producerConnection, Connection consumerConnection) throws Throwable {
      long time = System.currentTimeMillis();

      Session session = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue queue = session.createQueue(getQueueName());

      byte[] bytes = new byte[0xf + 1];
      for (int i = 0; i <= 0xf; i++) {
         bytes[i] = (byte) i;
      }

      MessageProducer producer = session.createProducer(queue);
      for (int i = 0; i < NUM_MESSAGES; i++) {
         instanceLog.debug("Sending " + i);
         BytesMessage message = session.createBytesMessage();

         message.writeBytes(bytes);
         message.setIntProperty("count", i);
         producer.send(message);
      }

      Session sessionConsumer = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue consumerQueue = sessionConsumer.createQueue(getQueueName());
      final MessageConsumer consumer = sessionConsumer.createConsumer(consumerQueue);

      for (int i = 0; i < NUM_MESSAGES; i++) {
         BytesMessage m = (BytesMessage) consumer.receive(5000);
         Assert.assertNotNull("Could not receive message count=" + i + " on consumer", m);

         m.reset();

         long size = m.getBodyLength();
         byte[] bytesReceived = new byte[(int) size];
         m.readBytes(bytesReceived);

         instanceLog.debug("Received " + ByteUtil.bytesToHex(bytesReceived, 1) + " count - " + m.getIntProperty("count"));

         Assert.assertArrayEquals(bytes, bytesReceived);
      }

      long taken = (System.currentTimeMillis() - time) / 1000;
      instanceLog.debug("taken = " + taken);
   }

   @Test(timeout = 60000)
   public void testBytesMessageSendReceiveFromAMQPToAMQP() throws Throwable {
      testBytesMessageSendReceive(createConnection(), createConnection());
   }

   @Test(timeout = 60000)
   public void testBytesMessageSendReceiveFromCoreToAMQP() throws Throwable {
      testBytesMessageSendReceive(createCoreConnection(), createConnection());
   }

   @Test(timeout = 60000)
   public void testBytesMessageSendReceiveFromAMQPToCore() throws Throwable {
      testBytesMessageSendReceive(createConnection(), createCoreConnection());
   }

   private void testMessageSendReceive(Connection producerConnection, Connection consumerConnection) throws Throwable {
      long time = System.currentTimeMillis();

      Session session = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue queue = session.createQueue(getQueueName());

      byte[] bytes = new byte[0xf + 1];
      for (int i = 0; i <= 0xf; i++) {
         bytes[i] = (byte) i;
      }

      MessageProducer producer = session.createProducer(queue);
      for (int i = 0; i < NUM_MESSAGES; i++) {
         instanceLog.debug("Sending " + i);
         Message message = session.createMessage();

         message.setIntProperty("count", i);
         producer.send(message);
      }

      Session sessionConsumer = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue consumerQueue = sessionConsumer.createQueue(getQueueName());
      final MessageConsumer consumer = sessionConsumer.createConsumer(consumerQueue);

      for (int i = 0; i < NUM_MESSAGES; i++) {
         Message m = consumer.receive(5000);
         Assert.assertNotNull("Could not receive message count=" + i + " on consumer", m);
      }

      long taken = (System.currentTimeMillis() - time) / 1000;
      instanceLog.debug("taken = " + taken);
   }

   @Test(timeout = 60000)
   public void testMessageSendReceiveFromAMQPToAMQP() throws Throwable {
      testMessageSendReceive(createConnection(), createConnection());
   }

   @Test(timeout = 60000)
   public void testMessageSendReceiveFromCoreToAMQP() throws Throwable {
      testMessageSendReceive(createCoreConnection(), createConnection());
   }

   @Test(timeout = 60000)
   public void testMessageSendReceiveFromAMQPToCore() throws Throwable {
      testMessageSendReceive(createConnection(), createCoreConnection());
   }

   private void testMapMessageSendReceive(Connection producerConnection, Connection consumerConnection) throws Throwable {
      long time = System.currentTimeMillis();

      Session session = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue queue = session.createQueue(getQueueName());

      MessageProducer producer = session.createProducer(queue);
      for (int i = 0; i < NUM_MESSAGES; i++) {
         instanceLog.debug("Sending " + i);
         MapMessage message = session.createMapMessage();

         message.setInt("i", i);
         message.setIntProperty("count", i);
         producer.send(message);
      }

      Session sessionConsumer = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue consumerQueue = sessionConsumer.createQueue(getQueueName());
      final MessageConsumer consumer = sessionConsumer.createConsumer(consumerQueue);

      for (int i = 0; i < NUM_MESSAGES; i++) {
         MapMessage m = (MapMessage) consumer.receive(5000);
         Assert.assertNotNull("Could not receive message count=" + i + " on consumer", m);

         Assert.assertEquals(i, m.getInt("i"));
         Assert.assertEquals(i, m.getIntProperty("count"));
      }

      long taken = (System.currentTimeMillis() - time) / 1000;
      instanceLog.debug("taken = " + taken);
   }

   @Test(timeout = 60000)
   public void testMapMessageSendReceiveFromAMQPToAMQP() throws Throwable {
      testMapMessageSendReceive(createConnection(), createConnection());
   }

   @Test(timeout = 60000)
   public void testMapMessageSendReceiveFromCoreToAMQP() throws Throwable {
      testMapMessageSendReceive(createCoreConnection(), createConnection());
   }

   @Test(timeout = 60000)
   public void testMapMessageSendReceiveFromAMQPToCore() throws Throwable {
      testMapMessageSendReceive(createConnection(), createCoreConnection());
   }

   private void testTextMessageSendReceive(Connection producerConnection, Connection consumerConnection) throws Throwable {
      long time = System.currentTimeMillis();

      Session session = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue queue = session.createQueue(getQueueName());

      MessageProducer producer = session.createProducer(queue);
      for (int i = 0; i < NUM_MESSAGES; i++) {
         instanceLog.debug("Sending " + i);
         TextMessage message = session.createTextMessage("text" + i);
         message.setStringProperty("text", "text" + i);
         producer.send(message);
      }

      Session sessionConsumer = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue consumerQueue = sessionConsumer.createQueue(getQueueName());
      final MessageConsumer consumer = sessionConsumer.createConsumer(consumerQueue);

      for (int i = 0; i < NUM_MESSAGES; i++) {
         TextMessage m = (TextMessage) consumer.receive(5000);
         Assert.assertNotNull("Could not receive message count=" + i + " on consumer", m);
         Assert.assertEquals("text" + i, m.getText());
      }

      long taken = (System.currentTimeMillis() - time) / 1000;
      instanceLog.debug("taken = " + taken);
   }

   @Test(timeout = 60000)
   public void testTextMessageSendReceiveFromAMQPToAMQP() throws Throwable {
      testTextMessageSendReceive(createConnection(), createConnection());
   }

   @Test(timeout = 60000)
   public void testTextMessageSendReceiveFromCoreToAMQP() throws Throwable {
      testTextMessageSendReceive(createCoreConnection(), createConnection());
   }

   @Test(timeout = 60000)
   public void testTextMessageSendReceiveFromAMQPToCore() throws Throwable {
      testTextMessageSendReceive(createConnection(), createCoreConnection());
   }

   private void testStreamMessageSendReceive(Connection producerConnection, Connection consumerConnection) throws Throwable {
      Session session = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue queue = session.createQueue(getQueueName());

      MessageProducer producer = session.createProducer(queue);
      for (int i = 0; i < NUM_MESSAGES; i++) {
         StreamMessage message = session.createStreamMessage();
         message.writeInt(i);
         message.writeBoolean(true);
         message.writeString("test");
         producer.send(message);
      }

      Session sessionConsumer = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue consumerQueue = sessionConsumer.createQueue(getQueueName());
      final MessageConsumer consumer = sessionConsumer.createConsumer(consumerQueue);

      for (int i = 0; i < NUM_MESSAGES; i++) {
         StreamMessage m = (StreamMessage) consumer.receive(5000);
         Assert.assertNotNull("Could not receive message count=" + i + " on consumer", m);

         Assert.assertEquals(i, m.readInt());
         Assert.assertEquals(true, m.readBoolean());
         Assert.assertEquals("test", m.readString());
      }
   }

   @Test(timeout = 60000)
   public void testStreamMessageSendReceiveFromAMQPToAMQP() throws Throwable {
      testStreamMessageSendReceive(createConnection(), createConnection());
   }

   @Test(timeout = 60000)
   public void testStreamMessageSendReceiveFromCoreToAMQP() throws Throwable {
      testStreamMessageSendReceive(createCoreConnection(), createConnection());
   }

   @Test(timeout = 60000)
   public void testStreamMessageSendReceiveFromAMQPToCore() throws Throwable {
      testStreamMessageSendReceive(createConnection(), createCoreConnection());
   }

   private void testObjectMessageWithArrayListPayload(Connection producerConnection, Connection consumerConnection) throws Throwable {
      ArrayList<String> payload = new ArrayList<>();
      payload.add("aString");

      Session session = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue queue = session.createQueue(getQueueName());
      MessageProducer producer = session.createProducer(queue);
      ObjectMessage objectMessage = session.createObjectMessage(payload);
      producer.send(objectMessage);
      session.close();

      session = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue consumerQueue = session.createQueue(getQueueName());
      MessageConsumer cons = session.createConsumer(consumerQueue);
      consumerConnection.start();

      objectMessage = (ObjectMessage) cons.receive(5000);
      assertNotNull(objectMessage);
      @SuppressWarnings("unchecked")
      ArrayList<String> received = (ArrayList<String>) objectMessage.getObject();
      assertEquals(received.get(0), "aString");

      consumerConnection.close();
   }

   @Test(timeout = 60000)
   public void testObjectMessageWithArrayListPayloadFromAMQPToAMQP() throws Throwable {
      testObjectMessageWithArrayListPayload(createConnection(), createConnection());
   }

   @Test(timeout = 60000)
   public void testObjectMessageWithArrayListPayloadFromCoreToAMQP() throws Throwable {
      testObjectMessageWithArrayListPayload(createCoreConnection(), createConnection());
   }

   @Test(timeout = 60000)
   public void testObjectMessageWithArrayListPayloadFromAMQPToCore() throws Throwable {
      testObjectMessageWithArrayListPayload(createConnection(), createCoreConnection());
   }

   private void testObjectMessageUsingCustomType(Connection producerConnection, Connection consumerConnection) throws Throwable {
      long time = System.currentTimeMillis();

      Session session = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue queue = session.createQueue(getQueueName());

      MessageProducer producer = session.createProducer(queue);
      for (int i = 0; i < NUM_MESSAGES; i++) {
         instanceLog.debug("Sending " + i);
         ObjectMessage message = session.createObjectMessage(new AnythingSerializable(i));
         producer.send(message);
      }

      Session sessionConsumer = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue consumerQueue = sessionConsumer.createQueue(getQueueName());
      final MessageConsumer consumer = sessionConsumer.createConsumer(consumerQueue);

      for (int i = 0; i < NUM_MESSAGES; i++) {
         ObjectMessage msg = (ObjectMessage) consumer.receive(5000);
         Assert.assertNotNull("Could not receive message count=" + i + " on consumer", msg);

         AnythingSerializable someSerialThing = (AnythingSerializable) msg.getObject();
         Assert.assertEquals(i, someSerialThing.getCount());
      }

      long taken = (System.currentTimeMillis() - time) / 1000;
      instanceLog.debug("taken = " + taken);
   }

   @Test(timeout = 60000)
   public void testObjectMessageUsingCustomTypeFromAMQPToAMQP() throws Throwable {
      testObjectMessageUsingCustomType(createConnection(), createConnection());
   }

   @Test(timeout = 60000)
   public void testObjectMessageUsingCustomTypeFromCoreToAMQP() throws Throwable {
      testObjectMessageUsingCustomType(createCoreConnection(), createConnection());
   }

   @Test(timeout = 60000)
   public void testObjectMessageUsingCustomTypeFromAMQPToCore() throws Throwable {
      testObjectMessageUsingCustomType(createConnection(), createCoreConnection());
   }

   public static class AnythingSerializable implements Serializable {
      private static final long serialVersionUID = 5972085029690947807L;

      private int count;

      public AnythingSerializable(int count) {
         this.count = count;
      }

      public int getCount() {
         return count;
      }
   }

   private void testPropertiesArePreserved(Connection producerConnection, Connection consumerConnection) throws Exception {
      Session session = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      Queue queue = session.createQueue(getQueueName());
      MessageProducer producer = session.createProducer(queue);
      TextMessage message = session.createTextMessage();
      message.setText("msg:0");
      message.setBooleanProperty("true", true);
      message.setBooleanProperty("false", false);
      message.setStringProperty("foo", "bar");
      message.setDoubleProperty("double", 66.6);
      message.setFloatProperty("float", 56.789f);
      message.setIntProperty("int", 8);
      message.setByteProperty("byte", (byte) 10);

      producer.send(message);
      producer.send(message);

      consumerConnection.start();
      Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue consumerQueue = consumerSession.createQueue(getQueueName());
      MessageConsumer messageConsumer = consumerSession.createConsumer(consumerQueue);
      TextMessage received = (TextMessage) messageConsumer.receive(5000);
      Assert.assertNotNull(received);
      Assert.assertEquals("msg:0", received.getText());
      Assert.assertEquals(received.getBooleanProperty("true"), true);
      Assert.assertEquals(received.getBooleanProperty("false"), false);
      Assert.assertEquals(received.getStringProperty("foo"), "bar");
      Assert.assertEquals(received.getDoubleProperty("double"), 66.6, 0.0001);
      Assert.assertEquals(received.getFloatProperty("float"), 56.789f, 0.0001);
      Assert.assertEquals(received.getIntProperty("int"), 8);
      Assert.assertEquals(received.getByteProperty("byte"), (byte) 10);

      received = (TextMessage) messageConsumer.receive(5000);
      Assert.assertNotNull(received);

      consumerConnection.close();
   }

   @Test(timeout = 60000)
   public void testPropertiesArePreservedFromAMQPToAMQP() throws Throwable {
      testPropertiesArePreserved(createConnection(), createConnection());
   }

   @Test(timeout = 60000)
   public void testPropertiesArePreservedFromCoreToAMQP() throws Throwable {
      testPropertiesArePreserved(createCoreConnection(), createConnection());
   }

   @Test(timeout = 60000)
   public void testPropertiesArePreservedFromAMQPToCore() throws Throwable {
      testPropertiesArePreserved(createConnection(), createCoreConnection());
   }
}