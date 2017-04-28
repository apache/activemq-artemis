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

   @Test(timeout = 60000)
   public void testAddressControlSendMessage() throws Exception {
      SimpleString address = RandomUtil.randomSimpleString();
      server.createQueue(address, RoutingType.ANYCAST, address, null, true, false);

      AddressControl addressControl = ManagementControlHelper.createAddressControl(address, mBeanServer);
      Assert.assertEquals(1, addressControl.getQueueNames().length);
      addressControl.sendMessage(null, org.apache.activemq.artemis.api.core.Message.BYTES_TYPE, Base64.encodeBytes("test".getBytes()), false, fullUser, fullPass);

      Wait.waitFor(() -> addressControl.getMessageCount() == 1);

      Assert.assertEquals(1, addressControl.getMessageCount());

      Connection connection = createConnection("myClientId");
      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         javax.jms.Queue queue = session.createQueue(address.toString());
         MessageConsumer consumer = session.createConsumer(queue);
         Message message = consumer.receive(500);
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
      server.createQueue(address, RoutingType.ANYCAST, address, null, true, false);

      AddressControl addressControl = ManagementControlHelper.createAddressControl(address, mBeanServer);
      Assert.assertEquals(1, addressControl.getQueueNames().length);
      addressControl.sendMessage(null, org.apache.activemq.artemis.api.core.Message.TEXT_TYPE, "test", false, fullUser, fullPass);

      Wait.waitFor(() -> addressControl.getMessageCount() == 1);

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

   @Test(timeout = 60000)
   public void testBytesMessageSendReceive() throws Throwable {
      long time = System.currentTimeMillis();

      Connection connection = createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue queue = session.createQueue(getQueueName());

      byte[] bytes = new byte[0xf + 1];
      for (int i = 0; i <= 0xf; i++) {
         bytes[i] = (byte) i;
      }

      MessageProducer producer = session.createProducer(queue);
      for (int i = 0; i < NUM_MESSAGES; i++) {
         System.out.println("Sending " + i);
         BytesMessage message = session.createBytesMessage();

         message.writeBytes(bytes);
         message.setIntProperty("count", i);
         producer.send(message);
      }

      Session sessionConsumer = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      final MessageConsumer consumer = sessionConsumer.createConsumer(queue);

      for (int i = 0; i < NUM_MESSAGES; i++) {
         BytesMessage m = (BytesMessage) consumer.receive(5000);
         Assert.assertNotNull("Could not receive message count=" + i + " on consumer", m);

         m.reset();

         long size = m.getBodyLength();
         byte[] bytesReceived = new byte[(int) size];
         m.readBytes(bytesReceived);

         System.out.println("Received " + ByteUtil.bytesToHex(bytesReceived, 1) + " count - " + m.getIntProperty("count"));

         Assert.assertArrayEquals(bytes, bytesReceived);
      }

      long taken = (System.currentTimeMillis() - time) / 1000;
      System.out.println("taken = " + taken);
   }

   @Test(timeout = 60000)
   public void testMessageSendReceive() throws Throwable {
      long time = System.currentTimeMillis();

      Connection connection = createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue queue = session.createQueue(getQueueName());

      byte[] bytes = new byte[0xf + 1];
      for (int i = 0; i <= 0xf; i++) {
         bytes[i] = (byte) i;
      }

      MessageProducer producer = session.createProducer(queue);
      for (int i = 0; i < NUM_MESSAGES; i++) {
         System.out.println("Sending " + i);
         Message message = session.createMessage();

         message.setIntProperty("count", i);
         producer.send(message);
      }

      Session sessionConsumer = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      final MessageConsumer consumer = sessionConsumer.createConsumer(queue);

      for (int i = 0; i < NUM_MESSAGES; i++) {
         Message m = consumer.receive(5000);
         Assert.assertNotNull("Could not receive message count=" + i + " on consumer", m);
      }

      long taken = (System.currentTimeMillis() - time) / 1000;
      System.out.println("taken = " + taken);
   }

   @Test(timeout = 60000)
   public void testMapMessageSendReceive() throws Throwable {
      long time = System.currentTimeMillis();

      Connection connection = createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue queue = session.createQueue(getQueueName());

      MessageProducer producer = session.createProducer(queue);
      for (int i = 0; i < NUM_MESSAGES; i++) {
         System.out.println("Sending " + i);
         MapMessage message = session.createMapMessage();

         message.setInt("i", i);
         message.setIntProperty("count", i);
         producer.send(message);
      }

      Session sessionConsumer = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      final MessageConsumer consumer = sessionConsumer.createConsumer(queue);

      for (int i = 0; i < NUM_MESSAGES; i++) {
         MapMessage m = (MapMessage) consumer.receive(5000);
         Assert.assertNotNull("Could not receive message count=" + i + " on consumer", m);

         Assert.assertEquals(i, m.getInt("i"));
         Assert.assertEquals(i, m.getIntProperty("count"));
      }

      long taken = (System.currentTimeMillis() - time) / 1000;
      System.out.println("taken = " + taken);
   }

   @Test(timeout = 60000)
   public void testTextMessageSendReceive() throws Throwable {
      long time = System.currentTimeMillis();

      Connection connection = createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue queue = session.createQueue(getQueueName());

      MessageProducer producer = session.createProducer(queue);
      for (int i = 0; i < NUM_MESSAGES; i++) {
         System.out.println("Sending " + i);
         TextMessage message = session.createTextMessage("text" + i);
         message.setStringProperty("text", "text" + i);
         producer.send(message);
      }

      Session sessionConsumer = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      final MessageConsumer consumer = sessionConsumer.createConsumer(queue);

      for (int i = 0; i < NUM_MESSAGES; i++) {
         TextMessage m = (TextMessage) consumer.receive(5000);
         Assert.assertNotNull("Could not receive message count=" + i + " on consumer", m);
         Assert.assertEquals("text" + i, m.getText());
      }

      long taken = (System.currentTimeMillis() - time) / 1000;
      System.out.println("taken = " + taken);
   }

   @Test(timeout = 60000)
   public void testStreamMessageSendReceive() throws Throwable {
      Connection connection = createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue queue = session.createQueue(getQueueName());

      MessageProducer producer = session.createProducer(queue);
      for (int i = 0; i < NUM_MESSAGES; i++) {
         StreamMessage message = session.createStreamMessage();
         message.writeInt(i);
         message.writeBoolean(true);
         message.writeString("test");
         producer.send(message);
      }

      Session sessionConsumer = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      final MessageConsumer consumer = sessionConsumer.createConsumer(queue);

      for (int i = 0; i < NUM_MESSAGES; i++) {
         StreamMessage m = (StreamMessage) consumer.receive(5000);
         Assert.assertNotNull("Could not receive message count=" + i + " on consumer", m);

         Assert.assertEquals(i, m.readInt());
         Assert.assertEquals(true, m.readBoolean());
         Assert.assertEquals("test", m.readString());
      }
   }

   @Test(timeout = 60000)
   public void testObjectMessageWithArrayListPayload() throws Throwable {
      ArrayList<String> payload = new ArrayList<>();
      payload.add("aString");

      Connection connection = createConnection();

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue queue = session.createQueue(getQueueName());
      MessageProducer producer = session.createProducer(queue);
      ObjectMessage objectMessage = session.createObjectMessage(payload);
      producer.send(objectMessage);
      session.close();

      session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageConsumer cons = session.createConsumer(queue);
      connection.start();

      objectMessage = (ObjectMessage) cons.receive(5000);
      assertNotNull(objectMessage);
      @SuppressWarnings("unchecked")
      ArrayList<String> received = (ArrayList<String>) objectMessage.getObject();
      assertEquals(received.get(0), "aString");

      connection.close();
   }

   @Test(timeout = 60000)
   public void testObjectMessageUsingCustomType() throws Throwable {
      long time = System.currentTimeMillis();

      Connection connection = createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue queue = session.createQueue(getQueueName());

      MessageProducer producer = session.createProducer(queue);
      for (int i = 0; i < NUM_MESSAGES; i++) {
         System.out.println("Sending " + i);
         ObjectMessage message = session.createObjectMessage(new AnythingSerializable(i));
         producer.send(message);
      }

      Session sessionConsumer = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      final MessageConsumer consumer = sessionConsumer.createConsumer(queue);

      for (int i = 0; i < NUM_MESSAGES; i++) {
         ObjectMessage msg = (ObjectMessage) consumer.receive(5000);
         Assert.assertNotNull("Could not receive message count=" + i + " on consumer", msg);

         AnythingSerializable someSerialThing = (AnythingSerializable) msg.getObject();
         Assert.assertEquals(i, someSerialThing.getCount());
      }

      long taken = (System.currentTimeMillis() - time) / 1000;
      System.out.println("taken = " + taken);
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

   @Test(timeout = 60000)
   public void testPropertiesArePreserved() throws Exception {
      Connection connection = createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

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

      connection.start();

      MessageConsumer messageConsumer = session.createConsumer(queue);
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

      connection.close();
   }
}
