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
package org.apache.activemq.artemis.tests.integration.openwire.interop;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.MapMessage;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;

import org.apache.activemq.ActiveMQMessageConsumer;
import org.apache.activemq.ActiveMQMessageProducer;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.tests.integration.openwire.BasicOpenWireTest;
import org.apache.activemq.command.ActiveMQDestination;
import org.junit.Before;
import org.junit.Test;

/**
 * This test covers interactions between core clients and
 * openwire clients, i.e. core producers sending messages
 * to be received by openwire receivers, and vice versa.
 */
public class GeneralInteropTest extends BasicOpenWireTest {

   private ServerLocator locator;

   @Before
   @Override
   public void setUp() throws Exception {
      super.setUp();
      locator = this.createInVMNonHALocator();
   }

   @Test
   public void testReceivingFromCore() throws Exception {
      final String text = "HelloWorld";

      //text messages
      sendTextMessageUsingCoreJms(queueName, text);

      connection.start();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      ActiveMQDestination destination = createDestination(session, ActiveMQDestination.QUEUE_TYPE);

      System.out.println("destination: " + destination);
      final ActiveMQMessageConsumer consumer = (ActiveMQMessageConsumer) session.createConsumer(destination);
      TextMessage textMessage = (TextMessage) consumer.receive(5000);

      assertEquals(text, textMessage.getText());

      assertEquals(destination, textMessage.getJMSDestination());

      //map messages
      sendMapMessageUsingCoreJms(queueName);

      MapMessage mapMessage = (MapMessage) consumer.receive(5000);

      assertEquals(destination, mapMessage.getJMSDestination());

      assertTrue(mapMessage.getBoolean("aboolean"));
      assertEquals((byte) 4, mapMessage.getByte("abyte"));
      byte[] bytes = mapMessage.getBytes("abytes");
      assertEquals(2, bytes.length);
      assertEquals((byte) 4, bytes[0]);
      assertEquals((byte) 5, bytes[1]);
      assertEquals('a', mapMessage.getChar("achar"));
      Double doubleVal = mapMessage.getDouble("adouble");
      assertTrue(doubleVal.equals(Double.valueOf(4.4)));
      Float floatVal = mapMessage.getFloat("afloat");
      assertTrue(floatVal.equals(Float.valueOf(4.5F)));
      assertEquals(40, mapMessage.getInt("aint"));
      assertEquals(80L, mapMessage.getLong("along"));
      assertEquals(65, mapMessage.getShort("ashort"));
      assertEquals("hello", mapMessage.getString("astring"));

      //object message
      SimpleSerializable obj = new SimpleSerializable();
      sendObjectMessageUsingCoreJms(queueName, obj);

      ObjectMessage objectMessage = (ObjectMessage) consumer.receive(5000);
      SimpleSerializable data = (SimpleSerializable) objectMessage.getObject();

      assertEquals(obj.objName, data.objName);
      assertEquals(obj.intVal, data.intVal);
      assertEquals(obj.longVal, data.longVal);

      //stream messages
      sendStreamMessageUsingCoreJms(queueName);

      StreamMessage streamMessage = (StreamMessage) consumer.receive(5000);

      assertEquals(destination, streamMessage.getJMSDestination());

      assertTrue(streamMessage.readBoolean());
      assertEquals((byte) 2, streamMessage.readByte());

      byte[] streamBytes = new byte[2];
      streamMessage.readBytes(streamBytes);

      assertEquals(6, streamBytes[0]);
      assertEquals(7, streamBytes[1]);

      assertEquals('b', streamMessage.readChar());
      Double streamDouble = streamMessage.readDouble();
      assertTrue(streamDouble.equals(Double.valueOf(6.5)));
      Float streamFloat = streamMessage.readFloat();
      assertTrue(streamFloat.equals(Float.valueOf(93.9F)));
      assertEquals(7657, streamMessage.readInt());
      assertEquals(239999L, streamMessage.readLong());
      assertEquals((short) 34222, streamMessage.readShort());
      assertEquals("hello streammessage", streamMessage.readString());

      //bytes messages
      final byte[] bytesData = text.getBytes(StandardCharsets.UTF_8);
      sendBytesMessageUsingCoreJms(queueName, bytesData);

      BytesMessage bytesMessage = (BytesMessage) consumer.receive(5000);
      byte[] rawBytes = new byte[bytesData.length];
      bytesMessage.readBytes(rawBytes);

      for (int i = 0; i < bytesData.length; i++) {
         assertEquals("failed at " + i, bytesData[i], rawBytes[i]);
      }
      assertTrue(bytesMessage.readBoolean());
      assertEquals(99999L, bytesMessage.readLong());
      assertEquals('h', bytesMessage.readChar());
      assertEquals(987, bytesMessage.readInt());
      assertEquals((short) 1099, bytesMessage.readShort());
      assertEquals("hellobytes", bytesMessage.readUTF());

      //generic message
      sendMessageUsingCoreJms(queueName);

      javax.jms.Message genericMessage = consumer.receive(5000);

      assertEquals(destination, genericMessage.getJMSDestination());
      String value = genericMessage.getStringProperty("stringProperty");
      assertEquals("HelloMessage", value);
      assertFalse(genericMessage.getBooleanProperty("booleanProperty"));
      assertEquals(99999L, genericMessage.getLongProperty("longProperty"));
      assertEquals(979, genericMessage.getIntProperty("intProperty"));
      assertEquals((short) 1099, genericMessage.getShortProperty("shortProperty"));
      assertEquals("HelloMessage", genericMessage.getStringProperty("stringProperty"));
   }

   @Test
   public void testMutipleReceivingFromCore() throws Exception {
      final String text = "HelloWorld";
      final int num = 100;
      //text messages
      sendMultipleTextMessagesUsingCoreJms(queueName, text, 100);

      connection.start();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      ActiveMQDestination destination = createDestination(session, ActiveMQDestination.QUEUE_TYPE);

      final ActiveMQMessageConsumer consumer = (ActiveMQMessageConsumer) session.createConsumer(destination);

      for (int i = 0; i < num; i++) {
         TextMessage textMessage = (TextMessage) consumer.receive(5000);
         assertEquals(text + i, textMessage.getText());

         assertEquals(destination, textMessage.getJMSDestination());
      }
   }

   private void sendMultipleTextMessagesUsingCoreJms(String queueName, String text, int num) throws Exception {
      Connection jmsConn = null;
      try {
         jmsConn = coreCf.createConnection();
         Session session = jmsConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue queue = session.createQueue(queueName);
         MessageProducer producer = session.createProducer(queue);
         for (int i = 0; i < num; i++) {
            TextMessage msg = session.createTextMessage(text + i);
            producer.send(msg);
         }
      } finally {
         if (jmsConn != null) {
            jmsConn.close();
         }
      }
   }

   private void sendMessageUsingCoreJms(String queueName) throws Exception {
      Connection jmsConn = null;
      try {
         jmsConn = coreCf.createConnection();
         Session session = jmsConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         javax.jms.Message message = session.createMessage();

         message.setBooleanProperty("booleanProperty", false);
         message.setLongProperty("longProperty", 99999L);
         message.setByteProperty("byteProperty", (byte) 5);
         message.setIntProperty("intProperty", 979);
         message.setShortProperty("shortProperty", (short) 1099);
         message.setStringProperty("stringProperty", "HelloMessage");

         Queue queue = session.createQueue(queueName);
         MessageProducer producer = session.createProducer(queue);

         producer.send(message);
      } finally {
         if (jmsConn != null) {
            jmsConn.close();
         }
      }

   }

   private void sendBytesMessageUsingCoreJms(String queueName, byte[] data) throws Exception {
      Connection jmsConn = null;
      try {
         jmsConn = coreCf.createConnection();
         Session session = jmsConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         BytesMessage bytesMessage = session.createBytesMessage();

         bytesMessage.writeBytes(data);
         bytesMessage.writeBoolean(true);
         bytesMessage.writeLong(99999L);
         bytesMessage.writeChar('h');
         bytesMessage.writeInt(987);
         bytesMessage.writeShort((short) 1099);
         bytesMessage.writeUTF("hellobytes");

         Queue queue = session.createQueue(queueName);
         MessageProducer producer = session.createProducer(queue);

         producer.send(bytesMessage);
      } finally {
         if (jmsConn != null) {
            jmsConn.close();
         }
      }

   }

   private void sendObjectMessageUsingCoreJms(String queueName, Serializable object) throws Exception {
      Connection jmsConn = null;
      try {
         jmsConn = coreCf.createConnection();
         Session session = jmsConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         ObjectMessage objectMessage = session.createObjectMessage(object);

         Queue queue = session.createQueue(queueName);
         MessageProducer producer = session.createProducer(queue);

         producer.send(objectMessage);
      } finally {
         if (jmsConn != null) {
            jmsConn.close();
         }
      }

   }

   private void sendStreamMessageUsingCoreJms(String queueName) throws Exception {
      Connection jmsConn = null;
      try {
         jmsConn = coreCf.createConnection();
         Session session = jmsConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         StreamMessage msg = session.createStreamMessage();
         msg.writeBoolean(true);
         msg.writeByte((byte) 2);
         msg.writeBytes(new byte[]{6, 7});
         msg.writeChar('b');
         msg.writeDouble(6.5);
         msg.writeFloat((float) 93.9);
         msg.writeInt(7657);
         msg.writeLong(239999L);
         msg.writeShort((short) 34222);
         msg.writeString("hello streammessage");

         Queue queue = session.createQueue(queueName);
         MessageProducer producer = session.createProducer(queue);

         producer.send(msg);
      } finally {
         if (jmsConn != null) {
            jmsConn.close();
         }
      }

   }

   private void sendMapMessageUsingCoreJms(String queueName) throws Exception {
      Connection jmsConn = null;
      try {
         jmsConn = coreCf.createConnection();
         Session session = jmsConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MapMessage mapMessage = session.createMapMessage();
         mapMessage.setBoolean("aboolean", true);
         mapMessage.setByte("abyte", (byte) 4);
         mapMessage.setBytes("abytes", new byte[]{4, 5});
         mapMessage.setChar("achar", 'a');
         mapMessage.setDouble("adouble", 4.4);
         mapMessage.setFloat("afloat", 4.5f);
         mapMessage.setInt("aint", 40);
         mapMessage.setLong("along", 80L);
         mapMessage.setShort("ashort", (short) 65);
         mapMessage.setString("astring", "hello");

         Queue queue = session.createQueue(queueName);
         MessageProducer producer = session.createProducer(queue);

         producer.send(mapMessage);
      } finally {
         if (jmsConn != null) {
            jmsConn.close();
         }
      }

   }

   private void sendTextMessageUsingCoreJms(String address, String text) throws Exception {
      Connection jmsConn = null;
      try {
         jmsConn = coreCf.createConnection();
         Session session = jmsConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         TextMessage msg = session.createTextMessage(text);
         Queue queue = session.createQueue(address);
         MessageProducer producer = session.createProducer(queue);

         producer.send(msg);
      } finally {
         if (jmsConn != null) {
            jmsConn.close();
         }
      }

   }

   @Test
   public void testSendingToCoreJms() throws Exception {
      final String text = "HelloWorld";
      Connection jmsConn = null;

      try {
         jmsConn = coreCf.createConnection();
         jmsConn.start();

         Session session = jmsConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue queue = session.createQueue(this.queueName);
         MessageConsumer coreConsumer = session.createConsumer(queue);

         //text messages
         sendTextMessageUsingOpenWire(text);

         TextMessage txtMessage = (TextMessage) coreConsumer.receive(5000);
         assertEquals(text, txtMessage.getText());
         assertEquals(txtMessage.getJMSDestination(), queue);

         // map messages
         sendMapMessageUsingOpenWire();

         MapMessage mapMessage = (MapMessage) coreConsumer.receive(5000);

         assertEquals(mapMessage.getJMSDestination(), queue);

         assertTrue(mapMessage.getBoolean("aboolean"));
         assertEquals((byte) 4, mapMessage.getByte("abyte"));
         byte[] bytes = mapMessage.getBytes("abytes");
         assertEquals(2, bytes.length);
         assertEquals((byte) 4, bytes[0]);
         assertEquals((byte) 5, bytes[1]);
         assertEquals('a', mapMessage.getChar("achar"));
         Double doubleVal = mapMessage.getDouble("adouble");
         assertTrue(doubleVal.equals(Double.valueOf(4.4)));
         Float floatVal = mapMessage.getFloat("afloat");
         assertTrue(floatVal.equals(Float.valueOf(4.5F)));
         assertEquals(40, mapMessage.getInt("aint"));
         assertEquals(80L, mapMessage.getLong("along"));
         assertEquals(65, mapMessage.getShort("ashort"));
         assertEquals("hello", mapMessage.getString("astring"));

         //object message
         SimpleSerializable obj = new SimpleSerializable();
         sendObjectMessageUsingOpenWire(obj);

         ObjectMessage objectMessage = (ObjectMessage) coreConsumer.receive(5000);

         assertEquals(objectMessage.getJMSDestination(), queue);

         SimpleSerializable data = (SimpleSerializable) objectMessage.getObject();

         assertEquals(obj.objName, data.objName);
         assertEquals(obj.intVal, data.intVal);
         assertEquals(obj.longVal, data.longVal);

         //stream messages
         sendStreamMessageUsingOpenWire(queueName);

         StreamMessage streamMessage = (StreamMessage) coreConsumer.receive(5000);

         assertEquals(streamMessage.getJMSDestination(), queue);
         assertTrue(streamMessage.readBoolean());
         assertEquals((byte) 2, streamMessage.readByte());

         byte[] streamBytes = new byte[2];
         streamMessage.readBytes(streamBytes);

         assertEquals(6, streamBytes[0]);
         assertEquals(7, streamBytes[1]);

         assertEquals('b', streamMessage.readChar());
         Double streamDouble = streamMessage.readDouble();
         assertTrue(streamDouble.equals(Double.valueOf(6.5)));
         Float streamFloat = streamMessage.readFloat();
         assertTrue(streamFloat.equals(Float.valueOf(93.9F)));
         assertEquals(7657, streamMessage.readInt());
         assertEquals(239999L, streamMessage.readLong());
         assertEquals((short) 34222, streamMessage.readShort());
         assertEquals("hello streammessage", streamMessage.readString());

         //bytes messages
         final byte[] bytesData = text.getBytes(StandardCharsets.UTF_8);
         sendBytesMessageUsingOpenWire(bytesData);

         BytesMessage bytesMessage = (BytesMessage) coreConsumer.receive(5000);

         assertEquals(bytesMessage.getJMSDestination(), queue);
         byte[] rawBytes = new byte[bytesData.length];
         bytesMessage.readBytes(rawBytes);

         for (int i = 0; i < bytesData.length; i++) {
            assertEquals(bytesData[i], rawBytes[i]);
         }

         //generic message
         sendMessageUsingOpenWire(queueName);

         javax.jms.Message genericMessage = coreConsumer.receive(5000);
         assertEquals(genericMessage.getJMSDestination(), queue);
         String value = genericMessage.getStringProperty("stringProperty");
         assertEquals("HelloMessage", value);
         assertFalse(genericMessage.getBooleanProperty("booleanProperty"));
         assertEquals(99999L, genericMessage.getLongProperty("longProperty"));
         assertEquals(979, genericMessage.getIntProperty("intProperty"));
         assertEquals((short) 1099, genericMessage.getShortProperty("shortProperty"));
         assertEquals("HelloMessage", genericMessage.getStringProperty("stringProperty"));
      } finally {
         if (jmsConn != null) {
            jmsConn.close();
         }
      }
   }

   @Test
   public void testMultipleSendingToCoreJms() throws Exception {
      final String text = "HelloWorld";
      final int num = 100;
      Connection jmsConn = null;

      try {
         jmsConn = coreCf.createConnection();
         jmsConn.start();

         Session session = jmsConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue queue = session.createQueue(this.queueName);
         MessageConsumer coreConsumer = session.createConsumer(queue);

         //text messages
         sendMultipleTextMessagesUsingOpenWire(text, num);

         for (int i = 0; i < num; i++) {
            TextMessage txtMessage = (TextMessage) coreConsumer.receive(5000);
            assertEquals(txtMessage.getJMSDestination(), queue);
            assertEquals(text + i, txtMessage.getText());
         }
      } finally {
         if (jmsConn != null) {
            jmsConn.close();
         }
      }
   }

   private void sendMultipleTextMessagesUsingOpenWire(String text, int num) throws Exception {
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      ActiveMQDestination destination = createDestination(session, ActiveMQDestination.QUEUE_TYPE);

      final ActiveMQMessageProducer producer = (ActiveMQMessageProducer) session.createProducer(destination);

      for (int i = 0; i < num; i++) {
         TextMessage textMessage = session.createTextMessage(text + i);
         producer.send(textMessage);
      }
   }

   private void sendMessageUsingOpenWire(String queueName) throws Exception {
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      ActiveMQDestination destination = createDestination(session, ActiveMQDestination.QUEUE_TYPE);

      System.out.println("destination: " + destination);
      final ActiveMQMessageProducer producer = (ActiveMQMessageProducer) session.createProducer(destination);

      javax.jms.Message message = session.createMessage();

      message.setBooleanProperty("booleanProperty", false);
      message.setLongProperty("longProperty", 99999L);
      message.setByteProperty("byteProperty", (byte) 5);
      message.setIntProperty("intProperty", 979);
      message.setShortProperty("shortProperty", (short) 1099);
      message.setStringProperty("stringProperty", "HelloMessage");

      producer.send(message);
   }

   private void sendBytesMessageUsingOpenWire(byte[] bytesData) throws Exception {
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      ActiveMQDestination destination = createDestination(session, ActiveMQDestination.QUEUE_TYPE);

      System.out.println("destination: " + destination);
      final ActiveMQMessageProducer producer = (ActiveMQMessageProducer) session.createProducer(destination);

      BytesMessage bytesMessage = session.createBytesMessage();
      bytesMessage.writeBytes(bytesData);
      bytesMessage.writeBoolean(true);
      bytesMessage.writeLong(99999L);
      bytesMessage.writeChar('h');
      bytesMessage.writeInt(987);
      bytesMessage.writeShort((short) 1099);
      bytesMessage.writeUTF("hellobytes");

      producer.send(bytesMessage);
   }

   private void sendStreamMessageUsingOpenWire(String queueName) throws Exception {
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      ActiveMQDestination destination = createDestination(session, ActiveMQDestination.QUEUE_TYPE);

      System.out.println("destination: " + destination);
      final ActiveMQMessageProducer producer = (ActiveMQMessageProducer) session.createProducer(destination);

      StreamMessage streamMessage = session.createStreamMessage();
      streamMessage.writeBoolean(true);
      streamMessage.writeByte((byte) 2);
      streamMessage.writeBytes(new byte[]{6, 7});
      streamMessage.writeChar('b');
      streamMessage.writeDouble(6.5);
      streamMessage.writeFloat((float) 93.9);
      streamMessage.writeInt(7657);
      streamMessage.writeLong(239999L);
      streamMessage.writeShort((short) 34222);
      streamMessage.writeString("hello streammessage");

      producer.send(streamMessage);
   }

   private void sendObjectMessageUsingOpenWire(SimpleSerializable obj) throws Exception {
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      ActiveMQDestination destination = createDestination(session, ActiveMQDestination.QUEUE_TYPE);

      System.out.println("destination: " + destination);
      final ActiveMQMessageProducer producer = (ActiveMQMessageProducer) session.createProducer(destination);

      ObjectMessage objectMessage = session.createObjectMessage(obj);

      producer.send(objectMessage);
   }

   private void sendMapMessageUsingOpenWire() throws Exception {
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      ActiveMQDestination destination = createDestination(session, ActiveMQDestination.QUEUE_TYPE);

      System.out.println("destination: " + destination);
      final ActiveMQMessageProducer producer = (ActiveMQMessageProducer) session.createProducer(destination);

      MapMessage mapMessage = session.createMapMessage();
      mapMessage.setBoolean("aboolean", true);
      mapMessage.setByte("abyte", (byte) 4);
      mapMessage.setBytes("abytes", new byte[]{4, 5});
      mapMessage.setChar("achar", 'a');
      mapMessage.setDouble("adouble", 4.4);
      mapMessage.setFloat("afloat", 4.5f);
      mapMessage.setInt("aint", 40);
      mapMessage.setLong("along", 80L);
      mapMessage.setShort("ashort", (short) 65);
      mapMessage.setString("astring", "hello");

      producer.send(mapMessage);
   }

   private void sendTextMessageUsingOpenWire(String text) throws Exception {
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      ActiveMQDestination destination = createDestination(session, ActiveMQDestination.QUEUE_TYPE);

      final ActiveMQMessageProducer producer = (ActiveMQMessageProducer) session.createProducer(destination);

      TextMessage textMessage = session.createTextMessage(text);

      producer.send(textMessage);
   }

   private static class SimpleSerializable implements Serializable {

      private static final long serialVersionUID = -1034113865185130710L;
      public String objName = "simple-serializable";
      public int intVal = 9999;
      public long longVal = 88888888L;
   }
}
