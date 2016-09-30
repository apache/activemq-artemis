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
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;
import java.nio.charset.StandardCharsets;

import org.apache.activemq.ActiveMQMessageProducer;
import org.apache.activemq.artemis.tests.integration.openwire.BasicOpenWireTest;
import org.apache.activemq.command.ActiveMQDestination;
import org.junit.Before;
import org.junit.Test;

public class CompressedInteropTest extends BasicOpenWireTest {

   private static final String TEXT;

   static {
      StringBuilder builder = new StringBuilder();

      for (int i = 0; i < 20; i++) {
         builder.append("The quick red fox jumped over the lazy brown dog. ");
      }
      TEXT = builder.toString();
   }

   @Before
   @Override
   public void setUp() throws Exception {
      factory.setUseCompression(true);
      super.setUp();
      connection.start();
      assertTrue(connection.isUseCompression());
   }

   @Test
   public void testCoreReceiveOpenWireCompressedMessages() throws Exception {
      //TextMessage
      sendCompressedTextMessageUsingOpenWire();
      receiveTextMessageUsingCore();
      //BytesMessage
      sendCompressedBytesMessageUsingOpenWire();
      receiveBytesMessageUsingCore();
      //MapMessage
      sendCompressedMapMessageUsingOpenWire();
      receiveMapMessageUsingCore();
      //StreamMessage
      sendCompressedStreamMessageUsingOpenWire();
      receiveStreamMessageUsingCore();
      //ObjectMessage
      sendCompressedObjectMessageUsingOpenWire();
      receiveObjectMessageUsingCore();
   }

   private void sendCompressedStreamMessageUsingOpenWire() throws Exception {
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      ActiveMQDestination destination = createDestination(session, ActiveMQDestination.QUEUE_TYPE);

      final ActiveMQMessageProducer producer = (ActiveMQMessageProducer) session.createProducer(destination);

      StreamMessage streamMessage = session.createStreamMessage();

      streamMessage.writeBoolean(true);
      streamMessage.writeByte((byte) 10);
      streamMessage.writeBytes(TEXT.getBytes());
      streamMessage.writeChar('A');
      streamMessage.writeDouble(55.3D);
      streamMessage.writeFloat(79.1F);
      streamMessage.writeInt(37);
      streamMessage.writeLong(56652L);
      streamMessage.writeObject(new String("VVVV"));
      streamMessage.writeShort((short) 333);
      streamMessage.writeString(TEXT);

      producer.send(streamMessage);
   }

   private void receiveStreamMessageUsingCore() throws Exception {
      StreamMessage streamMessage = (StreamMessage) receiveMessageUsingCore();
      boolean booleanVal = streamMessage.readBoolean();
      assertTrue(booleanVal);
      byte byteVal = streamMessage.readByte();
      assertEquals((byte) 10, byteVal);
      byte[] originVal = TEXT.getBytes();
      byte[] bytesVal = new byte[originVal.length];
      streamMessage.readBytes(bytesVal);
      for (int i = 0; i < bytesVal.length; i++) {
         assertTrue(bytesVal[i] == originVal[i]);
      }
      char charVal = streamMessage.readChar();
      assertEquals('A', charVal);
      double doubleVal = streamMessage.readDouble();
      assertEquals(55.3D, doubleVal, 0.1D);
      float floatVal = streamMessage.readFloat();
      assertEquals(79.1F, floatVal, 0.1F);
      int intVal = streamMessage.readInt();
      assertEquals(37, intVal);
      long longVal = streamMessage.readLong();
      assertEquals(56652L, longVal);
      Object objectVal = streamMessage.readObject();
      Object origVal = new String("VVVV");
      assertTrue(objectVal.equals(origVal));
      short shortVal = streamMessage.readShort();
      assertEquals((short) 333, shortVal);
      String strVal = streamMessage.readString();
      assertEquals(TEXT, strVal);
   }

   private void sendCompressedObjectMessageUsingOpenWire() throws Exception {
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      ActiveMQDestination destination = createDestination(session, ActiveMQDestination.QUEUE_TYPE);

      final ActiveMQMessageProducer producer = (ActiveMQMessageProducer) session.createProducer(destination);

      ObjectMessage objectMessage = session.createObjectMessage();
      objectMessage.setObject(TEXT);

      producer.send(objectMessage);
   }

   private void receiveObjectMessageUsingCore() throws Exception {
      ObjectMessage objectMessage = (ObjectMessage) receiveMessageUsingCore();
      Object objectVal = objectMessage.getObject();
      assertEquals(TEXT, objectVal);
   }

   private void sendCompressedMapMessageUsingOpenWire() throws Exception {
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      ActiveMQDestination destination = createDestination(session, ActiveMQDestination.QUEUE_TYPE);

      final ActiveMQMessageProducer producer = (ActiveMQMessageProducer) session.createProducer(destination);

      MapMessage mapMessage = session.createMapMessage();

      mapMessage.setBoolean("boolean-type", true);
      mapMessage.setByte("byte-type", (byte) 10);
      mapMessage.setBytes("bytes-type", TEXT.getBytes());
      mapMessage.setChar("char-type", 'A');
      mapMessage.setDouble("double-type", 55.3D);
      mapMessage.setFloat("float-type", 79.1F);
      mapMessage.setInt("int-type", 37);
      mapMessage.setLong("long-type", 56652L);
      mapMessage.setObject("object-type", new String("VVVV"));
      mapMessage.setShort("short-type", (short) 333);
      mapMessage.setString("string-type", TEXT);

      producer.send(mapMessage);
   }

   private void receiveMapMessageUsingCore() throws Exception {
      MapMessage mapMessage = (MapMessage) receiveMessageUsingCore();

      boolean booleanVal = mapMessage.getBoolean("boolean-type");
      assertTrue(booleanVal);
      byte byteVal = mapMessage.getByte("byte-type");
      assertEquals((byte) 10, byteVal);
      byte[] bytesVal = mapMessage.getBytes("bytes-type");
      byte[] originVal = TEXT.getBytes();
      assertEquals(originVal.length, bytesVal.length);
      for (int i = 0; i < bytesVal.length; i++) {
         assertTrue(bytesVal[i] == originVal[i]);
      }
      char charVal = mapMessage.getChar("char-type");
      assertEquals('A', charVal);
      double doubleVal = mapMessage.getDouble("double-type");
      assertEquals(55.3D, doubleVal, 0.1D);
      float floatVal = mapMessage.getFloat("float-type");
      assertEquals(79.1F, floatVal, 0.1F);
      int intVal = mapMessage.getInt("int-type");
      assertEquals(37, intVal);
      long longVal = mapMessage.getLong("long-type");
      assertEquals(56652L, longVal);
      Object objectVal = mapMessage.getObject("object-type");
      Object origVal = new String("VVVV");
      assertTrue(objectVal.equals(origVal));
      short shortVal = mapMessage.getShort("short-type");
      assertEquals((short) 333, shortVal);
      String strVal = mapMessage.getString("string-type");
      assertEquals(TEXT, strVal);
   }

   private void sendCompressedBytesMessageUsingOpenWire() throws Exception {
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      ActiveMQDestination destination = createDestination(session, ActiveMQDestination.QUEUE_TYPE);

      final ActiveMQMessageProducer producer = (ActiveMQMessageProducer) session.createProducer(destination);

      BytesMessage bytesMessage = session.createBytesMessage();
      bytesMessage.writeBytes(TEXT.getBytes());

      producer.send(bytesMessage);
   }

   private void receiveBytesMessageUsingCore() throws Exception {
      BytesMessage bytesMessage = (BytesMessage) receiveMessageUsingCore();

      byte[] bytes = new byte[TEXT.getBytes(StandardCharsets.UTF_8).length];
      bytesMessage.readBytes(bytes);
      assertTrue(bytesMessage.readBytes(new byte[255]) == -1);

      String rcvString = new String(bytes, StandardCharsets.UTF_8);
      assertEquals(TEXT, rcvString);
   }

   private void receiveTextMessageUsingCore() throws Exception {
      TextMessage txtMessage = (TextMessage) receiveMessageUsingCore();
      assertEquals(TEXT, txtMessage.getText());
   }

   private Message receiveMessageUsingCore() throws Exception {
      Connection jmsConn = null;
      Message message = null;
      try {
         jmsConn = coreCf.createConnection();
         jmsConn.start();

         Session session = jmsConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue queue = session.createQueue(this.queueName);
         MessageConsumer coreConsumer = session.createConsumer(queue);

         message = coreConsumer.receive(5000);
      } finally {
         if (jmsConn != null) {
            jmsConn.close();
         }
      }
      return message;
   }

   private void sendCompressedTextMessageUsingOpenWire() throws Exception {
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      ActiveMQDestination destination = createDestination(session, ActiveMQDestination.QUEUE_TYPE);

      final ActiveMQMessageProducer producer = (ActiveMQMessageProducer) session.createProducer(destination);

      TextMessage textMessage = session.createTextMessage(TEXT);

      producer.send(textMessage);
   }

}
