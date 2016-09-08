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
package org.apache.activemq.artemis.core.protocol.proton;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.core.protocol.proton.converter.jms.ServerJMSObjectMessage;
import org.apache.activemq.artemis.core.protocol.proton.converter.message.EncodedMessage;
import org.apache.activemq.artemis.core.server.impl.ServerMessageImpl;
import org.apache.blacklist.ABadClass;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.message.Message;
import org.apache.qpid.proton.message.ProtonJMessage;
import org.apache.qpid.proton.message.impl.MessageImpl;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.journal.EncodingSupport;
import org.apache.activemq.artemis.core.protocol.proton.converter.ProtonMessageConverter;
import org.apache.activemq.artemis.core.protocol.proton.converter.jms.ServerJMSBytesMessage;
import org.apache.activemq.artemis.core.protocol.proton.converter.jms.ServerJMSMapMessage;
import org.apache.activemq.artemis.core.protocol.proton.converter.jms.ServerJMSMessage;
import org.apache.activemq.artemis.core.protocol.proton.converter.jms.ServerJMSStreamMessage;
import org.apache.activemq.artemis.core.protocol.proton.converter.jms.ServerJMSTextMessage;
import org.apache.activemq.artemis.core.server.ServerMessage;
import org.apache.activemq.artemis.utils.SimpleIDGenerator;
import org.junit.Assert;
import org.junit.Test;
import org.proton.plug.util.NettyWritable;

public class TestConversions extends Assert {

   @Test
   public void testObjectMessageWhiteList() throws Exception {
      Map<String, Object> mapprop = createPropertiesMap();
      ApplicationProperties properties = new ApplicationProperties(mapprop);
      MessageImpl message = (MessageImpl) Message.Factory.create();
      message.setApplicationProperties(properties);

      byte[] bodyBytes = new byte[4];

      for (int i = 0; i < bodyBytes.length; i++) {
         bodyBytes[i] = (byte) 0xff;
      }

      message.setBody(new AmqpValue(new Boolean(true)));

      EncodedMessage encodedMessage = encodeMessage(message);

      ProtonMessageConverter converter = new ProtonMessageConverter(new SimpleIDGenerator(0));
      ServerJMSObjectMessage serverMessage = (ServerJMSObjectMessage) converter.inboundJMSType(encodedMessage);

      verifyProperties(serverMessage);

      assertEquals(true, serverMessage.getObject());

      Object obj = converter.outbound((ServerMessage) serverMessage.getInnerMessage(), 0);

      AmqpValue value = (AmqpValue) ((Message)obj).getBody();
      assertEquals(value.getValue(), true);

   }

   @Test
   public void testObjectMessageNotOnWhiteList() throws Exception {


      ProtonMessageConverter converter = new ProtonMessageConverter(new SimpleIDGenerator(0));
      ServerMessageImpl message = new ServerMessageImpl(1, 1024);
      message.setType((byte) 2);
      ServerJMSObjectMessage serverMessage = new ServerJMSObjectMessage(message, 1024);
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      ObjectOutputStream ois = new ObjectOutputStream(out);
      ois.writeObject(new ABadClass());
      serverMessage.getInnerMessage().getBodyBuffer().writeBytes(out.toByteArray());

      try {
         converter.outbound((ServerMessage) serverMessage.getInnerMessage(), 0);
         fail("should throw ClassNotFoundException");
      }
      catch (ClassNotFoundException e) {
         //ignore
      }
   }


   @Test
   public void testSimpleConversionBytes() throws Exception {
      Map<String, Object> mapprop = createPropertiesMap();
      ApplicationProperties properties = new ApplicationProperties(mapprop);
      MessageImpl message = (MessageImpl) Message.Factory.create();
      message.setApplicationProperties(properties);

      byte[] bodyBytes = new byte[4];

      for (int i = 0; i < bodyBytes.length; i++) {
         bodyBytes[i] = (byte) 0xff;
      }

      message.setBody(new Data(new Binary(bodyBytes)));

      EncodedMessage encodedMessage = encodeMessage(message);

      ProtonMessageConverter converter = new ProtonMessageConverter(new SimpleIDGenerator(0));
      ServerJMSBytesMessage serverMessage = (ServerJMSBytesMessage) converter.inboundJMSType(encodedMessage);

      verifyProperties(serverMessage);

      assertEquals(bodyBytes.length, serverMessage.getBodyLength());

      byte[] newBodyBytes = new byte[4];

      serverMessage.readBytes(newBodyBytes);

      Assert.assertArrayEquals(bodyBytes, newBodyBytes);

      Object obj = converter.outbound((ServerMessage) serverMessage.getInnerMessage(), 0);

      System.out.println("output = " + obj);

   }

   private void verifyProperties(javax.jms.Message message) throws Exception {
      assertEquals(true, message.getBooleanProperty("true"));
      assertEquals(false, message.getBooleanProperty("false"));
      assertEquals("bar", message.getStringProperty("foo"));
   }

   private Map<String, Object> createPropertiesMap() {
      Map<String, Object> mapprop = new HashMap<>();

      mapprop.put("true", Boolean.TRUE);
      mapprop.put("false", Boolean.FALSE);
      mapprop.put("foo", "bar");
      return mapprop;
   }

   @Test
   public void testSimpleConversionMap() throws Exception {
      Map<String, Object> mapprop = createPropertiesMap();
      ApplicationProperties properties = new ApplicationProperties(mapprop);
      MessageImpl message = (MessageImpl) Message.Factory.create();
      message.setApplicationProperties(properties);

      Map<String, Object> mapValues = new HashMap<>();
      mapValues.put("somestr", "value");
      mapValues.put("someint", Integer.valueOf(1));

      message.setBody(new AmqpValue(mapValues));

      EncodedMessage encodedMessage = encodeMessage(message);

      ProtonMessageConverter converter = new ProtonMessageConverter(new SimpleIDGenerator(0));
      ServerJMSMapMessage serverMessage = (ServerJMSMapMessage) converter.inboundJMSType(encodedMessage);

      verifyProperties(serverMessage);

      Assert.assertEquals(1, serverMessage.getInt("someint"));
      Assert.assertEquals("value", serverMessage.getString("somestr"));

      Object obj = converter.outbound((ServerMessage) serverMessage.getInnerMessage(), 0);

      reEncodeMsg(obj);

      MessageImpl outMessage = (MessageImpl) obj;
      AmqpValue value = (AmqpValue) outMessage.getBody();
      Map mapoutput = (Map) value.getValue();

      assertEquals(Integer.valueOf(1), mapoutput.get("someint"));

      System.out.println("output = " + obj);

   }

   @Test
   public void testSimpleConversionStream() throws Exception {
      Map<String, Object> mapprop = createPropertiesMap();
      ApplicationProperties properties = new ApplicationProperties(mapprop);
      MessageImpl message = (MessageImpl) Message.Factory.create();
      message.setApplicationProperties(properties);

      List<Object> objects = new LinkedList<>();
      objects.add(new Integer(10));
      objects.add("10");

      message.setBody(new AmqpSequence(objects));

      EncodedMessage encodedMessage = encodeMessage(message);

      ProtonMessageConverter converter = new ProtonMessageConverter(new SimpleIDGenerator(0));
      ServerJMSStreamMessage serverMessage = (ServerJMSStreamMessage) converter.inboundJMSType(encodedMessage);

      simulatePersistence(serverMessage);

      verifyProperties(serverMessage);

      serverMessage.reset();

      assertEquals(10, serverMessage.readInt());
      assertEquals("10", serverMessage.readString());

      Object obj = converter.outbound((ServerMessage) serverMessage.getInnerMessage(), 0);

      reEncodeMsg(obj);

      MessageImpl outMessage = (MessageImpl) obj;
      List list = ((AmqpSequence) outMessage.getBody()).getValue();
      Assert.assertEquals(Integer.valueOf(10), list.get(0));
      Assert.assertEquals("10", list.get(1));

   }

   @Test
   public void testSimpleConversionText() throws Exception {
      Map<String, Object> mapprop = createPropertiesMap();
      ApplicationProperties properties = new ApplicationProperties(mapprop);
      MessageImpl message = (MessageImpl) Message.Factory.create();
      message.setApplicationProperties(properties);

      String text = "someText";
      message.setBody(new AmqpValue(text));

      EncodedMessage encodedMessage = encodeMessage(message);

      ProtonMessageConverter converter = new ProtonMessageConverter(new SimpleIDGenerator(0));
      ServerJMSTextMessage serverMessage = (ServerJMSTextMessage) converter.inboundJMSType(encodedMessage);

      simulatePersistence(serverMessage);

      verifyProperties(serverMessage);

      Assert.assertEquals(text, serverMessage.getText());

      Object obj = converter.outbound((ServerMessage) serverMessage.getInnerMessage(), 0);

      reEncodeMsg(obj);

      MessageImpl outMessage = (MessageImpl) obj;
      AmqpValue value = (AmqpValue) outMessage.getBody();
      String textValue = (String) value.getValue();

      Assert.assertEquals(text, textValue);

      System.out.println("output = " + obj);

   }

   private void simulatePersistence(ServerJMSMessage serverMessage) {
      serverMessage.getInnerMessage().setAddress(new SimpleString("jms.queue.SomeAddress"));
      // This is just to simulate what would happen during the persistence of the message
      // We need to still be able to recover the message when we read it back
      ((EncodingSupport) serverMessage.getInnerMessage()).encode(new EmptyBuffer());
   }

   private ProtonJMessage reEncodeMsg(Object obj) {
      ProtonJMessage objOut = (ProtonJMessage) obj;

      ByteBuf nettyBuffer = PooledByteBufAllocator.DEFAULT.heapBuffer(1024);

      objOut.encode(new NettyWritable(nettyBuffer));
      return objOut;
   }

   private EncodedMessage encodeMessage(MessageImpl message) {
      ByteBuf buf = PooledByteBufAllocator.DEFAULT.heapBuffer(1024 * 1024);
      message.encode(new NettyWritable(buf));
      byte[] bytesConvert = new byte[buf.writerIndex()];
      buf.readBytes(bytesConvert);
      return new EncodedMessage(0, bytesConvert, 0, bytesConvert.length);
   }

   class EmptyBuffer implements ActiveMQBuffer {

      @Override
      public ByteBuf byteBuf() {
         return null;
      }

      @Override
      public int capacity() {
         return 0;
      }

      @Override
      public int readerIndex() {
         return 0;
      }

      @Override
      public void readerIndex(int readerIndex) {

      }

      @Override
      public int writerIndex() {
         return 0;
      }

      @Override
      public void writerIndex(int writerIndex) {

      }

      @Override
      public void setIndex(int readerIndex, int writerIndex) {

      }

      @Override
      public int readableBytes() {
         return 0;
      }

      @Override
      public int writableBytes() {
         return 0;
      }

      @Override
      public boolean readable() {
         return false;
      }

      @Override
      public boolean writable() {
         return false;
      }

      @Override
      public void clear() {

      }

      @Override
      public void markReaderIndex() {

      }

      @Override
      public void resetReaderIndex() {

      }

      @Override
      public void markWriterIndex() {

      }

      @Override
      public void resetWriterIndex() {

      }

      @Override
      public void discardReadBytes() {

      }

      @Override
      public byte getByte(int index) {
         return 0;
      }

      @Override
      public short getUnsignedByte(int index) {
         return 0;
      }

      @Override
      public short getShort(int index) {
         return 0;
      }

      @Override
      public int getUnsignedShort(int index) {
         return 0;
      }

      @Override
      public int getInt(int index) {
         return 0;
      }

      @Override
      public long getUnsignedInt(int index) {
         return 0;
      }

      @Override
      public long getLong(int index) {
         return 0;
      }

      @Override
      public void getBytes(int index, ActiveMQBuffer dst) {

      }

      @Override
      public void getBytes(int index, ActiveMQBuffer dst, int length) {

      }

      @Override
      public void getBytes(int index, ActiveMQBuffer dst, int dstIndex, int length) {

      }

      @Override
      public void getBytes(int index, byte[] dst) {

      }

      @Override
      public void getBytes(int index, byte[] dst, int dstIndex, int length) {

      }

      @Override
      public void getBytes(int index, ByteBuffer dst) {

      }

      @Override
      public char getChar(int index) {
         return 0;
      }

      @Override
      public float getFloat(int index) {
         return 0;
      }

      @Override
      public double getDouble(int index) {
         return 0;
      }

      @Override
      public void setByte(int index, byte value) {

      }

      @Override
      public void setShort(int index, short value) {

      }

      @Override
      public void setInt(int index, int value) {

      }

      @Override
      public void setLong(int index, long value) {

      }

      @Override
      public void setBytes(int index, ActiveMQBuffer src) {

      }

      @Override
      public void setBytes(int index, ActiveMQBuffer src, int length) {

      }

      @Override
      public void setBytes(int index, ActiveMQBuffer src, int srcIndex, int length) {

      }

      @Override
      public void setBytes(int index, byte[] src) {

      }

      @Override
      public void setBytes(int index, byte[] src, int srcIndex, int length) {

      }

      @Override
      public void setBytes(int index, ByteBuffer src) {

      }

      @Override
      public void setChar(int index, char value) {

      }

      @Override
      public void setFloat(int index, float value) {

      }

      @Override
      public void setDouble(int index, double value) {

      }

      @Override
      public byte readByte() {
         return 0;
      }

      @Override
      public int readUnsignedByte() {
         return 0;
      }

      @Override
      public short readShort() {
         return 0;
      }

      @Override
      public int readUnsignedShort() {
         return 0;
      }

      @Override
      public int readInt() {
         return 0;
      }

      @Override
      public long readUnsignedInt() {
         return 0;
      }

      @Override
      public long readLong() {
         return 0;
      }

      @Override
      public char readChar() {
         return 0;
      }

      @Override
      public float readFloat() {
         return 0;
      }

      @Override
      public double readDouble() {
         return 0;
      }

      @Override
      public boolean readBoolean() {
         return false;
      }

      @Override
      public SimpleString readNullableSimpleString() {
         return null;
      }

      @Override
      public String readNullableString() {
         return null;
      }

      @Override
      public SimpleString readSimpleString() {
         return null;
      }

      @Override
      public String readString() {
         return null;
      }

      @Override
      public String readUTF() {
         return null;
      }

      @Override
      public ActiveMQBuffer readBytes(int length) {
         return null;
      }

      @Override
      public ActiveMQBuffer readSlice(int length) {
         return null;
      }

      @Override
      public void readBytes(ActiveMQBuffer dst) {

      }

      @Override
      public void readBytes(ActiveMQBuffer dst, int length) {

      }

      @Override
      public void readBytes(ActiveMQBuffer dst, int dstIndex, int length) {

      }

      @Override
      public void readBytes(byte[] dst) {

      }

      @Override
      public void readBytes(byte[] dst, int dstIndex, int length) {

      }

      @Override
      public void readBytes(ByteBuffer dst) {

      }

      @Override
      public int skipBytes(int length) {
         return length;
      }

      @Override
      public void writeByte(byte value) {

      }

      @Override
      public void writeShort(short value) {

      }

      @Override
      public void writeInt(int value) {

      }

      @Override
      public void writeLong(long value) {

      }

      @Override
      public void writeChar(char chr) {

      }

      @Override
      public void writeFloat(float value) {

      }

      @Override
      public void writeDouble(double value) {

      }

      @Override
      public void writeBoolean(boolean val) {

      }

      @Override
      public void writeNullableSimpleString(SimpleString val) {

      }

      @Override
      public void writeNullableString(String val) {

      }

      @Override
      public void writeSimpleString(SimpleString val) {

      }

      @Override
      public void writeString(String val) {

      }

      @Override
      public void writeUTF(String utf) {

      }

      @Override
      public void writeBytes(ActiveMQBuffer src, int length) {

      }

      @Override
      public void writeBytes(ActiveMQBuffer src, int srcIndex, int length) {

      }

      @Override
      public void writeBytes(byte[] src) {

      }

      @Override
      public void writeBytes(byte[] src, int srcIndex, int length) {

      }

      @Override
      public void writeBytes(ByteBuffer src) {

      }

      @Override
      public void readFully(byte[] b) throws IOException {
      }

      @Override
      public void readFully(byte[] b, int off, int len) throws IOException {
      }

      @Override
      public String readLine() throws IOException {
         return null;
      }

      @Override
      public ActiveMQBuffer copy() {
         return null;
      }

      @Override
      public ActiveMQBuffer copy(int index, int length) {
         return null;
      }

      @Override
      public ActiveMQBuffer slice() {
         return null;
      }

      @Override
      public ActiveMQBuffer slice(int index, int length) {
         return null;
      }

      @Override
      public ActiveMQBuffer duplicate() {
         return null;
      }

      @Override
      public ByteBuffer toByteBuffer() {
         return null;
      }

      @Override
      public ByteBuffer toByteBuffer(int index, int length) {
         return null;
      }
   }
}
