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
package org.apache.activemq.artemis.protocol.amqp.converter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSBytesMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSMapMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSStreamMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSTextMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.message.EncodedMessage;
import org.apache.activemq.artemis.protocol.amqp.util.NettyWritable;
import org.apache.activemq.artemis.utils.SimpleIDGenerator;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.message.Message;
import org.apache.qpid.proton.message.ProtonJMessage;
import org.apache.qpid.proton.message.impl.MessageImpl;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.activemq.artemis.api.core.Message.BYTES_TYPE;
import static org.apache.activemq.artemis.api.core.Message.MAP_TYPE;
import static org.apache.activemq.artemis.api.core.Message.STREAM_TYPE;
import static org.apache.activemq.artemis.api.core.Message.TEXT_TYPE;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.wrapMessage;

public class TestConversions extends Assert {

   @Test
   public void testAmqpValueOfBooleanIsPassedThrough() throws Exception {
      Map<String, Object> mapprop = createPropertiesMap();
      ApplicationProperties properties = new ApplicationProperties(mapprop);
      MessageImpl message = (MessageImpl) Message.Factory.create();
      message.setApplicationProperties(properties);

      byte[] bodyBytes = new byte[4];

      for (int i = 0; i < bodyBytes.length; i++) {
         bodyBytes[i] = (byte) 0xff;
      }

      message.setBody(new AmqpValue(new Boolean(true)));

      AMQPMessage encodedMessage = new AMQPMessage(message, null);

      ProtonMessageConverter converter = new ProtonMessageConverter(new SimpleIDGenerator(0));
      org.apache.activemq.artemis.api.core.Message serverMessage = converter.inbound(encodedMessage);

      verifyProperties(new ServerJMSMessage(serverMessage, 0));

      EncodedMessage encoded = (EncodedMessage) converter.outbound(serverMessage, 0);
      Message amqpMessage = encoded.decode();

      AmqpValue value = (AmqpValue) amqpMessage.getBody();
      assertEquals(value.getValue(), true);
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

      AMQPMessage encodedMessage = new AMQPMessage(message, null);

      ProtonMessageConverter converter = new ProtonMessageConverter(new SimpleIDGenerator(0));
      org.apache.activemq.artemis.api.core.Message serverMessage = converter.inbound(encodedMessage);

      ServerJMSBytesMessage bytesMessage = (ServerJMSBytesMessage) wrapMessage(BYTES_TYPE, serverMessage, 0);

      verifyProperties(bytesMessage);

      assertEquals(bodyBytes.length, bytesMessage.getBodyLength());

      byte[] newBodyBytes = new byte[4];

      bytesMessage.readBytes(newBodyBytes);

      Assert.assertArrayEquals(bodyBytes, newBodyBytes);

      Object obj = converter.outbound(serverMessage, 0);

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

      AMQPMessage encodedMessage = new AMQPMessage(message, null);

      ProtonMessageConverter converter = new ProtonMessageConverter(new SimpleIDGenerator(0));
      org.apache.activemq.artemis.api.core.Message serverMessage = converter.inbound(encodedMessage);

      ServerJMSMapMessage mapMessage = (ServerJMSMapMessage) wrapMessage(MAP_TYPE, serverMessage, 0);
      mapMessage.decode();

      verifyProperties(mapMessage);

      Assert.assertEquals(1, mapMessage.getInt("someint"));
      Assert.assertEquals("value", mapMessage.getString("somestr"));

      EncodedMessage encoded = (EncodedMessage) converter.outbound(serverMessage, 0);
      Message amqpMessage = encoded.decode();

      AmqpValue value = (AmqpValue) amqpMessage.getBody();
      Map<?, ?> mapoutput = (Map<?, ?>) value.getValue();

      assertEquals(Integer.valueOf(1), mapoutput.get("someint"));

      System.out.println("output = " + amqpMessage);
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

      AMQPMessage encodedMessage = new AMQPMessage(message, null);

      ProtonMessageConverter converter = new ProtonMessageConverter(new SimpleIDGenerator(0));
      org.apache.activemq.artemis.api.core.Message serverMessage = converter.inbound(encodedMessage);

      ServerJMSStreamMessage streamMessage = (ServerJMSStreamMessage) wrapMessage(STREAM_TYPE, serverMessage, 0);

      verifyProperties(streamMessage);

      streamMessage.reset();

      assertEquals(10, streamMessage.readInt());
      assertEquals("10", streamMessage.readString());

      EncodedMessage encoded = (EncodedMessage) converter.outbound(serverMessage, 0);
      Message amqpMessage = encoded.decode();

      List<?> list = ((AmqpSequence) amqpMessage.getBody()).getValue();
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

      AMQPMessage encodedMessage = new AMQPMessage(message, null);

      ProtonMessageConverter converter = new ProtonMessageConverter(new SimpleIDGenerator(0));
      org.apache.activemq.artemis.api.core.Message serverMessage = converter.inbound(encodedMessage);

      ServerJMSTextMessage textMessage = (ServerJMSTextMessage) wrapMessage(TEXT_TYPE, serverMessage, 0);
      textMessage.decode();

      verifyProperties(textMessage);

      Assert.assertEquals(text, textMessage.getText());

      EncodedMessage encoded = (EncodedMessage) converter.outbound(serverMessage, 0);
      Message amqpMessage = encoded.decode();

      AmqpValue value = (AmqpValue) amqpMessage.getBody();
      String textValue = (String) value.getValue();

      Assert.assertEquals(text, textValue);

      System.out.println("output = " + amqpMessage);
   }

   private ProtonJMessage reEncodeMsg(Object obj) {
      ProtonJMessage objOut = (ProtonJMessage) obj;

      ByteBuf nettyBuffer = PooledByteBufAllocator.DEFAULT.heapBuffer(1024);

      objOut.encode(new NettyWritable(nettyBuffer));
      return objOut;
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

      @Override
      public void release() {
         //no-op
      }

      @Override
      public void writeBytes(ByteBuf src, int srcIndex, int length) {

      }
   }
}
