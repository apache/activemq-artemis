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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSBytesMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSMapMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSStreamMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSTextMessage;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.message.Message;
import org.apache.qpid.proton.message.impl.MessageImpl;
import org.junit.Assert;
import org.junit.Test;


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

      AMQPMessage encodedMessage = new AMQPMessage(message);

      ICoreMessage serverMessage = encodedMessage.toCore();

      verifyProperties(ServerJMSMessage.wrapCoreMessage(serverMessage));

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

      AMQPMessage encodedMessage = new AMQPMessage(message);

      ICoreMessage serverMessage = encodedMessage.toCore();

      ServerJMSBytesMessage bytesMessage = (ServerJMSBytesMessage) ServerJMSMessage.wrapCoreMessage(serverMessage);

      verifyProperties(bytesMessage);

      assertEquals(bodyBytes.length, bytesMessage.getBodyLength());

      byte[] newBodyBytes = new byte[4];

      bytesMessage.readBytes(newBodyBytes);

      Assert.assertArrayEquals(bodyBytes, newBodyBytes);

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

      AMQPMessage encodedMessage = new AMQPMessage(message);

      ICoreMessage serverMessage = encodedMessage.toCore();
      serverMessage.getReadOnlyBodyBuffer();

      ServerJMSMapMessage mapMessage = (ServerJMSMapMessage) ServerJMSMessage.wrapCoreMessage(serverMessage);
      mapMessage.decode();

      verifyProperties(mapMessage);

      Assert.assertEquals(1, mapMessage.getInt("someint"));
      Assert.assertEquals("value", mapMessage.getString("somestr"));

      AMQPMessage newAMQP = CoreAmqpConverter.fromCore(mapMessage.getInnerMessage());
      System.out.println(newAMQP.getProtonMessage().getBody());
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

      AMQPMessage encodedMessage = new AMQPMessage(message);

      ICoreMessage serverMessage = encodedMessage.toCore();

      ServerJMSStreamMessage streamMessage = (ServerJMSStreamMessage) ServerJMSMessage.wrapCoreMessage(serverMessage);

      verifyProperties(streamMessage);

      streamMessage.reset();

      assertEquals(10, streamMessage.readInt());
      assertEquals("10", streamMessage.readString());
   }

   @Test
   public void testSimpleConversionText() throws Exception {
      Map<String, Object> mapprop = createPropertiesMap();
      ApplicationProperties properties = new ApplicationProperties(mapprop);
      MessageImpl message = (MessageImpl) Message.Factory.create();
      message.setApplicationProperties(properties);

      String text = "someText";
      message.setBody(new AmqpValue(text));

      AMQPMessage encodedMessage = new AMQPMessage(message);

      ICoreMessage serverMessage = encodedMessage.toCore();

      ServerJMSTextMessage textMessage = (ServerJMSTextMessage) ServerJMSMessage.wrapCoreMessage(serverMessage);
      textMessage.decode();

      verifyProperties(textMessage);

      Assert.assertEquals(text, textMessage.getText());

   }

}
