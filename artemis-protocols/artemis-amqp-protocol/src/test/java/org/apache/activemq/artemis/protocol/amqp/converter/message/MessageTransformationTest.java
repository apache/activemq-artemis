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
package org.apache.activemq.artemis.protocol.amqp.converter.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.AMQPConverter;
import org.apache.activemq.artemis.protocol.amqp.util.NettyReadable;
import org.apache.activemq.artemis.protocol.amqp.util.NettyWritable;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.message.impl.MessageImpl;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import io.netty.buffer.Unpooled;

/**
 * Tests some basic encode / decode functionality on the transformers.
 */
public class MessageTransformationTest {

   @Rule
   public TestName test = new TestName();

   @Test
   public void testBodyOnlyEncodeDecode() throws Exception {
      MessageImpl incomingMessage = (MessageImpl) Proton.message();

      incomingMessage.setBody(new AmqpValue("String payload for AMQP message conversion performance testing."));

      ICoreMessage core = encodeAndCreateAMQPMessage(incomingMessage).toCore();
      AMQPMessage outboudMessage = AMQPConverter.getInstance().fromCore(core);

      assertNull(outboudMessage.getHeader());

      Section body = outboudMessage.getBody();
      assertNotNull(body);
      assertTrue(body instanceof AmqpValue);
      assertTrue(((AmqpValue) body).getValue() instanceof String);
   }

   @Test
   public void testPropertiesButNoHeadersEncodeDecode() throws Exception {
      MessageImpl incomingMessage = (MessageImpl) Proton.message();

      incomingMessage.setBody(new AmqpValue("String payload for AMQP message conversion performance testing."));
      incomingMessage.setMessageId("ID:SomeQualifier:0:0:1");

      ICoreMessage core = encodeAndCreateAMQPMessage(incomingMessage).toCore();
      AMQPMessage outboudMessage = AMQPConverter.getInstance().fromCore(core);

      assertNull(outboudMessage.getHeader());
      assertNotNull(outboudMessage.getProperties());
   }

   @Test
   public void testHeaderButNoPropertiesEncodeDecode() throws Exception {
      MessageImpl incomingMessage = (MessageImpl) Proton.message();

      incomingMessage.setBody(new AmqpValue("String payload for AMQP message conversion performance testing."));
      incomingMessage.setDurable(true);

      ICoreMessage core = encodeAndCreateAMQPMessage(incomingMessage).toCore();
      AMQPMessage outboudMessage = AMQPConverter.getInstance().fromCore(core);

      assertNotNull(outboudMessage.getHeader());

      Section body = outboudMessage.getBody();
      assertNotNull(body);
      assertTrue(body instanceof AmqpValue);
      assertTrue(((AmqpValue) body).getValue() instanceof String);
   }

   @Test
   public void testComplexQpidJMSMessageEncodeDecode() throws Exception {
      Map<String, Object> applicationProperties = new HashMap<>();
      Map<Symbol, Object> messageAnnotations = new HashMap<>();

      applicationProperties.put("property-1", "string-1");
      applicationProperties.put("property-2", 512);
      applicationProperties.put("property-3", true);
      applicationProperties.put("property-4", "string-2");
      applicationProperties.put("property-5", 512);
      applicationProperties.put("property-6", true);
      applicationProperties.put("property-7", "string-3");
      applicationProperties.put("property-8", 512);
      applicationProperties.put("property-9", true);

      messageAnnotations.put(Symbol.valueOf("x-opt-jms-msg-type"), 0);
      messageAnnotations.put(Symbol.valueOf("x-opt-jms-dest"), 0);
      messageAnnotations.put(Symbol.valueOf("x-opt-jms-reply-to"), 0);
      messageAnnotations.put(Symbol.valueOf("x-opt-delivery-delay"), 2000);

      MessageImpl message = (MessageImpl) Proton.message();

      // Header Values
      message.setPriority((short) 9);
      message.setDurable(true);
      message.setDeliveryCount(2);
      message.setTtl(5000);

      // Properties
      message.setMessageId("ID:SomeQualifier:0:0:1");
      message.setGroupId("Group-ID-1");
      message.setGroupSequence(15);
      message.setAddress("queue://test-queue");
      message.setReplyTo("queue://reply-queue");
      message.setCreationTime(System.currentTimeMillis());
      message.setContentType("text/plain");
      message.setCorrelationId("ID:SomeQualifier:0:7:9");
      message.setUserId("username".getBytes(StandardCharsets.UTF_8));

      // Application Properties / Message Annotations / Body
      message.setApplicationProperties(new ApplicationProperties(applicationProperties));
      message.setMessageAnnotations(new MessageAnnotations(messageAnnotations));
      message.setBody(new AmqpValue("String payload for AMQP message conversion performance testing."));

      ICoreMessage core = encodeAndCreateAMQPMessage(message).toCore();
      AMQPMessage outboudMessage = AMQPConverter.getInstance().fromCore(core);

      assertEquals(10, outboudMessage.getApplicationProperties().getValue().size());
      assertEquals(4, outboudMessage.getMessageAnnotations().getValue().size());
   }

   private AMQPMessage encodeAndCreateAMQPMessage(MessageImpl message) {
      NettyWritable encoded = new NettyWritable(Unpooled.buffer(1024));
      message.encode(encoded);

      NettyReadable readable = new NettyReadable(encoded.getByteBuf());

      return new AMQPMessage(AMQPMessage.DEFAULT_MESSAGE_FORMAT, readable, null, null);
   }
}
