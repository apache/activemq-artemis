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

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.core.server.ServerMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.ProtonMessageConverter;
import org.apache.activemq.artemis.protocol.amqp.util.NettyWritable;
import org.apache.activemq.artemis.utils.IDGenerator;
import org.apache.activemq.artemis.utils.SimpleIDGenerator;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.message.Message;
import org.apache.qpid.proton.message.ProtonJMessage;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Some simple performance tests for the Message Transformers.
 */
@Ignore("Useful for profiling but slow and not meant as a unit test")
public class JMSTransformationSpeedComparisonTest {

   @Rule
   public TestName test = new TestName();

   private IDGenerator idGenerator;
   private ProtonMessageConverter converter;

   private final int WARM_CYCLES = 1000;
   private final int PROFILE_CYCLES = 1000000;

   @Before
   public void setUp() {
      idGenerator = new SimpleIDGenerator(0);
      converter = new ProtonMessageConverter(idGenerator);
   }

   @Test
   public void testBodyOnlyMessage() throws Exception {

      Message message = Proton.message();
      message.setBody(new AmqpValue("String payload for AMQP message conversion performance testing."));
      EncodedMessage encoded = encode(message);

      // Warm up
      for (int i = 0; i < WARM_CYCLES; ++i) {
         ServerMessage intermediate = converter.inbound(encoded);
         encode(converter.outbound(intermediate, 1));
      }

      long totalDuration = 0;

      long startTime = System.nanoTime();
      for (int i = 0; i < PROFILE_CYCLES; ++i) {
         ServerMessage intermediate = converter.inbound(encoded);
         encode(converter.outbound(intermediate, 1));
      }
      totalDuration += System.nanoTime() - startTime;

      LOG_RESULTS(totalDuration);
   }

   @Test
   public void testMessageWithNoPropertiesOrAnnotations() throws Exception {

      Message message = Proton.message();

      message.setAddress("queue://test-queue");
      message.setDeliveryCount(1);
      message.setCreationTime(System.currentTimeMillis());
      message.setContentType("text/plain");
      message.setBody(new AmqpValue("String payload for AMQP message conversion performance testing."));

      EncodedMessage encoded = encode(message);

      // Warm up
      for (int i = 0; i < WARM_CYCLES; ++i) {
         ServerMessage intermediate = converter.inbound(encoded);
         encode(converter.outbound(intermediate, 1));
      }

      long totalDuration = 0;

      long startTime = System.nanoTime();
      for (int i = 0; i < PROFILE_CYCLES; ++i) {
         ServerMessage intermediate = converter.inbound(encoded);
         encode(converter.outbound(intermediate, 1));
      }
      totalDuration += System.nanoTime() - startTime;

      LOG_RESULTS(totalDuration);
   }

   @Test
   public void testTypicalQpidJMSMessage() throws Exception {

      EncodedMessage encoded = encode(createTypicalQpidJMSMessage());

      // Warm up
      for (int i = 0; i < WARM_CYCLES; ++i) {
         ServerMessage intermediate = converter.inbound(encoded);
         encode(converter.outbound(intermediate, 1));
      }

      long totalDuration = 0;

      long startTime = System.nanoTime();
      for (int i = 0; i < PROFILE_CYCLES; ++i) {
         ServerMessage intermediate = converter.inbound(encoded);
         encode(converter.outbound(intermediate, 1));
      }
      totalDuration += System.nanoTime() - startTime;

      LOG_RESULTS(totalDuration);
   }

   @Test
   public void testComplexQpidJMSMessage() throws Exception {

      EncodedMessage encoded = encode(createComplexQpidJMSMessage());

      // Warm up
      for (int i = 0; i < WARM_CYCLES; ++i) {
         ServerMessage intermediate = converter.inbound(encoded);
         encode(converter.outbound(intermediate, 1));
      }

      long totalDuration = 0;

      long startTime = System.nanoTime();
      for (int i = 0; i < PROFILE_CYCLES; ++i) {
         ServerMessage intermediate = converter.inbound(encoded);
         encode(converter.outbound(intermediate, 1));
      }
      totalDuration += System.nanoTime() - startTime;

      LOG_RESULTS(totalDuration);
   }

   @Test
   public void testTypicalQpidJMSMessageInBoundOnly() throws Exception {

      EncodedMessage encoded = encode(createTypicalQpidJMSMessage());

      // Warm up
      for (int i = 0; i < WARM_CYCLES; ++i) {
         converter.inbound(encoded);
      }

      long totalDuration = 0;

      long startTime = System.nanoTime();
      for (int i = 0; i < PROFILE_CYCLES; ++i) {
         converter.inbound(encoded);
      }

      totalDuration += System.nanoTime() - startTime;

      LOG_RESULTS(totalDuration);
   }

   @Test
   public void testTypicalQpidJMSMessageOutBoundOnly() throws Exception {

      EncodedMessage encoded = encode(createTypicalQpidJMSMessage());
      ServerMessage intermediate = converter.inbound(encoded);

      // Warm up
      for (int i = 0; i < WARM_CYCLES; ++i) {
         encode(converter.outbound(intermediate, 1));
      }

      long totalDuration = 0;

      long startTime = System.nanoTime();
      for (int i = 0; i < PROFILE_CYCLES; ++i) {
         encode(converter.outbound(intermediate, 1));
      }

      totalDuration += System.nanoTime() - startTime;

      LOG_RESULTS(totalDuration);
   }

   private Message createTypicalQpidJMSMessage() {
      Map<String, Object> applicationProperties = new HashMap<>();
      Map<Symbol, Object> messageAnnotations = new HashMap<>();

      applicationProperties.put("property-1", "string");
      applicationProperties.put("property-2", 512);
      applicationProperties.put("property-3", true);

      messageAnnotations.put(Symbol.valueOf("x-opt-jms-msg-type"), 0);
      messageAnnotations.put(Symbol.valueOf("x-opt-jms-dest"), 0);

      Message message = Proton.message();

      message.setAddress("queue://test-queue");
      message.setDeliveryCount(1);
      message.setApplicationProperties(new ApplicationProperties(applicationProperties));
      message.setMessageAnnotations(new MessageAnnotations(messageAnnotations));
      message.setCreationTime(System.currentTimeMillis());
      message.setContentType("text/plain");
      message.setBody(new AmqpValue("String payload for AMQP message conversion performance testing."));

      return message;
   }

   private Message createComplexQpidJMSMessage() {
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

      Message message = Proton.message();

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

      return message;
   }

   private EncodedMessage encode(Object target) {
      if (target instanceof ProtonJMessage) {
         ProtonJMessage amqp = (ProtonJMessage) target;

         ByteBuf nettyBuffer = Unpooled.buffer(1024);
         amqp.encode(new NettyWritable(nettyBuffer));

         return new EncodedMessage(0, nettyBuffer.array(), nettyBuffer.arrayOffset() + nettyBuffer.readerIndex(), nettyBuffer.readableBytes());
      } else {
         return null;
      }
   }

   private void LOG_RESULTS(long duration) {
      String result = "[JMS] Total time for " + PROFILE_CYCLES + " cycles of transforms = " + TimeUnit.NANOSECONDS.toMillis(duration) + " ms -> "
         + test.getMethodName();

      System.out.println(result);
   }
}
