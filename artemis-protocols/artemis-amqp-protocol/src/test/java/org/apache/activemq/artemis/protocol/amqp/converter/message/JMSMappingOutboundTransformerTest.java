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

import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.AMQP_DATA;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.AMQP_NULL;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.AMQP_SEQUENCE;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.AMQP_UNKNOWN;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.AMQP_VALUE_BINARY;
import static org.apache.activemq.artemis.protocol.amqp.converter.message.AMQPMessageSupport.JMS_AMQP_ORIGINAL_ENCODING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.jms.JMSException;

import org.apache.activemq.artemis.core.buffers.impl.ResetLimitWrappedActiveMQBuffer;
import org.apache.activemq.artemis.core.server.impl.ServerMessageImpl;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerDestination;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSBytesMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSMapMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSObjectMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSStreamMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSTextMessage;
import org.apache.activemq.artemis.protocol.amqp.util.NettyWritable;
import org.apache.activemq.artemis.utils.IDGenerator;
import org.apache.activemq.artemis.utils.SimpleIDGenerator;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.message.Message;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class JMSMappingOutboundTransformerTest {

   private final UUID TEST_OBJECT_VALUE = UUID.fromString("fee14b62-09e0-4ac6-a4c3-4206c630d844");
   private final String TEST_ADDRESS = "queue://testAddress";

   private IDGenerator idGenerator;
   private JMSMappingOutboundTransformer transformer;

   public static final byte QUEUE_TYPE = 0x00;
   public static final byte TOPIC_TYPE = 0x01;
   public static final byte TEMP_QUEUE_TYPE = 0x02;
   public static final byte TEMP_TOPIC_TYPE = 0x03;

   @Before
   public void setUp() {
      idGenerator = new SimpleIDGenerator(0);
      transformer = new JMSMappingOutboundTransformer(idGenerator);
   }

   // ----- no-body Message type tests ---------------------------------------//

   @Test
   public void testConvertMessageToAmqpMessageWithNoBody() throws Exception {
      ServerJMSMessage outbound = createMessage();
      outbound.encode();

      EncodedMessage encoded = transform(outbound);
      assertNotNull(encoded);

      Message amqp = encoded.decode();

      assertNull(amqp.getBody());
   }

   @Test
   public void testConvertTextMessageToAmqpMessageWithNoBodyOriginalEncodingWasNull() throws Exception {
      ServerJMSTextMessage outbound = createTextMessage();
      outbound.setShortProperty(JMS_AMQP_ORIGINAL_ENCODING, AMQP_NULL);
      outbound.encode();

      EncodedMessage encoded = transform(outbound);
      assertNotNull(encoded);

      Message amqp = encoded.decode();

      assertNull(amqp.getBody());
   }

   // ----- BytesMessage type tests ---------------------------------------//

   @Test
   public void testConvertEmptyBytesMessageToAmqpMessageWithDataBody() throws Exception {
      ServerJMSBytesMessage outbound = createBytesMessage();
      outbound.encode();

      EncodedMessage encoded = transform(outbound);
      assertNotNull(encoded);

      Message amqp = encoded.decode();

      assertNotNull(amqp.getBody());
      assertTrue(amqp.getBody() instanceof Data);
      assertTrue(((Data) amqp.getBody()).getValue() instanceof Binary);
      assertEquals(0, ((Data) amqp.getBody()).getValue().getLength());
   }

   @Test
   public void testConvertUncompressedBytesMessageToAmqpMessageWithDataBody() throws Exception {
      byte[] expectedPayload = new byte[] {8, 16, 24, 32};
      ServerJMSBytesMessage outbound = createBytesMessage();
      outbound.writeBytes(expectedPayload);
      outbound.encode();

      EncodedMessage encoded = transform(outbound);
      assertNotNull(encoded);

      Message amqp = encoded.decode();

      assertNotNull(amqp.getBody());
      assertTrue(amqp.getBody() instanceof Data);
      assertTrue(((Data) amqp.getBody()).getValue() instanceof Binary);
      assertEquals(4, ((Data) amqp.getBody()).getValue().getLength());

      Binary amqpData = ((Data) amqp.getBody()).getValue();
      Binary inputData = new Binary(expectedPayload);

      assertTrue(inputData.equals(amqpData));
   }

   @Ignore("Compressed message body support not yet implemented.")
   @Test
   public void testConvertCompressedBytesMessageToAmqpMessageWithDataBody() throws Exception {
      byte[] expectedPayload = new byte[] {8, 16, 24, 32};
      ServerJMSBytesMessage outbound = createBytesMessage(true);
      outbound.writeBytes(expectedPayload);
      outbound.encode();

      EncodedMessage encoded = transform(outbound);
      assertNotNull(encoded);

      Message amqp = encoded.decode();

      assertNotNull(amqp.getBody());
      assertTrue(amqp.getBody() instanceof Data);
      assertTrue(((Data) amqp.getBody()).getValue() instanceof Binary);
      assertEquals(4, ((Data) amqp.getBody()).getValue().getLength());

      Binary amqpData = ((Data) amqp.getBody()).getValue();
      Binary inputData = new Binary(expectedPayload);

      assertTrue(inputData.equals(amqpData));
   }

   @Test
   public void testConvertEmptyBytesMessageToAmqpMessageWithAmqpValueBody() throws Exception {
      ServerJMSBytesMessage outbound = createBytesMessage();
      outbound.setShortProperty(JMS_AMQP_ORIGINAL_ENCODING, AMQP_VALUE_BINARY);
      outbound.encode();

      EncodedMessage encoded = transform(outbound);
      assertNotNull(encoded);

      Message amqp = encoded.decode();

      assertNotNull(amqp.getBody());
      assertTrue(amqp.getBody() instanceof AmqpValue);
      assertTrue(((AmqpValue) amqp.getBody()).getValue() instanceof Binary);
      assertEquals(0, ((Binary) ((AmqpValue) amqp.getBody()).getValue()).getLength());
   }

   @Test
   public void testConvertUncompressedBytesMessageToAmqpMessageWithAmqpValueBody() throws Exception {
      byte[] expectedPayload = new byte[] {8, 16, 24, 32};
      ServerJMSBytesMessage outbound = createBytesMessage();
      outbound.setShortProperty(JMS_AMQP_ORIGINAL_ENCODING, AMQP_VALUE_BINARY);
      outbound.writeBytes(expectedPayload);
      outbound.encode();

      EncodedMessage encoded = transform(outbound);
      assertNotNull(encoded);

      Message amqp = encoded.decode();

      assertNotNull(amqp.getBody());
      assertTrue(amqp.getBody() instanceof AmqpValue);
      assertTrue(((AmqpValue) amqp.getBody()).getValue() instanceof Binary);
      assertEquals(4, ((Binary) ((AmqpValue) amqp.getBody()).getValue()).getLength());

      Binary amqpData = (Binary) ((AmqpValue) amqp.getBody()).getValue();
      Binary inputData = new Binary(expectedPayload);

      assertTrue(inputData.equals(amqpData));
   }

   @Ignore("Compressed message body support not yet implemented.")
   @Test
   public void testConvertCompressedBytesMessageToAmqpMessageWithAmqpValueBody() throws Exception {
      byte[] expectedPayload = new byte[] {8, 16, 24, 32};
      ServerJMSBytesMessage outbound = createBytesMessage(true);
      outbound.setShortProperty(JMS_AMQP_ORIGINAL_ENCODING, AMQP_VALUE_BINARY);
      outbound.writeBytes(expectedPayload);
      outbound.encode();

      EncodedMessage encoded = transform(outbound);
      assertNotNull(encoded);

      Message amqp = encoded.decode();

      assertNotNull(amqp.getBody());
      assertTrue(amqp.getBody() instanceof AmqpValue);
      assertTrue(((AmqpValue) amqp.getBody()).getValue() instanceof Binary);
      assertEquals(4, ((Binary) ((AmqpValue) amqp.getBody()).getValue()).getLength());

      Binary amqpData = (Binary) ((AmqpValue) amqp.getBody()).getValue();
      Binary inputData = new Binary(expectedPayload);

      assertTrue(inputData.equals(amqpData));
   }

   // ----- MapMessage type tests --------------------------------------------//

   @Test
   public void testConvertMapMessageToAmqpMessageWithNoBody() throws Exception {
      ServerJMSMapMessage outbound = createMapMessage();
      outbound.encode();

      EncodedMessage encoded = transform(outbound);
      assertNotNull(encoded);

      Message amqp = encoded.decode();

      assertNotNull(amqp.getBody());
      assertTrue(amqp.getBody() instanceof AmqpValue);
      assertTrue(((AmqpValue) amqp.getBody()).getValue() instanceof Map);
   }

   @Test
   public void testConvertMapMessageToAmqpMessageWithByteArrayValueInBody() throws Exception {
      final byte[] byteArray = new byte[] {1, 2, 3, 4, 5};

      ServerJMSMapMessage outbound = createMapMessage();
      outbound.setBytes("bytes", byteArray);
      outbound.encode();

      EncodedMessage encoded = transform(outbound);
      assertNotNull(encoded);

      Message amqp = encoded.decode();

      assertNotNull(amqp.getBody());
      assertTrue(amqp.getBody() instanceof AmqpValue);
      assertTrue(((AmqpValue) amqp.getBody()).getValue() instanceof Map);

      @SuppressWarnings("unchecked")
      Map<Object, Object> amqpMap = (Map<Object, Object>) ((AmqpValue) amqp.getBody()).getValue();

      assertEquals(1, amqpMap.size());
      Binary readByteArray = (Binary) amqpMap.get("bytes");
      assertNotNull(readByteArray);
   }

   @Test
   public void testConvertMapMessageToAmqpMessage() throws Exception {
      ServerJMSMapMessage outbound = createMapMessage();
      outbound.setString("property-1", "string");
      outbound.setInt("property-2", 1);
      outbound.setBoolean("property-3", true);
      outbound.encode();

      EncodedMessage encoded = transform(outbound);
      assertNotNull(encoded);

      Message amqp = encoded.decode();

      assertNotNull(amqp.getBody());
      assertTrue(amqp.getBody() instanceof AmqpValue);
      assertTrue(((AmqpValue) amqp.getBody()).getValue() instanceof Map);

      @SuppressWarnings("unchecked")
      Map<Object, Object> amqpMap = (Map<Object, Object>) ((AmqpValue) amqp.getBody()).getValue();

      assertEquals(3, amqpMap.size());
      assertTrue("string".equals(amqpMap.get("property-1")));
   }

   @Test
   public void testConvertCompressedMapMessageToAmqpMessage() throws Exception {
      ServerJMSMapMessage outbound = createMapMessage(true);
      outbound.setString("property-1", "string");
      outbound.setInt("property-2", 1);
      outbound.setBoolean("property-3", true);
      outbound.encode();

      EncodedMessage encoded = transform(outbound);
      assertNotNull(encoded);

      Message amqp = encoded.decode();

      assertNotNull(amqp.getBody());
      assertTrue(amqp.getBody() instanceof AmqpValue);
      assertTrue(((AmqpValue) amqp.getBody()).getValue() instanceof Map);

      @SuppressWarnings("unchecked")
      Map<Object, Object> amqpMap = (Map<Object, Object>) ((AmqpValue) amqp.getBody()).getValue();

      assertEquals(3, amqpMap.size());
      assertTrue("string".equals(amqpMap.get("property-1")));
   }

   // ----- StreamMessage type tests -----------------------------------------//

   @Test
   public void testConvertStreamMessageToAmqpMessageWithAmqpValueBody() throws Exception {
      ServerJMSStreamMessage outbound = createStreamMessage();
      outbound.encode();

      EncodedMessage encoded = transform(outbound);
      assertNotNull(encoded);

      Message amqp = encoded.decode();

      assertNotNull(amqp.getBody());
      assertTrue(amqp.getBody() instanceof AmqpValue);
      assertTrue(((AmqpValue) amqp.getBody()).getValue() instanceof List);
   }

   @Test
   public void testConvertStreamMessageToAmqpMessageWithAmqpSequencey() throws Exception {
      ServerJMSStreamMessage outbound = createStreamMessage();
      outbound.setShortProperty(JMS_AMQP_ORIGINAL_ENCODING, AMQP_SEQUENCE);
      outbound.encode();

      EncodedMessage encoded = transform(outbound);
      assertNotNull(encoded);

      Message amqp = encoded.decode();

      assertNotNull(amqp.getBody());
      assertTrue(amqp.getBody() instanceof AmqpSequence);
      assertTrue(((AmqpSequence) amqp.getBody()).getValue() instanceof List);
   }

   @Test
   public void testConvertCompressedStreamMessageToAmqpMessageWithAmqpValueBody() throws Exception {
      ServerJMSStreamMessage outbound = createStreamMessage(true);
      outbound.writeBoolean(false);
      outbound.writeString("test");
      outbound.encode();

      EncodedMessage encoded = transform(outbound);
      assertNotNull(encoded);

      Message amqp = encoded.decode();

      assertNotNull(amqp.getBody());
      assertTrue(amqp.getBody() instanceof AmqpValue);
      assertTrue(((AmqpValue) amqp.getBody()).getValue() instanceof List);

      @SuppressWarnings("unchecked")
      List<Object> amqpList = (List<Object>) ((AmqpValue) amqp.getBody()).getValue();

      assertEquals(2, amqpList.size());
   }

   @Test
   public void testConvertCompressedStreamMessageToAmqpMessageWithAmqpSequencey() throws Exception {
      ServerJMSStreamMessage outbound = createStreamMessage(true);
      outbound.setShortProperty(JMS_AMQP_ORIGINAL_ENCODING, AMQP_SEQUENCE);
      outbound.writeBoolean(false);
      outbound.writeString("test");
      outbound.encode();

      EncodedMessage encoded = transform(outbound);
      assertNotNull(encoded);

      Message amqp = encoded.decode();

      assertNotNull(amqp.getBody());
      assertTrue(amqp.getBody() instanceof AmqpSequence);
      assertTrue(((AmqpSequence) amqp.getBody()).getValue() instanceof List);

      @SuppressWarnings("unchecked")
      List<Object> amqpList = ((AmqpSequence) amqp.getBody()).getValue();

      assertEquals(2, amqpList.size());
   }

   // ----- ObjectMessage type tests -----------------------------------------//

   @Test
   public void testConvertEmptyObjectMessageToAmqpMessageWithDataBody() throws Exception {
      ServerJMSObjectMessage outbound = createObjectMessage();
      outbound.encode();

      EncodedMessage encoded = transform(outbound);
      assertNotNull(encoded);

      Message amqp = encoded.decode();

      assertNotNull(amqp.getBody());
      assertTrue(amqp.getBody() instanceof Data);
      assertEquals(5, ((Data) amqp.getBody()).getValue().getLength());
   }

   @Test
   public void testConvertEmptyObjectMessageToAmqpMessageUnknownEncodingGetsDataSection() throws Exception {
      ServerJMSObjectMessage outbound = createObjectMessage();
      outbound.setShortProperty(JMS_AMQP_ORIGINAL_ENCODING, AMQP_UNKNOWN);
      outbound.encode();

      EncodedMessage encoded = transform(outbound);
      assertNotNull(encoded);

      Message amqp = encoded.decode();

      assertNotNull(amqp.getBody());
      assertTrue(amqp.getBody() instanceof Data);
      assertEquals(5, ((Data) amqp.getBody()).getValue().getLength());
   }

   @Test
   public void testConvertEmptyObjectMessageToAmqpMessageWithAmqpValueBody() throws Exception {
      ServerJMSObjectMessage outbound = createObjectMessage();
      outbound.setShortProperty(JMS_AMQP_ORIGINAL_ENCODING, AMQP_VALUE_BINARY);
      outbound.encode();

      EncodedMessage encoded = transform(outbound);
      assertNotNull(encoded);

      Message amqp = encoded.decode();

      assertNotNull(amqp.getBody());
      assertTrue(amqp.getBody() instanceof AmqpValue);
      assertTrue(((AmqpValue) amqp.getBody()).getValue() instanceof Binary);
      assertEquals(5, ((Binary) ((AmqpValue) amqp.getBody()).getValue()).getLength());
   }

   @Test
   public void testConvertObjectMessageToAmqpMessageWithDataBody() throws Exception {
      ServerJMSObjectMessage outbound = createObjectMessage(TEST_OBJECT_VALUE);
      outbound.encode();

      EncodedMessage encoded = transform(outbound);
      assertNotNull(encoded);

      Message amqp = encoded.decode();

      assertNotNull(amqp.getBody());
      assertTrue(amqp.getBody() instanceof Data);
      assertFalse(0 == ((Data) amqp.getBody()).getValue().getLength());

      Object value = deserialize(((Data) amqp.getBody()).getValue().getArray());
      assertNotNull(value);
      assertTrue(value instanceof UUID);
   }

   @Test
   public void testConvertObjectMessageToAmqpMessageUnknownEncodingGetsDataSection() throws Exception {
      ServerJMSObjectMessage outbound = createObjectMessage(TEST_OBJECT_VALUE);
      outbound.setShortProperty(JMS_AMQP_ORIGINAL_ENCODING, AMQP_UNKNOWN);
      outbound.encode();

      EncodedMessage encoded = transform(outbound);
      assertNotNull(encoded);

      Message amqp = encoded.decode();

      assertNotNull(amqp.getBody());
      assertTrue(amqp.getBody() instanceof Data);
      assertFalse(0 == ((Data) amqp.getBody()).getValue().getLength());

      Object value = deserialize(((Data) amqp.getBody()).getValue().getArray());
      assertNotNull(value);
      assertTrue(value instanceof UUID);
   }

   @Test
   public void testConvertObjectMessageToAmqpMessageWithAmqpValueBody() throws Exception {
      ServerJMSObjectMessage outbound = createObjectMessage(TEST_OBJECT_VALUE);
      outbound.setShortProperty(JMS_AMQP_ORIGINAL_ENCODING, AMQP_VALUE_BINARY);
      outbound.encode();

      EncodedMessage encoded = transform(outbound);
      assertNotNull(encoded);

      Message amqp = encoded.decode();

      assertNotNull(amqp.getBody());
      assertTrue(amqp.getBody() instanceof AmqpValue);
      assertTrue(((AmqpValue) amqp.getBody()).getValue() instanceof Binary);
      assertFalse(0 == ((Binary) ((AmqpValue) amqp.getBody()).getValue()).getLength());

      Object value = deserialize(((Binary) ((AmqpValue) amqp.getBody()).getValue()).getArray());
      assertNotNull(value);
      assertTrue(value instanceof UUID);
   }

   @Test
   public void testConvertCompressedObjectMessageToAmqpMessageWithDataBody() throws Exception {
      ServerJMSObjectMessage outbound = createObjectMessage(TEST_OBJECT_VALUE, true);
      outbound.encode();

      EncodedMessage encoded = transform(outbound);
      assertNotNull(encoded);

      Message amqp = encoded.decode();

      assertNotNull(amqp.getBody());
      assertTrue(amqp.getBody() instanceof Data);
      assertFalse(0 == ((Data) amqp.getBody()).getValue().getLength());

      Object value = deserialize(((Data) amqp.getBody()).getValue().getArray());
      assertNotNull(value);
      assertTrue(value instanceof UUID);
   }

   @Test
   public void testConvertCompressedObjectMessageToAmqpMessageUnknownEncodingGetsDataSection() throws Exception {
      ServerJMSObjectMessage outbound = createObjectMessage(TEST_OBJECT_VALUE, true);
      outbound.setShortProperty(JMS_AMQP_ORIGINAL_ENCODING, AMQP_UNKNOWN);
      outbound.encode();

      EncodedMessage encoded = transform(outbound);
      assertNotNull(encoded);

      Message amqp = encoded.decode();

      assertNotNull(amqp.getBody());
      assertTrue(amqp.getBody() instanceof Data);
      assertFalse(0 == ((Data) amqp.getBody()).getValue().getLength());

      Object value = deserialize(((Data) amqp.getBody()).getValue().getArray());
      assertNotNull(value);
      assertTrue(value instanceof UUID);
   }

   @Test
   public void testConvertCompressedObjectMessageToAmqpMessageWithAmqpValueBody() throws Exception {
      ServerJMSObjectMessage outbound = createObjectMessage(TEST_OBJECT_VALUE, true);
      outbound.setShortProperty(JMS_AMQP_ORIGINAL_ENCODING, AMQP_VALUE_BINARY);
      outbound.encode();

      EncodedMessage encoded = transform(outbound);
      assertNotNull(encoded);

      Message amqp = encoded.decode();

      assertNotNull(amqp.getBody());
      assertTrue(amqp.getBody() instanceof AmqpValue);
      assertTrue(((AmqpValue) amqp.getBody()).getValue() instanceof Binary);
      assertFalse(0 == ((Binary) ((AmqpValue) amqp.getBody()).getValue()).getLength());

      Object value = deserialize(((Binary) ((AmqpValue) amqp.getBody()).getValue()).getArray());
      assertNotNull(value);
      assertTrue(value instanceof UUID);
   }

   // ----- TextMessage type tests -------------------------------------------//

   @Test
   public void testConvertTextMessageToAmqpMessageWithNoBody() throws Exception {
      ServerJMSTextMessage outbound = createTextMessage();
      outbound.encode();

      EncodedMessage encoded = transform(outbound);
      assertNotNull(encoded);

      Message amqp = encoded.decode();

      assertNotNull(amqp.getBody());
      assertTrue(amqp.getBody() instanceof AmqpValue);
      assertNull(((AmqpValue) amqp.getBody()).getValue());
   }

   @Test
   public void testConvertTextMessageCreatesBodyUsingOriginalEncodingWithDataSection() throws Exception {
      String contentString = "myTextMessageContent";
      ServerJMSTextMessage outbound = createTextMessage(contentString);
      outbound.setShortProperty(JMS_AMQP_ORIGINAL_ENCODING, AMQP_DATA);
      outbound.encode();

      EncodedMessage encoded = transform(outbound);
      assertNotNull(encoded);

      Message amqp = encoded.decode();

      assertNotNull(amqp.getBody());
      assertTrue(amqp.getBody() instanceof Data);
      assertTrue(((Data) amqp.getBody()).getValue() instanceof Binary);

      Binary data = ((Data) amqp.getBody()).getValue();
      String contents = new String(data.getArray(), data.getArrayOffset(), data.getLength(), StandardCharsets.UTF_8);
      assertEquals(contentString, contents);
   }

   @Test
   public void testConvertTextMessageContentNotStoredCreatesBodyUsingOriginalEncodingWithDataSection() throws Exception {
      String contentString = "myTextMessageContent";
      ServerJMSTextMessage outbound = createTextMessage(contentString);
      outbound.setShortProperty(JMS_AMQP_ORIGINAL_ENCODING, AMQP_DATA);
      outbound.encode();

      EncodedMessage encoded = transform(outbound);
      assertNotNull(encoded);

      Message amqp = encoded.decode();

      assertNotNull(amqp.getBody());
      assertTrue(amqp.getBody() instanceof Data);
      assertTrue(((Data) amqp.getBody()).getValue() instanceof Binary);

      Binary data = ((Data) amqp.getBody()).getValue();
      String contents = new String(data.getArray(), data.getArrayOffset(), data.getLength(), StandardCharsets.UTF_8);
      assertEquals(contentString, contents);
   }

   @Test
   public void testConvertTextMessageCreatesAmqpValueStringBody() throws Exception {
      String contentString = "myTextMessageContent";
      ServerJMSTextMessage outbound = createTextMessage(contentString);
      outbound.encode();

      EncodedMessage encoded = transform(outbound);
      assertNotNull(encoded);

      Message amqp = encoded.decode();

      assertNotNull(amqp.getBody());
      assertTrue(amqp.getBody() instanceof AmqpValue);
      assertEquals(contentString, ((AmqpValue) amqp.getBody()).getValue());
   }

   @Test
   public void testConvertTextMessageContentNotStoredCreatesAmqpValueStringBody() throws Exception {
      String contentString = "myTextMessageContent";
      ServerJMSTextMessage outbound = createTextMessage(contentString);
      outbound.encode();

      EncodedMessage encoded = transform(outbound);
      assertNotNull(encoded);

      Message amqp = encoded.decode();

      assertNotNull(amqp.getBody());
      assertTrue(amqp.getBody() instanceof AmqpValue);
      assertEquals(contentString, ((AmqpValue) amqp.getBody()).getValue());
   }

   @Test
   public void testConvertCompressedTextMessageCreatesDataSectionBody() throws Exception {
      String contentString = "myTextMessageContent";
      ServerJMSTextMessage outbound = createTextMessage(contentString, true);
      outbound.setShortProperty(JMS_AMQP_ORIGINAL_ENCODING, AMQP_DATA);
      outbound.encode();

      EncodedMessage encoded = transform(outbound);
      assertNotNull(encoded);

      Message amqp = encoded.decode();

      assertNotNull(amqp.getBody());
      assertTrue(amqp.getBody() instanceof Data);
      assertTrue(((Data) amqp.getBody()).getValue() instanceof Binary);

      Binary data = ((Data) amqp.getBody()).getValue();
      String contents = new String(data.getArray(), data.getArrayOffset(), data.getLength(), StandardCharsets.UTF_8);
      assertEquals(contentString, contents);
   }

   // ----- Test JMSDestination Handling -------------------------------------//

   @Test
   public void testConvertMessageWithJMSDestinationNull() throws Exception {
      doTestConvertMessageWithJMSDestination(null, null);
   }

   @Test
   public void testConvertMessageWithJMSDestinationQueue() throws Exception {
      doTestConvertMessageWithJMSDestination(createDestination(QUEUE_TYPE), QUEUE_TYPE);
   }

   @Ignore("Artemis code doesn't provide a means of supplying a typed destination to AMQP")
   @Test
   public void testConvertMessageWithJMSDestinationTemporaryQueue() throws Exception {
      doTestConvertMessageWithJMSDestination(createDestination(TEMP_QUEUE_TYPE), TEMP_QUEUE_TYPE);
   }

   @Ignore("Artemis code doesn't provide a means of supplying a typed destination to AMQP")
   @Test
   public void testConvertMessageWithJMSDestinationTopic() throws Exception {
      doTestConvertMessageWithJMSDestination(createDestination(TOPIC_TYPE), TOPIC_TYPE);
   }

   @Ignore("Artemis code doesn't provide a means of supplying a typed destination to AMQP")
   @Test
   public void testConvertMessageWithJMSDestinationTemporaryTopic() throws Exception {
      doTestConvertMessageWithJMSDestination(createDestination(TEMP_TOPIC_TYPE), TEMP_TOPIC_TYPE);
   }

   private void doTestConvertMessageWithJMSDestination(ServerDestination jmsDestination, Object expectedAnnotationValue) throws Exception {
      ServerJMSTextMessage textMessage = createTextMessage();
      textMessage.setText("myTextMessageContent");
      textMessage.setJMSDestination(jmsDestination);

      EncodedMessage encoded = transform(textMessage);
      assertNotNull(encoded);

      Message amqp = encoded.decode();

      MessageAnnotations ma = amqp.getMessageAnnotations();
      Map<Symbol, Object> maMap = ma == null ? null : ma.getValue();
      if (maMap != null) {
         Object actualValue = maMap.get(JMSMappingOutboundTransformer.JMS_DEST_TYPE_MSG_ANNOTATION);
         assertEquals("Unexpected annotation value", expectedAnnotationValue, actualValue);
      } else if (expectedAnnotationValue != null) {
         fail("Expected annotation value, but there were no annotations");
      }

      if (jmsDestination != null) {
         assertEquals("Unexpected 'to' address", jmsDestination.getAddress(), amqp.getAddress());
      }
   }

   // ----- Test JMSReplyTo Handling -----------------------------------------//

   @Test
   public void testConvertMessageWithJMSReplyToNull() throws Exception {
      doTestConvertMessageWithJMSReplyTo(null, null);
   }

   @Test
   public void testConvertMessageWithJMSReplyToQueue() throws Exception {
      doTestConvertMessageWithJMSReplyTo(createDestination(QUEUE_TYPE), QUEUE_TYPE);
   }

   @Ignore("Artemis code doesn't provide a means of supplying a typed destination to AMQP")
   @Test
   public void testConvertMessageWithJMSReplyToTemporaryQueue() throws Exception {
      doTestConvertMessageWithJMSReplyTo(createDestination(TEMP_QUEUE_TYPE), TEMP_QUEUE_TYPE);
   }

   @Ignore("Artemis code doesn't provide a means of supplying a typed destination to AMQP")
   @Test
   public void testConvertMessageWithJMSReplyToTopic() throws Exception {
      doTestConvertMessageWithJMSReplyTo(createDestination(TOPIC_TYPE), TOPIC_TYPE);
   }

   @Ignore("Artemis code doesn't provide a means of supplying a typed destination to AMQP")
   @Test
   public void testConvertMessageWithJMSReplyToTemporaryTopic() throws Exception {
      doTestConvertMessageWithJMSReplyTo(createDestination(TEMP_TOPIC_TYPE), TEMP_TOPIC_TYPE);
   }

   private void doTestConvertMessageWithJMSReplyTo(ServerDestination jmsReplyTo, Object expectedAnnotationValue) throws Exception {
      ServerJMSTextMessage textMessage = createTextMessage();
      textMessage.setText("myTextMessageContent");
      textMessage.setJMSReplyTo(jmsReplyTo);

      EncodedMessage encoded = transform(textMessage);
      assertNotNull(encoded);

      Message amqp = encoded.decode();

      MessageAnnotations ma = amqp.getMessageAnnotations();
      Map<Symbol, Object> maMap = ma == null ? null : ma.getValue();
      if (maMap != null) {
         Object actualValue = maMap.get(JMSMappingOutboundTransformer.JMS_REPLY_TO_TYPE_MSG_ANNOTATION);
         assertEquals("Unexpected annotation value", expectedAnnotationValue, actualValue);
      } else if (expectedAnnotationValue != null) {
         fail("Expected annotation value, but there were no annotations");
      }

      if (jmsReplyTo != null) {
         assertEquals("Unexpected 'reply-to' address", jmsReplyTo.getAddress(), amqp.getReplyTo());
      }
   }

   // ----- Utility Methods used for this Test -------------------------------//

   public EncodedMessage transform(ServerJMSMessage message) throws Exception {
      // Useful for testing but not recommended for real life use.
      ByteBuf nettyBuffer = Unpooled.buffer(1024);
      NettyWritable buffer = new NettyWritable(nettyBuffer);

      long messageFormat = transformer.transform(message, buffer);

      EncodedMessage encoded = new EncodedMessage(messageFormat, nettyBuffer.array(), nettyBuffer.arrayOffset() + nettyBuffer.readerIndex(), nettyBuffer.readableBytes());

      return encoded;
   }

   private ServerDestination createDestination(byte destType) {
      ServerDestination destination = null;
      switch (destType) {
         case QUEUE_TYPE:
            destination = new ServerDestination(TEST_ADDRESS);
            break;
         case TOPIC_TYPE:
            destination = new ServerDestination(TEST_ADDRESS);
            break;
         case TEMP_QUEUE_TYPE:
            destination = new ServerDestination(TEST_ADDRESS);
            break;
         case TEMP_TOPIC_TYPE:
            destination = new ServerDestination(TEST_ADDRESS);
            break;
         default:
            throw new IllegalArgumentException("Invliad Destination Type given/");
      }

      return destination;
   }

   private ServerJMSMessage createMessage() {
      return new ServerJMSMessage(newMessage(org.apache.activemq.artemis.api.core.Message.DEFAULT_TYPE), 0);
   }

   private ServerJMSBytesMessage createBytesMessage() {
      return createBytesMessage(false);
   }

   private ServerJMSBytesMessage createBytesMessage(boolean compression) {
      ServerJMSBytesMessage message = new ServerJMSBytesMessage(newMessage(org.apache.activemq.artemis.api.core.Message.BYTES_TYPE), 0);

      if (compression) {
         // TODO
      }

      return message;
   }

   private ServerJMSMapMessage createMapMessage() {
      return createMapMessage(false);
   }

   private ServerJMSMapMessage createMapMessage(boolean compression) {
      ServerJMSMapMessage message = new ServerJMSMapMessage(newMessage(org.apache.activemq.artemis.api.core.Message.MAP_TYPE), 0);

      if (compression) {
         // TODO
      }

      return message;
   }

   private ServerJMSStreamMessage createStreamMessage() {
      return createStreamMessage(false);
   }

   private ServerJMSStreamMessage createStreamMessage(boolean compression) {
      ServerJMSStreamMessage message = new ServerJMSStreamMessage(newMessage(org.apache.activemq.artemis.api.core.Message.STREAM_TYPE), 0);

      if (compression) {
         // TODO
      }

      return message;
   }

   private ServerJMSObjectMessage createObjectMessage() {
      return createObjectMessage(null);
   }

   private ServerJMSObjectMessage createObjectMessage(Serializable payload) {
      return createObjectMessage(payload, false);
   }

   private ServerJMSObjectMessage createObjectMessage(Serializable payload, boolean compression) {
      ServerJMSObjectMessage result = AMQPMessageSupport.createObjectMessage(idGenerator);

      if (compression) {
         // TODO
      }

      try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); ObjectOutputStream oos = new ObjectOutputStream(baos);) {

         oos.writeObject(payload);
         byte[] data = baos.toByteArray();
         result.setSerializedForm(new Binary(data));
      } catch (Exception ex) {
         throw new AssertionError("Should not fail to setObject in this test");
      }

      return result;
   }

   private ServerJMSTextMessage createTextMessage() {
      return createTextMessage(null);
   }

   private ServerJMSTextMessage createTextMessage(String text) {
      return createTextMessage(text, false);
   }

   private ServerJMSTextMessage createTextMessage(String text, boolean compression) {
      ServerJMSTextMessage result = AMQPMessageSupport.createTextMessage(idGenerator);

      if (compression) {
         // TODO
      }

      try {
         result.setText(text);
      } catch (JMSException e) {
      }

      return result;
   }

   private Object deserialize(byte[] payload) throws Exception {
      try (ByteArrayInputStream bis = new ByteArrayInputStream(payload); ObjectInputStream ois = new ObjectInputStream(bis);) {

         return ois.readObject();
      }
   }

   private ServerMessageImpl newMessage(byte messageType) {
      ServerMessageImpl message = new ServerMessageImpl(idGenerator.generateID(), 512);
      message.setType(messageType);
      ((ResetLimitWrappedActiveMQBuffer) message.getBodyBuffer()).setMessage(null);
      return message;
   }
}
