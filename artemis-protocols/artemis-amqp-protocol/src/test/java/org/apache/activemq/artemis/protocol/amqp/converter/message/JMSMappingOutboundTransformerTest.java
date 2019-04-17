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

import static org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport.AMQP_DATA;
import static org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport.AMQP_NULL;
import static org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport.AMQP_SEQUENCE;
import static org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport.AMQP_UNKNOWN;
import static org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport.AMQP_VALUE_BINARY;
import static org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport.AMQP_VALUE_LIST;
import static org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport.JMS_AMQP_ORIGINAL_ENCODING;
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

import javax.jms.Destination;
import javax.jms.JMSException;

import org.apache.activemq.artemis.core.buffers.impl.ResetLimitWrappedActiveMQBuffer;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.jms.client.ActiveMQQueue;
import org.apache.activemq.artemis.jms.client.ActiveMQTemporaryQueue;
import org.apache.activemq.artemis.jms.client.ActiveMQTemporaryTopic;
import org.apache.activemq.artemis.jms.client.ActiveMQTopic;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.AMQPConverter;
import org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSBytesMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSMapMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSObjectMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSStreamMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSTextMessage;
import org.apache.activemq.artemis.utils.PrefixUtil;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.junit.Test;

public class JMSMappingOutboundTransformerTest {

   private final UUID TEST_OBJECT_VALUE = UUID.fromString("fee14b62-09e0-4ac6-a4c3-4206c630d844");
   private final String TEST_ADDRESS = "queue://testAddress";

   public static final byte QUEUE_TYPE = 0x00;
   public static final byte TOPIC_TYPE = 0x01;
   public static final byte TEMP_QUEUE_TYPE = 0x02;
   public static final byte TEMP_TOPIC_TYPE = 0x03;

   // ----- no-body Message type tests ---------------------------------------//

   @Test
   public void testConvertMessageToAmqpMessageWithNoBody() throws Exception {
      ServerJMSMessage outbound = createMessage();
      outbound.encode();

      AMQPMessage amqp = AMQPConverter.getInstance().fromCore(outbound.getInnerMessage());

      assertNull(amqp.getBody());
   }

   @Test
   public void testConvertTextMessageToAmqpMessageWithNoBodyOriginalEncodingWasNull() throws Exception {
      ServerJMSMessage outbound = createMessage();
      outbound.setShortProperty(JMS_AMQP_ORIGINAL_ENCODING, AMQP_NULL);
      outbound.encode();

      AMQPMessage amqp = AMQPConverter.getInstance().fromCore(outbound.getInnerMessage());

      assertNull(amqp.getBody());
   }

   // ----- BytesMessage type tests ---------------------------------------//

   @Test
   public void testConvertBytesMessageToAmqpMessageWithDataBody() throws Exception {
      byte[] expectedPayload = new byte[]{8, 16, 24, 32};
      ServerJMSBytesMessage outbound = createBytesMessage();
      outbound.writeBytes(expectedPayload);
      outbound.encode();

      AMQPMessage amqp = AMQPConverter.getInstance().fromCore(outbound.getInnerMessage());

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

      AMQPMessage amqp = AMQPConverter.getInstance().fromCore(outbound.getInnerMessage());

      assertNotNull(amqp.getBody());
      assertTrue(amqp.getBody() instanceof AmqpValue);
      assertTrue(((AmqpValue) amqp.getBody()).getValue() instanceof Binary);
      assertEquals(0, ((Binary) ((AmqpValue) amqp.getBody()).getValue()).getLength());
   }

   @Test
   public void testConvertBytesMessageToAmqpMessageWithAmqpValueBody() throws Exception {
      byte[] expectedPayload = new byte[]{8, 16, 24, 32};
      ServerJMSBytesMessage outbound = createBytesMessage();
      outbound.setShortProperty(JMS_AMQP_ORIGINAL_ENCODING, AMQP_VALUE_BINARY);
      outbound.writeBytes(expectedPayload);
      outbound.encode();

      AMQPMessage amqp = AMQPConverter.getInstance().fromCore(outbound.getInnerMessage());

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

      AMQPMessage amqp = AMQPConverter.getInstance().fromCore(outbound.getInnerMessage());

      assertNotNull(amqp.getBody());
      assertTrue(amqp.getBody() instanceof AmqpValue);
      assertTrue(((AmqpValue) amqp.getBody()).getValue() instanceof Map);
   }

   @Test
   public void testConvertMapMessageToAmqpMessageWithByteArrayValueInBody() throws Exception {
      final byte[] byteArray = new byte[]{1, 2, 3, 4, 5};

      ServerJMSMapMessage outbound = createMapMessage();
      outbound.setBytes("bytes", byteArray);
      outbound.encode();

      AMQPMessage amqp = AMQPConverter.getInstance().fromCore(outbound.getInnerMessage());

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

      AMQPMessage amqp = AMQPConverter.getInstance().fromCore(outbound.getInnerMessage());

      assertNotNull(amqp.getBody());
      assertTrue(amqp.getBody() instanceof AmqpValue);
      assertTrue(((AmqpValue) amqp.getBody()).getValue() instanceof Map);

      @SuppressWarnings("unchecked")
      Map<Object, Object> amqpMap = (Map<Object, Object>) ((AmqpValue) amqp.getBody()).getValue();

      assertEquals(3, amqpMap.size());
      assertTrue("string".equals(amqpMap.get("property-1")));
   }

   //----- StreamMessage type tests -----------------------------------------//

   @Test
   public void testConvertStreamMessageToAmqpMessageWithAmqpValueBodyNoPropertySet() throws Exception {
      ServerJMSStreamMessage outbound = createStreamMessage();
      outbound.writeBoolean(false);
      outbound.writeString("test");
      outbound.encode();

      AMQPMessage amqp = AMQPConverter.getInstance().fromCore(outbound.getInnerMessage());

      assertNotNull(amqp.getBody());
      assertTrue(amqp.getBody() instanceof AmqpValue);

      AmqpValue list = (AmqpValue) amqp.getBody();

      @SuppressWarnings("unchecked")
      List<Object> amqpList = (List<Object>) list.getValue();

      assertEquals(2, amqpList.size());
   }

   @Test
   public void testConvertStreamMessageToAmqpMessageWithAmqpValueBody() throws Exception {
      ServerJMSStreamMessage outbound = createStreamMessage();
      outbound.setShortProperty(JMS_AMQP_ORIGINAL_ENCODING, AMQP_VALUE_LIST);
      outbound.writeBoolean(false);
      outbound.writeString("test");
      outbound.encode();

      AMQPMessage amqp = AMQPConverter.getInstance().fromCore(outbound.getInnerMessage());

      assertNotNull(amqp.getBody());
      assertTrue(amqp.getBody() instanceof AmqpValue);

      AmqpValue list = (AmqpValue) amqp.getBody();

      @SuppressWarnings("unchecked")
      List<Object> amqpList = (List<Object>) list.getValue();

      assertEquals(2, amqpList.size());
   }

   @Test
   public void testConvertStreamMessageToAmqpMessageWithAmqpSequencey() throws Exception {
      ServerJMSStreamMessage outbound = createStreamMessage();
      outbound.setShortProperty(JMS_AMQP_ORIGINAL_ENCODING, AMQP_SEQUENCE);
      outbound.writeBoolean(false);
      outbound.writeString("test");
      outbound.encode();

      AMQPMessage amqp = AMQPConverter.getInstance().fromCore(outbound.getInnerMessage());

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

      AMQPMessage amqp = AMQPConverter.getInstance().fromCore(outbound.getInnerMessage());

      assertNotNull(amqp.getBody());
      assertTrue(amqp.getBody() instanceof Data);
      assertEquals(5, ((Data) amqp.getBody()).getValue().getLength());
   }

   @Test
   public void testConvertEmptyObjectMessageToAmqpMessageUnknownEncodingGetsDataSection() throws Exception {
      ServerJMSObjectMessage outbound = createObjectMessage();
      outbound.setShortProperty(JMS_AMQP_ORIGINAL_ENCODING, AMQP_UNKNOWN);
      outbound.encode();

      AMQPMessage amqp = AMQPConverter.getInstance().fromCore(outbound.getInnerMessage());

      assertNotNull(amqp.getBody());
      assertTrue(amqp.getBody() instanceof Data);
      assertEquals(5, ((Data) amqp.getBody()).getValue().getLength());
   }

   @Test
   public void testConvertObjectMessageToAmqpMessageWithDataBody() throws Exception {
      ServerJMSObjectMessage outbound = createObjectMessage(TEST_OBJECT_VALUE);
      outbound.encode();

      AMQPMessage amqp = AMQPConverter.getInstance().fromCore(outbound.getInnerMessage());

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

      AMQPMessage amqp = AMQPConverter.getInstance().fromCore(outbound.getInnerMessage());

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

      AMQPMessage amqp = AMQPConverter.getInstance().fromCore(outbound.getInnerMessage());

      assertNotNull(amqp.getBody());
      assertTrue(amqp.getBody() instanceof AmqpValue);
      assertTrue(((AmqpValue) amqp.getBody()).getValue() instanceof Binary);
      assertFalse(0 == ((Binary) ((AmqpValue) amqp.getBody()).getValue()).getLength());

      Object value = deserialize(((Binary) ((AmqpValue) amqp.getBody()).getValue()).getArray());
      assertNotNull(value);
      assertTrue(value instanceof UUID);
   }

   @Test
   public void testConvertEmptyObjectMessageToAmqpMessageWithAmqpValueBody() throws Exception {
      ServerJMSObjectMessage outbound = createObjectMessage();
      outbound.setShortProperty(JMS_AMQP_ORIGINAL_ENCODING, AMQP_VALUE_BINARY);
      outbound.encode();

      AMQPMessage amqp = AMQPConverter.getInstance().fromCore(outbound.getInnerMessage());

      assertNotNull(amqp.getBody());
      assertTrue(amqp.getBody() instanceof AmqpValue);
      assertTrue(((AmqpValue) amqp.getBody()).getValue() instanceof Binary);
      assertEquals(5, ((Binary) ((AmqpValue) amqp.getBody()).getValue()).getLength());
   }

   // ----- TextMessage type tests -------------------------------------------//

   @Test
   public void testConvertTextMessageToAmqpMessageWithNoBody() throws Exception {
      ServerJMSTextMessage outbound = createTextMessage();
      outbound.encode();

      AMQPMessage amqp = AMQPConverter.getInstance().fromCore(outbound.getInnerMessage());

      assertNotNull(amqp.getBody());
      assertTrue(amqp.getBody() instanceof AmqpValue);
      assertNull(((AmqpValue) amqp.getBody()).getValue());
   }

   @Test
   public void testConvertTextMessageCreatesAmqpValueStringBody() throws Exception {
      String contentString = "myTextMessageContent";
      ServerJMSTextMessage outbound = createTextMessage(contentString);
      outbound.encode();

      AMQPMessage amqp = AMQPConverter.getInstance().fromCore(outbound.getInnerMessage());

      assertNotNull(amqp.getBody());
      assertTrue(amqp.getBody() instanceof AmqpValue);
      assertEquals(contentString, ((AmqpValue) amqp.getBody()).getValue());
   }

   @Test
   public void testConvertTextMessageContentNotStoredCreatesAmqpValueStringBody() throws Exception {
      String contentString = "myTextMessageContent";
      ServerJMSTextMessage outbound = createTextMessage(contentString);
      outbound.encode();

      AMQPMessage amqp = AMQPConverter.getInstance().fromCore(outbound.getInnerMessage());

      assertNotNull(amqp.getBody());
      assertTrue(amqp.getBody() instanceof AmqpValue);
      assertEquals(contentString, ((AmqpValue) amqp.getBody()).getValue());
   }

   @Test
   public void testConvertTextMessageCreatesDataSectionBody() throws Exception {
      String contentString = "myTextMessageContent";
      ServerJMSTextMessage outbound = createTextMessage(contentString);
      outbound.encode();

      AMQPMessage amqp = AMQPConverter.getInstance().fromCore(outbound.getInnerMessage());

      assertNotNull(amqp.getBody());
      assertTrue(amqp.getBody() instanceof AmqpValue);

      AmqpValue value = (AmqpValue) amqp.getBody();

      assertEquals(contentString, value.getValue());
   }

   @Test
   public void testConvertTextMessageCreatesBodyUsingOriginalEncodingWithDataSection() throws Exception {
      String contentString = "myTextMessageContent";
      ServerJMSTextMessage outbound = createTextMessage(contentString);
      outbound.setShortProperty(JMS_AMQP_ORIGINAL_ENCODING, AMQP_DATA);
      outbound.encode();

      AMQPMessage amqp = AMQPConverter.getInstance().fromCore(outbound.getInnerMessage());

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

      AMQPMessage amqp = AMQPConverter.getInstance().fromCore(outbound.getInnerMessage());

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

   private void doTestConvertMessageWithJMSDestination(Destination jmsDestination, Object expectedAnnotationValue) throws Exception {
      ServerJMSTextMessage textMessage = createTextMessage();
      textMessage.setText("myTextMessageContent");
      textMessage.setJMSDestination(jmsDestination);

      AMQPMessage amqp = AMQPConverter.getInstance().fromCore(textMessage.getInnerMessage());

      MessageAnnotations ma = amqp.getMessageAnnotations();
      Map<Symbol, Object> maMap = ma == null ? null : ma.getValue();
      if (maMap != null) {
         Object actualValue = maMap.get(AMQPMessageSupport.JMS_DEST_TYPE_MSG_ANNOTATION);
         assertEquals("Unexpected annotation value", expectedAnnotationValue, actualValue);
      } else if (expectedAnnotationValue != null) {
         fail("Expected annotation value, but there were no annotations");
      }

      if (jmsDestination != null) {
         assertEquals("Unexpected 'to' address", AMQPMessageSupport.toAddress(jmsDestination), amqp.getAddress());
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

   private void doTestConvertMessageWithJMSReplyTo(Destination jmsReplyTo, Object expectedAnnotationValue) throws Exception {
      ServerJMSTextMessage textMessage = createTextMessage();
      textMessage.setText("myTextMessageContent");
      textMessage.setJMSReplyTo(jmsReplyTo);

      AMQPMessage amqp = AMQPConverter.getInstance().fromCore(textMessage.getInnerMessage());

      MessageAnnotations ma = amqp.getMessageAnnotations();
      Map<Symbol, Object> maMap = ma == null ? null : ma.getValue();
      if (maMap != null) {
         Object actualValue = maMap.get(AMQPMessageSupport.JMS_REPLY_TO_TYPE_MSG_ANNOTATION);
         assertEquals("Unexpected annotation value", expectedAnnotationValue, actualValue);
      } else if (expectedAnnotationValue != null) {
         fail("Expected annotation value, but there were no annotations");
      }

      if (jmsReplyTo != null) {
         assertEquals("Unexpected 'reply-to' address", AMQPMessageSupport.toAddress(jmsReplyTo).toString(), amqp.getReplyTo().toString());
      }
   }

   // ----- Utility Methods used for this Test -------------------------------//

   private Destination createDestination(byte destType) {
      Destination destination = null;
      String prefix = PrefixUtil.getURIPrefix(TEST_ADDRESS);
      String address = PrefixUtil.removePrefix(TEST_ADDRESS, prefix);
      switch (destType) {
         case QUEUE_TYPE:
            destination = new ActiveMQQueue(address);
            break;
         case TOPIC_TYPE:
            destination = new ActiveMQTopic(address);
            break;
         case TEMP_QUEUE_TYPE:
            destination = new ActiveMQTemporaryQueue(address, null);
            break;
         case TEMP_TOPIC_TYPE:
            destination = new ActiveMQTemporaryTopic(address, null);
            break;
         default:
            throw new IllegalArgumentException("Invliad Destination Type given/");
      }

      return destination;
   }

   private ServerJMSMessage createMessage() {
      return new ServerJMSMessage(newMessage(org.apache.activemq.artemis.api.core.Message.DEFAULT_TYPE));
   }

   private ServerJMSBytesMessage createBytesMessage() {
      return new ServerJMSBytesMessage(newMessage(org.apache.activemq.artemis.api.core.Message.BYTES_TYPE));
   }

   private ServerJMSMapMessage createMapMessage() {
      return new ServerJMSMapMessage(newMessage(org.apache.activemq.artemis.api.core.Message.MAP_TYPE));
   }

   private ServerJMSStreamMessage createStreamMessage() {
      return new ServerJMSStreamMessage(newMessage(org.apache.activemq.artemis.api.core.Message.STREAM_TYPE));
   }

   private ServerJMSObjectMessage createObjectMessage() {
      return createObjectMessage(null);
   }

   private ServerJMSObjectMessage createObjectMessage(Serializable payload) {
      ServerJMSObjectMessage result = AMQPMessageSupport.createObjectMessage(0, null);

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
      ServerJMSTextMessage result = AMQPMessageSupport.createTextMessage(0, null);

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

   private CoreMessage newMessage(byte messageType) {
      CoreMessage message = new CoreMessage(0, 512);
      message.setType(messageType);
      ((ResetLimitWrappedActiveMQBuffer) message.getBodyBuffer()).setMessage(null);
      return message;
   }
}
