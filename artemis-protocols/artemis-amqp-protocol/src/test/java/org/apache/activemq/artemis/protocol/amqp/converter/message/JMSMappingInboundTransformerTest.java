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

import javax.jms.Destination;
import javax.jms.MapMessage;
import javax.jms.Queue;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.jms.client.ActiveMQMessage;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.AMQPMessageSupport;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSBytesMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSMapMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSObjectMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSStreamMessage;
import org.apache.activemq.artemis.protocol.amqp.converter.jms.ServerJMSTextMessage;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.message.Message;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class JMSMappingInboundTransformerTest {

   @Before
   public void setUp() {
   }

   // ----- Null Body Section ------------------------------------------------//

   /**
    * Test that a message with no body section, but with the content type set to
    * {@value AMQPMessageSupport#OCTET_STREAM_CONTENT_TYPE} results in a BytesMessage
    *
    * @throws Exception
    *         if an error occurs during the test.
    */
   @Test
   public void testCreateBytesMessageFromNoBodySectionAndContentType() throws Exception {
      Message message = Message.Factory.create();
      message.setContentType(AMQPMessageSupport.OCTET_STREAM_CONTENT_TYPE);

      AMQPMessage messageEncode = new AMQPMessage(message);

      ICoreMessage coreMessage = messageEncode.toCore();

      javax.jms.Message jmsMessage = ServerJMSMessage.wrapCoreMessage(coreMessage);

      assertNotNull("Message should not be null", jmsMessage);
      assertEquals("Unexpected message class type", ServerJMSBytesMessage.class, jmsMessage.getClass());
   }

   /**
    * Test that a message with no body section, and no content-type results in a BytesMessage
    * when not otherwise annotated to indicate the type of JMS message it is.
    *
    * @throws Exception
    *         if an error occurs during the test.
    */
   @Test
   public void testCreateBytesMessageFromNoBodySectionAndNoContentType() throws Exception {
      Message message = Message.Factory.create();

      javax.jms.Message jmsMessage = ServerJMSMessage.wrapCoreMessage(new AMQPMessage(message).toCore());

      assertNotNull("Message should not be null", jmsMessage);
      assertEquals("Unexpected message class type", ServerJMSBytesMessage.class, jmsMessage.getClass());
   }

   /**
    * Test that a message with no body section, but with the content type set to
    * {@value AMQPMessageSupport#SERIALIZED_JAVA_OBJECT_CONTENT_TYPE} results in an
    * ObjectMessage when not otherwise annotated to indicate the type of JMS message it is.
    *
    * @throws Exception
    *         if an error occurs during the test.
    */
   @Test
   public void testCreateObjectMessageFromNoBodySectionAndContentType() throws Exception {
      Message message = Message.Factory.create();
      message.setContentType(AMQPMessageSupport.SERIALIZED_JAVA_OBJECT_CONTENT_TYPE.toString());

      javax.jms.Message jmsMessage = ServerJMSMessage.wrapCoreMessage(new AMQPMessage(message).toCore());

      assertNotNull("Message should not be null", jmsMessage);
      assertEquals("Unexpected message class type", ServerJMSObjectMessage.class, jmsMessage.getClass());
   }

   @Test
   public void testCreateTextMessageFromNoBodySectionAndContentType() throws Exception {
      Message message = Message.Factory.create();
      message.setContentType("text/plain");

      javax.jms.Message jmsMessage = ServerJMSMessage.wrapCoreMessage(new AMQPMessage(message).toCore());

      assertNotNull("Message should not be null", jmsMessage);
      assertEquals("Unexpected message class type", ServerJMSTextMessage.class, jmsMessage.getClass());
   }

   /**
    * Test that a message with no body section, and with the content type set to an unknown
    * value results in a plain Message when not otherwise annotated to indicate the type of JMS
    * message it is.
    *
    * @throws Exception
    *         if an error occurs during the test.
    */
   @Test
   public void testCreateGenericMessageFromNoBodySectionAndUnknownContentType() throws Exception {
      Message message = Message.Factory.create();
      message.setContentType("unknown-content-type");

      javax.jms.Message jmsMessage = ServerJMSMessage.wrapCoreMessage(new AMQPMessage(message).toCore());

      assertNotNull("Message should not be null", jmsMessage);
      assertEquals("Unexpected message class type", ActiveMQMessage.class, jmsMessage.getClass());
   }

   // ----- Data Body Section ------------------------------------------------//

   /**
    * Test that a data body containing nothing, but with the content type set to
    * {@value AMQPMessageSupport#OCTET_STREAM_CONTENT_TYPE} results in a BytesMessage when not
    * otherwise annotated to indicate the type of JMS message it is.
    *
    * @throws Exception
    *         if an error occurs during the test.
    */
   @Test
   public void testCreateBytesMessageFromDataWithEmptyBinaryAndContentType() throws Exception {
      Message message = Proton.message();
      Binary binary = new Binary(new byte[0]);
      message.setBody(new Data(binary));
      message.setContentType(AMQPMessageSupport.OCTET_STREAM_CONTENT_TYPE);

      javax.jms.Message jmsMessage = ServerJMSMessage.wrapCoreMessage(new AMQPMessage(message).toCore());

      assertNotNull("Message should not be null", jmsMessage);
      assertEquals("Unexpected message class type", ServerJMSBytesMessage.class, jmsMessage.getClass());
   }

   /**
    * Test that a message with an empty data body section, and with the content type set to an
    * unknown value results in a BytesMessage when not otherwise annotated to indicate the type
    * of JMS message it is.
    *
    * @throws Exception
    *         if an error occurs during the test.
    */
   public void testCreateBytesMessageFromDataWithUnknownContentType() throws Exception {
      Message message = Proton.message();
      Binary binary = new Binary(new byte[0]);
      message.setBody(new Data(binary));
      message.setContentType("unknown-content-type");

      javax.jms.Message jmsMessage = ServerJMSMessage.wrapCoreMessage(new AMQPMessage(message).toCore());

      assertNotNull("Message should not be null", jmsMessage);
      assertEquals("Unexpected message class type", ServerJMSBytesMessage.class, jmsMessage.getClass());
   }

   /**
    * Test that a receiving a data body containing nothing and no content type being set results
    * in a BytesMessage when not otherwise annotated to indicate the type of JMS message it is.
    *
    * @throws Exception
    *         if an error occurs during the test.
    */
   @Test
   public void testCreateBytesMessageFromDataWithEmptyBinaryAndNoContentType() throws Exception {
      Message message = Proton.message();
      Binary binary = new Binary(new byte[0]);
      message.setBody(new Data(binary));

      assertNull(message.getContentType());

      javax.jms.Message jmsMessage = ServerJMSMessage.wrapCoreMessage(new AMQPMessage(message).toCore());

      assertNotNull("Message should not be null", jmsMessage);
      assertEquals("Unexpected message class type", ServerJMSBytesMessage.class, jmsMessage.getClass());
   }

   /**
    * Test that receiving a data body containing nothing, but with the content type set to
    * {@value AMQPMessageSupport#SERIALIZED_JAVA_OBJECT_CONTENT_TYPE} results in an
    * ObjectMessage when not otherwise annotated to indicate the type of JMS message it is.
    *
    * @throws Exception
    *         if an error occurs during the test.
    */
   @Test
   public void testCreateObjectMessageFromDataWithContentTypeAndEmptyBinary() throws Exception {
      Message message = Proton.message();
      Binary binary = new Binary(new byte[0]);
      message.setBody(new Data(binary));
      message.setContentType(AMQPMessageSupport.SERIALIZED_JAVA_OBJECT_CONTENT_TYPE.toString());

      javax.jms.Message jmsMessage = ServerJMSMessage.wrapCoreMessage(new AMQPMessage(message).toCore());

      assertNotNull("Message should not be null", jmsMessage);
      assertEquals("Unexpected message class type", ServerJMSObjectMessage.class, jmsMessage.getClass());
   }

   @Test
   public void testCreateTextMessageFromDataWithContentTypeTextPlain() throws Exception {
      doCreateTextMessageFromDataWithContentTypeTestImpl("text/plain;charset=iso-8859-1", StandardCharsets.ISO_8859_1);
      doCreateTextMessageFromDataWithContentTypeTestImpl("text/plain;charset=us-ascii", StandardCharsets.US_ASCII);
      doCreateTextMessageFromDataWithContentTypeTestImpl("text/plain;charset=utf-8", StandardCharsets.UTF_8);
      doCreateTextMessageFromDataWithContentTypeTestImpl("text/plain", StandardCharsets.UTF_8);
   }

   @Test
   public void testCreateTextMessageFromDataWithContentTypeTextJson() throws Exception {
      doCreateTextMessageFromDataWithContentTypeTestImpl("text/json;charset=iso-8859-1", StandardCharsets.ISO_8859_1);
      doCreateTextMessageFromDataWithContentTypeTestImpl("text/json;charset=us-ascii", StandardCharsets.US_ASCII);
      doCreateTextMessageFromDataWithContentTypeTestImpl("text/json;charset=utf-8", StandardCharsets.UTF_8);
      doCreateTextMessageFromDataWithContentTypeTestImpl("text/json", StandardCharsets.UTF_8);
   }

   @Test
   public void testCreateTextMessageFromDataWithContentTypeTextHtml() throws Exception {
      doCreateTextMessageFromDataWithContentTypeTestImpl("text/html;charset=iso-8859-1", StandardCharsets.ISO_8859_1);
      doCreateTextMessageFromDataWithContentTypeTestImpl("text/html;charset=us-ascii", StandardCharsets.US_ASCII);
      doCreateTextMessageFromDataWithContentTypeTestImpl("text/html;charset=utf-8", StandardCharsets.UTF_8);
      doCreateTextMessageFromDataWithContentTypeTestImpl("text/html", StandardCharsets.UTF_8);
   }

   @Test
   public void testCreateTextMessageFromDataWithContentTypeTextFoo() throws Exception {
      doCreateTextMessageFromDataWithContentTypeTestImpl("text/foo;charset=iso-8859-1", StandardCharsets.ISO_8859_1);
      doCreateTextMessageFromDataWithContentTypeTestImpl("text/foo;charset=us-ascii", StandardCharsets.US_ASCII);
      doCreateTextMessageFromDataWithContentTypeTestImpl("text/foo;charset=utf-8", StandardCharsets.UTF_8);
      doCreateTextMessageFromDataWithContentTypeTestImpl("text/foo", StandardCharsets.UTF_8);
   }

   @Test
   public void testCreateTextMessageFromDataWithContentTypeApplicationJson() throws Exception {
      doCreateTextMessageFromDataWithContentTypeTestImpl("application/json;charset=iso-8859-1", StandardCharsets.ISO_8859_1);
      doCreateTextMessageFromDataWithContentTypeTestImpl("application/json;charset=us-ascii", StandardCharsets.US_ASCII);
      doCreateTextMessageFromDataWithContentTypeTestImpl("application/json;charset=utf-8", StandardCharsets.UTF_8);
      doCreateTextMessageFromDataWithContentTypeTestImpl("application/json", StandardCharsets.UTF_8);
   }

   @Test
   public void testCreateTextMessageFromDataWithContentTypeApplicationJsonVariant() throws Exception {
      doCreateTextMessageFromDataWithContentTypeTestImpl("application/something+json;charset=iso-8859-1", StandardCharsets.ISO_8859_1);
      doCreateTextMessageFromDataWithContentTypeTestImpl("application/something+json;charset=us-ascii", StandardCharsets.US_ASCII);
      doCreateTextMessageFromDataWithContentTypeTestImpl("application/something+json;charset=utf-8", StandardCharsets.UTF_8);
      doCreateTextMessageFromDataWithContentTypeTestImpl("application/something+json", StandardCharsets.UTF_8);
   }

   @Test
   public void testCreateTextMessageFromDataWithContentTypeApplicationJavascript() throws Exception {
      doCreateTextMessageFromDataWithContentTypeTestImpl("application/javascript;charset=iso-8859-1", StandardCharsets.ISO_8859_1);
      doCreateTextMessageFromDataWithContentTypeTestImpl("application/javascript;charset=us-ascii", StandardCharsets.US_ASCII);
      doCreateTextMessageFromDataWithContentTypeTestImpl("application/javascript;charset=utf-8", StandardCharsets.UTF_8);
      doCreateTextMessageFromDataWithContentTypeTestImpl("application/javascript", StandardCharsets.UTF_8);
   }

   @Test
   public void testCreateTextMessageFromDataWithContentTypeApplicationEcmascript() throws Exception {
      doCreateTextMessageFromDataWithContentTypeTestImpl("application/ecmascript;charset=iso-8859-1", StandardCharsets.ISO_8859_1);
      doCreateTextMessageFromDataWithContentTypeTestImpl("application/ecmascript;charset=us-ascii", StandardCharsets.US_ASCII);
      doCreateTextMessageFromDataWithContentTypeTestImpl("application/ecmascript;charset=utf-8", StandardCharsets.UTF_8);
      doCreateTextMessageFromDataWithContentTypeTestImpl("application/ecmascript", StandardCharsets.UTF_8);
   }

   @Test
   public void testCreateTextMessageFromDataWithContentTypeApplicationXml() throws Exception {
      doCreateTextMessageFromDataWithContentTypeTestImpl("application/xml;charset=iso-8859-1", StandardCharsets.ISO_8859_1);
      doCreateTextMessageFromDataWithContentTypeTestImpl("application/xml;charset=us-ascii", StandardCharsets.US_ASCII);
      doCreateTextMessageFromDataWithContentTypeTestImpl("application/xml;charset=utf-8", StandardCharsets.UTF_8);
      doCreateTextMessageFromDataWithContentTypeTestImpl("application/xml", StandardCharsets.UTF_8);
   }

   @Test
   public void testCreateTextMessageFromDataWithContentTypeApplicationXmlVariant() throws Exception {
      doCreateTextMessageFromDataWithContentTypeTestImpl("application/something+xml;charset=iso-8859-1", StandardCharsets.ISO_8859_1);
      doCreateTextMessageFromDataWithContentTypeTestImpl("application/something+xml;charset=us-ascii", StandardCharsets.US_ASCII);
      doCreateTextMessageFromDataWithContentTypeTestImpl("application/something+xml;charset=utf-8", StandardCharsets.UTF_8);
      doCreateTextMessageFromDataWithContentTypeTestImpl("application/something+xml", StandardCharsets.UTF_8);
   }

   @Test
   public void testCreateTextMessageFromDataWithContentTypeApplicationXmlDtd() throws Exception {
      doCreateTextMessageFromDataWithContentTypeTestImpl("application/xml-dtd;charset=iso-8859-1", StandardCharsets.ISO_8859_1);
      doCreateTextMessageFromDataWithContentTypeTestImpl("application/xml-dtd;charset=us-ascii", StandardCharsets.US_ASCII);
      doCreateTextMessageFromDataWithContentTypeTestImpl("application/xml-dtd;charset=utf-8", StandardCharsets.UTF_8);
      doCreateTextMessageFromDataWithContentTypeTestImpl("application/xml-dtd", StandardCharsets.UTF_8);
   }

   private void doCreateTextMessageFromDataWithContentTypeTestImpl(String contentType, Charset expectedCharset) throws Exception {
      Message message = Proton.message();
      Binary binary = new Binary(new byte[0]);
      message.setBody(new Data(binary));
      message.setContentType(contentType);

      javax.jms.Message jmsMessage = ServerJMSMessage.wrapCoreMessage(new AMQPMessage(message).toCore());

      assertNotNull("Message should not be null", jmsMessage);
      if (StandardCharsets.UTF_8.equals(expectedCharset)) {
         assertEquals("Unexpected message class type", ServerJMSTextMessage.class, jmsMessage.getClass());
      } else {
         assertEquals("Unexpected message class type", ServerJMSBytesMessage.class, jmsMessage.getClass());
      }
   }

   // ----- AmqpValue transformations ----------------------------------------//

   /**
    * Test that an amqp-value body containing a string results in a TextMessage when not
    * otherwise annotated to indicate the type of JMS message it is.
    *
    * @throws Exception
    *         if an error occurs during the test.
    */
   @Test
   public void testCreateTextMessageFromAmqpValueWithString() throws Exception {
      Message message = Proton.message();
      message.setBody(new AmqpValue("content"));

      javax.jms.Message jmsMessage = ServerJMSMessage.wrapCoreMessage(new AMQPMessage(message).toCore());

      assertNotNull("Message should not be null", jmsMessage);
      assertEquals("Unexpected message class type", ServerJMSTextMessage.class, jmsMessage.getClass());
   }

   /**
    * Test that an amqp-value body containing a null results in an TextMessage when not
    * otherwise annotated to indicate the type of JMS message it is.
    *
    * @throws Exception
    *         if an error occurs during the test.
    */
   @Test
   public void testCreateTextMessageFromAmqpValueWithNull() throws Exception {
      Message message = Proton.message();
      message.setBody(new AmqpValue(null));

      javax.jms.Message jmsMessage = ServerJMSMessage.wrapCoreMessage(new AMQPMessage(message).toCore());

      assertNotNull("Message should not be null", jmsMessage);
      assertEquals("Unexpected message class type", ServerJMSTextMessage.class, jmsMessage.getClass());
   }

   /**
    * Test that a message with an AmqpValue section containing a Binary, but with the content
    * type set to {@value AMQPMessageSupport#SERIALIZED_JAVA_OBJECT_CONTENT_TYPE} results in an
    * ObjectMessage when not otherwise annotated to indicate the type of JMS message it is.
    *
    * @throws Exception
    *         if an error occurs during the test.
    */
   @Test
   public void testCreateObjectMessageFromAmqpValueWithBinaryAndContentType() throws Exception {
      Message message = Message.Factory.create();
      message.setBody(new AmqpValue(new Binary(new byte[0])));
      message.setContentType(AMQPMessageSupport.SERIALIZED_JAVA_OBJECT_CONTENT_TYPE.toString());

      javax.jms.Message jmsMessage = ServerJMSMessage.wrapCoreMessage(new AMQPMessage(message).toCore());

      assertNotNull("Message should not be null", jmsMessage);
      assertEquals("Unexpected message class type", ServerJMSObjectMessage.class, jmsMessage.getClass());
   }

   /**
    * Test that an amqp-value body containing a map results in an MapMessage when not otherwise
    * annotated to indicate the type of JMS message it is.
    *
    * @throws Exception
    *         if an error occurs during the test.
    */
   @Test
   public void testCreateAmqpMapMessageFromAmqpValueWithMap() throws Exception {
      Message message = Proton.message();
      Map<String, String> map = new HashMap<>();
      message.setBody(new AmqpValue(map));

      javax.jms.Message jmsMessage = ServerJMSMessage.wrapCoreMessage(new AMQPMessage(message).toCore());

      assertNotNull("Message should not be null", jmsMessage);
      assertEquals("Unexpected message class type", ServerJMSMapMessage.class, jmsMessage.getClass());
   }

   /**
    * Test that an amqp-value body containing a map that has an AMQP Binary as one of the
    * entries encoded into the Map results in an MapMessage where a byte array can be read from
    * the entry.
    *
    * @throws Exception
    *         if an error occurs during the test.
    */
   @Test
   public void testCreateAmqpMapMessageFromAmqpValueWithMapContainingBinaryEntry() throws Exception {
      final String ENTRY_NAME = "bytesEntry";

      Message message = Proton.message();
      Map<String, Object> map = new HashMap<>();

      byte[] inputBytes = new byte[] {1, 2, 3, 4, 5};
      map.put(ENTRY_NAME, new Binary(inputBytes));

      message.setBody(new AmqpValue(map));

      javax.jms.Message jmsMessage = ServerJMSMessage.wrapCoreMessage(new AMQPMessage(message).toCore());

      assertNotNull("Message should not be null", jmsMessage);
      assertEquals("Unexpected message class type", ServerJMSMapMessage.class, jmsMessage.getClass());

      MapMessage mapMessage = (MapMessage) jmsMessage;
      byte[] outputBytes = mapMessage.getBytes(ENTRY_NAME);
      assertNotNull(outputBytes);
      assertTrue(Arrays.equals(inputBytes, outputBytes));
   }

   /**
    * Test that an amqp-value body containing a list results in an StreamMessage when not
    * otherwise annotated to indicate the type of JMS message it is.
    *
    * @throws Exception
    *         if an error occurs during the test.
    */
   @Test
   public void testCreateAmqpStreamMessageFromAmqpValueWithList() throws Exception {
      Message message = Proton.message();
      List<String> list = new ArrayList<>();
      message.setBody(new AmqpValue(list));

      javax.jms.Message jmsMessage = ServerJMSMessage.wrapCoreMessage(new AMQPMessage(message).toCore());

      assertNotNull("Message should not be null", jmsMessage);
      assertEquals("Unexpected message class type", ServerJMSStreamMessage.class, jmsMessage.getClass());
   }

   /**
    * Test that an amqp-sequence body containing a list results in an StreamMessage when not
    * otherwise annotated to indicate the type of JMS message it is.
    *
    * @throws Exception
    *         if an error occurs during the test.
    */
   @Test
   public void testCreateAmqpStreamMessageFromAmqpSequence() throws Exception {
      Message message = Proton.message();
      List<String> list = new ArrayList<>();
      message.setBody(new AmqpSequence(list));

      javax.jms.Message jmsMessage = ServerJMSMessage.wrapCoreMessage(new AMQPMessage(message).toCore());

      assertNotNull("Message should not be null", jmsMessage);
      assertEquals("Unexpected message class type", ServerJMSStreamMessage.class, jmsMessage.getClass());
   }

   /**
    * Test that an amqp-value body containing a binary value results in BytesMessage when not
    * otherwise annotated to indicate the type of JMS message it is.
    *
    * @throws Exception
    *         if an error occurs during the test.
    */
   @Test
   public void testCreateAmqpBytesMessageFromAmqpValueWithBinary() throws Exception {
      Message message = Proton.message();
      Binary binary = new Binary(new byte[0]);
      message.setBody(new AmqpValue(binary));

      javax.jms.Message jmsMessage = ServerJMSMessage.wrapCoreMessage(new AMQPMessage(message).toCore());

      assertNotNull("Message should not be null", jmsMessage);
      assertEquals("Unexpected message class type", ServerJMSBytesMessage.class, jmsMessage.getClass());
   }

   /**
    * Test that an amqp-value body containing a value which can't be categorized results in an
    * exception from the transformer and then try the transformer's own fallback transformer to
    * result in an BytesMessage.
    *
    * @throws Exception
    *         if an error occurs during the test.
    */
   @Test
   public void testCreateBytesMessageFromAmqpValueWithUncategorisedContent() throws Exception {
      Message message = Proton.message();
      message.setBody(new AmqpValue(UUID.randomUUID()));

      javax.jms.Message jmsMessage = ServerJMSMessage.wrapCoreMessage(new AMQPMessage(message).toCore());

      assertNotNull("Message should not be null", jmsMessage);
      assertEquals("Unexpected message class type", ServerJMSBytesMessage.class, jmsMessage.getClass());
   }

   @Test
   public void testTransformMessageWithAmqpValueStringCreatesTextMessage() throws Exception {
      String contentString = "myTextMessageContent";
      Message message = Message.Factory.create();
      message.setBody(new AmqpValue(contentString));

      javax.jms.Message jmsMessage = ServerJMSMessage.wrapCoreMessage(new AMQPMessage(message).toCore());

      assertTrue("Expected TextMessage", jmsMessage instanceof TextMessage);
      assertEquals("Unexpected message class type", ServerJMSTextMessage.class, jmsMessage.getClass());

      TextMessage textMessage = (TextMessage) jmsMessage;

      assertNotNull(textMessage.getText());
      assertEquals(contentString, textMessage.getText());
   }

   // ----- Destination Conversions ------------------------------------------//

   @Test
   public void testTransformWithNoToTypeDestinationTypeAnnotation() throws Exception {
      doTransformWithToTypeDestinationTypeAnnotationTestImpl(null, Destination.class);
   }

   @Test
   public void testTransformWithQueueStringToTypeDestinationTypeAnnotation() throws Exception {
      doTransformWithToTypeDestinationTypeAnnotationTestImpl("queue", Queue.class);
   }

   @Test
   public void testTransformWithTemporaryQueueStringToTypeDestinationTypeAnnotation() throws Exception {
      doTransformWithToTypeDestinationTypeAnnotationTestImpl("queue,temporary", TemporaryQueue.class);
   }

   @Test
   public void testTransformWithTopicStringToTypeDestinationTypeAnnotation() throws Exception {
      doTransformWithToTypeDestinationTypeAnnotationTestImpl("topic", Topic.class);
   }

   @Test
   public void testTransformWithTemporaryTopicStringToTypeDestinationTypeAnnotation() throws Exception {
      doTransformWithToTypeDestinationTypeAnnotationTestImpl("topic,temporary", TemporaryTopic.class);
   }

   private void doTransformWithToTypeDestinationTypeAnnotationTestImpl(Object toTypeAnnotationValue, Class<? extends Destination> expectedClass)
      throws Exception {

      String toAddress = "toAddress";
      Message amqp = Message.Factory.create();
      amqp.setBody(new AmqpValue("myTextMessageContent"));
      amqp.setAddress(toAddress);
      if (toTypeAnnotationValue != null) {
         Map<Symbol, Object> map = new HashMap<>();
         map.put(Symbol.valueOf("x-opt-to-type"), toTypeAnnotationValue);
         MessageAnnotations ma = new MessageAnnotations(map);
         amqp.setMessageAnnotations(ma);
      }

      javax.jms.Message jmsMessage = ServerJMSMessage.wrapCoreMessage(new AMQPMessage(amqp).toCore());
      assertTrue("Expected TextMessage", jmsMessage instanceof TextMessage);
   }

   // ----- ReplyTo Conversions ----------------------------------------------//

   @Test
   public void testTransformWithNoReplyToTypeDestinationTypeAnnotation() throws Exception {
      doTransformWithReplyToTypeDestinationTypeAnnotationTestImpl(null, Destination.class);
   }

   @Test
   public void testTransformWithQueueStringReplyToTypeDestinationTypeAnnotation() throws Exception {
      doTransformWithReplyToTypeDestinationTypeAnnotationTestImpl("queue", Queue.class);
   }

   @Test
   public void testTransformWithTemporaryQueueStringReplyToTypeDestinationTypeAnnotation() throws Exception {
      doTransformWithReplyToTypeDestinationTypeAnnotationTestImpl("queue,temporary", TemporaryQueue.class);
   }

   @Test
   public void testTransformWithTopicStringReplyToTypeDestinationTypeAnnotation() throws Exception {
      doTransformWithReplyToTypeDestinationTypeAnnotationTestImpl("topic", Topic.class);
   }

   @Test
   public void testTransformWithTemporaryTopicStringReplyToTypeDestinationTypeAnnotation() throws Exception {
      doTransformWithReplyToTypeDestinationTypeAnnotationTestImpl("topic,temporary", TemporaryTopic.class);
   }

   private void doTransformWithReplyToTypeDestinationTypeAnnotationTestImpl(Object replyToTypeAnnotationValue, Class<? extends Destination> expectedClass)
      throws Exception {

      String replyToAddress = "replyToAddress";
      Message amqp = Message.Factory.create();
      amqp.setBody(new AmqpValue("myTextMessageContent"));
      amqp.setReplyTo(replyToAddress);
      if (replyToTypeAnnotationValue != null) {
         Map<Symbol, Object> map = new HashMap<>();
         map.put(Symbol.valueOf("x-opt-reply-type"), replyToTypeAnnotationValue);
         MessageAnnotations ma = new MessageAnnotations(map);
         amqp.setMessageAnnotations(ma);
      }

      javax.jms.Message jmsMessage = ServerJMSMessage.wrapCoreMessage(new AMQPMessage(amqp).toCore());
      assertTrue("Expected TextMessage", jmsMessage instanceof TextMessage);
   }

}
