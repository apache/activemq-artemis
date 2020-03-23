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
package org.apache.activemq.artemis.core.protocol.openwire;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import static org.apache.activemq.artemis.core.protocol.openwire.OpenWireMessageSupport.setBinaryObjectProperty;
import static org.apache.activemq.artemis.core.protocol.openwire.OpenWireMessageSupport.AMQ_MSG_MESSAGE_ID;

import static org.apache.activemq.artemis.api.core.Message.TEXT_TYPE;
import static org.apache.activemq.artemis.api.core.Message.BYTES_TYPE;
import static org.apache.activemq.artemis.api.core.Message.STREAM_TYPE;
import static org.apache.activemq.artemis.api.core.Message.OBJECT_TYPE;
import static org.apache.activemq.artemis.api.core.Message.MAP_TYPE;

import static org.apache.activemq.command.CommandTypes.ACTIVEMQ_TEXT_MESSAGE;
import static org.apache.activemq.command.CommandTypes.ACTIVEMQ_BYTES_MESSAGE;
import static org.apache.activemq.command.CommandTypes.ACTIVEMQ_STREAM_MESSAGE;
import static org.apache.activemq.command.CommandTypes.ACTIVEMQ_OBJECT_MESSAGE;
import static org.apache.activemq.command.CommandTypes.ACTIVEMQ_MAP_MESSAGE;

import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.core.protocol.openwire.amq.AMQConsumer;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.impl.MessageReferenceImpl;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.MessageDispatch;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

public class CoreOpenWireConverterTest extends ConverterTest {

   @Rule
   public MockitoRule rule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

   private AMQConsumer getConsumerMock() {
      AMQConsumer consumer = Mockito.mock(AMQConsumer.class);
      Mockito.when(consumer.getId()).thenReturn(new ConsumerId("123"));
      Mockito.when(consumer.getOpenwireDestination()).thenReturn(fooDestination);
      return consumer;
   }

   @Test
   public void setBinaryMessageProperty() throws Exception {
      ICoreMessage coreMessage = new CoreMessage(1, 1024);
      setBinaryObjectProperty(format, coreMessage, AMQ_MSG_MESSAGE_ID, fooMessageId);
      MessageReference reference = new MessageReferenceImpl(coreMessage, null);
      MessageDispatch amqDispatch = CoreOpenWireConverter.createMessageDispatch(reference, coreMessage, format,
            getConsumerMock());
      assertTrue(amqDispatch.getMessage() instanceof org.apache.activemq.command.Message);
      assertEquals(fooMessageId, amqDispatch.getMessage().getMessageId());
   }

   @Test
   public void convertTextMessage() throws Exception {
      ICoreMessage coreMessage = new CoreMessage(1, 1024);
      coreMessage.setType(TEXT_TYPE);
      coreMessage.getBodyBuffer().writeString("test");
      MessageReference reference = new MessageReferenceImpl(coreMessage, null);
      MessageDispatch amqDispatch = CoreOpenWireConverter.createMessageDispatch(reference, coreMessage, format,
            getConsumerMock());
      assertTrue(amqDispatch.getMessage() instanceof org.apache.activemq.command.Message);
      assertEquals(ACTIVEMQ_TEXT_MESSAGE, amqDispatch.getMessage().getDataStructureType());
   }

   @Test
   public void convertBytesMessage() throws Exception {
      ICoreMessage coreMessage = new CoreMessage(1, 1024);
      coreMessage.setType(BYTES_TYPE);
      coreMessage.getBodyBuffer().writeString("test");
      MessageReference reference = new MessageReferenceImpl(coreMessage, null);
      MessageDispatch amqDispatch = CoreOpenWireConverter.createMessageDispatch(reference, coreMessage, format,
            getConsumerMock());
      assertTrue(amqDispatch.getMessage() instanceof org.apache.activemq.command.Message);
      assertEquals(ACTIVEMQ_BYTES_MESSAGE, amqDispatch.getMessage().getDataStructureType());
   }

   @Test
   public void convertStreamMessage() throws Exception {
      ICoreMessage coreMessage = new CoreMessage(1, 1024);
      coreMessage.setType(STREAM_TYPE);
      coreMessage.getBodyBuffer().writeString("test");
      MessageReference reference = new MessageReferenceImpl(coreMessage, null);
      MessageDispatch amqDispatch = CoreOpenWireConverter.createMessageDispatch(reference, coreMessage, format,
            getConsumerMock());
      assertTrue(amqDispatch.getMessage() instanceof org.apache.activemq.command.Message);
      assertEquals(ACTIVEMQ_STREAM_MESSAGE, amqDispatch.getMessage().getDataStructureType());
   }

   @Test
   public void convertObjectMessage() throws Exception {
      ICoreMessage coreMessage = new CoreMessage(1, 1024);
      coreMessage.setType(OBJECT_TYPE);
      coreMessage.getBodyBuffer().writeString("test");
      MessageReference reference = new MessageReferenceImpl(coreMessage, null);
      MessageDispatch amqDispatch = CoreOpenWireConverter.createMessageDispatch(reference, coreMessage, format,
            getConsumerMock());
      assertTrue(amqDispatch.getMessage() instanceof org.apache.activemq.command.Message);
      assertEquals(ACTIVEMQ_OBJECT_MESSAGE, amqDispatch.getMessage().getDataStructureType());
   }

   @Test
   public void convertMapMessage() throws Exception {
      ICoreMessage coreMessage = new CoreMessage(1, 1024);
      coreMessage.setType(MAP_TYPE);
      MessageReference reference = new MessageReferenceImpl(coreMessage, null);
      MessageDispatch amqDispatch = CoreOpenWireConverter.createMessageDispatch(reference, coreMessage, format,
            getConsumerMock());
      assertTrue(amqDispatch.getMessage() instanceof org.apache.activemq.command.Message);
      assertEquals(ACTIVEMQ_MAP_MESSAGE, amqDispatch.getMessage().getDataStructureType());
   }

}
