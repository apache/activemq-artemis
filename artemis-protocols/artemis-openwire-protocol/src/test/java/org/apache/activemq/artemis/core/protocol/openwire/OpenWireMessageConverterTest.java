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

import static org.apache.activemq.artemis.core.protocol.openwire.OpenWireMessageSupport.getBinaryObjectProperty;
import static org.apache.activemq.artemis.core.protocol.openwire.OpenWireMessageSupport.setBinaryObjectProperty;
import static org.apache.activemq.artemis.core.protocol.openwire.OpenWireMessageSupport.AMQ_MSG_DATASTRUCTURE;
import static org.apache.activemq.artemis.core.protocol.openwire.OpenWireMessageSupport.AMQ_MSG_MESSAGE_ID;
import static org.apache.activemq.artemis.core.protocol.openwire.OpenWireMessageSupport.AMQ_MSG_ORIG_DESTINATION;
import static org.apache.activemq.artemis.core.protocol.openwire.OpenWireMessageSupport.AMQ_MSG_PRODUCER_ID;

import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.core.protocol.openwire.amq.AMQConsumer;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.impl.MessageReferenceImpl;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.BrokerId;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.MessageDispatch;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

public class OpenWireMessageConverterTest extends ConverterTest {

   @Rule
   public MockitoRule rule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

   @Test
   public void convertOpenWireToCore() throws Exception {
      ActiveMQMessage amqMessage = new ActiveMQMessage();
      amqMessage.setPersistent(true);
      amqMessage.setExpiration(0);
      amqMessage.setPriority((byte) javax.jms.Message.DEFAULT_PRIORITY);
      amqMessage.setTimestamp(System.currentTimeMillis());
      amqMessage.setContent(encodeOpenWireContent("test"));
      amqMessage.setCompressed(false);
      amqMessage.setArrival(System.currentTimeMillis());
      amqMessage.setBrokerInTime(System.currentTimeMillis());
      amqMessage.setBrokerPath(new BrokerId[] {new BrokerId("broker1"), new BrokerId("broker2")});
      amqMessage.setCluster(new BrokerId[] {new BrokerId("broker1"), new BrokerId("broker2")});
      amqMessage.setCommandId(1);
      amqMessage.setGroupID("group1");
      amqMessage.setGroupSequence(1);
      amqMessage.setMessageId(fooMessageId);
      amqMessage.setUserID("user1");
      amqMessage.setProducerId(foorProducerId);
      amqMessage.setReplyTo(new ActiveMQQueue("bar"));
      amqMessage.setDroppable(false);
      amqMessage.setOriginalDestination(fooDestination);

      ICoreMessage coreMessage = OpenWireMessageConverter.getInstance().createInboundMessage(amqMessage, format, null);
      assertTrue(coreMessage instanceof org.apache.activemq.artemis.api.core.Message);

      MessageId messageId = (MessageId) getBinaryObjectProperty(format, coreMessage, AMQ_MSG_MESSAGE_ID);
      assertEquals(fooMessageId, messageId);
   }

   @Test
   public void convertCoreToOpenWire() throws Exception {
      AMQConsumer consumer = Mockito.mock(AMQConsumer.class);
      Mockito.when(consumer.getId()).thenReturn(new ConsumerId("123"));
      Mockito.when(consumer.getOpenwireDestination()).thenReturn(fooDestination);

      ICoreMessage coreMessage = new CoreMessage(1, 1024);
      coreMessage.getBodyBuffer().writeString("test");
      setBinaryObjectProperty(format, coreMessage, AMQ_MSG_DATASTRUCTURE, fooDestination);
      setBinaryObjectProperty(format, coreMessage, AMQ_MSG_MESSAGE_ID, fooMessageId);
      setBinaryObjectProperty(format, coreMessage, AMQ_MSG_ORIG_DESTINATION, fooDestination);
      setBinaryObjectProperty(format, coreMessage, AMQ_MSG_PRODUCER_ID, foorProducerId);

      MessageReference reference = new MessageReferenceImpl(coreMessage, null);
      MessageDispatch amqDispatch = OpenWireMessageConverter.getInstance().createMessageDispatch(reference, coreMessage,
            format, consumer);

      assertTrue(amqDispatch.getMessage() instanceof org.apache.activemq.command.Message);
      assertEquals(fooMessageId, amqDispatch.getMessage().getMessageId());
   }

}
