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
import static org.apache.activemq.artemis.core.protocol.openwire.OpenWireMessageSupport.AMQ_MSG_MESSAGE_ID;

import static org.apache.activemq.artemis.api.core.Message.TEXT_TYPE;
import static org.apache.activemq.artemis.api.core.Message.BYTES_TYPE;
import static org.apache.activemq.artemis.api.core.Message.STREAM_TYPE;
import static org.apache.activemq.artemis.api.core.Message.OBJECT_TYPE;
import static org.apache.activemq.artemis.api.core.Message.MAP_TYPE;

import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.command.ActiveMQMapMessage;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQObjectMessage;
import org.apache.activemq.command.ActiveMQStreamMessage;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.MessageId;
import org.junit.Test;

public class OpenWireCoreConverterTest extends ConverterTest {

   @Test
   public void getBinaryMessageProperty() throws Exception {
      org.apache.activemq.command.Message amqMessage = new ActiveMQMessage();
      amqMessage.setMessageId(fooMessageId);
      ICoreMessage coreMessage = OpenWireCoreConverter.createInboundMessage(amqMessage, format, null);
      assertTrue(coreMessage instanceof org.apache.activemq.artemis.api.core.Message);
      MessageId messageId = (MessageId) getBinaryObjectProperty(format, coreMessage, AMQ_MSG_MESSAGE_ID);
      assertEquals(fooMessageId, messageId);
   }

   @Test
   public void convertTextMessage() throws Exception {
      ActiveMQTextMessage amqMessage = new ActiveMQTextMessage();
      amqMessage.setContent(encodeOpenWireContent("test"));
      ICoreMessage coreMessage = OpenWireCoreConverter.createInboundMessage(amqMessage, format, null);
      assertTrue(coreMessage instanceof org.apache.activemq.artemis.api.core.Message);
      assertEquals(TEXT_TYPE, coreMessage.getType());
   }

   @Test
   public void convertBytesMessage() throws Exception {
      ActiveMQBytesMessage amqMessage = new ActiveMQBytesMessage();
      amqMessage.setContent(encodeOpenWireContent("test"));
      ICoreMessage coreMessage = OpenWireCoreConverter.createInboundMessage(amqMessage, format, null);
      assertTrue(coreMessage instanceof org.apache.activemq.artemis.api.core.Message);
      assertEquals(BYTES_TYPE, coreMessage.getType());
   }

   @Test
   public void convertStreamMessage() throws Exception {
      ActiveMQStreamMessage amqMessage = new ActiveMQStreamMessage();
      amqMessage.setContent(encodeOpenWireContent("test"));
      ICoreMessage coreMessage = OpenWireCoreConverter.createInboundMessage(amqMessage, format, null);
      assertTrue(coreMessage instanceof org.apache.activemq.artemis.api.core.Message);
      assertEquals(STREAM_TYPE, coreMessage.getType());
   }

   @Test
   public void convertObjectMessage() throws Exception {
      ActiveMQObjectMessage amqMessage = new ActiveMQObjectMessage();
      amqMessage.setContent(encodeOpenWireContent("test"));
      ICoreMessage coreMessage = OpenWireCoreConverter.createInboundMessage(amqMessage, format, null);
      assertTrue(coreMessage instanceof org.apache.activemq.artemis.api.core.Message);
      assertEquals(OBJECT_TYPE, coreMessage.getType());
   }

   @Test
   public void convertMapMessage() throws Exception {
      ActiveMQMapMessage amqMessage = new ActiveMQMapMessage();
      amqMessage.setString("user", "fvaleri");
      ICoreMessage coreMessage = OpenWireCoreConverter.createInboundMessage(amqMessage, format, null);
      assertTrue(coreMessage instanceof org.apache.activemq.artemis.api.core.Message);
      assertEquals(MAP_TYPE, coreMessage.getType());
   }

}
