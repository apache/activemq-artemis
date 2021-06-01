/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.core.protocol.openwire;

import org.apache.activemq.ActiveMQMessageAuditNoSync;
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.core.protocol.openwire.amq.AMQConsumer;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.impl.MessageReferenceImpl;
import org.apache.activemq.artemis.reader.MessageUtil;
import org.apache.activemq.artemis.utils.UUID;
import org.apache.activemq.artemis.utils.UUIDGenerator;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.openwire.OpenWireFormatFactory;
import org.apache.activemq.wireformat.WireFormat;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertFalse;

public class OpenWireMessageConverterTest {

   final OpenWireFormatFactory formatFactory = new OpenWireFormatFactory();
   final WireFormat openWireFormat =  formatFactory.createWireFormat();
   final byte[] content = new byte[] {'a','a'};
   final String address = "Q";
   final ActiveMQDestination destination = new ActiveMQQueue(address);
   final UUID nodeUUID = UUIDGenerator.getInstance().generateUUID();

   @Test
   public void createMessageDispatch() throws Exception {

      ActiveMQMessageAuditNoSync mqMessageAuditNoSync = new ActiveMQMessageAuditNoSync();

      for (int i = 0; i < 10; i++) {

         ICoreMessage msg = new CoreMessage().initBuffer(100);
         msg.setMessageID(i);
         msg.getBodyBuffer().writeBytes(content);
         msg.setAddress(address);

         MessageReference messageReference = new MessageReferenceImpl(msg, Mockito.mock(Queue.class));
         AMQConsumer amqConsumer = Mockito.mock(AMQConsumer.class);
         Mockito.when(amqConsumer.getOpenwireDestination()).thenReturn(destination);

         MessageDispatch dispatch = OpenWireMessageConverter.createMessageDispatch(messageReference, msg, openWireFormat, amqConsumer, nodeUUID);

         MessageId messageId = dispatch.getMessage().getMessageId();
         assertFalse(mqMessageAuditNoSync.isDuplicate(messageId));
      }


      for (int i = 10; i < 20; i++) {

         CoreMessage msg = new CoreMessage().initBuffer(100);
         msg.setMessageID(i);
         msg.getBodyBuffer().writeBytes(content);
         msg.setAddress(address);

         // share a connection id
         msg.getProperties().putProperty(MessageUtil.CONNECTION_ID_PROPERTY_NAME, "MyClient");


         MessageReference messageReference = new MessageReferenceImpl(msg, Mockito.mock(Queue.class));
         AMQConsumer amqConsumer = Mockito.mock(AMQConsumer.class);
         Mockito.when(amqConsumer.getOpenwireDestination()).thenReturn(destination);

         MessageDispatch dispatch = OpenWireMessageConverter.createMessageDispatch(messageReference, msg, openWireFormat, amqConsumer, nodeUUID);

         MessageId messageId = dispatch.getMessage().getMessageId();
         assertFalse(mqMessageAuditNoSync.isDuplicate(messageId));
      }

   }
}