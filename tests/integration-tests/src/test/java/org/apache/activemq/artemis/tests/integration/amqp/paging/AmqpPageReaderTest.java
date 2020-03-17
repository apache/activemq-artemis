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

package org.apache.activemq.artemis.tests.integration.amqp.paging;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.paging.cursor.impl.PageReaderTest;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPStandardMessage;
import org.apache.activemq.artemis.spi.core.protocol.MessagePersister;
import org.apache.activemq.artemis.tests.integration.amqp.AmqpTestSupport;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.qpid.proton.message.impl.MessageImpl;
import org.junit.Assert;
import org.junit.Test;

public class AmqpPageReaderTest extends PageReaderTest {

   public MessageImpl createProtonMessage(String address) {
      AmqpMessage message = new AmqpMessage();
      final StringBuilder builder = new StringBuilder();
      for (int i = 0; i < 1000; i++) {
         builder.append('0');
      }
      final String data = builder.toString();
      message.setText(data);
      message.setAddress(address);
      message.setDurable(true);

      MessageImpl protonMessage = (MessageImpl) message.getWrappedMessage();

      return protonMessage;
   }

   @Override
   protected Message createMessage(SimpleString address, int msgId, byte[] content) {
      MessageImpl protonMessage = createProtonMessage(address.toString());
      AMQPStandardMessage amqpStandardMessage =  AmqpTestSupport.encodeAndDecodeMessage(0, protonMessage, 2 * 1024);
      amqpStandardMessage.setMessageID(msgId);

      return amqpStandardMessage;
   }


   @Test
   public void testEncodeSize() throws Exception {

      Message message = createMessage(SimpleString.toSimpleString("Test"), 1, new byte[10]);

      MessagePersister persister = (MessagePersister)message.getPersister();

      ActiveMQBuffer buffer = ActiveMQBuffers.dynamicBuffer(1024);
      persister.encode(buffer, message);

      Assert.assertEquals(persister.getEncodeSize(message), buffer.writerIndex());

      // the very first byte is the persisterID, we skip that since we are calling the Persister directly
      buffer.readerIndex(1);
      Message messageRead = persister.decode(buffer, null, null);

      // The current persister does not guarantee the same encode size after loading
      /// if this ever changes we can uncomment the next line.
      // Assert.assertEquals(persister.getEncodeSize(message), persister.getEncodeSize(messageRead));


   }
}
