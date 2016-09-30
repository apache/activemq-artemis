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
package org.apache.activemq.artemis.tests.integration.amqp;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSender;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.message.impl.MessageImpl;
import org.junit.Test;

public class ProtonMaxFrameSizeTest extends ProtonTestBase {

   private static final int FRAME_SIZE = 512;

   @Override
   protected void configureAmqp(Map<String, Object> params) {
      params.put("maxFrameSize", FRAME_SIZE);
   }

   @Test
   public void testMultipleTransfers() throws Exception {

      String testQueueName = "ConnectionFrameSize";
      int nMsgs = 200;

      AmqpClient client = new AmqpClient(new URI(tcpAmqpConnectionUri), userName, password);

      AmqpConnection amqpConnection = client.createConnection();

      try {
         amqpConnection.connect();

         AmqpSession session = amqpConnection.createSession();
         AmqpSender sender = session.createSender("jms.queue." + testQueueName);

         final int payload = FRAME_SIZE * 16;

         for (int i = 0; i < nMsgs; ++i) {
            AmqpMessage message = createAmqpMessage((byte) 'A', payload);
            sender.send(message);
         }

         int count = getMessageCount(server.getPostOffice(), "jms.queue." + testQueueName);
         assertEquals(nMsgs, count);

         AmqpReceiver receiver = session.createReceiver("jms.queue." + testQueueName);
         receiver.flow(nMsgs);

         for (int i = 0; i < nMsgs; ++i) {
            AmqpMessage message = receiver.receive(5, TimeUnit.SECONDS);
            assertNotNull("failed at " + i, message);
            MessageImpl wrapped = (MessageImpl) message.getWrappedMessage();
            Data data = (Data) wrapped.getBody();
            System.out.println("received : message: " + data.getValue().getLength());
            assertEquals(payload, data.getValue().getLength());
            message.accept();
         }

      } finally {
         amqpConnection.close();
      }
   }

   private AmqpMessage createAmqpMessage(byte value, int payloadSize) {
      AmqpMessage message = new AmqpMessage();
      byte[] payload = new byte[payloadSize];
      for (int i = 0; i < payload.length; i++) {
         payload[i] = value;
      }
      message.setBytes(payload);
      return message;
   }

}
