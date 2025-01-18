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
package org.apache.activemq.artemis.tests.integration.amqp;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSender;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

/**
 * Test for scheduled message support using AMQP message annotations.
 */
public class AmqpDuplicationDetectionTest extends AmqpClientTestSupport {

   @Test
   @Timeout(60)
   public void testSimpleDuplicateDetection() throws Exception {
      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());

      try {
         AmqpSession session = connection.createSession();

         AmqpSender sender = session.createSender(getQueueName());
         AmqpReceiver receiver = session.createReceiver(getQueueName());

         // Get the Queue View early to avoid racing the delivery.
         final Queue queueView = getProxyToQueue(getQueueName());
         assertNotNull(queueView);

         String dupID = "batata";
         AmqpMessage message = new AmqpMessage();
         message.setApplicationProperty(org.apache.activemq.artemis.api.core.Message.HDR_DUPLICATE_DETECTION_ID.toString(), dupID);
         message.setText("Test-Message");
         sender.send(message);

         receiver.flow(1);
         AmqpMessage received = receiver.receive(5, TimeUnit.MILLISECONDS);
         assertNotNull(received);

         // Retry message with same dupID
         AmqpMessage message2 = new AmqpMessage();
         message2.setApplicationProperty(org.apache.activemq.artemis.api.core.Message.HDR_DUPLICATE_DETECTION_ID.toString(), dupID);
         message2.setText("Test-Message");
         sender.send(message2);

         // But does not receive because is duplicated
         receiver.flow(1);
         AmqpMessage received2 = receiver.receive(5, TimeUnit.MILLISECONDS);
         assertNull(received2);

         // Now try with a different id
         String dupID2 = "doce";
         AmqpMessage message3 = new AmqpMessage();
         message3.setApplicationProperty(org.apache.activemq.artemis.api.core.Message.HDR_DUPLICATE_DETECTION_ID.toString(), dupID2);
         message3.setText("Test-Message");
         sender.send(message3);

         receiver.flow(1);
         AmqpMessage received3 = receiver.receive(5, TimeUnit.MILLISECONDS);
         assertNotNull(received3);

         sender.close();
      } finally {
         connection.close();
      }
   }
}

