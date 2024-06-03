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
package org.apache.activemq.artemis.tests.integration.amqp.journal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPMessage;
import org.apache.activemq.artemis.tests.integration.amqp.AmqpClientTestSupport;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.junit.jupiter.api.Test;

public class AmqpJournalLoadingTest extends AmqpClientTestSupport {

   @Test
   public void durableMessageDataNotScannedOnRestartTest() throws Exception {
      sendMessages(getQueueName(), 1, true);
      final Queue queueView = getProxyToQueue(getQueueName());
      Wait.assertTrue("All messages should arrive", () -> queueView.getMessageCount() == 1);

      server.stop();
      server.start();

      final Queue afterRestartQueueView = getProxyToQueue(getQueueName());

      Wait.assertTrue("All messages should arrive", () -> afterRestartQueueView.getMessageCount() == 1);

      ArrayList<AMQPMessage> messageReference = new ArrayList<>(1);

      afterRestartQueueView.forEach((next) -> {
         final AMQPMessage message = (AMQPMessage)next.getMessage();
         assertEquals(AMQPMessage.MessageDataScanningStatus.RELOAD_PERSISTENCE, message.getDataScanningStatus());
         assertTrue(message.isDurable());
         // Doing the check again in case isDurable messed up the scanning status. It should not change the status by definition
         assertEquals(AMQPMessage.MessageDataScanningStatus.RELOAD_PERSISTENCE, message.getDataScanningStatus());
         messageReference.add(message);
      });

      assertEquals(1, messageReference.size());

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpReceiver receiver = session.createReceiver(getQueueName());

      receiver.flow(1);
      AmqpMessage receive = receiver.receive(5, TimeUnit.SECONDS);
      assertNotNull(receive);
      assertTrue(receive.isDurable());

      assertEquals(1, afterRestartQueueView.getMessageCount());

      assertEquals(AMQPMessage.MessageDataScanningStatus.SCANNED, messageReference.get(0).getDataScanningStatus());


      receive.accept();

      receiver.close();

      Wait.assertEquals(0, () -> afterRestartQueueView.getMessageCount());

      connection.close();
   }

}
