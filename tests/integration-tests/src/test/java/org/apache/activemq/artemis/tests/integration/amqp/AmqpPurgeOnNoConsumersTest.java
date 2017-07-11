/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.amqp;

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class AmqpPurgeOnNoConsumersTest extends AmqpClientTestSupport {

   @Test(timeout = 60000)
   public void testQueueReceiverReadMessage() throws Exception {
      String queue = "purgeQueue";
      SimpleString ssQueue = new SimpleString(queue);
      server.addAddressInfo(new AddressInfo(ssQueue, RoutingType.ANYCAST));
      server.createQueue(ssQueue, RoutingType.ANYCAST, ssQueue, null, true, false, 1, true, false);

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      final AmqpReceiver receiver = session.createReceiver(queue);

      Queue queueView = getProxyToQueue(queue);
      assertEquals(0, queueView.getMessageCount());

      Thread t = new Thread(new Runnable() {
         @Override
         public void run() {
            for (int i = 0; i < 4; i++) {
               try {
                  AmqpMessage receive = receiver.receive(5, TimeUnit.SECONDS);
                  receive.accept();
                  assertNotNull(receive);
               } catch (Exception e) {
                  e.printStackTrace();
               }
            }
            try {
               receiver.close();
            } catch (IOException e) {
               e.printStackTrace();
            }
         }
      });

      t.start();

      receiver.flow(5);

      sendMessages(queue, 5);

      t.join(5000);

      assertEquals(0, queueView.getMessageCount());

      connection.close();
   }
}
