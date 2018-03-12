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

import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.files.FileStoreMonitor;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpSender;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.junit.Test;

import java.net.URI;
import java.nio.file.FileStore;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class GlobalDiskFullTest extends AmqpClientTestSupport {

   @Override
   protected void addConfiguration(ActiveMQServer server) {
      Configuration serverConfig = server.getConfiguration();
      serverConfig.setDiskScanPeriod(100);
   }

   @Test
   public void testProducerOnDiskFull() throws Exception {
      FileStoreMonitor monitor = ((ActiveMQServerImpl)server).getMonitor().setMaxUsage(0.0);
      final CountDownLatch latch = new CountDownLatch(1);
      monitor.addCallback(new FileStoreMonitor.Callback() {
         @Override
         public void over(FileStore store, double usage) {
            latch.countDown();
         }
         @Override
         public void under(FileStore store, double usage) {
         }
      });
      latch.await(2, TimeUnit.SECONDS);

      AmqpClient client = createAmqpClient(new URI("tcp://localhost:" + AMQP_PORT));
      AmqpConnection connection = addConnection(client.connect());

      try {
         AmqpSession session = connection.createSession();
         AmqpSender sender = session.createSender(getQueueName());
         final AmqpMessage message = new AmqpMessage();
         byte[] payload = new byte[1000];
         message.setBytes(payload);

         sender.setSendTimeout(1000);
         sender.send(message);

         org.apache.activemq.artemis.core.server.Queue queueView = getProxyToQueue(getQueueName());
         assertEquals("shouldn't receive any messages", 0, queueView.getMessageCount());

         AmqpSender anonSender = session.createSender();
         final AmqpMessage message1 = new AmqpMessage();
         message1.setBytes(payload);
         message1.setAddress(getQueueName());

         anonSender.setSendTimeout(1000);
         anonSender.send(message1);

         queueView = getProxyToQueue(getQueueName());
         assertEquals("shouldn't receive any messages", 0, queueView.getMessageCount());

      } finally {
         connection.close();
      }
   }
}
