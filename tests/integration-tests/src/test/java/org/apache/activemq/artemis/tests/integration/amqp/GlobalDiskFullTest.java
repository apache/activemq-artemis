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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.files.FileStoreMonitor;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpSender;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.junit.Assert;
import org.junit.Test;

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
         public void tick(long usableSpace, long totalSpace) {
         }

         @Override
         public void over(long usableSpace, long totalSpace) {
            latch.countDown();
         }
         @Override
         public void under(long usableSpace, long totalSpace) {
         }
      });

      Assert.assertTrue(latch.await(1, TimeUnit.MINUTES));

      AmqpClient client = createAmqpClient(new URI("tcp://localhost:" + AMQP_PORT));
      AmqpConnection connection = addConnection(client.connect());

      try {
         AmqpSession session = connection.createSession();
         AmqpSender sender = session.createSender(getQueueName());
         byte[] payload = new byte[1000];


         AmqpSender anonSender = session.createSender();

         CountDownLatch sentWithName = new CountDownLatch(1);
         CountDownLatch sentAnon = new CountDownLatch(1);

         Thread threadWithName = new Thread() {
            @Override
            public void run() {

               try {
                  final AmqpMessage message = new AmqpMessage();
                  message.setBytes(payload);
                  sender.setSendTimeout(-1);
                  sender.send(message);
               } catch (Exception e) {
                  e.printStackTrace();
               } finally {
                  sentWithName.countDown();
               }
            }
         };

         threadWithName.start();


         Thread threadWithAnon = new Thread() {
            @Override
            public void run() {
               try {
                  final AmqpMessage message = new AmqpMessage();
                  message.setBytes(payload);
                  anonSender.setSendTimeout(-1);
                  message.setAddress(getQueueName());
                  anonSender.send(message);
               } catch (Exception e) {
                  e.printStackTrace();
               } finally {
                  sentAnon.countDown();
               }
            }
         };

         threadWithAnon.start();

         Assert.assertFalse("Thread sender should be blocked", sentWithName.await(500, TimeUnit.MILLISECONDS));
         Assert.assertFalse("Thread sender anonymous should be blocked", sentAnon.await(500, TimeUnit.MILLISECONDS));

         monitor.setMaxUsage(100.0);

         Assert.assertTrue("Thread sender should be released", sentWithName.await(30, TimeUnit.SECONDS));
         Assert.assertTrue("Thread sender anonymous should be released", sentAnon.await(30, TimeUnit.SECONDS));

         threadWithName.join(TimeUnit.SECONDS.toMillis(30));
         threadWithAnon.join(TimeUnit.SECONDS.toMillis(30));
         Assert.assertFalse(threadWithName.isAlive());
         Assert.assertFalse(threadWithAnon.isAlive());
      } finally {
         connection.close();
      }
   }
}
