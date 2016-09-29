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
package org.apache.activemq.artemis.tests.integration.client;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.jms.client.ActiveMQTextMessage;
import org.apache.activemq.artemis.tests.integration.IntegrationTestLogger;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.ActiveMQThreadFactory;
import org.junit.Assert;
import org.junit.Test;

public class CoreClientTest extends ActiveMQTestBase {

   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   @Test
   public void testCoreClientNetty() throws Exception {
      testCoreClient(true, null);
   }

   @Test
   public void testCoreClientInVM() throws Exception {
      testCoreClient(false, null);
   }

   @Test
   public void testCoreClientWithInjectedThreadPools() throws Exception {

      ExecutorService threadPool = Executors.newCachedThreadPool(ActiveMQThreadFactory.defaultThreadFactory());
      ScheduledThreadPoolExecutor scheduledThreadPool = new ScheduledThreadPoolExecutor(10);

      ServerLocator locator = createNonHALocator(false);
      boolean setThreadPools = locator.setThreadPools(threadPool, scheduledThreadPool);

      assertTrue(setThreadPools);
      testCoreClient(true, locator);

      threadPool.shutdown();
      scheduledThreadPool.shutdown();

      threadPool.awaitTermination(60, TimeUnit.SECONDS);
      scheduledThreadPool.awaitTermination(60, TimeUnit.SECONDS);
   }

   @Test
   public void testCoreClientWithGlobalThreadPoolParamtersChanged() throws Exception {

      int originalScheduled = ActiveMQClient.getGlobalScheduledThreadPoolSize();
      int originalGlobal = ActiveMQClient.getGlobalThreadPoolSize();

      try {
         ActiveMQClient.setGlobalThreadPoolProperties(2, 1);
         ActiveMQClient.clearThreadPools();
         ServerLocator locator = createNonHALocator(false);
         testCoreClient(true, locator);
      } finally {
         // restoring original value otherwise future tests would be screwed up
         ActiveMQClient.setGlobalThreadPoolProperties(originalGlobal, originalScheduled);
         ActiveMQClient.clearThreadPools();
      }
   }

   private void testCoreClient(final boolean netty, ServerLocator serverLocator) throws Exception {
      final SimpleString QUEUE = new SimpleString("CoreClientTestQueue");

      ActiveMQServer server = addServer(ActiveMQServers.newActiveMQServer(createDefaultConfig(netty), false));

      server.start();

      ServerLocator locator = serverLocator == null ? createNonHALocator(netty) : serverLocator;

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 1000;

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = session.createMessage(ActiveMQTextMessage.TYPE, false, 0, System.currentTimeMillis(), (byte) 1);

         message.putStringProperty("foo", "bar");

         // One way around the setting destination problem is as follows -
         // Remove destination as an attribute from client producer.
         // The destination always has to be set explicitly before sending a message

         message.setAddress(QUEUE);

         message.getBodyBuffer().writeString("testINVMCoreClient");

         producer.send(message);
      }

      CoreClientTest.log.info("sent messages");

      ClientConsumer consumer = session.createConsumer(QUEUE);

      session.start();

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message2 = consumer.receive();

         ActiveMQBuffer buffer = message2.getBodyBuffer();

         Assert.assertEquals("testINVMCoreClient", buffer.readString());

         message2.acknowledge();
      }

      sf.close();
   }
}
