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
package org.apache.activemq.artemis.tests.extras.byteman;

import java.io.ByteArrayInputStream;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.Wait;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(BMUnitRunner.class)
public class LargeMessageOnShutdownTest extends ActiveMQTestBase {

   private static final SimpleString queueName = new SimpleString("largeMessageShutdownQueue");
   private static ActiveMQServer server;

   @Before
   @Override
   public void setUp() throws Exception {
      super.setUp();

      server = createServer(true, createDefaultNettyConfig());
      startServer();
      server.createQueue(queueName, RoutingType.MULTICAST, queueName, null, true, false);
   }

   @After
   @Override
   public void tearDown() throws Exception {
      super.tearDown();
      stopServer();
   }

   @Test
   @BMRules(
      rules = {
         @BMRule(
            name = "BlockOnFinalLargeMessagePacket",
            targetClass = "org.apache.activemq.artemis.core.server.impl.ServerSessionImpl",
            targetMethod = "doSend(Transaction,Message,SimpleString,boolean,boolean)",
            targetLocation = "EXIT",
            condition = "!flagged(\"testLargeMessageOnShutdown\")",
            action =
               "org.apache.activemq.artemis.tests.extras.byteman.LargeMessageOnShutdownTest.stopServer();" +
               "waitFor(\"testLargeMessageOnShutdown\", 5000);" +
               "flag(\"testLargeMessageOnShutdown\")"
         ),
         @BMRule(
            name = "ReleaseBlockOnSessionCleanup",
            targetClass = "org.apache.activemq.artemis.core.protocol.core.ServerSessionPacketHandler",
            targetMethod = "clearLargeMessage()",
            targetLocation = "EXIT",
            action = "signalWake(\"testLargeMessageOnShutdown\")"
         )
      }
   )
   public void testLargeMessageOnShutdown() throws Exception {

      byte[] payload = new byte[ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE * 2];

      // Send Large Message
      ClientSessionFactory csf1 = createSessionFactory(createNettyNonHALocator());
      try {
         ClientSession session1 = csf1.createSession();
         ClientProducer producer1 = session1.createProducer(queueName);
         ClientMessage message = session1.createMessage(true);

         message.setBodyInputStream(new ByteArrayInputStream(payload));
         producer1.send(message);
      } catch (Exception e) {
         // Expected due to shutdown.
      } finally {
         csf1.close();
      }

      waitForStoppedServer();
      startServer();

      // Consume Large Message
      ClientSessionFactory csf2 = createSessionFactory(createNettyNonHALocator());
      try {
         ClientSession session2 = csf2.createSession();
         session2.start();
         ClientConsumer consumer2 = session2.createConsumer(queueName);
         ClientMessage rmessage = consumer2.receive(10000);

         assertEquals(payload.length, rmessage.getBodyBuffer().readableBytes());
         assertEqualsBuffers(payload.length, ActiveMQBuffers.wrappedBuffer(payload), rmessage.getBodyBuffer());
      } finally {
         csf2.close();
      }
   }

   public static void stopServer() throws Exception {
      server.stop();
      waitForStoppedServer();
   }

   public static void startServer() throws Exception {
      server.start();
      server.waitForActivation(30, TimeUnit.SECONDS);
   }

   public static void waitForStoppedServer() throws Exception {
      Wait.waitFor(() -> server.getState() == ActiveMQServer.SERVER_STATE.STOPPED);
   }
}
