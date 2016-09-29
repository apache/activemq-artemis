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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.SendAcknowledgementHandler;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SessionSendAcknowledgementHandlerTest extends ActiveMQTestBase {

   private ActiveMQServer server;

   private final SimpleString address = new SimpleString("address");

   private final SimpleString queueName = new SimpleString("queue");

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      server = createServer(false);
      server.start();
   }

   @Test
   public void testSetInvalidSendACK() throws Exception {
      ServerLocator locator = createInVMNonHALocator();

      locator.setConfirmationWindowSize(-1);

      ClientSessionFactory csf = createSessionFactory(locator);
      ClientSession session = csf.createSession(null, null, false, true, true, false, 1);

      boolean failed = false;
      try {
         session.setSendAcknowledgementHandler(new SendAcknowledgementHandler() {
            @Override
            public void sendAcknowledged(Message message) {
            }
         });
      } catch (Throwable expected) {
         failed = true;
      }

      assertTrue("Expected a failure on setting ACK Handler", failed);

      session.createQueue(address, queueName, false);
   }

   @Test
   public void testSendAcknowledgementsNoWindowSize() throws Exception {
      verifySendAcknowledgements(0);
   }

   @Test
   public void testSendAcknowledgements() throws Exception {
      verifySendAcknowledgements(1024);
   }

   @Test
   public void testSendAcknowledgementsNoWindowSizeProducerOnly() throws Exception {
      verifySendAcknowledgementsProducerOnly(0);
   }

   @Test
   public void testSendAcknowledgementsProducer() throws Exception {
      verifySendAcknowledgementsProducerOnly(1024);
   }

   public void verifySendAcknowledgements(int windowSize) throws Exception {
      ServerLocator locator = createInVMNonHALocator();

      locator.setConfirmationWindowSize(windowSize);

      ClientSessionFactory csf = createSessionFactory(locator);
      ClientSession session = csf.createSession(null, null, false, true, true, false, 1);

      session.createQueue(address, queueName, false);

      ClientProducer prod = session.createProducer(address);

      final int numMessages = 1000;

      LatchAckHandler handler = new LatchAckHandler("session", new CountDownLatch(numMessages));

      LatchAckHandler producerHandler = new LatchAckHandler("producer", new CountDownLatch(numMessages));

      session.setSendAcknowledgementHandler(handler);

      for (int i = 0; i < numMessages; i++) {
         ClientMessage msg = session.createMessage(false);
         ClientMessage msg2 = session.createMessage(false);

         prod.send(msg);
         prod.send(address, msg2, producerHandler);
      }

      Assert.assertTrue("session must have acked, " + handler, handler.latch.await(5, TimeUnit.SECONDS));
      Assert.assertTrue("producer specific handler must have acked, " + producerHandler, producerHandler.latch.await(5, TimeUnit.SECONDS));
   }

   public void verifySendAcknowledgementsProducerOnly(int windowSize) throws Exception {
      ServerLocator locator = createInVMNonHALocator();

      locator.setConfirmationWindowSize(windowSize);

      ClientSessionFactory csf = createSessionFactory(locator);
      ClientSession session = csf.createSession(null, null, false, true, true, false, 1);

      session.createQueue(address, queueName, false);

      ClientProducer prod = session.createProducer(address);

      final int numMessages = 1000;

      LatchAckHandler producerHandler = new LatchAckHandler("producer", new CountDownLatch(numMessages));

      for (int i = 0; i < numMessages; i++) {
         ClientMessage msg2 = session.createMessage(false);

         prod.send(address, msg2, producerHandler);
      }

      Assert.assertTrue("producer specific handler must have acked, " + producerHandler, producerHandler.latch.await(5, TimeUnit.SECONDS));
   }

   public static final class LatchAckHandler implements SendAcknowledgementHandler {

      public CountDownLatch latch;
      private final String name;

      public LatchAckHandler(String name, CountDownLatch latch) {
         this.name = name;
         this.latch = latch;
      }

      @Override
      public void sendAcknowledged(Message message) {
         latch.countDown();
      }

      @Override
      public String toString() {
         return "SendAckHandler(name=" + name + ", latch=" + latch + ")";
      }
   }
}
