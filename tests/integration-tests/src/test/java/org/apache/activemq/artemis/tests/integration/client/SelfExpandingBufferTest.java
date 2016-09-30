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

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.tests.integration.IntegrationTestLogger;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.Assert;
import org.junit.Test;

public class SelfExpandingBufferTest extends ActiveMQTestBase {

   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   ActiveMQServer service;

   SimpleString ADDRESS = new SimpleString("Address");

   @Test
   public void testSelfExpandingBufferNettyPersistent() throws Exception {
      testSelfExpandingBuffer(true, true);
   }

   @Test
   public void testSelfExpandingBufferInVMPersistent() throws Exception {
      testSelfExpandingBuffer(false, true);
   }

   @Test
   public void testSelfExpandingBufferNettyNonPersistent() throws Exception {
      testSelfExpandingBuffer(true, false);
   }

   @Test
   public void testSelfExpandingBufferInVMNonPersistent() throws Exception {
      testSelfExpandingBuffer(false, false);
   }

   private void testSelfExpandingBuffer(final boolean netty, final boolean persistent) throws Exception {
      setUpService(netty, persistent);

      ClientSessionFactory factory;

      ServerLocator locator = createFactory(netty);

      factory = createSessionFactory(locator);

      ClientSession session = factory.createSession(false, true, true);

      try {

         session.createQueue(ADDRESS, ADDRESS, true);

         ClientMessage msg = session.createMessage(true);

         ActiveMQBuffer buffer = msg.getBodyBuffer();

         SelfExpandingBufferTest.log.info("buffer is " + buffer);

         byte[] bytes = RandomUtil.randomBytes(10 * buffer.capacity());

         buffer.writeBytes(bytes);

         ClientProducer prod = session.createProducer(ADDRESS);

         prod.send(msg);

         // Send same message again

         prod.send(msg);

         ClientConsumer cons = session.createConsumer(ADDRESS);

         session.start();

         ClientMessage msg2 = cons.receive(3000);

         Assert.assertNotNull(msg2);

         byte[] receivedBytes = new byte[bytes.length];

         // log.info("buffer start pos should be at " + PacketImpl.PACKET_HEADERS_SIZE + DataConstants.SIZE_INT);
         //
         // log.info("buffer pos at " + msg2.getBodyBuffer().readerIndex());
         //
         // log.info("buffer length should be " + msg2.getBodyBuffer().readInt(PacketImpl.PACKET_HEADERS_SIZE));

         msg2.getBodyBuffer().readBytes(receivedBytes);

         ActiveMQTestBase.assertEqualsByteArrays(bytes, receivedBytes);

         msg2 = cons.receive(3000);

         Assert.assertNotNull(msg2);

         msg2.getBodyBuffer().readBytes(receivedBytes);

         ActiveMQTestBase.assertEqualsByteArrays(bytes, receivedBytes);
      } finally {
         session.close();
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void setUpService(final boolean netty, final boolean persistent) throws Exception {
      service = createServer(persistent, createDefaultConfig(netty));
      service.start();
   }
}
