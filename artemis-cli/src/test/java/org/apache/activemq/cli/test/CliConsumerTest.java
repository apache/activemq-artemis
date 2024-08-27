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
package org.apache.activemq.cli.test;

import javax.jms.Connection;

import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.cli.commands.messages.Consumer;
import org.apache.activemq.artemis.cli.commands.messages.Producer;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.management.ManagementContext;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.utils.Wait;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class CliConsumerTest extends CliTestBase {
   private ActiveMQServer server;
   private Connection connection;
   private ActiveMQConnectionFactory cf;
   private static final int TEST_MESSAGE_COUNT = 10;

   @BeforeEach
   @Override
   public void setup() throws Exception {
      setupAuth();
      super.setup();
      server = ((Pair<ManagementContext, ActiveMQServer>)startServer()).getB();
      cf = getConnectionFactory(61616);
      connection = cf.createConnection("admin", "admin");
   }

   @AfterEach
   @Override
   public void tearDown() throws Exception {
      closeConnection(cf, connection);
      super.tearDown();
   }

   private void produceMessages(String address, String message, long msgCount) throws Exception {
      produceMessages(address, message, msgCount, null);
   }

   private void produceMessages(String address, String message, long msgCount, String properties) throws Exception {
      new Producer()
         .setMessage(message)
         .setProperties(properties)
         .setMessageCount(msgCount)
         .setDestination(address)
         .setUser("admin")
         .setPassword("admin")
         .execute(new TestActionContext());
   }

   private void produceMessages(String address, long msgCount) throws Exception {
      produceMessages(address, null, msgCount);
   }

   @Test
   public void testConsumeMessageTimeoutZero() throws Exception {
      sendAndConsume(TEST_MESSAGE_COUNT, 0);
   }

   @Test
   public void testConsumeMessageTimeoutOneSecond() throws Exception {
      sendAndConsume(TEST_MESSAGE_COUNT, 1000);
   }

   @Test
   public void testConsumeMessageTimeoutNegativeOne() throws Exception {
      sendAndConsume(TEST_MESSAGE_COUNT, -1);
   }

   private void sendAndConsume(long messageCount, int timeout) throws Exception {
      String address = "test";

      produceMessages(address, messageCount);

      Wait.assertEquals(messageCount, () -> server.locateQueue(address).getMessageCount(), 2000, 50);

      TestActionContext context = new TestActionContext();

      new Consumer()
         .setReceiveTimeout(timeout)
         .setMessageCount(messageCount)
         .setDestination(address)
         .setUser("admin")
         .setPassword("admin")
         .execute(context);

      if (timeout == -1) {
         assertTrue(context.getStdout().contains("wait forever"));
      } else {
         assertTrue(context.getStdout().contains("wait " + timeout + "ms"));
      }

      Wait.assertEquals(0L, () -> server.locateQueue(address).getMessageCount(), 2000, 50);
   }
}
