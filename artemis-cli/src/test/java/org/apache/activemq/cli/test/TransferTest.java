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

package org.apache.activemq.cli.test;

import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.cli.commands.messages.Transfer;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.management.ManagementContext;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.jms.Connection;
import javax.jms.Session;

public class TransferTest extends CliTestBase {
   private Connection connection;
   private ActiveMQConnectionFactory cf;
   private ActiveMQServer server;
   private static final int TEST_MESSAGE_COUNT = 10;

   @Before
   @Override
   public void setup() throws Exception {
      setupAuth();
      super.setup();
      server = ((Pair<ManagementContext, ActiveMQServer>)startServer()).getB();
      cf = getConnectionFactory(61616);
      connection = cf.createConnection("admin", "admin");
   }

   @After
   @Override
   public void tearDown() throws Exception {
      closeConnection(cf, connection);
      super.tearDown();
   }

   @Test
   public void testTransferMessages() throws Exception {
      testTransferMessages(TEST_MESSAGE_COUNT, 0);
   }

   @Test
   public void testTransferMessagesWithMessageCount() throws Exception {
      testTransferMessages(TEST_MESSAGE_COUNT, 5);
   }

   private void testTransferMessages(int messages, int limit) throws Exception {
      String sourceQueueName = "SOURCE_QUEUE";
      String targetQueueName = "TARGET_QUEUE";

      Session session = createSession(connection);
      produceMessages(session, sourceQueueName, messages, false);

      Queue sourceQueue = server.locateQueue(sourceQueueName);

      Assert.assertEquals(messages, sourceQueue.getMessageCount());

      Transfer transfer = new Transfer()
         .setSourceUser("admin")
         .setSourcePassword("admin")
         .setSourceQueue(sourceQueueName)
         .setTargetUser("admin")
         .setTargetPassword("admin")
         .setTargetQueue(targetQueueName);

      if (limit > 0) {
         transfer.setMessageCount(limit);

         Assert.assertEquals(limit, transfer.execute(new TestActionContext()));

         Queue targetQueue = server.locateQueue(targetQueueName);
         Assert.assertEquals(messages - limit, sourceQueue.getMessageCount());
         Assert.assertEquals(limit, targetQueue.getMessageCount());
      } else {
         Assert.assertEquals(messages, transfer.execute(new TestActionContext()));

         Queue targetQueue = server.locateQueue(targetQueueName);
         Assert.assertEquals(0, sourceQueue.getMessageCount());
         Assert.assertEquals(messages, targetQueue.getMessageCount());
      }
   }
}
