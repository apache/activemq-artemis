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
package org.apache.activemq.artemis.tests.integration.cli;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.UUID;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.cli.commands.ActionContext;
import org.apache.activemq.artemis.cli.commands.queue.CreateQueue;
import org.apache.activemq.artemis.cli.commands.queue.DeleteQueue;
import org.apache.activemq.artemis.cli.commands.AbstractAction;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.tests.util.JMSTestBase;
import org.junit.Before;
import org.junit.Test;

public class QueueCommandTest extends JMSTestBase {

   //the command
   private ByteArrayOutputStream output;
   private ByteArrayOutputStream error;

   @Before
   @Override
   public void setUp() throws Exception {
      super.setUp();
      this.output = new ByteArrayOutputStream(1024);
      this.error = new ByteArrayOutputStream(1024);
   }

   @Test
   public void testCreateCoreQueueShowsErrorWhenAddressDoesNotExists() throws Exception {
      String queueName = "queue1";
      CreateQueue command = new CreateQueue();
      command.setName(queueName);
      command.execute(new ActionContext(System.in, new PrintStream(output), new PrintStream(error)));
      checkExecutionFailure(command, "AMQ119203: Address Does Not Exist:");
      assertFalse(server.queueQuery(new SimpleString(queueName)).isExists());
   }

   @Test
   public void testCreateCoreQueueAutoCreateAddressDefaultAddress() throws Exception {
      String queueName = UUID.randomUUID().toString();
      CreateQueue command = new CreateQueue();
      command.setName(queueName);
      command.setAutoCreateAddress(true);
      command.execute(new ActionContext(System.in, new PrintStream(output), new PrintStream(error)));
      checkExecutionPassed(command);
      assertNotNull(server.getAddressInfo(new SimpleString(queueName)));

      Queue queue = server.locateQueue(new SimpleString(queueName));
      assertEquals(-1, queue.getMaxConsumers());
      assertEquals(false, queue.isDeleteOnNoConsumers());
      assertTrue(server.queueQuery(new SimpleString(queueName)).isExists());
   }

   @Test
   public void testCreateCoreQueueAddressExists() throws Exception {
      String queueName = "queue";
      String address = "address";

      CreateQueue command = new CreateQueue();
      command.setName(queueName);
      command.setAutoCreateAddress(false);
      command.setAddress(address);

      server.createOrUpdateAddressInfo(new AddressInfo(new SimpleString(address)));

      command.execute(new ActionContext(System.in, new PrintStream(output), new PrintStream(error)));
      checkExecutionPassed(command);
      assertNotNull(server.getAddressInfo(new SimpleString(address)));

      Queue queue = server.locateQueue(new SimpleString(queueName));
      assertEquals(-1, queue.getMaxConsumers());
      assertEquals(false, queue.isDeleteOnNoConsumers());
      assertTrue(server.queueQuery(new SimpleString(queueName)).isExists());
   }

   @Test
   public void testCreateCoreQueueWithFilter() throws Exception {
      String queueName = "queue2";
      String filerString = "color='green'";

      CreateQueue command = new CreateQueue();
      command.setName(queueName);
      command.setFilter("color='green'");
      command.setAutoCreateAddress(true);
      command.execute(new ActionContext(System.in, new PrintStream(output), new PrintStream(error)));

      checkExecutionPassed(command);
      Queue queue = server.locateQueue(new SimpleString(queueName));
      assertNotNull(queue);
      assertEquals(new SimpleString(filerString), queue.getFilter().getFilterString());
   }

   @Test
   public void testCreateQueueAlreadyExists() throws Exception {
      String queueName = "queue2";
      String filerString = "color='green'";

      CreateQueue command = new CreateQueue();
      command.setName(queueName);
      command.setFilter("color='green'");
      command.setAutoCreateAddress(true);
      command.execute(new ActionContext());
      command.execute(new ActionContext(System.in, new PrintStream(output), new PrintStream(error)));
      checkExecutionFailure(command, "AMQ119019: Queue already exists " + queueName);
   }

   @Test
   public void testDeleteCoreQueue() throws Exception {
      SimpleString queueName = new SimpleString("deleteQueue");

      CreateQueue command = new CreateQueue();
      command.setName(queueName.toString());
      command.setFilter("color='green'");
      command.setAutoCreateAddress(true);
      command.execute(new ActionContext());

      DeleteQueue delete = new DeleteQueue();
      delete.setName(queueName.toString());
      delete.execute(new ActionContext(System.in, new PrintStream(output), new PrintStream(error)));
      checkExecutionPassed(delete);

      assertFalse(server.queueQuery(queueName).isExists());
   }

   @Test
   public void testDeleteQueueDoesNotExist() throws Exception {
      SimpleString queueName = new SimpleString("deleteQueue");

      DeleteQueue delete = new DeleteQueue();
      delete.setName(queueName.toString());
      delete.execute(new ActionContext(System.in, new PrintStream(output), new PrintStream(error)));
      checkExecutionFailure(delete, "AMQ119017: Queue " + queueName + " does not exist");

      assertFalse(server.queueQuery(queueName).isExists());
   }

   @Test
   public void testDeleteQueueWithConsumersFails() throws Exception {
      SimpleString queueName = new SimpleString("deleteQueue");

      CreateQueue command = new CreateQueue();
      command.setName(queueName.toString());
      command.setFilter("color='green'");
      command.setAutoCreateAddress(true);
      command.execute(new ActionContext());

      server.locateQueue(queueName).addConsumer(new DummyServerConsumer());

      DeleteQueue delete = new DeleteQueue();
      delete.setName(queueName.toString());
      delete.execute(new ActionContext(System.in, new PrintStream(output), new PrintStream(error)));
      checkExecutionFailure(delete, "AMQ119025: Cannot delete queue " + queueName + " on binding deleteQueue");
   }

   @Test
   public void testDeleteQueueWithConsumersFailsAndRemoveConsumersTrue() throws Exception {
      SimpleString queueName = new SimpleString("deleteQueue");

      CreateQueue command = new CreateQueue();
      command.setName(queueName.toString());
      command.setFilter("color='green'");
      command.setAutoCreateAddress(true);
      command.execute(new ActionContext());

      server.locateQueue(queueName).addConsumer(new DummyServerConsumer());

      DeleteQueue delete = new DeleteQueue();
      delete.setName(queueName.toString());
      delete.setRemoveConsumers(true);
      delete.execute(new ActionContext(System.in, new PrintStream(output), new PrintStream(error)));
      checkExecutionPassed(command);
   }

   @Test
   public void testAutoDeleteAddress() throws Exception {
      SimpleString queueName = new SimpleString("deleteQueue");

      CreateQueue command = new CreateQueue();
      command.setName(queueName.toString());
      command.setFilter("color='green'");
      command.setAutoCreateAddress(true);
      command.execute(new ActionContext());
      assertNotNull(server.getAddressInfo(queueName));

      server.locateQueue(queueName).addConsumer(new DummyServerConsumer());

      DeleteQueue delete = new DeleteQueue();
      delete.setName(queueName.toString());
      delete.setRemoveConsumers(true);
      delete.setAutoDeleteAddress(true);
      delete.execute(new ActionContext(System.in, new PrintStream(output), new PrintStream(error)));

      checkExecutionPassed(command);
      assertNull(server.getAddressInfo(queueName));
   }

   private void checkExecutionPassed(AbstractAction command) throws Exception {
      String fullMessage = output.toString();
      System.out.println("output: " + fullMessage);
      assertTrue(fullMessage, fullMessage.contains("successfully"));
   }

   private void checkExecutionFailure(AbstractAction command, String message) throws Exception {
      String fullMessage = error.toString();
      System.out.println("error: " + fullMessage);
      assertTrue(fullMessage, fullMessage.contains(message));
   }
}
