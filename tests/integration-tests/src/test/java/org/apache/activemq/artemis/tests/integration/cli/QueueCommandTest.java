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
import java.util.EnumSet;
import java.util.Set;
import java.util.UUID;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.cli.commands.AbstractAction;
import org.apache.activemq.artemis.cli.commands.ActionContext;
import org.apache.activemq.artemis.cli.commands.queue.CreateQueue;
import org.apache.activemq.artemis.cli.commands.queue.DeleteQueue;
import org.apache.activemq.artemis.cli.commands.queue.PurgeQueue;
import org.apache.activemq.artemis.cli.commands.queue.UpdateQueue;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.QueueQueryResult;
import org.apache.activemq.artemis.api.core.RoutingType;
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
      command.setMulticast(true);
      command.setAnycast(false);
      command.execute(new ActionContext(System.in, new PrintStream(output), new PrintStream(error)));
      checkExecutionFailure(command, "AMQ229203");
      assertFalse(server.queueQuery(new SimpleString(queueName)).isExists());
   }

   @Test
   public void testCreateCoreQueueAutoCreateAddressDefaultAddress() throws Exception {
      String queueName = UUID.randomUUID().toString();
      CreateQueue command = new CreateQueue();
      command.setName(queueName);
      command.setAutoCreateAddress(true);
      command.setMulticast(true);
      command.setAnycast(false);
      command.execute(new ActionContext(System.in, new PrintStream(output), new PrintStream(error)));
      checkExecutionPassed(command);
      assertNotNull(server.getAddressInfo(new SimpleString(queueName)));

      Queue queue = server.locateQueue(new SimpleString(queueName));
      assertEquals(-1, queue.getMaxConsumers());
      assertEquals(false, queue.isPurgeOnNoConsumers());
      assertTrue(server.queueQuery(new SimpleString(queueName)).isExists());
   }

   @Test
   public void testCreateCoreQueueAddressExists() throws Exception {
      String queueName = "queue";
      String address = "address";

      CreateQueue command = new CreateQueue();
      command.setName(queueName);
      command.setAutoCreateAddress(false);
      command.setMulticast(true);
      command.setAnycast(false);
      command.setAddress(address);

      server.addOrUpdateAddressInfo(new AddressInfo(new SimpleString(address), RoutingType.MULTICAST));

      command.execute(new ActionContext(System.in, new PrintStream(output), new PrintStream(error)));
      checkExecutionPassed(command);
      assertNotNull(server.getAddressInfo(new SimpleString(address)));

      Queue queue = server.locateQueue(new SimpleString(queueName));
      assertEquals(-1, queue.getMaxConsumers());
      assertEquals(false, queue.isPurgeOnNoConsumers());
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
      command.setMulticast(true);
      command.setAnycast(false);
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
      command.setMulticast(true);
      command.setAnycast(false);
      command.execute(new ActionContext());
      command.execute(new ActionContext(System.in, new PrintStream(output), new PrintStream(error)));
      checkExecutionFailure(command, "AMQ229019");
   }

   @Test
   public void testDeleteCoreQueue() throws Exception {
      SimpleString queueName = new SimpleString("deleteQueue");

      CreateQueue command = new CreateQueue();
      command.setName(queueName.toString());
      command.setFilter("color='green'");
      command.setAutoCreateAddress(true);
      command.setMulticast(true);
      command.setAnycast(false);
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
      checkExecutionFailure(delete, "AMQ229017");

      assertFalse(server.queueQuery(queueName).isExists());
   }

   @Test
   public void testDeleteQueueWithConsumersFails() throws Exception {
      SimpleString queueName = new SimpleString("deleteQueue");

      CreateQueue command = new CreateQueue();
      command.setName(queueName.toString());
      command.setFilter("color='green'");
      command.setAutoCreateAddress(true);
      command.setMulticast(true);
      command.setAnycast(false);
      command.execute(new ActionContext());

      server.locateQueue(queueName).addConsumer(new DummyServerConsumer());

      DeleteQueue delete = new DeleteQueue();
      delete.setName(queueName.toString());
      delete.execute(new ActionContext(System.in, new PrintStream(output), new PrintStream(error)));
      checkExecutionFailure(delete, "AMQ229025");
   }

   @Test
   public void testDeleteQueueWithConsumersFailsAndRemoveConsumersTrue() throws Exception {
      SimpleString queueName = new SimpleString("deleteQueue");

      CreateQueue command = new CreateQueue();
      command.setName(queueName.toString());
      command.setFilter("color='green'");
      command.setAutoCreateAddress(true);
      command.setMulticast(true);
      command.setAnycast(false);
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
      command.setMulticast(true);
      command.setAnycast(false);
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

   @Test
   public void testUpdateCoreQueue() throws Exception {
      final String queueName = "updateQueue";
      final SimpleString queueNameString = new SimpleString(queueName);
      final String addressName = "address";
      final SimpleString addressSimpleString = new SimpleString(addressName);
      final int oldMaxConsumers = -1;
      final RoutingType oldRoutingType = RoutingType.MULTICAST;
      final boolean oldPurgeOnNoConsumers = false;
      final AddressInfo addressInfo = new AddressInfo(addressSimpleString, EnumSet.of(RoutingType.ANYCAST, RoutingType.MULTICAST));
      server.addAddressInfo(addressInfo);
      server.createQueue(new QueueConfiguration(queueNameString).setAddress(addressSimpleString).setRoutingType(oldRoutingType).setMaxConsumers(oldMaxConsumers).setPurgeOnNoConsumers(oldPurgeOnNoConsumers).setAutoCreateAddress(false));

      final int newMaxConsumers = 1;
      final RoutingType newRoutingType = RoutingType.ANYCAST;
      final boolean newPurgeOnNoConsumers = true;
      final UpdateQueue updateQueue = new UpdateQueue();
      updateQueue.setName(queueName);
      updateQueue.setPurgeOnNoConsumers(newPurgeOnNoConsumers);
      updateQueue.setAnycast(true);
      updateQueue.setMulticast(false);
      updateQueue.setMaxConsumers(newMaxConsumers);
      updateQueue.execute(new ActionContext(System.in, new PrintStream(output), new PrintStream(error)));

      checkExecutionPassed(updateQueue);

      final QueueQueryResult queueQueryResult = server.queueQuery(queueNameString);
      assertEquals("maxConsumers", newMaxConsumers, queueQueryResult.getMaxConsumers());
      assertEquals("routingType", newRoutingType, queueQueryResult.getRoutingType());
      assertTrue("purgeOnNoConsumers", newPurgeOnNoConsumers == queueQueryResult.isPurgeOnNoConsumers());
   }

   @Test
   public void testUpdateCoreQueueCannotChangeRoutingType() throws Exception {
      final String queueName = "updateQueue";
      final SimpleString queueNameString = new SimpleString(queueName);
      final String addressName = "address";
      final SimpleString addressSimpleString = new SimpleString(addressName);
      final int oldMaxConsumers = 10;
      final RoutingType oldRoutingType = RoutingType.MULTICAST;
      final boolean oldPurgeOnNoConsumers = false;
      final Set<RoutingType> supportedRoutingTypes = EnumSet.of(oldRoutingType);
      final AddressInfo addressInfo = new AddressInfo(addressSimpleString, EnumSet.copyOf(supportedRoutingTypes));
      server.addAddressInfo(addressInfo);
      server.createQueue(new QueueConfiguration(queueNameString).setAddress(addressSimpleString).setRoutingType(oldRoutingType).setMaxConsumers(oldMaxConsumers).setPurgeOnNoConsumers(oldPurgeOnNoConsumers).setAutoCreateAddress(false));

      final RoutingType newRoutingType = RoutingType.ANYCAST;
      final UpdateQueue updateQueue = new UpdateQueue();
      updateQueue.setName(queueName);
      updateQueue.setAnycast(true);
      updateQueue.setMulticast(false);
      updateQueue.setMaxConsumers(-1);
      updateQueue.execute(new ActionContext(System.in, new PrintStream(output), new PrintStream(error)));

      checkExecutionFailure(updateQueue, "AMQ229211");

      final QueueQueryResult queueQueryResult = server.queueQuery(queueNameString);
      assertEquals("maxConsumers", oldMaxConsumers, queueQueryResult.getMaxConsumers());
      assertEquals("routingType", oldRoutingType, queueQueryResult.getRoutingType());
      assertTrue("purgeOnNoConsumers", oldPurgeOnNoConsumers == queueQueryResult.isPurgeOnNoConsumers());
   }

   @Test
   public void testUpdateCoreQueueCannotLowerMaxConsumers() throws Exception {
      final String queueName = "updateQueue";
      final SimpleString queueNameString = new SimpleString(queueName);
      final String addressName = "address";
      final SimpleString addressSimpleString = new SimpleString(addressName);
      final int oldMaxConsumers = 2;
      final RoutingType oldRoutingType = RoutingType.MULTICAST;
      final boolean oldPurgeOnNoConsumers = false;
      final AddressInfo addressInfo = new AddressInfo(addressSimpleString, oldRoutingType);
      server.addAddressInfo(addressInfo);
      server.createQueue(new QueueConfiguration(queueNameString).setAddress(addressSimpleString).setRoutingType(oldRoutingType).setMaxConsumers(oldMaxConsumers).setPurgeOnNoConsumers(oldPurgeOnNoConsumers).setAutoCreateAddress(false));

      server.locateQueue(queueNameString).addConsumer(new DummyServerConsumer());
      server.locateQueue(queueNameString).addConsumer(new DummyServerConsumer());

      final int newMaxConsumers = 1;
      final UpdateQueue updateQueue = new UpdateQueue();
      updateQueue.setName(queueName);
      updateQueue.setMaxConsumers(newMaxConsumers);
      updateQueue.execute(new ActionContext(System.in, new PrintStream(output), new PrintStream(error)));

      checkExecutionFailure(updateQueue, "AMQ229210");

      final QueueQueryResult queueQueryResult = server.queueQuery(queueNameString);
      assertEquals("maxConsumers", oldMaxConsumers, queueQueryResult.getMaxConsumers());
   }

   @Test
   public void testUpdateCoreQueueDoesNotExist() throws Exception {
      SimpleString queueName = new SimpleString("updateQueue");

      UpdateQueue updateQueue = new UpdateQueue();
      updateQueue.setName(queueName.toString());
      updateQueue.execute(new ActionContext(System.in, new PrintStream(output), new PrintStream(error)));
      checkExecutionFailure(updateQueue, "AMQ229017: Queue " + queueName + " does not exist");

      assertFalse(server.queueQuery(queueName).isExists());
   }

   @Test
   public void testPurgeQueue() throws Exception {
      SimpleString queueName = new SimpleString("purgeQueue");

      CreateQueue command = new CreateQueue();
      command.setName(queueName.toString());
      command.setAutoCreateAddress(true);
      command.setAnycast(true);
      command.execute(new ActionContext());

      PurgeQueue purge = new PurgeQueue();
      purge.setName(queueName.toString());
      purge.execute(new ActionContext(System.in, new PrintStream(output), new PrintStream(error)));
      checkExecutionPassed(purge);
   }

   @Test
   public void testPurgeQueueDoesNotExist() throws Exception {
      SimpleString queueName = new SimpleString("purgeQueue");

      PurgeQueue purge = new PurgeQueue();
      purge.setName(queueName.toString());
      purge.execute(new ActionContext(System.in, new PrintStream(output), new PrintStream(error)));
      checkExecutionFailure(purge, "AMQ229067: Cannot find resource with name queue." + queueName);

      assertFalse(server.queueQuery(queueName).isExists());
   }

   private void checkExecutionPassed(AbstractAction command) throws Exception {
      String fullMessage = output.toString();
      instanceLog.debug("output: " + fullMessage);
      assertTrue(fullMessage, fullMessage.contains("successfully"));
   }

   private void checkExecutionFailure(AbstractAction command, String message) throws Exception {
      String fullMessage = error.toString();
      instanceLog.debug("error: " + fullMessage);
      assertTrue(fullMessage, fullMessage.contains(message));
   }
}
