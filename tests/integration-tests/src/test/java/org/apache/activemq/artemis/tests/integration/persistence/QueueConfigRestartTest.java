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
package org.apache.activemq.artemis.tests.integration.persistence;

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.postoffice.QueueBinding;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Assert;
import org.junit.Test;

public class QueueConfigRestartTest extends ActiveMQTestBase {

   // Constants -----------------------------------------------------

   private static final String ADDRESS = "ADDRESS";

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------


   @Test
   public void testQueueConfigPurgeOnNoConsumerAndRestart() throws Exception {
      ActiveMQServer server = createServer(true);

      server.start();

      SimpleString address = new SimpleString("test.address");
      SimpleString queue = new SimpleString("test.queue");

      server.createQueue(address, RoutingType.MULTICAST, queue, null, null, true, false, false, 10, true, true);

      QueueBinding queueBinding1 = (QueueBinding)server.getPostOffice().getBinding(queue);
      Assert.assertTrue(queueBinding1.getQueue().isPurgeOnNoConsumers());

      server.stop();

      server.start();

      QueueBinding queueBinding2 = (QueueBinding)server.getPostOffice().getBinding(queue);
      Assert.assertTrue(queueBinding2.getQueue().isPurgeOnNoConsumers());
   }

   @Test
   public void testQueueConfigLastValueAndRestart() throws Exception {
      ActiveMQServer server = createServer(true);

      server.start();

      SimpleString address = new SimpleString("test.address");
      SimpleString queue = new SimpleString("test.queue");

      server.createQueue(address, RoutingType.MULTICAST, queue, null, null, true, false, false, false,false, 10, true, false, true, true);

      QueueBinding queueBinding1 = (QueueBinding)server.getPostOffice().getBinding(queue);
      Assert.assertTrue(queueBinding1.getQueue().isLastValue());

      server.stop();

      server.start();

      QueueBinding queueBinding2 = (QueueBinding)server.getPostOffice().getBinding(queue);
      Assert.assertTrue(queueBinding2.getQueue().isLastValue());
   }

   @Test
   public void testQueueConfigExclusiveAndRestart() throws Exception {
      ActiveMQServer server = createServer(true);

      server.start();

      SimpleString address = new SimpleString("test.address");
      SimpleString queue = new SimpleString("test.queue");

      server.createQueue(address, RoutingType.MULTICAST, queue, null, null, true, false, false, false,false, 10, true, true, true, true);

      QueueBinding queueBinding1 = (QueueBinding)server.getPostOffice().getBinding(queue);
      Assert.assertTrue(queueBinding1.getQueue().isExclusive());

      server.stop();

      server.start();

      QueueBinding queueBinding2 = (QueueBinding)server.getPostOffice().getBinding(queue);
      Assert.assertTrue(queueBinding2.getQueue().isExclusive());
   }


   @Test
   public void testQueueConfigUserAndRestart() throws Exception {
      ActiveMQServer server = createServer(true);

      server.start();

      SimpleString address = new SimpleString("test.address");
      SimpleString queue = new SimpleString("test.queue");

      server.createQueue(address, RoutingType.MULTICAST, queue, null, SimpleString.toSimpleString("bob"), true, false, false, 10, true, true);

      QueueBinding queueBinding1 = (QueueBinding)server.getPostOffice().getBinding(queue);
      Assert.assertEquals(SimpleString.toSimpleString("bob"), queueBinding1.getQueue().getUser());

      server.stop();

      server.start();

      QueueBinding queueBinding2 = (QueueBinding)server.getPostOffice().getBinding(queue);
      Assert.assertTrue(queueBinding2.getQueue().isPurgeOnNoConsumers());
   }
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
