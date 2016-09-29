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
package org.apache.activemq.artemis.tests.unit.core.server.cluster.impl;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.cluster.impl.RemoteQueueBindingImpl;
import org.apache.activemq.artemis.tests.unit.core.postoffice.impl.FakeQueue;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.Test;

public class RemoteQueueBindImplTest extends ActiveMQTestBase {

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testAddRemoveConsumer() throws Exception {

      final long id = RandomUtil.randomLong();
      final SimpleString address = RandomUtil.randomSimpleString();
      final SimpleString uniqueName = RandomUtil.randomSimpleString();
      final SimpleString routingName = RandomUtil.randomSimpleString();
      final Long remoteQueueID = RandomUtil.randomLong();
      final SimpleString filterString = new SimpleString("A>B");
      final Queue storeAndForwardQueue = new FakeQueue(null);
      final SimpleString bridgeName = RandomUtil.randomSimpleString();
      final int distance = 0;
      RemoteQueueBindingImpl binding = new RemoteQueueBindingImpl(id, address, uniqueName, routingName, remoteQueueID, filterString, storeAndForwardQueue, bridgeName, distance);

      for (int i = 0; i < 100; i++) {
         binding.addConsumer(new SimpleString("B" + i + "<A"));
      }

      assertEquals(100, binding.getFilters().size());

      for (int i = 0; i < 100; i++) {
         binding.removeConsumer(new SimpleString("B" + i + "<A"));
      }

      assertEquals(0, binding.getFilters().size());

   }
}
