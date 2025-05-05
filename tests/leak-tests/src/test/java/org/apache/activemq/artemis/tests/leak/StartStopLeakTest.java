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
package org.apache.activemq.artemis.tests.leak;

import io.github.checkleak.core.CheckLeak;
import org.apache.activemq.artemis.core.postoffice.impl.PostOfficeImpl;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.core.server.impl.ServerStatus;
import org.apache.activemq.artemis.protocol.amqp.connect.mirror.AckManager;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;


/** Making sure the server will not leak after started / stopped */
public class StartStopLeakTest extends ActiveMQTestBase {

   @Test
   public void testAckManagerLeak() throws Throwable {
      CheckLeak checkLeak = new CheckLeak();
      internalTest(checkLeak);

      clearServers();

      MemoryAssertions.assertMemory(checkLeak, 0, ActiveMQServerImpl.class.getName());
   }

   // Creating a sub method to facilitate clearing references towards ActiveMQServerImpl
   private void internalTest(CheckLeak checkLeak) throws Exception {
      assertNull(ServerStatus.getServer(), () -> "A previous test left a server hanging on ServerStatus -> " + ServerStatus.getServer());

      ActiveMQServer server = createServer(false, true);
      for (int i = 0; i < 5; i++) {
         server.start();
         MemoryAssertions.assertMemory(checkLeak, 1, AckManager.class.getName());

         assertSame(server, ServerStatus.getServer());

         server.stop(false);
         assertEquals(0, server.getExternalComponents().size());

         assertNull(ServerStatus.getServer());
      }

      MemoryAssertions.assertMemory(checkLeak, 1, PostOfficeImpl.class.getName());
      MemoryAssertions.assertMemory(checkLeak, 0, AckManager.class.getName());
      assertEquals(0, server.getExternalComponents().size());
      MemoryAssertions.basicMemoryAsserts();
   }
}