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
import org.apache.activemq.artemis.protocol.amqp.connect.mirror.AckManager;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AckManagerLeakTest extends ActiveMQTestBase {

   @Test
   public void testAckManagerLeak() throws Throwable {
      CheckLeak checkLeak = new CheckLeak();

      ActiveMQServer server = createServer(false, true);
      for (int i = 0; i < 5; i++) {
         server.start();
         MemoryAssertions.assertMemory(checkLeak, 1, AckManager.class.getName());
         server.stop(false);
         MemoryAssertions.assertMemory(checkLeak, 0, AckManager.class.getName());
      }

      MemoryAssertions.assertMemory(checkLeak, 1, PostOfficeImpl.class.getName());
      MemoryAssertions.assertMemory(checkLeak, 0, AckManager.class.getName());
      assertEquals(0, server.getExternalComponents().size());
      MemoryAssertions.basicMemoryAsserts();
   }
}