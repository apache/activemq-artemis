/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.integration.server;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Assert;
import org.junit.Test;

public class QueuePeristPauseTest extends ActiveMQTestBase {

   @Test
      public void testPauseQueue() throws Exception {
      ActiveMQServer server = createServer(true, false);
      server.start();

      Queue queue = server.createQueue(new QueueConfiguration("q1").setRoutingType(RoutingType.ANYCAST));

      queue.pause(true);

      server.stop();
      server.start();

      for (int i = 0; i < 4; i++) {
         server.stop();
         server.start();
         queue = server.locateQueue(SimpleString.toSimpleString("q1"));
         Assert.assertTrue(queue.isPaused());
      }

      queue.resume();

      for (int i = 0; i < 4; i++) {
         server.stop();
         server.start();
         queue = server.locateQueue(SimpleString.toSimpleString("q1"));
         Assert.assertFalse(queue.isPaused());
      }

      server.stop();
   }
}
