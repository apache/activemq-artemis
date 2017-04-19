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
package org.apache.activemq.artemis.tests.integration.cluster;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.artemis.core.server.NodeManager;
import org.apache.activemq.artemis.core.server.impl.FileLockNodeManager;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.SpawnedVMSupport;
import org.apache.activemq.artemis.utils.UUID;
import org.junit.Assert;
import org.junit.Test;

public class RealNodeManagerTest extends NodeManagerTest {

   @Test
   public void testId() throws Exception {
      NodeManager nodeManager = new FileLockNodeManager(new File(getTemporaryDir()), false);
      nodeManager.start();
      UUID id1 = nodeManager.getUUID();
      nodeManager.stop();
      nodeManager.start();
      ActiveMQTestBase.assertEqualsByteArrays(id1.asBytes(), nodeManager.getUUID().asBytes());
      nodeManager.stop();
   }

   @Override
   public void performWork(NodeManagerAction... actions) throws Exception {
      List<Process> processes = new ArrayList<>();
      for (NodeManagerAction action : actions) {
         Process p = SpawnedVMSupport.spawnVM(NodeManagerAction.class.getName(), "-Xms512m", "-Xmx512m", new String[0], true, true, true, action.getWork());
         processes.add(p);
      }
      for (Process process : processes) {
         process.waitFor();
      }
      for (Process process : processes) {
         if (process.exitValue() == 9) {
            Assert.fail("failed see output");
         }
      }

   }
}
