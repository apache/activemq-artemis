/**
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
package org.apache.activemq.tests.integration.cluster;

import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.core.server.NodeManager;
import org.apache.activemq.core.server.impl.FileLockNodeManager;
import org.apache.activemq.tests.util.SpawnedVMSupport;
import org.apache.activemq.utils.UUID;
import org.junit.Test;

/**
 * @author <a href="mailto:andy.taylor@jboss.com">Andy Taylor</a>
 *         Date: Oct 18, 2010
 *         Time: 10:34:25 AM
 */
public class RealNodeManagerTest extends NodeManagerTest
{
   @Test
   public void testId() throws Exception
   {
      NodeManager nodeManager = new FileLockNodeManager(getTemporaryDir(), false);
      nodeManager.start();
      UUID id1 = nodeManager.getUUID();
      nodeManager.stop();
      nodeManager.start();
      assertEqualsByteArrays(id1.asBytes(), nodeManager.getUUID().asBytes());
      nodeManager.stop();
   }

   @Override
   public void performWork(NodeManagerAction... actions) throws Exception
   {
      List<Process> processes = new ArrayList<Process>();
      for (NodeManagerAction action : actions)
      {
         Process p = SpawnedVMSupport.spawnVM(NodeManagerAction.class.getName(), "-Xms512m", "-Xmx512m", new String[0], true, true, action.getWork());
         processes.add(p);
      }
      for (Process process : processes)
      {
         process.waitFor();
      }
      for (Process process : processes)
      {
         if (process.exitValue() == 9)
         {
            fail("failed see output");
         }
      }

   }
}
