/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.hornetq.tests.integration.cluster.ha;

import org.hornetq.core.config.HAPolicyConfiguration;
import org.hornetq.core.config.ha.ColocatedPolicyConfiguration;
import org.hornetq.core.config.ha.SharedStoreMasterPolicyConfiguration;
import org.hornetq.core.config.ha.SharedStoreSlavePolicyConfiguration;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.integration.cluster.distribution.ClusterTestBase;
import org.junit.Before;
import org.junit.Test;


public class HAAutomaticBackupSharedStore extends ClusterTestBase
{
   @Before
   public void setup() throws Exception
   {
      super.setUp();

      setupServers();

      setUpHAPolicy(0);
      setUpHAPolicy(1);
      setUpHAPolicy(2);

      setupClusterConnection("cluster0", "queues", false, 1, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", false, 1, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", false, 1, isNetty(), 2, 0, 1);
   }

   @Test
   public void basicDiscovery() throws Exception
   {
      startServers(0, 1, 2, 3, 4, 5);

      createQueue(3, "queues.testaddress", "queue0", null, false);
      createQueue(4, "queues.testaddress", "queue0", null, false);
      createQueue(5, "queues.testaddress", "queue0", null, false);

   }

   protected void setupServers() throws Exception
   {
      // The lives
      setupLiveServer(0, isFileStorage(), true, isNetty(), false);
      setupLiveServer(1, isFileStorage(), true, isNetty(), false);
      setupLiveServer(2, isFileStorage(), true, isNetty(), false);

   }

   private void setUpHAPolicy(int node)
   {
      HornetQServer server = getServer(node);
      ColocatedPolicyConfiguration haPolicyConfiguration = new ColocatedPolicyConfiguration();
      HAPolicyConfiguration liveConfiguration = new SharedStoreMasterPolicyConfiguration();
      haPolicyConfiguration.setLiveConfig(liveConfiguration);

      HAPolicyConfiguration backupConfiguration = new SharedStoreSlavePolicyConfiguration();
      haPolicyConfiguration.setBackupConfig(backupConfiguration);
      server.getConfiguration().setHAPolicyConfiguration(haPolicyConfiguration);
   }

   public boolean isNetty()
   {
      return true;
   }
}
