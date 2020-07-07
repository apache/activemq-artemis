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
package org.apache.activemq.artemis.tests.integration.cluster.failover;

import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.core.config.MetricsConfiguration;
import org.apache.activemq.artemis.core.config.ha.SharedStoreMasterPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.SharedStoreSlavePolicyConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.cluster.impl.MessageLoadBalancingType;
import org.apache.activemq.artemis.core.server.metrics.plugins.SimpleMetricsPlugin;
import org.apache.activemq.artemis.tests.integration.cluster.distribution.ClusterTestBase;
import org.apache.activemq.artemis.tests.util.Wait;
import org.junit.Before;
import org.junit.Test;

public class SharedStoreMetricsLeakTest extends ClusterTestBase {

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      setupServers();
   }

   private void setupServers() throws Exception {
      setupLiveServer(0, isFileStorage(), true, isNetty(), false);
      setupBackupServer(1, 0, isFileStorage(), true, isNetty());

      getServer(0).getConfiguration().setHAPolicyConfiguration(new SharedStoreMasterPolicyConfiguration().setFailoverOnServerShutdown(true));
      getServer(0).getConfiguration().setMetricsConfiguration(new MetricsConfiguration().setJvmThread(false).setJvmGc(false).setJvmMemory(false).setPlugin(new SimpleMetricsPlugin().init(null)));
      getServer(1).getConfiguration().setHAPolicyConfiguration(new SharedStoreSlavePolicyConfiguration().setFailoverOnServerShutdown(true).setAllowFailBack(true));
      getServer(1).getConfiguration().setMetricsConfiguration(new MetricsConfiguration().setJvmThread(false).setJvmGc(false).setJvmMemory(false).setPlugin(new SimpleMetricsPlugin().init(null)));

      // configure cluster for bother servers
      setupClusterConnection("cluster", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 0, 1);
      setupClusterConnection("cluster", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 1, 0);
   }

   private boolean isNetty() {
      return true;
   }

   @Test
   public void testForMeterLeaks() throws Exception {
      ActiveMQServer live = getServer(0);
      ActiveMQServer backup = getServer(1);

      live.start();
      assertTrue(live.waitForActivation(5, TimeUnit.SECONDS));

      backup.start();
      assertFalse(backup.waitForActivation(1, TimeUnit.SECONDS));

      // there should be a handful of metrics available from the ActiveMQServerImpl itself
      long baseline = backup.getMetricsManager().getMeterRegistry().getMeters().size();

      live.stop();
      assertTrue(backup.waitForActivation(5, TimeUnit.SECONDS));

      // after failover more meters should get registered
      Wait.assertTrue(() -> backup.getMetricsManager().getMeterRegistry().getMeters().size() > baseline, 2000, 100);

      live.start();
      assertTrue(live.waitForActivation(5, TimeUnit.SECONDS));

      // after failback the number of registered meters should return to baseline
      Wait.assertTrue(() -> backup.getMetricsManager().getMeterRegistry().getMeters().size() == baseline, 2000, 100);

      live.stop();
      backup.stop();
   }
}
