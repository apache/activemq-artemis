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

import org.apache.activemq.artemis.api.core.ActiveMQClusterSecurityException;
import org.apache.activemq.artemis.core.client.impl.ClientSessionFactoryInternal;
import org.apache.activemq.artemis.core.client.impl.ServerLocatorImpl;
import org.apache.activemq.artemis.core.server.cluster.ActiveMQServerSideProtocolManagerFactory;
import org.apache.activemq.artemis.core.server.cluster.ClusterControl;
import org.apache.activemq.artemis.core.server.cluster.ClusterController;
import org.apache.activemq.artemis.core.server.cluster.impl.MessageLoadBalancingType;
import org.apache.activemq.artemis.tests.integration.cluster.distribution.ClusterTestBase;
import org.junit.Before;
import org.junit.Test;

public class ClusterControllerTest extends ClusterTestBase {

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      setupServer(0, isFileStorage(), true);
      setupServer(1, isFileStorage(), true);

      getServer(0).getConfiguration().getAcceptorConfigurations().add(createTransportConfiguration(false, true, generateParams(0, false)));
      getServer(1).getConfiguration().getAcceptorConfigurations().add(createTransportConfiguration(false, true, generateParams(1, false)));

      getServer(0).getConfiguration().setSecurityEnabled(true);
      getServer(1).getConfiguration().setSecurityEnabled(true);

      getServer(1).getConfiguration().setClusterPassword("something different");

      setupClusterConnection("cluster0", "queues", MessageLoadBalancingType.ON_DEMAND, 1, true, 0);
      setupClusterConnection("cluster0", "queues", MessageLoadBalancingType.ON_DEMAND, 1, true, 1);

      startServers(0);
      startServers(1);
   }

   @Test
   public void controlWithDifferentConnector() throws Exception {
      try (ServerLocatorImpl locator = (ServerLocatorImpl) createInVMNonHALocator()) {
         locator.setProtocolManagerFactory(ActiveMQServerSideProtocolManagerFactory.getInstance(locator));
         ClusterController controller = new ClusterController(getServer(0), getServer(0).getScheduledPool());
         ClusterControl clusterControl = controller.connectToNodeInCluster((ClientSessionFactoryInternal) locator.createSessionFactory());
         clusterControl.authorize();
      }
   }

   @Test
   public void controlWithDifferentPassword() throws Exception {
      try (ServerLocatorImpl locator = (ServerLocatorImpl) createInVMNonHALocator()) {
         locator.setProtocolManagerFactory(ActiveMQServerSideProtocolManagerFactory.getInstance(locator));
         ClusterController controller = new ClusterController(getServer(1), getServer(1).getScheduledPool());
         ClusterControl clusterControl = controller.connectToNodeInCluster((ClientSessionFactoryInternal) locator.createSessionFactory());
         try {
            clusterControl.authorize();
            fail("should throw ActiveMQClusterSecurityException");
         } catch (Exception e) {
            assertTrue("should throw ActiveMQClusterSecurityException", e instanceof ActiveMQClusterSecurityException);
         }
      }
   }
}
