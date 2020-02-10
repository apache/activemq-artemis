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
package org.apache.activemq.artemis.tests.integration.cluster.distribution;

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.server.cluster.impl.MessageLoadBalancingType;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.integration.IntegrationTestLogger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ClusteredQueuesTest extends ClusterTestBase {

   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      start();
   }

   private void start() throws Exception {
      setupServers();
   }

   protected boolean isNetty() {
      return false;
   }

   @Override
   protected void setSessionFactoryCreateLocator(int node, boolean ha, TransportConfiguration serverTotc) {
      super.setSessionFactoryCreateLocator(node, ha, serverTotc);
      locators[node].setConsumerWindowSize(0);
   }

   @Test
   public void testNoClusteredQueueonDefault() throws Exception {
      setupCluster(MessageLoadBalancingType.ON_DEMAND);

      ClusteredQueuesTest.log.info("Doing test");

      startServers(0, 1);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());

      createQueue(0, "queues.testaddress", "queues.testaddress", null, true, RoutingType.ANYCAST);
      addConsumer(0, 0, "queues.testaddress", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);

      Binding bindable = servers[1].getPostOffice().getBinding(new SimpleString("queues.testaddress"));
      Assert.assertTrue(bindable == null);
      stopServers(0, 1);
      ClusteredQueuesTest.log.info("Test done");
   }

   @Test
   public void testClusteredQueueAnycast() throws Exception {
      setupCluster(MessageLoadBalancingType.ON_DEMAND);
      AddressSettings setting = new AddressSettings().setClusteredQueues(true);

      servers[0].getAddressSettingsRepository().addMatch("queues.testaddress", setting);
      servers[1].getAddressSettingsRepository().addMatch("queues.testaddress", setting);

      ClusteredQueuesTest.log.info("Doing test");

      startServers(0, 1);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());

      createQueue(0, "queues.testaddress", "queues.testaddress", null, true, RoutingType.ANYCAST);
      addConsumer(0, 0, "queues.testaddress", null);

      waitForBindings(1, "queues.testaddress", 1, 0, true);

      Binding bindable = servers[1].getPostOffice().getBinding(new SimpleString("queues.testaddress"));
      Assert.assertTrue(bindable != null);
      stopServers(0, 1);
      ClusteredQueuesTest.log.info("Test done");
   }

   @Test
   public void testClusteredQueueMulticast() throws Exception {
      setupCluster(MessageLoadBalancingType.ON_DEMAND);
      AddressSettings setting = new AddressSettings().setClusteredQueues(true);

      servers[0].getAddressSettingsRepository().addMatch("queues.testaddress", setting);
      servers[1].getAddressSettingsRepository().addMatch("queues.testaddress", setting);
      servers[0].getAddressSettingsRepository().addMatch("queue0", setting);
      servers[1].getAddressSettingsRepository().addMatch("queue0", setting);

      ClusteredQueuesTest.log.info("Doing test");

      startServers(0, 1);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());

      createQueue(0, "queues.testaddress", "queues0", null, true, RoutingType.MULTICAST);
      addConsumer(0, 0, "queues0", null);
      waitForBindings(1, "queues.testaddress", 1, 0, true);

      Binding bindable = servers[1].getPostOffice().getBinding(new SimpleString("queues0"));
      Assert.assertTrue(bindable != null);
      stopServers(0, 1);
      ClusteredQueuesTest.log.info("Test done");
   }

   protected void setupCluster(final MessageLoadBalancingType messageLoadBalancingType) throws Exception {
      setupClusterConnection("cluster0", "queues", messageLoadBalancingType, 1, isNetty(), 0, 1);
      setupClusterConnection("cluster1", "queues", messageLoadBalancingType, 1, isNetty(), 1, 0);
   }

   protected void setupServers() throws Exception {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
   }

   protected void stopServers() throws Exception {
      closeAllConsumers();
      closeAllSessionFactories();
      closeAllServerLocatorsFactories();
      stopServers(0, 1);
      clearServer(0, 1);
   }
}