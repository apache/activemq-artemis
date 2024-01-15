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

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.cluster.RemoteQueueBinding;
import org.apache.activemq.artemis.core.server.cluster.impl.MessageLoadBalancingType;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.util.Wait;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MessageLoadBalancingTest extends ClusterTestBase {

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      start();
   }

   private void start() throws Exception {
      setupServers();

      setRedistributionDelay(0);
   }

   protected boolean isNetty() {
      return false;
   }

   @Test
   public void testMessageLoadBalancingOff() throws Exception {
      setupCluster(MessageLoadBalancingType.OFF);

      startServers(0, 1);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);

      addConsumer(1, 1, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 0, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 1, 1, false);
      waitForBindings(1, "queues.testaddress", 1, 0, false);

      addConsumer(0, 0, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 1, 1, false);
      waitForBindings(1, "queues.testaddress", 1, 1, false);

      send(0, "queues.testaddress", 10, false, null);

      ClientMessage message = getConsumer(1).receive(1000);
      Assert.assertNull(message);

      for (int i = 0; i < 10; i++) {
         message = getConsumer(0).receive(5000);
         Assert.assertNotNull("" + i, message);
         message.acknowledge();
      }

      ClientMessage clientMessage = getConsumer(0).receiveImmediate();
      Assert.assertNull(clientMessage);
   }

   @Test
   public void testMessageLoadBalancingWithFiltersUpdate() throws Exception {
      setupCluster(MessageLoadBalancingType.ON_DEMAND);

      startServers(0, 1);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);

      waitForBindings(0, "queues.testaddress", 1, 0, true);
      waitForBindings(0, "queues.testaddress", 1, 0, false);
      waitForBindings(1, "queues.testaddress", 1, 0, true);
      waitForBindings(1, "queues.testaddress", 1, 0, false);

      Binding[] bindings = new Binding[2];
      PostOffice[] po = new PostOffice[2];
      for (int i = 0; i < 2; i++) {
         po[i] = servers[i].getPostOffice();
         bindings[i] = po[i].getBinding(new SimpleString("queue0"));
         Assert.assertNotNull(bindings[i]);

         Queue queue0 = (Queue)bindings[i].getBindable();
         Assert.assertNotNull(queue0);

         QueueConfiguration qconfig = queue0.getQueueConfiguration();
         Assert.assertNotNull(qconfig);

         qconfig.setFilterString("color = 'red'");
         po[i].updateQueue(qconfig, true);
      }

      SimpleString clusterName0 = bindings[1].getClusterName();
      RemoteQueueBinding remoteBinding = (RemoteQueueBinding) po[0].getBinding(clusterName0);
      Assert.assertNotNull(remoteBinding);

      Wait.assertEquals("color = 'red'", () -> {
         Filter filter = remoteBinding.getFilter();
         if (filter == null) {
            return filter;
         }
         return filter.getFilterString().toString();
      });
   }

   protected void setupCluster(final MessageLoadBalancingType messageLoadBalancingType) throws Exception {
      setupClusterConnection("cluster0", "queues", messageLoadBalancingType, 1, isNetty(), 0, 1);

      setupClusterConnection("cluster1", "queues", messageLoadBalancingType, 1, isNetty(), 1, 0);
   }

   protected void setRedistributionDelay(final long delay) {
      AddressSettings as = new AddressSettings().setRedistributionDelay(delay);

      getServer(0).getAddressSettingsRepository().addMatch("queues.*", as);
      getServer(1).getAddressSettingsRepository().addMatch("queues.*", as);
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
