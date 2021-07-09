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
package org.apache.activemq.artemis.tests.extras.byteman;

import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.core.config.ScaleDownConfiguration;
import org.apache.activemq.artemis.core.config.ha.LiveOnlyPolicyConfiguration;
import org.apache.activemq.artemis.core.server.cluster.impl.MessageLoadBalancingType;
import org.apache.activemq.artemis.tests.integration.cluster.distribution.ClusterTestBase;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(BMUnitRunner.class)
public class ScaleDownFailureTest extends ClusterTestBase {

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      setupLiveServer(0, isFileStorage(), HAType.SharedNothingReplication, isNetty(), true);
      setupLiveServer(1, isFileStorage(), HAType.SharedNothingReplication, isNetty(), true);
      if (isGrouped()) {
         ScaleDownConfiguration scaleDownConfiguration = new ScaleDownConfiguration();
         scaleDownConfiguration.setGroupName("bill");
         ((LiveOnlyPolicyConfiguration) servers[0].getConfiguration().getHAPolicyConfiguration()).setScaleDownConfiguration(scaleDownConfiguration);
         ((LiveOnlyPolicyConfiguration) servers[1].getConfiguration().getHAPolicyConfiguration()).setScaleDownConfiguration(scaleDownConfiguration);
      }
      setupClusterConnection("cluster0", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 0, 1);
      setupClusterConnection("cluster1", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 1, 0);
      startServers(0, 1);
      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
   }

   protected boolean isNetty() {
      return true;
   }

   protected boolean isGrouped() {
      return false;
   }

   @Test
   @BMRule(
      name = "blow-up",
      targetClass = "org.apache.activemq.artemis.api.core.client.ServerLocator",
      targetMethod = "createSessionFactory(org.apache.activemq.artemis.api.core.TransportConfiguration, int, boolean)",
      isInterface = true,
      targetLocation = "ENTRY",
      action = "throw new Exception()")
   public void testScaleDownWhenRemoteServerIsUnavailable() throws Exception {
      final int TEST_SIZE = 1;
      final String addressName = "testAddress";
      final String queueName1 = "testQueue1";
      final String queueName2 = "testQueue2";

      // create 2 queues on each node mapped to the same address
      createQueue(0, addressName, queueName1, null, false);
      createQueue(0, addressName, queueName2, null, false);
      createQueue(1, addressName, queueName1, null, false);
      createQueue(1, addressName, queueName2, null, false);

      // send messages to node 0
      send(0, addressName, TEST_SIZE, false, null);

      // consume a message from node 0
      addConsumer(0, 0, queueName2, null, false);
      ClientMessage clientMessage = consumers[0].getConsumer().receive();
      Assert.assertNotNull(clientMessage);
      clientMessage.acknowledge();
      consumers[0].getSession().commit();
      removeConsumer(0);

      servers[0].stop();

      addConsumer(0, 1, queueName1, null);
      clientMessage = consumers[0].getConsumer().receive(250);
      Assert.assertNull(clientMessage);
   }
}
