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

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.core.config.ScaleDownConfiguration;
import org.apache.activemq.artemis.core.config.ha.LiveOnlyPolicyConfiguration;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.cluster.impl.MessageLoadBalancingType;
import org.apache.activemq.artemis.tests.integration.cluster.distribution.ClusterTestBase;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.jboss.logging.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(BMUnitRunner.class)
public class ScaleDownFailoverTest extends ClusterTestBase {
   private static final Logger log = Logger.getLogger(ScaleDownFailoverTest.class);

   protected static int stopCount = 0;
   private static ActiveMQServer[] staticServers;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      stopCount = 0;
      setupLiveServer(0, isFileStorage(), HAType.SharedNothingReplication, isNetty(), true);
      setupLiveServer(1, isFileStorage(), HAType.SharedNothingReplication, isNetty(), true);
      setupLiveServer(2, isFileStorage(), HAType.SharedNothingReplication, isNetty(), true);
      ScaleDownConfiguration scaleDownConfiguration = new ScaleDownConfiguration();
      ScaleDownConfiguration scaleDownConfiguration2 = new ScaleDownConfiguration();
      scaleDownConfiguration2.setEnabled(false);
      ScaleDownConfiguration scaleDownConfiguration3 = new ScaleDownConfiguration();
      scaleDownConfiguration3.setEnabled(false);
      ((LiveOnlyPolicyConfiguration) servers[0].getConfiguration().getHAPolicyConfiguration()).setScaleDownConfiguration(scaleDownConfiguration);
      ((LiveOnlyPolicyConfiguration) servers[1].getConfiguration().getHAPolicyConfiguration()).setScaleDownConfiguration(scaleDownConfiguration2);
      ((LiveOnlyPolicyConfiguration) servers[2].getConfiguration().getHAPolicyConfiguration()).setScaleDownConfiguration(scaleDownConfiguration3);
      if (isGrouped()) {
         scaleDownConfiguration.setGroupName("bill");
         scaleDownConfiguration2.setGroupName("bill");
         scaleDownConfiguration3.setGroupName("bill");
      }
      staticServers = servers;
      setupClusterConnection("cluster0", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 0, 1, 2);
      setupClusterConnection("cluster1", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 1, 0, 2);
      setupClusterConnection("cluster2", "queues", MessageLoadBalancingType.ON_DEMAND, 1, isNetty(), 2, 0, 1);
      scaleDownConfiguration.getConnectors().addAll(servers[0].getConfiguration().getClusterConfigurations().iterator().next().getStaticConnectors());
      scaleDownConfiguration2.getConnectors().addAll(servers[1].getConfiguration().getClusterConfigurations().iterator().next().getStaticConnectors());
      scaleDownConfiguration3.getConnectors().addAll(servers[2].getConfiguration().getClusterConfigurations().iterator().next().getStaticConnectors());

      startServers(0, 1, 2);
      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());
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
      action = "org.apache.activemq.artemis.tests.extras.byteman.ScaleDownFailoverTest.fail($1);")
   public void testScaleDownWhenFirstServerFails() throws Exception {
      final int TEST_SIZE = 2;
      final String addressName = "testAddress";
      final String queueName1 = "testQueue1";
      final String queueName2 = "testQueue2";

      // create 2 queues on each node mapped to the same address
      createQueue(0, addressName, queueName1, null, false);
      createQueue(0, addressName, queueName2, null, false);
      createQueue(1, addressName, queueName1, null, false);
      createQueue(1, addressName, queueName2, null, false);
      createQueue(2, addressName, queueName1, null, false);
      createQueue(2, addressName, queueName2, null, false);

      // send messages to node 0
      send(0, addressName, TEST_SIZE, false, null);

      // consume a message from node 0
      addConsumer(0, 0, queueName2, null);
      ClientMessage clientMessage = consumers[0].getConsumer().receive(250);
      Assert.assertNotNull(clientMessage);
      clientMessage.acknowledge();
      removeConsumer(0);

      servers[0].stop();

      // verify that at least one server stopped
      Assert.assertTrue(!servers[1].isStarted() || !servers[2].isStarted());

      int remainingServer;
      if (servers[1].isStarted())
         remainingServer = 1;
      else
         remainingServer = 2;

      // get the 2 messages from queue 1
      addConsumer(0, remainingServer, queueName1, null);
      clientMessage = consumers[0].getConsumer().receive(250);
      Assert.assertNotNull(clientMessage);
      clientMessage.acknowledge();
      clientMessage = consumers[0].getConsumer().receive(250);
      Assert.assertNotNull(clientMessage);
      clientMessage.acknowledge();

      // ensure there are no more messages on queue 1
      clientMessage = consumers[0].getConsumer().receive(250);
      Assert.assertNull(clientMessage);
      removeConsumer(0);

      // get the 1 message from queue 2
      addConsumer(0, remainingServer, queueName2, null);
      clientMessage = consumers[0].getConsumer().receive(250);
      Assert.assertNotNull(clientMessage);
      clientMessage.acknowledge();

      // ensure there are no more messages on queue 1
      clientMessage = consumers[0].getConsumer().receive(250);
      Assert.assertNull(clientMessage);
      removeConsumer(0);
   }

   public static void fail(TransportConfiguration tc) {
      // only kill one server
      if (stopCount == 0) {
         try {
            for (ActiveMQServer activeMQServer : staticServers) {
               if (activeMQServer != null) {
                  for (TransportConfiguration transportConfiguration : activeMQServer.getConfiguration().getAcceptorConfigurations()) {
                     if (transportConfiguration.getParams().get(TransportConstants.PORT_PROP_NAME).equals(tc.getParams().get(TransportConstants.PORT_PROP_NAME))) {
                        activeMQServer.stop();
                        stopCount++;
                        log.debug("Stopping server listening at: " + tc.getParams().get(TransportConstants.PORT_PROP_NAME));
                     }
                  }
               }
            }
         } catch (Exception e) {
            e.printStackTrace();
         }
      }
   }
}
