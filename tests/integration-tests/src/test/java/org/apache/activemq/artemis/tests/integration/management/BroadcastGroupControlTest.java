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
package org.apache.activemq.artemis.tests.integration.management;

import javax.json.JsonArray;
import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.artemis.api.core.BroadcastGroupConfiguration;
import org.apache.activemq.artemis.api.core.JsonUtil;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.UDPBroadcastEndpointFactory;
import org.apache.activemq.artemis.api.core.management.BroadcastGroupControl;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BroadcastGroupControlTest extends ManagementTestBase {

   private ActiveMQServer server;
   BroadcastGroupConfiguration broadcastGroupConfig;
   BroadcastGroupControl broadcastGroupControl;

   @Test
   public void testAttributes() throws Exception {
      UDPBroadcastEndpointFactory udpCfg = (UDPBroadcastEndpointFactory) broadcastGroupConfig.getEndpointFactory();
      Assert.assertEquals(broadcastGroupConfig.getName(), broadcastGroupControl.getName());
      Assert.assertEquals(udpCfg.getGroupAddress(), broadcastGroupControl.getGroupAddress());
      Assert.assertEquals(udpCfg.getGroupPort(), broadcastGroupControl.getGroupPort());
      Assert.assertEquals(udpCfg.getLocalBindPort(), broadcastGroupControl.getLocalBindPort());
      Assert.assertEquals(broadcastGroupConfig.getBroadcastPeriod(), broadcastGroupControl.getBroadcastPeriod());

      Object[] connectorPairs = broadcastGroupControl.getConnectorPairs();
      Assert.assertEquals(1, connectorPairs.length);

      String connectorPairData = (String) connectorPairs[0];
      Assert.assertEquals(broadcastGroupConfig.getConnectorInfos().get(0), connectorPairData);
      String jsonString = broadcastGroupControl.getConnectorPairsAsJSON();
      Assert.assertNotNull(jsonString);
      JsonArray array = JsonUtil.readJsonArray(jsonString);
      Assert.assertEquals(1, array.size());
      Assert.assertEquals(broadcastGroupConfig.getConnectorInfos().get(0), array.getString(0));

      Assert.assertTrue(broadcastGroupControl.isStarted());
   }

   @Test
   public void testStartStop() throws Exception {
      // started by the server
      Assert.assertTrue(broadcastGroupControl.isStarted());

      broadcastGroupControl.stop();
      Assert.assertFalse(broadcastGroupControl.isStarted());

      broadcastGroupControl.start();
      Assert.assertTrue(broadcastGroupControl.isStarted());
   }

   protected BroadcastGroupControl createManagementControl(final String name) throws Exception {
      return ManagementControlHelper.createBroadcastGroupControl(name, mbeanServer);
   }

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      TransportConfiguration connectorConfiguration = new TransportConfiguration(NETTY_CONNECTOR_FACTORY);
      List<String> connectorInfos = new ArrayList<>();
      connectorInfos.add(connectorConfiguration.getName());
      broadcastGroupConfig = new BroadcastGroupConfiguration().setName(RandomUtil.randomString()).setBroadcastPeriod(RandomUtil.randomPositiveInt()).setConnectorInfos(connectorInfos).setEndpointFactory(new UDPBroadcastEndpointFactory().setGroupAddress(getUDPDiscoveryAddress()).setGroupPort(getUDPDiscoveryPort()).setLocalBindPort(1198));

      Configuration config = createDefaultInVMConfig().setJMXManagementEnabled(true).addConnectorConfiguration(connectorConfiguration.getName(), connectorConfiguration).addBroadcastGroupConfiguration(broadcastGroupConfig);
      server = addServer(ActiveMQServers.newActiveMQServer(config, mbeanServer, false));
      server.start();

      broadcastGroupControl = createManagementControl(broadcastGroupConfig.getName());
   }
}
