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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.BridgeConfiguration;
import org.apache.activemq.artemis.core.config.CoreQueueConfiguration;
import org.apache.activemq.artemis.core.remoting.impl.invm.TransportConstants;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.cluster.impl.BridgeImpl;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(BMUnitRunner.class)
public class BridgeServerLocatorConfigurationTest extends ActiveMQTestBase {

   private static final long BRIDGE_TTL = 1234L;
   private static final String BRIDGE_NAME = "bridge1";

   protected boolean isNetty() {
      return false;
   }

   private String getConnector() {
      if (isNetty()) {
         return NETTY_CONNECTOR_FACTORY;
      }
      return INVM_CONNECTOR_FACTORY;
   }

   @Test
   @BMRule(name = "check connection ttl",
      targetClass = "org.apache.activemq.artemis.tests.extras.byteman.BridgeServerLocatorConfigurationTest",
      targetMethod = "getBridgeTTL(ActiveMQServer, String)", targetLocation = "EXIT",
      action = "$! = $0.getConfiguredBridge($1).serverLocator.getConnectionTTL();")
   /**
    * Checks the connection ttl by using byteman to override the methods on this class to return the value of private variables in the Bridge.
    * @throws Exception
    *
    * The byteman rule on this test overwrites the {@link #getBridgeTTL} method to retrieve the bridge called {@link @BRIDGE_NAME}.
    * It the overrides the return value to be the value of the connection ttl. Note that the unused String parameter is required to
    * ensure that byteman populates the $1 variable, otherwise it will not bind correctly.
    */ public void testConnectionTTLOnBridge() throws Exception {
      Map<String, Object> server0Params = new HashMap<>();
      ActiveMQServer serverWithBridge = createClusteredServerWithParams(isNetty(), 0, true, server0Params);

      Map<String, Object> server1Params = new HashMap<>();
      if (isNetty()) {
         server1Params.put("port", org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants.DEFAULT_PORT + 1);
      } else {
         server1Params.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
      }
      ActiveMQServer server1 = createClusteredServerWithParams(isNetty(), 1, true, server1Params);
      try {
         final String testAddress = "testAddress";
         final String queueName0 = "queue0";
         final String forwardAddress = "forwardAddress";
         final String queueName1 = "queue1";

         Map<String, TransportConfiguration> connectors = new HashMap<>();
         TransportConfiguration server1tc = new TransportConfiguration(getConnector(), server1Params);
         connectors.put(server1tc.getName(), server1tc);

         serverWithBridge.getConfiguration().setConnectorConfigurations(connectors);

         ArrayList<String> staticConnectors = new ArrayList<>();
         staticConnectors.add(server1tc.getName());

         BridgeConfiguration bridgeConfiguration = new BridgeConfiguration().setName(BRIDGE_NAME).setQueueName(queueName0).setForwardingAddress(forwardAddress).setConnectionTTL(BRIDGE_TTL).setRetryInterval(1000).setReconnectAttempts(0).setReconnectAttemptsOnSameNode(0).setConfirmationWindowSize(1024).setStaticConnectors(staticConnectors);

         List<BridgeConfiguration> bridgeConfigs = new ArrayList<>();
         bridgeConfigs.add(bridgeConfiguration);
         serverWithBridge.getConfiguration().setBridgeConfigurations(bridgeConfigs);

         CoreQueueConfiguration queueConfig0 = new CoreQueueConfiguration().setAddress(testAddress).setName(queueName0);
         List<CoreQueueConfiguration> queueConfigs0 = new ArrayList<>();
         queueConfigs0.add(queueConfig0);
         serverWithBridge.getConfiguration().setQueueConfigurations(queueConfigs0);

         CoreQueueConfiguration queueConfig1 = new CoreQueueConfiguration().setAddress(forwardAddress).setName(queueName1);
         List<CoreQueueConfiguration> queueConfigs1 = new ArrayList<>();
         queueConfigs1.add(queueConfig1);
         server1.getConfiguration().setQueueConfigurations(queueConfigs1);

         server1.start();
         waitForServerToStart(server1);

         serverWithBridge.start();
         waitForServerToStart(serverWithBridge);

         long bridgeTTL = getBridgeTTL(serverWithBridge, BRIDGE_NAME);

         assertEquals(BRIDGE_TTL, bridgeTTL);
      } finally {
         serverWithBridge.stop();

         server1.stop();
      }
   }

   /**
    * Method for byteman to wrap around and do its magic with to return the ttl from private members
    * rather than -1
    *
    * @param bridgeServer
    * @param bridgeName
    * @return
    */
   private long getBridgeTTL(ActiveMQServer bridgeServer, String bridgeName) {
      return -1L;
   }

   /**
    * Byteman seems to need this method so that it gets back the concrete type not the interface
    *
    * @param bridgeServer
    * @return
    */
   private BridgeImpl getConfiguredBridge(ActiveMQServer bridgeServer) {
      return getConfiguredBridge(bridgeServer, BRIDGE_NAME);
   }

   private BridgeImpl getConfiguredBridge(ActiveMQServer bridgeServer, String bridgeName) {
      return (BridgeImpl) bridgeServer.getClusterManager().getBridges().get(bridgeName);
   }
}
