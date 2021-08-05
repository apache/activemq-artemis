/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.integration.management;

import org.apache.activemq.artemis.api.core.JsonUtil;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.management.BrokerBalancerControl;
import org.apache.activemq.artemis.core.server.balancing.policies.FirstElementPolicy;
import org.apache.activemq.artemis.core.server.balancing.targets.TargetKey;
import org.apache.activemq.artemis.tests.integration.balancing.BalancingTestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.json.JsonObject;
import javax.json.JsonValue;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;
import java.util.Map;

public class BrokerBalancerControlTest extends BalancingTestBase {

   private MBeanServer mbeanServer;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      mbeanServer = MBeanServerFactory.createMBeanServer();
   }

   @Override
   @After
   public void tearDown() throws Exception {
      super.tearDown();

      MBeanServerFactory.releaseMBeanServer(mbeanServer);
   }


   @Test
   public void testGetTarget() throws Exception {
      BrokerBalancerControl brokerBalancerControl = getBrokerBalancerControlForTarget();

      CompositeData targetData = brokerBalancerControl.getTarget("admin");
      Assert.assertNotNull(targetData);

      String nodeID = (String)targetData.get("nodeID");
      Assert.assertEquals(getServer(1).getNodeID().toString(), nodeID);

      Boolean local = (Boolean)targetData.get("local");
      Assert.assertEquals(false, local);

      CompositeData connectorData = (CompositeData)targetData.get("connector");
      Assert.assertNotNull(connectorData);

      TransportConfiguration connector = getDefaultServerConnector(1);

      String connectorName = (String)connectorData.get("name");
      Assert.assertEquals(connector.getName(), connectorName);

      String connectorFactoryClassName = (String)connectorData.get("factoryClassName");
      Assert.assertEquals(connector.getFactoryClassName(), connectorFactoryClassName);

      TabularData connectorParams = (TabularData)connectorData.get("params");
      Assert.assertNotNull(connectorParams);

      for (Map.Entry<String, Object> param : connector.getParams().entrySet()) {
         CompositeData paramData = connectorParams.get(new Object[]{param.getKey()});
         Assert.assertEquals(String.valueOf(param.getValue()), paramData.get("value"));
      }
   }
   @Test
   public void testGetTargetAsJSON() throws Exception {
      BrokerBalancerControl brokerBalancerControl = getBrokerBalancerControlForTarget();

      String targetJSON = brokerBalancerControl.getTargetAsJSON("admin");
      Assert.assertNotNull(targetJSON);

      JsonObject targetData = JsonUtil.readJsonObject(targetJSON);
      Assert.assertNotNull(targetData);

      String nodeID = targetData.getString("nodeID");
      Assert.assertEquals(getServer(1).getNodeID().toString(), nodeID);

      Boolean local = targetData.getBoolean("local");
      Assert.assertEquals(false, local);

      JsonObject connectorData = targetData.getJsonObject("connector");
      Assert.assertNotNull(connectorData);

      TransportConfiguration connector = getDefaultServerConnector(1);

      String connectorName = connectorData.getString("name");
      Assert.assertEquals(connector.getName(), connectorName);

      String connectorFactoryClassName = connectorData.getString("factoryClassName");
      Assert.assertEquals(connector.getFactoryClassName(), connectorFactoryClassName);

      JsonObject connectorParams = connectorData.getJsonObject("params");
      Assert.assertNotNull(connectorParams);

      for (Map.Entry<String, Object> param : connector.getParams().entrySet()) {
         JsonValue paramData = connectorParams.get(param.getKey());
         Assert.assertEquals(String.valueOf(param.getValue()), paramData.toString());
      }
   }


   @Test
   public void testGetLocalTarget() throws Exception {
      BrokerBalancerControl brokerBalancerControl = getBrokerBalancerControlForLocalTarget();

      CompositeData targetData = brokerBalancerControl.getTarget("admin");
      Assert.assertNotNull(targetData);

      String nodeID = (String)targetData.get("nodeID");
      Assert.assertEquals(getServer(0).getNodeID().toString(), nodeID);

      Boolean local = (Boolean)targetData.get("local");
      Assert.assertEquals(true, local);

      CompositeData connectorData = (CompositeData)targetData.get("connector");
      Assert.assertNull(connectorData);
   }

   @Test
   public void testGetLocalTargetAsJSON() throws Exception {
      BrokerBalancerControl brokerBalancerControl = getBrokerBalancerControlForLocalTarget();

      String targetJSON = brokerBalancerControl.getTargetAsJSON("admin");
      Assert.assertNotNull(targetJSON);

      JsonObject targetData = JsonUtil.readJsonObject(targetJSON);
      Assert.assertNotNull(targetData);

      String nodeID = targetData.getString("nodeID");
      Assert.assertEquals(getServer(0).getNodeID().toString(), nodeID);

      Boolean local = targetData.getBoolean("local");
      Assert.assertEquals(true, local);

      Assert.assertTrue(targetData.isNull("connector"));
   }

   private BrokerBalancerControl getBrokerBalancerControlForTarget() throws Exception {
      setupLiveServerWithDiscovery(0, GROUP_ADDRESS, GROUP_PORT, true, true, false);
      setupBalancerServerWithDiscovery(0, TargetKey.USER_NAME, FirstElementPolicy.NAME, null, false, null, 1);
      getServer(0).setMBeanServer(mbeanServer);

      setupLiveServerWithDiscovery(1, GROUP_ADDRESS, GROUP_PORT, true, true, false);

      startServers(0, 1);

      return ManagementControlHelper.createBrokerBalancerControl(BROKER_BALANCER_NAME, mbeanServer);
   }

   private BrokerBalancerControl getBrokerBalancerControlForLocalTarget() throws Exception {
      setupLiveServerWithDiscovery(0, GROUP_ADDRESS, GROUP_PORT, true, true, false);
      setupBalancerServerWithDiscovery(0, TargetKey.USER_NAME, FirstElementPolicy.NAME, null, true, null, 1);
      getServer(0).setMBeanServer(mbeanServer);

      startServers(0);

      return ManagementControlHelper.createBrokerBalancerControl(BROKER_BALANCER_NAME, mbeanServer);
   }
}
