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

import java.util.Map;

import org.apache.activemq.artemis.api.core.management.ClusterConnectionControl;
import org.apache.activemq.artemis.api.core.management.ResourceNames;

public class ClusterConnectionControlUsingCoreTest extends ClusterConnectionControlTest {

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // ClusterConnectionControlTest overrides --------------------------------

   @Override
   protected ClusterConnectionControl createManagementControl(final String name) throws Exception {
      return new ClusterConnectionControl() {
         private final CoreMessagingProxy proxy = new CoreMessagingProxy(addServerLocator(createInVMNonHALocator()), ResourceNames.CORE_CLUSTER_CONNECTION + name);

         @Override
         public Object[] getStaticConnectors() {
            return (Object[]) proxy.retrieveAttributeValue("staticConnectors");
         }

         @Override
         public String getStaticConnectorsAsJSON() throws Exception {
            return (String) proxy.retrieveAttributeValue("staticConnectorsAsJSON");
         }

         @Override
         public String getAddress() {
            return (String) proxy.retrieveAttributeValue("address");
         }

         @Override
         public String getDiscoveryGroupName() {
            return (String) proxy.retrieveAttributeValue("discoveryGroupName");
         }

         @Override
         public int getMaxHops() {
            return (Integer) proxy.retrieveAttributeValue("maxHops", Integer.class);
         }

         @Override
         public long getRetryInterval() {
            return (Long) proxy.retrieveAttributeValue("retryInterval", Long.class);
         }

         @Override
         public String getTopology() {
            return (String) proxy.retrieveAttributeValue("topology");
         }

         @Override
         public Map<String, String> getNodes() throws Exception {
            return (Map<String, String>) proxy.retrieveAttributeValue("nodes");
         }

         @Override
         public boolean isDuplicateDetection() {
            return (Boolean) proxy.retrieveAttributeValue("duplicateDetection", Boolean.class);
         }

         @Override
         public String getMessageLoadBalancingType() {
            return (String) proxy.retrieveAttributeValue("messageLoadBalancingType");
         }

         @Override
         public String getName() {
            return (String) proxy.retrieveAttributeValue("name");
         }

         @Override
         public String getNodeID() {
            return (String) proxy.retrieveAttributeValue("nodeID");
         }

         @Override
         public boolean isStarted() {
            return (Boolean) proxy.retrieveAttributeValue("started", Boolean.class);
         }

         @Override
         public void start() throws Exception {
            proxy.invokeOperation("start");
         }

         @Override
         public void stop() throws Exception {
            proxy.invokeOperation("stop");
         }

      };
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
