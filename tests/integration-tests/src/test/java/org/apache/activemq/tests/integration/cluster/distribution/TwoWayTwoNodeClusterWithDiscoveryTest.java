/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.activemq6.tests.integration.cluster.distribution;

/**
 * A TwoWayTwoNodeClusterWithDiscoveryTest
 *
 * @author jmesnil
 *
 *
 */
public class TwoWayTwoNodeClusterWithDiscoveryTest extends TwoWayTwoNodeClusterTest
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   protected static final String groupAddress = "230.1.2.3";

   protected static final int groupPort = 6745;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setupClusters()
   {
      setupDiscoveryClusterConnection("cluster0", 0, "dg1", "queues", false, 1, isNetty());
      setupDiscoveryClusterConnection("cluster1", 1, "dg1", "queues", false, 1, isNetty());
   }

   @Override
   protected void setupServers() throws Exception
   {
      setupLiveServerWithDiscovery(0, groupAddress, groupPort, isFileStorage(), isNetty(), false);
      setupLiveServerWithDiscovery(1, groupAddress, groupPort, isFileStorage(), isNetty(), false);
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
