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
package org.apache.activemq.artemis.api.core.management;

import java.util.Map;

/**
 * A ClusterConnectionControl is used to manage a cluster connection.
 */
public interface ClusterConnectionControl extends ActiveMQComponentControl {

   /**
    * Returns the configuration name of this cluster connection.
    */
   @Attribute(desc = "name of this cluster connection")
   String getName();

   /**
    * Returns the address used by this cluster connection.
    */
   @Attribute(desc = "address used by this cluster connection")
   String getAddress();

   /**
    * Returns the node ID used by this cluster connection.
    */
   @Attribute(desc = "node ID used by this cluster connection")
   String getNodeID();

   /**
    * Return whether this cluster connection use duplicate detection.
    */
   @Attribute(desc = "whether this cluster connection use duplicate detection")
   boolean isDuplicateDetection();

   /**
    * Return the type of message load balancing strategy this bridge will use.
    */
   @Attribute(desc = "type of message load balancing strategy this bridge will use")
   String getMessageLoadBalancingType();

   /**
    * Return the Topology that this Cluster Connection knows about
    */
   @Attribute(desc = "Topology that this Cluster Connection knows about")
   String getTopology();

   /**
    * Returns the maximum number of hops used by this cluster connection.
    */
   @Attribute(desc = "maximum number of hops used by this cluster connection")
   int getMaxHops();

   /**
    * Returns the list of static connectors
    */
   @Attribute(desc = "list of static connectors")
   Object[] getStaticConnectors();

   /**
    * Returns the list of static connectors as JSON
    */
   @Attribute(desc = "list of static connectors as JSON")
   String getStaticConnectorsAsJSON() throws Exception;

   /**
    * Returns the name of the discovery group used by this cluster connection.
    */
   @Attribute(desc = "name of the discovery group used by this cluster connection")
   String getDiscoveryGroupName();

   /**
    * Returns the connection retry interval used by this cluster connection.
    */
   @Attribute(desc = "connection retry interval used by this cluster connection")
   long getRetryInterval();

   /**
    * Returns a map of the nodes connected to this cluster connection.
    * <br>
    * keys are node IDs, values are the addresses used to connect to the nodes.
    */
   @Attribute(desc = "map of the nodes connected to this cluster connection (keys are node IDs, values are the addresses used to connect to the nodes)")
   Map<String, String> getNodes() throws Exception;
}
