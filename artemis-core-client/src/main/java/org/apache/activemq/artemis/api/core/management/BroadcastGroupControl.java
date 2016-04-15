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

/**
 * A BroadcastGroupControl is used to manage a broadcast group.
 */
public interface BroadcastGroupControl extends ActiveMQComponentControl {

   /**
    * Returns the configuration name of this broadcast group.
    */
   @Attribute(desc = "name of this broadcast group")
   String getName();

   /**
    * Returns the local port this broadcast group is bound to.
    */
   @Attribute(desc = "local port this broadcast group is bound to")
   int getLocalBindPort() throws Exception;

   /**
    * Returns the address this broadcast group is broadcasting to.
    */
   @Attribute(desc = "address this broadcast group is broadcasting to")
   String getGroupAddress() throws Exception;

   /**
    * Returns the port this broadcast group is broadcasting to.
    */
   @Attribute(desc = "port this broadcast group is broadcasting to")
   int getGroupPort() throws Exception;

   /**
    * Returns the period used by this broadcast group.
    */
   @Attribute(desc = "period used by this broadcast group")
   long getBroadcastPeriod();

   /**
    * Returns the pairs of live-backup connectors that are broadcasted by this broadcast group.
    */
   @Attribute(desc = "pairs of live-backup connectors that are broadcasted by this broadcast group")
   Object[] getConnectorPairs();

   /**
    * Returns the pairs of live-backup connectors that are broadcasted by this broadcast group
    * using JSON serialization.
    */
   @Attribute(desc = "pairs of live-backup connectors that are broadcasted by this broadcast group using JSON serialization")
   String getConnectorPairsAsJSON() throws Exception;
}
