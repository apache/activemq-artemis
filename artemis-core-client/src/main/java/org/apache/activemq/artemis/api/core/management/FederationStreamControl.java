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
 * A BridgeControl is used to manage a federation stream.
 */
public interface FederationStreamControl {

   /**
    * Returns the name of this federation stream
    */
   @Attribute(desc = "name of this federation stream")
   String getName();

   /**
    * Returns any list of static connectors used by this federation stream
    */
   @Attribute(desc = "list of static connectors used by this federation stream")
   String[] getStaticConnectors() throws Exception;

   /**
    * Returns the name of the discovery group used by this federation stream.
    */
   @Attribute(desc = "name of the discovery group used by this federation stream")
   String getDiscoveryGroupName();

   /**
    * Returns the retry interval used by this federation stream.
    */
   @Attribute(desc = "retry interval used by this federation stream")
   long getRetryInterval();

   /**
    * Returns the retry interval multiplier used by this federation stream.
    */
   @Attribute(desc = "retry interval multiplier used by this federation stream")
   double getRetryIntervalMultiplier();

   /**
    * Returns the max retry interval used by this federation stream.
    */
   @Attribute(desc = "max retry interval used by this federation stream")
   long getMaxRetryInterval();

   /**
    * Returns the number of reconnection attempts used by this federation stream.
    */
   @Attribute(desc = "number of reconnection attempts used by this federation stream")
   int getReconnectAttempts();

   /**
    * Returns {@code true} if steam allows a shared connection, {@code false} else.
    */
   @Attribute(desc = "whether this stream will allow the connection to be shared")
   boolean isSharedConnection();

   /**
    * Returns {@code true} if  this connection is configured to pull, {@code false} else.
    */
   @Attribute(desc = "whether this connection is configured to pull")
   boolean isPull();

   /**
    * Returns {@code true} the connection is configured for HA, {@code false} else.
    */
   @Attribute(desc = "whether this connection is configured for HA")
   boolean isHA();

   /**
    * Returns the name of the user the federation is associated with
    */
   @Attribute(desc = "name of the user the federation is associated with")
   String getUser();

}
