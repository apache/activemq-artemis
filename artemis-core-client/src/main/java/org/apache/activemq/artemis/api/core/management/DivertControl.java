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
 * A DivertControl is used to manage a divert.
 */
public interface DivertControl {

   /**
    * Returns the filter used by this divert.
    */
   @Attribute(desc = "filter used by this divert")
   String getFilter();

   /**
    * Returns whether this divert is exclusive.
    * <br>
    * if {@code true} messages will be exclusively diverted and will not be routed to the origin address,
    * else messages will be routed both to the origin address and the forwarding address.
    */
   @Attribute(desc = "whether this divert is exclusive")
   boolean isExclusive();

   /**
    * Returns the cluster-wide unique name of this divert.
    */
   @Attribute(desc = "cluster-wide unique name of this divert")
   String getUniqueName();

   /**
    * Returns the routing name of this divert.
    */
   @Attribute(desc = "routing name of this divert")
   String getRoutingName();

   /**
    * Returns the origin address used by this divert.
    */
   @Attribute(desc = "origin address used by this divert")
   String getAddress();

   /**
    * Returns the forwarding address used by this divert.
    */
   @Attribute(desc = "forwarding address used by this divert")
   String getForwardingAddress();

   /**
    * Return the name of the org.apache.activemq.artemis.core.server.cluster.Transformer implementation associated with this divert.
    */
   @Attribute(desc = "name of the org.apache.activemq.artemis.core.server.cluster.Transformer implementation associated with this divert")
   String getTransformerClassName();

   /**
    * Returns a map of the properties configured for the transformer.
    */
   @Attribute(desc = "map of key, value pairs used to configure the transformer in JSON form")
   String getTransformerPropertiesAsJSON();

   /**
    * Returns a map of the properties configured for the transformer.
    */
   @Attribute(desc = "map of key, value pairs used to configure the transformer")
   Map<String, String> getTransformerProperties() throws Exception;

   /**
    * Returns the routing type used by this divert.
    */
   @Attribute(desc = "routing type used by this divert")
   String getRoutingType();
}
