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
package org.apache.activemq6.api.core.management;

/**
 * A DivertControl is used to manage a divert.
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public interface DivertControl
{
   /**
    * Returns the filter used by this divert.
    */
   String getFilter();

   /**
    * Returns whether this divert is exclusive.
    * <br>
    * if {@code true} messages will be exclusively diverted and will not be routed to the origin address,
    * else messages will be routed both to the origin address and the forwarding address.
    */
   boolean isExclusive();

   /**
    * Returns the cluster-wide unique name of this divert.
    */
   String getUniqueName();

   /**
    * Returns the routing name of this divert.
    */
   String getRoutingName();

   /**
    * Returns the origin address used by this divert.
    */
   String getAddress();

   /**
    * Returns the forwarding address used by this divert.
    */
   String getForwardingAddress();

   /**
    * Return the name of the org.apache.activemq6.core.server.cluster.Transformer implementation associated to this bridge.
    */
   String getTransformerClassName();
}
