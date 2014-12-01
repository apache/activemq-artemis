/**
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
package org.apache.activemq.api.core;

/**
 * This interface is needed for making a DiscoveryGroupConfiguration backward
 * compatible with version 2.2 clients. It is used to extract from new
 * {@link org.apache.activemq.api.core.BroadcastEndpointFactoryConfiguration} the four
 * UDP attributes in order to form a version 2.2 DiscoveryGroupConfiguration
 * in time of serialization.
 *
 * @see DiscoveryGroupConfiguration#readObject(java.io.ObjectInputStream)
 * @see DiscoveryGroupConfiguration#writeObject(java.io.ObjectOutputStream)
 *
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         12/13/12
 */
public interface DiscoveryGroupConfigurationCompatibilityHelper
{
// XXX No javadocs
   String getLocalBindAddress();

// XXX No javadocs
   int getLocalBindPort();

// XXX No javadocs
   String getGroupAddress();

// XXX No javadocs
   int getGroupPort();
}
