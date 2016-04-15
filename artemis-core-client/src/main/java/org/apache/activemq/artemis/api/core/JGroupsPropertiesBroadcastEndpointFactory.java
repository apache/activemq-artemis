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
package org.apache.activemq.artemis.api.core;

import org.apache.activemq.artemis.api.core.jgroups.JChannelManager;

public class JGroupsPropertiesBroadcastEndpointFactory implements BroadcastEndpointFactory {

   private String properties;

   private String channelName;

   private final JChannelManager manager = new JChannelManager();

   @Override
   public BroadcastEndpoint createBroadcastEndpoint() throws Exception {
      return new JGroupsPropertiesBroadcastEndpoint(manager, properties, channelName).initChannel();
   }

   public String getProperties() {
      return properties;
   }

   public JGroupsPropertiesBroadcastEndpointFactory setProperties(String properties) {
      this.properties = properties;
      return this;
   }

   public String getChannelName() {
      return channelName;
   }

   public JGroupsPropertiesBroadcastEndpointFactory setChannelName(String channelName) {
      this.channelName = channelName;
      return this;
   }
}
