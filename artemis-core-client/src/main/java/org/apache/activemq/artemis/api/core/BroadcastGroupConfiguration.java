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

import java.io.Serializable;
import java.util.List;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;

/**
 * The basic configuration used to determine how the server will broadcast members
 * This is analogous to {@link DiscoveryGroupConfiguration}
 */
public final class BroadcastGroupConfiguration implements Serializable {

   private static final long serialVersionUID = 2335634694112319124L;

   private String name = null;

   private long broadcastPeriod = ActiveMQDefaultConfiguration.getDefaultBroadcastPeriod();

   private BroadcastEndpointFactory endpointFactory = null;

   private List<String> connectorInfos = null;

   public BroadcastGroupConfiguration() {
   }

   public String getName() {
      return name;
   }

   public long getBroadcastPeriod() {
      return broadcastPeriod;
   }

   public List<String> getConnectorInfos() {
      return connectorInfos;
   }

   public BroadcastGroupConfiguration setName(final String name) {
      this.name = name;
      return this;
   }

   public BroadcastGroupConfiguration setBroadcastPeriod(final long broadcastPeriod) {
      this.broadcastPeriod = broadcastPeriod;
      return this;
   }

   public BroadcastGroupConfiguration setConnectorInfos(final List<String> connectorInfos) {
      this.connectorInfos = connectorInfos;
      return this;
   }

   public BroadcastEndpointFactory getEndpointFactory() {
      return endpointFactory;
   }

   public BroadcastGroupConfiguration setEndpointFactory(BroadcastEndpointFactory endpointFactory) {
      this.endpointFactory = endpointFactory;
      return this;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + (int) (broadcastPeriod ^ (broadcastPeriod >>> 32));
      result = prime * result + ((connectorInfos == null) ? 0 : connectorInfos.hashCode());
      result = prime * result + ((endpointFactory == null) ? 0 : endpointFactory.hashCode());
      result = prime * result + ((name == null) ? 0 : name.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      BroadcastGroupConfiguration other = (BroadcastGroupConfiguration) obj;
      if (broadcastPeriod != other.broadcastPeriod)
         return false;
      if (connectorInfos == null) {
         if (other.connectorInfos != null)
            return false;
      } else if (!connectorInfos.equals(other.connectorInfos))
         return false;
      if (endpointFactory == null) {
         if (other.endpointFactory != null)
            return false;
      } else if (!endpointFactory.equals(other.endpointFactory))
         return false;
      if (name == null) {
         if (other.name != null)
            return false;
      } else if (!name.equals(other.name))
         return false;
      return true;
   }
}
