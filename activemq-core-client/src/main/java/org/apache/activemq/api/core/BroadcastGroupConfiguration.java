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
package org.apache.activemq.api.core;

import java.io.Serializable;
import java.util.List;

import org.apache.activemq.api.config.HornetQDefaultConfiguration;


/**
 * The basic configuration used to determine how the server will broadcast members
 * This is analogous to {@link org.apache.activemq.api.core.DiscoveryGroupConfiguration}
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
 *
 */
public final class BroadcastGroupConfiguration implements Serializable
{
   private static final long serialVersionUID = 2335634694112319124L;

   private String name = null;

   private long broadcastPeriod = HornetQDefaultConfiguration.getDefaultBroadcastPeriod();

   private BroadcastEndpointFactoryConfiguration endpointFactoryConfiguration = null;

   private List<String> connectorInfos = null;

   public BroadcastGroupConfiguration()
   {
   }

   public String getName()
   {
      return name;
   }

   public long getBroadcastPeriod()
   {
      return broadcastPeriod;
   }

   public List<String> getConnectorInfos()
   {
      return connectorInfos;
   }

   public BroadcastGroupConfiguration setName(final String name)
   {
      this.name = name;
      return this;
   }

   public BroadcastGroupConfiguration setBroadcastPeriod(final long broadcastPeriod)
   {
      this.broadcastPeriod = broadcastPeriod;
      return this;
   }

   public BroadcastGroupConfiguration setConnectorInfos(final List<String> connectorInfos)
   {
      this.connectorInfos = connectorInfos;
      return this;
   }

   public BroadcastEndpointFactoryConfiguration getEndpointFactoryConfiguration()
   {
      return endpointFactoryConfiguration;
   }

   public BroadcastGroupConfiguration setEndpointFactoryConfiguration(BroadcastEndpointFactoryConfiguration endpointFactoryConfiguration)
   {
      this.endpointFactoryConfiguration = endpointFactoryConfiguration;
      return this;
   }

   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = 1;
      result = prime * result + (int)(broadcastPeriod ^ (broadcastPeriod >>> 32));
      result = prime * result + ((connectorInfos == null) ? 0 : connectorInfos.hashCode());
      result = prime * result + ((endpointFactoryConfiguration == null) ? 0 : endpointFactoryConfiguration.hashCode());
      result = prime * result + ((name == null) ? 0 : name.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj)
   {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      BroadcastGroupConfiguration other = (BroadcastGroupConfiguration)obj;
      if (broadcastPeriod != other.broadcastPeriod)
         return false;
      if (connectorInfos == null)
      {
         if (other.connectorInfos != null)
            return false;
      }
      else if (!connectorInfos.equals(other.connectorInfos))
         return false;
      if (endpointFactoryConfiguration == null)
      {
         if (other.endpointFactoryConfiguration != null)
            return false;
      }
      else if (!endpointFactoryConfiguration.equals(other.endpointFactoryConfiguration))
         return false;
      if (name == null)
      {
         if (other.name != null)
            return false;
      }
      else if (!name.equals(other.name))
         return false;
      return true;
   }
}
