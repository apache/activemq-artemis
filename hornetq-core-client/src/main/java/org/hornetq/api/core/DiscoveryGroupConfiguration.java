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
package org.hornetq.api.core;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.hornetq.utils.UUIDGenerator;

/**
 * This file represents how we are using Discovery.
 * <p>
 * The discovery configuration could either use plain UDP, or JGroups.<br>
 * If using UDP, all the UDP properties will be filled and the jgroups properties will be
 * {@code null}.<br>
 * If using JGroups, all the UDP properties will be -1 or {@code null} and the jgroups properties
 * will be filled.<br>
 * If by any reason, both properties are filled, the JGroups takes precedence. That means, if
 * {@code jgroupsFile != null} then the Grouping method used will be JGroups.
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author Clebert Suconic
 */
public final class DiscoveryGroupConfiguration implements Serializable
{
   private static final long serialVersionUID = 8657206421727863400L;

   private String name;

   private long refreshTimeout;

   private long discoveryInitialWaitTimeout;

   /*
   * The localBindAddress is needed so we can be backward compatible with 2.2 clients
   * */
   private transient String localBindAddress;

   /*
   * The localBindPort is needed so we can be backward compatible with 2.2 clients
   * */
   private transient int localBindPort;

   /*
   * The groupAddress is needed so we can be backward compatible with 2.2 clients
   * */
   private String groupAddress;

   /*
   * The groupPort is needed so we can be backward compatible with 2.2 clients
   * */
   private int groupPort;

   /*
   * This is the actual object used by the class, it has to be transient so we can handle deserialization with a 2.2 client
   * */
   private transient BroadcastEndpointFactoryConfiguration endpointFactoryConfiguration;

   public DiscoveryGroupConfiguration(final String name,
                                      final long refreshTimeout,
                                      final long discoveryInitialWaitTimeout, BroadcastEndpointFactoryConfiguration
         endpointFactoryConfiguration)
   {
      this.name = name;
      this.refreshTimeout = refreshTimeout;
      this.discoveryInitialWaitTimeout = discoveryInitialWaitTimeout;
      this.endpointFactoryConfiguration = endpointFactoryConfiguration;
      if (endpointFactoryConfiguration instanceof DiscoveryGroupConfigurationCompatibilityHelper)
      {
         DiscoveryGroupConfigurationCompatibilityHelper dgcch = (DiscoveryGroupConfigurationCompatibilityHelper) endpointFactoryConfiguration;
         localBindAddress = dgcch.getLocalBindAddress();
         localBindPort = dgcch.getLocalBindPort();
         groupAddress = dgcch.getGroupAddress();
         groupPort = dgcch.getGroupPort();
      }
   }

   public DiscoveryGroupConfiguration(final long refreshTimeout,
                                      final long discoveryInitialWaitTimeout,
                                            BroadcastEndpointFactoryConfiguration endpointFactoryConfiguration)
   {
      this(UUIDGenerator.getInstance().generateStringUUID(), refreshTimeout, discoveryInitialWaitTimeout, endpointFactoryConfiguration);
   }

   public String getName()
   {
      return name;
   }

   public long getRefreshTimeout()
   {
      return refreshTimeout;
   }

   /**
    * @param name the name to set
    */
   public void setName(final String name)
   {
      this.name = name;
   }

   /**
    * @param refreshTimeout the refreshTimeout to set
    */
   public void setRefreshTimeout(final long refreshTimeout)
   {
      this.refreshTimeout = refreshTimeout;
   }

   /**
    * @return the discoveryInitialWaitTimeout
    */
   public long getDiscoveryInitialWaitTimeout()
   {
      return discoveryInitialWaitTimeout;
   }

   /**
    * @param discoveryInitialWaitTimeout the discoveryInitialWaitTimeout to set
    */
   public void setDiscoveryInitialWaitTimeout(long discoveryInitialWaitTimeout)
   {
      this.discoveryInitialWaitTimeout = discoveryInitialWaitTimeout;
   }

   public BroadcastEndpointFactoryConfiguration getBroadcastEndpointFactoryConfiguration()
   {
      return endpointFactoryConfiguration;
   }

   private void writeObject(ObjectOutputStream out) throws IOException
   {
      out.defaultWriteObject();
      if (groupPort < 0)
      {
         out.writeObject(endpointFactoryConfiguration);
      }
   }

   private void readObject(ObjectInputStream in) throws ClassNotFoundException, IOException
   {
      in.defaultReadObject();
      if (groupPort < 0)
      {
         endpointFactoryConfiguration = (BroadcastEndpointFactoryConfiguration) in.readObject();
      }
      else
      {
         endpointFactoryConfiguration = new UDPBroadcastGroupConfiguration(groupAddress, groupPort, localBindAddress, localBindPort);
      }
   }

   @Override
   public boolean equals(Object o)
   {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      DiscoveryGroupConfiguration that = (DiscoveryGroupConfiguration) o;

      if (discoveryInitialWaitTimeout != that.discoveryInitialWaitTimeout) return false;
      if (refreshTimeout != that.refreshTimeout) return false;
      if (name != null ? !name.equals(that.name) : that.name != null) return false;

      return true;
   }

   @Override
   public int hashCode()
   {
      int result = name != null ? name.hashCode() : 0;
      result = 31 * result + (int) (refreshTimeout ^ (refreshTimeout >>> 32));
      result = 31 * result + (int) (discoveryInitialWaitTimeout ^ (discoveryInitialWaitTimeout >>> 32));
      return result;
   }

   @Override
   public String toString()
   {
      return "DiscoveryGroupConfiguration{" +
         "name='" + name + '\'' +
         ", refreshTimeout=" + refreshTimeout +
         ", discoveryInitialWaitTimeout=" + discoveryInitialWaitTimeout +
         '}';
   }
}
