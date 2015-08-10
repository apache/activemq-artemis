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

import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.utils.UUIDGenerator;

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
 */
public final class DiscoveryGroupConfiguration implements Serializable {

   private static final long serialVersionUID = 8657206421727863400L;

   private String name = UUIDGenerator.getInstance().generateStringUUID();

   private long refreshTimeout = ActiveMQClient.DEFAULT_DISCOVERY_REFRESH_TIMEOUT;

   private long discoveryInitialWaitTimeout = ActiveMQClient.DEFAULT_DISCOVERY_INITIAL_WAIT_TIMEOUT;

   /*
   * This is the actual object used by the class, it has to be transient so we can handle deserialization with a 2.2 client
   * */
   private BroadcastEndpointFactory endpointFactory;

   public DiscoveryGroupConfiguration() {
   }

   public String getName() {
      return name;
   }

   public long getRefreshTimeout() {
      return refreshTimeout;
   }

   /**
    * @param name the name to set
    */
   public DiscoveryGroupConfiguration setName(final String name) {
      this.name = name;
      return this;
   }

   /**
    * @param refreshTimeout the refreshTimeout to set
    */
   public DiscoveryGroupConfiguration setRefreshTimeout(final long refreshTimeout) {
      this.refreshTimeout = refreshTimeout;
      return this;
   }

   /**
    * @return the discoveryInitialWaitTimeout
    */
   public long getDiscoveryInitialWaitTimeout() {
      return discoveryInitialWaitTimeout;
   }

   /**
    * @param discoveryInitialWaitTimeout the discoveryInitialWaitTimeout to set
    */
   public DiscoveryGroupConfiguration setDiscoveryInitialWaitTimeout(long discoveryInitialWaitTimeout) {
      this.discoveryInitialWaitTimeout = discoveryInitialWaitTimeout;
      return this;
   }

   public BroadcastEndpointFactory getBroadcastEndpointFactory() {
      return endpointFactory;
   }

   public DiscoveryGroupConfiguration setBroadcastEndpointFactory(BroadcastEndpointFactory endpointFactory) {
      this.endpointFactory = endpointFactory;
      return this;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o)
         return true;
      if (o == null || getClass() != o.getClass())
         return false;

      DiscoveryGroupConfiguration that = (DiscoveryGroupConfiguration) o;

      if (discoveryInitialWaitTimeout != that.discoveryInitialWaitTimeout)
         return false;
      if (refreshTimeout != that.refreshTimeout)
         return false;
      if (name != null ? !name.equals(that.name) : that.name != null)
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = name != null ? name.hashCode() : 0;
      result = 31 * result + (int) (refreshTimeout ^ (refreshTimeout >>> 32));
      result = 31 * result + (int) (discoveryInitialWaitTimeout ^ (discoveryInitialWaitTimeout >>> 32));
      return result;
   }

   @Override
   public String toString() {
      return "DiscoveryGroupConfiguration{" +
         "name='" + name + '\'' +
         ", refreshTimeout=" + refreshTimeout +
         ", discoveryInitialWaitTimeout=" + discoveryInitialWaitTimeout +
         '}';
   }
}
