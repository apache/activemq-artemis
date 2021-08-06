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
package org.apache.activemq.artemis.core.config.balancing;

import org.apache.activemq.artemis.core.server.balancing.targets.TargetKey;

import java.io.Serializable;

public class BrokerBalancerConfiguration implements Serializable {

   private String name = null;
   private TargetKey targetKey = TargetKey.SOURCE_IP;
   private String targetKeyFilter = null;
   private String localTargetFilter = null;
   private int cacheTimeout = -1;
   private PoolConfiguration poolConfiguration = null;
   private PolicyConfiguration policyConfiguration = null;

   public String getName() {
      return name;
   }

   public BrokerBalancerConfiguration setName(String name) {
      this.name = name;
      return this;
   }

   public TargetKey getTargetKey() {
      return targetKey;
   }

   public BrokerBalancerConfiguration setTargetKey(TargetKey targetKey) {
      this.targetKey = targetKey;
      return this;
   }

   public String getTargetKeyFilter() {
      return targetKeyFilter;
   }

   public BrokerBalancerConfiguration setTargetKeyFilter(String targetKeyFilter) {
      this.targetKeyFilter = targetKeyFilter;
      return this;
   }

   public String getLocalTargetFilter() {
      return localTargetFilter;
   }

   public BrokerBalancerConfiguration setLocalTargetFilter(String localTargetFilter) {
      this.localTargetFilter = localTargetFilter;
      return this;
   }

   public int getCacheTimeout() {
      return cacheTimeout;
   }

   public BrokerBalancerConfiguration setCacheTimeout(int cacheTimeout) {
      this.cacheTimeout = cacheTimeout;
      return this;
   }

   public PolicyConfiguration getPolicyConfiguration() {
      return policyConfiguration;
   }

   public BrokerBalancerConfiguration setPolicyConfiguration(PolicyConfiguration policyConfiguration) {
      this.policyConfiguration = policyConfiguration;
      return this;
   }

   public PoolConfiguration getPoolConfiguration() {
      return poolConfiguration;
   }

   public BrokerBalancerConfiguration setPoolConfiguration(PoolConfiguration poolConfiguration) {
      this.poolConfiguration = poolConfiguration;
      return this;
   }
}
