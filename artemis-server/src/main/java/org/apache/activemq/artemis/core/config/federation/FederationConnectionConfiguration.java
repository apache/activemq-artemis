/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.core.config.federation;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

public class FederationConnectionConfiguration implements Serializable {

   public static long DEFAULT_CIRCUIT_BREAKER_TIMEOUT = 30000;

   private boolean isHA;
   private String discoveryGroupName;
   private List<String> staticConnectors;
   private int priorityAdjustment;

   private long circuitBreakerTimeout = DEFAULT_CIRCUIT_BREAKER_TIMEOUT;
   private String username;
   private String password;

   public String getDiscoveryGroupName() {
      return discoveryGroupName;
   }

   public FederationConnectionConfiguration setDiscoveryGroupName(String discoveryGroupName) {
      this.discoveryGroupName = discoveryGroupName;
      return this;
   }

   public List<String> getStaticConnectors() {
      return staticConnectors;
   }

   public FederationConnectionConfiguration setStaticConnectors(List<String> staticConnectors) {
      this.staticConnectors = staticConnectors;
      return this;
   }

   public boolean isHA() {
      return isHA;
   }

   public FederationConnectionConfiguration setHA(boolean HA) {
      isHA = HA;
      return this;
   }

   public long getCircuitBreakerTimeout() {
      return circuitBreakerTimeout;
   }

   public FederationConnectionConfiguration setCircuitBreakerTimeout(long circuitBreakerTimeout) {
      this.circuitBreakerTimeout = circuitBreakerTimeout;
      return this;
   }

   public String getUsername() {
      return username;
   }

   public FederationConnectionConfiguration setUsername(String username) {
      this.username = username;
      return this;
   }

   public String getPassword() {
      return password;
   }

   public FederationConnectionConfiguration setPassword(String password) {
      this.password = password;
      return this;
   }

   public int getPriorityAdjustment() {
      return priorityAdjustment;
   }

   public FederationConnectionConfiguration setPriorityAdjustment(int priorityAdjustment) {
      this.priorityAdjustment = priorityAdjustment;
      return this;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof FederationConnectionConfiguration)) return false;
      FederationConnectionConfiguration that = (FederationConnectionConfiguration) o;
      return isHA == that.isHA &&
            circuitBreakerTimeout == that.circuitBreakerTimeout &&
            Objects.equals(discoveryGroupName, that.discoveryGroupName) &&
            Objects.equals(staticConnectors, that.staticConnectors) &&
            Objects.equals(priorityAdjustment, that.priorityAdjustment) &&
            Objects.equals(username, that.username) &&
            Objects.equals(password, that.password);
   }

   @Override
   public int hashCode() {
      return Objects.hash(isHA, discoveryGroupName, staticConnectors, priorityAdjustment, circuitBreakerTimeout, username, password);
   }
}
