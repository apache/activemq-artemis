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
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class FederationUpstreamConfiguration implements Serializable {

   private String name;

   private FederationConnectionConfiguration connectionConfiguration = new FederationConnectionConfiguration();
   private Set<String> policyRefs = new HashSet<>();

   public String getName() {
      return name;
   }

   public FederationUpstreamConfiguration setName(String name) {
      this.name = name;
      return this;
   }

   public Set<String> getPolicyRefs() {
      return policyRefs;
   }

   public FederationUpstreamConfiguration addPolicyRef(String name) {
      policyRefs.add(name);
      return this;
   }

   public FederationUpstreamConfiguration addPolicyRefs(Collection<String> names) {
      policyRefs.addAll(names);
      return this;
   }

   public FederationConnectionConfiguration getConnectionConfiguration() {
      return connectionConfiguration;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof FederationUpstreamConfiguration)) return false;
      FederationUpstreamConfiguration that = (FederationUpstreamConfiguration) o;
      return Objects.equals(name, that.name) &&
           Objects.equals(connectionConfiguration, that.connectionConfiguration) &&
           Objects.equals(policyRefs, that.policyRefs);
   }

   @Override
   public int hashCode() {
      return Objects.hash(name, connectionConfiguration, policyRefs);
   }
}
