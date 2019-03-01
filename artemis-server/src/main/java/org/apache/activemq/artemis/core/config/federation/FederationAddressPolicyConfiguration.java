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
package org.apache.activemq.artemis.core.config.federation;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class FederationAddressPolicyConfiguration implements FederationPolicy<FederationAddressPolicyConfiguration>, Serializable {

   private String name;
   private Set<Matcher> includes = new HashSet<>();
   private Set<Matcher> excludes = new HashSet<>();
   private Boolean autoDelete;
   private Long autoDeleteDelay;
   private Long autoDeleteMessageCount;
   private int maxHops;
   private String transformerRef;

   @Override
   public String getName() {
      return name;
   }

   @Override
   public FederationAddressPolicyConfiguration setName(String name) {
      this.name = name;
      return this;
   }

   public Set<Matcher> getIncludes() {
      return includes;
   }

   public Set<Matcher> getExcludes() {
      return excludes;
   }

   public FederationAddressPolicyConfiguration addInclude(Matcher include) {
      includes.add(include);
      return this;
   }

   public FederationAddressPolicyConfiguration addExclude(Matcher exclude) {
      excludes.add(exclude);
      return this;
   }

   public int getMaxHops() {
      return maxHops;
   }

   public FederationAddressPolicyConfiguration setMaxHops(int maxHops) {
      this.maxHops = maxHops;
      return this;
   }

   public Long getAutoDeleteMessageCount() {
      return autoDeleteMessageCount;
   }

   public FederationAddressPolicyConfiguration setAutoDeleteMessageCount(Long autoDeleteMessageCount) {
      this.autoDeleteMessageCount = autoDeleteMessageCount;
      return this;
   }

   public Long getAutoDeleteDelay() {
      return autoDeleteDelay;
   }

   public FederationAddressPolicyConfiguration setAutoDeleteDelay(Long autoDeleteDelay) {
      this.autoDeleteDelay = autoDeleteDelay;
      return this;
   }

   public Boolean getAutoDelete() {
      return autoDelete;
   }

   public FederationAddressPolicyConfiguration setAutoDelete(Boolean autoDelete) {
      this.autoDelete = autoDelete;
      return this;
   }

   public String getTransformerRef() {
      return transformerRef;
   }

   public FederationAddressPolicyConfiguration setTransformerRef(String transformerRef) {
      this.transformerRef = transformerRef;
      return this;
   }

   public static class Matcher implements Serializable {

      private String addressMatch;

      public String getAddressMatch() {
         return addressMatch;
      }

      public Matcher setAddressMatch(String addressMatch) {
         this.addressMatch = addressMatch;
         return this;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (!(o instanceof Matcher)) return false;
         Matcher matcher = (Matcher) o;
         return Objects.equals(addressMatch, matcher.addressMatch);
      }

      @Override
      public int hashCode() {
         return Objects.hash(addressMatch);
      }
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof FederationAddressPolicyConfiguration)) return false;
      FederationAddressPolicyConfiguration that = (FederationAddressPolicyConfiguration) o;
      return maxHops == that.maxHops &&
            Objects.equals(name, that.name) &&
            Objects.equals(includes, that.includes) &&
            Objects.equals(excludes, that.excludes) &&
            Objects.equals(autoDelete, that.autoDelete) &&
            Objects.equals(autoDeleteDelay, that.autoDeleteDelay) &&
            Objects.equals(autoDeleteMessageCount, that.autoDeleteMessageCount) &&
            Objects.equals(transformerRef, that.transformerRef);
   }

   @Override
   public int hashCode() {
      return Objects.hash(name, includes, excludes, autoDelete, autoDeleteDelay, autoDeleteMessageCount, maxHops, transformerRef);
   }
}
