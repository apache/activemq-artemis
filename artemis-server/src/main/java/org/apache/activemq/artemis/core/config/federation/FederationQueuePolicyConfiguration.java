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

public class FederationQueuePolicyConfiguration implements FederationPolicy<FederationQueuePolicyConfiguration>, Serializable {

   private String name;
   private boolean includeFederated;
   private Set<Matcher> includes = new HashSet<>();
   private Set<Matcher> excludes = new HashSet<>();
   private Integer priorityAdjustment;
   private String transformerRef;

   @Override
   public String getName() {
      return name;
   }

   @Override
   public FederationQueuePolicyConfiguration setName(String name) {
      this.name = name;
      return this;
   }

   public Set<Matcher> getIncludes() {
      return includes;
   }

   public Set<Matcher> getExcludes() {
      return excludes;
   }

   public FederationQueuePolicyConfiguration addInclude(Matcher include) {
      includes.add(include);
      return this;
   }

   public FederationQueuePolicyConfiguration addExclude(Matcher exclude) {
      excludes.add(exclude);
      return this;
   }

   public boolean isIncludeFederated() {
      return includeFederated;
   }

   public FederationQueuePolicyConfiguration setIncludeFederated(boolean includeFederated) {
      this.includeFederated = includeFederated;
      return this;
   }

   public Integer getPriorityAdjustment() {
      return priorityAdjustment;
   }

   public FederationQueuePolicyConfiguration setPriorityAdjustment(Integer priorityAdjustment) {
      this.priorityAdjustment = priorityAdjustment;
      return this;
   }

   public String getTransformerRef() {
      return transformerRef;
   }

   public FederationQueuePolicyConfiguration setTransformerRef(String transformerRef) {
      this.transformerRef = transformerRef;
      return this;
   }

   public static class Matcher implements Serializable {

      private String queueMatch;
      private String addressMatch;

      public String getQueueMatch() {
         return queueMatch;
      }

      public Matcher setQueueMatch(String queueMatch) {
         this.queueMatch = queueMatch;
         return this;
      }

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
         return Objects.equals(queueMatch, matcher.queueMatch) &&
               Objects.equals(addressMatch, matcher.addressMatch);
      }

      @Override
      public int hashCode() {
         return Objects.hash(queueMatch, addressMatch);
      }
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof FederationQueuePolicyConfiguration)) return false;
      FederationQueuePolicyConfiguration that = (FederationQueuePolicyConfiguration) o;
      return includeFederated == that.includeFederated &&
            Objects.equals(name, that.name) &&
            Objects.equals(includes, that.includes) &&
            Objects.equals(excludes, that.excludes) &&
            Objects.equals(priorityAdjustment, that.priorityAdjustment) &&
            Objects.equals(transformerRef, that.transformerRef);
   }

   @Override
   public int hashCode() {
      return Objects.hash(name, includeFederated, includes, excludes, priorityAdjustment, transformerRef);
   }
}
