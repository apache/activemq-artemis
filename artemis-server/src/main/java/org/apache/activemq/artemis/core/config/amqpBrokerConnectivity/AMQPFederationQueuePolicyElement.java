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
package org.apache.activemq.artemis.core.config.amqpBrokerConnectivity;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.activemq.artemis.core.config.TransformerConfiguration;

public final class AMQPFederationQueuePolicyElement implements Serializable {

   private static final long serialVersionUID = 7519912064917015520L;

   private final Set<QueueMatch> includes = new HashSet<>();
   private final Set<QueueMatch> excludes = new HashSet<>();
   private final Map<String, Object> properties = new HashMap<>();

   private String name;
   private boolean includeFederated;
   private Integer priorityAdjustment;
   private TransformerConfiguration transformerConfig;

   public String getName() {
      return name;
   }

   public AMQPFederationQueuePolicyElement setName(String name) {
      this.name = name;
      return this;
   }

   public Set<QueueMatch> getIncludes() {
      return includes;
   }

   public AMQPFederationQueuePolicyElement addToIncludes(String addressMatch, String queueMatch) {
      includes.add(new QueueMatch().setAddressMatch(addressMatch).setQueueMatch(queueMatch));
      return this;
   }

   public AMQPFederationQueuePolicyElement addInclude(QueueMatch match) {
      includes.add(match);
      return this;
   }

   public AMQPFederationQueuePolicyElement setIncludes(Set<QueueMatch> includes) {
      this.includes.clear();
      if (includes != null) {
         this.includes.addAll(includes);
      }

      return this;
   }

   public Set<QueueMatch> getExcludes() {
      return excludes;
   }

   public AMQPFederationQueuePolicyElement addExclude(QueueMatch match) {
      excludes.add(match);
      return this;
   }

   public AMQPFederationQueuePolicyElement addToExcludes(String addressMatch, String queueMatch) {
      excludes.add(new QueueMatch().setAddressMatch(addressMatch).setQueueMatch(queueMatch));
      return this;
   }

   public AMQPFederationQueuePolicyElement setExcludes(Set<QueueMatch> excludes) {
      this.excludes.clear();
      if (excludes != null) {
         this.excludes.addAll(excludes);
      }

      return this;
   }

   public AMQPFederationQueuePolicyElement addProperty(String key, String value) {
      properties.put(key, value);
      return this;
   }

   public AMQPFederationQueuePolicyElement addProperty(String key, Number value) {
      properties.put(key, value);
      return this;
   }

   public Map<String, Object> getProperties() {
      return properties;
   }

   public AMQPFederationQueuePolicyElement setProperties(Map<String, Object> properties) {
      this.properties.clear();
      if (properties != null) {
         this.properties.putAll(properties);
      }

      return this;
   }

   public boolean isIncludeFederated() {
      return includeFederated;
   }

   public AMQPFederationQueuePolicyElement setIncludeFederated(boolean includeFederated) {
      this.includeFederated = includeFederated;
      return this;
   }

   public Integer getPriorityAdjustment() {
      return priorityAdjustment;
   }

   public AMQPFederationQueuePolicyElement setPriorityAdjustment(Integer priorityAdjustment) {
      this.priorityAdjustment = priorityAdjustment;
      return this;
   }

   public AMQPFederationQueuePolicyElement setTransformerConfiguration(TransformerConfiguration transformerConfig) {
      this.transformerConfig = transformerConfig;
      return this;
   }

   public TransformerConfiguration getTransformerConfiguration() {
      return transformerConfig;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (!(o instanceof AMQPFederationQueuePolicyElement)) {
         return false;
      }

      final AMQPFederationQueuePolicyElement that = (AMQPFederationQueuePolicyElement) o;

      return includeFederated == that.includeFederated &&
             Objects.equals(name, that.name) &&
             Objects.equals(includes, that.includes) &&
             Objects.equals(excludes, that.excludes) &&
             Objects.equals(priorityAdjustment, that.priorityAdjustment);
   }

   @Override
   public int hashCode() {
      return Objects.hash(name, includeFederated, includes, excludes, priorityAdjustment);
   }

   // We are required to implement a named match type so that we can perform this configuration
   // from the broker properties mechanism where there is no means of customizing the property
   // set to parse address and queue names from some string encoded value. This could be simplified
   // at some point if another configuration mechanism is created. The name value is not used
   // internally in the AMQP federation implementation.

   public static class QueueMatch implements Serializable {

      private static final long serialVersionUID = -1641189627591828008L;

      private String name;
      private String addressMatch;
      private String queueMatch;

      public String getName() {
         return name;
      }

      public QueueMatch setName(String name) {
         this.name = name;
         return this;
      }

      public String getAddressMatch() {
         return addressMatch;
      }

      public QueueMatch setAddressMatch(String addressMatch) {
         this.addressMatch = addressMatch;
         return this;
      }

      public String getQueueMatch() {
         return queueMatch;
      }

      public QueueMatch setQueueMatch(String queueMatch) {
         this.queueMatch = queueMatch;
         return this;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) {
            return true;
         }

         if (!(o instanceof QueueMatch)) {
            return false;
         }

         final QueueMatch matcher = (QueueMatch) o;

         return Objects.equals(queueMatch, matcher.queueMatch) &&
                Objects.equals(addressMatch, matcher.addressMatch);
      }

      @Override
      public int hashCode() {
         return Objects.hash(queueMatch, addressMatch);
      }
   }
}
