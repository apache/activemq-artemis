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
package org.apache.activemq.artemis.api.core.management;

import javax.json.JsonObject;

import org.apache.activemq.artemis.api.core.JsonUtil;

// XXX no javadocs
public final class AddressSettingsInfo {

   private final String addressFullMessagePolicy;

   private final long maxSizeBytes;

   private final int pageSizeBytes;

   private int pageCacheMaxSize;

   private final int maxDeliveryAttempts;

   private final double redeliveryMultiplier;

   private final long maxRedeliveryDelay;

   private final long redeliveryDelay;

   private final String deadLetterAddress;

   private final String expiryAddress;

   private final boolean lastValueQueue;

   private final long redistributionDelay;

   private final boolean sendToDLAOnNoRoute;

   private final long slowConsumerThreshold;

   private final long slowConsumerCheckPeriod;

   private final String slowConsumerPolicy;

   private final boolean autoCreateJmsQueues;

   private final boolean autoDeleteJmsQueues;

   private final boolean autoCreateJmsTopics;

   private final boolean autoDeleteJmsTopics;

   // Static --------------------------------------------------------

   public static AddressSettingsInfo from(final String jsonString) {
      JsonObject object = JsonUtil.readJsonObject(jsonString);
      return new AddressSettingsInfo(object.getString("addressFullMessagePolicy"), object.getJsonNumber("maxSizeBytes").longValue(), object.getInt("pageSizeBytes"), object.getInt("pageCacheMaxSize"), object.getInt("maxDeliveryAttempts"), object.getJsonNumber("redeliveryDelay").longValue(), object.getJsonNumber("redeliveryMultiplier").doubleValue(), object.getJsonNumber("maxRedeliveryDelay").longValue(), object.getString("DLA"), object.getString("expiryAddress"), object.getBoolean("lastValueQueue"), object.getJsonNumber("redistributionDelay").longValue(), object.getBoolean("sendToDLAOnNoRoute"), object.getJsonNumber("slowConsumerThreshold").longValue(), object.getJsonNumber("slowConsumerCheckPeriod").longValue(), object.getString("slowConsumerPolicy"), object.getBoolean("autoCreateJmsQueues"), object.getBoolean("autoCreateJmsTopics"), object.getBoolean("autoDeleteJmsQueues"), object.getBoolean("autoDeleteJmsTopics"));
   }

   // Constructors --------------------------------------------------

   public AddressSettingsInfo(String addressFullMessagePolicy,
                              long maxSizeBytes,
                              int pageSizeBytes,
                              int pageCacheMaxSize,
                              int maxDeliveryAttempts,
                              long redeliveryDelay,
                              double redeliveryMultiplier,
                              long maxRedeliveryDelay,
                              String deadLetterAddress,
                              String expiryAddress,
                              boolean lastValueQueue,
                              long redistributionDelay,
                              boolean sendToDLAOnNoRoute,
                              long slowConsumerThreshold,
                              long slowConsumerCheckPeriod,
                              String slowConsumerPolicy,
                              boolean autoCreateJmsQueues,
                              boolean autoCreateJmsTopics,
                              boolean autoDeleteJmsQueues,
                              boolean autoDeleteJmsTopics) {
      this.addressFullMessagePolicy = addressFullMessagePolicy;
      this.maxSizeBytes = maxSizeBytes;
      this.pageSizeBytes = pageSizeBytes;
      this.pageCacheMaxSize = pageCacheMaxSize;
      this.maxDeliveryAttempts = maxDeliveryAttempts;
      this.redeliveryDelay = redeliveryDelay;
      this.redeliveryMultiplier = redeliveryMultiplier;
      this.maxRedeliveryDelay = maxRedeliveryDelay;
      this.deadLetterAddress = deadLetterAddress;
      this.expiryAddress = expiryAddress;
      this.lastValueQueue = lastValueQueue;
      this.redistributionDelay = redistributionDelay;
      this.sendToDLAOnNoRoute = sendToDLAOnNoRoute;
      this.slowConsumerThreshold = slowConsumerThreshold;
      this.slowConsumerCheckPeriod = slowConsumerCheckPeriod;
      this.slowConsumerPolicy = slowConsumerPolicy;
      this.autoCreateJmsQueues = autoCreateJmsQueues;
      this.autoDeleteJmsQueues = autoDeleteJmsQueues;
      this.autoCreateJmsTopics = autoCreateJmsTopics;
      this.autoDeleteJmsTopics = autoDeleteJmsTopics;
   }

   // Public --------------------------------------------------------

   public int getPageCacheMaxSize() {
      return pageCacheMaxSize;
   }

   public void setPageCacheMaxSize(int pageCacheMaxSize) {
      this.pageCacheMaxSize = pageCacheMaxSize;
   }

   public String getAddressFullMessagePolicy() {
      return addressFullMessagePolicy;
   }

   public long getMaxSizeBytes() {
      return maxSizeBytes;
   }

   public int getPageSizeBytes() {
      return pageSizeBytes;
   }

   public int getMaxDeliveryAttempts() {
      return maxDeliveryAttempts;
   }

   public long getRedeliveryDelay() {
      return redeliveryDelay;
   }

   public String getDeadLetterAddress() {
      return deadLetterAddress;
   }

   public String getExpiryAddress() {
      return expiryAddress;
   }

   public boolean isLastValueQueue() {
      return lastValueQueue;
   }

   public long getRedistributionDelay() {
      return redistributionDelay;
   }

   public boolean isSendToDLAOnNoRoute() {
      return sendToDLAOnNoRoute;
   }

   public double getRedeliveryMultiplier() {
      return redeliveryMultiplier;
   }

   public long getMaxRedeliveryDelay() {
      return maxRedeliveryDelay;
   }

   public long getSlowConsumerThreshold() {
      return slowConsumerThreshold;
   }

   public long getSlowConsumerCheckPeriod() {
      return slowConsumerCheckPeriod;
   }

   public String getSlowConsumerPolicy() {
      return slowConsumerPolicy;
   }

   public boolean isAutoCreateJmsQueues() {
      return autoCreateJmsQueues;
   }

   public boolean isAutoDeleteJmsQueues() {
      return autoDeleteJmsQueues;
   }

   public boolean isAutoCreateJmsTopics() {
      return autoCreateJmsTopics;
   }

   public boolean isAutoDeleteJmsTopics() {
      return autoDeleteJmsTopics;
   }
}

