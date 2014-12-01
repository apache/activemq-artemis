/**
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
package org.apache.activemq.api.core.management;

import org.apache.activemq.utils.json.JSONObject;

/**
 * A AddressSettingsInfo
 *
 * @author jmesnil
 *
 *
 */
// XXX no javadocs
public final class AddressSettingsInfo
{
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

   // Static --------------------------------------------------------

   public static AddressSettingsInfo from(final String jsonString) throws Exception
   {
      JSONObject object = new JSONObject(jsonString);
      return new AddressSettingsInfo(object.getString("addressFullMessagePolicy"),
                                     object.getLong("maxSizeBytes"),
                                     object.getInt("pageSizeBytes"),
                                     object.getInt("pageCacheMaxSize"),
                                     object.getInt("maxDeliveryAttempts"),
                                     object.getLong("redeliveryDelay"),
                                     object.getDouble("redeliveryMultiplier"),
                                     object.getLong("maxRedeliveryDelay"),
                                     object.getString("DLA"),
                                     object.getString("expiryAddress"),
                                     object.getBoolean("lastValueQueue"),
                                     object.getLong("redistributionDelay"),
                                     object.getBoolean("sendToDLAOnNoRoute"),
                                     object.getLong("slowConsumerThreshold"),
                                     object.getLong("slowConsumerCheckPeriod"),
                                     object.getString("slowConsumerPolicy"));
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
                              String slowConsumerPolicy)
   {
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
   }

   // Public --------------------------------------------------------

   public int getPageCacheMaxSize()
   {
      return pageCacheMaxSize;
   }

   public void setPageCacheMaxSize(int pageCacheMaxSize)
   {
      this.pageCacheMaxSize = pageCacheMaxSize;
   }

   public String getAddressFullMessagePolicy()
   {
      return addressFullMessagePolicy;
   }

   public long getMaxSizeBytes()
   {
      return maxSizeBytes;
   }

   public int getPageSizeBytes()
   {
      return pageSizeBytes;
   }

   public int getMaxDeliveryAttempts()
   {
      return maxDeliveryAttempts;
   }

   public long getRedeliveryDelay()
   {
      return redeliveryDelay;
   }

   public String getDeadLetterAddress()
   {
      return deadLetterAddress;
   }

   public String getExpiryAddress()
   {
      return expiryAddress;
   }

   public boolean isLastValueQueue()
   {
      return lastValueQueue;
   }

   public long getRedistributionDelay()
   {
      return redistributionDelay;
   }

   public boolean isSendToDLAOnNoRoute()
   {
      return sendToDLAOnNoRoute;
   }

   public double getRedeliveryMultiplier()
   {
      return redeliveryMultiplier;
   }

   public long getMaxRedeliveryDelay()
   {
      return maxRedeliveryDelay;
   }

   public long getSlowConsumerThreshold()
   {
      return slowConsumerThreshold;
   }

   public long getSlowConsumerCheckPeriod()
   {
      return slowConsumerCheckPeriod;
   }

   public String getSlowConsumerPolicy()
   {
      return slowConsumerPolicy;
   }
}

