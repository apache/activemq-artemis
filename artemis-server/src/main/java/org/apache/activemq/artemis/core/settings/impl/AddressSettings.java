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
package org.apache.activemq.artemis.core.settings.impl;

import java.io.Serializable;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.journal.EncodingSupport;
import org.apache.activemq.artemis.core.settings.Mergeable;
import org.apache.activemq.artemis.utils.BufferHelper;

/**
 * Configuration settings that are applied on the address level
 */
public class AddressSettings implements Mergeable<AddressSettings>, Serializable, EncodingSupport {

   private static final long serialVersionUID = 1607502280582336366L;

   /**
    * defaults used if null, this allows merging
    */
   public static final long DEFAULT_MAX_SIZE_BYTES = -1;

   public static final AddressFullMessagePolicy DEFAULT_ADDRESS_FULL_MESSAGE_POLICY = AddressFullMessagePolicy.PAGE;

   public static final long DEFAULT_PAGE_SIZE = 10 * 1024 * 1024;

   public static final int DEFAULT_MAX_DELIVERY_ATTEMPTS = 10;

   public static final int DEFAULT_PAGE_MAX_CACHE = 5;

   public static final int DEFAULT_MESSAGE_COUNTER_HISTORY_DAY_LIMIT = 0;

   public static final long DEFAULT_REDELIVER_DELAY = 0L;

   public static final double DEFAULT_REDELIVER_MULTIPLIER = 1.0;

   public static final boolean DEFAULT_LAST_VALUE_QUEUE = false;

   @Deprecated
   public static final boolean DEFAULT_AUTO_CREATE_JMS_QUEUES = true;

   @Deprecated
   public static final boolean DEFAULT_AUTO_DELETE_JMS_QUEUES = true;

   @Deprecated
   public static final boolean DEFAULT_AUTO_CREATE_TOPICS = true;

   @Deprecated
   public static final boolean DEFAULT_AUTO_DELETE_TOPICS = true;

   public static final boolean DEFAULT_AUTO_CREATE_QUEUES = true;

   public static final boolean DEFAULT_AUTO_DELETE_QUEUES = true;

   public static final boolean DEFAULT_AUTO_CREATE_ADDRESSES = true;

   public static final boolean DEFAULT_AUTO_DELETE_ADDRESSES = true;

   public static final long DEFAULT_REDISTRIBUTION_DELAY = -1;

   public static final long DEFAULT_EXPIRY_DELAY = -1;

   public static final boolean DEFAULT_SEND_TO_DLA_ON_NO_ROUTE = false;

   public static final long DEFAULT_SLOW_CONSUMER_THRESHOLD = -1;

   public static final long DEFAULT_SLOW_CONSUMER_CHECK_PERIOD = 5;

   public static final int MANAGEMENT_BROWSE_PAGE_SIZE = 200;

   public static final SlowConsumerPolicy DEFAULT_SLOW_CONSUMER_POLICY = SlowConsumerPolicy.NOTIFY;

   public static final int DEFAULT_QUEUE_PREFETCH = 1000;

   // Default address drop threshold, applied to address settings with BLOCK policy.  -1 means no threshold enabled.
   public static final long DEFAULT_ADDRESS_REJECT_THRESHOLD = -1;

   private AddressFullMessagePolicy addressFullMessagePolicy = null;

   private Long maxSizeBytes = null;

   private Long pageSizeBytes = null;

   private Integer pageMaxCache = null;

   private Boolean dropMessagesWhenFull = null;

   private Integer maxDeliveryAttempts = null;

   private Integer messageCounterHistoryDayLimit = null;

   private Long redeliveryDelay = null;

   private Double redeliveryMultiplier = null;

   private Long maxRedeliveryDelay = null;

   private SimpleString deadLetterAddress = null;

   private SimpleString expiryAddress = null;

   private Long expiryDelay = AddressSettings.DEFAULT_EXPIRY_DELAY;

   private Boolean lastValueQueue = null;

   private Long redistributionDelay = null;

   private Boolean sendToDLAOnNoRoute = null;

   private Long slowConsumerThreshold = null;

   private Long slowConsumerCheckPeriod = null;

   private SlowConsumerPolicy slowConsumerPolicy = null;

   private Boolean autoCreateJmsQueues = null;

   private Boolean autoDeleteJmsQueues = null;

   private Boolean autoCreateJmsTopics = null;

   private Boolean autoDeleteJmsTopics = null;

   private Boolean autoCreateQueues = null;

   private Boolean autoDeleteQueues = null;

   private Boolean autoCreateAddresses = null;

   private Boolean autoDeleteAddresses = null;

   private Integer managementBrowsePageSize = AddressSettings.MANAGEMENT_BROWSE_PAGE_SIZE;

   private Long maxSizeBytesRejectThreshold = null;

   //from amq5
   //make it transient
   private transient Integer queuePrefetch = null;

   public AddressSettings(AddressSettings other) {
      this.addressFullMessagePolicy = other.addressFullMessagePolicy;
      this.maxSizeBytes = other.maxSizeBytes;
      this.pageSizeBytes = other.pageSizeBytes;
      this.pageMaxCache = other.pageMaxCache;
      this.dropMessagesWhenFull = other.dropMessagesWhenFull;
      this.maxDeliveryAttempts = other.maxDeliveryAttempts;
      this.messageCounterHistoryDayLimit = other.messageCounterHistoryDayLimit;
      this.redeliveryDelay = other.redeliveryDelay;
      this.redeliveryMultiplier = other.redeliveryMultiplier;
      this.maxRedeliveryDelay = other.maxRedeliveryDelay;
      this.deadLetterAddress = other.deadLetterAddress;
      this.expiryAddress = other.expiryAddress;
      this.expiryDelay = other.expiryDelay;
      this.lastValueQueue = other.lastValueQueue;
      this.redistributionDelay = other.redistributionDelay;
      this.sendToDLAOnNoRoute = other.sendToDLAOnNoRoute;
      this.slowConsumerThreshold = other.slowConsumerThreshold;
      this.slowConsumerCheckPeriod = other.slowConsumerCheckPeriod;
      this.slowConsumerPolicy = other.slowConsumerPolicy;
      this.autoCreateJmsQueues = other.autoCreateJmsQueues;
      this.autoDeleteJmsQueues = other.autoDeleteJmsQueues;
      this.autoCreateJmsTopics = other.autoCreateJmsTopics;
      this.autoDeleteJmsTopics = other.autoDeleteJmsTopics;
      this.autoCreateQueues = other.autoCreateQueues;
      this.autoDeleteQueues = other.autoDeleteQueues;
      this.autoCreateAddresses = other.autoCreateAddresses;
      this.autoDeleteAddresses = other.autoDeleteAddresses;
      this.managementBrowsePageSize = other.managementBrowsePageSize;
      this.queuePrefetch = other.queuePrefetch;
      this.maxSizeBytesRejectThreshold = other.maxSizeBytesRejectThreshold;
   }

   public AddressSettings() {
   }

   @Deprecated
   public boolean isAutoCreateJmsQueues() {
      return autoCreateJmsQueues != null ? autoCreateJmsQueues : AddressSettings.DEFAULT_AUTO_CREATE_JMS_QUEUES;
   }

   @Deprecated
   public AddressSettings setAutoCreateJmsQueues(final boolean autoCreateJmsQueues) {
      this.autoCreateJmsQueues = autoCreateJmsQueues;
      return this;
   }

   @Deprecated
   public boolean isAutoDeleteJmsQueues() {
      return autoDeleteJmsQueues != null ? autoDeleteJmsQueues : AddressSettings.DEFAULT_AUTO_DELETE_JMS_QUEUES;
   }

   @Deprecated
   public AddressSettings setAutoDeleteJmsQueues(final boolean autoDeleteJmsQueues) {
      this.autoDeleteJmsQueues = autoDeleteJmsQueues;
      return this;
   }

   @Deprecated
   public boolean isAutoCreateJmsTopics() {
      return autoCreateJmsTopics != null ? autoCreateJmsTopics : AddressSettings.DEFAULT_AUTO_CREATE_TOPICS;
   }

   @Deprecated
   public AddressSettings setAutoCreateJmsTopics(final boolean autoCreateJmsTopics) {
      this.autoCreateJmsTopics = autoCreateJmsTopics;
      return this;
   }

   @Deprecated
   public boolean isAutoDeleteJmsTopics() {
      return autoDeleteJmsTopics != null ? autoDeleteJmsTopics : AddressSettings.DEFAULT_AUTO_DELETE_TOPICS;
   }

   @Deprecated
   public AddressSettings setAutoDeleteJmsTopics(final boolean autoDeleteJmsTopics) {
      this.autoDeleteJmsTopics = autoDeleteJmsTopics;
      return this;
   }

   public boolean isAutoCreateQueues() {
      return autoCreateQueues != null ? autoCreateQueues : AddressSettings.DEFAULT_AUTO_CREATE_QUEUES;
   }

   public AddressSettings setAutoCreateQueues(Boolean autoCreateQueues) {
      this.autoCreateQueues = autoCreateQueues;
      return this;
   }

   public boolean isAutoDeleteQueues() {
      return autoDeleteQueues != null ? autoDeleteQueues : AddressSettings.DEFAULT_AUTO_DELETE_QUEUES;
   }

   public AddressSettings setAutoDeleteQueues(Boolean autoDeleteQueues) {
      this.autoDeleteQueues = autoDeleteQueues;
      return this;
   }

   public boolean isAutoCreateAddresses() {
      return autoCreateAddresses != null ? autoCreateAddresses : AddressSettings.DEFAULT_AUTO_CREATE_ADDRESSES;
   }

   public AddressSettings setAutoCreateAddresses(Boolean autoCreateAddresses) {
      this.autoCreateAddresses = autoCreateAddresses;
      return this;
   }

   public boolean isAutoDeleteAddresses() {
      return autoDeleteAddresses != null ? autoDeleteAddresses : AddressSettings.DEFAULT_AUTO_DELETE_ADDRESSES;
   }

   public AddressSettings setAutoDeleteAddresses(Boolean autoDeleteAddresses) {
      this.autoDeleteAddresses = autoDeleteAddresses;
      return this;
   }

   public boolean isLastValueQueue() {
      return lastValueQueue != null ? lastValueQueue : AddressSettings.DEFAULT_LAST_VALUE_QUEUE;
   }

   public AddressSettings setLastValueQueue(final boolean lastValueQueue) {
      this.lastValueQueue = lastValueQueue;
      return this;
   }

   public AddressFullMessagePolicy getAddressFullMessagePolicy() {
      return addressFullMessagePolicy != null ? addressFullMessagePolicy : AddressSettings.DEFAULT_ADDRESS_FULL_MESSAGE_POLICY;
   }

   public AddressSettings setAddressFullMessagePolicy(final AddressFullMessagePolicy addressFullMessagePolicy) {
      this.addressFullMessagePolicy = addressFullMessagePolicy;
      return this;
   }

   public long getPageSizeBytes() {
      return pageSizeBytes != null ? pageSizeBytes : AddressSettings.DEFAULT_PAGE_SIZE;
   }

   public AddressSettings setPageSizeBytes(final long pageSize) {
      pageSizeBytes = pageSize;
      return this;
   }

   public int getPageCacheMaxSize() {
      return pageMaxCache != null ? pageMaxCache : AddressSettings.DEFAULT_PAGE_MAX_CACHE;
   }

   public AddressSettings setPageCacheMaxSize(final int pageMaxCache) {
      this.pageMaxCache = pageMaxCache;
      return this;
   }

   public long getMaxSizeBytes() {
      return maxSizeBytes != null ? maxSizeBytes : AddressSettings.DEFAULT_MAX_SIZE_BYTES;
   }

   public AddressSettings setMaxSizeBytes(final long maxSizeBytes) {
      this.maxSizeBytes = maxSizeBytes;
      return this;
   }

   public int getMaxDeliveryAttempts() {
      return maxDeliveryAttempts != null ? maxDeliveryAttempts : AddressSettings.DEFAULT_MAX_DELIVERY_ATTEMPTS;
   }

   public AddressSettings setMaxDeliveryAttempts(final int maxDeliveryAttempts) {
      this.maxDeliveryAttempts = maxDeliveryAttempts;
      return this;
   }

   public int getMessageCounterHistoryDayLimit() {
      return messageCounterHistoryDayLimit != null ? messageCounterHistoryDayLimit : AddressSettings.DEFAULT_MESSAGE_COUNTER_HISTORY_DAY_LIMIT;
   }

   public AddressSettings setMessageCounterHistoryDayLimit(final int messageCounterHistoryDayLimit) {
      this.messageCounterHistoryDayLimit = messageCounterHistoryDayLimit;
      return this;
   }

   public long getRedeliveryDelay() {
      return redeliveryDelay != null ? redeliveryDelay : AddressSettings.DEFAULT_REDELIVER_DELAY;
   }

   public AddressSettings setRedeliveryDelay(final long redeliveryDelay) {
      this.redeliveryDelay = redeliveryDelay;
      return this;
   }

   public double getRedeliveryMultiplier() {
      return redeliveryMultiplier != null ? redeliveryMultiplier : AddressSettings.DEFAULT_REDELIVER_MULTIPLIER;
   }

   public AddressSettings setRedeliveryMultiplier(final double redeliveryMultiplier) {
      this.redeliveryMultiplier = redeliveryMultiplier;
      return this;
   }

   public long getMaxRedeliveryDelay() {
      // default is redelivery-delay * 10 as specified on the docs and at this JIRA:
      // https://issues.jboss.org/browse/HORNETQ-1263
      return maxRedeliveryDelay != null ? maxRedeliveryDelay : (getRedeliveryDelay() * 10);
   }

   public AddressSettings setMaxRedeliveryDelay(final long maxRedeliveryDelay) {
      this.maxRedeliveryDelay = maxRedeliveryDelay;
      return this;
   }

   public SimpleString getDeadLetterAddress() {
      return deadLetterAddress;
   }

   public AddressSettings setDeadLetterAddress(final SimpleString deadLetterAddress) {
      this.deadLetterAddress = deadLetterAddress;
      return this;
   }

   public SimpleString getExpiryAddress() {
      return expiryAddress;
   }

   public AddressSettings setExpiryAddress(final SimpleString expiryAddress) {
      this.expiryAddress = expiryAddress;
      return this;
   }

   public Long getExpiryDelay() {
      return expiryDelay;
   }

   public AddressSettings setExpiryDelay(final Long expiryDelay) {
      this.expiryDelay = expiryDelay;
      return this;
   }

   public boolean isSendToDLAOnNoRoute() {
      return sendToDLAOnNoRoute != null ? sendToDLAOnNoRoute : AddressSettings.DEFAULT_SEND_TO_DLA_ON_NO_ROUTE;
   }

   public AddressSettings setSendToDLAOnNoRoute(final boolean value) {
      sendToDLAOnNoRoute = value;
      return this;
   }

   public long getRedistributionDelay() {
      return redistributionDelay != null ? redistributionDelay : AddressSettings.DEFAULT_REDISTRIBUTION_DELAY;
   }

   public AddressSettings setRedistributionDelay(final long redistributionDelay) {
      this.redistributionDelay = redistributionDelay;
      return this;
   }

   public long getSlowConsumerThreshold() {
      return slowConsumerThreshold != null ? slowConsumerThreshold : AddressSettings.DEFAULT_SLOW_CONSUMER_THRESHOLD;
   }

   public AddressSettings setSlowConsumerThreshold(final long slowConsumerThreshold) {
      this.slowConsumerThreshold = slowConsumerThreshold;
      return this;
   }

   public long getSlowConsumerCheckPeriod() {
      return slowConsumerCheckPeriod != null ? slowConsumerCheckPeriod : AddressSettings.DEFAULT_SLOW_CONSUMER_CHECK_PERIOD;
   }

   public AddressSettings setSlowConsumerCheckPeriod(final long slowConsumerCheckPeriod) {
      this.slowConsumerCheckPeriod = slowConsumerCheckPeriod;
      return this;
   }

   public SlowConsumerPolicy getSlowConsumerPolicy() {
      return slowConsumerPolicy != null ? slowConsumerPolicy : AddressSettings.DEFAULT_SLOW_CONSUMER_POLICY;
   }

   public AddressSettings setSlowConsumerPolicy(final SlowConsumerPolicy slowConsumerPolicy) {
      this.slowConsumerPolicy = slowConsumerPolicy;
      return this;
   }

   public int getManagementBrowsePageSize() {
      return managementBrowsePageSize != null ? managementBrowsePageSize : AddressSettings.MANAGEMENT_BROWSE_PAGE_SIZE;
   }

   public AddressSettings setManagementBrowsePageSize(int managementBrowsePageSize) {
      this.managementBrowsePageSize = managementBrowsePageSize;
      return this;
   }

   public int getQueuePrefetch() {
      return queuePrefetch != null ? queuePrefetch : AddressSettings.DEFAULT_QUEUE_PREFETCH;
   }

   public AddressSettings setQueuePrefetch(int queuePrefetch) {
      this.queuePrefetch = queuePrefetch;
      return this;
   }

   public long getMaxSizeBytesRejectThreshold() {
      return (maxSizeBytesRejectThreshold == null) ? AddressSettings.DEFAULT_ADDRESS_REJECT_THRESHOLD : maxSizeBytesRejectThreshold;
   }

   public AddressSettings setMaxSizeBytesRejectThreshold(long maxSizeBytesRejectThreshold) {
      this.maxSizeBytesRejectThreshold = maxSizeBytesRejectThreshold;
      return this;
   }

   /**
    * merge 2 objects in to 1
    *
    * @param merged
    */
   @Override
   public void merge(final AddressSettings merged) {
      if (maxDeliveryAttempts == null) {
         maxDeliveryAttempts = merged.maxDeliveryAttempts;
      }
      if (dropMessagesWhenFull == null) {
         dropMessagesWhenFull = merged.dropMessagesWhenFull;
      }
      if (maxSizeBytes == null) {
         maxSizeBytes = merged.maxSizeBytes;
      }
      if (pageMaxCache == null) {
         pageMaxCache = merged.pageMaxCache;
      }
      if (pageSizeBytes == null) {
         pageSizeBytes = merged.getPageSizeBytes();
      }
      if (messageCounterHistoryDayLimit == null) {
         messageCounterHistoryDayLimit = merged.messageCounterHistoryDayLimit;
      }
      if (redeliveryDelay == null) {
         redeliveryDelay = merged.redeliveryDelay;
      }
      if (redeliveryMultiplier == null) {
         redeliveryMultiplier = merged.redeliveryMultiplier;
      }
      if (maxRedeliveryDelay == null) {
         maxRedeliveryDelay = merged.maxRedeliveryDelay;
      }
      if (deadLetterAddress == null) {
         deadLetterAddress = merged.deadLetterAddress;
      }
      if (expiryAddress == null) {
         expiryAddress = merged.expiryAddress;
      }
      if (expiryDelay == null) {
         expiryDelay = merged.expiryDelay;
      }
      if (redistributionDelay == null) {
         redistributionDelay = merged.redistributionDelay;
      }
      if (sendToDLAOnNoRoute == null) {
         sendToDLAOnNoRoute = merged.sendToDLAOnNoRoute;
      }
      if (addressFullMessagePolicy == null) {
         addressFullMessagePolicy = merged.addressFullMessagePolicy;
      }
      if (slowConsumerThreshold == null) {
         slowConsumerThreshold = merged.slowConsumerThreshold;
      }
      if (slowConsumerCheckPeriod == null) {
         slowConsumerCheckPeriod = merged.slowConsumerCheckPeriod;
      }
      if (slowConsumerPolicy == null) {
         slowConsumerPolicy = merged.slowConsumerPolicy;
      }
      if (autoCreateJmsQueues == null) {
         autoCreateJmsQueues = merged.autoCreateJmsQueues;
      }
      if (autoDeleteJmsQueues == null) {
         autoDeleteJmsQueues = merged.autoDeleteJmsQueues;
      }
      if (autoCreateJmsTopics == null) {
         autoCreateJmsTopics = merged.autoCreateJmsTopics;
      }
      if (autoDeleteJmsTopics == null) {
         autoDeleteJmsTopics = merged.autoDeleteJmsTopics;
      }
      if (autoCreateQueues == null) {
         autoCreateQueues = merged.autoCreateQueues;
      }
      if (autoDeleteQueues == null) {
         autoDeleteQueues = merged.autoDeleteQueues;
      }
      if (autoCreateAddresses == null) {
         autoCreateAddresses = merged.autoCreateAddresses;
      }
      if (autoDeleteAddresses == null) {
         autoDeleteAddresses = merged.autoDeleteAddresses;
      }
      if (managementBrowsePageSize == null) {
         managementBrowsePageSize = merged.managementBrowsePageSize;
      }
      if (queuePrefetch == null) {
         queuePrefetch = merged.queuePrefetch;
      }
      if (maxSizeBytesRejectThreshold == null) {
         maxSizeBytesRejectThreshold = merged.maxSizeBytesRejectThreshold;
      }
   }

   @Override
   public void decode(ActiveMQBuffer buffer) {
      SimpleString policyStr = buffer.readNullableSimpleString();

      if (policyStr != null) {
         addressFullMessagePolicy = AddressFullMessagePolicy.valueOf(policyStr.toString());
      } else {
         addressFullMessagePolicy = null;
      }

      maxSizeBytes = BufferHelper.readNullableLong(buffer);

      pageSizeBytes = BufferHelper.readNullableLong(buffer);

      pageMaxCache = BufferHelper.readNullableInteger(buffer);

      dropMessagesWhenFull = BufferHelper.readNullableBoolean(buffer);

      maxDeliveryAttempts = BufferHelper.readNullableInteger(buffer);

      messageCounterHistoryDayLimit = BufferHelper.readNullableInteger(buffer);

      redeliveryDelay = BufferHelper.readNullableLong(buffer);

      redeliveryMultiplier = BufferHelper.readNullableDouble(buffer);

      maxRedeliveryDelay = BufferHelper.readNullableLong(buffer);

      deadLetterAddress = buffer.readNullableSimpleString();

      expiryAddress = buffer.readNullableSimpleString();

      expiryDelay = BufferHelper.readNullableLong(buffer);

      lastValueQueue = BufferHelper.readNullableBoolean(buffer);

      redistributionDelay = BufferHelper.readNullableLong(buffer);

      sendToDLAOnNoRoute = BufferHelper.readNullableBoolean(buffer);

      slowConsumerThreshold = BufferHelper.readNullableLong(buffer);

      slowConsumerCheckPeriod = BufferHelper.readNullableLong(buffer);

      policyStr = buffer.readNullableSimpleString();

      if (policyStr != null) {
         slowConsumerPolicy = SlowConsumerPolicy.valueOf(policyStr.toString());
      } else {
         slowConsumerPolicy = null;
      }

      autoCreateJmsQueues = BufferHelper.readNullableBoolean(buffer);

      autoDeleteJmsQueues = BufferHelper.readNullableBoolean(buffer);

      autoCreateJmsTopics = BufferHelper.readNullableBoolean(buffer);

      autoDeleteJmsTopics = BufferHelper.readNullableBoolean(buffer);

      autoCreateQueues = BufferHelper.readNullableBoolean(buffer);

      autoDeleteQueues = BufferHelper.readNullableBoolean(buffer);

      autoCreateAddresses = BufferHelper.readNullableBoolean(buffer);

      autoDeleteAddresses = BufferHelper.readNullableBoolean(buffer);

      managementBrowsePageSize = BufferHelper.readNullableInteger(buffer);

      maxSizeBytesRejectThreshold = BufferHelper.readNullableLong(buffer);
   }

   @Override
   public int getEncodeSize() {

      return BufferHelper.sizeOfNullableSimpleString(addressFullMessagePolicy != null ? addressFullMessagePolicy.toString() : null) +
         BufferHelper.sizeOfNullableLong(maxSizeBytes) +
         BufferHelper.sizeOfNullableLong(pageSizeBytes) +
         BufferHelper.sizeOfNullableInteger(pageMaxCache) +
         BufferHelper.sizeOfNullableBoolean(dropMessagesWhenFull) +
         BufferHelper.sizeOfNullableInteger(maxDeliveryAttempts) +
         BufferHelper.sizeOfNullableInteger(messageCounterHistoryDayLimit) +
         BufferHelper.sizeOfNullableLong(redeliveryDelay) +
         BufferHelper.sizeOfNullableDouble(redeliveryMultiplier) +
         BufferHelper.sizeOfNullableLong(maxRedeliveryDelay) +
         SimpleString.sizeofNullableString(deadLetterAddress) +
         SimpleString.sizeofNullableString(expiryAddress) +
         BufferHelper.sizeOfNullableLong(expiryDelay) +
         BufferHelper.sizeOfNullableBoolean(lastValueQueue) +
         BufferHelper.sizeOfNullableLong(redistributionDelay) +
         BufferHelper.sizeOfNullableBoolean(sendToDLAOnNoRoute) +
         BufferHelper.sizeOfNullableLong(slowConsumerCheckPeriod) +
         BufferHelper.sizeOfNullableLong(slowConsumerThreshold) +
         BufferHelper.sizeOfNullableSimpleString(slowConsumerPolicy != null ? slowConsumerPolicy.toString() : null) +
         BufferHelper.sizeOfNullableBoolean(autoCreateJmsQueues) +
         BufferHelper.sizeOfNullableBoolean(autoDeleteJmsQueues) +
         BufferHelper.sizeOfNullableBoolean(autoCreateJmsTopics) +
         BufferHelper.sizeOfNullableBoolean(autoDeleteJmsTopics) +
         BufferHelper.sizeOfNullableBoolean(autoCreateQueues) +
         BufferHelper.sizeOfNullableBoolean(autoDeleteQueues) +
         BufferHelper.sizeOfNullableBoolean(autoCreateAddresses) +
         BufferHelper.sizeOfNullableBoolean(autoDeleteAddresses) +
         BufferHelper.sizeOfNullableInteger(managementBrowsePageSize) +
         BufferHelper.sizeOfNullableLong(maxSizeBytesRejectThreshold);
   }

   @Override
   public void encode(ActiveMQBuffer buffer) {
      buffer.writeNullableSimpleString(addressFullMessagePolicy != null ? new SimpleString(addressFullMessagePolicy.toString()) : null);

      BufferHelper.writeNullableLong(buffer, maxSizeBytes);

      BufferHelper.writeNullableLong(buffer, pageSizeBytes);

      BufferHelper.writeNullableInteger(buffer, pageMaxCache);

      BufferHelper.writeNullableBoolean(buffer, dropMessagesWhenFull);

      BufferHelper.writeNullableInteger(buffer, maxDeliveryAttempts);

      BufferHelper.writeNullableInteger(buffer, messageCounterHistoryDayLimit);

      BufferHelper.writeNullableLong(buffer, redeliveryDelay);

      BufferHelper.writeNullableDouble(buffer, redeliveryMultiplier);

      BufferHelper.writeNullableLong(buffer, maxRedeliveryDelay);

      buffer.writeNullableSimpleString(deadLetterAddress);

      buffer.writeNullableSimpleString(expiryAddress);

      BufferHelper.writeNullableLong(buffer, expiryDelay);

      BufferHelper.writeNullableBoolean(buffer, lastValueQueue);

      BufferHelper.writeNullableLong(buffer, redistributionDelay);

      BufferHelper.writeNullableBoolean(buffer, sendToDLAOnNoRoute);

      BufferHelper.writeNullableLong(buffer, slowConsumerThreshold);

      BufferHelper.writeNullableLong(buffer, slowConsumerCheckPeriod);

      buffer.writeNullableSimpleString(slowConsumerPolicy != null ? new SimpleString(slowConsumerPolicy.toString()) : null);

      BufferHelper.writeNullableBoolean(buffer, autoCreateJmsQueues);

      BufferHelper.writeNullableBoolean(buffer, autoDeleteJmsQueues);

      BufferHelper.writeNullableBoolean(buffer, autoCreateJmsTopics);

      BufferHelper.writeNullableBoolean(buffer, autoDeleteJmsTopics);

      BufferHelper.writeNullableBoolean(buffer, autoCreateQueues);

      BufferHelper.writeNullableBoolean(buffer, autoDeleteQueues);

      BufferHelper.writeNullableBoolean(buffer, autoCreateAddresses);

      BufferHelper.writeNullableBoolean(buffer, autoDeleteAddresses);

      BufferHelper.writeNullableInteger(buffer, managementBrowsePageSize);

      BufferHelper.writeNullableLong(buffer, maxSizeBytesRejectThreshold);
   }

   /* (non-Javadoc)
       * @see java.lang.Object#hashCode()
       */
   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((addressFullMessagePolicy == null) ? 0 : addressFullMessagePolicy.hashCode());
      result = prime * result + ((deadLetterAddress == null) ? 0 : deadLetterAddress.hashCode());
      result = prime * result + ((dropMessagesWhenFull == null) ? 0 : dropMessagesWhenFull.hashCode());
      result = prime * result + ((expiryAddress == null) ? 0 : expiryAddress.hashCode());
      result = prime * result + ((expiryDelay == null) ? 0 : expiryDelay.hashCode());
      result = prime * result + ((lastValueQueue == null) ? 0 : lastValueQueue.hashCode());
      result = prime * result + ((maxDeliveryAttempts == null) ? 0 : maxDeliveryAttempts.hashCode());
      result = prime * result + ((maxSizeBytes == null) ? 0 : maxSizeBytes.hashCode());
      result = prime * result + ((messageCounterHistoryDayLimit == null) ? 0 : messageCounterHistoryDayLimit.hashCode());
      result = prime * result + ((pageSizeBytes == null) ? 0 : pageSizeBytes.hashCode());
      result = prime * result + ((pageMaxCache == null) ? 0 : pageMaxCache.hashCode());
      result = prime * result + ((redeliveryDelay == null) ? 0 : redeliveryDelay.hashCode());
      result = prime * result + ((redeliveryMultiplier == null) ? 0 : redeliveryMultiplier.hashCode());
      result = prime * result + ((maxRedeliveryDelay == null) ? 0 : maxRedeliveryDelay.hashCode());
      result = prime * result + ((redistributionDelay == null) ? 0 : redistributionDelay.hashCode());
      result = prime * result + ((sendToDLAOnNoRoute == null) ? 0 : sendToDLAOnNoRoute.hashCode());
      result = prime * result + ((slowConsumerThreshold == null) ? 0 : slowConsumerThreshold.hashCode());
      result = prime * result + ((slowConsumerCheckPeriod == null) ? 0 : slowConsumerCheckPeriod.hashCode());
      result = prime * result + ((slowConsumerPolicy == null) ? 0 : slowConsumerPolicy.hashCode());
      result = prime * result + ((autoCreateJmsQueues == null) ? 0 : autoCreateJmsQueues.hashCode());
      result = prime * result + ((autoDeleteJmsQueues == null) ? 0 : autoDeleteJmsQueues.hashCode());
      result = prime * result + ((autoCreateJmsTopics == null) ? 0 : autoCreateJmsTopics.hashCode());
      result = prime * result + ((autoDeleteJmsTopics == null) ? 0 : autoDeleteJmsTopics.hashCode());
      result = prime * result + ((autoCreateQueues == null) ? 0 : autoCreateQueues.hashCode());
      result = prime * result + ((autoDeleteQueues == null) ? 0 : autoDeleteQueues.hashCode());
      result = prime * result + ((autoCreateAddresses == null) ? 0 : autoCreateAddresses.hashCode());
      result = prime * result + ((autoDeleteAddresses == null) ? 0 : autoDeleteAddresses.hashCode());
      result = prime * result + ((managementBrowsePageSize == null) ? 0 : managementBrowsePageSize.hashCode());
      result = prime * result + ((queuePrefetch == null) ? 0 : queuePrefetch.hashCode());
      result = prime * result + ((maxSizeBytesRejectThreshold == null) ? 0 : queuePrefetch.hashCode());
      return result;
   }

   /* (non-Javadoc)
    * @see java.lang.Object#equals(java.lang.Object)
    */
   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      AddressSettings other = (AddressSettings) obj;
      if (addressFullMessagePolicy == null) {
         if (other.addressFullMessagePolicy != null)
            return false;
      } else if (!addressFullMessagePolicy.equals(other.addressFullMessagePolicy))
         return false;
      if (deadLetterAddress == null) {
         if (other.deadLetterAddress != null)
            return false;
      } else if (!deadLetterAddress.equals(other.deadLetterAddress))
         return false;
      if (dropMessagesWhenFull == null) {
         if (other.dropMessagesWhenFull != null)
            return false;
      } else if (!dropMessagesWhenFull.equals(other.dropMessagesWhenFull))
         return false;
      if (expiryAddress == null) {
         if (other.expiryAddress != null)
            return false;
      } else if (!expiryAddress.equals(other.expiryAddress))
         return false;
      if (expiryDelay == null) {
         if (other.expiryDelay != null)
            return false;
      } else if (!expiryDelay.equals(other.expiryDelay))
         return false;
      if (lastValueQueue == null) {
         if (other.lastValueQueue != null)
            return false;
      } else if (!lastValueQueue.equals(other.lastValueQueue))
         return false;
      if (maxDeliveryAttempts == null) {
         if (other.maxDeliveryAttempts != null)
            return false;
      } else if (!maxDeliveryAttempts.equals(other.maxDeliveryAttempts))
         return false;
      if (maxSizeBytes == null) {
         if (other.maxSizeBytes != null)
            return false;
      } else if (!maxSizeBytes.equals(other.maxSizeBytes))
         return false;
      if (messageCounterHistoryDayLimit == null) {
         if (other.messageCounterHistoryDayLimit != null)
            return false;
      } else if (!messageCounterHistoryDayLimit.equals(other.messageCounterHistoryDayLimit))
         return false;
      if (pageSizeBytes == null) {
         if (other.pageSizeBytes != null)
            return false;
      } else if (!pageSizeBytes.equals(other.pageSizeBytes))
         return false;
      if (pageMaxCache == null) {
         if (other.pageMaxCache != null)
            return false;
      } else if (!pageMaxCache.equals(other.pageMaxCache))
         return false;
      if (redeliveryDelay == null) {
         if (other.redeliveryDelay != null)
            return false;
      } else if (!redeliveryDelay.equals(other.redeliveryDelay))
         return false;
      if (redeliveryMultiplier == null) {
         if (other.redeliveryMultiplier != null)
            return false;
      } else if (!redeliveryMultiplier.equals(other.redeliveryMultiplier))
         return false;
      if (maxRedeliveryDelay == null) {
         if (other.maxRedeliveryDelay != null)
            return false;
      } else if (!maxRedeliveryDelay.equals(other.maxRedeliveryDelay))
         return false;
      if (redistributionDelay == null) {
         if (other.redistributionDelay != null)
            return false;
      } else if (!redistributionDelay.equals(other.redistributionDelay))
         return false;
      if (sendToDLAOnNoRoute == null) {
         if (other.sendToDLAOnNoRoute != null)
            return false;
      } else if (!sendToDLAOnNoRoute.equals(other.sendToDLAOnNoRoute))
         return false;
      if (slowConsumerThreshold == null) {
         if (other.slowConsumerThreshold != null)
            return false;
      } else if (!slowConsumerThreshold.equals(other.slowConsumerThreshold))
         return false;
      if (slowConsumerCheckPeriod == null) {
         if (other.slowConsumerCheckPeriod != null)
            return false;
      } else if (!slowConsumerCheckPeriod.equals(other.slowConsumerCheckPeriod))
         return false;
      if (slowConsumerPolicy == null) {
         if (other.slowConsumerPolicy != null)
            return false;
      } else if (!slowConsumerPolicy.equals(other.slowConsumerPolicy))
         return false;
      if (autoCreateJmsQueues == null) {
         if (other.autoCreateJmsQueues != null)
            return false;
      } else if (!autoCreateJmsQueues.equals(other.autoCreateJmsQueues))
         return false;
      if (autoDeleteJmsQueues == null) {
         if (other.autoDeleteJmsQueues != null)
            return false;
      } else if (!autoDeleteJmsQueues.equals(other.autoDeleteJmsQueues))
         return false;
      if (autoCreateJmsTopics == null) {
         if (other.autoCreateJmsTopics != null)
            return false;
      } else if (!autoCreateJmsTopics.equals(other.autoCreateJmsTopics))
         return false;
      if (autoDeleteJmsTopics == null) {
         if (other.autoDeleteJmsTopics != null)
            return false;
      } else if (!autoDeleteJmsTopics.equals(other.autoDeleteJmsTopics))
         return false;
      if (autoCreateQueues == null) {
         if (other.autoCreateQueues != null)
            return false;
      } else if (!autoCreateQueues.equals(other.autoCreateQueues))
         return false;
      if (autoDeleteQueues == null) {
         if (other.autoDeleteQueues != null)
            return false;
      } else if (!autoDeleteQueues.equals(other.autoDeleteQueues))
         return false;
      if (autoCreateAddresses == null) {
         if (other.autoCreateAddresses != null)
            return false;
      } else if (!autoCreateAddresses.equals(other.autoCreateAddresses))
         return false;
      if (autoDeleteAddresses == null) {
         if (other.autoDeleteAddresses != null)
            return false;
      } else if (!autoDeleteAddresses.equals(other.autoDeleteAddresses))
         return false;
      if (managementBrowsePageSize == null) {
         if (other.managementBrowsePageSize != null)
            return false;
      } else if (!managementBrowsePageSize.equals(other.managementBrowsePageSize))
         return false;
      if (queuePrefetch == null) {
         if (other.queuePrefetch != null)
            return false;
      } else if (!queuePrefetch.equals(other.queuePrefetch))
         return false;

      if (maxSizeBytesRejectThreshold == null) {
         if (other.maxSizeBytesRejectThreshold != null)
            return false;
      } else if (!maxSizeBytesRejectThreshold.equals(other.maxSizeBytesRejectThreshold))
         return false;
      return true;
   }

   /* (non-Javadoc)
    * @see java.lang.Object#toString()
    */
   @Override
   public String toString() {
      return "AddressSettings [addressFullMessagePolicy=" + addressFullMessagePolicy +
         ", deadLetterAddress=" +
         deadLetterAddress +
         ", dropMessagesWhenFull=" +
         dropMessagesWhenFull +
         ", expiryAddress=" +
         expiryAddress +
         ", expiryDelay=" +
         expiryDelay +
         ", lastValueQueue=" +
         lastValueQueue +
         ", maxDeliveryAttempts=" +
         maxDeliveryAttempts +
         ", maxSizeBytes=" +
         maxSizeBytes +
         ", maxSizeBytesRejectThreshold=" +
         maxSizeBytesRejectThreshold +
         ", messageCounterHistoryDayLimit=" +
         messageCounterHistoryDayLimit +
         ", pageSizeBytes=" +
         pageSizeBytes +
         ", pageMaxCache=" +
         pageMaxCache +
         ", redeliveryDelay=" +
         redeliveryDelay +
         ", redeliveryMultiplier=" +
         redeliveryMultiplier +
         ", maxRedeliveryDelay=" +
         maxRedeliveryDelay +
         ", redistributionDelay=" +
         redistributionDelay +
         ", sendToDLAOnNoRoute=" +
         sendToDLAOnNoRoute +
         ", slowConsumerThreshold=" +
         slowConsumerThreshold +
         ", slowConsumerCheckPeriod=" +
         slowConsumerCheckPeriod +
         ", slowConsumerPolicy=" +
         slowConsumerPolicy +
         ", autoCreateJmsQueues=" +
         autoCreateJmsQueues +
         ", autoDeleteJmsQueues=" +
         autoDeleteJmsQueues +
         ", autoCreateJmsTopics=" +
         autoCreateJmsTopics +
         ", autoDeleteJmsTopics=" +
         autoDeleteJmsTopics +
         ", autoCreateQueues=" +
         autoCreateQueues +
         ", autoDeleteQueues=" +
         autoDeleteQueues +
         ", autoCreateAddresses=" +
         autoCreateAddresses +
         ", autoDeleteAddresses=" +
         autoDeleteAddresses +
         ", managementBrowsePageSize=" +
         managementBrowsePageSize +
         "]";
   }
}
