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
import java.util.Objects;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.core.journal.EncodingSupport;
import org.apache.activemq.artemis.core.settings.Mergeable;
import org.apache.activemq.artemis.utils.bean.MetaBean;
import org.apache.activemq.artemis.utils.BufferHelper;

/**
 * Configuration settings that are applied on the address level
 */
public class AddressSettings implements Mergeable<AddressSettings>, Serializable, EncodingSupport {

   static MetaBean<AddressSettings> metaBean = new MetaBean<>();

   private static final long serialVersionUID = 1607502280582336366L;

   /**
    * defaults used if null, this allows merging
    */
   public static final long DEFAULT_MAX_SIZE_BYTES = -1;

   public static final long DEFAULT_MAX_SIZE_MESSAGES = -1;

   public static final int DEFAULT_MAX_READ_PAGE_MESSAGES = -1;

   public static final AddressFullMessagePolicy DEFAULT_ADDRESS_FULL_MESSAGE_POLICY = AddressFullMessagePolicy.PAGE;

   public static final int DEFAULT_PAGE_SIZE = 10 * 1024 * 1024;

   public static final int DEFAULT_MAX_DELIVERY_ATTEMPTS = 10;

   public static final int DEFAULT_PAGE_MAX_CACHE = 5;

   public static final int DEFAULT_MESSAGE_COUNTER_HISTORY_DAY_LIMIT = 0;

   public static final long DEFAULT_REDELIVER_DELAY = 0L;

   public static final double DEFAULT_REDELIVER_MULTIPLIER = 1.0;

   public static final double DEFAULT_REDELIVER_COLLISION_AVOIDANCE_FACTOR = 0.0;

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

   public static final boolean DEFAULT_AUTO_DELETE_CREATED_QUEUES = false;

   public static final long DEFAULT_AUTO_DELETE_QUEUES_DELAY = 0;

   public static final boolean DEFAULT_AUTO_DELETE_QUEUES_SKIP_USAGE_CHECK = false;

   public static final long DEFAULT_AUTO_DELETE_QUEUES_MESSAGE_COUNT = 0;

   public static final DeletionPolicy DEFAULT_CONFIG_DELETE_QUEUES = DeletionPolicy.OFF;

   public static final boolean DEFAULT_AUTO_CREATE_ADDRESSES = true;

   public static final boolean DEFAULT_AUTO_DELETE_ADDRESSES = true;

   public static final long DEFAULT_AUTO_DELETE_ADDRESSES_DELAY = 0;

   public static final boolean DEFAULT_AUTO_DELETE_ADDRESSES_SKIP_USAGE_CHECK = false;

   public static final DeletionPolicy DEFAULT_CONFIG_DELETE_ADDRESSES = DeletionPolicy.OFF;

   public static final DeletionPolicy DEFAULT_CONFIG_DELETE_DIVERTS = DeletionPolicy.OFF;

   public static final long DEFAULT_REDISTRIBUTION_DELAY = -1;

   public static final boolean DEFAULT_AUTO_CREATE_EXPIRY_RESOURCES = false;

   public static final SimpleString DEFAULT_EXPIRY_QUEUE_PREFIX = SimpleString.of("EXP.");

   public static final SimpleString DEFAULT_EXPIRY_QUEUE_SUFFIX = SimpleString.of("");

   public static final long DEFAULT_EXPIRY_DELAY = -1;

   public static final long DEFAULT_MIN_EXPIRY_DELAY = -1;

   public static final long DEFAULT_MAX_EXPIRY_DELAY = -1;

   public static final boolean DEFAULT_SEND_TO_DLA_ON_NO_ROUTE = false;

   public static final long DEFAULT_SLOW_CONSUMER_THRESHOLD = -1;

   public static final long DEFAULT_SLOW_CONSUMER_CHECK_PERIOD = 5;

   public static final int MANAGEMENT_BROWSE_PAGE_SIZE = 200;

   public static final SlowConsumerPolicy DEFAULT_SLOW_CONSUMER_POLICY = SlowConsumerPolicy.NOTIFY;

   public static final int DEFAULT_QUEUE_PREFETCH = 1000;

   // Default address drop threshold, applied to address settings with BLOCK policy.  -1 means no threshold enabled.
   public static final long DEFAULT_ADDRESS_REJECT_THRESHOLD = -1;

   public static final boolean DEFAULT_AUTO_CREATE_DEAD_LETTER_RESOURCES = false;

   public static final SimpleString DEFAULT_DEAD_LETTER_QUEUE_PREFIX = SimpleString.of("DLQ.");

   public static final SimpleString DEFAULT_DEAD_LETTER_QUEUE_SUFFIX = SimpleString.of("");

   public static final boolean DEFAULT_ENABLE_METRICS = true;

   public static final int MANAGEMENT_MESSAGE_ATTRIBUTE_SIZE_LIMIT = 256;

   public static final SlowConsumerThresholdMeasurementUnit DEFAULT_SLOW_CONSUMER_THRESHOLD_MEASUREMENT_UNIT = SlowConsumerThresholdMeasurementUnit.MESSAGES_PER_SECOND;

   public static final boolean DEFAULT_ENABLE_INGRESS_TIMESTAMP = false;

   static {
      metaBean.add(AddressFullMessagePolicy.class, "addressFullMessagePolicy", (t, p) -> t.addressFullMessagePolicy = p, t -> t.addressFullMessagePolicy);
   }
   private AddressFullMessagePolicy addressFullMessagePolicy = null;

   static {
      metaBean.add(Long.class, "maxSizeBytes", (t, p) -> t.maxSizeBytes = p, t -> t.maxSizeBytes);
   }
   private Long maxSizeBytes = null;

   static {
      metaBean.add(Integer.class, "maxReadPageBytes", (t, p) -> t.maxReadPageBytes = p, t -> t.maxReadPageBytes);
   }
   private Integer maxReadPageBytes = null;

   static {
      metaBean.add(Integer.class, "maxReadPageMessages", (t, p) -> t.maxReadPageMessages = p, t -> t.maxReadPageMessages);
   }
   private Integer maxReadPageMessages = null;

   static {
      metaBean.add(Integer.class, "prefetchPageBytes", (t, p) -> t.prefetchPageBytes = p, t -> t.prefetchPageBytes);
   }
   private Integer prefetchPageBytes = null;

   static {
      metaBean.add(Integer.class, "prefetchPageMessages", (t, p) -> t.prefetchPageMessages = p, t -> t.prefetchPageMessages);
   }
   private Integer prefetchPageMessages = null;

   static {
      metaBean.add(Long.class, "pageLimitBytes", (t, p) -> t.pageLimitBytes = p, t -> t.pageLimitBytes);
   }
   private Long pageLimitBytes = null;

   static {
      metaBean.add(Long.class, "pageLimitMessages", (t, p) -> t.pageLimitMessages = p, t -> t.pageLimitMessages);
   }
   private Long pageLimitMessages = null;

   static {
      metaBean.add(PageFullMessagePolicy.class, "pageFullMessagePolicy", (t, p) -> t.pageFullMessagePolicy = p, t -> t.pageFullMessagePolicy);
   }
   private PageFullMessagePolicy pageFullMessagePolicy = null;

   static {
      metaBean.add(Long.class, "maxSizeMessages", (t, p) -> t.maxSizeMessages = p, t -> t.maxSizeMessages);
   }
   private Long maxSizeMessages = null;

   static {
      metaBean.add(Integer.class, "pageSizeBytes", (t, p) -> t.pageSizeBytes = p, t -> t.pageSizeBytes);
   }
   private Integer pageSizeBytes = null;

   static {
      metaBean.add(Integer.class, "pageCacheMaxSize", (t, p) -> t.pageCacheMaxSize = p, t -> t.pageCacheMaxSize);
   }
   private Integer pageCacheMaxSize = null;

   static {
      metaBean.add(Boolean.class, "dropMessagesWhenFull", (t, p) -> t.dropMessagesWhenFull = p, t -> t.dropMessagesWhenFull);
   }
   private Boolean dropMessagesWhenFull = null;

   static {
      metaBean.add(Integer.class, "maxDeliveryAttempts", (t, p) -> t.maxDeliveryAttempts = p, t -> t.maxDeliveryAttempts);
   }
   private Integer maxDeliveryAttempts = null;

   static {
      metaBean.add(Integer.class, "messageCounterHistoryDayLimit", (t, p) -> t.messageCounterHistoryDayLimit = p, t -> t.messageCounterHistoryDayLimit);
   }
   private Integer messageCounterHistoryDayLimit = null;

   static {
      metaBean.add(Long.class, "redeliveryDelay", (t, p) -> t.redeliveryDelay = p, t -> t.redeliveryDelay);
   }
   private Long redeliveryDelay = null;

   static {
      metaBean.add(Double.class, "redeliveryMultiplier", (t, p) -> t.redeliveryMultiplier = p, t -> t.redeliveryMultiplier);
   }
   private Double redeliveryMultiplier = null;

   static {
      metaBean.add(Double.class, "redeliveryCollisionAvoidanceFactor", (t, p) -> t.redeliveryCollisionAvoidanceFactor = p, t -> t.redeliveryCollisionAvoidanceFactor);
   }
   private Double redeliveryCollisionAvoidanceFactor = null;

   static {
      metaBean.add(Long.class, "maxRedeliveryDelay", (t, p) -> t.maxRedeliveryDelay = p, t -> t.maxRedeliveryDelay);
   }
   private Long maxRedeliveryDelay = null;

   static {
      metaBean.add(SimpleString.class, "deadLetterAddress", (t, p) -> t.deadLetterAddress = p, t -> t.deadLetterAddress);
   }
   private SimpleString deadLetterAddress = null;

   static {
      metaBean.add(SimpleString.class, "expiryAddress", (t, p) -> t.expiryAddress = p, t -> t.expiryAddress);
   }
   private SimpleString expiryAddress = null;

   static {
      metaBean.add(Long.class, "expiryDelay", (t, p) -> t.expiryDelay = p, t -> t.expiryDelay);
   }
   private Long expiryDelay = null;

   static {
      metaBean.add(Long.class, "minExpiryDelay", (t, p) -> t.minExpiryDelay = p, t -> t.minExpiryDelay);
   }
   private Long minExpiryDelay = null;

   static {
      metaBean.add(Long.class, "maxExpiryDelay", (t, p) -> t.maxExpiryDelay = p, t -> t.maxExpiryDelay);
   }
   private Long maxExpiryDelay = null;

   static {
      metaBean.add(Boolean.class, "defaultLastValueQueue", (t, p) -> t.defaultLastValueQueue = p, t -> t.defaultLastValueQueue);
   }
   private Boolean defaultLastValueQueue = null;

   static {
      metaBean.add(SimpleString.class, "defaultLastValueKey", (t, p) -> t.defaultLastValueKey = p, t -> t.defaultLastValueKey);
   }
   private SimpleString defaultLastValueKey = null;

   static {
      metaBean.add(Boolean.class, "defaultNonDestructive", (t, p) -> t.defaultNonDestructive = p, t -> t.defaultNonDestructive);
   }
   private Boolean defaultNonDestructive = null;

   static {
      metaBean.add(Boolean.class, "defaultExclusiveQueue", (t, p) -> t.defaultExclusiveQueue = p, t -> t.defaultExclusiveQueue);
   }
   private Boolean defaultExclusiveQueue = null;

   static {
      metaBean.add(Boolean.class, "defaultGroupRebalance", (t, p) -> t.defaultGroupRebalance = p, t -> t.defaultGroupRebalance);
   }
   private Boolean defaultGroupRebalance = null;

   static {
      metaBean.add(Boolean.class, "defaultGroupRebalancePauseDispatch", (t, p) -> t.defaultGroupRebalancePauseDispatch = p, t -> t.defaultGroupRebalancePauseDispatch);
   }
   private Boolean defaultGroupRebalancePauseDispatch = null;

   static {
      metaBean.add(Integer.class, "defaultGroupBuckets", (t, p) -> t.defaultGroupBuckets = p, t -> t.defaultGroupBuckets);
   }
   private Integer defaultGroupBuckets = null;

   static {
      metaBean.add(SimpleString.class, "defaultGroupFirstKey", (t, p) -> t.defaultGroupFirstKey = p, t -> t.defaultGroupFirstKey);
   }
   private SimpleString defaultGroupFirstKey = null;

   static {
      metaBean.add(Long.class, "redistributionDelay", (t, p) -> t.redistributionDelay = p, t -> t.redistributionDelay);
   }
   private Long redistributionDelay = null;

   static {
      metaBean.add(Boolean.class, "sendToDLAOnNoRoute", (t, p) -> t.sendToDLAOnNoRoute = p, t -> t.sendToDLAOnNoRoute);
   }
   private Boolean sendToDLAOnNoRoute = null;

   static {
      metaBean.add(Long.class, "slowConsumerThreshold", (t, p) -> t.slowConsumerThreshold = p, t -> t.slowConsumerThreshold);
   }
   private Long slowConsumerThreshold = null;

   static {
      metaBean.add(SlowConsumerThresholdMeasurementUnit.class, "slowConsumerThresholdMeasurementUnit", (t, p) -> t.slowConsumerThresholdMeasurementUnit = p, t -> t.slowConsumerThresholdMeasurementUnit);
   }
   private SlowConsumerThresholdMeasurementUnit slowConsumerThresholdMeasurementUnit = DEFAULT_SLOW_CONSUMER_THRESHOLD_MEASUREMENT_UNIT;

   static {
      metaBean.add(Long.class, "slowConsumerCheckPeriod", (t, p) -> t.slowConsumerCheckPeriod = p, t -> t.slowConsumerCheckPeriod);
   }
   private Long slowConsumerCheckPeriod = null;

   static {
      metaBean.add(SlowConsumerPolicy.class, "slowConsumerPolicy", (t, p) -> t.slowConsumerPolicy = p, t -> t.slowConsumerPolicy);
   }
   private SlowConsumerPolicy slowConsumerPolicy = null;

   static {
      metaBean.add(Boolean.class, "autoCreateJmsQueues", (t, p) -> t.autoCreateJmsQueues = (Boolean) p, t -> t.autoCreateJmsQueues, t -> t.autoCreateJmsQueues != null);
   }
   @Deprecated
   private Boolean autoCreateJmsQueues = null;

   static {
      metaBean.add(Boolean.class, "autoDeleteJmsQueues", (t, p) -> t.autoDeleteJmsQueues = (Boolean) p, t -> t.autoDeleteJmsQueues, t -> t.autoDeleteJmsQueues != null);
   }
   @Deprecated
   private Boolean autoDeleteJmsQueues = null;

   static {
      metaBean.add(Boolean.class, "autoCreateJmsTopics", (t, p) -> t.autoCreateJmsTopics = (Boolean) p, t -> t.autoCreateJmsTopics, t -> t.autoCreateJmsTopics != null);
   }
   @Deprecated
   private Boolean autoCreateJmsTopics = null;

   static {
      metaBean.add(Boolean.class, "autoDeleteJmsTopics", (t, p) -> t.autoDeleteJmsTopics = (Boolean) p, t -> t.autoDeleteJmsTopics, t -> t.autoDeleteJmsTopics != null);
   }
   @Deprecated
   private Boolean autoDeleteJmsTopics = null;

   static {
      metaBean.add(Boolean.class, "autoCreateQueues", (t, p) -> t.autoCreateQueues = p, t -> t.autoCreateQueues);
   }
   private Boolean autoCreateQueues = null;

   static {
      metaBean.add(Boolean.class, "autoDeleteQueues", (t, p) -> t.autoDeleteQueues = p, t -> t.autoDeleteQueues);
   }
   private Boolean autoDeleteQueues = null;

   static {
      metaBean.add(Boolean.class, "autoDeleteCreatedQueues", (t, p) -> t.autoDeleteCreatedQueues = p, t -> t.autoDeleteCreatedQueues);
   }
   private Boolean autoDeleteCreatedQueues = null;

   static {
      metaBean.add(Long.class, "autoDeleteQueuesDelay", (t, p) -> t.autoDeleteQueuesDelay = p, t -> t.autoDeleteQueuesDelay);
   }
   private Long autoDeleteQueuesDelay = null;

   static {
      metaBean.add(Boolean.class, "autoDeleteQueuesSkipUsageCheck", (t, p) -> t.autoDeleteQueuesSkipUsageCheck = p, t -> t.autoDeleteQueuesSkipUsageCheck);
   }
   private Boolean autoDeleteQueuesSkipUsageCheck = null;

   static {
      metaBean.add(Long.class, "autoDeleteQueuesMessageCount", (t, p) -> t.autoDeleteQueuesMessageCount = p, t -> t.autoDeleteQueuesMessageCount);
   }
   private Long autoDeleteQueuesMessageCount = null;

   static {
      metaBean.add(Long.class, "defaultRingSize", (t, p) -> t.defaultRingSize = p, t -> t.defaultRingSize);
   }
   private Long defaultRingSize = null;

   static {
      metaBean.add(Long.class, "retroactiveMessageCount", (t, p) -> t.retroactiveMessageCount = p, t -> t.retroactiveMessageCount);
   }
   private Long retroactiveMessageCount = null;

   static {
      metaBean.add(DeletionPolicy.class, "configDeleteQueues", (t, p) -> t.configDeleteQueues = p, t -> t.configDeleteQueues);
   }
   private DeletionPolicy configDeleteQueues = null;

   static {
      metaBean.add(Boolean.class, "autoCreateAddresses", (t, p) -> t.autoCreateAddresses = p, t -> t.autoCreateAddresses);
   }
   private Boolean autoCreateAddresses = null;

   static {
      metaBean.add(Boolean.class, "autoDeleteAddresses", (t, p) -> t.autoDeleteAddresses = p, t -> t.autoDeleteAddresses);
   }
   private Boolean autoDeleteAddresses = null;

   static {
      metaBean.add(Long.class, "autoDeleteAddressesDelay", (t, p) -> t.autoDeleteAddressesDelay = p, t -> t.autoDeleteAddressesDelay);
   }
   private Long autoDeleteAddressesDelay = null;

   static {
      metaBean.add(Boolean.class, "autoDeleteAddressesSkipUsageCheck", (t, p) -> t.autoDeleteAddressesSkipUsageCheck = p, t -> t.autoDeleteAddressesSkipUsageCheck);
   }
   private Boolean autoDeleteAddressesSkipUsageCheck = null;

   static {
      metaBean.add(DeletionPolicy.class, "configDeleteAddresses", (t, p) -> t.configDeleteAddresses = p, t -> t.configDeleteAddresses);
   }
   private DeletionPolicy configDeleteAddresses = null;

   static {
      metaBean.add(DeletionPolicy.class, "configDeleteDiverts", (t, p) -> t.configDeleteDiverts = p, t -> t.configDeleteDiverts);
   }
   private DeletionPolicy configDeleteDiverts = null;

   static {
      metaBean.add(Integer.class, "managementBrowsePageSize", (t, p) -> t.managementBrowsePageSize = p, t -> t.managementBrowsePageSize);
   }
   private Integer managementBrowsePageSize = AddressSettings.MANAGEMENT_BROWSE_PAGE_SIZE;

   static {
      metaBean.add(Long.class, "maxSizeBytesRejectThreshold", (t, p) -> t.maxSizeBytesRejectThreshold = p, t -> t.maxSizeBytesRejectThreshold);
   }
   private Long maxSizeBytesRejectThreshold = null;

   static {
      metaBean.add(Integer.class, "defaultMaxConsumers", (t, p) -> t.defaultMaxConsumers = p, t -> t.defaultMaxConsumers);
   }
   private Integer defaultMaxConsumers = null;

   static {
      metaBean.add(Boolean.class, "defaultPurgeOnNoConsumers", (t, p) -> t.defaultPurgeOnNoConsumers = p, t -> t.defaultPurgeOnNoConsumers);
   }
   private Boolean defaultPurgeOnNoConsumers = null;

   static {
      metaBean.add(Integer.class, "defaultConsumersBeforeDispatch", (t, p) -> t.defaultConsumersBeforeDispatch = p, t -> t.defaultConsumersBeforeDispatch);
   }
   private Integer defaultConsumersBeforeDispatch = null;

   static {
      metaBean.add(Long.class, "defaultDelayBeforeDispatch", (t, p) -> t.defaultDelayBeforeDispatch = p, t -> t.defaultDelayBeforeDispatch);
   }
   private Long defaultDelayBeforeDispatch = null;

   static {
      metaBean.add(RoutingType.class, "defaultQueueRoutingType", (t, p) -> t.defaultQueueRoutingType = p, t -> t.defaultQueueRoutingType);
   }
   private RoutingType defaultQueueRoutingType = null;

   static {
      metaBean.add(RoutingType.class, "defaultAddressRoutingType", (t, p) -> t.defaultAddressRoutingType = p, t -> t.defaultAddressRoutingType);
   }
   private RoutingType defaultAddressRoutingType = null;

   static {
      metaBean.add(Integer.class, "defaultConsumerWindowSize", (t, p) -> t.defaultConsumerWindowSize = p, t -> t.defaultConsumerWindowSize);
   }
   private Integer defaultConsumerWindowSize = null;

   static {
      metaBean.add(Boolean.class, "autoCreateDeadLetterResources", (t, p) -> t.autoCreateDeadLetterResources = p, t -> t.autoCreateDeadLetterResources);
   }
   private Boolean autoCreateDeadLetterResources = null;

   static {
      metaBean.add(SimpleString.class, "deadLetterQueuePrefix", (t, p) -> t.deadLetterQueuePrefix = p, t -> t.deadLetterQueuePrefix);
   }
   private SimpleString deadLetterQueuePrefix = null;

   static {
      metaBean.add(SimpleString.class, "deadLetterQueueSuffix", (t, p) -> t.deadLetterQueueSuffix = p, t -> t.deadLetterQueueSuffix);
   }
   private SimpleString deadLetterQueueSuffix = null;

   static {
      metaBean.add(Boolean.class, "autoCreateExpiryResources", (t, p) -> t.autoCreateExpiryResources = p, t -> t.autoCreateExpiryResources);
   }
   private Boolean autoCreateExpiryResources = null;

   static {
      metaBean.add(SimpleString.class, "expiryQueuePrefix", (t, p) -> t.expiryQueuePrefix = p, t -> t.expiryQueuePrefix);
   }
   private SimpleString expiryQueuePrefix = null;

   static {
      metaBean.add(SimpleString.class, "expiryQueueSuffix", (t, p) -> t.expiryQueueSuffix = p, t -> t.expiryQueueSuffix);
   }
   private SimpleString expiryQueueSuffix = null;

   static {
      metaBean.add(Boolean.class, "enableMetrics", (t, p) -> t.enableMetrics = p, t -> t.enableMetrics);
   }
   private Boolean enableMetrics = null;

   static {
      metaBean.add(Integer.class, "managementMessageAttributeSizeLimit", (t, p) -> t.managementMessageAttributeSizeLimit = p, t -> t.managementMessageAttributeSizeLimit);
   }
   private Integer managementMessageAttributeSizeLimit = null;

   static {
      metaBean.add(Boolean.class, "enableIngressTimestamp", (t, p) -> t.enableIngressTimestamp = p, t -> t.enableIngressTimestamp);
   }
   private Boolean enableIngressTimestamp = null;

   static {
      metaBean.add(Integer.class, "idCacheSize", (t, p) -> t.idCacheSize = p, t -> t.idCacheSize);
   }
   private Integer idCacheSize = null;

   static {
      metaBean.add(Integer.class, "queuePrefetch", (t, p) -> t.queuePrefetch = p, t -> t.queuePrefetch);
   }

   static {
      metaBean.add(Integer.class, "initialQueueBufferSize", (t, p) -> t.initialQueueBufferSize = p, t -> t.initialQueueBufferSize);
   }
   private Integer initialQueueBufferSize = null;

   //from amq5
   //make it transient
   @Deprecated
   private transient Integer queuePrefetch = null;

   public AddressSettings(AddressSettings other) {
      metaBean.copy(other, this);
   }

   public AddressSettings() {
   }

   @Deprecated
   public boolean isAutoCreateJmsQueues() {
      return autoCreateJmsQueues != null ? autoCreateJmsQueues : AddressSettings.DEFAULT_AUTO_CREATE_JMS_QUEUES;
   }

   public String toJSON() {
      return metaBean.toJSON(this, true).toString();
   }

   public static AddressSettings fromJSON(String jsonString) {
      AddressSettings newSettings = new AddressSettings();
      metaBean.fromJSON(newSettings, jsonString);
      return newSettings;
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

   public Boolean isAutoCreateQueues() {
      return autoCreateQueues != null ? autoCreateQueues : AddressSettings.DEFAULT_AUTO_CREATE_QUEUES;
   }

   public AddressSettings setAutoCreateQueues(Boolean autoCreateQueues) {
      this.autoCreateQueues = autoCreateQueues;
      return this;
   }

   public Boolean isAutoDeleteQueues() {
      return autoDeleteQueues != null ? autoDeleteQueues : AddressSettings.DEFAULT_AUTO_DELETE_QUEUES;
   }

   public AddressSettings setAutoDeleteQueues(Boolean autoDeleteQueues) {
      this.autoDeleteQueues = autoDeleteQueues;
      return this;
   }

   public AddressSettings setAutoDeleteCreatedQueues(Boolean autoDeleteCreatedQueues) {
      this.autoDeleteCreatedQueues = autoDeleteCreatedQueues;
      return this;
   }

   public Boolean isAutoDeleteCreatedQueues() {
      return autoDeleteCreatedQueues != null ? autoDeleteCreatedQueues : AddressSettings.DEFAULT_AUTO_DELETE_CREATED_QUEUES;
   }


   public long getAutoDeleteQueuesDelay() {
      return autoDeleteQueuesDelay != null ? autoDeleteQueuesDelay : AddressSettings.DEFAULT_AUTO_DELETE_QUEUES_DELAY;
   }

   public AddressSettings setAutoDeleteQueuesDelay(final long autoDeleteQueuesDelay) {
      this.autoDeleteQueuesDelay = autoDeleteQueuesDelay;
      return this;
   }

   public boolean getAutoDeleteQueuesSkipUsageCheck() {
      return autoDeleteQueuesSkipUsageCheck != null ? autoDeleteQueuesSkipUsageCheck : AddressSettings.DEFAULT_AUTO_DELETE_QUEUES_SKIP_USAGE_CHECK;
   }

   public AddressSettings setAutoDeleteQueuesSkipUsageCheck(final boolean autoDeleteQueuesSkipUsageCheck) {
      this.autoDeleteQueuesSkipUsageCheck = autoDeleteQueuesSkipUsageCheck;
      return this;
   }

   public long getAutoDeleteQueuesMessageCount() {
      return autoDeleteQueuesMessageCount != null ? autoDeleteQueuesMessageCount : AddressSettings.DEFAULT_AUTO_DELETE_QUEUES_MESSAGE_COUNT;
   }

   public AddressSettings setAutoDeleteQueuesMessageCount(final long autoDeleteQueuesMessageCount) {
      this.autoDeleteQueuesMessageCount = autoDeleteQueuesMessageCount;
      return this;
   }

   public DeletionPolicy getConfigDeleteQueues() {
      return configDeleteQueues != null ? configDeleteQueues : AddressSettings.DEFAULT_CONFIG_DELETE_QUEUES;
   }

   public AddressSettings setConfigDeleteQueues(DeletionPolicy configDeleteQueues) {
      this.configDeleteQueues = configDeleteQueues;
      return this;
   }

   public Boolean isAutoCreateAddresses() {
      return autoCreateAddresses != null ? autoCreateAddresses : AddressSettings.DEFAULT_AUTO_CREATE_ADDRESSES;
   }

   public AddressSettings setAutoCreateAddresses(Boolean autoCreateAddresses) {
      this.autoCreateAddresses = autoCreateAddresses;
      return this;
   }

   public Boolean isAutoDeleteAddresses() {
      return autoDeleteAddresses != null ? autoDeleteAddresses : AddressSettings.DEFAULT_AUTO_DELETE_ADDRESSES;
   }

   public AddressSettings setAutoDeleteAddresses(Boolean autoDeleteAddresses) {
      this.autoDeleteAddresses = autoDeleteAddresses;
      return this;
   }

   public long getAutoDeleteAddressesDelay() {
      return autoDeleteAddressesDelay != null ? autoDeleteAddressesDelay : AddressSettings.DEFAULT_AUTO_DELETE_ADDRESSES_DELAY;
   }

   public AddressSettings setAutoDeleteAddressesDelay(final long autoDeleteAddressesDelay) {
      this.autoDeleteAddressesDelay = autoDeleteAddressesDelay;
      return this;
   }

   public boolean isAutoDeleteAddressesSkipUsageCheck() {
      return autoDeleteAddressesSkipUsageCheck != null ? autoDeleteAddressesSkipUsageCheck : AddressSettings.DEFAULT_AUTO_DELETE_ADDRESSES_SKIP_USAGE_CHECK;
   }

   public AddressSettings setAutoDeleteAddressesSkipUsageCheck(final boolean autoDeleteAddressesSkipUsageCheck) {
      this.autoDeleteAddressesSkipUsageCheck = autoDeleteAddressesSkipUsageCheck;
      return this;
   }

   public DeletionPolicy getConfigDeleteAddresses() {
      return configDeleteAddresses != null ? configDeleteAddresses : AddressSettings.DEFAULT_CONFIG_DELETE_ADDRESSES;
   }

   public AddressSettings setConfigDeleteAddresses(DeletionPolicy configDeleteAddresses) {
      this.configDeleteAddresses = configDeleteAddresses;
      return this;
   }

   public AddressSettings setConfigDeleteDiverts(DeletionPolicy configDeleteDiverts) {
      this.configDeleteDiverts = configDeleteDiverts;
      return this;
   }

   public DeletionPolicy getConfigDeleteDiverts() {
      return configDeleteDiverts != null ? configDeleteDiverts : AddressSettings.DEFAULT_CONFIG_DELETE_DIVERTS;
   }

   public Integer getDefaultMaxConsumers() {
      return defaultMaxConsumers != null ? defaultMaxConsumers : ActiveMQDefaultConfiguration.getDefaultMaxQueueConsumers();
   }

   public AddressSettings setDefaultMaxConsumers(Integer defaultMaxConsumers) {
      this.defaultMaxConsumers = defaultMaxConsumers;
      return this;
   }

   public Integer getDefaultConsumersBeforeDispatch() {
      return defaultConsumersBeforeDispatch != null ? defaultConsumersBeforeDispatch : ActiveMQDefaultConfiguration.getDefaultConsumersBeforeDispatch();
   }

   public AddressSettings setDefaultConsumersBeforeDispatch(Integer defaultConsumersBeforeDispatch) {
      this.defaultConsumersBeforeDispatch = defaultConsumersBeforeDispatch;
      return this;
   }

   public Long getDefaultDelayBeforeDispatch() {
      return defaultDelayBeforeDispatch != null ? defaultDelayBeforeDispatch : ActiveMQDefaultConfiguration.getDefaultDelayBeforeDispatch();
   }

   public AddressSettings setDefaultDelayBeforeDispatch(Long defaultDelayBeforeDispatch) {
      this.defaultDelayBeforeDispatch = defaultDelayBeforeDispatch;
      return this;
   }

   public Boolean isDefaultPurgeOnNoConsumers() {
      return defaultPurgeOnNoConsumers != null ? defaultPurgeOnNoConsumers : ActiveMQDefaultConfiguration.getDefaultPurgeOnNoConsumers();
   }

   public AddressSettings setDefaultPurgeOnNoConsumers(Boolean defaultPurgeOnNoConsumers) {
      this.defaultPurgeOnNoConsumers = defaultPurgeOnNoConsumers;
      return this;
   }

   public RoutingType getDefaultQueueRoutingType() {
      return defaultQueueRoutingType != null ? defaultQueueRoutingType : ActiveMQDefaultConfiguration.getDefaultRoutingType();
   }

   public AddressSettings setDefaultQueueRoutingType(RoutingType defaultQueueRoutingType) {
      this.defaultQueueRoutingType = defaultQueueRoutingType;
      return this;
   }

   public RoutingType getDefaultAddressRoutingType() {
      return defaultAddressRoutingType != null ? defaultAddressRoutingType : ActiveMQDefaultConfiguration.getDefaultRoutingType();
   }

   public AddressSettings setDefaultAddressRoutingType(RoutingType defaultAddressRoutingType) {
      this.defaultAddressRoutingType = defaultAddressRoutingType;
      return this;
   }

   public boolean isDefaultLastValueQueue() {
      return defaultLastValueQueue != null ? defaultLastValueQueue : AddressSettings.DEFAULT_LAST_VALUE_QUEUE;
   }

   public AddressSettings setDefaultLastValueQueue(final boolean defaultLastValueQueue) {
      this.defaultLastValueQueue = defaultLastValueQueue;
      return this;
   }

   public SimpleString getDefaultLastValueKey() {
      return defaultLastValueKey != null ? defaultLastValueKey : ActiveMQDefaultConfiguration.getDefaultLastValueKey();
   }

   public AddressSettings setDefaultLastValueKey(final SimpleString defaultLastValueKey) {
      this.defaultLastValueKey = defaultLastValueKey;
      return this;
   }

   public boolean isDefaultNonDestructive() {
      return defaultNonDestructive != null ? defaultNonDestructive : ActiveMQDefaultConfiguration.getDefaultNonDestructive();
   }

   public AddressSettings setDefaultNonDestructive(final boolean defaultNonDestructive) {
      this.defaultNonDestructive = defaultNonDestructive;
      return this;
   }

   public Boolean isDefaultExclusiveQueue() {
      return defaultExclusiveQueue != null ? defaultExclusiveQueue : ActiveMQDefaultConfiguration.getDefaultExclusive();
   }

   public AddressSettings setDefaultExclusiveQueue(Boolean defaultExclusiveQueue) {
      this.defaultExclusiveQueue = defaultExclusiveQueue;
      return this;
   }

   public AddressFullMessagePolicy getAddressFullMessagePolicy() {
      return addressFullMessagePolicy != null ? addressFullMessagePolicy : AddressSettings.DEFAULT_ADDRESS_FULL_MESSAGE_POLICY;
   }

   public AddressSettings setAddressFullMessagePolicy(final AddressFullMessagePolicy addressFullMessagePolicy) {
      this.addressFullMessagePolicy = addressFullMessagePolicy;
      return this;
   }

   public int getPageSizeBytes() {
      return pageSizeBytes != null ? pageSizeBytes : AddressSettings.DEFAULT_PAGE_SIZE;
   }

   public AddressSettings setPageSizeBytes(final int pageSize) {
      pageSizeBytes = pageSize;
      return this;
   }

   public int getPageCacheMaxSize() {
      return pageCacheMaxSize != null ? pageCacheMaxSize : AddressSettings.DEFAULT_PAGE_MAX_CACHE;
   }

   public AddressSettings setPageCacheMaxSize(final int pageCacheMaxSize) {
      this.pageCacheMaxSize = pageCacheMaxSize;
      return this;
   }

   public long getMaxSizeBytes() {
      return maxSizeBytes != null ? maxSizeBytes : AddressSettings.DEFAULT_MAX_SIZE_BYTES;
   }

   public long getMaxSizeMessages() {
      return maxSizeMessages != null ? maxSizeMessages : AddressSettings.DEFAULT_MAX_SIZE_MESSAGES;
   }

   public AddressSettings setMaxSizeMessages(final long maxSizeMessages) {
      this.maxSizeMessages = maxSizeMessages;
      return this;
   }

   public AddressSettings setMaxSizeBytes(final long maxSizeBytes) {
      this.maxSizeBytes = maxSizeBytes;
      return this;
   }

   public int getMaxReadPageMessages() {
      return maxReadPageMessages != null ? maxReadPageMessages : AddressSettings.DEFAULT_MAX_READ_PAGE_MESSAGES;
   }

   public AddressSettings setMaxReadPageMessages(final int maxReadPageMessages) {
      this.maxReadPageMessages = maxReadPageMessages;
      return this;
   }


   public int getPrefetchPageMessages() {
      return prefetchPageMessages != null ? prefetchPageMessages : getMaxReadPageMessages();
   }

   public AddressSettings setPrefetchPageMessages(final int prefetchPageMessages) {
      this.prefetchPageMessages = prefetchPageMessages <= 0 ? null : prefetchPageMessages;
      return this;
   }

   public Long getPageLimitBytes() {
      return pageLimitBytes;
   }

   public AddressSettings setPageLimitBytes(Long pageLimitBytes) {
      this.pageLimitBytes = pageLimitBytes;
      return this;
   }

   public Long getPageLimitMessages() {
      return pageLimitMessages;
   }

   public AddressSettings setPageLimitMessages(Long pageLimitMessages) {
      this.pageLimitMessages = pageLimitMessages;
      return this;
   }

   public PageFullMessagePolicy getPageFullMessagePolicy() {
      return this.pageFullMessagePolicy;
   }

   public AddressSettings setPageFullMessagePolicy(PageFullMessagePolicy policy) {
      this.pageFullMessagePolicy = policy;
      return this;
   }

   public int getMaxReadPageBytes() {
      return maxReadPageBytes != null ? maxReadPageBytes : 2 * getPageSizeBytes();
   }

   public AddressSettings setMaxReadPageBytes(final int maxReadPageBytes) {
      this.maxReadPageBytes = maxReadPageBytes;
      return this;
   }

   public int getPrefetchPageBytes() {
      return prefetchPageBytes != null ? prefetchPageBytes : getMaxReadPageBytes();
   }

   public AddressSettings setPrefetchPageBytes(final int prefetchPageBytes) {
      this.prefetchPageBytes = prefetchPageBytes <= 0 ? null : prefetchPageBytes;
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

   public double getRedeliveryCollisionAvoidanceFactor() {
      return redeliveryCollisionAvoidanceFactor != null ? redeliveryCollisionAvoidanceFactor : AddressSettings.DEFAULT_REDELIVER_COLLISION_AVOIDANCE_FACTOR;
   }

   public AddressSettings setRedeliveryCollisionAvoidanceFactor(final double redeliveryCollisionAvoidanceFactor) {
      this.redeliveryCollisionAvoidanceFactor = redeliveryCollisionAvoidanceFactor;
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

   public boolean isAutoCreateExpiryResources() {
      return autoCreateExpiryResources != null ? autoCreateExpiryResources : AddressSettings.DEFAULT_AUTO_CREATE_EXPIRY_RESOURCES;
   }

   public AddressSettings setAutoCreateExpiryResources(final boolean value) {
      autoCreateExpiryResources = value;
      return this;
   }

   public SimpleString getExpiryQueuePrefix() {
      return expiryQueuePrefix != null ? expiryQueuePrefix : AddressSettings.DEFAULT_EXPIRY_QUEUE_PREFIX;
   }

   public AddressSettings setExpiryQueuePrefix(final SimpleString value) {
      expiryQueuePrefix = value;
      return this;
   }

   public SimpleString getExpiryQueueSuffix() {
      return expiryQueueSuffix != null ? expiryQueueSuffix : AddressSettings.DEFAULT_EXPIRY_QUEUE_SUFFIX;
   }

   public AddressSettings setExpiryQueueSuffix(final SimpleString value) {
      expiryQueueSuffix = value;
      return this;
   }

   public Long getExpiryDelay() {
      return expiryDelay != null ? expiryDelay : AddressSettings.DEFAULT_EXPIRY_DELAY;
   }

   public AddressSettings setExpiryDelay(final Long expiryDelay) {
      this.expiryDelay = expiryDelay;
      return this;
   }

   public Long getMinExpiryDelay() {
      return minExpiryDelay != null ? minExpiryDelay : AddressSettings.DEFAULT_MIN_EXPIRY_DELAY;
   }

   public AddressSettings setMinExpiryDelay(final Long minExpiryDelay) {
      this.minExpiryDelay = minExpiryDelay;
      return this;
   }

   public Long getMaxExpiryDelay() {
      return maxExpiryDelay != null ? maxExpiryDelay : AddressSettings.DEFAULT_MAX_EXPIRY_DELAY;
   }

   public AddressSettings setMaxExpiryDelay(final Long maxExpiryDelay) {
      this.maxExpiryDelay = maxExpiryDelay;
      return this;
   }

   public boolean isSendToDLAOnNoRoute() {
      return sendToDLAOnNoRoute != null ? sendToDLAOnNoRoute : AddressSettings.DEFAULT_SEND_TO_DLA_ON_NO_ROUTE;
   }

   public AddressSettings setSendToDLAOnNoRoute(final boolean value) {
      sendToDLAOnNoRoute = value;
      return this;
   }

   public boolean isAutoCreateDeadLetterResources() {
      return autoCreateDeadLetterResources != null ? autoCreateDeadLetterResources : AddressSettings.DEFAULT_AUTO_CREATE_DEAD_LETTER_RESOURCES;
   }

   public AddressSettings setAutoCreateDeadLetterResources(final boolean value) {
      autoCreateDeadLetterResources = value;
      return this;
   }

   public SimpleString getDeadLetterQueuePrefix() {
      return deadLetterQueuePrefix != null ? deadLetterQueuePrefix : AddressSettings.DEFAULT_DEAD_LETTER_QUEUE_PREFIX;
   }

   public AddressSettings setDeadLetterQueuePrefix(final SimpleString value) {
      deadLetterQueuePrefix = value;
      return this;
   }

   public SimpleString getDeadLetterQueueSuffix() {
      return deadLetterQueueSuffix != null ? deadLetterQueueSuffix : AddressSettings.DEFAULT_DEAD_LETTER_QUEUE_SUFFIX;
   }

   public AddressSettings setDeadLetterQueueSuffix(final SimpleString value) {
      deadLetterQueueSuffix = value;
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

   public SlowConsumerThresholdMeasurementUnit getSlowConsumerThresholdMeasurementUnit() {
      return slowConsumerThresholdMeasurementUnit != null ? slowConsumerThresholdMeasurementUnit : AddressSettings.DEFAULT_SLOW_CONSUMER_THRESHOLD_MEASUREMENT_UNIT;
   }

   public AddressSettings setSlowConsumerThresholdMeasurementUnit(final SlowConsumerThresholdMeasurementUnit slowConsumerThresholdMeasurementUnit) {
      this.slowConsumerThresholdMeasurementUnit = slowConsumerThresholdMeasurementUnit;
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

   @Deprecated
   public int getQueuePrefetch() {
      return queuePrefetch != null ? queuePrefetch : AddressSettings.DEFAULT_QUEUE_PREFETCH;
   }

   @Deprecated
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
    * @return the defaultConsumerWindowSize
    */
   public int getDefaultConsumerWindowSize() {
      return defaultConsumerWindowSize != null ? defaultConsumerWindowSize : ActiveMQClient.DEFAULT_CONSUMER_WINDOW_SIZE;
   }

   /**
    * @param defaultConsumerWindowSize the defaultConsumerWindowSize to set
    */
   public AddressSettings setDefaultConsumerWindowSize(int defaultConsumerWindowSize) {
      this.defaultConsumerWindowSize = defaultConsumerWindowSize;
      return this;
   }

   /**
    * @return the defaultGroupBuckets
    */
   public boolean isDefaultGroupRebalance() {
      return defaultGroupRebalance != null ? defaultGroupRebalance : ActiveMQDefaultConfiguration.getDefaultGroupRebalance();
   }

   /**
    * @param defaultGroupRebalance the defaultGroupBuckets to set
    */
   public AddressSettings setDefaultGroupRebalance(boolean defaultGroupRebalance) {
      this.defaultGroupRebalance = defaultGroupRebalance;
      return this;
   }

   /**
    * @return the defaultGroupRebalancePauseDispatch
    */
   public boolean isDefaultGroupRebalancePauseDispatch() {
      return defaultGroupRebalancePauseDispatch != null ? defaultGroupRebalancePauseDispatch : ActiveMQDefaultConfiguration.getDefaultGroupRebalancePauseDispatch();
   }

   /**
    * @param defaultGroupRebalancePauseDispatch the defaultGroupBuckets to set
    */
   public AddressSettings setDefaultGroupRebalancePauseDispatch(boolean defaultGroupRebalancePauseDispatch) {
      this.defaultGroupRebalancePauseDispatch = defaultGroupRebalancePauseDispatch;
      return this;
   }

   /**
    * @return the defaultGroupBuckets
    */
   public int getDefaultGroupBuckets() {
      return defaultGroupBuckets != null ? defaultGroupBuckets : ActiveMQDefaultConfiguration.getDefaultGroupBuckets();
   }

   /**
    * @return the defaultGroupFirstKey
    */
   public SimpleString getDefaultGroupFirstKey() {
      return defaultGroupFirstKey != null ? defaultGroupFirstKey : ActiveMQDefaultConfiguration.getDefaultGroupFirstKey();
   }

   /**
    * @param defaultGroupFirstKey the defaultGroupFirstKey to set
    */
   public AddressSettings setDefaultGroupFirstKey(SimpleString defaultGroupFirstKey) {
      this.defaultGroupFirstKey = defaultGroupFirstKey;
      return this;
   }

   /**
    * @param defaultGroupBuckets the defaultGroupBuckets to set
    */
   public AddressSettings setDefaultGroupBuckets(int defaultGroupBuckets) {
      this.defaultGroupBuckets = defaultGroupBuckets;
      return this;
   }

   public long getDefaultRingSize() {
      return defaultRingSize != null ? defaultRingSize : ActiveMQDefaultConfiguration.DEFAULT_RING_SIZE;
   }

   public AddressSettings setDefaultRingSize(final long defaultRingSize) {
      this.defaultRingSize = defaultRingSize;
      return this;
   }

   public long getRetroactiveMessageCount() {
      return retroactiveMessageCount != null ? retroactiveMessageCount : ActiveMQDefaultConfiguration.DEFAULT_RETROACTIVE_MESSAGE_COUNT;
   }

   public AddressSettings setRetroactiveMessageCount(final long defaultRetroactiveMessageCount) {
      this.retroactiveMessageCount = defaultRetroactiveMessageCount;
      return this;
   }

   public boolean isEnableMetrics() {
      return enableMetrics != null ? enableMetrics : AddressSettings.DEFAULT_ENABLE_METRICS;
   }

   public AddressSettings setEnableMetrics(final boolean enableMetrics) {
      this.enableMetrics = enableMetrics;
      return this;
   }

   public int getManagementMessageAttributeSizeLimit() {
      return managementMessageAttributeSizeLimit != null ? managementMessageAttributeSizeLimit : AddressSettings.MANAGEMENT_MESSAGE_ATTRIBUTE_SIZE_LIMIT;
   }

   public AddressSettings setManagementMessageAttributeSizeLimit(int managementMessageAttributeSizeLimit) {
      this.managementMessageAttributeSizeLimit = managementMessageAttributeSizeLimit;
      return this;
   }

   public boolean isEnableIngressTimestamp() {
      return enableIngressTimestamp != null ? enableIngressTimestamp : AddressSettings.DEFAULT_ENABLE_INGRESS_TIMESTAMP;
   }

   public AddressSettings setEnableIngressTimestamp(final boolean enableIngressTimestamp) {
      this.enableIngressTimestamp = enableIngressTimestamp;
      return this;
   }

   public Integer getIDCacheSize() {
      return idCacheSize;
   }

   public AddressSettings setIDCacheSize(int idCacheSize) {
      this.idCacheSize = idCacheSize;
      return this;
   }

   public Integer getInitialQueueBufferSize() {
      return initialQueueBufferSize;
   }

   public AddressSettings setInitialQueueBufferSize(int initialQueueBufferSize) {
      this.initialQueueBufferSize = initialQueueBufferSize;
      return this;
   }

   /**
    * Merge two AddressSettings instances in one instance
    *
    * @param merged
    */
   @Override
   public void merge(final AddressSettings merged) {
      metaBean.forEach((type, name, setter, getter, gate) -> {
         if (getter.apply(AddressSettings.this) == null) {
            setter.accept(this, getter.apply(merged));
         }
      });
   }

   /**
    * Merge two AddressSettings instances in a new instance
    *
    * @param merged
    */
   @Override
   public AddressSettings mergeCopy(final AddressSettings merged) {
      AddressSettings target = new AddressSettings();

      metaBean.forEach((type, name, setter, getter, gate) -> {
         Object sourceValue = getter.apply(AddressSettings.this);
         if (sourceValue != null) {
            setter.accept(target, sourceValue);
         } else {
            setter.accept(target, getter.apply(merged));
         }
      });

      return target;
   }

   @Override
   public void decode(ActiveMQBuffer buffer) {
      int original = buffer.readerIndex();
      try {
         decode(buffer, false);
      } catch (Throwable e) {
         buffer.readerIndex(original);
         // Try a compatible version where the wire was broken
         decode(buffer, true);
      }
   }

   public void decode(ActiveMQBuffer buffer, boolean tryCompatible) {
      SimpleString policyStr = buffer.readNullableSimpleString();

      if (policyStr != null) {
         addressFullMessagePolicy = AddressFullMessagePolicy.valueOf(policyStr.toString());
      } else {
         addressFullMessagePolicy = null;
      }

      maxSizeBytes = BufferHelper.readNullableLong(buffer);

      Long pageSizeLong = BufferHelper.readNullableLong(buffer);
      pageSizeBytes = pageSizeLong == null ? null : pageSizeLong.intValue();

      pageCacheMaxSize = BufferHelper.readNullableInteger(buffer);

      dropMessagesWhenFull = BufferHelper.readNullableBoolean(buffer);

      maxDeliveryAttempts = BufferHelper.readNullableInteger(buffer);

      messageCounterHistoryDayLimit = BufferHelper.readNullableInteger(buffer);

      redeliveryDelay = BufferHelper.readNullableLong(buffer);

      redeliveryMultiplier = BufferHelper.readNullableDouble(buffer);

      maxRedeliveryDelay = BufferHelper.readNullableLong(buffer);

      deadLetterAddress = buffer.readNullableSimpleString();

      expiryAddress = buffer.readNullableSimpleString();

      expiryDelay = BufferHelper.readNullableLong(buffer);

      defaultLastValueQueue = BufferHelper.readNullableBoolean(buffer);

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

      policyStr = tryCompatible ? null : buffer.readNullableSimpleString();

      if (policyStr != null) {
         configDeleteQueues = DeletionPolicy.valueOf(policyStr.toString());
      } else {
         configDeleteQueues = null;
      }

      autoCreateAddresses = BufferHelper.readNullableBoolean(buffer);

      autoDeleteAddresses = BufferHelper.readNullableBoolean(buffer);

      policyStr = tryCompatible ? null : buffer.readNullableSimpleString();

      if (policyStr != null) {
         configDeleteAddresses = DeletionPolicy.valueOf(policyStr.toString());
      } else {
         configDeleteAddresses = null;
      }
      managementBrowsePageSize = BufferHelper.readNullableInteger(buffer);

      maxSizeBytesRejectThreshold = BufferHelper.readNullableLong(buffer);

      defaultMaxConsumers = BufferHelper.readNullableInteger(buffer);

      defaultPurgeOnNoConsumers = BufferHelper.readNullableBoolean(buffer);

      defaultQueueRoutingType = RoutingType.getType(buffer.readByte());

      defaultAddressRoutingType = RoutingType.getType(buffer.readByte());

      if (buffer.readableBytes() > 0) {
         defaultExclusiveQueue = BufferHelper.readNullableBoolean(buffer);
      }

      if (buffer.readableBytes() > 0) {
         defaultConsumersBeforeDispatch = BufferHelper.readNullableInteger(buffer);
      }

      if (buffer.readableBytes() > 0) {
         defaultDelayBeforeDispatch = BufferHelper.readNullableLong(buffer);
      }

      if (buffer.readableBytes() > 0) {
         defaultConsumerWindowSize = BufferHelper.readNullableInteger(buffer);
      }

      if (buffer.readableBytes() > 0) {
         defaultLastValueKey = buffer.readNullableSimpleString();
      }

      if (buffer.readableBytes() > 0) {
         defaultNonDestructive = BufferHelper.readNullableBoolean(buffer);
      }

      if (buffer.readableBytes() > 0) {
         autoDeleteQueuesDelay = BufferHelper.readNullableLong(buffer);
      }

      if (buffer.readableBytes() > 0) {
         autoDeleteAddressesDelay = BufferHelper.readNullableLong(buffer);
      }

      if (buffer.readableBytes() > 0) {
         defaultGroupRebalance = BufferHelper.readNullableBoolean(buffer);
      }

      if (buffer.readableBytes() > 0) {
         defaultGroupBuckets = BufferHelper.readNullableInteger(buffer);
      }

      if (buffer.readableBytes() > 0) {
         autoDeleteQueuesMessageCount = BufferHelper.readNullableLong(buffer);
      }

      if (buffer.readableBytes() > 0) {
         autoDeleteCreatedQueues = BufferHelper.readNullableBoolean(buffer);
      }

      if (buffer.readableBytes() > 0) {
         defaultRingSize = BufferHelper.readNullableLong(buffer);
      }

      if (buffer.readableBytes() > 0) {
         redeliveryCollisionAvoidanceFactor = BufferHelper.readNullableDouble(buffer);
      }

      if (buffer.readableBytes() > 0) {
         defaultGroupFirstKey = buffer.readNullableSimpleString();
      }

      if (buffer.readableBytes() > 0) {
         retroactiveMessageCount = BufferHelper.readNullableLong(buffer);
      }

      if (buffer.readableBytes() > 0) {
         autoCreateDeadLetterResources = BufferHelper.readNullableBoolean(buffer);
      }

      if (buffer.readableBytes() > 0) {
         deadLetterQueuePrefix = buffer.readNullableSimpleString();
      }

      if (buffer.readableBytes() > 0) {
         deadLetterQueueSuffix = buffer.readNullableSimpleString();
      }

      if (buffer.readableBytes() > 0) {
         autoCreateExpiryResources = BufferHelper.readNullableBoolean(buffer);
      }

      if (buffer.readableBytes() > 0) {
         expiryQueuePrefix = buffer.readNullableSimpleString();
      }

      if (buffer.readableBytes() > 0) {
         expiryQueueSuffix = buffer.readNullableSimpleString();
      }

      if (buffer.readableBytes() > 0) {
         minExpiryDelay = BufferHelper.readNullableLong(buffer);
      }

      if (buffer.readableBytes() > 0) {
         maxExpiryDelay = BufferHelper.readNullableLong(buffer);
      }

      if (buffer.readableBytes() > 0) {
         enableMetrics = BufferHelper.readNullableBoolean(buffer);
      }

      if (buffer.readableBytes() > 0) {
         defaultGroupRebalancePauseDispatch = BufferHelper.readNullableBoolean(buffer);
      }

      if (buffer.readableBytes() > 0) {
         managementMessageAttributeSizeLimit = BufferHelper.readNullableInteger(buffer);
      }

      if (buffer.readableBytes() > 0) {
         Integer slowConsumerMeasurementUnitEnumValue = BufferHelper.readNullableInteger(buffer);
         if (slowConsumerMeasurementUnitEnumValue != null) {
            slowConsumerThresholdMeasurementUnit = SlowConsumerThresholdMeasurementUnit.valueOf(slowConsumerMeasurementUnitEnumValue);
         }
      }

      if (buffer.readableBytes() > 0) {
         enableIngressTimestamp = BufferHelper.readNullableBoolean(buffer);
      }

      if (buffer.readableBytes() > 0) {
         policyStr = tryCompatible ? null : buffer.readNullableSimpleString();

         if (policyStr != null) {
            configDeleteDiverts = DeletionPolicy.valueOf(policyStr.toString());
         } else {
            configDeleteDiverts = null;
         }
      }

      if (buffer.readableBytes() > 0) {
         maxSizeMessages = BufferHelper.readNullableLong(buffer);
      }

      if (buffer.readableBytes() > 0) {
         maxReadPageBytes = BufferHelper.readNullableInteger(buffer);
      }

      if (buffer.readableBytes() > 0) {
         maxReadPageMessages = BufferHelper.readNullableInteger(buffer);
      }

      if (buffer.readableBytes() > 0) {
         pageLimitBytes = BufferHelper.readNullableLong(buffer);
      }

      if (buffer.readableBytes() > 0) {
         pageLimitMessages = BufferHelper.readNullableLong(buffer);
      }

      if (buffer.readableBytes() > 0) {
         policyStr = buffer.readNullableSimpleString();

         if (policyStr != null) {
            pageFullMessagePolicy = PageFullMessagePolicy.valueOf(policyStr.toString());
         } else {
            pageFullMessagePolicy = null;
         }
      }

      if (buffer.readableBytes() > 0) {
         autoDeleteQueuesSkipUsageCheck = BufferHelper.readNullableBoolean(buffer);
      }

      if (buffer.readableBytes() > 0) {
         autoDeleteAddressesSkipUsageCheck = BufferHelper.readNullableBoolean(buffer);
      }

      if (buffer.readableBytes() > 0) {
         idCacheSize = BufferHelper.readNullableInteger(buffer);
      }

      if (buffer.readableBytes() > 0) {
         prefetchPageBytes = BufferHelper.readNullableInteger(buffer);
      }

      if (buffer.readableBytes() > 0) {
         prefetchPageMessages = BufferHelper.readNullableInteger(buffer);
      }

      // WARNING: no more additions, this method is deprecated, any current persist usage should be in JSON format
      //          This method serves the purpose of loading older records, but any new records should be on the new format
   }

   @Override
   public int getEncodeSize() {
      ////// this method is no longer in use, any new usage of encoding an AddressSetting should be through its JSON format

      throw new UnsupportedOperationException("Encode of AddressSettings is no longer supported, please use JSON method and PersistAddressSettingJSON");
   }

   @Override
   public void encode(ActiveMQBuffer buffer) {
      ////// this method is no longer in use, any new usage of encoding an AddressSetting should be through its JSON format

      throw new UnsupportedOperationException("Encode of AddressSettings is no longer supported, please use JSON method and PersistAddressSettingJSON");
   }

   @Override
   public boolean equals(Object o) {
      if (this == o)
         return true;
      if (o == null || getClass() != o.getClass())
         return false;

      AddressSettings that = (AddressSettings) o;

      if (addressFullMessagePolicy != that.addressFullMessagePolicy)
         return false;
      if (!Objects.equals(maxSizeBytes, that.maxSizeBytes))
         return false;
      if (!Objects.equals(maxReadPageBytes, that.maxReadPageBytes))
         return false;
      if (!Objects.equals(maxReadPageMessages, that.maxReadPageMessages))
         return false;
      if (!Objects.equals(prefetchPageBytes, that.prefetchPageBytes))
         return false;
      if (!Objects.equals(prefetchPageMessages, that.prefetchPageMessages))
         return false;
      if (!Objects.equals(pageLimitBytes, that.pageLimitBytes))
         return false;
      if (!Objects.equals(pageLimitMessages, that.pageLimitMessages))
         return false;
      if (pageFullMessagePolicy != that.pageFullMessagePolicy)
         return false;
      if (!Objects.equals(maxSizeMessages, that.maxSizeMessages))
         return false;
      if (!Objects.equals(pageSizeBytes, that.pageSizeBytes))
         return false;
      if (!Objects.equals(pageCacheMaxSize, that.pageCacheMaxSize))
         return false;
      if (!Objects.equals(dropMessagesWhenFull, that.dropMessagesWhenFull))
         return false;
      if (!Objects.equals(maxDeliveryAttempts, that.maxDeliveryAttempts))
         return false;
      if (!Objects.equals(messageCounterHistoryDayLimit, that.messageCounterHistoryDayLimit))
         return false;
      if (!Objects.equals(redeliveryDelay, that.redeliveryDelay))
         return false;
      if (!Objects.equals(redeliveryMultiplier, that.redeliveryMultiplier))
         return false;
      if (!Objects.equals(redeliveryCollisionAvoidanceFactor, that.redeliveryCollisionAvoidanceFactor))
         return false;
      if (!Objects.equals(maxRedeliveryDelay, that.maxRedeliveryDelay))
         return false;
      if (!Objects.equals(deadLetterAddress, that.deadLetterAddress))
         return false;
      if (!Objects.equals(expiryAddress, that.expiryAddress))
         return false;
      if (!Objects.equals(expiryDelay, that.expiryDelay))
         return false;
      if (!Objects.equals(minExpiryDelay, that.minExpiryDelay))
         return false;
      if (!Objects.equals(maxExpiryDelay, that.maxExpiryDelay))
         return false;
      if (!Objects.equals(defaultLastValueQueue, that.defaultLastValueQueue))
         return false;
      if (!Objects.equals(defaultLastValueKey, that.defaultLastValueKey))
         return false;
      if (!Objects.equals(defaultNonDestructive, that.defaultNonDestructive))
         return false;
      if (!Objects.equals(defaultExclusiveQueue, that.defaultExclusiveQueue))
         return false;
      if (!Objects.equals(defaultGroupRebalance, that.defaultGroupRebalance))
         return false;
      if (!Objects.equals(defaultGroupRebalancePauseDispatch, that.defaultGroupRebalancePauseDispatch))
         return false;
      if (!Objects.equals(defaultGroupBuckets, that.defaultGroupBuckets))
         return false;
      if (!Objects.equals(defaultGroupFirstKey, that.defaultGroupFirstKey))
         return false;
      if (!Objects.equals(redistributionDelay, that.redistributionDelay))
         return false;
      if (!Objects.equals(sendToDLAOnNoRoute, that.sendToDLAOnNoRoute))
         return false;
      if (!Objects.equals(slowConsumerThreshold, that.slowConsumerThreshold))
         return false;
      if (slowConsumerThresholdMeasurementUnit != that.slowConsumerThresholdMeasurementUnit)
         return false;
      if (!Objects.equals(slowConsumerCheckPeriod, that.slowConsumerCheckPeriod))
         return false;
      if (slowConsumerPolicy != that.slowConsumerPolicy)
         return false;
      if (!Objects.equals(autoCreateJmsQueues, that.autoCreateJmsQueues))
         return false;
      if (!Objects.equals(autoDeleteJmsQueues, that.autoDeleteJmsQueues))
         return false;
      if (!Objects.equals(autoCreateJmsTopics, that.autoCreateJmsTopics))
         return false;
      if (!Objects.equals(autoDeleteJmsTopics, that.autoDeleteJmsTopics))
         return false;
      if (!Objects.equals(autoCreateQueues, that.autoCreateQueues))
         return false;
      if (!Objects.equals(autoDeleteQueues, that.autoDeleteQueues))
         return false;
      if (!Objects.equals(autoDeleteCreatedQueues, that.autoDeleteCreatedQueues))
         return false;
      if (!Objects.equals(autoDeleteQueuesDelay, that.autoDeleteQueuesDelay))
         return false;
      if (!Objects.equals(autoDeleteQueuesSkipUsageCheck, that.autoDeleteQueuesSkipUsageCheck))
         return false;
      if (!Objects.equals(autoDeleteQueuesMessageCount, that.autoDeleteQueuesMessageCount))
         return false;
      if (!Objects.equals(defaultRingSize, that.defaultRingSize))
         return false;
      if (!Objects.equals(retroactiveMessageCount, that.retroactiveMessageCount))
         return false;
      if (configDeleteQueues != that.configDeleteQueues)
         return false;
      if (!Objects.equals(autoCreateAddresses, that.autoCreateAddresses))
         return false;
      if (!Objects.equals(autoDeleteAddresses, that.autoDeleteAddresses))
         return false;
      if (!Objects.equals(autoDeleteAddressesDelay, that.autoDeleteAddressesDelay))
         return false;
      if (!Objects.equals(autoDeleteAddressesSkipUsageCheck, that.autoDeleteAddressesSkipUsageCheck))
         return false;
      if (configDeleteAddresses != that.configDeleteAddresses)
         return false;
      if (configDeleteDiverts != that.configDeleteDiverts)
         return false;
      if (!Objects.equals(managementBrowsePageSize, that.managementBrowsePageSize))
         return false;
      if (!Objects.equals(maxSizeBytesRejectThreshold, that.maxSizeBytesRejectThreshold))
         return false;
      if (!Objects.equals(defaultMaxConsumers, that.defaultMaxConsumers))
         return false;
      if (!Objects.equals(defaultPurgeOnNoConsumers, that.defaultPurgeOnNoConsumers))
         return false;
      if (!Objects.equals(defaultConsumersBeforeDispatch, that.defaultConsumersBeforeDispatch))
         return false;
      if (!Objects.equals(defaultDelayBeforeDispatch, that.defaultDelayBeforeDispatch))
         return false;
      if (defaultQueueRoutingType != that.defaultQueueRoutingType)
         return false;
      if (defaultAddressRoutingType != that.defaultAddressRoutingType)
         return false;
      if (!Objects.equals(defaultConsumerWindowSize, that.defaultConsumerWindowSize))
         return false;
      if (!Objects.equals(autoCreateDeadLetterResources, that.autoCreateDeadLetterResources))
         return false;
      if (!Objects.equals(deadLetterQueuePrefix, that.deadLetterQueuePrefix))
         return false;
      if (!Objects.equals(deadLetterQueueSuffix, that.deadLetterQueueSuffix))
         return false;
      if (!Objects.equals(autoCreateExpiryResources, that.autoCreateExpiryResources))
         return false;
      if (!Objects.equals(expiryQueuePrefix, that.expiryQueuePrefix))
         return false;
      if (!Objects.equals(expiryQueueSuffix, that.expiryQueueSuffix))
         return false;
      if (!Objects.equals(enableMetrics, that.enableMetrics))
         return false;
      if (!Objects.equals(managementMessageAttributeSizeLimit, that.managementMessageAttributeSizeLimit))
         return false;
      if (!Objects.equals(enableIngressTimestamp, that.enableIngressTimestamp))
         return false;
      if (!Objects.equals(idCacheSize, that.idCacheSize))
         return false;
      if (!Objects.equals(initialQueueBufferSize, that.initialQueueBufferSize)) {
         return false;
      }
      return Objects.equals(queuePrefetch, that.queuePrefetch);
   }

   @Override
   public int hashCode() {
      int result = addressFullMessagePolicy != null ? addressFullMessagePolicy.hashCode() : 0;
      result = 31 * result + (maxSizeBytes != null ? maxSizeBytes.hashCode() : 0);
      result = 31 * result + (maxReadPageBytes != null ? maxReadPageBytes.hashCode() : 0);
      result = 31 * result + (maxReadPageMessages != null ? maxReadPageMessages.hashCode() : 0);
      result = 31 * result + (prefetchPageBytes != null ? prefetchPageBytes.hashCode() : 0);
      result = 31 * result + (prefetchPageMessages != null ? prefetchPageMessages.hashCode() : 0);
      result = 31 * result + (pageLimitBytes != null ? pageLimitBytes.hashCode() : 0);
      result = 31 * result + (pageLimitMessages != null ? pageLimitMessages.hashCode() : 0);
      result = 31 * result + (pageFullMessagePolicy != null ? pageFullMessagePolicy.hashCode() : 0);
      result = 31 * result + (maxSizeMessages != null ? maxSizeMessages.hashCode() : 0);
      result = 31 * result + (pageSizeBytes != null ? pageSizeBytes.hashCode() : 0);
      result = 31 * result + (pageCacheMaxSize != null ? pageCacheMaxSize.hashCode() : 0);
      result = 31 * result + (dropMessagesWhenFull != null ? dropMessagesWhenFull.hashCode() : 0);
      result = 31 * result + (maxDeliveryAttempts != null ? maxDeliveryAttempts.hashCode() : 0);
      result = 31 * result + (messageCounterHistoryDayLimit != null ? messageCounterHistoryDayLimit.hashCode() : 0);
      result = 31 * result + (redeliveryDelay != null ? redeliveryDelay.hashCode() : 0);
      result = 31 * result + (redeliveryMultiplier != null ? redeliveryMultiplier.hashCode() : 0);
      result = 31 * result + (redeliveryCollisionAvoidanceFactor != null ? redeliveryCollisionAvoidanceFactor.hashCode() : 0);
      result = 31 * result + (maxRedeliveryDelay != null ? maxRedeliveryDelay.hashCode() : 0);
      result = 31 * result + (deadLetterAddress != null ? deadLetterAddress.hashCode() : 0);
      result = 31 * result + (expiryAddress != null ? expiryAddress.hashCode() : 0);
      result = 31 * result + (expiryDelay != null ? expiryDelay.hashCode() : 0);
      result = 31 * result + (minExpiryDelay != null ? minExpiryDelay.hashCode() : 0);
      result = 31 * result + (maxExpiryDelay != null ? maxExpiryDelay.hashCode() : 0);
      result = 31 * result + (defaultLastValueQueue != null ? defaultLastValueQueue.hashCode() : 0);
      result = 31 * result + (defaultLastValueKey != null ? defaultLastValueKey.hashCode() : 0);
      result = 31 * result + (defaultNonDestructive != null ? defaultNonDestructive.hashCode() : 0);
      result = 31 * result + (defaultExclusiveQueue != null ? defaultExclusiveQueue.hashCode() : 0);
      result = 31 * result + (defaultGroupRebalance != null ? defaultGroupRebalance.hashCode() : 0);
      result = 31 * result + (defaultGroupRebalancePauseDispatch != null ? defaultGroupRebalancePauseDispatch.hashCode() : 0);
      result = 31 * result + (defaultGroupBuckets != null ? defaultGroupBuckets.hashCode() : 0);
      result = 31 * result + (defaultGroupFirstKey != null ? defaultGroupFirstKey.hashCode() : 0);
      result = 31 * result + (redistributionDelay != null ? redistributionDelay.hashCode() : 0);
      result = 31 * result + (sendToDLAOnNoRoute != null ? sendToDLAOnNoRoute.hashCode() : 0);
      result = 31 * result + (slowConsumerThreshold != null ? slowConsumerThreshold.hashCode() : 0);
      result = 31 * result + (slowConsumerThresholdMeasurementUnit != null ? slowConsumerThresholdMeasurementUnit.hashCode() : 0);
      result = 31 * result + (slowConsumerCheckPeriod != null ? slowConsumerCheckPeriod.hashCode() : 0);
      result = 31 * result + (slowConsumerPolicy != null ? slowConsumerPolicy.hashCode() : 0);
      result = 31 * result + (autoCreateJmsQueues != null ? autoCreateJmsQueues.hashCode() : 0);
      result = 31 * result + (autoDeleteJmsQueues != null ? autoDeleteJmsQueues.hashCode() : 0);
      result = 31 * result + (autoCreateJmsTopics != null ? autoCreateJmsTopics.hashCode() : 0);
      result = 31 * result + (autoDeleteJmsTopics != null ? autoDeleteJmsTopics.hashCode() : 0);
      result = 31 * result + (autoCreateQueues != null ? autoCreateQueues.hashCode() : 0);
      result = 31 * result + (autoDeleteQueues != null ? autoDeleteQueues.hashCode() : 0);
      result = 31 * result + (autoDeleteCreatedQueues != null ? autoDeleteCreatedQueues.hashCode() : 0);
      result = 31 * result + (autoDeleteQueuesDelay != null ? autoDeleteQueuesDelay.hashCode() : 0);
      result = 31 * result + (autoDeleteQueuesSkipUsageCheck != null ? autoDeleteQueuesSkipUsageCheck.hashCode() : 0);
      result = 31 * result + (autoDeleteQueuesMessageCount != null ? autoDeleteQueuesMessageCount.hashCode() : 0);
      result = 31 * result + (defaultRingSize != null ? defaultRingSize.hashCode() : 0);
      result = 31 * result + (retroactiveMessageCount != null ? retroactiveMessageCount.hashCode() : 0);
      result = 31 * result + (configDeleteQueues != null ? configDeleteQueues.hashCode() : 0);
      result = 31 * result + (autoCreateAddresses != null ? autoCreateAddresses.hashCode() : 0);
      result = 31 * result + (autoDeleteAddresses != null ? autoDeleteAddresses.hashCode() : 0);
      result = 31 * result + (autoDeleteAddressesDelay != null ? autoDeleteAddressesDelay.hashCode() : 0);
      result = 31 * result + (autoDeleteAddressesSkipUsageCheck != null ? autoDeleteAddressesSkipUsageCheck.hashCode() : 0);
      result = 31 * result + (configDeleteAddresses != null ? configDeleteAddresses.hashCode() : 0);
      result = 31 * result + (configDeleteDiverts != null ? configDeleteDiverts.hashCode() : 0);
      result = 31 * result + (managementBrowsePageSize != null ? managementBrowsePageSize.hashCode() : 0);
      result = 31 * result + (maxSizeBytesRejectThreshold != null ? maxSizeBytesRejectThreshold.hashCode() : 0);
      result = 31 * result + (defaultMaxConsumers != null ? defaultMaxConsumers.hashCode() : 0);
      result = 31 * result + (defaultPurgeOnNoConsumers != null ? defaultPurgeOnNoConsumers.hashCode() : 0);
      result = 31 * result + (defaultConsumersBeforeDispatch != null ? defaultConsumersBeforeDispatch.hashCode() : 0);
      result = 31 * result + (defaultDelayBeforeDispatch != null ? defaultDelayBeforeDispatch.hashCode() : 0);
      result = 31 * result + (defaultQueueRoutingType != null ? defaultQueueRoutingType.hashCode() : 0);
      result = 31 * result + (defaultAddressRoutingType != null ? defaultAddressRoutingType.hashCode() : 0);
      result = 31 * result + (defaultConsumerWindowSize != null ? defaultConsumerWindowSize.hashCode() : 0);
      result = 31 * result + (autoCreateDeadLetterResources != null ? autoCreateDeadLetterResources.hashCode() : 0);
      result = 31 * result + (deadLetterQueuePrefix != null ? deadLetterQueuePrefix.hashCode() : 0);
      result = 31 * result + (deadLetterQueueSuffix != null ? deadLetterQueueSuffix.hashCode() : 0);
      result = 31 * result + (autoCreateExpiryResources != null ? autoCreateExpiryResources.hashCode() : 0);
      result = 31 * result + (expiryQueuePrefix != null ? expiryQueuePrefix.hashCode() : 0);
      result = 31 * result + (expiryQueueSuffix != null ? expiryQueueSuffix.hashCode() : 0);
      result = 31 * result + (enableMetrics != null ? enableMetrics.hashCode() : 0);
      result = 31 * result + (managementMessageAttributeSizeLimit != null ? managementMessageAttributeSizeLimit.hashCode() : 0);
      result = 31 * result + (enableIngressTimestamp != null ? enableIngressTimestamp.hashCode() : 0);
      result = 31 * result + (idCacheSize != null ? idCacheSize.hashCode() : 0);
      result = 31 * result + (queuePrefetch != null ? queuePrefetch.hashCode() : 0);
      result = 31 * result + (initialQueueBufferSize != null ? initialQueueBufferSize.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "AddressSettings{" + "addressFullMessagePolicy=" + addressFullMessagePolicy + ", maxSizeBytes=" + maxSizeBytes + ", maxReadPageBytes=" + maxReadPageBytes + ", maxReadPageMessages=" + maxReadPageMessages + ", prefetchPageBytes=" + prefetchPageBytes + ", prefetchPageMessages=" + prefetchPageMessages + ", pageLimitBytes=" + pageLimitBytes + ", pageLimitMessages=" + pageLimitMessages + ", pageFullMessagePolicy=" + pageFullMessagePolicy + ", maxSizeMessages=" + maxSizeMessages + ", pageSizeBytes=" + pageSizeBytes + ", pageMaxCache=" + pageCacheMaxSize + ", dropMessagesWhenFull=" + dropMessagesWhenFull + ", maxDeliveryAttempts=" + maxDeliveryAttempts + ", messageCounterHistoryDayLimit=" + messageCounterHistoryDayLimit + ", redeliveryDelay=" + redeliveryDelay + ", redeliveryMultiplier=" + redeliveryMultiplier + ", redeliveryCollisionAvoidanceFactor=" + redeliveryCollisionAvoidanceFactor + ", maxRedeliveryDelay=" + maxRedeliveryDelay + ", deadLetterAddress=" + deadLetterAddress + ", expiryAddress=" + expiryAddress + ", expiryDelay=" + expiryDelay + ", minExpiryDelay=" + minExpiryDelay + ", maxExpiryDelay=" + maxExpiryDelay + ", defaultLastValueQueue=" + defaultLastValueQueue + ", defaultLastValueKey=" + defaultLastValueKey + ", defaultNonDestructive=" + defaultNonDestructive + ", defaultExclusiveQueue=" + defaultExclusiveQueue + ", defaultGroupRebalance=" + defaultGroupRebalance + ", defaultGroupRebalancePauseDispatch=" + defaultGroupRebalancePauseDispatch + ", defaultGroupBuckets=" + defaultGroupBuckets + ", defaultGroupFirstKey=" + defaultGroupFirstKey + ", redistributionDelay=" + redistributionDelay + ", sendToDLAOnNoRoute=" + sendToDLAOnNoRoute + ", slowConsumerThreshold=" + slowConsumerThreshold + ", slowConsumerThresholdMeasurementUnit=" + slowConsumerThresholdMeasurementUnit + ", slowConsumerCheckPeriod=" + slowConsumerCheckPeriod + ", slowConsumerPolicy=" + slowConsumerPolicy + ", autoCreateJmsQueues=" + autoCreateJmsQueues + ", autoDeleteJmsQueues=" + autoDeleteJmsQueues + ", autoCreateJmsTopics=" + autoCreateJmsTopics + ", autoDeleteJmsTopics=" + autoDeleteJmsTopics + ", autoCreateQueues=" + autoCreateQueues + ", autoDeleteQueues=" + autoDeleteQueues + ", autoDeleteCreatedQueues=" + autoDeleteCreatedQueues + ", autoDeleteQueuesDelay=" + autoDeleteQueuesDelay + ", autoDeleteQueuesSkipUsageCheck=" + autoDeleteQueuesSkipUsageCheck + ", autoDeleteQueuesMessageCount=" + autoDeleteQueuesMessageCount + ", defaultRingSize=" + defaultRingSize + ", retroactiveMessageCount=" + retroactiveMessageCount + ", configDeleteQueues=" + configDeleteQueues + ", autoCreateAddresses=" + autoCreateAddresses + ", autoDeleteAddresses=" + autoDeleteAddresses + ", autoDeleteAddressesDelay=" + autoDeleteAddressesDelay + ", autoDeleteAddressesSkipUsageCheck=" + autoDeleteAddressesSkipUsageCheck + ", configDeleteAddresses=" + configDeleteAddresses + ", configDeleteDiverts=" + configDeleteDiverts + ", managementBrowsePageSize=" + managementBrowsePageSize + ", maxSizeBytesRejectThreshold=" + maxSizeBytesRejectThreshold + ", defaultMaxConsumers=" + defaultMaxConsumers + ", defaultPurgeOnNoConsumers=" + defaultPurgeOnNoConsumers + ", defaultConsumersBeforeDispatch=" + defaultConsumersBeforeDispatch + ", defaultDelayBeforeDispatch=" + defaultDelayBeforeDispatch + ", defaultQueueRoutingType=" + defaultQueueRoutingType + ", defaultAddressRoutingType=" + defaultAddressRoutingType + ", defaultConsumerWindowSize=" + defaultConsumerWindowSize + ", autoCreateDeadLetterResources=" + autoCreateDeadLetterResources + ", deadLetterQueuePrefix=" + deadLetterQueuePrefix + ", deadLetterQueueSuffix=" + deadLetterQueueSuffix + ", autoCreateExpiryResources=" + autoCreateExpiryResources + ", expiryQueuePrefix=" + expiryQueuePrefix + ", expiryQueueSuffix=" + expiryQueueSuffix + ", enableMetrics=" + enableMetrics + ", managementMessageAttributeSizeLimit=" + managementMessageAttributeSizeLimit + ", enableIngressTimestamp=" + enableIngressTimestamp + ", idCacheSize=" + idCacheSize + ", queuePrefetch=" + queuePrefetch + ", initialQueueBufferSize=" + initialQueueBufferSize
             + '}';
   }
}
