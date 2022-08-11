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
import java.io.StringReader;
import java.util.Map;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.core.journal.EncodingSupport;
import org.apache.activemq.artemis.core.settings.Mergeable;
import org.apache.activemq.artemis.json.JsonObject;
import org.apache.activemq.artemis.json.JsonObjectBuilder;
import org.apache.activemq.artemis.json.JsonString;
import org.apache.activemq.artemis.json.JsonValue;
import org.apache.activemq.artemis.utils.BufferHelper;
import org.apache.activemq.artemis.utils.DataConstants;
import org.apache.activemq.artemis.utils.JsonLoader;

/**
 * Configuration settings that are applied on the address level
 */
public class AddressSettings implements Mergeable<AddressSettings>, Serializable, EncodingSupport {

   private static final long serialVersionUID = 1607502280582336366L;

   public static final String ADDRESS_FULL_MESSAGE_POLICY = "address-full-message-policy";

   public static final String MAX_SIZE_BYTES = "max-size-bytes";

   public static final String MAX_READ_PAGE_BYTES = "max-size-page-bytes";

   public static final String MAX_READ_PAGE_MESSAGES = "max-read-page-messages";

   public static final String MAX_SIZE_MESSAGES = "max-size-messages";

   public static final String PAGE_SIZE_BYTES = "page-size-bytes";

   public static final String PAGE_MAX_CACHE_SIZE = "page-max-cache-size";

   public static final String DROP_MESSAGES_WHEN_FULL = "drop-messages-when-full";

   public static final String MAX_DELIVERY_ATTEMPTS = "max-delivery-attempts";

   public static final String MESSAGE_COUNTER_HISTORY_DAY_LIMIT = "message-counter-history-day-limit";

   public static final String REDELIVERY_DELAY = "redelivery-delay";

   public static final String REDELIVERY_DELAY_MULTIPLIER = "redelivery-delay-multiplier";

   public static final String REDELIVERY_COLLISION_AVOIDANCE_FACTOR = "redelivery-collision-avoidance-factor";

   public static final String MAX_REDELIVERY_DELAY = "max-redelivery-delay";

   public static final String DEAD_LETTER_ADDRESS = "dead-letter-address";

   public static final String EXPIRY_ADDRESS = "expiry-address";

   public static final String EXPIRY_DELAY = "expiry-delay";

   public static final String MIN_EXPIRY_DELAY = "min-expiry-delay";

   public static final String MAX_EXPIRY_DELAY = "max-expiry-delay";

   public static final String DEFAULT_LAST_VALUE_QUEUE_PROP = "default-last-value-queue";

   public static final String DEFAULT_LAST_VALUE_KEY = "default-last-value-key";

   public static final String DEFAULT_NON_DESTRUCTIVE = "default-non-destructive";

   public static final String DEFAULT_EXCLUSIVE_QUEUE = "default-exclusive-queue";

   public static final String DEFAULT_GROUP_REBALANCE = "default-group-rebalance";

   public static final String DEFAULT_GROUP_REBALANCE_PAUSE_DISPATCH = "default-group-rebalance-pause-dispatch";

   public static final String DEFAULT_GROUP_BUCKETS = "default-group-buckets";

   public static final String DEFAULT_GROUP_FIRST_KEY = "default-group-first-key";

   public static final String REDISTRIBUTION_DELAY = "redistribution-delay";

   public static final String SEND_TO_DLA_ON_NO_ROUTE = "send-to-dla-on-no-route";

   public static final String SLOW_CONSUMER_THRESHOLD = "slow-consumer-threshold";

   public static final String SLOW_CONSUMER_THRESHOLD_MEASUREMENT_UNIT = "slow-consumer-threshold-measurement-unit";

   public static final String SLOW_CONSUMER_CHECK_PERIOD = "slow-consumer-check-period";

   public static final String SLOW_CONSUMER_POLICY = "slow-consumer-policy";

   public static final String AUTO_CREATE_QUEUES = "auto-create-queues";

   public static final String AUTO_DELETE_QUEUES = "auto-delete-queues";

   public static final String AUTO_DELETE_CREATED_QUEUES = "auto-delete-created-queues";

   public static final String AUTO_DELETE_QUEUES_DELAY = "auto-delete-queues-delay";

   public static final String AUTO_DELETE_QUEUE_MESSAGE_COUNT = "auto-delete-queues-message-count";

   public static final String DEFAULT_RING_SIZE = "default-ring-size";

   public static final String RETROACTIVE_MESSAGE_COUNT = "retroactive-message-count";

   public static final String CONFIG_DELETE_QUEUES = "config-delete-queues";

   public static final String AUTO_CREATE_ADDRESSES = "auto-create-addresses";

   public static final String AUTO_DELETE_ADDRESSES = "auto-delete-addresses";

   public static final String AUTO_DELETE_ADDRESS_DELAY = "auto-delete-address-delay";

   public static final String CONFIG_DELETE_ADDRESSES = "config-delete-addresses";

   public static final String CONFIG_DELETE_DIVERTS = "config-delete-diverts";

   public static final String MANAGEMENT_BROWSE_PAGE_SIZE_PROP = "management-browse-page-size";

   public static final String MAX_SIZE_BYTES_REJECT_THRESHOLD = "max-size-bytes-reject-threshold";

   public static final String DEFAULT_MAX_CONSUMERS = "default-max-consumers";

   public static final String DEFAULT_PURGE_ON_NO_CONSUMERS = "default-purge-on-no-consumers";

   public static final String DEFAULT_CONSUMERS_BEFORE_DISPATCH = "default-consumers-before-dispatch";

   public static final String DEFAULT_DELAY_BEFORE_DISPATCH = "default-delay-before-dispatch";

   public static final String DEFAULT_QUEUE_ROUTING_TYPE = "default-queue-routing-type";

   public static final String DEFAULT_ADDRESS_ROUTING_TYPE = "default-address-routing-type";

   public static final String DEFAULT_CONSUMER_WINDOW_SIZE_PROP = "default-consumer-window-size";

   public static final String AUTO_CREATE_DEAD_LETTER_RESOURCES = "auto-create-dead-letter-resources";

   public static final String DEAD_LETTER_QUEUE_PREFIX = "dead-letter-queue-prefix";

   public static final String DEAD_LETTER_QUEUE_SUFFIX = "dead-letter-queue-suffix";

   public static final String AUTO_CREATE_EXPIRY_RESOURCES = "auto-create-expiry-resources";

   public static final String EXPIRY_QUEUE_PREFIX = "expiry-queue-prefix";

   public static final String EXPIRY_QUEUE_SUFFIX = "expiry-address-suffix";

   public static final String ENABLE_METRICS = "enable-metrics";

   public static final String MANAGEMENT_MESSAGE_ATTRIBUTE_SIZE_LIMIT_PROP = "management-message-attribute-size-limit";

   public static final String  ENABLE_INGRESS_TIMESTAMP = "enable-ingress-timestamp";

   public static final String  QUEUE_PREFETCH = "queue-prefetch";

   public static final String  AUTO_DELETE_QUEUES_SKIP_USAGE_CHECK = "auto-delete-queues-skip-usage-check";

   public static final String  AUTO_DELETE_ADDRESSES_SKIP_USAGE_CHECK = "auto-delete-addresses-skip-usage-check";

   public static final String  PREFETCH_PAGE_MESSAGES = "prefetch-page-messages";

   public static final String  PAGE_LIMIT_BYTES = "page-limit-bytes";

   public static final String  PAGE_LIMIT_MESSAGES = "page-limit-messages";

   public static final String  PAGE_FULL_MESSAGE_POLICY = "page-full-message-policy";

   public static final String  PREFETCH_PAGE_BYTES = "prefetch-page-bytes";

   public static final String  ID_CACHE_SIZE = "id-cache-size";

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

   public static final SimpleString DEFAULT_EXPIRY_QUEUE_PREFIX = SimpleString.toSimpleString("EXP.");

   public static final SimpleString DEFAULT_EXPIRY_QUEUE_SUFFIX = SimpleString.toSimpleString("");

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

   public static final SimpleString DEFAULT_DEAD_LETTER_QUEUE_PREFIX = SimpleString.toSimpleString("DLQ.");

   public static final SimpleString DEFAULT_DEAD_LETTER_QUEUE_SUFFIX = SimpleString.toSimpleString("");

   public static final boolean DEFAULT_ENABLE_METRICS = true;

   public static final int MANAGEMENT_MESSAGE_ATTRIBUTE_SIZE_LIMIT = 256;

   public static final SlowConsumerThresholdMeasurementUnit DEFAULT_SLOW_CONSUMER_THRESHOLD_MEASUREMENT_UNIT = SlowConsumerThresholdMeasurementUnit.MESSAGES_PER_SECOND;

   public static final boolean DEFAULT_ENABLE_INGRESS_TIMESTAMP = false;

   private AddressFullMessagePolicy addressFullMessagePolicy = null;

   private Long maxSizeBytes = null;

   private Integer maxReadPageBytes = null;

   private Integer maxReadPageMessages = null;

   private Integer prefetchPageBytes = null;

   private Integer prefetchPageMessages = null;

   private Long pageLimitBytes = null;

   private Long pageLimitMessages = null;

   private PageFullMessagePolicy pageFullMessagePolicy = null;

   private Long maxSizeMessages = null;

   private Integer pageSizeBytes = null;

   private Integer pageMaxCache = null;

   private Boolean dropMessagesWhenFull = null;

   private Integer maxDeliveryAttempts = null;

   private Integer messageCounterHistoryDayLimit = null;

   private Long redeliveryDelay = null;

   private Double redeliveryMultiplier = null;

   private Double redeliveryCollisionAvoidanceFactor = null;

   private Long maxRedeliveryDelay = null;

   private SimpleString deadLetterAddress = null;

   private SimpleString expiryAddress = null;

   private Long expiryDelay = null;

   private Long minExpiryDelay = null;

   private Long maxExpiryDelay = null;

   private Boolean defaultLastValueQueue = null;

   private SimpleString defaultLastValueKey = null;

   private Boolean defaultNonDestructive = null;

   private Boolean defaultExclusiveQueue = null;

   private Boolean defaultGroupRebalance = null;

   private Boolean defaultGroupRebalancePauseDispatch = null;

   private Integer defaultGroupBuckets = null;

   private SimpleString defaultGroupFirstKey = null;

   private Long redistributionDelay = null;

   private Boolean sendToDLAOnNoRoute = null;

   private Long slowConsumerThreshold = null;

   private SlowConsumerThresholdMeasurementUnit slowConsumerThresholdMeasurementUnit = DEFAULT_SLOW_CONSUMER_THRESHOLD_MEASUREMENT_UNIT;

   private Long slowConsumerCheckPeriod = null;

   private SlowConsumerPolicy slowConsumerPolicy = null;

   @Deprecated
   private Boolean autoCreateJmsQueues = null;

   @Deprecated
   private Boolean autoDeleteJmsQueues = null;

   @Deprecated
   private Boolean autoCreateJmsTopics = null;

   @Deprecated
   private Boolean autoDeleteJmsTopics = null;

   private Boolean autoCreateQueues = null;

   private Boolean autoDeleteQueues = null;

   private Boolean autoDeleteCreatedQueues = null;

   private Long autoDeleteQueuesDelay = null;

   private Boolean autoDeleteQueuesSkipUsageCheck = null;

   private Long autoDeleteQueuesMessageCount = null;

   private Long defaultRingSize = null;

   private Long retroactiveMessageCount = null;

   private DeletionPolicy configDeleteQueues = null;

   private Boolean autoCreateAddresses = null;

   private Boolean autoDeleteAddresses = null;

   private Long autoDeleteAddressesDelay = null;

   private Boolean autoDeleteAddressesSkipUsageCheck = null;

   private DeletionPolicy configDeleteAddresses = null;

   private DeletionPolicy configDeleteDiverts = null;

   private Integer managementBrowsePageSize = AddressSettings.MANAGEMENT_BROWSE_PAGE_SIZE;

   private Long maxSizeBytesRejectThreshold = null;

   private Integer defaultMaxConsumers = null;

   private Boolean defaultPurgeOnNoConsumers = null;

   private Integer defaultConsumersBeforeDispatch = null;

   private Long defaultDelayBeforeDispatch = null;

   private RoutingType defaultQueueRoutingType = null;

   private RoutingType defaultAddressRoutingType = null;

   private Integer defaultConsumerWindowSize = null;

   private Boolean autoCreateDeadLetterResources = null;

   private SimpleString deadLetterQueuePrefix = null;

   private SimpleString deadLetterQueueSuffix = null;

   private Boolean autoCreateExpiryResources = null;

   private SimpleString expiryQueuePrefix = null;

   private SimpleString expiryQueueSuffix = null;

   private Boolean enableMetrics = null;

   private Integer managementMessageAttributeSizeLimit = null;

   private Boolean enableIngressTimestamp = null;

   private Integer idCacheSize = null;

   //from amq5
   //make it transient
   private transient Integer queuePrefetch = null;

   public AddressSettings(AddressSettings other) {
      this.addressFullMessagePolicy = other.addressFullMessagePolicy;
      this.maxSizeBytes = other.maxSizeBytes;
      this.maxSizeMessages = other.maxSizeMessages;
      this.maxReadPageMessages = other.maxReadPageMessages;
      this.maxReadPageBytes = other.maxReadPageBytes;
      this.pageLimitBytes = other.pageLimitBytes;
      this.pageLimitMessages = other.pageLimitMessages;
      this.pageFullMessagePolicy = other.pageFullMessagePolicy;
      this.pageSizeBytes = other.pageSizeBytes;
      this.pageMaxCache = other.pageMaxCache;
      this.dropMessagesWhenFull = other.dropMessagesWhenFull;
      this.maxDeliveryAttempts = other.maxDeliveryAttempts;
      this.messageCounterHistoryDayLimit = other.messageCounterHistoryDayLimit;
      this.redeliveryDelay = other.redeliveryDelay;
      this.redeliveryMultiplier = other.redeliveryMultiplier;
      this.redeliveryCollisionAvoidanceFactor = other.redeliveryCollisionAvoidanceFactor;
      this.maxRedeliveryDelay = other.maxRedeliveryDelay;
      this.deadLetterAddress = other.deadLetterAddress;
      this.autoCreateDeadLetterResources = other.autoCreateDeadLetterResources;
      this.deadLetterQueuePrefix = other.deadLetterQueuePrefix;
      this.deadLetterQueueSuffix = other.deadLetterQueueSuffix;
      this.expiryAddress = other.expiryAddress;
      this.autoCreateExpiryResources = other.autoCreateExpiryResources;
      this.expiryQueuePrefix = other.expiryQueuePrefix;
      this.expiryQueueSuffix = other.expiryQueueSuffix;
      this.expiryDelay = other.expiryDelay;
      this.minExpiryDelay = other.minExpiryDelay;
      this.maxExpiryDelay = other.maxExpiryDelay;
      this.defaultLastValueQueue = other.defaultLastValueQueue;
      this.defaultLastValueKey = other.defaultLastValueKey;
      this.defaultNonDestructive = other.defaultNonDestructive;
      this.defaultExclusiveQueue = other.defaultExclusiveQueue;
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
      this.autoDeleteCreatedQueues = other.autoDeleteCreatedQueues;
      this.autoDeleteQueuesDelay = other.autoDeleteQueuesDelay;
      this.autoDeleteQueuesSkipUsageCheck = other.autoDeleteQueuesSkipUsageCheck;
      this.configDeleteQueues = other.configDeleteQueues;
      this.autoCreateAddresses = other.autoCreateAddresses;
      this.autoDeleteAddresses = other.autoDeleteAddresses;
      this.autoDeleteAddressesDelay = other.autoDeleteAddressesDelay;
      this.autoDeleteAddressesSkipUsageCheck = other.autoDeleteAddressesSkipUsageCheck;
      this.configDeleteAddresses = other.configDeleteAddresses;
      this.configDeleteDiverts = other.configDeleteDiverts;
      this.managementBrowsePageSize = other.managementBrowsePageSize;
      this.queuePrefetch = other.queuePrefetch;
      this.maxSizeBytesRejectThreshold = other.maxSizeBytesRejectThreshold;
      this.defaultMaxConsumers = other.defaultMaxConsumers;
      this.defaultPurgeOnNoConsumers = other.defaultPurgeOnNoConsumers;
      this.defaultConsumersBeforeDispatch = other.defaultConsumersBeforeDispatch;
      this.defaultDelayBeforeDispatch = other.defaultDelayBeforeDispatch;
      this.defaultQueueRoutingType = other.defaultQueueRoutingType;
      this.defaultAddressRoutingType = other.defaultAddressRoutingType;
      this.defaultConsumerWindowSize = other.defaultConsumerWindowSize;
      this.defaultGroupRebalance = other.defaultGroupRebalance;
      this.defaultGroupRebalancePauseDispatch = other.defaultGroupRebalancePauseDispatch;
      this.defaultGroupBuckets = other.defaultGroupBuckets;
      this.defaultGroupFirstKey = other.defaultGroupFirstKey;
      this.defaultRingSize = other.defaultRingSize;
      this.enableMetrics = other.enableMetrics;
      this.managementMessageAttributeSizeLimit = other.managementMessageAttributeSizeLimit;
      this.slowConsumerThresholdMeasurementUnit = other.slowConsumerThresholdMeasurementUnit;
      this.enableIngressTimestamp = other.enableIngressTimestamp;
      this.idCacheSize = other.idCacheSize;
   }

   public AddressSettings() {
   }

   /**
    * Set the value of a parameter based on its "key" {@code String}. Valid key names and corresponding {@code static}
    * {@code final} are:
    * <p><ul>
    * <li>address-full-message-policy: {@link #ADDRESS_FULL_MESSAGE_POLICY}
    * <li>max-size-bytes: {@link #MAX_SIZE_BYTES}
    * <li>max-size-page-bytes: {@link #MAX_READ_PAGE_BYTES}
    * <li>max-read-page-messages: {@link #MAX_READ_PAGE_MESSAGES}
    * <li>max-size-messages: {@link #MAX_SIZE_MESSAGES}
    * <li>page-size-bytes: {@link #PAGE_SIZE_BYTES}
    * <li>page-max-cache-size: {@link #PAGE_MAX_CACHE_SIZE}
    * <li>drop-messages-when-full: {@link #DROP_MESSAGES_WHEN_FULL}
    * <li>max-delivery-attempts: {@link #MAX_DELIVERY_ATTEMPTS}
    * <li>message-counter-history-day-limit: {@link #MESSAGE_COUNTER_HISTORY_DAY_LIMIT}
    * <li>redelivery-delay: {@link #REDELIVERY_DELAY}
    * <li>redelivery-delay-multiplier: {@link #REDELIVERY_DELAY_MULTIPLIER}
    * <li>redelivery-collision-avoidance-factor: {@link #REDELIVERY_COLLISION_AVOIDANCE_FACTOR}
    * <li>max-redelivery-delay: {@link #MAX_REDELIVERY_DELAY}
    * <li>dead-letter-address: {@link #DEAD_LETTER_ADDRESS}
    * <li>expiry-address: {@link #EXPIRY_ADDRESS}
    * <li>expiry-delay: {@link #EXPIRY_DELAY}
    * <li>min-expiry-delay: {@link #MIN_EXPIRY_DELAY}
    * <li>max-expiry-delay: {@link #MAX_EXPIRY_DELAY}
    * <li>default-last-value-queue: {@link #DEFAULT_LAST_VALUE_QUEUE_PROP}
    * <li>default-last-value-key: {@link #DEFAULT_LAST_VALUE_KEY}
    * <li>default-non-destructive: {@link #DEFAULT_NON_DESTRUCTIVE}
    * <li>default-exclusive-queue: {@link #DEFAULT_EXCLUSIVE_QUEUE}
    * <li>default-group-rebalance: {@link #DEFAULT_GROUP_REBALANCE}
    * <li>default-group-rebalance-pause-dispatch: {@link #DEFAULT_GROUP_REBALANCE_PAUSE_DISPATCH}
    * <li>default-group-buckets: {@link #DEFAULT_GROUP_BUCKETS}
    * <li>default-group-first-key: {@link #DEFAULT_GROUP_FIRST_KEY}
    * <li>redistribution-delay: {@link #REDISTRIBUTION_DELAY}
    * <li>send-to-dla-on-no-route: {@link #SEND_TO_DLA_ON_NO_ROUTE}
    * <li>slow-consumer-threshold: {@link #SLOW_CONSUMER_THRESHOLD}
    * <li>slow-consumer-threshold-measurement-unit: {@link #SLOW_CONSUMER_THRESHOLD_MEASUREMENT_UNIT}
    * <li>slow-consumer-check-period: {@link #SLOW_CONSUMER_CHECK_PERIOD}
    * <li>slow-consumer-policy: {@link #SLOW_CONSUMER_POLICY}
    * <li>auto-create-queues: {@link #AUTO_CREATE_QUEUES}
    * <li>auto-delete-queues: {@link #AUTO_DELETE_QUEUES}
    * <li>auto-delete-created-queues: {@link #AUTO_DELETE_CREATED_QUEUES}
    * <li>auto-delete-queues-delay: {@link #AUTO_DELETE_QUEUES_DELAY}
    * <li>auto-delete-queues-message-count: {@link #AUTO_DELETE_QUEUE_MESSAGE_COUNT}
    * <li>default-ring-size: {@link #DEFAULT_RING_SIZE}
    * <li>retroactive-message-count: {@link #RETROACTIVE_MESSAGE_COUNT}
    * <li>config-delete-queues: {@link #CONFIG_DELETE_QUEUES}
    * <li>auto-create-addresses: {@link #AUTO_CREATE_ADDRESSES}
    * <li>auto-delete-addresses: {@link #AUTO_DELETE_ADDRESSES}
    * <li>auto-delete-address-delay: {@link #AUTO_DELETE_ADDRESS_DELAY}
    * <li>config-delete-addresses: {@link #CONFIG_DELETE_ADDRESSES}
    * <li>config-delete-diverts: {@link #CONFIG_DELETE_DIVERTS}
    * <li>management-browse-page-size: {@link #MANAGEMENT_BROWSE_PAGE_SIZE_PROP}
    * <li>max-size-bytes-reject-threshold: {@link #MAX_SIZE_BYTES_REJECT_THRESHOLD}
    * <li>default-max-consumers: {@link #DEFAULT_MAX_CONSUMERS}
    * <li>default-purge-on-no-consumers: {@link #DEFAULT_PURGE_ON_NO_CONSUMERS}
    * <li>default-consumers-before-dispatch: {@link #DEFAULT_CONSUMERS_BEFORE_DISPATCH}
    * <li>default-delay-before-dispatch: {@link #DEFAULT_DELAY_BEFORE_DISPATCH}
    * <li>default-queue-routing-type: {@link #DEFAULT_QUEUE_ROUTING_TYPE}
    * <li>default-address-routing-type: {@link #DEFAULT_ADDRESS_ROUTING_TYPE}
    * <li>default-consumer-window-size: {@link #DEFAULT_CONSUMER_WINDOW_SIZE_PROP}
    * <li>auto-create-dead-letter-resources: {@link #AUTO_CREATE_DEAD_LETTER_RESOURCES}
    * <li>dead-letter-queue-prefix: {@link #DEAD_LETTER_QUEUE_PREFIX}
    * <li>dead-letter-queue-suffix: {@link #DEAD_LETTER_QUEUE_SUFFIX}
    * <li>auto-create-expiry-resources: {@link #AUTO_CREATE_EXPIRY_RESOURCES}
    * <li>expiry-queue-prefix: {@link #EXPIRY_QUEUE_PREFIX}
    * <li>expiry-address-suffix: {@link #EXPIRY_QUEUE_SUFFIX}
    * <li>enable-metrics: {@link #ENABLE_METRICS}
    * <li>management-message-attribute-size-limit: {@link #MANAGEMENT_MESSAGE_ATTRIBUTE_SIZE_LIMIT_PROP}
    * <li>enable-ingress-timestamp: {@link # ENABLE_INGRESS_TIMESTAMP}
    * <li>queue-prefetch: {@link # QUEUE_PREFETCH}
    * <li>auto-delete-queues-skip-usage-check: {@link # AUTO_DELETE_QUEUES_SKIP_USAGE_CHECK}
    * <li>auto-delete-addresses-skip-usage-check: {@link # AUTO_DELETE_ADDRESSES_SKIP_USAGE_CHECK}
    * <li>prefetch-page-messages: {@link # PREFETCH_PAGE_MESSAGES}
    * <li>page-limit-bytes: {@link # PAGE_LIMIT_BYTES}
    * <li>page-limit-messages: {@link # PAGE_LIMIT_MESSAGES}
    * <li>page-full-message-policy: {@link # PAGE_FULL_MESSAGE_POLICY}
    * <li>prefetch-page-bytes: {@link # PREFETCH_PAGE_BYTES}
    * <li>id-cache-size: {@link # ID_CACHE_SIZE}
    * </ul><p>
    * The {@code String}-based values will be converted to the proper value types based on the underlying property. For
    * example, if you pass the value "TRUE" for the key "auto-create-queues" the {@code String} "TRUE" will be converted
    * to the {@code Boolean} {@code true}.
    *
    * @param key the key to set to the value
    * @param value the value to set for the key
    * @return this {@code QueueConfiguration}
    */
   public AddressSettings set(String key, String value) {
      if (key != null && value != null) {
         if (key.equals(ADDRESS_FULL_MESSAGE_POLICY)) {
            setAddressFullMessagePolicy(AddressFullMessagePolicy.valueOf(value));
         } else if (key.equals(MAX_SIZE_BYTES)) {
            setMaxSizeBytes(Long.parseLong(value));
         } else if (key.equals(MAX_READ_PAGE_BYTES)) {
            setMaxReadPageBytes(Integer.valueOf(value));
         } else if (key.equals(MAX_READ_PAGE_MESSAGES)) {
            setMaxReadPageMessages(Integer.valueOf(value));
         } else if (key.equals(MAX_SIZE_MESSAGES)) {
            setMaxSizeMessages(Long.parseLong(value));
         } else if (key.equals(PAGE_SIZE_BYTES)) {
            setPageSizeBytes(Integer.valueOf(value));
         } else if (key.equals(PAGE_MAX_CACHE_SIZE)) {
            setPageCacheMaxSize(Integer.valueOf(value));
         } else if (key.equals(DROP_MESSAGES_WHEN_FULL)) {
            setDropMessagesWhenFull(Boolean.valueOf(value));
         } else if (key.equals(MAX_DELIVERY_ATTEMPTS)) {
            setMaxDeliveryAttempts(Integer.valueOf(value));
         } else if (key.equals(MESSAGE_COUNTER_HISTORY_DAY_LIMIT)) {
            setMessageCounterHistoryDayLimit(Integer.valueOf(value));
         } else if (key.equals(REDELIVERY_DELAY)) {
            setRedeliveryDelay(Long.parseLong(value));
         } else if (key.equals(REDELIVERY_DELAY_MULTIPLIER)) {
            setRedeliveryMultiplier(Double.parseDouble(value));
         } else if (key.equals(REDELIVERY_COLLISION_AVOIDANCE_FACTOR)) {
            setRedeliveryCollisionAvoidanceFactor(Double.parseDouble(value));
         } else if (key.equals(MAX_REDELIVERY_DELAY)) {
            setMaxRedeliveryDelay(Long.parseLong(value));
         } else if (key.equals(DEAD_LETTER_ADDRESS)) {
            setDeadLetterAddress(new SimpleString(value));
         } else if (key.equals(EXPIRY_ADDRESS)) {
            setExpiryAddress(new SimpleString(value));
         } else if (key.equals(EXPIRY_DELAY)) {
            setExpiryDelay(Long.valueOf(value));
         } else if (key.equals(MIN_EXPIRY_DELAY)) {
            setMinExpiryDelay(Long.valueOf(value));
         } else if (key.equals(MAX_EXPIRY_DELAY)) {
            setMaxExpiryDelay(Long.valueOf(value));
         } else if (key.equals(DEFAULT_LAST_VALUE_QUEUE_PROP)) {
            setDefaultLastValueQueue(Boolean.parseBoolean(value));
         } else if (key.equals(DEFAULT_LAST_VALUE_KEY)) {
            setDefaultLastValueKey(new SimpleString(value));
         } else if (key.equals(DEFAULT_NON_DESTRUCTIVE)) {
            setDefaultNonDestructive(Boolean.parseBoolean(value));
         } else if (key.equals(DEFAULT_EXCLUSIVE_QUEUE)) {
            setDefaultExclusiveQueue(Boolean.valueOf(value));
         } else if (key.equals(DEFAULT_GROUP_REBALANCE)) {
            setDefaultGroupRebalance(Boolean.parseBoolean(value));
         } else if (key.equals(DEFAULT_GROUP_REBALANCE_PAUSE_DISPATCH)) {
            setDefaultGroupRebalancePauseDispatch(Boolean.parseBoolean(value));
         } else if (key.equals(DEFAULT_GROUP_BUCKETS)) {
            setDefaultGroupBuckets(Integer.valueOf(value));
         } else if (key.equals(DEFAULT_GROUP_FIRST_KEY)) {
            setDefaultGroupFirstKey(new SimpleString(value));
         } else if (key.equals(REDISTRIBUTION_DELAY)) {
            setRedistributionDelay(Long.parseLong(value));
         } else if (key.equals(SEND_TO_DLA_ON_NO_ROUTE)) {
            setSendToDLAOnNoRoute(Boolean.parseBoolean(value));
         } else if (key.equals(SLOW_CONSUMER_THRESHOLD)) {
            setSlowConsumerThreshold(Long.parseLong(value));
         } else if (key.equals(SLOW_CONSUMER_THRESHOLD_MEASUREMENT_UNIT)) {
            setSlowConsumerThresholdMeasurementUnit(SlowConsumerThresholdMeasurementUnit.valueOf(value));
         } else if (key.equals(SLOW_CONSUMER_CHECK_PERIOD)) {
            setSlowConsumerCheckPeriod(Long.parseLong(value));
         } else if (key.equals(SLOW_CONSUMER_POLICY)) {
            setSlowConsumerPolicy(SlowConsumerPolicy.valueOf(value));
         } else if (key.equals(AUTO_CREATE_QUEUES)) {
            setAutoCreateQueues(Boolean.valueOf(value));
         } else if (key.equals(AUTO_DELETE_QUEUES)) {
            setAutoDeleteQueues(Boolean.valueOf(value));
         } else if (key.equals(AUTO_DELETE_CREATED_QUEUES)) {
            setAutoDeleteCreatedQueues(Boolean.valueOf(value));
         } else if (key.equals(AUTO_DELETE_QUEUES_DELAY)) {
            setAutoDeleteQueuesDelay(Long.parseLong(value));
         } else if (key.equals(AUTO_DELETE_QUEUE_MESSAGE_COUNT)) {
            setAutoDeleteQueuesMessageCount(Long.parseLong(value));
         } else if (key.equals(DEFAULT_RING_SIZE)) {
            setDefaultRingSize(Long.parseLong(value));
         } else if (key.equals(RETROACTIVE_MESSAGE_COUNT)) {
            setRetroactiveMessageCount(Long.parseLong(value));
         } else if (key.equals(CONFIG_DELETE_QUEUES)) {
            setConfigDeleteQueues(DeletionPolicy.valueOf(value));
         } else if (key.equals(AUTO_CREATE_ADDRESSES)) {
            setAutoCreateAddresses(Boolean.valueOf(value));
         } else if (key.equals(AUTO_DELETE_ADDRESSES)) {
            setAutoDeleteAddresses(Boolean.valueOf(value));
         } else if (key.equals(AUTO_DELETE_ADDRESS_DELAY)) {
            setAutoDeleteAddressesDelay(Long.parseLong(value));
         } else if (key.equals(CONFIG_DELETE_ADDRESSES)) {
            setConfigDeleteAddresses(DeletionPolicy.valueOf(value));
         } else if (key.equals(CONFIG_DELETE_DIVERTS)) {
            setConfigDeleteDiverts(DeletionPolicy.valueOf(value));
         } else if (key.equals(MANAGEMENT_BROWSE_PAGE_SIZE_PROP)) {
            setManagementBrowsePageSize(Integer.valueOf(value));
         } else if (key.equals(MAX_SIZE_BYTES_REJECT_THRESHOLD)) {
            setMaxSizeBytesRejectThreshold(Long.parseLong(value));
         } else if (key.equals(DEFAULT_MAX_CONSUMERS)) {
            setDefaultMaxConsumers(Integer.valueOf(value));
         } else if (key.equals(DEFAULT_PURGE_ON_NO_CONSUMERS)) {
            setDefaultPurgeOnNoConsumers(Boolean.valueOf(value));
         } else if (key.equals(DEFAULT_CONSUMERS_BEFORE_DISPATCH)) {
            setDefaultConsumersBeforeDispatch(Integer.valueOf(value));
         } else if (key.equals(DEFAULT_DELAY_BEFORE_DISPATCH)) {
            setDefaultDelayBeforeDispatch(Long.valueOf(value));
         } else if (key.equals(DEFAULT_QUEUE_ROUTING_TYPE)) {
            setDefaultQueueRoutingType(RoutingType.valueOf(value));
         } else if (key.equals(DEFAULT_ADDRESS_ROUTING_TYPE)) {
            setDefaultAddressRoutingType(RoutingType.valueOf(value));
         } else if (key.equals(DEFAULT_CONSUMER_WINDOW_SIZE_PROP)) {
            setDefaultConsumerWindowSize(Integer.valueOf(value));
         } else if (key.equals(AUTO_CREATE_DEAD_LETTER_RESOURCES)) {
            setAutoCreateDeadLetterResources(Boolean.parseBoolean(value));
         } else if (key.equals(DEAD_LETTER_QUEUE_PREFIX)) {
            setDeadLetterQueuePrefix(new SimpleString(value));
         } else if (key.equals(DEAD_LETTER_QUEUE_SUFFIX)) {
            setDeadLetterQueueSuffix(new SimpleString(value));
         } else if (key.equals(AUTO_CREATE_EXPIRY_RESOURCES)) {
            setAutoCreateExpiryResources(Boolean.parseBoolean(value));
         } else if (key.equals(EXPIRY_QUEUE_PREFIX)) {
            setExpiryQueuePrefix(new SimpleString(value));
         } else if (key.equals(EXPIRY_QUEUE_SUFFIX)) {
            setExpiryQueueSuffix(new SimpleString(value));
         } else if (key.equals(ENABLE_METRICS)) {
            setEnableMetrics(Boolean.parseBoolean(value));
         } else if (key.equals(MANAGEMENT_MESSAGE_ATTRIBUTE_SIZE_LIMIT_PROP)) {
            setManagementMessageAttributeSizeLimit(Integer.valueOf(value));
         } else if (key.equals(ENABLE_INGRESS_TIMESTAMP)) {
            setEnableIngressTimestamp(Boolean.parseBoolean(value));
         } else if (key.equals(QUEUE_PREFETCH)) {
            setQueuePrefetch(Integer.valueOf(value));
         } else if (key.equals(AUTO_DELETE_QUEUES_SKIP_USAGE_CHECK)) {
            setAutoDeleteQueuesSkipUsageCheck(Boolean.parseBoolean(value));
         } else if (key.equals(AUTO_DELETE_ADDRESSES_SKIP_USAGE_CHECK)) {
            setAutoDeleteAddressesSkipUsageCheck(Boolean.parseBoolean(value));
         } else if (key.equals(PREFETCH_PAGE_MESSAGES)) {
            setPrefetchPageMessages(Integer.valueOf(value));
         } else if (key.equals(PAGE_LIMIT_BYTES)) {
            setPageLimitBytes(Long.valueOf(value));
         } else if (key.equals(PAGE_LIMIT_MESSAGES)) {
            setPageLimitMessages(Long.valueOf(value));
         } else if (key.equals(PAGE_FULL_MESSAGE_POLICY)) {
            setPageFullMessagePolicy(PageFullMessagePolicy.valueOf(value));
         } else if (key.equals(PREFETCH_PAGE_BYTES)) {
            setPrefetchPageBytes(Integer.valueOf(value));
         } else if (key.equals(ID_CACHE_SIZE)) {
            setIDCacheSize(Integer.valueOf(value));
         }
      }
      return this;
   }

   /**
    * This method returns a JSON-formatted {@code String} representation of this {@code AddressSettings}. It is a
    * simple collection of key/value pairs. The keys used are referenced in {@link #set(String, String)}.
    *
    * @return a JSON-formatted {@code String} representation of this {@code AddressSettings}
    */
   public String toJSON() {
      JsonObjectBuilder builder = JsonLoader.createObjectBuilder();

      if (getAddressFullMessagePolicy() != null) {
         builder.add(ADDRESS_FULL_MESSAGE_POLICY, getAddressFullMessagePolicy().toString());
      }

      builder.add(MAX_SIZE_BYTES, getMaxSizeBytes());
      builder.add(MAX_SIZE_MESSAGES, getMaxSizeMessages());
      builder.add(MAX_READ_PAGE_MESSAGES, getMaxReadPageMessages());
      builder.add(MAX_READ_PAGE_BYTES, getMaxReadPageBytes());
      builder.add(PAGE_SIZE_BYTES, getPageSizeBytes());
      builder.add(PAGE_MAX_CACHE_SIZE, getPageCacheMaxSize());
      if (getDropMessagesWhenFull() != null) {
         builder.add(DROP_MESSAGES_WHEN_FULL, getDropMessagesWhenFull());
      }
      builder.add(MAX_DELIVERY_ATTEMPTS, getMaxDeliveryAttempts());
      builder.add(MESSAGE_COUNTER_HISTORY_DAY_LIMIT, getMessageCounterHistoryDayLimit());
      builder.add(REDELIVERY_DELAY, getRedeliveryDelay());
      builder.add(REDELIVERY_DELAY_MULTIPLIER, getRedeliveryMultiplier());
      builder.add(REDELIVERY_COLLISION_AVOIDANCE_FACTOR, getRedeliveryCollisionAvoidanceFactor());
      builder.add(MAX_REDELIVERY_DELAY, getMaxRedeliveryDelay());
      if (getDeadLetterAddress() != null) {
         builder.add(DEAD_LETTER_ADDRESS, getDeadLetterAddress().toString());
      }
      builder.add(AUTO_CREATE_DEAD_LETTER_RESOURCES, isAutoCreateDeadLetterResources());
      if (getDeadLetterQueuePrefix() != null) {
         builder.add(DEAD_LETTER_QUEUE_PREFIX, getDeadLetterQueuePrefix().toString());
      }
      if (getDeadLetterQueueSuffix() != null) {
         builder.add(DEAD_LETTER_QUEUE_SUFFIX, getDeadLetterQueueSuffix().toString());
      }
      if (getExpiryAddress() != null) {
         builder.add(EXPIRY_ADDRESS, getExpiryAddress().toString());
      }
      builder.add(AUTO_CREATE_EXPIRY_RESOURCES, isAutoCreateExpiryResources());
      if (getExpiryQueuePrefix() != null) {
         builder.add(EXPIRY_QUEUE_PREFIX, getExpiryQueuePrefix().toString());
      }
      if (getExpiryQueueSuffix() != null) {
         builder.add(EXPIRY_QUEUE_SUFFIX, getExpiryQueueSuffix().toString());
      }
      builder.add(EXPIRY_DELAY, getExpiryDelay());
      builder.add(MIN_EXPIRY_DELAY, getMinExpiryDelay());
      builder.add(MAX_EXPIRY_DELAY, getMaxExpiryDelay());
      builder.add(DEFAULT_LAST_VALUE_QUEUE_PROP, isDefaultLastValueQueue());
      if (getDefaultLastValueKey() != null ) {
         builder.add(DEFAULT_LAST_VALUE_KEY, getDefaultLastValueKey().toString());
      }
      builder.add(DEFAULT_NON_DESTRUCTIVE, isDefaultNonDestructive());
      builder.add(DEFAULT_EXCLUSIVE_QUEUE, isDefaultExclusiveQueue());
      builder.add(REDISTRIBUTION_DELAY, getRedistributionDelay());
      builder.add(SEND_TO_DLA_ON_NO_ROUTE, isSendToDLAOnNoRoute());
      builder.add(SLOW_CONSUMER_THRESHOLD, getSlowConsumerThreshold());
      builder.add(SLOW_CONSUMER_CHECK_PERIOD, getSlowConsumerCheckPeriod());
      if (getSlowConsumerPolicy() != null ) {
         builder.add(SLOW_CONSUMER_POLICY, getSlowConsumerPolicy().toString());
      }
      builder.add(AUTO_CREATE_QUEUES, isAutoCreateQueues());
      builder.add(AUTO_DELETE_QUEUES, isAutoDeleteQueues());
      builder.add(AUTO_DELETE_CREATED_QUEUES, isAutoDeleteCreatedQueues());
      builder.add(AUTO_DELETE_QUEUES_DELAY, getAutoDeleteQueuesDelay());
      builder.add(AUTO_DELETE_QUEUE_MESSAGE_COUNT, getAutoDeleteQueuesMessageCount());
      if (getConfigDeleteQueues() != null) {
         builder.add(CONFIG_DELETE_QUEUES, getConfigDeleteQueues().toString());
      }
      builder.add(AUTO_CREATE_ADDRESSES, isAutoCreateAddresses());
      builder.add(AUTO_DELETE_ADDRESSES, isAutoDeleteAddresses());
      builder.add(AUTO_DELETE_ADDRESS_DELAY, getAutoDeleteAddressesDelay());
      if (getConfigDeleteAddresses() != null) {
         builder.add(CONFIG_DELETE_ADDRESSES, getConfigDeleteAddresses().toString());
      }
      if (getConfigDeleteDiverts() != null) {
         builder.add(CONFIG_DELETE_DIVERTS, getConfigDeleteDiverts().toString());
      }
      builder.add(MANAGEMENT_BROWSE_PAGE_SIZE_PROP, getManagementBrowsePageSize());
      builder.add(QUEUE_PREFETCH, getQueuePrefetch());
      builder.add(MAX_SIZE_BYTES_REJECT_THRESHOLD, getMaxSizeBytesRejectThreshold());
      builder.add(DEFAULT_MAX_CONSUMERS, getDefaultMaxConsumers());
      if (defaultPurgeOnNoConsumers != null) {
         builder.add(DEFAULT_PURGE_ON_NO_CONSUMERS, defaultPurgeOnNoConsumers);
      }
      builder.add(DEFAULT_CONSUMERS_BEFORE_DISPATCH, getDefaultConsumersBeforeDispatch());
      builder.add(DEFAULT_DELAY_BEFORE_DISPATCH, getDefaultDelayBeforeDispatch());
      if (getDefaultQueueRoutingType() != null ) {
         builder.add(DEFAULT_QUEUE_ROUTING_TYPE, getDefaultQueueRoutingType().toString());
      }
      if (getDefaultAddressRoutingType() != null ) {
         builder.add(DEFAULT_ADDRESS_ROUTING_TYPE, getDefaultAddressRoutingType().toString());
      }
      builder.add(DEFAULT_CONSUMER_WINDOW_SIZE_PROP, getDefaultConsumerWindowSize());
      builder.add(DEFAULT_GROUP_REBALANCE, isDefaultGroupRebalance());
      builder.add(DEFAULT_GROUP_REBALANCE_PAUSE_DISPATCH, isDefaultGroupRebalancePauseDispatch());
      builder.add(DEFAULT_GROUP_BUCKETS, getDefaultGroupBuckets());
      if (getDefaultGroupFirstKey() != null ) {
         builder.add(DEFAULT_GROUP_FIRST_KEY, getDefaultGroupFirstKey().toString());
      }
      builder.add(DEFAULT_RING_SIZE, getDefaultRingSize());
      builder.add(RETROACTIVE_MESSAGE_COUNT, getRetroactiveMessageCount());
      builder.add(ENABLE_METRICS, isEnableMetrics());
      builder.add(MANAGEMENT_MESSAGE_ATTRIBUTE_SIZE_LIMIT_PROP, getManagementMessageAttributeSizeLimit());
      if (getSlowConsumerThresholdMeasurementUnit() != null ) {
         builder.add(SLOW_CONSUMER_THRESHOLD_MEASUREMENT_UNIT, getSlowConsumerThresholdMeasurementUnit().toString());
      }
      builder.add(ENABLE_INGRESS_TIMESTAMP, isEnableIngressTimestamp());
      builder.add(AUTO_DELETE_QUEUES_SKIP_USAGE_CHECK, getAutoDeleteQueuesSkipUsageCheck());
      builder.add(AUTO_DELETE_ADDRESSES_SKIP_USAGE_CHECK, isAutoDeleteAddressesSkipUsageCheck());
      builder.add(PREFETCH_PAGE_MESSAGES, getPrefetchPageMessages());
      builder.add(PAGE_LIMIT_BYTES, getPageLimitBytes());
      builder.add(PAGE_LIMIT_MESSAGES, getPageLimitMessages());
      if (getPageFullMessagePolicy() != null ) {
         builder.add(PAGE_FULL_MESSAGE_POLICY, getPageFullMessagePolicy().toString());
      }
      builder.add(PREFETCH_PAGE_BYTES, getPrefetchPageBytes());
      builder.add(ID_CACHE_SIZE, getIDCacheSize());
      return builder.build().toString();
   }

   public Boolean getDropMessagesWhenFull() {
      return dropMessagesWhenFull;
   }

   public AddressSettings setDropMessagesWhenFull(Boolean dropMessagesWhenFull) {
      this.dropMessagesWhenFull = dropMessagesWhenFull;
      return this;
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
      return pageMaxCache != null ? pageMaxCache : AddressSettings.DEFAULT_PAGE_MAX_CACHE;
   }

   public AddressSettings setPageCacheMaxSize(final int pageMaxCache) {
      this.pageMaxCache = pageMaxCache;
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
      if (maxSizeMessages == null) {
         maxSizeMessages = merged.maxSizeMessages;
      }
      if (maxReadPageBytes == null) {
         maxReadPageBytes = merged.maxReadPageBytes;
      }
      if (maxReadPageMessages == null) {
         maxReadPageMessages = merged.maxReadPageMessages;
      }
      if (pageMaxCache == null) {
         pageMaxCache = merged.pageMaxCache;
      }
      if (pageSizeBytes == null) {
         pageSizeBytes = merged.pageSizeBytes;
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
      if (redeliveryCollisionAvoidanceFactor == null) {
         redeliveryCollisionAvoidanceFactor = merged.redeliveryCollisionAvoidanceFactor;
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
      if (minExpiryDelay == null) {
         minExpiryDelay = merged.minExpiryDelay;
      }
      if (maxExpiryDelay == null) {
         maxExpiryDelay = merged.maxExpiryDelay;
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
      if (slowConsumerThresholdMeasurementUnit == null) {
         slowConsumerThresholdMeasurementUnit = merged.slowConsumerThresholdMeasurementUnit;
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
      if (autoDeleteCreatedQueues == null) {
         autoDeleteCreatedQueues = merged.autoDeleteCreatedQueues;
      }
      if (autoDeleteQueuesDelay == null) {
         autoDeleteQueuesDelay = merged.autoDeleteQueuesDelay;
      }
      if (autoDeleteQueuesSkipUsageCheck == null) {
         autoDeleteQueuesSkipUsageCheck = merged.autoDeleteQueuesSkipUsageCheck;
      }
      if (autoDeleteQueuesMessageCount == null) {
         autoDeleteQueuesMessageCount = merged.autoDeleteQueuesMessageCount;
      }
      if (configDeleteQueues == null) {
         configDeleteQueues = merged.configDeleteQueues;
      }
      if (autoCreateAddresses == null) {
         autoCreateAddresses = merged.autoCreateAddresses;
      }
      if (autoDeleteAddresses == null) {
         autoDeleteAddresses = merged.autoDeleteAddresses;
      }
      if (autoDeleteAddressesDelay == null) {
         autoDeleteAddressesDelay = merged.autoDeleteAddressesDelay;
      }
      if (autoDeleteAddressesSkipUsageCheck == null) {
         autoDeleteAddressesSkipUsageCheck = merged.autoDeleteAddressesSkipUsageCheck;
      }
      if (configDeleteAddresses == null) {
         configDeleteAddresses = merged.configDeleteAddresses;
      }
      if (configDeleteDiverts == null) {
         configDeleteDiverts = merged.configDeleteDiverts;
      }
      if (managementBrowsePageSize == null) {
         managementBrowsePageSize = merged.managementBrowsePageSize;
      }
      if (managementMessageAttributeSizeLimit == null) {
         managementMessageAttributeSizeLimit = merged.managementMessageAttributeSizeLimit;
      }
      if (queuePrefetch == null) {
         queuePrefetch = merged.queuePrefetch;
      }
      if (maxSizeBytesRejectThreshold == null) {
         maxSizeBytesRejectThreshold = merged.maxSizeBytesRejectThreshold;
      }
      if (defaultMaxConsumers == null) {
         defaultMaxConsumers = merged.defaultMaxConsumers;
      }
      if (defaultPurgeOnNoConsumers == null) {
         defaultPurgeOnNoConsumers = merged.defaultPurgeOnNoConsumers;
      }
      if (defaultQueueRoutingType == null) {
         defaultQueueRoutingType = merged.defaultQueueRoutingType;
      }
      if (defaultAddressRoutingType == null) {
         defaultAddressRoutingType = merged.defaultAddressRoutingType;
      }
      if (defaultExclusiveQueue == null) {
         defaultExclusiveQueue = merged.defaultExclusiveQueue;
      }
      if (defaultConsumerWindowSize == null) {
         defaultConsumerWindowSize = merged.defaultConsumerWindowSize;
      }
      if (defaultLastValueQueue == null) {
         defaultLastValueQueue = merged.defaultLastValueQueue;
      }
      if (defaultLastValueKey == null) {
         defaultLastValueKey = merged.defaultLastValueKey;
      }
      if (defaultNonDestructive == null) {
         defaultNonDestructive = merged.defaultNonDestructive;
      }
      if (defaultConsumersBeforeDispatch == null) {
         defaultConsumersBeforeDispatch = merged.defaultConsumersBeforeDispatch;
      }
      if (defaultDelayBeforeDispatch == null) {
         defaultDelayBeforeDispatch = merged.defaultDelayBeforeDispatch;
      }
      if (defaultGroupRebalance == null) {
         defaultGroupRebalance = merged.defaultGroupRebalance;
      }
      if (defaultGroupRebalancePauseDispatch == null) {
         defaultGroupRebalancePauseDispatch = merged.defaultGroupRebalancePauseDispatch;
      }
      if (defaultGroupBuckets == null) {
         defaultGroupBuckets = merged.defaultGroupBuckets;
      }
      if (defaultGroupFirstKey == null) {
         defaultGroupFirstKey = merged.defaultGroupFirstKey;
      }
      if (defaultRingSize == null) {
         defaultRingSize = merged.defaultRingSize;
      }
      if (retroactiveMessageCount == null) {
         retroactiveMessageCount = merged.retroactiveMessageCount;
      }
      if (autoCreateDeadLetterResources == null) {
         autoCreateDeadLetterResources = merged.autoCreateDeadLetterResources;
      }
      if (deadLetterQueuePrefix == null) {
         deadLetterQueuePrefix = merged.deadLetterQueuePrefix;
      }
      if (deadLetterQueueSuffix == null) {
         deadLetterQueueSuffix = merged.deadLetterQueueSuffix;
      }
      if (autoCreateExpiryResources == null) {
         autoCreateExpiryResources = merged.autoCreateExpiryResources;
      }
      if (expiryQueuePrefix == null) {
         expiryQueuePrefix = merged.expiryQueuePrefix;
      }
      if (expiryQueueSuffix == null) {
         expiryQueueSuffix = merged.expiryQueueSuffix;
      }
      if (enableMetrics == null) {
         enableMetrics = merged.enableMetrics;
      }
      if (enableIngressTimestamp == null) {
         enableIngressTimestamp = merged.enableIngressTimestamp;
      }
      if (pageFullMessagePolicy == null) {
         pageFullMessagePolicy = merged.pageFullMessagePolicy;
      }
      if (pageLimitBytes == null) {
         pageLimitBytes = merged.pageLimitBytes;
      }
      if (pageLimitMessages == null) {
         pageLimitMessages = merged.pageLimitMessages;
      }
      if (idCacheSize == null) {
         idCacheSize = merged.idCacheSize;
      }
      if (prefetchPageMessages == null) {
         prefetchPageMessages = merged.prefetchPageMessages;
      }
      if (prefetchPageBytes == null) {
         prefetchPageBytes = merged.prefetchPageBytes;
      }
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
   }

   @Override
   public int getEncodeSize() {

      return BufferHelper.sizeOfNullableSimpleString(addressFullMessagePolicy != null ? addressFullMessagePolicy.toString() : null) +
         BufferHelper.sizeOfNullableLong(maxSizeBytes) +
         BufferHelper.sizeOfNullableLong(pageSizeBytes == null ? null : Long.valueOf(pageSizeBytes)) +
         BufferHelper.sizeOfNullableInteger(pageMaxCache) +
         BufferHelper.sizeOfNullableBoolean(dropMessagesWhenFull) +
         BufferHelper.sizeOfNullableInteger(maxDeliveryAttempts) +
         BufferHelper.sizeOfNullableInteger(messageCounterHistoryDayLimit) +
         BufferHelper.sizeOfNullableLong(redeliveryDelay) +
         BufferHelper.sizeOfNullableDouble(redeliveryMultiplier) +
         BufferHelper.sizeOfNullableDouble(redeliveryCollisionAvoidanceFactor) +
         BufferHelper.sizeOfNullableLong(maxRedeliveryDelay) +
         SimpleString.sizeofNullableString(deadLetterAddress) +
         SimpleString.sizeofNullableString(expiryAddress) +
         BufferHelper.sizeOfNullableLong(expiryDelay) +
         BufferHelper.sizeOfNullableLong(minExpiryDelay) +
         BufferHelper.sizeOfNullableLong(maxExpiryDelay) +
         BufferHelper.sizeOfNullableBoolean(defaultLastValueQueue) +
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
         BufferHelper.sizeOfNullableBoolean(autoDeleteQueues) + BufferHelper.sizeOfNullableSimpleString(configDeleteQueues != null ? configDeleteQueues.toString() : null) +
         BufferHelper.sizeOfNullableBoolean(autoCreateAddresses) +
         BufferHelper.sizeOfNullableBoolean(autoDeleteAddresses) + BufferHelper.sizeOfNullableSimpleString(configDeleteAddresses != null ? configDeleteAddresses.toString() : null) +
         BufferHelper.sizeOfNullableSimpleString(configDeleteDiverts != null ? configDeleteDiverts.toString() : null) +
         BufferHelper.sizeOfNullableInteger(managementBrowsePageSize) +
         BufferHelper.sizeOfNullableLong(maxSizeBytesRejectThreshold) +
         BufferHelper.sizeOfNullableInteger(defaultMaxConsumers) +
         BufferHelper.sizeOfNullableBoolean(defaultPurgeOnNoConsumers) +
         DataConstants.SIZE_BYTE +
         DataConstants.SIZE_BYTE +
         BufferHelper.sizeOfNullableBoolean(defaultExclusiveQueue) +
         BufferHelper.sizeOfNullableInteger(defaultConsumersBeforeDispatch) +
         BufferHelper.sizeOfNullableLong(defaultDelayBeforeDispatch) +
         BufferHelper.sizeOfNullableInteger(defaultConsumerWindowSize) +
         SimpleString.sizeofNullableString(defaultLastValueKey) +
         BufferHelper.sizeOfNullableBoolean(defaultNonDestructive) +
         BufferHelper.sizeOfNullableLong(autoDeleteQueuesDelay) +
         BufferHelper.sizeOfNullableBoolean(autoDeleteQueuesSkipUsageCheck) +
         BufferHelper.sizeOfNullableLong(autoDeleteAddressesDelay) +
         BufferHelper.sizeOfNullableBoolean(autoDeleteAddressesSkipUsageCheck) +
         BufferHelper.sizeOfNullableBoolean(defaultGroupRebalance) +
         BufferHelper.sizeOfNullableInteger(defaultGroupBuckets) +
         SimpleString.sizeofNullableString(defaultGroupFirstKey) +
         BufferHelper.sizeOfNullableLong(autoDeleteQueuesMessageCount) +
         BufferHelper.sizeOfNullableBoolean(autoDeleteCreatedQueues) +
         BufferHelper.sizeOfNullableLong(defaultRingSize) +
         BufferHelper.sizeOfNullableLong(retroactiveMessageCount) +
         BufferHelper.sizeOfNullableBoolean(autoCreateDeadLetterResources) +
         SimpleString.sizeofNullableString(deadLetterQueuePrefix) +
         SimpleString.sizeofNullableString(deadLetterQueueSuffix) +
         BufferHelper.sizeOfNullableBoolean(autoCreateExpiryResources) +
         SimpleString.sizeofNullableString(expiryQueuePrefix) +
         SimpleString.sizeofNullableString(expiryQueueSuffix) +
         BufferHelper.sizeOfNullableBoolean(enableMetrics) +
         BufferHelper.sizeOfNullableBoolean(defaultGroupRebalancePauseDispatch) +
         BufferHelper.sizeOfNullableInteger(managementMessageAttributeSizeLimit) +
         BufferHelper.sizeOfNullableInteger(slowConsumerThresholdMeasurementUnit.getValue()) +
         BufferHelper.sizeOfNullableBoolean(enableIngressTimestamp) +
         BufferHelper.sizeOfNullableLong(maxSizeMessages) +
         BufferHelper.sizeOfNullableInteger(maxReadPageMessages) +
         BufferHelper.sizeOfNullableInteger(maxReadPageBytes) +
         BufferHelper.sizeOfNullableLong(pageLimitBytes) +
         BufferHelper.sizeOfNullableLong(pageLimitMessages) +
         BufferHelper.sizeOfNullableInteger(idCacheSize) +
         BufferHelper.sizeOfNullableSimpleString(pageFullMessagePolicy != null ? pageFullMessagePolicy.toString() : null) +
         BufferHelper.sizeOfNullableInteger(prefetchPageBytes) +
         BufferHelper.sizeOfNullableInteger(prefetchPageMessages);
   }

   @Override
   public void encode(ActiveMQBuffer buffer) {
      buffer.writeNullableSimpleString(addressFullMessagePolicy != null ? new SimpleString(addressFullMessagePolicy.toString()) : null);

      BufferHelper.writeNullableLong(buffer, maxSizeBytes);

      BufferHelper.writeNullableLong(buffer, pageSizeBytes == null ? null : Long.valueOf(pageSizeBytes));

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

      BufferHelper.writeNullableBoolean(buffer, defaultLastValueQueue);

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

      buffer.writeNullableSimpleString(configDeleteQueues != null ? new SimpleString(configDeleteQueues.toString()) : null);

      BufferHelper.writeNullableBoolean(buffer, autoCreateAddresses);

      BufferHelper.writeNullableBoolean(buffer, autoDeleteAddresses);

      buffer.writeNullableSimpleString(configDeleteAddresses != null ? new SimpleString(configDeleteAddresses.toString()) : null);

      BufferHelper.writeNullableInteger(buffer, managementBrowsePageSize);

      BufferHelper.writeNullableLong(buffer, maxSizeBytesRejectThreshold);

      BufferHelper.writeNullableInteger(buffer, defaultMaxConsumers);

      BufferHelper.writeNullableBoolean(buffer, defaultPurgeOnNoConsumers);

      buffer.writeByte(defaultQueueRoutingType == null ? -1 : defaultQueueRoutingType.getType());

      buffer.writeByte(defaultAddressRoutingType == null ? -1 : defaultAddressRoutingType.getType());

      BufferHelper.writeNullableBoolean(buffer, defaultExclusiveQueue);

      BufferHelper.writeNullableInteger(buffer, defaultConsumersBeforeDispatch);

      BufferHelper.writeNullableLong(buffer, defaultDelayBeforeDispatch);

      BufferHelper.writeNullableInteger(buffer, defaultConsumerWindowSize);

      buffer.writeNullableSimpleString(defaultLastValueKey);

      BufferHelper.writeNullableBoolean(buffer, defaultNonDestructive);

      BufferHelper.writeNullableLong(buffer, autoDeleteQueuesDelay);

      BufferHelper.writeNullableLong(buffer, autoDeleteAddressesDelay);

      BufferHelper.writeNullableBoolean(buffer, defaultGroupRebalance);

      BufferHelper.writeNullableInteger(buffer, defaultGroupBuckets);

      BufferHelper.writeNullableLong(buffer, autoDeleteQueuesMessageCount);

      BufferHelper.writeNullableBoolean(buffer, autoDeleteCreatedQueues);

      BufferHelper.writeNullableLong(buffer, defaultRingSize);

      BufferHelper.writeNullableDouble(buffer, redeliveryCollisionAvoidanceFactor);

      buffer.writeNullableSimpleString(defaultGroupFirstKey);

      BufferHelper.writeNullableLong(buffer, retroactiveMessageCount);

      BufferHelper.writeNullableBoolean(buffer, autoCreateDeadLetterResources);

      buffer.writeNullableSimpleString(deadLetterQueuePrefix);

      buffer.writeNullableSimpleString(deadLetterQueueSuffix);

      BufferHelper.writeNullableBoolean(buffer, autoCreateExpiryResources);

      buffer.writeNullableSimpleString(expiryQueuePrefix);

      buffer.writeNullableSimpleString(expiryQueueSuffix);

      BufferHelper.writeNullableLong(buffer, minExpiryDelay);

      BufferHelper.writeNullableLong(buffer, maxExpiryDelay);

      BufferHelper.writeNullableBoolean(buffer, enableMetrics);

      BufferHelper.writeNullableBoolean(buffer, defaultGroupRebalancePauseDispatch);

      BufferHelper.writeNullableInteger(buffer, managementMessageAttributeSizeLimit);

      BufferHelper.writeNullableInteger(buffer, slowConsumerThresholdMeasurementUnit == null ? null : slowConsumerThresholdMeasurementUnit.getValue());

      BufferHelper.writeNullableBoolean(buffer, enableIngressTimestamp);

      buffer.writeNullableSimpleString(configDeleteDiverts != null ? new SimpleString(configDeleteDiverts.toString()) : null);

      BufferHelper.writeNullableLong(buffer, maxSizeMessages);

      BufferHelper.writeNullableInteger(buffer, maxReadPageBytes);

      BufferHelper.writeNullableInteger(buffer, maxReadPageMessages);

      BufferHelper.writeNullableLong(buffer, pageLimitBytes);

      BufferHelper.writeNullableLong(buffer, pageLimitMessages);

      buffer.writeNullableSimpleString(pageFullMessagePolicy != null ? new SimpleString(pageFullMessagePolicy.toString()) : null);

      BufferHelper.writeNullableBoolean(buffer, autoDeleteQueuesSkipUsageCheck);

      BufferHelper.writeNullableBoolean(buffer, autoDeleteAddressesSkipUsageCheck);

      BufferHelper.writeNullableInteger(buffer, idCacheSize);

      BufferHelper.writeNullableInteger(buffer, prefetchPageBytes);

      BufferHelper.writeNullableInteger(buffer, prefetchPageMessages);
   }


   /**
    * This method returns a {@code AddressSettings} created from the JSON-formatted input {@code String}. The input
    * should be a simple object of key/value pairs. Valid keys are referenced in {@link #set(String, String)}.
    *
    * @param jsonString
    * @return the {@code QueueConfiguration} created from the JSON-formatted input {@code String}
    */
   public static AddressSettings fromJSON(String jsonString) {
      JsonObject json = JsonLoader.readObject(new StringReader(jsonString));

      AddressSettings result = new AddressSettings();

      for (Map.Entry<String, JsonValue> entry : json.entrySet()) {
         result.set(entry.getKey(), entry.getValue().getValueType() == JsonValue.ValueType.STRING ? ((JsonString)entry.getValue()).getString() : entry.getValue().toString());
      }

      return result;
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
      result = prime * result + ((minExpiryDelay == null) ? 0 : expiryDelay.hashCode());
      result = prime * result + ((maxExpiryDelay == null) ? 0 : expiryDelay.hashCode());
      result = prime * result + ((defaultLastValueQueue == null) ? 0 : defaultLastValueQueue.hashCode());
      result = prime * result + ((defaultLastValueKey == null) ? 0 : defaultLastValueKey.hashCode());
      result = prime * result + ((defaultNonDestructive == null) ? 0 : defaultNonDestructive.hashCode());
      result = prime * result + ((defaultExclusiveQueue == null) ? 0 : defaultExclusiveQueue.hashCode());
      result = prime * result + ((maxDeliveryAttempts == null) ? 0 : maxDeliveryAttempts.hashCode());
      result = prime * result + ((maxSizeBytes == null) ? 0 : maxSizeBytes.hashCode());
      result = prime * result + ((messageCounterHistoryDayLimit == null) ? 0 : messageCounterHistoryDayLimit.hashCode());
      result = prime * result + ((pageSizeBytes == null) ? 0 : pageSizeBytes.hashCode());
      result = prime * result + ((pageMaxCache == null) ? 0 : pageMaxCache.hashCode());
      result = prime * result + ((redeliveryDelay == null) ? 0 : redeliveryDelay.hashCode());
      result = prime * result + ((redeliveryMultiplier == null) ? 0 : redeliveryMultiplier.hashCode());
      result = prime * result + ((redeliveryCollisionAvoidanceFactor == null) ? 0 : redeliveryCollisionAvoidanceFactor.hashCode());
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
      result = prime * result + ((autoDeleteCreatedQueues == null) ? 0 : autoDeleteCreatedQueues.hashCode());
      result = prime * result + ((autoDeleteQueuesDelay == null) ? 0 : autoDeleteQueuesDelay.hashCode());
      result = prime * result + ((autoDeleteQueuesSkipUsageCheck == null) ? 0 : autoDeleteQueuesSkipUsageCheck.hashCode());
      result = prime * result + ((autoDeleteQueuesMessageCount == null) ? 0 : autoDeleteQueuesMessageCount.hashCode());
      result = prime * result + ((configDeleteQueues == null) ? 0 : configDeleteQueues.hashCode());
      result = prime * result + ((autoCreateAddresses == null) ? 0 : autoCreateAddresses.hashCode());
      result = prime * result + ((autoDeleteAddresses == null) ? 0 : autoDeleteAddresses.hashCode());
      result = prime * result + ((autoDeleteAddressesDelay == null) ? 0 : autoDeleteAddressesDelay.hashCode());
      result = prime * result + ((autoDeleteAddressesSkipUsageCheck == null) ? 0 : autoDeleteAddressesSkipUsageCheck.hashCode());
      result = prime * result + ((configDeleteAddresses == null) ? 0 : configDeleteAddresses.hashCode());
      result = prime * result + ((configDeleteDiverts == null) ? 0 : configDeleteDiverts.hashCode());
      result = prime * result + ((managementBrowsePageSize == null) ? 0 : managementBrowsePageSize.hashCode());
      result = prime * result + ((queuePrefetch == null) ? 0 : queuePrefetch.hashCode());
      result = prime * result + ((maxSizeBytesRejectThreshold == null) ? 0 : maxSizeBytesRejectThreshold.hashCode());
      result = prime * result + ((defaultMaxConsumers == null) ? 0 : defaultMaxConsumers.hashCode());
      result = prime * result + ((defaultPurgeOnNoConsumers == null) ? 0 : defaultPurgeOnNoConsumers.hashCode());
      result = prime * result + ((defaultQueueRoutingType == null) ? 0 : defaultQueueRoutingType.hashCode());
      result = prime * result + ((defaultAddressRoutingType == null) ? 0 : defaultAddressRoutingType.hashCode());
      result = prime * result + ((defaultConsumersBeforeDispatch == null) ? 0 : defaultConsumersBeforeDispatch.hashCode());
      result = prime * result + ((defaultDelayBeforeDispatch == null) ? 0 : defaultDelayBeforeDispatch.hashCode());
      result = prime * result + ((defaultConsumerWindowSize == null) ? 0 : defaultConsumerWindowSize.hashCode());
      result = prime * result + ((defaultGroupRebalance == null) ? 0 : defaultGroupRebalance.hashCode());
      result = prime * result + ((defaultGroupRebalancePauseDispatch == null) ? 0 : defaultGroupRebalancePauseDispatch.hashCode());
      result = prime * result + ((defaultGroupBuckets == null) ? 0 : defaultGroupBuckets.hashCode());
      result = prime * result + ((defaultGroupFirstKey == null) ? 0 : defaultGroupFirstKey.hashCode());
      result = prime * result + ((defaultRingSize == null) ? 0 : defaultRingSize.hashCode());
      result = prime * result + ((retroactiveMessageCount == null) ? 0 : retroactiveMessageCount.hashCode());
      result = prime * result + ((autoCreateDeadLetterResources == null) ? 0 : autoCreateDeadLetterResources.hashCode());
      result = prime * result + ((deadLetterQueuePrefix == null) ? 0 : deadLetterQueuePrefix.hashCode());
      result = prime * result + ((deadLetterQueueSuffix == null) ? 0 : deadLetterQueueSuffix.hashCode());
      result = prime * result + ((autoCreateExpiryResources == null) ? 0 : autoCreateExpiryResources.hashCode());
      result = prime * result + ((expiryQueuePrefix == null) ? 0 : expiryQueuePrefix.hashCode());
      result = prime * result + ((expiryQueueSuffix == null) ? 0 : expiryQueueSuffix.hashCode());
      result = prime * result + ((enableMetrics == null) ? 0 : enableMetrics.hashCode());
      result = prime * result + ((managementMessageAttributeSizeLimit == null) ? 0 : managementMessageAttributeSizeLimit.hashCode());
      result = prime * result + ((slowConsumerThresholdMeasurementUnit == null) ? 0 : slowConsumerThresholdMeasurementUnit.hashCode());
      result = prime * result + ((enableIngressTimestamp == null) ? 0 : enableIngressTimestamp.hashCode());
      result = prime * result + ((maxSizeMessages == null) ? 0 : maxSizeMessages.hashCode());
      result = prime * result + ((pageLimitBytes == null) ? 0 : pageLimitBytes.hashCode());
      result = prime * result + ((pageLimitMessages == null) ? 0 : pageLimitMessages.hashCode());
      result = prime * result + ((pageFullMessagePolicy == null) ? 0 : pageFullMessagePolicy.hashCode());
      result = prime * result + ((idCacheSize == null) ? 0 : idCacheSize.hashCode());
      result = prime * result + ((prefetchPageBytes == null) ? 0 : prefetchPageBytes.hashCode());
      result = prime * result + ((prefetchPageMessages == null) ? 0 : prefetchPageMessages.hashCode());

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
      if (minExpiryDelay == null) {
         if (other.minExpiryDelay != null)
            return false;
      } else if (!minExpiryDelay.equals(other.minExpiryDelay))
         return false;
      if (maxExpiryDelay == null) {
         if (other.maxExpiryDelay != null)
            return false;
      } else if (!maxExpiryDelay.equals(other.maxExpiryDelay))
         return false;
      if (defaultLastValueQueue == null) {
         if (other.defaultLastValueQueue != null)
            return false;
      } else if (!defaultLastValueQueue.equals(other.defaultLastValueQueue))
         return false;
      if (defaultLastValueKey == null) {
         if (other.defaultLastValueKey != null)
            return false;
      } else if (!defaultLastValueKey.equals(other.defaultLastValueKey))
         return false;
      if (defaultNonDestructive == null) {
         if (other.defaultNonDestructive != null)
            return false;
      } else if (!defaultNonDestructive.equals(other.defaultNonDestructive))
         return false;
      if (defaultExclusiveQueue == null) {
         if (other.defaultExclusiveQueue != null)
            return false;
      } else if (!defaultExclusiveQueue.equals(other.defaultExclusiveQueue))
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
      if (redeliveryCollisionAvoidanceFactor == null) {
         if (other.redeliveryCollisionAvoidanceFactor != null)
            return false;
      } else if (!redeliveryCollisionAvoidanceFactor.equals(other.redeliveryCollisionAvoidanceFactor))
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
      if (autoDeleteCreatedQueues == null) {
         if (other.autoDeleteCreatedQueues != null)
            return false;
      } else if (!autoDeleteCreatedQueues.equals(other.autoDeleteCreatedQueues))
         return false;
      if (autoDeleteQueuesDelay == null) {
         if (other.autoDeleteQueuesDelay != null)
            return false;
      } else if (!autoDeleteQueuesDelay.equals(other.autoDeleteQueuesDelay))
         return false;
      if (autoDeleteQueuesSkipUsageCheck == null) {
         if (other.autoDeleteQueuesSkipUsageCheck != null)
            return false;
      } else if (!autoDeleteQueuesSkipUsageCheck.equals(other.autoDeleteQueuesSkipUsageCheck))
         return false;
      if (autoDeleteQueuesMessageCount == null) {
         if (other.autoDeleteQueuesMessageCount != null)
            return false;
      } else if (!autoDeleteQueuesMessageCount.equals(other.autoDeleteQueuesMessageCount))
         return false;
      if (configDeleteQueues == null) {
         if (other.configDeleteQueues != null)
            return false;
      } else if (!configDeleteQueues.equals(other.configDeleteQueues))
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
      if (autoDeleteAddressesDelay == null) {
         if (other.autoDeleteAddressesDelay != null)
            return false;
      } else if (!autoDeleteAddressesDelay.equals(other.autoDeleteAddressesDelay))
         return false;
      if (autoDeleteAddressesSkipUsageCheck == null) {
         if (other.autoDeleteAddressesSkipUsageCheck != null)
            return false;
      } else if (!autoDeleteAddressesSkipUsageCheck.equals(other.autoDeleteAddressesSkipUsageCheck))
         return false;
      if (configDeleteAddresses == null) {
         if (other.configDeleteAddresses != null)
            return false;
      } else if (!configDeleteAddresses.equals(other.configDeleteAddresses))
         return false;
      if (configDeleteDiverts == null) {
         if (other.configDeleteDiverts != null)
            return false;
      } else if (!configDeleteDiverts.equals(other.configDeleteDiverts))
         return false;
      if (managementBrowsePageSize == null) {
         if (other.managementBrowsePageSize != null)
            return false;
      } else if (!managementBrowsePageSize.equals(other.managementBrowsePageSize))
         return false;
      if (managementMessageAttributeSizeLimit == null) {
         if (other.managementMessageAttributeSizeLimit != null)
            return false;
      } else if (!managementMessageAttributeSizeLimit.equals(other.managementMessageAttributeSizeLimit))
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

      if (defaultMaxConsumers == null) {
         if (other.defaultMaxConsumers != null)
            return false;
      } else if (!defaultMaxConsumers.equals(other.defaultMaxConsumers))
         return false;

      if (defaultPurgeOnNoConsumers == null) {
         if (other.defaultPurgeOnNoConsumers != null)
            return false;
      } else if (!defaultPurgeOnNoConsumers.equals(other.defaultPurgeOnNoConsumers))
         return false;

      if (defaultQueueRoutingType == null) {
         if (other.defaultQueueRoutingType != null)
            return false;
      } else if (!defaultQueueRoutingType.equals(other.defaultQueueRoutingType))
         return false;

      if (defaultAddressRoutingType == null) {
         if (other.defaultAddressRoutingType != null)
            return false;
      } else if (!defaultAddressRoutingType.equals(other.defaultAddressRoutingType))
         return false;

      if (defaultConsumersBeforeDispatch == null) {
         if (other.defaultConsumersBeforeDispatch != null)
            return false;
      } else if (!defaultConsumersBeforeDispatch.equals(other.defaultConsumersBeforeDispatch))
         return false;

      if (defaultDelayBeforeDispatch == null) {
         if (other.defaultDelayBeforeDispatch != null)
            return false;
      } else if (!defaultDelayBeforeDispatch.equals(other.defaultDelayBeforeDispatch))
         return false;

      if (defaultConsumerWindowSize == null) {
         if (other.defaultConsumerWindowSize != null)
            return false;
      } else if (!defaultConsumerWindowSize.equals(other.defaultConsumerWindowSize))
         return false;

      if (defaultGroupRebalance == null) {
         if (other.defaultGroupRebalance != null)
            return false;
      } else if (!defaultGroupRebalance.equals(other.defaultGroupRebalance))
         return false;

      if (defaultGroupRebalancePauseDispatch == null) {
         if (other.defaultGroupRebalancePauseDispatch != null)
            return false;
      } else if (!defaultGroupRebalancePauseDispatch.equals(other.defaultGroupRebalancePauseDispatch))
         return false;

      if (defaultGroupBuckets == null) {
         if (other.defaultGroupBuckets != null)
            return false;
      } else if (!defaultGroupBuckets.equals(other.defaultGroupBuckets))
         return false;

      if (defaultGroupFirstKey == null) {
         if (other.defaultGroupFirstKey != null)
            return false;
      } else if (!defaultGroupFirstKey.equals(other.defaultGroupFirstKey))
         return false;

      if (defaultRingSize == null) {
         if (other.defaultRingSize != null)
            return false;
      } else if (!defaultRingSize.equals(other.defaultRingSize))
         return false;

      if (retroactiveMessageCount == null) {
         if (other.retroactiveMessageCount != null)
            return false;
      } else if (!retroactiveMessageCount.equals(other.retroactiveMessageCount))
         return false;

      if (autoCreateDeadLetterResources == null) {
         if (other.autoCreateDeadLetterResources != null)
            return false;
      } else if (!autoCreateDeadLetterResources.equals(other.autoCreateDeadLetterResources))
         return false;

      if (deadLetterQueuePrefix == null) {
         if (other.deadLetterQueuePrefix != null)
            return false;
      } else if (!deadLetterQueuePrefix.equals(other.deadLetterQueuePrefix))
         return false;

      if (deadLetterQueueSuffix == null) {
         if (other.deadLetterQueueSuffix != null)
            return false;
      } else if (!deadLetterQueueSuffix.equals(other.deadLetterQueueSuffix))
         return false;

      if (autoCreateExpiryResources == null) {
         if (other.autoCreateExpiryResources != null)
            return false;
      } else if (!autoCreateExpiryResources.equals(other.autoCreateExpiryResources))
         return false;

      if (expiryQueuePrefix == null) {
         if (other.expiryQueuePrefix != null)
            return false;
      } else if (!expiryQueuePrefix.equals(other.expiryQueuePrefix))
         return false;

      if (expiryQueueSuffix == null) {
         if (other.expiryQueueSuffix != null)
            return false;
      } else if (!expiryQueueSuffix.equals(other.expiryQueueSuffix))
         return false;

      if (enableMetrics == null) {
         if (other.enableMetrics != null)
            return false;
      } else if (!enableMetrics.equals(other.enableMetrics))
         return false;

      if (slowConsumerThresholdMeasurementUnit != other.slowConsumerThresholdMeasurementUnit)
         return false;

      if (enableIngressTimestamp == null) {
         if (other.enableIngressTimestamp != null)
            return false;
      } else if (!enableIngressTimestamp.equals(other.enableIngressTimestamp))
         return false;

      if (maxSizeMessages == null) {
         if (other.maxSizeMessages != null)
            return false;
      } else if (!maxSizeMessages.equals(other.maxSizeMessages))
         return false;

      if (pageLimitBytes == null) {
         if (other.pageLimitBytes != null) {
            return false;
         }
      } else if (!pageLimitBytes.equals(other.pageLimitBytes)) {
         return false;
      }

      if (pageLimitMessages == null) {
         if (other.pageLimitMessages != null) {
            return false;
         }
      } else if (!pageLimitMessages.equals(other.pageLimitMessages)) {
         return false;
      }

      if (pageFullMessagePolicy == null) {
         if (other.pageFullMessagePolicy != null) {
            return false;
         }
      } else if (!pageFullMessagePolicy.equals(other.pageFullMessagePolicy)) {
         return false;
      }

      if (idCacheSize == null) {
         if (other.idCacheSize != null) {
            return false;
         }
      } else if (!idCacheSize.equals(other.idCacheSize)) {
         return false;
      }

      if (prefetchPageMessages == null) {
         if (other.prefetchPageMessages != null) {
            return false;
         }
      } else if (!prefetchPageMessages.equals(other.prefetchPageMessages)) {
         return false;
      }

      if (prefetchPageBytes == null) {
         if (other.prefetchPageBytes != null) {
            return false;
         }
      } else if (!prefetchPageBytes.equals(other.prefetchPageBytes)) {
         return false;
      }

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
         ", minExpiryDelay=" +
         minExpiryDelay +
         ", maxExpiryDelay=" +
         maxExpiryDelay +
         ", defaultLastValueQueue=" +
         defaultLastValueQueue +
         ", defaultLastValueKey=" +
         defaultLastValueKey +
         ", defaultNonDestructive=" +
         defaultNonDestructive +
         ", defaultExclusiveQueue=" +
         defaultExclusiveQueue +
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
         ", redeliveryCollisionAvoidanceFactor=" +
         redeliveryCollisionAvoidanceFactor +
         ", maxRedeliveryDelay=" +
         maxRedeliveryDelay +
         ", redistributionDelay=" +
         redistributionDelay +
         ", sendToDLAOnNoRoute=" +
         sendToDLAOnNoRoute +
         ", slowConsumerThreshold=" +
         slowConsumerThreshold +
         ", slowConsumerThresholdMeasurementUnit=" +
         slowConsumerThresholdMeasurementUnit +
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
         ", autoDeleteCreatedQueues=" +
         autoDeleteCreatedQueues +
         ", autoDeleteQueuesDelay=" +
         autoDeleteQueuesDelay +
         ", autoDeleteQueuesSkipUsageCheck=" +
         autoDeleteQueuesSkipUsageCheck +
         ", autoDeleteQueuesMessageCount=" +
         autoDeleteQueuesMessageCount +
         ", configDeleteQueues=" +
         configDeleteQueues +
         ", autoCreateAddresses=" +
         autoCreateAddresses +
         ", autoDeleteAddresses=" +
         autoDeleteAddresses +
         ", autoDeleteAddressesDelay=" +
         autoDeleteAddressesDelay +
         ", autoDeleteAddressesSkipUsageCheck=" +
         autoDeleteAddressesSkipUsageCheck +
         ", configDeleteAddresses=" +
         configDeleteAddresses  +
         ", configDeleteDiverts=" +
         configDeleteDiverts +
         ", managementBrowsePageSize=" +
         managementBrowsePageSize +
         ", managementMessageAttributeSizeLimit=" +
         managementMessageAttributeSizeLimit +
         ", defaultMaxConsumers=" +
         defaultMaxConsumers +
         ", defaultPurgeOnNoConsumers=" +
         defaultPurgeOnNoConsumers +
         ", defaultQueueRoutingType=" +
         defaultQueueRoutingType +
         ", defaultAddressRoutingType=" +
         defaultAddressRoutingType +
         ", defaultConsumersBeforeDispatch=" +
         defaultConsumersBeforeDispatch +
         ", defaultDelayBeforeDispatch=" +
         defaultDelayBeforeDispatch +
         ", defaultClientWindowSize=" +
         defaultConsumerWindowSize +
         ", defaultGroupRebalance=" +
         defaultGroupRebalance +
         ", defaultGroupRebalancePauseDispatch=" +
         defaultGroupRebalancePauseDispatch +
         ", defaultGroupBuckets=" +
         defaultGroupBuckets +
         ", defaultGroupFirstKey=" +
         defaultGroupFirstKey +
         ", defaultRingSize=" +
         defaultRingSize +
         ", retroactiveMessageCount=" +
         retroactiveMessageCount +
         ", autoCreateDeadLetterResources=" +
         autoCreateDeadLetterResources +
         ", deadLetterQueuePrefix=" +
         deadLetterQueuePrefix +
         ", deadLetterQueueSuffix=" +
         deadLetterQueueSuffix +
         ", autoCreateExpiryResources=" +
         autoCreateExpiryResources +
         ", expiryQueuePrefix=" +
         expiryQueuePrefix +
         ", expiryQueueSuffix=" +
         expiryQueueSuffix +
         ", enableMetrics=" +
         enableMetrics +
         ", enableIngressTime=" +
         enableIngressTimestamp +
         ", pageLimitBytes=" +
         pageLimitBytes +
         ", pageLimitMessages=" +
         pageLimitMessages +
         ", pageFullMessagePolicy=" +
         pageFullMessagePolicy +
         ", idCacheSize=" +
         idCacheSize +
         ", prefetchPageMessages=" +
         prefetchPageMessages +
         ", prefetchPageBytes=" +
         prefetchPageBytes +
         "]";
   }
}