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
package org.apache.activemq.artemis.core.settings;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.core.settings.impl.DeletionPolicy;
import org.apache.activemq.artemis.core.settings.impl.PageFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.SlowConsumerPolicy;
import org.apache.activemq.artemis.core.settings.impl.SlowConsumerThresholdMeasurementUnit;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.Assert;
import org.junit.Test;

public class AddressSettingsTest extends ActiveMQTestBase {

   @Test
   public void testDefaults() {
      AddressSettings addressSettings = new AddressSettings();
      Assert.assertEquals(null, addressSettings.getDeadLetterAddress());
      Assert.assertEquals(null, addressSettings.getExpiryAddress());
      Assert.assertEquals(AddressSettings.DEFAULT_MAX_DELIVERY_ATTEMPTS, addressSettings.getMaxDeliveryAttempts());
      Assert.assertEquals(addressSettings.getMaxSizeBytes(), AddressSettings.DEFAULT_MAX_SIZE_BYTES);
      Assert.assertEquals(AddressSettings.DEFAULT_PAGE_SIZE, addressSettings.getPageSizeBytes());
      Assert.assertEquals(AddressSettings.DEFAULT_MESSAGE_COUNTER_HISTORY_DAY_LIMIT, addressSettings.getMessageCounterHistoryDayLimit());
      Assert.assertEquals(AddressSettings.DEFAULT_REDELIVER_DELAY, addressSettings.getRedeliveryDelay());
      Assert.assertEquals(AddressSettings.DEFAULT_REDELIVER_MULTIPLIER, addressSettings.getRedeliveryMultiplier(), 0.000001);
      Assert.assertEquals(AddressSettings.DEFAULT_SLOW_CONSUMER_THRESHOLD, addressSettings.getSlowConsumerThreshold());
      Assert.assertEquals(AddressSettings.DEFAULT_SLOW_CONSUMER_THRESHOLD_MEASUREMENT_UNIT, addressSettings.getSlowConsumerThresholdMeasurementUnit());
      Assert.assertEquals(AddressSettings.DEFAULT_SLOW_CONSUMER_CHECK_PERIOD, addressSettings.getSlowConsumerCheckPeriod());
      Assert.assertEquals(AddressSettings.DEFAULT_SLOW_CONSUMER_POLICY, addressSettings.getSlowConsumerPolicy());
      Assert.assertEquals(AddressSettings.DEFAULT_AUTO_CREATE_JMS_QUEUES, addressSettings.isAutoCreateJmsQueues());
      Assert.assertEquals(AddressSettings.DEFAULT_AUTO_DELETE_JMS_QUEUES, addressSettings.isAutoDeleteJmsQueues());
      Assert.assertEquals(AddressSettings.DEFAULT_AUTO_CREATE_TOPICS, addressSettings.isAutoCreateJmsTopics());
      Assert.assertEquals(AddressSettings.DEFAULT_AUTO_DELETE_TOPICS, addressSettings.isAutoDeleteJmsTopics());
      Assert.assertEquals(AddressSettings.DEFAULT_AUTO_CREATE_QUEUES, addressSettings.isAutoCreateQueues());
      Assert.assertEquals(AddressSettings.DEFAULT_AUTO_DELETE_QUEUES, addressSettings.isAutoDeleteQueues());
      Assert.assertEquals(AddressSettings.DEFAULT_AUTO_CREATE_ADDRESSES, addressSettings.isAutoCreateAddresses());
      Assert.assertEquals(AddressSettings.DEFAULT_AUTO_DELETE_ADDRESSES, addressSettings.isAutoDeleteAddresses());
      Assert.assertEquals(ActiveMQDefaultConfiguration.getDefaultPurgeOnNoConsumers(), addressSettings.isDefaultPurgeOnNoConsumers());
      Assert.assertEquals(Integer.valueOf(ActiveMQDefaultConfiguration.getDefaultMaxQueueConsumers()), addressSettings.getDefaultMaxConsumers());
   }

   @Test
   public void testSingleMerge() {
      AddressSettings addressSettings = new AddressSettings();
      AddressSettings addressSettingsToMerge = new AddressSettings();
      SimpleString DLQ = new SimpleString("testDLQ");
      SimpleString exp = new SimpleString("testExpiryQueue");
      addressSettingsToMerge.setDeadLetterAddress(DLQ);
      addressSettingsToMerge.setExpiryAddress(exp);
      addressSettingsToMerge.setMaxDeliveryAttempts(1000);
      addressSettingsToMerge.setAddressFullMessagePolicy(AddressFullMessagePolicy.DROP);
      addressSettingsToMerge.setMaxSizeBytes(1001);
      addressSettingsToMerge.setMaxSizeMessages(101);
      addressSettingsToMerge.setMessageCounterHistoryDayLimit(1002);
      addressSettingsToMerge.setRedeliveryDelay(1003);
      addressSettingsToMerge.setPageSizeBytes(1004);
      addressSettingsToMerge.setMaxSizeBytesRejectThreshold(10 * 1024);
      addressSettingsToMerge.setConfigDeleteDiverts(DeletionPolicy.FORCE);
      addressSettingsToMerge.setExpiryDelay(999L);
      addressSettingsToMerge.setMinExpiryDelay(888L);
      addressSettingsToMerge.setMaxExpiryDelay(777L);
      addressSettingsToMerge.setIDCacheSize(5);

      addressSettings.merge(addressSettingsToMerge);
      Assert.assertEquals(addressSettings.getDeadLetterAddress(), DLQ);
      Assert.assertEquals(addressSettings.getExpiryAddress(), exp);
      Assert.assertEquals(addressSettings.getMaxDeliveryAttempts(), 1000);
      Assert.assertEquals(addressSettings.getMaxSizeBytes(), 1001);
      Assert.assertEquals(addressSettings.getMaxSizeMessages(), 101);
      Assert.assertEquals(addressSettings.getMessageCounterHistoryDayLimit(), 1002);
      Assert.assertEquals(addressSettings.getRedeliveryDelay(), 1003);
      Assert.assertEquals(addressSettings.getPageSizeBytes(), 1004);
      Assert.assertEquals(AddressFullMessagePolicy.DROP, addressSettings.getAddressFullMessagePolicy());
      Assert.assertEquals(addressSettings.getMaxSizeBytesRejectThreshold(), 10 * 1024);
      Assert.assertEquals(DeletionPolicy.FORCE, addressSettings.getConfigDeleteDiverts());
      Assert.assertEquals(Long.valueOf(999), addressSettings.getExpiryDelay());
      Assert.assertEquals(Long.valueOf(888), addressSettings.getMinExpiryDelay());
      Assert.assertEquals(Long.valueOf(777), addressSettings.getMaxExpiryDelay());
      Assert.assertEquals(Integer.valueOf(5), addressSettings.getIDCacheSize());
   }

   @Test
   public void testMultipleMerge() {
      AddressSettings addressSettings = new AddressSettings();
      AddressSettings addressSettingsToMerge = new AddressSettings();
      SimpleString DLQ = new SimpleString("testDLQ");
      SimpleString exp = new SimpleString("testExpiryQueue");
      addressSettingsToMerge.setDeadLetterAddress(DLQ);
      addressSettingsToMerge.setExpiryAddress(exp);
      addressSettingsToMerge.setMaxDeliveryAttempts(1000);
      addressSettingsToMerge.setMaxSizeBytes(1001);
      addressSettingsToMerge.setMessageCounterHistoryDayLimit(1002);
      addressSettingsToMerge.setAddressFullMessagePolicy(AddressFullMessagePolicy.DROP);
      addressSettingsToMerge.setMaxSizeBytesRejectThreshold(10 * 1024);
      addressSettings.merge(addressSettingsToMerge);

      AddressSettings addressSettingsToMerge2 = new AddressSettings();
      SimpleString exp2 = new SimpleString("testExpiryQueue2");
      addressSettingsToMerge2.setExpiryAddress(exp2);
      addressSettingsToMerge2.setMaxSizeBytes(2001);
      addressSettingsToMerge2.setRedeliveryDelay(2003);
      addressSettingsToMerge2.setRedeliveryMultiplier(2.5);
      addressSettings.merge(addressSettingsToMerge2);

      Assert.assertEquals(addressSettings.getDeadLetterAddress(), DLQ);
      Assert.assertEquals(addressSettings.getExpiryAddress(), exp);
      Assert.assertEquals(addressSettings.getMaxDeliveryAttempts(), 1000);
      Assert.assertEquals(addressSettings.getMaxSizeBytes(), 1001);
      Assert.assertEquals(addressSettings.getMessageCounterHistoryDayLimit(), 1002);
      Assert.assertEquals(addressSettings.getRedeliveryDelay(), 2003);
      Assert.assertEquals(addressSettings.getRedeliveryMultiplier(), 2.5, 0.000001);
      Assert.assertEquals(AddressFullMessagePolicy.DROP, addressSettings.getAddressFullMessagePolicy());
      Assert.assertEquals(addressSettings.getMaxSizeBytesRejectThreshold(), 10 * 1024);
   }

   @Test
   public void testMultipleMergeAll() {
      AddressSettings addressSettings = new AddressSettings();
      AddressSettings addressSettingsToMerge = new AddressSettings();
      SimpleString DLQ = new SimpleString("testDLQ");
      SimpleString exp = new SimpleString("testExpiryQueue");
      addressSettingsToMerge.setDeadLetterAddress(DLQ);
      addressSettingsToMerge.setExpiryAddress(exp);
      addressSettingsToMerge.setMaxSizeBytes(1001);
      addressSettingsToMerge.setRedeliveryDelay(1003);
      addressSettingsToMerge.setRedeliveryMultiplier(1.0);
      addressSettingsToMerge.setAddressFullMessagePolicy(AddressFullMessagePolicy.DROP);
      addressSettings.merge(addressSettingsToMerge);

      AddressSettings addressSettingsToMerge2 = new AddressSettings();
      SimpleString exp2 = new SimpleString("testExpiryQueue2");
      SimpleString DLQ2 = new SimpleString("testDlq2");
      addressSettingsToMerge2.setExpiryAddress(exp2);
      addressSettingsToMerge2.setDeadLetterAddress(DLQ2);
      addressSettingsToMerge2.setMaxDeliveryAttempts(2000);
      addressSettingsToMerge2.setMaxSizeBytes(2001);
      addressSettingsToMerge2.setMessageCounterHistoryDayLimit(2002);
      addressSettingsToMerge2.setRedeliveryDelay(2003);
      addressSettingsToMerge2.setRedeliveryMultiplier(2.0);
      addressSettingsToMerge2.setMaxRedeliveryDelay(5000);
      addressSettingsToMerge.setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);
      addressSettings.merge(addressSettingsToMerge2);

      Assert.assertEquals(addressSettings.getDeadLetterAddress(), DLQ);
      Assert.assertEquals(addressSettings.getExpiryAddress(), exp);
      Assert.assertEquals(addressSettings.getMaxDeliveryAttempts(), 2000);
      Assert.assertEquals(addressSettings.getMaxSizeBytes(), 1001);
      Assert.assertEquals(addressSettings.getMessageCounterHistoryDayLimit(), 2002);
      Assert.assertEquals(addressSettings.getRedeliveryDelay(), 1003);
      Assert.assertEquals(addressSettings.getRedeliveryMultiplier(), 1.0, 0.000001);
      Assert.assertEquals(addressSettings.getMaxRedeliveryDelay(), 5000);
      Assert.assertEquals(AddressFullMessagePolicy.DROP, addressSettings.getAddressFullMessagePolicy());
   }

   @Test
   public void testJSON() {
      Boolean dropMessagesWhenFull = RandomUtil.randomBoolean();
      Boolean autoCreateQueues = RandomUtil.randomBoolean();
      Boolean autoDeleteQueues = RandomUtil.randomBoolean();
      Boolean autoDeleteCreatedQueues = RandomUtil.randomBoolean();
      Long autoDeleteQueuesDelay = RandomUtil.randomPositiveLong();
      Boolean autoDeleteQueuesSkipUsageCheck = RandomUtil.randomBoolean();
      Long autoDeleteQueuesMessageCount = RandomUtil.randomPositiveLong();
      DeletionPolicy configDeleteQueues = DeletionPolicy.getType(RandomUtil.randomInterval(0, 1));
      Boolean autoCreateAddresses = RandomUtil.randomBoolean();
      Boolean autoDeleteAddresses = RandomUtil.randomBoolean();
      Long autoDeleteAddressesDelay = RandomUtil.randomPositiveLong();
      Boolean autoDeleteAddressesSkipUsageCheck = RandomUtil.randomBoolean();
      DeletionPolicy configDeleteAddresses = DeletionPolicy.getType(RandomUtil.randomInterval(0, 1));
      DeletionPolicy configDeleteDiverts = DeletionPolicy.getType(RandomUtil.randomInterval(0, 1));
      Integer defaultMaxConsumers = RandomUtil.randomPositiveInt();
      Integer defaultConsumersBeforeDispatch = RandomUtil.randomPositiveInt();
      Long defaultDelayBeforeDispatch = RandomUtil.randomPositiveLong();
      Boolean defaultPurgeOnNoConsumers = RandomUtil.randomBoolean();
      RoutingType defaultQueueRoutingType = RoutingType.getType(RandomUtil.randomBoolean() ? (byte) 1 : 0);
      RoutingType defaultAddressRoutingType = RoutingType.getType(RandomUtil.randomBoolean() ? (byte) 1 : 0);
      Boolean defaultLastValueQueue = RandomUtil.randomBoolean();
      SimpleString defaultLastValueKey = RandomUtil.randomSimpleString();
      Boolean defaultNonDestructive = RandomUtil.randomBoolean();
      Boolean defaultExclusiveQueue = RandomUtil.randomBoolean();
      AddressFullMessagePolicy addressFullMessagePolicy = AddressFullMessagePolicy.getType(RandomUtil.randomInterval(0, 3));
      Integer pageSizeBytes = RandomUtil.randomPositiveInt();
      Integer pageCacheMaxSize = RandomUtil.randomPositiveInt();
      Long maxSizeMessages = RandomUtil.randomPositiveLong();
      Long maxSizeBytes = RandomUtil.randomPositiveLong();
      Integer maxReadPageMessages = RandomUtil.randomPositiveInt();
      Integer prefetchPageMessages = RandomUtil.randomPositiveInt();
      Long pageLimitBytes = RandomUtil.randomPositiveLong();
      Long pageLimitMessages = RandomUtil.randomPositiveLong();
      PageFullMessagePolicy pageFullMessagePolicy = PageFullMessagePolicy.getType(RandomUtil.randomInterval(0, 1));
      Integer maxReadPageBytes = RandomUtil.randomPositiveInt();
      Integer prefetchPageBytes = RandomUtil.randomPositiveInt();
      Integer maxDeliveryAttempts = RandomUtil.randomPositiveInt();
      Integer messageCounterHistoryDayLimit = RandomUtil.randomPositiveInt();
      Long redeliveryDelay = RandomUtil.randomPositiveLong();
      Double redeliveryMultiplier = RandomUtil.randomDouble();
      Double redeliveryCollisionAvoidanceFactor = RandomUtil.randomDouble();
      Long maxRedeliveryDelay = RandomUtil.randomPositiveLong();
      SimpleString deadLetterAddress = RandomUtil.randomSimpleString();
      SimpleString expiryAddress = RandomUtil.randomSimpleString();
      Boolean autoCreateExpiryResources = RandomUtil.randomBoolean();
      SimpleString expiryQueuePrefix = RandomUtil.randomSimpleString();
      SimpleString expiryQueueSuffix = RandomUtil.randomSimpleString();
      Long expiryDelay = RandomUtil.randomPositiveLong();
      Long minExpiryDelay = RandomUtil.randomPositiveLong();
      Long maxExpiryDelay = RandomUtil.randomPositiveLong();
      Boolean sendToDLAOnNoRoute = RandomUtil.randomBoolean();
      Boolean autoCreateDeadLetterResources = RandomUtil.randomBoolean();
      SimpleString deadLetterQueuePrefix = RandomUtil.randomSimpleString();
      SimpleString deadLetterQueueSuffix = RandomUtil.randomSimpleString();
      Long redistributionDelay = RandomUtil.randomPositiveLong();
      Long slowConsumerThreshold = RandomUtil.randomPositiveLong();
      SlowConsumerThresholdMeasurementUnit slowConsumerThresholdMeasurementUnit = SlowConsumerThresholdMeasurementUnit.MESSAGES_PER_MINUTE;
      Long slowConsumerCheckPeriod = RandomUtil.randomPositiveLong();
      SlowConsumerPolicy slowConsumerPolicy = SlowConsumerPolicy.getType(RandomUtil.randomInterval(0, 1));
      Integer managementBrowsePageSize = RandomUtil.randomPositiveInt();
      Integer queuePrefetch = RandomUtil.randomPositiveInt();
      Long maxSizeBytesRejectThreshold = RandomUtil.randomPositiveLong();
      Integer defaultConsumerWindowSize = RandomUtil.randomPositiveInt();
      Boolean defaultGroupRebalance = RandomUtil.randomBoolean();
      Boolean defaultGroupRebalancePauseDispatch = RandomUtil.randomBoolean();
      SimpleString defaultGroupFirstKey = RandomUtil.randomSimpleString();
      Integer defaultGroupBuckets = RandomUtil.randomPositiveInt();
      Long defaultRingSize = RandomUtil.randomPositiveLong();
      Long retroactiveMessageCount = RandomUtil.randomPositiveLong();
      Boolean enableMetrics = RandomUtil.randomBoolean();
      Integer managementMessageAttributeSizeLimit = RandomUtil.randomPositiveInt();
      Boolean enableIngressTimestamp = RandomUtil.randomBoolean();
      Integer iDCacheSize = RandomUtil.randomPositiveInt();

      AddressSettings a = new AddressSettings()
         .setDropMessagesWhenFull(dropMessagesWhenFull)
         .setAutoCreateQueues(autoCreateQueues)
         .setAutoDeleteQueues(autoDeleteQueues)
         .setAutoDeleteCreatedQueues(autoDeleteCreatedQueues)
         .setAutoDeleteQueuesDelay(autoDeleteQueuesDelay)
         .setAutoDeleteQueuesSkipUsageCheck(autoDeleteQueuesSkipUsageCheck)
         .setAutoDeleteQueuesMessageCount(autoDeleteQueuesMessageCount)
         .setConfigDeleteQueues(configDeleteQueues)
         .setAutoCreateAddresses(autoCreateAddresses)
         .setAutoDeleteAddresses(autoDeleteAddresses)
         .setAutoDeleteAddressesDelay(autoDeleteAddressesDelay)
         .setAutoDeleteAddressesSkipUsageCheck(autoDeleteAddressesSkipUsageCheck)
         .setConfigDeleteAddresses(configDeleteAddresses)
         .setConfigDeleteDiverts(configDeleteDiverts)
         .setDefaultMaxConsumers(defaultMaxConsumers)
         .setDefaultConsumersBeforeDispatch(defaultConsumersBeforeDispatch)
         .setDefaultDelayBeforeDispatch(defaultDelayBeforeDispatch)
         .setDefaultPurgeOnNoConsumers(defaultPurgeOnNoConsumers)
         .setDefaultQueueRoutingType(defaultQueueRoutingType)
         .setDefaultAddressRoutingType(defaultAddressRoutingType)
         .setDefaultLastValueQueue(defaultLastValueQueue)
         .setDefaultLastValueKey(defaultLastValueKey)
         .setDefaultNonDestructive(defaultNonDestructive)
         .setDefaultExclusiveQueue(defaultExclusiveQueue)
         .setAddressFullMessagePolicy(addressFullMessagePolicy)
         .setPageSizeBytes(pageSizeBytes)
         .setPageCacheMaxSize(pageCacheMaxSize)
         .setMaxSizeMessages(maxSizeMessages)
         .setMaxSizeBytes(maxSizeBytes)
         .setMaxReadPageMessages(maxReadPageMessages)
         .setPrefetchPageMessages(prefetchPageMessages)
         .setPageLimitBytes(pageLimitBytes)
         .setPageLimitMessages(pageLimitMessages)
         .setPageFullMessagePolicy(pageFullMessagePolicy)
         .setMaxReadPageBytes(maxReadPageBytes)
         .setPrefetchPageBytes(prefetchPageBytes)
         .setMaxDeliveryAttempts(maxDeliveryAttempts)
         .setMessageCounterHistoryDayLimit(messageCounterHistoryDayLimit)
         .setRedeliveryDelay(redeliveryDelay)
         .setRedeliveryMultiplier(redeliveryMultiplier)
         .setRedeliveryCollisionAvoidanceFactor(redeliveryCollisionAvoidanceFactor)
         .setMaxRedeliveryDelay(maxRedeliveryDelay)
         .setDeadLetterAddress(deadLetterAddress)
         .setExpiryAddress(expiryAddress)
         .setAutoCreateExpiryResources(autoCreateExpiryResources)
         .setExpiryQueuePrefix(expiryQueuePrefix)
         .setExpiryQueueSuffix(expiryQueueSuffix)
         .setExpiryDelay(expiryDelay)
         .setMinExpiryDelay(minExpiryDelay)
         .setMaxExpiryDelay(maxExpiryDelay)
         .setSendToDLAOnNoRoute(sendToDLAOnNoRoute)
         .setAutoCreateDeadLetterResources(autoCreateDeadLetterResources)
         .setDeadLetterQueuePrefix(deadLetterQueuePrefix)
         .setDeadLetterQueueSuffix(deadLetterQueueSuffix)
         .setRedistributionDelay(redistributionDelay)
         .setSlowConsumerThreshold(slowConsumerThreshold)
         .setSlowConsumerThresholdMeasurementUnit(slowConsumerThresholdMeasurementUnit)
         .setSlowConsumerCheckPeriod(slowConsumerCheckPeriod)
         .setSlowConsumerPolicy(slowConsumerPolicy)
         .setManagementBrowsePageSize(managementBrowsePageSize)
         .setQueuePrefetch(queuePrefetch)
         .setMaxSizeBytesRejectThreshold(maxSizeBytesRejectThreshold)
         .setDefaultConsumerWindowSize(defaultConsumerWindowSize)
         .setDefaultGroupRebalance(defaultGroupRebalance)
         .setDefaultGroupRebalancePauseDispatch(defaultGroupRebalancePauseDispatch)
         .setDefaultGroupFirstKey(defaultGroupFirstKey)
         .setDefaultGroupBuckets(defaultGroupBuckets)
         .setDefaultRingSize(defaultRingSize)
         .setRetroactiveMessageCount(retroactiveMessageCount)
         .setEnableMetrics(enableMetrics)
         .setManagementMessageAttributeSizeLimit(managementMessageAttributeSizeLimit)
         .setEnableIngressTimestamp(enableIngressTimestamp)
         .setIDCacheSize(iDCacheSize);

      AddressSettings b = AddressSettings.fromJSON(a.toJSON());

      assertEquals(dropMessagesWhenFull, b.getDropMessagesWhenFull());
      assertEquals(autoCreateQueues, b.isAutoCreateQueues());
      assertEquals(autoDeleteQueues, b.isAutoDeleteQueues());
      assertEquals(autoDeleteCreatedQueues, b.isAutoDeleteCreatedQueues());
      assertEquals(autoDeleteQueuesDelay, (Long) b.getAutoDeleteQueuesDelay());
      assertEquals(autoDeleteQueuesSkipUsageCheck, b.getAutoDeleteQueuesSkipUsageCheck());
      assertEquals(autoDeleteQueuesMessageCount, (Long) b.getAutoDeleteQueuesMessageCount());
      assertEquals(configDeleteQueues, b.getConfigDeleteQueues());
      assertEquals(autoCreateAddresses, b.isAutoCreateAddresses());
      assertEquals(autoDeleteAddresses, b.isAutoDeleteAddresses());
      assertEquals(autoDeleteAddressesDelay, (Long) b.getAutoDeleteAddressesDelay());
      assertEquals(autoDeleteAddressesSkipUsageCheck, b.isAutoDeleteAddressesSkipUsageCheck());
      assertEquals(configDeleteAddresses, b.getConfigDeleteAddresses());
      assertEquals(configDeleteDiverts, b.getConfigDeleteDiverts());
      assertEquals(defaultMaxConsumers, b.getDefaultMaxConsumers());
      assertEquals(defaultConsumersBeforeDispatch, b.getDefaultConsumersBeforeDispatch());
      assertEquals(defaultDelayBeforeDispatch, b.getDefaultDelayBeforeDispatch());
      assertEquals(defaultPurgeOnNoConsumers, b.isDefaultPurgeOnNoConsumers());
      assertEquals(defaultQueueRoutingType, b.getDefaultQueueRoutingType());
      assertEquals(defaultAddressRoutingType, b.getDefaultAddressRoutingType());
      assertEquals(defaultLastValueQueue, b.isDefaultLastValueQueue());
      assertEquals(defaultLastValueKey, b.getDefaultLastValueKey());
      assertEquals(defaultNonDestructive, b.isDefaultNonDestructive());
      assertEquals(defaultExclusiveQueue, b.isDefaultExclusiveQueue());
      assertEquals(addressFullMessagePolicy, b.getAddressFullMessagePolicy());
      assertEquals(pageSizeBytes, (Integer) b.getPageSizeBytes());
      assertEquals(pageCacheMaxSize, (Integer) b.getPageCacheMaxSize());
      assertEquals(maxSizeMessages, (Long) b.getMaxSizeMessages());
      assertEquals(maxSizeBytes, (Long) b.getMaxSizeBytes());
      assertEquals(maxReadPageMessages, (Integer) b.getMaxReadPageMessages());
      assertEquals(prefetchPageMessages, (Integer) b.getPrefetchPageMessages());
      assertEquals(pageLimitBytes, b.getPageLimitBytes());
      assertEquals(pageLimitMessages, b.getPageLimitMessages());
      assertEquals(pageFullMessagePolicy, b.getPageFullMessagePolicy());
      assertEquals(maxReadPageBytes, (Integer) b.getMaxReadPageBytes());
      assertEquals(prefetchPageBytes, (Integer) b.getPrefetchPageBytes());
      assertEquals(maxDeliveryAttempts, (Integer) b.getMaxDeliveryAttempts());
      assertEquals(messageCounterHistoryDayLimit, (Integer) b.getMessageCounterHistoryDayLimit());
      assertEquals(redeliveryDelay, (Long) b.getRedeliveryDelay());
      assertEquals(redeliveryMultiplier, (Double) b.getRedeliveryMultiplier());
      assertEquals(redeliveryCollisionAvoidanceFactor, (Double) b.getRedeliveryCollisionAvoidanceFactor());
      assertEquals(maxRedeliveryDelay, (Long) b.getMaxRedeliveryDelay());
      assertEquals(deadLetterAddress, b.getDeadLetterAddress());
      assertEquals(expiryAddress, b.getExpiryAddress());
      assertEquals(autoCreateExpiryResources, b.isAutoCreateExpiryResources());
      assertEquals(expiryQueuePrefix, b.getExpiryQueuePrefix());
      assertEquals(expiryQueueSuffix, b.getExpiryQueueSuffix());
      assertEquals(expiryDelay, b.getExpiryDelay());
      assertEquals(minExpiryDelay, b.getMinExpiryDelay());
      assertEquals(maxExpiryDelay, b.getMaxExpiryDelay());
      assertEquals(sendToDLAOnNoRoute, b.isSendToDLAOnNoRoute());
      assertEquals(autoCreateDeadLetterResources, b.isAutoCreateDeadLetterResources());
      assertEquals(deadLetterQueuePrefix, b.getDeadLetterQueuePrefix());
      assertEquals(deadLetterQueueSuffix, b.getDeadLetterQueueSuffix());
      assertEquals(redistributionDelay, (Long) b.getRedistributionDelay());
      assertEquals(slowConsumerThreshold, (Long) b.getSlowConsumerThreshold());
      assertEquals(slowConsumerThresholdMeasurementUnit, b.getSlowConsumerThresholdMeasurementUnit());
      assertEquals(slowConsumerCheckPeriod, (Long) b.getSlowConsumerCheckPeriod());
      assertEquals(slowConsumerPolicy, b.getSlowConsumerPolicy());
      assertEquals(managementBrowsePageSize, (Integer) b.getManagementBrowsePageSize());
      assertEquals(queuePrefetch, (Integer) b.getQueuePrefetch());
      assertEquals(maxSizeBytesRejectThreshold, (Long) b.getMaxSizeBytesRejectThreshold());
      assertEquals(defaultConsumerWindowSize, (Integer) b.getDefaultConsumerWindowSize());
      assertEquals(defaultGroupRebalance, b.isDefaultGroupRebalance());
      assertEquals(defaultGroupRebalancePauseDispatch, b.isDefaultGroupRebalancePauseDispatch());
      assertEquals(defaultGroupFirstKey, b.getDefaultGroupFirstKey());
      assertEquals(defaultGroupBuckets, (Integer) b.getDefaultGroupBuckets());
      assertEquals(defaultRingSize, (Long) b.getDefaultRingSize());
      assertEquals(retroactiveMessageCount, (Long) b.getRetroactiveMessageCount());
      assertEquals(enableMetrics, b.isEnableMetrics());
      assertEquals(managementMessageAttributeSizeLimit, (Integer) b.getManagementMessageAttributeSizeLimit());
      assertEquals(enableIngressTimestamp, b.isEnableIngressTimestamp());
      assertEquals(iDCacheSize, b.getIDCacheSize());
   }
}
