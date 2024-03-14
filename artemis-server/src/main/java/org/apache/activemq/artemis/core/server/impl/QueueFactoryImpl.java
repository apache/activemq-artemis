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
package org.apache.activemq.artemis.core.server.impl;

import java.util.concurrent.ScheduledExecutorService;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.paging.PagingManager;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.paging.cursor.PageSubscription;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.QueueConfig;
import org.apache.activemq.artemis.core.server.QueueFactory;
import org.apache.activemq.artemis.core.settings.HierarchicalRepository;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.utils.ExecutorFactory;

/**
 * A QueueFactoryImpl
 */
public class QueueFactoryImpl implements QueueFactory {

   protected final HierarchicalRepository<AddressSettings> addressSettingsRepository;

   protected final ScheduledExecutorService scheduledExecutor;

   /**
    * This is required for delete-all-reference to work correctly with paging, and controlling global-size
    */
   protected PostOffice postOffice;

   protected final StorageManager storageManager;

   protected final ExecutorFactory executorFactory;

   protected final ActiveMQServer server;

   public QueueFactoryImpl(final ExecutorFactory executorFactory,
                           final ScheduledExecutorService scheduledExecutor,
                           final HierarchicalRepository<AddressSettings> addressSettingsRepository,
                           final StorageManager storageManager,
                           final ActiveMQServer server) {

      this.addressSettingsRepository = addressSettingsRepository;
      this.scheduledExecutor = scheduledExecutor;
      this.storageManager = storageManager;
      this.executorFactory = executorFactory;
      this.server = server;
   }

   @Override
   public void setPostOffice(final PostOffice postOffice) {
      this.postOffice = postOffice;
   }

   @Deprecated
   @Override
   public Queue createQueueWith(final QueueConfig config) {
      final Queue queue;
      if (lastValueKey(config) != null) {
         queue = new LastValueQueue(config.id(), config.address(), config.name(), config.filter(), config.getPagingStore(), config.pageSubscription(), config.user(), config.isDurable(), config.isTemporary(), config.isAutoCreated(), config.deliveryMode(), config.maxConsumers(), config.isExclusive(), config.isGroupRebalance(), config.getGroupBuckets(), config.getGroupFirstKey(), config.consumersBeforeDispatch(), config.delayBeforeDispatch(), config.isPurgeOnNoConsumers(), lastValueKey(config), config.isNonDestructive(), config.isAutoDelete(), config.getAutoDeleteDelay(), config.getAutoDeleteMessageCount(), config.isConfigurationManaged(), scheduledExecutor, postOffice, storageManager, addressSettingsRepository, executorFactory.getExecutor(), server, this);
      } else {
         queue = new QueueImpl(config.id(), config.address(), config.name(), config.filter(), config.getPagingStore(), config.pageSubscription(), config.user(), config.isDurable(), config.isTemporary(), config.isAutoCreated(), config.deliveryMode(), config.maxConsumers(), config.isExclusive(), config.isGroupRebalance(), config.getGroupBuckets(), config.getGroupFirstKey(), config.isNonDestructive(), config.consumersBeforeDispatch(), config.delayBeforeDispatch(), config.isPurgeOnNoConsumers(), config.isAutoDelete(), config.getAutoDeleteDelay(), config.getAutoDeleteMessageCount(), config.isConfigurationManaged(), config.getRingSize(), scheduledExecutor, postOffice, storageManager, addressSettingsRepository, executorFactory.getExecutor(), server, this);
      }
      server.getCriticalAnalyzer().add(queue);
      return queue;
   }

   @Override
   public Queue createQueueWith(final QueueConfiguration config, PagingManager pagingManager, Filter filter) {
      validateState(config);
      final Queue queue;
      PageSubscription pageSubscription = getPageSubscription(config, pagingManager, filter);
      if (lastValueKey(config) != null) {
         queue = new LastValueQueue(config.setLastValueKey(lastValueKey(config)), filter, pageSubscription != null ? pageSubscription.getPagingStore() : null, pageSubscription, scheduledExecutor, postOffice, storageManager, addressSettingsRepository, executorFactory.getExecutor(), server, this);
      } else {
         queue = new QueueImpl(config, filter, pageSubscription != null ? pageSubscription.getPagingStore() : null, pageSubscription, scheduledExecutor, postOffice, storageManager, addressSettingsRepository, executorFactory.getExecutor(), server, this);
      }
      server.getCriticalAnalyzer().add(queue);
      return queue;
   }

   @Deprecated
   @Override
   public Queue createQueue(final long persistenceID,
                            final SimpleString address,
                            final SimpleString name,
                            final Filter filter,
                            final PageSubscription pageSubscription,
                            final SimpleString user,
                            final boolean durable,
                            final boolean temporary,
                            final boolean autoCreated) throws Exception {

      // Add default address info if one doesn't exist
      postOffice.addAddressInfo(new AddressInfo(address));

      AddressSettings addressSettings = addressSettingsRepository.getMatch(address.toString());

      Queue queue;
      if (lastValueKey(addressSettings) != null) {
         queue = new LastValueQueue(persistenceID, address, name, filter, pageSubscription == null ? null : pageSubscription.getPagingStore(), pageSubscription, user, durable, temporary, autoCreated, ActiveMQDefaultConfiguration.getDefaultRoutingType(), ActiveMQDefaultConfiguration.getDefaultMaxQueueConsumers(), ActiveMQDefaultConfiguration.getDefaultExclusive(), ActiveMQDefaultConfiguration.getDefaultGroupRebalance(), ActiveMQDefaultConfiguration.getDefaultGroupBuckets(), ActiveMQDefaultConfiguration.getDefaultGroupFirstKey(), ActiveMQDefaultConfiguration.getDefaultConsumersBeforeDispatch(), ActiveMQDefaultConfiguration.getDefaultDelayBeforeDispatch(), ActiveMQDefaultConfiguration.getDefaultPurgeOnNoConsumers(), lastValueKey(addressSettings), ActiveMQDefaultConfiguration.getDefaultNonDestructive(), ActiveMQDefaultConfiguration.getDefaultQueueAutoDelete(autoCreated), ActiveMQDefaultConfiguration.getDefaultQueueAutoDeleteDelay(), ActiveMQDefaultConfiguration.getDefaultQueueAutoDeleteMessageCount(),false, scheduledExecutor, postOffice, storageManager, addressSettingsRepository, executorFactory.getExecutor(), server, this);
      } else {
         queue = new QueueImpl(persistenceID, address, name, filter, pageSubscription == null ? null : pageSubscription.getPagingStore(), pageSubscription, user, durable, temporary, autoCreated, scheduledExecutor, postOffice, storageManager, addressSettingsRepository, executorFactory.getExecutor(), server, this);
      }

      server.getCriticalAnalyzer().add(queue);

      return queue;
   }

   @Override
   public void queueRemoved(Queue queue) {
      server.getCriticalAnalyzer().remove(queue);
   }

   public static PageSubscription getPageSubscription(QueueConfiguration queueConfiguration, PagingManager pagingManager, Filter filter) {
      PageSubscription pageSubscription;

      try {
         PagingStore pageStore = pagingManager.getPageStore(queueConfiguration.getAddress());
         if (pageStore != null) {
            pageSubscription = pageStore.getCursorProvider().createSubscription(queueConfiguration.getId(), filter, queueConfiguration.isDurable());
         } else {
            pageSubscription = null;
         }
      } catch (Exception e) {
         throw new IllegalStateException(e);
      }

      return pageSubscription;
   }

   private static SimpleString lastValueKey(final QueueConfig config) {
      if (config.lastValueKey() != null && !config.lastValueKey().isEmpty()) {
         return config.lastValueKey();
      } else if (config.isLastValue()) {
         return Message.HDR_LAST_VALUE_NAME;
      } else {
         return null;
      }
   }

   private static SimpleString lastValueKey(final QueueConfiguration config) {
      if (config.getLastValueKey() != null && !config.getLastValueKey().isEmpty()) {
         return config.getLastValueKey();
      } else if (config.isLastValue()) {
         return Message.HDR_LAST_VALUE_NAME;
      } else {
         return null;
      }
   }

   private static SimpleString lastValueKey(final AddressSettings addressSettings) {
      if (addressSettings.getDefaultLastValueKey() != null && !addressSettings.getDefaultLastValueKey().isEmpty()) {
         return addressSettings.getDefaultLastValueKey();
      } else if (addressSettings.isDefaultLastValueQueue()) {
         return Message.HDR_LAST_VALUE_NAME;
      } else  {
         return null;
      }
   }

   private void validateState(QueueConfiguration config) {
      if (isEmptyOrNull(config.getName())) {
         throw new IllegalStateException("name can't be null or empty!");
      }
      if (isEmptyOrNull(config.getAddress())) {
         throw new IllegalStateException("address can't be null or empty!");
      }
   }

   private static boolean isEmptyOrNull(SimpleString value) {
      return (value == null || value.length() == 0);
   }
}
