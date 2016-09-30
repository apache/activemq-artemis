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

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.paging.cursor.PageSubscription;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
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

   public QueueFactoryImpl(final ExecutorFactory executorFactory,
                           final ScheduledExecutorService scheduledExecutor,
                           final HierarchicalRepository<AddressSettings> addressSettingsRepository,
                           final StorageManager storageManager) {
      this.addressSettingsRepository = addressSettingsRepository;

      this.scheduledExecutor = scheduledExecutor;

      this.storageManager = storageManager;

      this.executorFactory = executorFactory;
   }

   @Override
   public void setPostOffice(final PostOffice postOffice) {
      this.postOffice = postOffice;
   }

   @Override
   public Queue createQueueWith(final QueueConfig config) {
      final AddressSettings addressSettings = addressSettingsRepository.getMatch(config.address().toString());
      final Queue queue;
      if (addressSettings.isLastValueQueue()) {
         queue = new LastValueQueue(config.id(), config.address(), config.name(), config.filter(), config.pageSubscription(), config.user(), config.isDurable(), config.isTemporary(), config.isAutoCreated(), scheduledExecutor, postOffice, storageManager, addressSettingsRepository, executorFactory.getExecutor());
      } else {
         queue = new QueueImpl(config.id(), config.address(), config.name(), config.filter(), config.pageSubscription(), config.user(), config.isDurable(), config.isTemporary(), config.isAutoCreated(), scheduledExecutor, postOffice, storageManager, addressSettingsRepository, executorFactory.getExecutor());
      }
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
                            final boolean autoCreated) {
      AddressSettings addressSettings = addressSettingsRepository.getMatch(address.toString());

      Queue queue;
      if (addressSettings.isLastValueQueue()) {
         queue = new LastValueQueue(persistenceID, address, name, filter, pageSubscription, user, durable, temporary, autoCreated, scheduledExecutor, postOffice, storageManager, addressSettingsRepository, executorFactory.getExecutor());
      } else {
         queue = new QueueImpl(persistenceID, address, name, filter, pageSubscription, user, durable, temporary, autoCreated, scheduledExecutor, postOffice, storageManager, addressSettingsRepository, executorFactory.getExecutor());
      }

      return queue;
   }
}
