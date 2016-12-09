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
package org.apache.activemq.artemis.tests.unit.core.postoffice.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.persistence.AddressBindingInfo;
import org.apache.activemq.artemis.core.persistence.GroupingInfo;
import org.apache.activemq.artemis.core.persistence.QueueBindingInfo;
import org.apache.activemq.artemis.core.persistence.impl.journal.JournalStorageManager;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.postoffice.impl.DuplicateIDCacheImpl;
import org.apache.activemq.artemis.core.server.impl.PostOfficeJournalLoader;
import org.apache.activemq.artemis.core.transaction.impl.ResourceManagerImpl;
import org.apache.activemq.artemis.tests.unit.core.server.impl.fakes.FakePostOffice;
import org.apache.activemq.artemis.tests.unit.util.FakePagingManager;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.ActiveMQThreadFactory;
import org.apache.activemq.artemis.utils.ExecutorFactory;
import org.apache.activemq.artemis.utils.OrderedExecutorFactory;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DuplicateDetectionUnitTest extends ActiveMQTestBase {
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   ExecutorService executor;

   ExecutorFactory factory;

   @Override
   @After
   public void tearDown() throws Exception {
      executor.shutdown();
      super.tearDown();
   }

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      executor = Executors.newFixedThreadPool(10, ActiveMQThreadFactory.defaultThreadFactory());
      factory = new OrderedExecutorFactory(executor);
   }

   // Public --------------------------------------------------------

   @Test
   public void testReloadDuplication() throws Exception {

      JournalStorageManager journal = null;

      try {
         clearDataRecreateServerDirs();

         SimpleString ADDRESS = new SimpleString("address");

         Configuration configuration = createDefaultInVMConfig();

         PostOffice postOffice = new FakePostOffice();

         ScheduledExecutorService scheduledThreadPool = Executors.newScheduledThreadPool(ActiveMQDefaultConfiguration.getDefaultScheduledThreadPoolMaxSize(), ActiveMQThreadFactory.defaultThreadFactory());

         journal = new JournalStorageManager(configuration, factory, factory);

         journal.start();
         journal.loadBindingJournal(new ArrayList<QueueBindingInfo>(), new ArrayList<GroupingInfo>(), new ArrayList<AddressBindingInfo>());

         HashMap<SimpleString, List<Pair<byte[], Long>>> mapDups = new HashMap<>();

         FakePagingManager pagingManager = new FakePagingManager();
         journal.loadMessageJournal(postOffice, pagingManager, new ResourceManagerImpl(0, 0, scheduledThreadPool), null, mapDups, null, null, new PostOfficeJournalLoader(postOffice, pagingManager, null, null, null, null, null, null));

         Assert.assertEquals(0, mapDups.size());

         DuplicateIDCacheImpl cacheID = new DuplicateIDCacheImpl(ADDRESS, 10, journal, true);

         for (int i = 0; i < 100; i++) {
            cacheID.addToCache(RandomUtil.randomBytes());
         }

         journal.stop();

         journal = new JournalStorageManager(configuration, factory, factory);
         journal.start();
         journal.loadBindingJournal(new ArrayList<QueueBindingInfo>(), new ArrayList<GroupingInfo>(), new ArrayList<AddressBindingInfo>());

         journal.loadMessageJournal(postOffice, pagingManager, new ResourceManagerImpl(0, 0, scheduledThreadPool), null, mapDups, null, null, new PostOfficeJournalLoader(postOffice, pagingManager, null, null, null, null, null, null));

         Assert.assertEquals(1, mapDups.size());

         List<Pair<byte[], Long>> values = mapDups.get(ADDRESS);

         Assert.assertEquals(10, values.size());

         cacheID = new DuplicateIDCacheImpl(ADDRESS, 10, journal, true);
         cacheID.load(values);

         for (int i = 0; i < 100; i++) {
            cacheID.addToCache(RandomUtil.randomBytes(), null);
         }

         journal.stop();

         mapDups.clear();

         journal = new JournalStorageManager(configuration, factory, factory);
         journal.start();
         journal.loadBindingJournal(new ArrayList<QueueBindingInfo>(), new ArrayList<GroupingInfo>(), new ArrayList<AddressBindingInfo>());

         journal.loadMessageJournal(postOffice, pagingManager, new ResourceManagerImpl(0, 0, scheduledThreadPool), null, mapDups, null, null, new PostOfficeJournalLoader(postOffice, pagingManager, null, null, null, null, null, null));

         Assert.assertEquals(1, mapDups.size());

         values = mapDups.get(ADDRESS);

         Assert.assertEquals(10, values.size());

         scheduledThreadPool.shutdown();
      } finally {
         if (journal != null) {
            try {
               journal.stop();
            } catch (Throwable ignored) {
            }
         }
      }

   }
}
