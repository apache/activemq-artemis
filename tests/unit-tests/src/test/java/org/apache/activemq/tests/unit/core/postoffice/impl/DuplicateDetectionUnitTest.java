/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.activemq.tests.unit.core.postoffice.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.activemq.api.config.HornetQDefaultConfiguration;
import org.apache.activemq.api.core.Pair;
import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.core.persistence.GroupingInfo;
import org.apache.activemq.core.persistence.QueueBindingInfo;
import org.apache.activemq.core.persistence.impl.journal.JournalStorageManager;
import org.apache.activemq.core.postoffice.PostOffice;
import org.apache.activemq.core.postoffice.impl.DuplicateIDCacheImpl;
import org.apache.activemq.core.server.impl.PostOfficeJournalLoader;
import org.apache.activemq.core.transaction.impl.ResourceManagerImpl;
import org.apache.activemq.tests.unit.core.server.impl.fakes.FakePostOffice;
import org.apache.activemq.tests.unit.util.FakePagingManager;
import org.apache.activemq.tests.util.RandomUtil;
import org.apache.activemq.tests.util.ServiceTestBase;
import org.apache.activemq.utils.ExecutorFactory;
import org.apache.activemq.utils.OrderedExecutorFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * A DuplicateDetectionUnitTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class DuplicateDetectionUnitTest extends ServiceTestBase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   ExecutorService executor;

   ExecutorFactory factory;

   @Override
   @After
   public void tearDown() throws Exception
   {
      executor.shutdown();
      super.tearDown();
   }

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      executor = Executors.newSingleThreadExecutor();
      factory = new OrderedExecutorFactory(executor);
   }

   // Public --------------------------------------------------------

   @Test
   public void testReloadDuplication() throws Exception
   {

      JournalStorageManager journal = null;

      try
      {
         clearDataRecreateServerDirs();

         SimpleString ADDRESS = new SimpleString("address");

         Configuration configuration = createDefaultConfig();

         PostOffice postOffice = new FakePostOffice();

         ScheduledExecutorService scheduledThreadPool = Executors.newScheduledThreadPool(HornetQDefaultConfiguration.getDefaultScheduledThreadPoolMaxSize());

         journal = new JournalStorageManager(configuration, factory, null);

         journal.start();
         journal.loadBindingJournal(new ArrayList<QueueBindingInfo>(), new ArrayList<GroupingInfo>());

         HashMap<SimpleString, List<Pair<byte[], Long>>> mapDups = new HashMap<SimpleString, List<Pair<byte[], Long>>>();

         FakePagingManager pagingManager = new FakePagingManager();
         journal.loadMessageJournal(postOffice,
                                    pagingManager,
                                    new ResourceManagerImpl(0, 0, scheduledThreadPool),
                                    null,
                                    mapDups,
                                    null,
                                    null,
                                    new PostOfficeJournalLoader(postOffice, pagingManager, null, null, null, null, null, null));

         Assert.assertEquals(0, mapDups.size());

         DuplicateIDCacheImpl cacheID = new DuplicateIDCacheImpl(ADDRESS, 10, journal, true);

         for (int i = 0; i < 100; i++)
         {
            cacheID.addToCache(RandomUtil.randomBytes(), null);
         }

         journal.stop();

         journal = new JournalStorageManager(configuration, factory, null);
         journal.start();
         journal.loadBindingJournal(new ArrayList<QueueBindingInfo>(), new ArrayList<GroupingInfo>());

         journal.loadMessageJournal(postOffice,
                                    pagingManager,
                                    new ResourceManagerImpl(0, 0, scheduledThreadPool),
                                    null,
                                    mapDups,
                                    null,
                                    null,
                                    new PostOfficeJournalLoader(postOffice, pagingManager, null, null, null, null, null, null));

         Assert.assertEquals(1, mapDups.size());

         List<Pair<byte[], Long>> values = mapDups.get(ADDRESS);

         Assert.assertEquals(10, values.size());

         cacheID = new DuplicateIDCacheImpl(ADDRESS, 10, journal, true);
         cacheID.load(values);

         for (int i = 0; i < 100; i++)
         {
            cacheID.addToCache(RandomUtil.randomBytes(), null);
         }

         journal.stop();

         mapDups.clear();

         journal = new JournalStorageManager(configuration, factory, null);
         journal.start();
         journal.loadBindingJournal(new ArrayList<QueueBindingInfo>(), new ArrayList<GroupingInfo>());

         journal.loadMessageJournal(postOffice,
                                    pagingManager,
                                    new ResourceManagerImpl(0, 0, scheduledThreadPool),
                                    null,
                                    mapDups,
                                    null,
                                    null,
                                    new PostOfficeJournalLoader(postOffice, pagingManager, null, null, null, null, null, null));

         Assert.assertEquals(1, mapDups.size());

         values = mapDups.get(ADDRESS);

         Assert.assertEquals(10, values.size());
      }
      finally
      {
         if (journal != null)
         {
            try
            {
               journal.stop();
            }
            catch (Throwable ignored)
            {
            }
         }
      }

   }
}
