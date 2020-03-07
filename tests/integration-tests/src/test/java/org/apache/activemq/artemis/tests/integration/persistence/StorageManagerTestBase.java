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
package org.apache.activemq.artemis.tests.integration.persistence;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.StoreConfiguration;
import org.apache.activemq.artemis.core.persistence.AddressBindingInfo;
import org.apache.activemq.artemis.core.persistence.GroupingInfo;
import org.apache.activemq.artemis.core.persistence.QueueBindingInfo;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.persistence.impl.journal.JDBCJournalStorageManager;
import org.apache.activemq.artemis.core.persistence.impl.journal.JournalStorageManager;
import org.apache.activemq.artemis.jms.persistence.JMSStorageManager;
import org.apache.activemq.artemis.jms.persistence.impl.journal.JMSJournalStorageManagerImpl;
import org.apache.activemq.artemis.tests.unit.core.server.impl.fakes.FakeJournalLoader;
import org.apache.activemq.artemis.tests.unit.core.server.impl.fakes.FakePostOffice;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.ExecutorFactory;
import org.apache.activemq.artemis.utils.TimeAndCounterIDGenerator;
import org.apache.activemq.artemis.utils.critical.EmptyCriticalAnalyzer;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public abstract class StorageManagerTestBase extends ActiveMQTestBase {

   protected ExecutorService executor;

   protected ExecutorFactory execFactory;

   protected ScheduledExecutorService scheduledExecutorService;

   protected StorageManager journal;

   protected JMSStorageManager jmsJournal;

   protected StoreConfiguration.StoreType storeType;

   public StorageManagerTestBase(StoreConfiguration.StoreType storeType) {
      this.storeType = storeType;
   }

   @Parameterized.Parameters(name = "storeType={0}")
   public static Collection<Object[]> data() {
      Object[][] params = new Object[][]{{StoreConfiguration.StoreType.FILE}, {StoreConfiguration.StoreType.DATABASE}};
      return Arrays.asList(params);
   }

   @Override
   @Before
   public void setUp() throws Exception {
      if (storeType == StoreConfiguration.StoreType.DATABASE) {
         Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
      }

      super.setUp();

      execFactory = getOrderedExecutor();

      scheduledExecutorService = new ScheduledThreadPoolExecutor(5);
   }

   @Override
   @After
   public void tearDown() throws Exception {
      Exception exception = null;

      if (journal != null) {
         try {
            journal.stop();
         } catch (Exception e) {
            exception = e;
         }

         journal = null;
      }

      if (jmsJournal != null) {
         try {
            jmsJournal.stop();
         } catch (Exception e) {
            if (exception != null)
               exception = e;
         }

         jmsJournal = null;
      }

      scheduledExecutorService.shutdown();

      destroyTables(Arrays.asList(new String[]{"MESSAGE", "BINDINGS", "LARGE_MESSAGE", "NODE_MANAGER_STORE"}));
      super.tearDown();
      if (exception != null)
         throw exception;
   }

   /**
    * @throws Exception
    */
   protected void createStorage() throws Exception {

      if (storeType == StoreConfiguration.StoreType.DATABASE) {
         journal = createJDBCJournalStorageManager(createDefaultJDBCConfig(true));
      } else {
         journal = createJournalStorageManager(createDefaultInVMConfig());
      }

      journal.start();

      journal.loadBindingJournal(new ArrayList<QueueBindingInfo>(), new ArrayList<GroupingInfo>(), new ArrayList<AddressBindingInfo>());

      journal.loadMessageJournal(new FakePostOffice(), null, null, null, null, null, null, new FakeJournalLoader());
   }

   /**
    * @param configuration
    */
   protected JournalStorageManager createJournalStorageManager(Configuration configuration) {
      JournalStorageManager jsm = new JournalStorageManager(configuration, EmptyCriticalAnalyzer.getInstance(), execFactory, execFactory);
      addActiveMQComponent(jsm);
      return jsm;
   }

   /**
    * @param configuration
    */
   protected JDBCJournalStorageManager createJDBCJournalStorageManager(Configuration configuration) {
      JDBCJournalStorageManager jsm = new JDBCJournalStorageManager(configuration, EmptyCriticalAnalyzer.getInstance(), execFactory, execFactory, scheduledExecutorService);
      addActiveMQComponent(jsm);
      return jsm;
   }

   /**
    * @throws Exception
    */
   protected void createJMSStorage() throws Exception {
      jmsJournal = new JMSJournalStorageManagerImpl(null, new TimeAndCounterIDGenerator(), createDefaultInVMConfig(), null);
      addActiveMQComponent(jmsJournal);
      jmsJournal.start();
      jmsJournal.load();
   }

}
