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
package org.apache.activemq.tests.integration.persistence;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.core.persistence.GroupingInfo;
import org.apache.activemq.core.persistence.QueueBindingInfo;
import org.apache.activemq.core.persistence.impl.journal.JournalStorageManager;
import org.apache.activemq.core.server.Queue;
import org.apache.activemq.jms.persistence.JMSStorageManager;
import org.apache.activemq.jms.persistence.impl.journal.JMSJournalStorageManagerImpl;
import org.apache.activemq.tests.unit.core.server.impl.fakes.FakeJournalLoader;
import org.apache.activemq.tests.unit.core.server.impl.fakes.FakePostOffice;
import org.apache.activemq.tests.util.ServiceTestBase;
import org.apache.activemq.utils.ExecutorFactory;
import org.apache.activemq.utils.TimeAndCounterIDGenerator;
import org.junit.After;
import org.junit.Before;

/**
 * A StorageManagerTestBase
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public abstract class StorageManagerTestBase extends ServiceTestBase
{

   protected ExecutorService executor;

   protected ExecutorFactory execFactory;

   protected JournalStorageManager journal;

   protected JMSStorageManager jmsJournal;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      execFactory = getOrderedExecutor();
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      Exception exception = null;

      if (journal != null)
      {
         try
         {
            journal.stop();
         }
         catch (Exception e)
         {
            exception = e;
         }

         journal = null;
      }

      if (jmsJournal != null)
      {
         try
         {
            jmsJournal.stop();
         }
         catch (Exception e)
         {
            if (exception != null)
               exception = e;
         }

         jmsJournal = null;
      }

      super.tearDown();
      if (exception != null)
         throw exception;
   }

   /**
    * @throws Exception
    */
   protected void createStorage() throws Exception
   {
      Configuration configuration = createDefaultConfig();

      journal = createJournalStorageManager(configuration);

      journal.start();

      journal.loadBindingJournal(new ArrayList<QueueBindingInfo>(), new ArrayList<GroupingInfo>());

      Map<Long, Queue> queues = new HashMap<Long, Queue>();

      journal.loadMessageJournal(new FakePostOffice(), null, null, null, null, null, null, new FakeJournalLoader());
   }

   /**
    * @param configuration
    */
   protected JournalStorageManager createJournalStorageManager(Configuration configuration)
   {
      JournalStorageManager jsm = new JournalStorageManager(configuration, execFactory, null);
      addHornetQComponent(jsm);
      return jsm;
   }

   /**
    * @throws Exception
    */
   protected void createJMSStorage() throws Exception
   {
      Configuration configuration = createDefaultConfig();

      jmsJournal = new JMSJournalStorageManagerImpl(new TimeAndCounterIDGenerator(), configuration, null);

      jmsJournal.start();

      jmsJournal.load();
   }
}
