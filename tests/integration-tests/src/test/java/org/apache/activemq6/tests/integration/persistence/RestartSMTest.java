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
package org.apache.activemq6.tests.integration.persistence;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.activemq6.core.config.Configuration;
import org.apache.activemq6.core.persistence.GroupingInfo;
import org.apache.activemq6.core.persistence.QueueBindingInfo;
import org.apache.activemq6.core.persistence.impl.journal.JournalStorageManager;
import org.apache.activemq6.core.postoffice.PostOffice;
import org.apache.activemq6.core.server.Queue;
import org.apache.activemq6.tests.integration.IntegrationTestLogger;
import org.apache.activemq6.tests.unit.core.server.impl.fakes.FakeJournalLoader;
import org.apache.activemq6.tests.unit.core.server.impl.fakes.FakePostOffice;
import org.apache.activemq6.tests.util.ServiceTestBase;
import org.apache.activemq6.utils.ExecutorFactory;
import org.junit.Before;
import org.junit.Test;

/**
 * A DeleteMessagesRestartTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 * Created Mar 2, 2009 10:14:38 AM
 *
 *
 */
public class RestartSMTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------
   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   // Attributes ----------------------------------------------------

   ExecutorService executor;

   ExecutorFactory execFactory;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      execFactory = getOrderedExecutor();
   }

   @Test
   public void testRestartStorageManager() throws Exception
   {
      File testdir = new File(getTestDir());
      deleteDirectory(testdir);

      Configuration configuration = createDefaultConfig();

      PostOffice postOffice = new FakePostOffice();

      final JournalStorageManager journal = new JournalStorageManager(configuration, execFactory, null);

      try
      {

         journal.start();

         List<QueueBindingInfo> queueBindingInfos = new ArrayList<QueueBindingInfo>();

         journal.loadBindingJournal(queueBindingInfos, new ArrayList<GroupingInfo>());

         Map<Long, Queue> queues = new HashMap<Long, Queue>();

         journal.loadMessageJournal(postOffice, null, null, null, null, null, null, new FakeJournalLoader());

         journal.stop();

         deleteDirectory(testdir);

         journal.start();

         queues = new HashMap<Long, Queue>();

         journal.loadMessageJournal(postOffice, null, null, null, null, null, null, new FakeJournalLoader());

         queueBindingInfos = new ArrayList<QueueBindingInfo>();

         journal.loadBindingJournal(queueBindingInfos, new ArrayList<GroupingInfo>());

         journal.start();
      }
      finally
      {

         try
         {
            journal.stop();
         }
         catch (Exception ex)
         {
            RestartSMTest.log.warn(ex.getMessage(), ex);
         }
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
