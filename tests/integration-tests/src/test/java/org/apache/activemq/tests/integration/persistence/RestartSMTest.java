/**
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
package org.apache.activemq.tests.integration.persistence;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.core.persistence.GroupingInfo;
import org.apache.activemq.core.persistence.QueueBindingInfo;
import org.apache.activemq.core.persistence.impl.journal.JournalStorageManager;
import org.apache.activemq.core.postoffice.PostOffice;
import org.apache.activemq.core.server.Queue;
import org.apache.activemq.tests.integration.IntegrationTestLogger;
import org.apache.activemq.tests.unit.core.server.impl.fakes.FakeJournalLoader;
import org.apache.activemq.tests.unit.core.server.impl.fakes.FakePostOffice;
import org.apache.activemq.tests.util.ServiceTestBase;
import org.apache.activemq.utils.ExecutorFactory;
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
