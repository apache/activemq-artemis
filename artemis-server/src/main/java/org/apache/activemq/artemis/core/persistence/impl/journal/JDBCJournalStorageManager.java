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
package org.apache.activemq.artemis.core.persistence.impl.journal;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.storage.DatabaseStorageConfiguration;
import org.apache.activemq.artemis.core.io.IOCriticalErrorListener;
import org.apache.activemq.artemis.core.journal.Journal;
import org.apache.activemq.artemis.jdbc.store.journal.JDBCJournalImpl;
import org.apache.activemq.artemis.utils.ExecutorFactory;

public class JDBCJournalStorageManager extends JournalStorageManager {

   public JDBCJournalStorageManager(Configuration config, ExecutorFactory executorFactory) {
      super(config, executorFactory);
   }

   public JDBCJournalStorageManager(final Configuration config,
                                final ExecutorFactory executorFactory,
                                final IOCriticalErrorListener criticalErrorListener) {
      super(config, executorFactory, criticalErrorListener);
   }

   @Override
   protected void init(Configuration config, IOCriticalErrorListener criticalErrorListener) {
      DatabaseStorageConfiguration dbConf = (DatabaseStorageConfiguration) config.getStoreConfiguration();

      Journal localBindings = new JDBCJournalImpl(dbConf.getJdbcConnectionUrl(), dbConf.getBindingsTableName(), dbConf.getJdbcDriverClassName());
      bindingsJournal = localBindings;

      Journal localMessage = new JDBCJournalImpl(dbConf.getJdbcConnectionUrl(), dbConf.getMessageTableName(), dbConf.getJdbcDriverClassName());
      messageJournal = localMessage;
   }

   @Override
   public synchronized void stop(boolean ioCriticalError) throws Exception {
      if (!started) {
         return;
      }

      if (!ioCriticalError) {
         performCachedLargeMessageDeletes();
         // Must call close to make sure last id is persisted
         if (journalLoaded && idGenerator != null)
            idGenerator.persistCurrentID();
      }

      final CountDownLatch latch = new CountDownLatch(1);
      executor.execute(new Runnable() {
         @Override
         public void run() {
            latch.countDown();
         }
      });

      latch.await(30, TimeUnit.SECONDS);

      beforeStop();

      ((JDBCJournalImpl) bindingsJournal).stop(false);

      messageJournal.stop();

      singleThreadExecutor.shutdown();

      journalLoaded = false;

      started = false;
   }

}
