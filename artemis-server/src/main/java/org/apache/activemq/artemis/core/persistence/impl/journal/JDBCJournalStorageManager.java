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

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.storage.DatabaseStorageConfiguration;
import org.apache.activemq.artemis.core.io.IOCriticalErrorListener;
import org.apache.activemq.artemis.core.io.nio.NIOSequentialFileFactory;
import org.apache.activemq.artemis.core.journal.Journal;
import org.apache.activemq.artemis.core.server.files.FileStoreMonitor;
import org.apache.activemq.artemis.jdbc.store.file.JDBCSequentialFileFactory;
import org.apache.activemq.artemis.jdbc.store.journal.JDBCJournalImpl;
import org.apache.activemq.artemis.utils.ExecutorFactory;

public class JDBCJournalStorageManager extends JournalStorageManager {

   public JDBCJournalStorageManager(Configuration config,
                                    ExecutorFactory executorFactory,
                                    ScheduledExecutorService scheduledExecutorService) {
      super(config, executorFactory, scheduledExecutorService);
   }

   public JDBCJournalStorageManager(final Configuration config,
                                    final ScheduledExecutorService scheduledExecutorService,
                                    final ExecutorFactory executorFactory,
                                    final IOCriticalErrorListener criticalErrorListener) {
      super(config, executorFactory, scheduledExecutorService, criticalErrorListener);
   }

   @Override
   protected synchronized void init(Configuration config, IOCriticalErrorListener criticalErrorListener) {
      try {
         DatabaseStorageConfiguration dbConf = (DatabaseStorageConfiguration) config.getStoreConfiguration();

         Journal localBindings = new JDBCJournalImpl(dbConf.getJdbcConnectionUrl(), dbConf.getBindingsTableName(), dbConf.getJdbcDriverClassName(), scheduledExecutorService, executorFactory.getExecutor());
         bindingsJournal = localBindings;

         Journal localMessage = new JDBCJournalImpl(dbConf.getJdbcConnectionUrl(), dbConf.getMessageTableName(), dbConf.getJdbcDriverClassName(), scheduledExecutorService, executorFactory.getExecutor());
         messageJournal = localMessage;

         bindingsJournal.start();
         messageJournal.start();

         largeMessagesFactory = new JDBCSequentialFileFactory(dbConf.getJdbcConnectionUrl(), dbConf.getLargeMessageTableName(), dbConf.getJdbcDriverClassName(), executorFactory.getExecutor());
         largeMessagesFactory.start();
      }
      catch (Exception e) {
         criticalErrorListener.onIOException(e, e.getMessage(), null);
      }
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

      bindingsJournal.stop();
      messageJournal.stop();
      largeMessagesFactory.stop();

      singleThreadExecutor.shutdown();

      journalLoaded = false;

      started = false;
   }

   @Override
   public ByteBuffer allocateDirectBuffer(int size) {
      return NIOSequentialFileFactory.allocateDirectByteBuffer(size);
   }

   @Override
   public void freeDirectBuffer(ByteBuffer buffer) {
   }

   @Override
   public void injectMonitor(FileStoreMonitor monitor) throws Exception {
   }
}
