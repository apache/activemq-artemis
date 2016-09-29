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
import org.apache.activemq.artemis.jdbc.store.JDBCUtils;
import org.apache.activemq.artemis.jdbc.store.file.JDBCSequentialFileFactory;
import org.apache.activemq.artemis.jdbc.store.journal.JDBCJournalImpl;
import org.apache.activemq.artemis.jdbc.store.sql.GenericSQLProvider;
import org.apache.activemq.artemis.jdbc.store.sql.SQLProvider;
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

         if (dbConf.getDataSource() != null) {
            SQLProvider.Factory sqlProviderFactory = dbConf.getSqlProviderFactory();
            if (sqlProviderFactory == null) {
               sqlProviderFactory = new GenericSQLProvider.Factory();
            }
            bindingsJournal = new JDBCJournalImpl(dbConf.getDataSource(), sqlProviderFactory.create(dbConf.getBindingsTableName()), dbConf.getBindingsTableName(), scheduledExecutorService, executorFactory.getExecutor());
            messageJournal = new JDBCJournalImpl(dbConf.getDataSource(), sqlProviderFactory.create(dbConf.getMessageTableName()), dbConf.getMessageTableName(), scheduledExecutorService, executorFactory.getExecutor());
            largeMessagesFactory = new JDBCSequentialFileFactory(dbConf.getDataSource(), sqlProviderFactory.create(dbConf.getLargeMessageTableName()), dbConf.getLargeMessageTableName(), executor);
         } else {
            String driverClassName = dbConf.getJdbcDriverClassName();
            bindingsJournal = new JDBCJournalImpl(dbConf.getJdbcConnectionUrl(), driverClassName, JDBCUtils.getSQLProvider(driverClassName, dbConf.getBindingsTableName()), scheduledExecutorService, executorFactory.getExecutor());
            messageJournal = new JDBCJournalImpl(dbConf.getJdbcConnectionUrl(), driverClassName, JDBCUtils.getSQLProvider(driverClassName, dbConf.getMessageTableName()), scheduledExecutorService, executorFactory.getExecutor());
            largeMessagesFactory = new JDBCSequentialFileFactory(dbConf.getJdbcConnectionUrl(), driverClassName, JDBCUtils.getSQLProvider(driverClassName, dbConf.getLargeMessageTableName()), executor);
         }
         largeMessagesFactory.start();
      } catch (Exception e) {
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
}
