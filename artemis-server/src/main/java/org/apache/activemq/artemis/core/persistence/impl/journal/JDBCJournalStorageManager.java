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
import java.sql.Connection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.storage.DatabaseStorageConfiguration;
import org.apache.activemq.artemis.core.io.IOCriticalErrorListener;
import org.apache.activemq.artemis.core.io.nio.NIOSequentialFileFactory;
import org.apache.activemq.artemis.jdbc.store.drivers.JDBCUtils;
import org.apache.activemq.artemis.jdbc.store.file.JDBCSequentialFileFactory;
import org.apache.activemq.artemis.jdbc.store.journal.JDBCJournalImpl;
import org.apache.activemq.artemis.jdbc.store.sql.PropertySQLProvider;
import org.apache.activemq.artemis.jdbc.store.sql.SQLProvider;
import org.apache.activemq.artemis.utils.ExecutorFactory;
import org.apache.activemq.artemis.utils.critical.CriticalAnalyzer;

public class JDBCJournalStorageManager extends JournalStorageManager {

   private Connection connection;

   public JDBCJournalStorageManager(Configuration config,
                                    CriticalAnalyzer analyzer,
                                    ExecutorFactory executorFactory,
                                    ExecutorFactory ioExecutorFactory,
                                    ScheduledExecutorService scheduledExecutorService) {
      super(config, analyzer, executorFactory, scheduledExecutorService, ioExecutorFactory);
   }

   public JDBCJournalStorageManager(final Configuration config,
                                    final CriticalAnalyzer analyzer,
                                    final ScheduledExecutorService scheduledExecutorService,
                                    final ExecutorFactory executorFactory,
                                    final ExecutorFactory ioExecutorFactory,
                                    final IOCriticalErrorListener criticalErrorListener) {
      super(config, analyzer, executorFactory, scheduledExecutorService, ioExecutorFactory, criticalErrorListener);
   }

   @Override
   protected synchronized void init(Configuration config, IOCriticalErrorListener criticalErrorListener) {
      try {
         final DatabaseStorageConfiguration dbConf = (DatabaseStorageConfiguration) config.getStoreConfiguration();
         final JDBCJournalImpl bindingsJournal;
         final JDBCJournalImpl messageJournal;
         final JDBCSequentialFileFactory largeMessagesFactory;
         if (dbConf.getDataSource() != null) {
            SQLProvider.Factory sqlProviderFactory = dbConf.getSqlProviderFactory();
            if (sqlProviderFactory == null) {
               sqlProviderFactory = new PropertySQLProvider.Factory(dbConf.getDataSource());
            }
            bindingsJournal = new JDBCJournalImpl(dbConf.getDataSource(), sqlProviderFactory.create(dbConf.getBindingsTableName(), SQLProvider.DatabaseStoreType.BINDINGS_JOURNAL), dbConf.getBindingsTableName(), scheduledExecutorService, executorFactory.getExecutor(), criticalErrorListener);
            messageJournal = new JDBCJournalImpl(dbConf.getDataSource(), sqlProviderFactory.create(dbConf.getMessageTableName(), SQLProvider.DatabaseStoreType.MESSAGE_JOURNAL), dbConf.getMessageTableName(), scheduledExecutorService, executorFactory.getExecutor(), criticalErrorListener);
            largeMessagesFactory = new JDBCSequentialFileFactory(dbConf.getDataSource(), sqlProviderFactory.create(dbConf.getLargeMessageTableName(), SQLProvider.DatabaseStoreType.LARGE_MESSAGE), executorFactory.getExecutor(), criticalErrorListener);
         } else {
            String driverClassName = dbConf.getJdbcDriverClassName();
            bindingsJournal = new JDBCJournalImpl(dbConf.getJdbcConnectionUrl(), driverClassName, JDBCUtils.getSQLProvider(driverClassName, dbConf.getBindingsTableName(), SQLProvider.DatabaseStoreType.BINDINGS_JOURNAL), scheduledExecutorService, executorFactory.getExecutor(), criticalErrorListener);
            messageJournal = new JDBCJournalImpl(dbConf.getJdbcConnectionUrl(), driverClassName, JDBCUtils.getSQLProvider(driverClassName, dbConf.getMessageTableName(), SQLProvider.DatabaseStoreType.MESSAGE_JOURNAL), scheduledExecutorService, executorFactory.getExecutor(), criticalErrorListener);
            largeMessagesFactory = new JDBCSequentialFileFactory(dbConf.getJdbcConnectionUrl(), driverClassName, JDBCUtils.getSQLProvider(driverClassName, dbConf.getLargeMessageTableName(), SQLProvider.DatabaseStoreType.LARGE_MESSAGE), executorFactory.getExecutor(), criticalErrorListener);
         }
         final int networkTimeout = dbConf.getJdbcNetworkTimeout();
         if (networkTimeout >= 0) {
            bindingsJournal.setNetworkTimeout(executorFactory.getExecutor(), networkTimeout);
         }
         if (networkTimeout >= 0) {
            messageJournal.setNetworkTimeout(executorFactory.getExecutor(), networkTimeout);
         }
         if (networkTimeout >= 0) {
            largeMessagesFactory.setNetworkTimeout(executorFactory.getExecutor(), networkTimeout);
         }
         this.bindingsJournal = bindingsJournal;
         this.messageJournal = messageJournal;
         this.largeMessagesFactory = largeMessagesFactory;
         largeMessagesFactory.start();
      } catch (Exception e) {
         criticalErrorListener.onIOException(e, e.getMessage(), null);
      }
   }

   @Override
   public synchronized void stop(boolean ioCriticalError, boolean sendFailover) throws Exception {
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
