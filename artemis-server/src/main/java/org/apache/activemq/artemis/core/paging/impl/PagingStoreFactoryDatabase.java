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
package org.apache.activemq.artemis.core.paging.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.storage.DatabaseStorageConfiguration;
import org.apache.activemq.artemis.core.io.IOCriticalErrorListener;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.core.paging.PagingManager;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.paging.PagingStoreFactory;
import org.apache.activemq.artemis.core.paging.cursor.PageCursorProvider;
import org.apache.activemq.artemis.core.paging.cursor.impl.PageCursorProviderImpl;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.server.files.FileStoreMonitor;
import org.apache.activemq.artemis.core.settings.HierarchicalRepository;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.jdbc.store.drivers.JDBCUtils;
import org.apache.activemq.artemis.jdbc.store.file.JDBCSequentialFile;
import org.apache.activemq.artemis.jdbc.store.file.JDBCSequentialFileFactory;
import org.apache.activemq.artemis.jdbc.store.file.JDBCSequentialFileFactoryDriver;
import org.apache.activemq.artemis.jdbc.store.sql.PropertySQLProvider;
import org.apache.activemq.artemis.jdbc.store.sql.SQLProvider;
import org.apache.activemq.artemis.utils.ExecutorFactory;
import org.apache.activemq.artemis.utils.actors.ArtemisExecutor;

/**
 * Integration point between Paging and JDBC
 */
public class PagingStoreFactoryDatabase implements PagingStoreFactory {

   // Constants -----------------------------------------------------

   private static final String ADDRESS_FILE = "address.txt";

   private static final String DIRECTORY_NAME = "directory.txt";

   // Attributes ----------------------------------------------------

   protected final boolean syncNonTransactional;

   private PagingManager pagingManager;

   private final ScheduledExecutorService scheduledExecutor;

   private final long syncTimeout;

   protected final StorageManager storageManager;

   private JDBCSequentialFileFactoryDriver dbDriver;

   private DatabaseStorageConfiguration dbConf;

   private ExecutorFactory executorFactory;

   private JDBCSequentialFileFactory pagingFactoryFileFactory;

   private JDBCSequentialFile directoryList;

   @Override
   public ScheduledExecutorService getScheduledExecutor() {
      return scheduledExecutor;
   }

   @Override
   public Executor newExecutor() {
      return executorFactory.getExecutor();
   }

   private boolean started = false;

   private final IOCriticalErrorListener criticalErrorListener;

   public PagingStoreFactoryDatabase(final DatabaseStorageConfiguration dbConf,
                                     final StorageManager storageManager,
                                     final long syncTimeout,
                                     final ScheduledExecutorService scheduledExecutor,
                                     final ExecutorFactory executorFactory,
                                     final boolean syncNonTransactional,
                                     final IOCriticalErrorListener critialErrorListener) throws Exception {
      this.storageManager = storageManager;
      this.executorFactory = executorFactory;
      this.syncNonTransactional = syncNonTransactional;
      this.scheduledExecutor = scheduledExecutor;
      this.syncTimeout = syncTimeout;
      this.dbConf = dbConf;
      this.criticalErrorListener = critialErrorListener;
      start();
   }

   public synchronized void start() throws Exception {
      if (!started) {
         //fix to prevent page table names to be longer than 30 chars (upper limit for Oracle12c identifiers length)
         final String pageStoreTableNamePrefix = dbConf.getPageStoreTableName();
         if (pageStoreTableNamePrefix.length() > 10) {
            throw new IllegalStateException("The maximum name size for the page store table prefix is 10 characters: THE PAGING STORE CAN'T START");
         }
         if (dbConf.getDataSource() != null) {
            SQLProvider.Factory sqlProviderFactory = dbConf.getSqlProviderFactory();
            if (sqlProviderFactory == null) {
               sqlProviderFactory = new PropertySQLProvider.Factory(dbConf.getDataSource());
            }
            pagingFactoryFileFactory = new JDBCSequentialFileFactory(dbConf.getDataSource(), sqlProviderFactory.create(pageStoreTableNamePrefix, SQLProvider.DatabaseStoreType.PAGE), executorFactory.getExecutor(), criticalErrorListener);
         } else {
            String driverClassName = dbConf.getJdbcDriverClassName();
            pagingFactoryFileFactory = new JDBCSequentialFileFactory(dbConf.getJdbcConnectionUrl(), driverClassName, JDBCUtils.getSQLProvider(driverClassName, pageStoreTableNamePrefix, SQLProvider.DatabaseStoreType.PAGE), executorFactory.getExecutor(), criticalErrorListener);
         }
         final int jdbcNetworkTimeout = dbConf.getJdbcNetworkTimeout();
         if (jdbcNetworkTimeout >= 0) {
            pagingFactoryFileFactory.setNetworkTimeout(this.executorFactory.getExecutor(), jdbcNetworkTimeout);
         }
         pagingFactoryFileFactory.start();
         started = true;
      }
   }
   // Public --------------------------------------------------------

   @Override
   public synchronized void stop() {
      if (started) {
         pagingFactoryFileFactory.stop();
         started = false;
      }
   }

   @Override
   public void injectMonitor(FileStoreMonitor monitor) throws Exception {
   }

   @Override
   public PageCursorProvider newCursorProvider(PagingStore store,
                                               StorageManager storageManager,
                                               AddressSettings addressSettings,
                                               ArtemisExecutor executor) {
      return new PageCursorProviderImpl(store, storageManager, executor, addressSettings.getPageCacheMaxSize());
   }

   @Override
   public synchronized PagingStore newStore(final SimpleString address, final AddressSettings settings) {

      return new PagingStoreImpl(address, scheduledExecutor, syncTimeout, pagingManager, storageManager, null, this, address, settings, executorFactory.getExecutor(), syncNonTransactional);
   }

   @Override
   public synchronized SequentialFileFactory newFileFactory(final SimpleString address) throws Exception {
      String tableName = "" + storageManager.generateID();
      SequentialFileFactory factory = newFileFactory(tableName, true);
      factory.start();

      SequentialFile file = factory.createSequentialFile(PagingStoreFactoryDatabase.ADDRESS_FILE);
      file.open();

      ActiveMQBuffer buffer = ActiveMQBuffers.fixedBuffer(SimpleString.sizeofNullableString(address));
      buffer.writeSimpleString(address);
      file.write(buffer, true);
      file.close();
      return factory;
   }

   @Override
   public void setPagingManager(final PagingManager pagingManager) {
      this.pagingManager = pagingManager;
   }

   @Override
   public synchronized List<PagingStore> reloadStores(final HierarchicalRepository<AddressSettings> addressSettingsRepository) throws Exception {
      // We assume the directory list < Integer.MAX_VALUE (this is only a list of addresses).
      JDBCSequentialFile directoryList = (JDBCSequentialFile) pagingFactoryFileFactory.createSequentialFile(DIRECTORY_NAME);
      directoryList.open();

      int size = ((Long) directoryList.size()).intValue();
      ActiveMQBuffer buffer = readActiveMQBuffer(directoryList, size);

      ArrayList<PagingStore> storesReturn = new ArrayList<>();

      while (buffer.readableBytes() > 0) {
         SimpleString table = buffer.readSimpleString();

         JDBCSequentialFileFactory factory = (JDBCSequentialFileFactory) newFileFactory(table.toString(), false);
         factory.start();

         JDBCSequentialFile addressFile = (JDBCSequentialFile) factory.createSequentialFile(ADDRESS_FILE);
         addressFile.open();

         size = ((Long) addressFile.size()).intValue();
         if (size == 0) {
            continue;
         }

         ActiveMQBuffer addrBuffer = readActiveMQBuffer(addressFile, size);
         SimpleString address = addrBuffer.readSimpleString();

         AddressSettings settings = addressSettingsRepository.getMatch(address.toString());

         PagingStore store = new PagingStoreImpl(address, scheduledExecutor, syncTimeout, pagingManager, storageManager, factory, this, address, settings, executorFactory.getExecutor(), syncNonTransactional);

         storesReturn.add(store);
      }
      directoryList.close();
      return storesReturn;
   }

   private synchronized SequentialFileFactory newFileFactory(final String directoryName, boolean writeToDirectory) throws Exception {
      JDBCSequentialFile directoryList = (JDBCSequentialFile) pagingFactoryFileFactory.createSequentialFile(DIRECTORY_NAME);
      directoryList.open();
      SimpleString simpleString = SimpleString.toSimpleString(directoryName);
      ActiveMQBuffer buffer = ActiveMQBuffers.fixedBuffer(simpleString.sizeof());
      buffer.writeSimpleString(simpleString);
      if (writeToDirectory) directoryList.write(buffer, true);
      directoryList.close();

      final SQLProvider sqlProvider;
      if (dbConf.getDataSource() != null) {
         final SQLProvider.Factory sqlProviderFactory;
         if (dbConf.getSqlProviderFactory() != null) {
            sqlProviderFactory = dbConf.getSqlProviderFactory();
         } else {
            sqlProviderFactory = new PropertySQLProvider.Factory(dbConf.getDataSource());
         }
         sqlProvider = sqlProviderFactory.create(getTableNameForGUID(directoryName), SQLProvider.DatabaseStoreType.PAGE);
      } else {
         sqlProvider = JDBCUtils.getSQLProvider(dbConf.getJdbcDriverClassName(), getTableNameForGUID(directoryName), SQLProvider.DatabaseStoreType.PAGE);
      }
      final JDBCSequentialFileFactory fileFactory = new JDBCSequentialFileFactory(pagingFactoryFileFactory.getDbDriver().getConnection(), sqlProvider, executorFactory.getExecutor(), criticalErrorListener);
      final int jdbcNetworkTimeout = dbConf.getJdbcNetworkTimeout();
      if (jdbcNetworkTimeout >= 0) {
         fileFactory.setNetworkTimeout(this.executorFactory.getExecutor(), jdbcNetworkTimeout);
      }
      return fileFactory;
   }

   private String getTableNameForGUID(String guid) {
      return dbConf.getPageStoreTableName() + guid.replace("-", "");
   }

   private ActiveMQBuffer readActiveMQBuffer(SequentialFile file, int size) throws Exception {
      ByteBuffer byteBuffer = ByteBuffer.allocate(size);
      byteBuffer.mark();
      file.read(byteBuffer);
      byteBuffer.reset();

      ActiveMQBuffer buffer = ActiveMQBuffers.wrappedBuffer(byteBuffer);
      buffer.writerIndex(size);
      return buffer;
   }
}
