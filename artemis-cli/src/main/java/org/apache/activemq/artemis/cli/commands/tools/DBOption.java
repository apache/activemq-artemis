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

package org.apache.activemq.artemis.cli.commands.tools;

import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;

import io.airlift.airline.Option;
import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.cli.commands.ActionContext;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.FileConfiguration;
import org.apache.activemq.artemis.core.config.storage.DatabaseStorageConfiguration;
import org.apache.activemq.artemis.core.paging.PagingManager;
import org.apache.activemq.artemis.core.paging.PagingStoreFactory;
import org.apache.activemq.artemis.core.paging.impl.PagingManagerImpl;
import org.apache.activemq.artemis.core.paging.impl.PagingStoreFactoryDatabase;
import org.apache.activemq.artemis.core.paging.impl.PagingStoreFactoryNIO;
import org.apache.activemq.artemis.core.persistence.impl.journal.JDBCJournalStorageManager;
import org.apache.activemq.artemis.core.persistence.impl.journal.JournalStorageManager;
import org.apache.activemq.artemis.core.server.JournalType;
import org.apache.activemq.artemis.core.settings.HierarchicalRepository;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.core.settings.impl.HierarchicalObjectRepository;
import org.apache.activemq.artemis.utils.ActiveMQThreadFactory;
import org.apache.activemq.artemis.utils.ExecutorFactory;
import org.apache.activemq.artemis.utils.actors.OrderedExecutorFactory;
import org.apache.activemq.artemis.utils.critical.EmptyCriticalAnalyzer;

public class DBOption extends OptionalLocking {

   protected JournalStorageManager storageManager;

   protected Configuration config;

   protected ExecutorService executor;

   protected ExecutorFactory executorFactory;

   protected ScheduledExecutorService scheduledExecutorService;

   @Option(name = "--output", description = "Output name for the file")
   private String output;

   @Option(name = "--jdbc", description = "It will activate jdbc")
   Boolean jdbc;

   @Option(name = "--jdbc-bindings-table-name", description = "Name of the jdbc bindigns table")
   private String jdbcBindings = ActiveMQDefaultConfiguration.getDefaultBindingsTableName();

   @Option(name = "--jdbc-message-table-name", description = "Name of the jdbc messages table")
   private String jdbcMessages = ActiveMQDefaultConfiguration.getDefaultLargeMessagesTableName();

   @Option(name = "--jdbc-large-message-table-name", description = "Name of the large messages table")
   private String jdbcLargeMessages = ActiveMQDefaultConfiguration.getDefaultLargeMessagesTableName();

   @Option(name = "--jdbc-page-store-table-name", description = "Name of the page store messages table")
   private String jdbcPageStore = ActiveMQDefaultConfiguration.getDefaultPageStoreTableName();

   @Option(name = "--jdbc-node-manager-table-name", description = "Name of the jdbc node manager table")
   private String jdbcNodeManager = ActiveMQDefaultConfiguration.getDefaultNodeManagerStoreTableName();

   @Option(name = "--jdbc-connection-url", description = "The connection used for the database")
   private String jdbcURL = null;

   @Option(name = "--jdbc-driver-class-name", description = "JDBC driver classname")
   private String jdbcClassName = ActiveMQDefaultConfiguration.getDefaultDriverClassName();

   public boolean isJDBC() throws Exception {
      parseDBConfig();
      return jdbc;
   }

   public String getJdbcBindings() throws Exception {
      parseDBConfig();
      return jdbcBindings;
   }

   public DBOption setJdbcBindings(String jdbcBindings) {
      this.jdbcBindings = jdbcBindings;
      return this;
   }

   public String getJdbcMessages() throws Exception {
      parseDBConfig();
      return jdbcMessages;
   }

   public DBOption setJdbcMessages(String jdbcMessages) {
      this.jdbcMessages = jdbcMessages;
      return this;
   }

   public String getJdbcLargeMessages() throws Exception {
      parseDBConfig();
      return jdbcLargeMessages;
   }

   public DBOption setJdbcLargeMessages(String jdbcLargeMessages) {
      this.jdbcLargeMessages = jdbcLargeMessages;
      return this;
   }

   public String getJdbcPageStore() throws Exception {
      parseDBConfig();
      return jdbcPageStore;
   }

   public DBOption setJdbcPageStore(String jdbcPageStore) {
      this.jdbcPageStore = jdbcPageStore;
      return this;
   }

   public String getJdbcNodeManager() throws Exception {
      parseDBConfig();
      return jdbcNodeManager;
   }

   public DBOption setJdbcNodeManager(String jdbcNodeManager) {
      this.jdbcNodeManager = jdbcNodeManager;
      return this;
   }

   public String getJdbcURL() throws Exception {
      parseDBConfig();
      return jdbcURL;
   }

   public DBOption setJdbcURL(String jdbcURL) {
      this.jdbcURL = jdbcURL;
      return this;
   }

   public String getJdbcClassName() throws Exception {
      parseDBConfig();
      return jdbcClassName;
   }

   public DBOption setJdbcClassName(String jdbcClassName) {
      this.jdbcClassName = jdbcClassName;
      return this;
   }


   @Override
   public Object execute(ActionContext context) throws Exception {
      super.execute(context);

      if (output != null) {
         FileOutputStream fileOutputStream = new FileOutputStream(output);
         PrintStream printStream = new PrintStream(fileOutputStream);
         context.out = printStream;

         Runtime.getRuntime().addShutdownHook(new Thread(printStream::close));
      }
      return null;
   }

   private void parseDBConfig() throws Exception {
      if (jdbc == null) {
         FileConfiguration fileConfiguration = getFileConfiguration();
         jdbc = fileConfiguration.isJDBC();
         if (jdbc) {
            DatabaseStorageConfiguration storageConfiguration = (DatabaseStorageConfiguration) fileConfiguration.getStoreConfiguration();
            jdbcBindings = storageConfiguration.getBindingsTableName();
            jdbcMessages = storageConfiguration.getMessageTableName();
            jdbcLargeMessages = storageConfiguration.getLargeMessageTableName();
            jdbcPageStore = storageConfiguration.getPageStoreTableName();
            jdbcNodeManager = storageConfiguration.getNodeManagerStoreTableName();
            jdbcURL = storageConfiguration.getJdbcConnectionUrl();
            jdbcClassName = storageConfiguration.getJdbcDriverClassName();
         }
      }
   }

   // Get a new configuration based on the passed parameters and not on the parsed configuration
   public Configuration getParameterConfiguration() throws Exception {
      Configuration configuration = readConfiguration();
      if (isJDBC()) {
         DatabaseStorageConfiguration storageConfiguration = new DatabaseStorageConfiguration();
         storageConfiguration.setJdbcConnectionUrl(getJdbcURL());
         storageConfiguration.setJdbcDriverClassName(getJdbcClassName());
         storageConfiguration.setBindingsTableName(getJdbcBindings());
         storageConfiguration.setMessageTableName(getJdbcMessages());
         storageConfiguration.setLargeMessageTableName(getJdbcLargeMessages());
         storageConfiguration.setPageStoreTableName(getJdbcPageStore());
         storageConfiguration.setNodeManagerStoreTableName(getJdbcNodeManager());
         configuration.setStoreConfiguration(storageConfiguration);
      } else {
         configuration.setBindingsDirectory(getBinding());
         configuration.setJournalDirectory(getJournal());
         configuration.setPagingDirectory(getPaging());
         configuration.setLargeMessagesDirectory(getLargeMessages());
         configuration.setJournalType(JournalType.NIO);
      }

      return configuration;
   }

   protected PagingManager pagingmanager;

   protected void initializeJournal(Configuration configuration) throws Exception {
      this.config = configuration;
      executor = Executors.newFixedThreadPool(5, ActiveMQThreadFactory.defaultThreadFactory());
      executorFactory = new OrderedExecutorFactory(executor);

      scheduledExecutorService = new ScheduledThreadPoolExecutor(configuration.getScheduledThreadPoolMaxSize(), new ThreadFactory() {
         @Override
         public Thread newThread(Runnable r) {
            return new Thread(r);
         }
      });

      HierarchicalRepository<AddressSettings> addressSettingsRepository = new HierarchicalObjectRepository<>(config.getWildcardConfiguration());
      addressSettingsRepository.setDefault(new AddressSettings());

      if (configuration.isJDBC()) {
         storageManager = new JDBCJournalStorageManager(config, null, scheduledExecutorService, executorFactory, executorFactory, null);

         PagingStoreFactory pageStoreFactory = new PagingStoreFactoryDatabase((DatabaseStorageConfiguration) configuration.getStoreConfiguration(),
                                                                              storageManager, 1000L,
                                                                              scheduledExecutorService, executorFactory,
                                                                             false, null);
         pagingmanager = new PagingManagerImpl(pageStoreFactory, addressSettingsRepository);
      } else {
         storageManager = new JournalStorageManager(config, EmptyCriticalAnalyzer.getInstance(), executorFactory, executorFactory);
         PagingStoreFactory pageStoreFactory = new PagingStoreFactoryNIO(storageManager, config.getPagingLocation(), 1000L, scheduledExecutorService, executorFactory, true, null);
         pagingmanager = new PagingManagerImpl(pageStoreFactory, addressSettingsRepository);

      }

   }

   protected void cleanup() throws Exception {
      executor.shutdown();
      scheduledExecutorService.shutdown();

      storageManager.stop();
   }

}
