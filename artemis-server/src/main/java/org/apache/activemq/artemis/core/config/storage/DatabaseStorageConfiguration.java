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
package org.apache.activemq.artemis.core.config.storage;

import javax.sql.DataSource;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.core.config.StoreConfiguration;
import org.apache.activemq.artemis.jdbc.store.sql.SQLProvider;

public class DatabaseStorageConfiguration implements StoreConfiguration {

   private String messageTableName = ActiveMQDefaultConfiguration.getDefaultMessageTableName();

   private String bindingsTableName = ActiveMQDefaultConfiguration.getDefaultBindingsTableName();

   private String largeMessagesTableName = ActiveMQDefaultConfiguration.getDefaultLargeMessagesTableName();

   private String pageStoreTableName = ActiveMQDefaultConfiguration.getDefaultPageStoreTableName();

   private String nodeManagerStoreTableName = ActiveMQDefaultConfiguration.getDefaultNodeManagerStoreTableName();

   private String jdbcConnectionUrl = ActiveMQDefaultConfiguration.getDefaultDatabaseUrl();

   private String jdbcDriverClassName = ActiveMQDefaultConfiguration.getDefaultDriverClassName();

   private DataSource dataSource;

   private SQLProvider.Factory sqlProviderFactory;

   private int jdbcNetworkTimeout = ActiveMQDefaultConfiguration.getDefaultJdbcNetworkTimeout();

   private long jdbcLockRenewPeriodMillis = ActiveMQDefaultConfiguration.getDefaultJdbcLockRenewPeriodMillis();

   private long jdbcLockExpirationMillis = ActiveMQDefaultConfiguration.getDefaultJdbcLockExpirationMillis();

   private long jdbcLockAcquisitionTimeoutMillis = ActiveMQDefaultConfiguration.getDefaultJdbcLockAcquisitionTimeoutMillis();

   private long jdbcJournalSyncPeriodMillis = ActiveMQDefaultConfiguration.getDefaultJdbcJournalSyncPeriodMillis();

   @Override
   public StoreType getStoreType() {
      return StoreType.DATABASE;
   }

   public String getMessageTableName() {
      return messageTableName;
   }

   public void setMessageTableName(String messageTableName) {
      this.messageTableName = messageTableName;
   }

   public String getBindingsTableName() {
      return bindingsTableName;
   }

   public void setBindingsTableName(String bindingsTableName) {
      this.bindingsTableName = bindingsTableName;
   }

   public String getLargeMessageTableName() {
      return largeMessagesTableName;
   }

   public void setLargeMessageTableName(String largeMessagesTableName) {
      this.largeMessagesTableName = largeMessagesTableName;
   }

   public String getPageStoreTableName() {
      return pageStoreTableName;
   }

   public void setNodeManagerStoreTableName(String nodeManagerStoreTableName) {
      this.nodeManagerStoreTableName = nodeManagerStoreTableName;
   }

   public String getNodeManagerStoreTableName() {
      return nodeManagerStoreTableName;
   }

   public void setPageStoreTableName(String pageStoreTableName) {
      this.pageStoreTableName = pageStoreTableName;
   }

   public void setJdbcConnectionUrl(String jdbcConnectionUrl) {
      this.jdbcConnectionUrl = jdbcConnectionUrl;
   }

   public String getJdbcConnectionUrl() {
      return jdbcConnectionUrl;
   }

   public void setJdbcDriverClassName(String jdbcDriverClassName) {
      this.jdbcDriverClassName = jdbcDriverClassName;
   }

   public String getJdbcDriverClassName() {
      return jdbcDriverClassName;
   }

   /**
    * The DataSource to use to store Artemis data in the data store (can be {@code null} if {@code jdbcConnectionUrl} and {@code jdbcDriverClassName} are used instead).
    *
    * @return the DataSource used to store Artemis data in the JDBC data store.
    */
   public DataSource getDataSource() {
      return dataSource;
   }

   /**
    * Configure the DataSource to use to store Artemis data in the data store.
    *
    * @param dataSource
    */
   public void setDataSource(DataSource dataSource) {
      this.dataSource = dataSource;
   }

   /**
    * The {@link SQLProvider.Factory} used to communicate with the JDBC data store.
    * It can be {@code null}. If the value is {@code null} and {@code dataSource} is set, the {@code {@link org.apache.activemq.artemis.jdbc.store.sql.PropertySQLProvider.Factory}} will be used,
    * else the type of the factory will be determined based on the {@code jdbcDriverClassName}.
    *
    * @return the factory used to communicate with the JDBC data store.
    */
   public SQLProvider.Factory getSqlProviderFactory() {
      return sqlProviderFactory;
   }

   public void setSqlProvider(SQLProvider.Factory sqlProviderFactory) {
      this.sqlProviderFactory = sqlProviderFactory;
   }

   public int getJdbcNetworkTimeout() {
      return this.jdbcNetworkTimeout;
   }

   public void setJdbcNetworkTimeout(int jdbcNetworkTimeout) {
      this.jdbcNetworkTimeout = jdbcNetworkTimeout;
   }

   public long getJdbcLockRenewPeriodMillis() {
      return jdbcLockRenewPeriodMillis;
   }

   public void setJdbcLockRenewPeriodMillis(long jdbcLockRenewPeriodMillis) {
      this.jdbcLockRenewPeriodMillis = jdbcLockRenewPeriodMillis;
   }

   public long getJdbcLockExpirationMillis() {
      return jdbcLockExpirationMillis;
   }

   public void setJdbcLockExpirationMillis(long jdbcLockExpirationMillis) {
      this.jdbcLockExpirationMillis = jdbcLockExpirationMillis;
   }

   public long getJdbcLockAcquisitionTimeoutMillis() {
      return jdbcLockAcquisitionTimeoutMillis;
   }

   public void setJdbcLockAcquisitionTimeoutMillis(long jdbcLockAcquisitionTimeoutMillis) {
      this.jdbcLockAcquisitionTimeoutMillis = jdbcLockAcquisitionTimeoutMillis;
   }

   public long getJdbcJournalSyncPeriodMillis() {
      return jdbcJournalSyncPeriodMillis;
   }

   public void setJdbcJournalSyncPeriodMillis(long jdbcJournalSyncPeriodMillis) {
      this.jdbcJournalSyncPeriodMillis = jdbcJournalSyncPeriodMillis;
   }
}
