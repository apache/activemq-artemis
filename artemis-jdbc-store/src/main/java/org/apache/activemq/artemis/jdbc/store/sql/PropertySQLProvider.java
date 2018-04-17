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

package org.apache.activemq.artemis.jdbc.store.sql;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.activemq.artemis.jdbc.store.journal.JDBCJournalImpl;
import org.jboss.logging.Logger;

import static java.lang.String.format;

/**
 * Property-based implementation of a {@link SQLProvider}'s factory.
 *
 * Properties are stored in a journal-sql.properties.
 *
 * Dialects specific to a database can be customized by suffixing the property keys with the name of the dialect.
 */
public class PropertySQLProvider implements SQLProvider {

   private enum LetterCase implements Function<String, String> {
      upper(String::toUpperCase),
      lower(String::toLowerCase),
      none(Function.identity());

      private final Function<String, String> transform;

      LetterCase(Function<String, String> transform) {
         this.transform = transform;
      }

      @Override
      public String apply(String s) {
         return transform.apply(s);
      }

      public static LetterCase parse(String value) {
         return LetterCase.valueOf(value);
      }
   }

   private static final int STATE_ROW_ID = 0;
   private static final int LIVE_LOCK_ROW_ID = 1;
   private static final int BACKUP_LOCK_ROW_ID = 2;
   private static final int NODE_ID_ROW_ID = 3;

   private final String tableName;
   private final Factory.SQLDialect dialect;
   private volatile Properties sql;

   protected PropertySQLProvider(Factory.SQLDialect dialect, String tableName, Properties sqlProperties) {
      this.dialect = dialect;
      this.sql = sqlProperties;
      final LetterCase tableNamesCase = LetterCase.parse(sql("table-names-case", dialect, sqlProperties));
      this.tableName = tableNamesCase.apply(tableName);
   }

   @Override
   public long getMaxBlobSize() {
      return Long.valueOf(sql("max-blob-size"));
   }

   @Override
   public String[] getCreateJournalTableSQL() {
      return new String[] {
              format(sql("create-journal-table"), tableName),
              format(sql("create-journal-index"), tableName),
      };
   }

   @Override
   public String getInsertJournalRecordsSQL() {
      return format(sql("insert-journal-record"), tableName);
   }

   @Override
   public String getSelectJournalRecordsSQL() {
      return format(sql("select-journal-record"), tableName);
   }

   @Override
   public String getDeleteJournalRecordsSQL() {
      return format(sql("delete-journal-record"), tableName);
   }

   @Override
   public String getDeleteJournalTxRecordsSQL() {
      return format(sql("delete-journal-tx-record"), tableName);
   }

   @Override
   public String getTableName() {
      return tableName;
   }

   @Override
   public String getCreateFileTableSQL() {
      return format(sql("create-file-table"), tableName);
   }

   @Override
   public String getInsertFileSQL() {
      return format(sql("insert-file"), tableName);
   }

   @Override
   public String getSelectFileNamesByExtensionSQL() {
      return format(sql("select-filenames-by-extension"), tableName);
   }

   @Override
   public String getSelectFileByFileName() {
      return format(sql("select-file-by-filename"), tableName);
   }

   @Override
   public String getAppendToLargeObjectSQL() {
      return format(sql("append-to-file"), tableName);
   }

   @Override
   public String getReadLargeObjectSQL() {
      return format(sql("read-large-object"), tableName);
   }

   @Override
   public String getDeleteFileSQL() {
      return format(sql("delete-file"), tableName);
   }

   @Override
   public String getUpdateFileNameByIdSQL() {
      return format(sql("update-filename-by-id"), tableName);
   }

   @Override
   public String getCopyFileRecordByIdSQL() {
      return format(sql("copy-file-record-by-id"), tableName);
   }

   @Override
   public String getDropFileTableSQL() {
      return format(sql("drop-table"), tableName);
   }

   @Override
   public String getCloneFileRecordByIdSQL() {
      return format(sql("clone-file-record"), tableName);
   }

   @Override
   public String getCountJournalRecordsSQL() {
      return format(sql("count-journal-record"), tableName);
   }

   @Override
   public boolean closeConnectionOnShutdown() {
      return Boolean.valueOf(sql("close-connection-on-shutdown"));
   }

   @Override
   public String createNodeManagerStoreTableSQL() {
      return format(sql("create-node-manager-store-table"), tableName);
   }

   @Override
   public String createStateSQL() {
      return format(sql("create-state"), tableName, STATE_ROW_ID);
   }

   @Override
   public String createNodeIdSQL() {
      return format(sql("create-state"), tableName, NODE_ID_ROW_ID);
   }

   @Override
   public String createLiveLockSQL() {
      return format(sql("create-state"), tableName, LIVE_LOCK_ROW_ID);
   }

   @Override
   public String createBackupLockSQL() {
      return format(sql("create-state"), tableName, BACKUP_LOCK_ROW_ID);
   }

   @Override
   public String tryAcquireLiveLockSQL() {
      return format(sql("try-acquire-lock"), tableName, LIVE_LOCK_ROW_ID);
   }

   @Override
   public String tryAcquireBackupLockSQL() {
      return format(sql("try-acquire-lock"), tableName, BACKUP_LOCK_ROW_ID);
   }

   @Override
   public String tryReleaseLiveLockSQL() {
      return format(sql("try-release-lock"), tableName, LIVE_LOCK_ROW_ID);
   }

   @Override
   public String tryReleaseBackupLockSQL() {
      return format(sql("try-release-lock"), tableName, BACKUP_LOCK_ROW_ID);
   }

   @Override
   public String isLiveLockedSQL() {
      return format(sql("is-locked"), tableName, LIVE_LOCK_ROW_ID);
   }

   @Override
   public String isBackupLockedSQL() {
      return format(sql("is-locked"), tableName, BACKUP_LOCK_ROW_ID);
   }

   @Override
   public String renewLiveLockSQL() {
      return format(sql("renew-lock"), tableName, LIVE_LOCK_ROW_ID);
   }

   @Override
   public String renewBackupLockSQL() {
      return format(sql("renew-lock"), tableName, BACKUP_LOCK_ROW_ID);
   }

   @Override
   public String currentTimestampSQL() {
      return format(sql("current-timestamp"), tableName);
   }

   @Override
   public String writeStateSQL() {
      return format(sql("write-state"), tableName, STATE_ROW_ID);
   }

   @Override
   public String readStateSQL() {
      return format(sql("read-state"), tableName, STATE_ROW_ID);
   }

   @Override
   public String writeNodeIdSQL() {
      return format(sql("write-nodeId"), tableName, NODE_ID_ROW_ID);
   }

   @Override
   public String readNodeIdSQL() {
      return format(sql("read-nodeId"), tableName, NODE_ID_ROW_ID);
   }

   @Override
   public String initializeNodeIdSQL() {
      return format(sql("initialize-nodeId"), tableName, NODE_ID_ROW_ID);
   }

   private String sql(final String key) {
      return sql(key, dialect, sql);
   }

   private static String sql(final String key, final Factory.SQLDialect dialect, final Properties sql) {
      if (dialect != null) {
         String result = sql.getProperty(key + "." + dialect.getKey());
         if (result != null) {
            return result;
         }
      }
      String result = sql.getProperty(key);
      return result;
   }

   public static final class Factory implements SQLProvider.Factory {

      private static final Logger logger = Logger.getLogger(JDBCJournalImpl.class);
      private static final String SQL_PROPERTIES_FILE = "journal-sql.properties";
      // can be null if no known dialect has been identified
      private SQLDialect dialect;
      private final Properties sql;

      public enum SQLDialect {
         ORACLE("oracle", "oracle"),
         POSTGRESQL("postgresql", "postgres"),
         DERBY("derby", "derby"),
         MYSQL("mysql", "mysql"),
         DB2("db2", "db2"),
         HSQL("hsql", "hsql", "hypersonic"),
         H2("h2", "h2"),
         MSSQL("mssql", "microsoft"),
         SYBASE("jconnect", "jconnect");

         private final String key;
         private final String[] driverKeys;

         SQLDialect(String key, String... driverKeys) {
            this.key = key;
            this.driverKeys = driverKeys;
         }

         String getKey() {
            return key;
         }

         private boolean match(String driverName) {
            for (String driverKey : driverKeys) {
               if (driverName.contains(driverKey)) {
                  return true;
               }
            }
            return false;
         }

         /**
          * Return null if no known dialect has been identified.
          */
         public static SQLDialect identifyDialect(String name) {
            if (name == null) {
               return null;
            }
            //use a lower case name to make it more resilient
            final String lowerCaseName = name.toLowerCase();
            return Stream.of(SQLDialect.values())
               .filter(dialect -> dialect.match(lowerCaseName))
               .findFirst()
               .orElse(null);
         }
      }

      public Factory(SQLDialect dialect) {
         this.dialect = dialect;
         try (InputStream stream = PropertySQLProvider.class.getClassLoader().getResourceAsStream(SQL_PROPERTIES_FILE)) {
            sql = new Properties();
            sql.load(stream);
         } catch (IOException e) {
            throw new RuntimeException("Unable to load properties from " + SQL_PROPERTIES_FILE);
         }
      }

      public Factory(DataSource dataSource) {
         this(investigateDialect(dataSource));
      }

      public static SQLDialect investigateDialect(Connection connection) {
         SQLDialect dialect = null;
         try {
            DatabaseMetaData metaData = connection.getMetaData();
            String dbProduct = metaData.getDatabaseProductName();
            dialect = identifyDialect(dbProduct);

            if (dialect == null) {
               logger.debug("Attempting to guess on driver name.");
               dialect = identifyDialect(metaData.getDriverName());
            }
            if (dialect == null) {
               logger.warnf("Unable to detect database dialect from connection metadata or JDBC driver name.");
            } else {
               logger.debugf("Detect database dialect as '%s'.", dialect);
            }
         } catch (Exception e) {
            logger.debug("Unable to read JDBC metadata.", e);
         }
         return dialect;
      }

      private static SQLDialect investigateDialect(DataSource dataSource) {
         try (Connection connection = dataSource.getConnection()) {
            return investigateDialect(connection);
         } catch (Exception e) {
            logger.debug("Unable to read JDBC metadata.", e);
            return null;
         }
      }

      public static SQLDialect identifyDialect(String name) {
         return SQLDialect.identifyDialect(name);
      }

      @Override
      public SQLProvider create(String tableName, DatabaseStoreType dbStoreType) {
         if (dialect == SQLDialect.ORACLE) {
            return new Oracle12CSQLProvider(tableName, sql, dbStoreType);
         } else {
            return new PropertySQLProvider(dialect, tableName, sql);
         }
      }
   }

}
