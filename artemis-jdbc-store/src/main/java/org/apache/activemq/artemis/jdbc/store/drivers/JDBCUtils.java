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
package org.apache.activemq.artemis.jdbc.store.drivers;

import java.sql.SQLException;
import java.util.Map;

import org.apache.activemq.artemis.jdbc.store.sql.PropertySQLProvider;
import org.apache.activemq.artemis.jdbc.store.sql.SQLProvider;
import org.jboss.logging.Logger;

public class JDBCUtils {

   private static final Logger logger = Logger.getLogger(JDBCUtils.class);

   public static SQLProvider.Factory getSQLProviderFactory(String url) {
      PropertySQLProvider.Factory.SQLDialect dialect = PropertySQLProvider.Factory.identifyDialect(url);
      logger.tracef("getSQLProvider Returning SQL provider for dialect %s for url::%s", dialect, url);
      return new PropertySQLProvider.Factory(dialect);
   }

   public static SQLProvider getSQLProvider(String driverClass, String tableName, SQLProvider.DatabaseStoreType storeType) {
      PropertySQLProvider.Factory.SQLDialect dialect = PropertySQLProvider.Factory.identifyDialect(driverClass);
      logger.tracef("getSQLProvider Returning SQL provider for dialect %s for driver::%s, tableName::%s", dialect, driverClass, tableName);
      PropertySQLProvider.Factory factory = new PropertySQLProvider.Factory(dialect);
      return factory.create(tableName, storeType);
   }

   public static SQLProvider getSQLProvider(Map<String, Object> dataSourceProperties, String tableName, SQLProvider.DatabaseStoreType storeType) {
      PropertySQLProvider.Factory.SQLDialect dialect = PropertySQLProvider.Factory.investigateDialect(dataSourceProperties);
      logger.tracef("getSQLProvider Returning SQL provider for dialect %s, tableName::%s", dialect, tableName);
      PropertySQLProvider.Factory factory = new PropertySQLProvider.Factory(dialect);
      return factory.create(tableName, storeType);
   }

   /**
    * Append to {@code errorMessage} a detailed description of the provided {@link SQLException}.<br>
    * The information appended are:
    * <ul>
    * <li>SQL STATEMENTS</li>
    * <li>SQL EXCEPTIONS details ({@link SQLException#getSQLState},
    * {@link SQLException#getErrorCode} and {@link SQLException#getMessage}) of the linked list ({@link SQLException#getNextException}) of exceptions</li>
    * </ul>
    *
    * @param errorMessage  the target where append the exceptions details
    * @param exception     the SQL exception (or warning)
    * @param sqlStatements the SQL statements related to the {@code exception}
    * @return {@code errorMessage}
    */
   public static StringBuilder appendSQLExceptionDetails(StringBuilder errorMessage,
                                                         SQLException exception,
                                                         CharSequence sqlStatements) {
      errorMessage.append("\nSQL STATEMENTS: \n").append(sqlStatements);
      return appendSQLExceptionDetails(errorMessage, exception);
   }

   /**
    * Append to {@code errorMessage} a detailed description of the provided {@link SQLException}.<br>
    * The information appended are:
    * <ul>
    * <li>SQL EXCEPTIONS details ({@link SQLException#getSQLState},
    * {@link SQLException#getErrorCode} and {@link SQLException#getMessage}) of the linked list ({@link SQLException#getNextException}) of exceptions</li>
    * </ul>
    *
    * @param errorMessage the target where append the exceptions details
    * @param exception    the SQL exception (or warning)
    * @return {@code errorMessage}
    */
   public static StringBuilder appendSQLExceptionDetails(StringBuilder errorMessage, SQLException exception) {
      errorMessage.append("\nSQL EXCEPTIONS: ");
      SQLException nextEx = exception;
      int level = 0;
      do {
         errorMessage.append('\n');
         for (int i = 0; i < level; i++) {
            errorMessage.append(' ');
         }
         formatSqlException(errorMessage, nextEx);
         nextEx = nextEx.getNextException();
         level++;
      }
      while (nextEx != null);
      return errorMessage;
   }

   private static StringBuilder formatSqlException(StringBuilder errorMessage, SQLException exception) {
      final String sqlState = exception.getSQLState();
      final int errorCode = exception.getErrorCode();
      final String message = exception.getMessage();
      return errorMessage.append("SQLState: ").append(sqlState).append(" ErrorCode: ").append(errorCode).append(" Message: ").append(message);
   }
}
