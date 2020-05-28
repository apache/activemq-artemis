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

import org.apache.activemq.artemis.jdbc.store.logging.LoggingConnection;
import org.apache.activemq.artemis.journal.ActiveMQJournalLogger;
import org.jboss.logging.Logger;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

public class JDBCConnectionProvider {

   private static final Logger logger = Logger.getLogger(JDBCConnectionProvider.class);
   private DataSource dataSource;
   private Executor networkTimeoutExecutor;
   private int networkTimeoutMillis;

   public JDBCConnectionProvider(DataSource dataSource) {
      this.dataSource = dataSource;
      this.networkTimeoutExecutor = null;
      this.networkTimeoutMillis = -1;
      addDerbyShutdownHook();
   }

   public synchronized Connection getConnection() throws SQLException {
      Connection connection;
      try {
         connection = dataSource.getConnection();
         if (logger.isTraceEnabled() && !(connection instanceof LoggingConnection)) {
            connection = new LoggingConnection(connection, logger);
         }
      } catch (SQLException e) {
         logger.error(JDBCUtils.appendSQLExceptionDetails(new StringBuilder(), e));
         throw e;
      }

      if (this.networkTimeoutMillis >= 0 && this.networkTimeoutExecutor == null) {
         logger.warn("Unable to set a network timeout on the JDBC connection: networkTimeoutExecutor is null");
      }

      if (this.networkTimeoutMillis >= 0 && this.networkTimeoutExecutor != null) {
         try {
            connection.setNetworkTimeout(this.networkTimeoutExecutor, this.networkTimeoutMillis);
         } catch (SQLException e) {
            logger.warn(JDBCUtils.appendSQLExceptionDetails(new StringBuilder(), e));
            ActiveMQJournalLogger.LOGGER.warn("Unable to set a network timeout on the JDBC connection");
         } catch (Throwable throwable) {
            //it included SecurityExceptions and UnsupportedOperationException
            logger.warn("Unable to set a network timeout on the JDBC connection", throwable);
         }
      }
      return connection;
   }

   private static AtomicBoolean shutAdded = new AtomicBoolean(false);

   private static class ShutdownDerby extends Thread {
      @Override
      public void run() {
         try {
            DriverManager.getConnection("jdbc:derby:;shutdown=true");
         } catch (Exception e) { }
      }

   }

   public void addDerbyShutdownHook() {
      // Shutdown the derby if using the derby embedded driver.
      try (Connection connection = getConnection()) {
         if (connection.getMetaData().getDriverName().equals("org.apache.derby.jdbc.EmbeddedDriver")) {
            if (shutAdded.compareAndSet(false, true)) {
               Runtime.getRuntime().addShutdownHook(new ShutdownDerby());
            }
         }
      } catch (SQLException e) {
         logger.error(JDBCUtils.appendSQLExceptionDetails(new StringBuilder(), e));
      }
   }

   public void setNetworkTimeout(Executor executor, int milliseconds) {
      this.networkTimeoutExecutor = executor;
      this.networkTimeoutMillis = milliseconds;
   }
}
