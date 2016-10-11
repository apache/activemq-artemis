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
package org.apache.activemq.artemis.osgi;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.activemq.artemis.core.config.storage.DatabaseStorageConfiguration;
import org.apache.activemq.artemis.jdbc.store.JDBCUtils;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.util.tracker.ServiceTrackerCustomizer;

public class DataSourceTracker implements ServiceTrackerCustomizer<DataSource, DataSource> {

   private static final Logger LOG = Logger.getLogger(ProtocolTracker.class.getName());
   private final String name;
   private final BundleContext context;
   private final DatabaseStorageConfiguration dsc;
   private final ServerTrackerCallBack callback;

   public DataSourceTracker(String name, BundleContext context, DatabaseStorageConfiguration dsc,
                            ServerTrackerCallBack callback) {
      this.name = name;
      this.context = context;
      this.dsc = dsc;
      this.callback = callback;
   }

   @Override
   public DataSource addingService(ServiceReference<DataSource> reference) {
      DataSource dataSource = context.getService(reference);
      dsc.setDataSource(dataSource);
      try (Connection conn = dataSource.getConnection()) {
         dsc.setSqlProvider(JDBCUtils.getSQLProviderFactory(conn.getMetaData().getURL()));
      } catch (SQLException ex) {
         LOG.log(Level.WARNING, "Error getting dataSource provider infos", ex);
      }
      callback.setDataSourceDependency(false);
      try {
         callback.start();
      } catch (Exception ex) {
         LOG.log(Level.WARNING, "Error starting broker " + name, ex);
      }
      return dataSource;
   }

   @Override
   public void modifiedService(ServiceReference<DataSource> reference, DataSource service) {
      // not supported
   }

   @Override
   public void removedService(ServiceReference<DataSource> reference, DataSource service) {
      callback.setDataSourceDependency(true);
      try {
         callback.stop();
      } catch (Exception ex) {
         LOG.log(Level.WARNING, "Error stopping broker " + name, ex);
      }
   }
}
