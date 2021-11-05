/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.broker.ft;

import java.net.URI;
import java.util.LinkedList;
import java.util.List;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.store.kahadb.FilteredKahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.MultiKahaDBPersistenceAdapter;

public class mKahaDbQueueMasterSlaveTest extends QueueMasterSlaveTestSupport {

   protected String PRIMARY_URL = "tcp://localhost:62001";
   protected String BACKUP_URL = "tcp://localhost:62002";

   @Override
   protected void createPrimary() throws Exception {
      primary = new BrokerService();
      primary.setBrokerName("primary");
      primary.addConnector(PRIMARY_URL);
      primary.setUseJmx(false);
      primary.setPersistent(true);
      primary.setDeleteAllMessagesOnStartup(true);

      MultiKahaDBPersistenceAdapter mKahaDB = new MultiKahaDBPersistenceAdapter();
      List<FilteredKahaDBPersistenceAdapter> adapters = new LinkedList<>();
      FilteredKahaDBPersistenceAdapter defaultEntry = new FilteredKahaDBPersistenceAdapter();
      defaultEntry.setPersistenceAdapter(new KahaDBPersistenceAdapter());
      defaultEntry.setPerDestination(true);
      adapters.add(defaultEntry);

      mKahaDB.setFilteredPersistenceAdapters(adapters);
      primary.setPersistenceAdapter(mKahaDB);

      primary.start();
   }

   @Override
   protected void createBackup() throws Exception {
      // use a separate thread as the backup will block waiting for
      // the exclusive db lock
      Thread t = new Thread() {
         @Override
         public void run() {
            try {
               BrokerService broker = new BrokerService();
               broker.setBrokerName("backup");
               TransportConnector connector = new TransportConnector();
               connector.setUri(new URI(BACKUP_URL));
               broker.addConnector(connector);
               // no need for broker.setMasterConnectorURI(masterConnectorURI)
               // as the db lock provides the backup/priamry initialisation
               broker.setUseJmx(false);
               broker.setPersistent(true);

               MultiKahaDBPersistenceAdapter mKahaDB = new MultiKahaDBPersistenceAdapter();
               List<FilteredKahaDBPersistenceAdapter> adapters = new LinkedList<>();
               FilteredKahaDBPersistenceAdapter defaultEntry = new FilteredKahaDBPersistenceAdapter();
               defaultEntry.setPersistenceAdapter(new KahaDBPersistenceAdapter());
               defaultEntry.setPerDestination(true);
               adapters.add(defaultEntry);

               mKahaDB.setFilteredPersistenceAdapters(adapters);
               broker.setPersistenceAdapter(mKahaDB);
               broker.start();
               backup.set(broker);
               backupStarted.countDown();
            } catch (IllegalStateException expectedOnShutdown) {
            } catch (Exception e) {
               fail("failed to start backup broker, reason:" + e);
            }
         }
      };
      t.start();
   }
}
