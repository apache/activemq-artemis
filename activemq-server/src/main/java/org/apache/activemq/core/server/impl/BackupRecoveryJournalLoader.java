/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.activemq6.core.server.impl;

import org.apache.activemq6.api.core.Pair;
import org.apache.activemq6.api.core.SimpleString;
import org.apache.activemq6.api.core.client.ClientSessionFactory;
import org.apache.activemq6.api.core.client.ServerLocator;
import org.apache.activemq6.core.client.impl.ServerLocatorInternal;
import org.apache.activemq6.core.config.Configuration;
import org.apache.activemq6.core.journal.Journal;
import org.apache.activemq6.core.paging.PagingManager;
import org.apache.activemq6.core.persistence.GroupingInfo;
import org.apache.activemq6.core.persistence.StorageManager;
import org.apache.activemq6.core.postoffice.PostOffice;
import org.apache.activemq6.core.server.HornetQServer;
import org.apache.activemq6.core.server.HornetQServerLogger;
import org.apache.activemq6.core.server.NodeManager;
import org.apache.activemq6.core.server.QueueFactory;
import org.apache.activemq6.core.server.cluster.ClusterController;
import org.apache.activemq6.core.server.cluster.HornetQServerSideProtocolManagerFactory;
import org.apache.activemq6.core.server.group.GroupingHandler;
import org.apache.activemq6.core.server.management.ManagementService;
import org.apache.activemq6.core.transaction.ResourceManager;

import java.util.List;
import java.util.Map;

/*
* Instead of loading into its own post office this will use its parent server (the actual live server) and load into that.
* Since the server is already running we have to make sure we don't route any message that may subsequently get deleted or acked.
* */
public class BackupRecoveryJournalLoader extends PostOfficeJournalLoader
{
   private HornetQServer parentServer;
   private ServerLocator locator;
   private final ClusterController clusterController;

   public BackupRecoveryJournalLoader(PostOffice postOffice,
                                      PagingManager pagingManager,
                                      StorageManager storageManager,
                                      QueueFactory queueFactory,
                                      NodeManager nodeManager,
                                      ManagementService managementService,
                                      GroupingHandler groupingHandler,
                                      Configuration configuration,
                                      HornetQServer parentServer,
                                      ServerLocatorInternal locator,
                                      ClusterController clusterController)
   {

      super(postOffice, pagingManager, storageManager, queueFactory, nodeManager, managementService, groupingHandler, configuration);
      this.parentServer = parentServer;
      this.locator = locator;
      this.clusterController = clusterController;
   }

   @Override
   public void handleGroupingBindings(List< GroupingInfo > groupingInfos)
   {
      //currently only the node that is configured with the local group handler can recover these as all other nodes are
      //remote handlers, this means that you can only use FULL backup server when using group handlers.
      //todo maybe in the future we can restart the handler on the live server as a local handler and redistribute the state
      if (groupingInfos != null && groupingInfos.size() > 0)
      {
         HornetQServerLogger.LOGGER.groupBindingsOnRecovery();
      }
   }

   @Override
   public void handleDuplicateIds(Map<SimpleString, List<Pair<byte[], Long>>> duplicateIDMap) throws Exception
   {
      //nothing to do here so override so we dont bother creating the caches
   }

   @Override
   public void postLoad(Journal messageJournal, ResourceManager resourceManager, Map<SimpleString, List<Pair<byte[], Long>>> duplicateIDMap) throws Exception
   {
      ScaleDownHandler scaleDownHandler = new ScaleDownHandler(pagingManager, postOffice, nodeManager, clusterController);
      locator.setProtocolManagerFactory(HornetQServerSideProtocolManagerFactory.getInstance());

      try (ClientSessionFactory sessionFactory = locator.createSessionFactory())
      {
         scaleDownHandler.scaleDown(sessionFactory, resourceManager, duplicateIDMap, parentServer.getConfiguration().getManagementAddress(), parentServer.getNodeID());
      }
   }

   @Override
   public void cleanUp()
   {
      super.cleanUp();
      locator.close();
   }
}
