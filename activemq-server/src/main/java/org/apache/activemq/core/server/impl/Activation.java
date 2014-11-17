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

import org.apache.activemq6.api.core.HornetQException;
import org.apache.activemq6.core.config.Configuration;
import org.apache.activemq6.core.paging.PagingManager;
import org.apache.activemq6.core.persistence.StorageManager;
import org.apache.activemq6.core.postoffice.PostOffice;
import org.apache.activemq6.core.protocol.core.Channel;
import org.apache.activemq6.core.protocol.core.ChannelHandler;
import org.apache.activemq6.core.remoting.server.RemotingService;
import org.apache.activemq6.core.replication.ReplicationManager;
import org.apache.activemq6.core.server.HornetQServer;
import org.apache.activemq6.core.server.NodeManager;
import org.apache.activemq6.core.server.QueueFactory;
import org.apache.activemq6.core.server.cluster.ha.HAManager;
import org.apache.activemq6.core.server.cluster.ha.StandaloneHAManager;
import org.apache.activemq6.core.server.group.GroupingHandler;
import org.apache.activemq6.core.server.management.ManagementService;
import org.apache.activemq6.spi.core.remoting.Acceptor;

/**
* An activation controls the lifecycle of the server and any components specific to the Activation itself.
*/
public abstract class Activation implements Runnable
{
   public abstract void close(boolean permanently, boolean restarting) throws Exception;

   /*
   * freeze the connection but allow the Activation to over ride this and decide if any connections should be left open.
   * */
   public void freezeConnections(RemotingService remotingService)
   {
      if (remotingService != null)
      {
         remotingService.freeze(null, null);
      }
   }

   /*
   * allow the activation t ooverride this if it needs to tidy up after freezing the connection. its a different method as
   * its called outside of the lock that the previous method is.
   * */
   public void postConnectionFreeze()
   {
   }

   /*
   * called before the server is closing the journals so the activation can tidy up stuff
   * */
   public void preStorageClose() throws Exception
   {
   }

   /*
   * called by the server to notify the Activation that the server is stopping
   * */
   public void sendLiveIsStopping()
   {
   }

   /*
   * called by the ha manager to notify the Activation that HA is now active
   * */
   public void haStarted()
   {
   }

   /*
   * allows the Activation to register a channel handler so it can handle any packets that are unique to the Activation
   * */
   public ChannelHandler getActivationChannelHandler(Channel channel, Acceptor acceptorUsed)
   {
      return null;
   }

   /*
   * returns the HA manager used for this Activation
   * */
   public HAManager getHAManager()
   {
      return new StandaloneHAManager();
   }

   /*
   * create the Journal loader needed for this Activation.
   * */
   public JournalLoader createJournalLoader(PostOffice postOffice, PagingManager pagingManager, StorageManager storageManager, QueueFactory queueFactory, NodeManager nodeManager, ManagementService managementService, GroupingHandler groupingHandler, Configuration configuration, HornetQServer parentServer) throws HornetQException
   {
      return new PostOfficeJournalLoader(postOffice,
            pagingManager,
            storageManager,
            queueFactory,
            nodeManager,
            managementService,
            groupingHandler,
            configuration);
   }

   /*
   * todo, remove this, its only needed for JMSServerManagerImpl, it should be sought elsewhere
   * */
   public ReplicationManager getReplicationManager()
   {
      return null;
   }
}
