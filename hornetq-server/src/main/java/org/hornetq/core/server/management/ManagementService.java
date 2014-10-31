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
package org.hornetq.core.server.management;

import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import javax.management.ObjectName;

import org.hornetq.api.core.BroadcastGroupConfiguration;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.management.ObjectNameBuilder;
import org.hornetq.core.config.BridgeConfiguration;
import org.hornetq.core.config.ClusterConnectionConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.DivertConfiguration;
import org.hornetq.core.management.impl.HornetQServerControlImpl;
import org.hornetq.core.messagecounter.MessageCounterManager;
import org.hornetq.core.paging.PagingManager;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.remoting.server.RemotingService;
import org.hornetq.core.security.Role;
import org.hornetq.core.server.Divert;
import org.hornetq.core.server.HornetQComponent;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.QueueFactory;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.cluster.Bridge;
import org.hornetq.core.server.cluster.BroadcastGroup;
import org.hornetq.core.server.cluster.ClusterConnection;
import org.hornetq.core.settings.HierarchicalRepository;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.core.transaction.ResourceManager;
import org.hornetq.spi.core.remoting.Acceptor;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 *
 */
public interface ManagementService extends NotificationService, HornetQComponent
{
   // Configuration

   MessageCounterManager getMessageCounterManager();

   SimpleString getManagementAddress();

   SimpleString getManagementNotificationAddress();

   ObjectNameBuilder getObjectNameBuilder();

   // Resource Registration

   void setStorageManager(StorageManager storageManager);

   HornetQServerControlImpl registerServer(final PostOffice postOffice,
                                           final StorageManager storageManager,
                                           final Configuration configuration,
                                           final HierarchicalRepository<AddressSettings> addressSettingsRepository,
                                           final HierarchicalRepository<Set<Role>> securityRepository,
                                           final ResourceManager resourceManager,
                                           final RemotingService remotingService,
                                           final HornetQServer messagingServer,
                                           final QueueFactory queueFactory,
                                           final ScheduledExecutorService scheduledThreadPool,
                                           final PagingManager pagingManager,
                                           final boolean backup) throws Exception;

   void unregisterServer() throws Exception;

   void registerInJMX(ObjectName objectName, Object managedResource) throws Exception;

   void unregisterFromJMX(final ObjectName objectName) throws Exception;

   void registerInRegistry(String resourceName, Object managedResource);

   void unregisterFromRegistry(final String resourceName);

   void registerAddress(SimpleString address) throws Exception;

   void unregisterAddress(SimpleString address) throws Exception;

   void registerQueue(Queue queue, SimpleString address, StorageManager storageManager) throws Exception;

   void unregisterQueue(SimpleString name, SimpleString address) throws Exception;

   void registerAcceptor(Acceptor acceptor, TransportConfiguration configuration) throws Exception;

   void unregisterAcceptors();

   void registerDivert(Divert divert, DivertConfiguration config) throws Exception;

   void unregisterDivert(SimpleString name) throws Exception;

   void registerBroadcastGroup(BroadcastGroup broadcastGroup, BroadcastGroupConfiguration configuration) throws Exception;

   void unregisterBroadcastGroup(String name) throws Exception;

 //  void registerDiscoveryGroup(DiscoveryGroup discoveryGroup, DiscoveryGroupConfiguration configuration) throws Exception;

   //void unregisterDiscoveryGroup(String name) throws Exception;

   void registerBridge(Bridge bridge, BridgeConfiguration configuration) throws Exception;

   void unregisterBridge(String name) throws Exception;

   void registerCluster(ClusterConnection cluster, ClusterConnectionConfiguration configuration) throws Exception;

   void unregisterCluster(String name) throws Exception;

   Object getResource(String resourceName);

   Object[] getResources(Class<?> resourceType);

   ServerMessage handleMessage(ServerMessage message) throws Exception;
}
