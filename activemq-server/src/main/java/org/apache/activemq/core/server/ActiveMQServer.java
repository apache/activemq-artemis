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
package org.apache.activemq.core.server;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.core.config.BridgeConfiguration;
import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.core.config.DivertConfiguration;
import org.apache.activemq.core.management.impl.ActiveMQServerControlImpl;
import org.apache.activemq.core.paging.PagingManager;
import org.apache.activemq.core.persistence.StorageManager;
import org.apache.activemq.core.postoffice.PostOffice;
import org.apache.activemq.core.remoting.server.RemotingService;
import org.apache.activemq.core.replication.ReplicationManager;
import org.apache.activemq.core.security.Role;
import org.apache.activemq.core.security.SecurityStore;
import org.apache.activemq.core.server.cluster.ClusterManager;
import org.apache.activemq.core.server.cluster.ha.HAPolicy;
import org.apache.activemq.core.server.group.GroupingHandler;
import org.apache.activemq.core.server.impl.Activation;
import org.apache.activemq.core.server.impl.ConnectorsService;
import org.apache.activemq.core.server.management.ManagementService;
import org.apache.activemq.core.settings.HierarchicalRepository;
import org.apache.activemq.core.settings.impl.AddressSettings;
import org.apache.activemq.core.transaction.ResourceManager;
import org.apache.activemq.core.version.Version;
import org.apache.activemq.spi.core.protocol.ProtocolManagerFactory;
import org.apache.activemq.spi.core.protocol.RemotingConnection;
import org.apache.activemq.spi.core.protocol.SessionCallback;
import org.apache.activemq.spi.core.security.ActiveMQSecurityManager;
import org.apache.activemq.utils.ExecutorFactory;

/**
 * This interface defines the internal interface of the ActiveMQ Server exposed to other components
 * of the server.
 * <p>
 * This is not part of our public API.
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public interface ActiveMQServer extends ActiveMQComponent
{

   /**
    * Sets the server identity.
    * <p>
    * The identity will be exposed on logs. It may help to debug issues on the log traces and
    * debugs.
    * <p>
    * This method was created mainly for testing but it may be used in scenarios where you need to
    * have more than one Server inside the same VM.
    */
   void setIdentity(String identity);

   String getIdentity();

   String describe();

   void addActivationParam(String key, Object val);

   Configuration getConfiguration();

   RemotingService getRemotingService();

   StorageManager getStorageManager();

   PagingManager getPagingManager();

   ManagementService getManagementService();

   ActiveMQSecurityManager getSecurityManager();

   Version getVersion();

   NodeManager getNodeManager();

   /**
    * Returns the resource to manage this ActiveMQ server.
    * @throws IllegalStateException if the server is not properly started.
    */
   ActiveMQServerControlImpl getActiveMQServerControl();

   void registerActivateCallback(ActivateCallback callback);

   void unregisterActivateCallback(ActivateCallback callback);

   ServerSession createSession(String name,
                               String username,
                               String password,
                               int minLargeMessageSize,
                               RemotingConnection remotingConnection,
                               boolean autoCommitSends,
                               boolean autoCommitAcks,
                               boolean preAcknowledge,
                               boolean xa,
                               String defaultAddress,
                               SessionCallback callback,
                               ServerSessionFactory sessionFactory) throws Exception;

   SecurityStore getSecurityStore();

   void removeSession(String name) throws Exception;

   Set<ServerSession> getSessions();

   HierarchicalRepository<Set<Role>> getSecurityRepository();

   HierarchicalRepository<AddressSettings> getAddressSettingsRepository();

   int getConnectionCount();

   PostOffice getPostOffice();

   QueueFactory getQueueFactory();

   ResourceManager getResourceManager();

   List<ServerSession> getSessions(String connectionID);

   /** @return a session containing the meta-key and meata-value */
   ServerSession lookupSession(String metakey, String metavalue);

   ClusterManager getClusterManager();

   SimpleString getNodeID();

   boolean isActive();

   /**
    * Wait for server initialization.
    * @param timeout
    * @param unit
    * @see CountDownLatch#await(long, TimeUnit)
    * @return {@code true} if the server was already initialized or if it was initialized within the
    *         timeout period, {@code false} otherwise.
    * @throws InterruptedException
    */
   boolean waitForActivation(long timeout, TimeUnit unit) throws InterruptedException;

   /**
    * Creates a shared queue. if non durable it will exist as long as there are consumers.
    *
    * Notice: the queue won't be deleted until the first consumer arrives.
    *
    * @param address
    * @param name
    * @param filterString
    * @param durable
    * @throws Exception
    */
   void createSharedQueue(final SimpleString address,
                           final SimpleString name,
                           final SimpleString filterString,
                           boolean durable) throws Exception;

   Queue createQueue(SimpleString address,
                     SimpleString queueName,
                     SimpleString filter,
                     boolean durable,
                     boolean temporary) throws Exception;

   Queue deployQueue(SimpleString address,
                     SimpleString queueName,
                     SimpleString filterString,
                     boolean durable,
                     boolean temporary) throws Exception;

   Queue locateQueue(SimpleString queueName) throws Exception;

   void destroyQueue(SimpleString queueName) throws Exception;

   void destroyQueue(SimpleString queueName, ServerSession session) throws Exception;

   void destroyQueue(SimpleString queueName, ServerSession session, boolean checkConsumerCount) throws Exception;

   void destroyQueue(SimpleString queueName, ServerSession session, boolean checkConsumerCount, boolean removeConsumers) throws Exception;

   String destroyConnectionWithSessionMetadata(String metaKey, String metaValue) throws Exception;

   ScheduledExecutorService getScheduledPool();

   ExecutorFactory getExecutorFactory();

   void setGroupingHandler(GroupingHandler groupingHandler);

   GroupingHandler getGroupingHandler();

   ReplicationManager getReplicationManager();

   void deployDivert(DivertConfiguration config) throws Exception;

   void destroyDivert(SimpleString name) throws Exception;

   ConnectorsService getConnectorsService();

   void deployBridge(BridgeConfiguration config) throws Exception;

   void destroyBridge(String name) throws Exception;

   ServerSession getSessionByID(String sessionID);

   void threadDump(String reason);

   /**
    * return true if there is a binding for this address (i.e. if there is a created queue)
    * @param address
    * @return
    */
   boolean isAddressBound(String address) throws Exception;

   void stop(boolean failoverOnServerShutdown) throws Exception;

   /*
   * add a ProtocolManagerFactory to be used. Note if @see Configuration#isResolveProtocols is tur then this factory will
   * replace any factories with the same protocol
   * */
   void addProtocolManagerFactory(ProtocolManagerFactory factory);

   /*
   * add a ProtocolManagerFactory to be used.
   * */
   void removeProtocolManagerFactory(ProtocolManagerFactory factory);

   ActiveMQServer createBackupServer(Configuration configuration);

   void addScaledDownNode(SimpleString scaledDownNodeId);

   boolean hasScaledDown(SimpleString scaledDownNodeId);

   Activation getActivation();

   HAPolicy getHAPolicy();

   void setHAPolicy(HAPolicy haPolicy);
}
