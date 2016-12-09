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
package org.apache.activemq.artemis.core.server;

import javax.management.MBeanServer;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.BridgeConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.DivertConfiguration;
import org.apache.activemq.artemis.core.management.impl.ActiveMQServerControlImpl;
import org.apache.activemq.artemis.core.paging.PagingManager;
import org.apache.activemq.artemis.core.persistence.OperationContext;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.remoting.server.RemotingService;
import org.apache.activemq.artemis.core.replication.ReplicationEndpoint;
import org.apache.activemq.artemis.core.replication.ReplicationManager;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.security.SecurityAuth;
import org.apache.activemq.artemis.core.security.SecurityStore;
import org.apache.activemq.artemis.core.server.cluster.ClusterManager;
import org.apache.activemq.artemis.core.server.cluster.ha.HAPolicy;
import org.apache.activemq.artemis.core.server.group.GroupingHandler;
import org.apache.activemq.artemis.core.server.impl.Activation;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.server.impl.ConnectorsService;
import org.apache.activemq.artemis.core.server.management.ManagementService;
import org.apache.activemq.artemis.core.server.reload.ReloadManager;
import org.apache.activemq.artemis.core.settings.HierarchicalRepository;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.core.transaction.ResourceManager;
import org.apache.activemq.artemis.core.version.Version;
import org.apache.activemq.artemis.spi.core.protocol.ProtocolManagerFactory;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.protocol.SessionCallback;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager;
import org.apache.activemq.artemis.utils.ExecutorFactory;

/**
 * This interface defines the internal interface of the ActiveMQ Artemis Server exposed to other components
 * of the server.
 * <p>
 * This is not part of our public API.
 */
public interface ActiveMQServer extends ActiveMQComponent {

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

   ServiceRegistry getServiceRegistry();

   RemotingService getRemotingService();

   StorageManager getStorageManager();

   PagingManager getPagingManager();

   ManagementService getManagementService();

   ActiveMQSecurityManager getSecurityManager();

   NetworkHealthCheck getNetworkHealthCheck();

   Version getVersion();

   NodeManager getNodeManager();

   /**
    * @return
    */
   ReplicationEndpoint getReplicationEndpoint();

   /**
    * it will release hold a lock for the activation.
    */
   void unlockActivation();

   /**
    * it will hold a lock for the activation. This will prevent the activation from happening.
    */
   void lockActivation();

   /**
    * Returns the resource to manage this ActiveMQ Artemis server.
    *
    * @throws IllegalStateException if the server is not properly started.
    */
   ActiveMQServerControlImpl getActiveMQServerControl();

   void destroyQueue(SimpleString queueName,
                     SecurityAuth session,
                     boolean checkConsumerCount,
                     boolean removeConsumers,
                     boolean autoDeleteAddress) throws Exception;

   void registerActivateCallback(ActivateCallback callback);

   void unregisterActivateCallback(ActivateCallback callback);

   /**
    * Register a listener to detect problems during activation
    *
    * @param listener @see org.apache.activemq.artemis.core.server.ActivationFailureListener
    */
   void registerActivationFailureListener(ActivationFailureListener listener);

   /**
    * Remove a previously registered failure listener
    *
    * @param listener
    */
   void unregisterActivationFailureListener(ActivationFailureListener listener);

   /**
    * Alert activation failure listeners of a failure.
    *
    * @param e the exception that caused the activation failure
    */
   void callActivationFailureListeners(Exception e);

   /**
    * @param callback {@link org.apache.activemq.artemis.core.server.PostQueueCreationCallback}
    */
   void registerPostQueueCreationCallback(PostQueueCreationCallback callback);

   /**
    * @param callback {@link org.apache.activemq.artemis.core.server.PostQueueCreationCallback}
    */
   void unregisterPostQueueCreationCallback(PostQueueCreationCallback callback);

   /**
    * @param queueName
    */
   void callPostQueueCreationCallbacks(SimpleString queueName) throws Exception;

   /**
    * @param callback {@link org.apache.activemq.artemis.core.server.PostQueueDeletionCallback}
    */
   void registerPostQueueDeletionCallback(PostQueueDeletionCallback callback);

   /**
    * @param callback {@link org.apache.activemq.artemis.core.server.PostQueueDeletionCallback}
    */
   void unregisterPostQueueDeletionCallback(PostQueueDeletionCallback callback);

   /**
    * @param queueName
    */
   void callPostQueueDeletionCallbacks(SimpleString address, SimpleString queueName) throws Exception;

   void checkQueueCreationLimit(String username) throws Exception;

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
                               boolean autoCreateQueues,
                               OperationContext context) throws Exception;

   SecurityStore getSecurityStore();

   void removeSession(String name) throws Exception;

   Set<ServerSession> getSessions();

   HierarchicalRepository<Set<Role>> getSecurityRepository();

   HierarchicalRepository<AddressSettings> getAddressSettingsRepository();

   OperationContext newOperationContext();

   int getConnectionCount();

   long getTotalConnectionCount();

   long getTotalMessageCount();

   long getTotalMessagesAdded();

   long getTotalMessagesAcknowledged();

   long getTotalConsumerCount();

   PostOffice getPostOffice();

   QueueFactory getQueueFactory();

   ResourceManager getResourceManager();

   List<ServerSession> getSessions(String connectionID);

   /**
    * @return a session containing the meta-key and meata-value
    */
   ServerSession lookupSession(String metakey, String metavalue);

   ClusterManager getClusterManager();

   SimpleString getNodeID();

   boolean isActive();

   String getUptime();

   long getUptimeMillis();

   /**
    * Returns whether the initial replication synchronization process with the backup server is complete; applicable for
    * either the live or backup server.
    */
   boolean isReplicaSync();

   /**
    * Wait for server initialization.
    *
    * @param timeout
    * @param unit
    * @return {@code true} if the server was already initialized or if it was initialized within the
    * timeout period, {@code false} otherwise.
    * @throws InterruptedException
    * @see java.util.concurrent.CountDownLatch#await(long, java.util.concurrent.TimeUnit)
    */
   boolean waitForActivation(long timeout, TimeUnit unit) throws InterruptedException;

   Queue createQueue(SimpleString address, RoutingType routingType, SimpleString queueName, SimpleString filter,
                     SimpleString user,
                     boolean durable,
                     boolean temporary,
                     boolean autoCreated,
                     Integer maxConsumers,
                     Boolean deleteOnNoConsumers,
                     boolean autoCreateAddress) throws Exception;

   /**
    * Creates a transient queue. A queue that will exist as long as there are consumers.
    * The queue will be deleted as soon as all the consumers are removed.
    * <p>
    * Notice: the queue won't be deleted until the first consumer arrives.
    *
    * @param address
    * @param name
    * @param filterString
    * @param durable
    * @throws org.apache.activemq.artemis.api.core.ActiveMQInvalidTransientQueueUseException if the shared queue already exists with a different {@code address} or {@code filterString}
    * @throws NullPointerException                                                           if {@code address} is {@code null}
    */
   void createSharedQueue(final SimpleString address, final RoutingType routingType, final SimpleString name, final SimpleString filterString,
                          final SimpleString user,
                          boolean durable) throws Exception;

   Queue createQueue(SimpleString address, RoutingType routingType, SimpleString queueName, SimpleString filter,
                     boolean durable,
                     boolean temporary) throws Exception;

   @Deprecated
   Queue createQueue(SimpleString address, SimpleString queueName, SimpleString filter, boolean durable, boolean temporary) throws Exception;

   Queue createQueue(SimpleString address, RoutingType routingType, SimpleString queueName, SimpleString filterString,
                     boolean durable,
                     boolean temporary,
                     int maxConsumers,
                     boolean deleteOnNoConsumers,
                     boolean autoCreateAddress) throws Exception;

   Queue createQueue(SimpleString address, RoutingType routingType, SimpleString queueName, SimpleString filter,
                     SimpleString user,
                     boolean durable,
                     boolean temporary) throws Exception;

   Queue createQueue(SimpleString address, RoutingType routingType, SimpleString queueName, SimpleString filter,
                     SimpleString user,
                     boolean durable,
                     boolean temporary,
                     int maxConsumers,
                     boolean deleteOnNoConsumers,
                     boolean autoCreateAddress) throws Exception;

   Queue createQueue(SimpleString address, RoutingType routingType, SimpleString queueName, SimpleString filter,
                     SimpleString user,
                     boolean durable,
                     boolean temporary,
                     boolean autoCreated) throws Exception;

   @Deprecated
   Queue deployQueue(String address, String queue, String filter, boolean durable, boolean temporary) throws Exception;

   @Deprecated
   Queue deployQueue(SimpleString address, SimpleString queue, SimpleString filter, boolean durable, boolean temporary) throws Exception;

   Queue deployQueue(SimpleString address, RoutingType routingType, SimpleString resourceName, SimpleString filterString,
                     boolean durable,
                     boolean temporary) throws Exception;

   Queue deployQueue(SimpleString address, RoutingType routingType, SimpleString queueName, SimpleString filterString,
                     boolean durable,
                     boolean temporary,
                     boolean autoCreated) throws Exception;

   Queue locateQueue(SimpleString queueName);

   BindingQueryResult bindingQuery(SimpleString address) throws Exception;

   QueueQueryResult queueQuery(SimpleString name) throws Exception;

   AddressQueryResult addressQuery(SimpleString name) throws Exception;

   Queue deployQueue(SimpleString address, RoutingType routingType, SimpleString queueName, SimpleString filterString,
                     boolean durable,
                     boolean temporary,
                     boolean autoCreated,
                     int maxConsumers,
                     boolean deleteOnNoConsumers,
                     boolean autoCreateAddress) throws Exception;

   void destroyQueue(SimpleString queueName) throws Exception;

   void destroyQueue(SimpleString queueName, SecurityAuth session) throws Exception;

   void destroyQueue(SimpleString queueName, SecurityAuth session, boolean checkConsumerCount) throws Exception;

   void destroyQueue(SimpleString queueName,
                     SecurityAuth session,
                     boolean checkConsumerCount,
                     boolean removeConsumers) throws Exception;

   String destroyConnectionWithSessionMetadata(String metaKey, String metaValue) throws Exception;

   ScheduledExecutorService getScheduledPool();

   ExecutorFactory getExecutorFactory();

   ExecutorFactory getIOExecutorFactory();

   void setGroupingHandler(GroupingHandler groupingHandler);

   GroupingHandler getGroupingHandler();

   ReplicationManager getReplicationManager();

   void deployDivert(DivertConfiguration config) throws Exception;

   void destroyDivert(SimpleString name) throws Exception;

   ConnectorsService getConnectorsService();

   void deployBridge(BridgeConfiguration config) throws Exception;

   void destroyBridge(String name) throws Exception;

   ServerSession getSessionByID(String sessionID);

   void threadDump();

   /**
    * return true if there is a binding for this address (i.e. if there is a created queue)
    *
    * @param address
    * @return
    */
   boolean isAddressBound(String address) throws Exception;

   void stop(boolean failoverOnServerShutdown) throws Exception;

   AddressInfo getAddressInfo(SimpleString address);

   Queue createQueue(SimpleString addressName,
                     SimpleString queueName,
                     RoutingType routingType,
                     SimpleString filterString,
                     SimpleString user,
                     boolean durable,
                     boolean temporary,
                     boolean ignoreIfExists,
                     boolean transientQueue,
                     boolean autoCreated,
                     int maxConsumers,
                     boolean deleteOnNoConsumers,
                     boolean autoCreateAddress) throws Exception;

   /*
            * add a ProtocolManagerFactory to be used. Note if @see Configuration#isResolveProtocols is tur then this factory will
            * replace any factories with the same protocol
            * */
   void addProtocolManagerFactory(ProtocolManagerFactory factory);

   /*
   * add a ProtocolManagerFactory to be used.
   * */
   void removeProtocolManagerFactory(ProtocolManagerFactory factory);

   ReloadManager getReloadManager();

   ActiveMQServer createBackupServer(Configuration configuration);

   void addScaledDownNode(SimpleString scaledDownNodeId);

   boolean hasScaledDown(SimpleString scaledDownNodeId);

   Activation getActivation();

   HAPolicy getHAPolicy();

   void setHAPolicy(HAPolicy haPolicy);

   void setMBeanServer(MBeanServer mBeanServer);

   void addExternalComponent(ActiveMQComponent externalComponent);

   boolean addClientConnection(String clientId, boolean unique);

   void removeClientConnection(String clientId);

   AddressInfo putAddressInfoIfAbsent(AddressInfo addressInfo) throws Exception;

   void createAddressInfo(AddressInfo addressInfo) throws Exception;

   AddressInfo createOrUpdateAddressInfo(AddressInfo addressInfo) throws Exception;

   void removeAddressInfo(SimpleString address, SecurityAuth session) throws Exception;

   String getInternalNamingPrefix();
}
