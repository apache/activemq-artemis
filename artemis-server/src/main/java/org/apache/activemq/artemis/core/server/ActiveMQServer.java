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
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.remoting.server.RemotingService;
import org.apache.activemq.artemis.core.replication.ReplicationManager;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.security.SecurityAuth;
import org.apache.activemq.artemis.core.security.SecurityStore;
import org.apache.activemq.artemis.core.server.cluster.ClusterManager;
import org.apache.activemq.artemis.core.server.cluster.ha.HAPolicy;
import org.apache.activemq.artemis.core.server.group.GroupingHandler;
import org.apache.activemq.artemis.core.server.impl.Activation;
import org.apache.activemq.artemis.core.server.impl.ConnectorsService;
import org.apache.activemq.artemis.core.server.management.ManagementService;
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

   Version getVersion();

   NodeManager getNodeManager();

   /**
    * Returns the resource to manage this ActiveMQ Artemis server.
    *
    * @throws IllegalStateException if the server is not properly started.
    */
   ActiveMQServerControlImpl getActiveMQServerControl();

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
                               boolean autoCreateQueues) throws Exception;

   SecurityStore getSecurityStore();

   void removeSession(String name) throws Exception;

   Set<ServerSession> getSessions();

   HierarchicalRepository<Set<Role>> getSecurityRepository();

   HierarchicalRepository<AddressSettings> getAddressSettingsRepository();

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
    * This is the queue creator responsible for automatic JMS Queue creations.
    *
    * @param queueCreator
    */
   void setJMSQueueCreator(QueueCreator queueCreator);

   /**
    * @see org.apache.activemq.artemis.core.server.ActiveMQServer#setJMSQueueCreator(QueueCreator)
    */
   QueueCreator getJMSQueueCreator();

   /**
    * This is the queue deleter responsible for automatic JMS Queue deletions.
    *
    * @param queueDeleter
    */
   void setJMSQueueDeleter(QueueDeleter queueDeleter);

   /**
    * @see org.apache.activemq.artemis.core.server.ActiveMQServer#setJMSQueueDeleter(QueueDeleter)
    */
   QueueDeleter getJMSQueueDeleter();

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
                          final SimpleString user,
                          boolean durable) throws Exception;

   Queue createQueue(SimpleString address,
                     SimpleString queueName,
                     SimpleString filter,
                     boolean durable,
                     boolean temporary) throws Exception;

   Queue createQueue(SimpleString address,
                     SimpleString queueName,
                     SimpleString filter,
                     SimpleString user,
                     boolean durable,
                     boolean temporary) throws Exception;

   Queue createQueue(SimpleString address,
                     SimpleString queueName,
                     SimpleString filter,
                     SimpleString user,
                     boolean durable,
                     boolean temporary,
                     boolean autoCreated) throws Exception;

   Queue deployQueue(SimpleString address,
                     SimpleString queueName,
                     SimpleString filterString,
                     boolean durable,
                     boolean temporary) throws Exception;

   Queue deployQueue(SimpleString address,
                     SimpleString queueName,
                     SimpleString filterString,
                     boolean durable,
                     boolean temporary,
                     boolean autoCreated) throws Exception;

   Queue locateQueue(SimpleString queueName);

   BindingQueryResult bindingQuery(SimpleString address) throws Exception;

   QueueQueryResult queueQuery(SimpleString name) throws Exception;

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

   void setMBeanServer(MBeanServer mBeanServer);
}
