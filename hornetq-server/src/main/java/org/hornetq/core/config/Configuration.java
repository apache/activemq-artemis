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
package org.hornetq.core.config;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hornetq.api.core.BroadcastGroupConfiguration;
import org.hornetq.api.core.DiscoveryGroupConfiguration;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.security.Role;
import org.hornetq.core.server.JournalType;
import org.hornetq.core.server.cluster.ha.HAPolicy;
import org.hornetq.core.server.group.impl.GroupingHandlerConfiguration;
import org.hornetq.core.settings.impl.AddressSettings;

/**
 * A Configuration is used to configure HornetQ servers.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public interface Configuration extends Serializable
{
   /**
    * To be used on dependency management on the application server
    */
   String getName();

   /**
    * To be used on dependency management on the application server
    */
   void setName(String name);

   /**
    * returns the name used to group of live/backup servers
    *
    * @return the name of the group
    *
    * @deprecated replaced by {@link org.hornetq.core.server.cluster.ha.HAPolicy#getBackupGroupName()}
    */
   @Deprecated
   String getBackupGroupName();

   /**
    * Used to configure groups of live/backup servers.
    *
    * @param nodeGroupName the node group name
    *
    * @deprecated replaced by {@link org.hornetq.core.server.cluster.ha.HAPolicy#setBackupGroupName(String)}
    */
   @Deprecated
   void setBackupGroupName(String nodeGroupName);

   /**
    * Returns whether this server is clustered. <br>
    * {@code true} if {@link #getClusterConfigurations()} is not empty.
    */
   boolean isClustered();

   /**
    * Returns whether a backup will automatically stop when a live server is restarting (i.e.
    * failing back).
    *
    * @return {@code true} if the backup will stop when the live server restarts
    *
    * @deprecated replaced by {@link org.hornetq.core.server.cluster.ha.HAPolicy#isAllowAutoFailBack()}
    */
   @Deprecated
   boolean isAllowAutoFailBack();

   /**
    * whether a backup will automatically stop when a live server is restarting (i.e. failing back).
    *
    * @param allowAutoFailBack true if allowed
    *
    * @deprecated replaced by {@link org.hornetq.core.server.cluster.ha.HAPolicy#setAllowAutoFailBack(boolean)}
    */
   @Deprecated
   void setAllowAutoFailBack(boolean allowAutoFailBack);

   /**
    * Returns whether delivery count is persisted before messages are delivered to the consumers. <br>
    * Default value is
    * {@value org.hornetq.api.config.HornetQDefaultConfiguration#DEFAULT_PERSIST_DELIVERY_COUNT_BEFORE_DELIVERY}.
    */
   boolean isPersistDeliveryCountBeforeDelivery();

   /**
    * Sets whether delivery count is persisted before messages are delivered to consumers.
    */
   void setPersistDeliveryCountBeforeDelivery(boolean persistDeliveryCountBeforeDelivery);

   /**
    * Returns {@code true} if this server is a backup, {@code false} if it is a live server. <br>
    * If a backup server has been activated, returns {@code false}. <br>
    * Default value is {@value org.hornetq.api.config.HornetQDefaultConfiguration#DEFAULT_BACKUP}.
    *
    * @deprecated replaced by {@link org.hornetq.core.server.cluster.ha.HAPolicy#isBackup()}
    */
   @Deprecated
   boolean isBackup();

   /**
    * Formerly set whether this server is a backup or not.
    *
    * @deprecated replaced by {@link org.hornetq.core.server.cluster.ha.HAPolicy#setPolicyType(org.hornetq.core.server.cluster.ha.HAPolicy.POLICY_TYPE)}
    */
   @Deprecated
   void setBackup(boolean backup);

   /**
    * Returns whether this server shares its data store with a corresponding live or backup server. <br>
    * Default value is {@value org.hornetq.api.config.HornetQDefaultConfiguration#DEFAULT_SHARED_STORE}.
    *
    * @deprecated replaced by {@link org.hornetq.core.server.cluster.ha.HAPolicy#isSharedStore()}
    */
   @Deprecated
   boolean isSharedStore();

   /**
    * Formerly set whether this server shares its data store with a backup or live server.
    *
    * @deprecated replaced by {@link org.hornetq.core.server.cluster.ha.HAPolicy#setPolicyType(org.hornetq.core.server.cluster.ha.HAPolicy.POLICY_TYPE)}
    */
   @Deprecated
   void setSharedStore(boolean sharedStore);

   /**
    * Returns whether this server will use files to configure and deploy its resources. <br>
    * Default value is {@value org.hornetq.api.config.HornetQDefaultConfiguration#DEFAULT_FILE_DEPLOYMENT_ENABLED}.
    */
   boolean isFileDeploymentEnabled();

   /**
    * Sets whether this server will use files to configure and deploy its resources.
    */
   void setFileDeploymentEnabled(boolean enable);

   /**
    * Returns whether this server is using persistence and store data. <br>
    * Default value is {@value org.hornetq.api.config.HornetQDefaultConfiguration#DEFAULT_PERSISTENCE_ENABLED}.
    */
   boolean isPersistenceEnabled();

   /**
    * Sets whether this server is using persistence and store data.
    */
   void setPersistenceEnabled(boolean enable);

   /**
    * Returns the period (in milliseconds) to scan configuration files used by deployment. <br>
    * Default value is {@value org.hornetq.api.config.HornetQDefaultConfiguration#DEFAULT_FILE_DEPLOYER_SCAN_PERIOD}.
    */
   long getFileDeployerScanPeriod();

   /**
    * Sets the period  to scan configuration files used by deployment.
    */
   void setFileDeployerScanPeriod(long period);

   /**
    * Returns the maximum number of threads in the thread pool of this server. <br>
    * Default value is {@value org.hornetq.api.config.HornetQDefaultConfiguration#DEFAULT_THREAD_POOL_MAX_SIZE}.
    */
   int getThreadPoolMaxSize();

   /**
    * Sets the maximum number of threads in the thread pool of this server.
    */
   void setThreadPoolMaxSize(int maxSize);

   /**
    * Returns the maximum number of threads in the <em>scheduled</em> thread pool of this server. <br>
    * Default value is {@value org.hornetq.api.config.HornetQDefaultConfiguration#DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE}.
    */
   int getScheduledThreadPoolMaxSize();

   /**
    * Sets the maximum number of threads in the <em>scheduled</em> thread pool of this server.
    */
   void setScheduledThreadPoolMaxSize(int maxSize);

   /**
    * Returns the interval time (in milliseconds) to invalidate security credentials. <br>
    * Default value is {@value org.hornetq.api.config.HornetQDefaultConfiguration#DEFAULT_SECURITY_INVALIDATION_INTERVAL}.
    */
   long getSecurityInvalidationInterval();

   /**
    * Sets the interval time (in milliseconds) to invalidate security credentials.
    */
   void setSecurityInvalidationInterval(long interval);

   /**
    * Returns whether security is enabled for this server. <br>
    * Default value is {@value org.hornetq.api.config.HornetQDefaultConfiguration#DEFAULT_SECURITY_ENABLED}.
    */
   boolean isSecurityEnabled();

   /**
    * Sets whether security is enabled for this server.
    */
   void setSecurityEnabled(boolean enabled);

   /**
    * Returns whether this server is manageable using JMX or not. <br>
    * Default value is {@value org.hornetq.api.config.HornetQDefaultConfiguration#DEFAULT_JMX_MANAGEMENT_ENABLED}.
    */
   boolean isJMXManagementEnabled();

   /**
    * Sets whether this server is manageable using JMX or not.
    */
   void setJMXManagementEnabled(boolean enabled);

   /**
    * Returns the domain used by JMX MBeans (provided JMX management is enabled). <br>
    * Default value is {@value org.hornetq.api.config.HornetQDefaultConfiguration#DEFAULT_JMX_DOMAIN}.
    */
   String getJMXDomain();

   /**
    * Sets the domain used by JMX MBeans (provided JMX management is enabled).
    * <p/>
    * Changing this JMX domain is required if multiple HornetQ servers are run inside
    * the same JVM and all servers are using the same MBeanServer.
    */
   void setJMXDomain(String domain);

   /**
    * Returns the list of interceptors classes used by this server for incoming messages (i.e. those being delivered to
    * the server from clients).  Invoking this method is the same as invoking <code>getIncomingInterceptorClassNames().</code>
    *
    * @deprecated As of HornetQ 2.3.0.Final, replaced by
    * {@link #getIncomingInterceptorClassNames()} and
    * {@link #getOutgoingInterceptorClassNames()}
    */
   @Deprecated
   List<String> getInterceptorClassNames();

   /**
    * Returns the list of interceptors classes used by this server for incoming messages (i.e. those being delivered to
    * the server from clients).
    */
   List<String> getIncomingInterceptorClassNames();

   /**
    * Returns the list of interceptors classes used by this server for outgoing messages (i.e. those being delivered to
    * clients from the server).
    */
   List<String> getOutgoingInterceptorClassNames();

   /**
    * Sets the list of interceptors classes used by this server for incoming messages (i.e. those
    * being delivered to the server from clients). Invoking this method is the same as invoking
    * <code>setIncomingInterceptorClassNames(List)</code> <br />
    * Classes must implement {@link org.hornetq.api.core.Interceptor}.
    * <p/>
    * Deprecated but not immediately deleted, as embedded users may be using this file.
    *
    * @deprecated As of HornetQ 2.3.0.Final, replaced by
    * {@link #setIncomingInterceptorClassNames(List)} and
    * {@link #setOutgoingInterceptorClassNames(List)}
    */
   @Deprecated
   void setInterceptorClassNames(List<String> interceptors);

   /**
    * Sets the list of interceptors classes used by this server for incoming messages (i.e. those being delivered to
    * the server from clients).
    * <br />
    * Classes must implement {@link org.hornetq.api.core.Interceptor}.
    */
   void setIncomingInterceptorClassNames(List<String> interceptors);

   /**
    * Sets the list of interceptors classes used by this server for outgoing messages (i.e. those being delivered to
    * clients from the server).
    * <br />
    * Classes must implement {@link org.hornetq.api.core.Interceptor}.
    */
   void setOutgoingInterceptorClassNames(List<String> interceptors);

   /**
    * Returns the connection time to live. <br>
    * This value overrides the connection time to live <em>sent by the client</em>. <br>
    * Default value is {@value org.hornetq.api.config.HornetQDefaultConfiguration#DEFAULT_CONNECTION_TTL_OVERRIDE}.
    */
   long getConnectionTTLOverride();

   /**
    * Sets the connection time to live.
    */
   void setConnectionTTLOverride(long ttl);

   /**
    * Returns whether code coming from connection is executed asynchronously or not. <br>
    * Default value is
    * {@value org.hornetq.api.config.HornetQDefaultConfiguration#DEFAULT_ASYNC_CONNECTION_EXECUTION_ENABLED}.
    */
   boolean isAsyncConnectionExecutionEnabled();

   /**
    * Sets whether code coming from connection is executed asynchronously or not.
    */
   void setEnabledAsyncConnectionExecution(boolean enabled);

   /**
    * Returns the acceptors configured for this server.
    */
   Set<TransportConfiguration> getAcceptorConfigurations();

   /**
    * Sets the acceptors configured for this server.
    */
   void setAcceptorConfigurations(Set<TransportConfiguration> infos);

   /**
    * Returns the connectors configured for this server.
    */
   Map<String, TransportConfiguration> getConnectorConfigurations();

   /**
    * Sets the connectors configured for this server.
    */
   void setConnectorConfigurations(Map<String, TransportConfiguration> infos);

   /**
    * Returns the broadcast groups configured for this server.
    */
   List<BroadcastGroupConfiguration> getBroadcastGroupConfigurations();

   /**
    * Sets the broadcast groups configured for this server.
    */
   void setBroadcastGroupConfigurations(List<BroadcastGroupConfiguration> configs);

   /**
    * Returns the discovery groups configured for this server.
    */
   Map<String, DiscoveryGroupConfiguration> getDiscoveryGroupConfigurations();

   /**
    * Sets the discovery groups configured for this server.
    */
   void setDiscoveryGroupConfigurations(Map<String, DiscoveryGroupConfiguration> configs);

   /**
    * Returns the grouping handler configured for this server.
    */
   GroupingHandlerConfiguration getGroupingHandlerConfiguration();

   /**
    * Sets the grouping handler configured for this server.
    */
   void setGroupingHandlerConfiguration(GroupingHandlerConfiguration groupingHandlerConfiguration);

   /**
    * Returns the bridges configured for this server.
    */
   List<BridgeConfiguration> getBridgeConfigurations();

   /**
    * Sets the bridges configured for this server.
    */
   void setBridgeConfigurations(final List<BridgeConfiguration> configs);

   /**
    * Returns the diverts configured for this server.
    */
   List<DivertConfiguration> getDivertConfigurations();

   /**
    * Sets the diverts configured for this server.
    */
   void setDivertConfigurations(final List<DivertConfiguration> configs);

   /**
    * Returns the cluster connections configured for this server.
    * <p/>
    * Modifying the returned list will modify the list of {@link ClusterConnectionConfiguration}
    * used by this configuration.
    */
   List<ClusterConnectionConfiguration> getClusterConfigurations();

   /**
    * Sets the cluster connections configured for this server.
    */
   void setClusterConfigurations(final List<ClusterConnectionConfiguration> configs);

   /**
    * Returns the queues configured for this server.
    */
   List<CoreQueueConfiguration> getQueueConfigurations();

   /**
    * Sets the queues configured for this server.
    */
   void setQueueConfigurations(final List<CoreQueueConfiguration> configs);

   /**
    * Returns the management address of this server. <br>
    * Clients can send management messages to this address to manage this server. <br>
    * Default value is {@value org.hornetq.api.config.HornetQDefaultConfiguration#DEFAULT_MANAGEMENT_ADDRESS}.
    */
   SimpleString getManagementAddress();

   /**
    * Sets the management address of this server.
    */
   void setManagementAddress(SimpleString address);

   /**
    * Returns the management notification address of this server. <br>
    * Clients can bind queues to this address to receive management notifications emitted by this
    * server. <br>
    * Default value is {@value org.hornetq.api.config.HornetQDefaultConfiguration#DEFAULT_MANAGEMENT_NOTIFICATION_ADDRESS}.
    */
   SimpleString getManagementNotificationAddress();

   /**
    * Sets the management notification address of this server.
    */
   void setManagementNotificationAddress(SimpleString address);

   /**
    * Returns the cluster user for this server. <br>
    * Default value is {@value org.hornetq.api.config.HornetQDefaultConfiguration#DEFAULT_CLUSTER_USER}.
    */
   String getClusterUser();

   /**
    * Sets the cluster user for this server.
    */
   void setClusterUser(String user);

   /**
    * Returns the cluster password for this server. <br>
    * Default value is {@value org.hornetq.api.config.HornetQDefaultConfiguration#DEFAULT_CLUSTER_PASSWORD}.
    */
   String getClusterPassword();

   /**
    * Should we notify any clients on close that they should failover.
    *
    * @return true if clients should failover
    * @see #setFailoverOnServerShutdown(boolean)
    *
    * @deprecated replaced by {@link org.hornetq.core.server.cluster.ha.HAPolicy#isFailoverOnServerShutdown()}
    */
   @Deprecated
   boolean isFailoverOnServerShutdown();

   /**
    * Sets whether to allow clients to failover on server shutdown.
    * <p/>
    * When a live server is restarted after failover the backup will shutdown if
    * {@link org.hornetq.core.server.cluster.ha.HAPolicy#isAllowAutoFailBack()} is true. This is not regarded as a normal shutdown. In this
    * case {@code failoverOnServerShutdown} is ignored, and the server will behave as if it was set
    * to {@code true}.
    *
    * @deprecated replaced by {@link org.hornetq.core.server.cluster.ha.HAPolicy#setFailoverOnServerShutdown(boolean)}
    */
   @Deprecated
   void setFailoverOnServerShutdown(boolean failoverOnServerShutdown);

   /**
    * Sets the cluster password for this server.
    */
   void setClusterPassword(String password);

   /**
    * Returns the size of the cache for pre-creating message IDs. <br>
    * Default value is {@value org.hornetq.api.config.HornetQDefaultConfiguration#DEFAULT_ID_CACHE_SIZE}.
    */
   int getIDCacheSize();

   /**
    * Sets the size of the cache for pre-creating message IDs.
    */
   void setIDCacheSize(int idCacheSize);

   /**
    * Returns whether message ID cache is persisted. <br>
    * Default value is {@value org.hornetq.api.config.HornetQDefaultConfiguration#DEFAULT_PERSIST_ID_CACHE}.
    */
   boolean isPersistIDCache();

   /**
    * Sets whether message ID cache is persisted.
    */
   void setPersistIDCache(boolean persist);

   // Journal related attributes ------------------------------------------------------------

   /**
    * Returns the file system directory used to store bindings. <br>
    * Default value is {@value org.hornetq.api.config.HornetQDefaultConfiguration#DEFAULT_BINDINGS_DIRECTORY}.
    */
   String getBindingsDirectory();

   /**
    * Sets the file system directory used to store bindings.
    */
   void setBindingsDirectory(String dir);

   /**
    * The max number of concurrent reads allowed on paging.
    * <p/>
    * Default value is {@value org.hornetq.api.config.HornetQDefaultConfiguration#DEFAULT_MAX_CONCURRENT_PAGE_IO}.
    */
   int getPageMaxConcurrentIO();

   /**
    * The max number of concurrent reads allowed on paging.
    * <p/>
    * Default = 5
    */
   void setPageMaxConcurrentIO(int maxIO);

   /**
    * Returns the file system directory used to store journal log. <br>
    * Default value is {@value org.hornetq.api.config.HornetQDefaultConfiguration#DEFAULT_JOURNAL_DIR}.
    */
   String getJournalDirectory();

   /**
    * Sets the file system directory used to store journal log.
    */
   void setJournalDirectory(String dir);

   /**
    * Returns the type of journal used by this server (either {@code NIO} or {@code ASYNCIO}).
    * <br>
    * Default value is ASYNCIO.
    */
   JournalType getJournalType();

   /**
    * Sets the type of journal used by this server (either {@code NIO} or {@code ASYNCIO}).
    */
   void setJournalType(JournalType type);

   /**
    * Returns whether the journal is synchronized when receiving transactional data. <br>
    * Default value is {@value org.hornetq.api.config.HornetQDefaultConfiguration#DEFAULT_JOURNAL_SYNC_TRANSACTIONAL}.
    */
   boolean isJournalSyncTransactional();

   /**
    * Sets whether the journal is synchronized when receiving transactional data.
    */
   void setJournalSyncTransactional(boolean sync);

   /**
    * Returns whether the journal is synchronized when receiving non-transactional data. <br>
    * Default value is {@value org.hornetq.api.config.HornetQDefaultConfiguration#DEFAULT_JOURNAL_SYNC_NON_TRANSACTIONAL}.
    */
   boolean isJournalSyncNonTransactional();

   /**
    * Sets whether the journal is synchronized when receiving non-transactional data.
    */
   void setJournalSyncNonTransactional(boolean sync);

   /**
    * Returns the size (in bytes) of each journal files. <br>
    * Default value is {@value org.hornetq.api.config.HornetQDefaultConfiguration#DEFAULT_JOURNAL_FILE_SIZE}.
    */
   int getJournalFileSize();

   /**
    * Sets the size (in bytes) of each journal files.
    */
   void setJournalFileSize(int size);

   /**
    * Returns the minimal number of journal files before compacting. <br>
    * Default value is {@value org.hornetq.api.config.HornetQDefaultConfiguration#DEFAULT_JOURNAL_COMPACT_MIN_FILES}.
    */
   int getJournalCompactMinFiles();

   /**
    * Sets the minimal number of journal files before compacting.
    */
   void setJournalCompactMinFiles(int minFiles);

   /**
    * Returns the percentage of live data before compacting the journal. <br>
    * Default value is {@value org.hornetq.api.config.HornetQDefaultConfiguration#DEFAULT_JOURNAL_COMPACT_PERCENTAGE}.
    */
   int getJournalCompactPercentage();

   /**
    * Sets the percentage of live data before compacting the journal.
    */
   void setJournalCompactPercentage(int percentage);

   /**
    * Returns the number of journal files to pre-create. <br>
    * Default value is {@value org.hornetq.api.config.HornetQDefaultConfiguration#DEFAULT_JOURNAL_MIN_FILES}.
    */
   int getJournalMinFiles();

   /**
    * Sets the number of journal files to pre-create.
    */
   void setJournalMinFiles(int files);

   // AIO and NIO need different values for these params

   /**
    * Returns the maximum number of write requests that can be in the AIO queue at any given time. <br>
    * Default value is {@value org.hornetq.api.config.HornetQDefaultConfiguration#DEFAULT_JOURNAL_MAX_IO_AIO}.
    */
   int getJournalMaxIO_AIO();

   /**
    * Sets the maximum number of write requests that can be in the AIO queue at any given time.
    */
   void setJournalMaxIO_AIO(int journalMaxIO);

   /**
    * Returns the timeout (in nanoseconds) used to flush buffers in the AIO queue.
    * <br>
    * Default value is {@value org.hornetq.core.journal.impl.JournalConstants#DEFAULT_JOURNAL_BUFFER_TIMEOUT_AIO}.
    */
   int getJournalBufferTimeout_AIO();

   /**
    * Sets the timeout (in nanoseconds) used to flush buffers in the AIO queue.
    */
   void setJournalBufferTimeout_AIO(int journalBufferTimeout);

   /**
    * Returns the buffer size (in bytes) for AIO.
    * <br>
    * Default value is {@value org.hornetq.core.journal.impl.JournalConstants#DEFAULT_JOURNAL_BUFFER_SIZE_AIO}.
    */
   int getJournalBufferSize_AIO();

   /**
    * Sets the buffer size (in bytes) for AIO.
    */
   void setJournalBufferSize_AIO(int journalBufferSize);

   /**
    * Returns the maximum number of write requests for NIO journal. <br>
    * Default value is {@value org.hornetq.api.config.HornetQDefaultConfiguration#DEFAULT_JOURNAL_MAX_IO_NIO}.
    */
   int getJournalMaxIO_NIO();

   /**
    * Sets the maximum number of write requests for NIO journal.
    */
   void setJournalMaxIO_NIO(int journalMaxIO);

   /**
    * Returns the timeout (in nanoseconds) used to flush buffers in the NIO.
    * <br>
    * Default value is {@value org.hornetq.core.journal.impl.JournalConstants#DEFAULT_JOURNAL_BUFFER_TIMEOUT_NIO}.
    */
   int getJournalBufferTimeout_NIO();

   /**
    * Sets the timeout (in nanoseconds) used to flush buffers in the NIO.
    */
   void setJournalBufferTimeout_NIO(int journalBufferTimeout);

   /**
    * Returns the buffer size (in bytes) for NIO.
    * <br>
    * Default value is {@value org.hornetq.core.journal.impl.JournalConstants#DEFAULT_JOURNAL_BUFFER_SIZE_NIO}.
    */
   int getJournalBufferSize_NIO();

   /**
    * Sets the buffer size (in bytes) for NIO.
    */
   void setJournalBufferSize_NIO(int journalBufferSize);

   /**
    * Returns whether the bindings directory is created on this server startup. <br>
    * Default value is {@value org.hornetq.api.config.HornetQDefaultConfiguration#DEFAULT_CREATE_BINDINGS_DIR}.
    */
   boolean isCreateBindingsDir();

   /**
    * Sets whether the bindings directory is created on this server startup.
    */
   void setCreateBindingsDir(boolean create);

   /**
    * Returns whether the journal directory is created on this server startup. <br>
    * Default value is {@value org.hornetq.api.config.HornetQDefaultConfiguration#DEFAULT_CREATE_JOURNAL_DIR}.
    */
   boolean isCreateJournalDir();

   /**
    * Sets whether the journal directory is created on this server startup.
    */
   void setCreateJournalDir(boolean create);

   // Undocumented attributes

   boolean isLogJournalWriteRate();

   void setLogJournalWriteRate(boolean rate);

   int getJournalPerfBlastPages();

   void setJournalPerfBlastPages(int pages);

   long getServerDumpInterval();

   void setServerDumpInterval(long interval);

   int getMemoryWarningThreshold();

   void setMemoryWarningThreshold(int memoryWarningThreshold);

   long getMemoryMeasureInterval();

   void setMemoryMeasureInterval(long memoryMeasureInterval);

   boolean isRunSyncSpeedTest();

   void setRunSyncSpeedTest(boolean run);

   // Paging Properties --------------------------------------------------------------------

   /**
    * Returns the file system directory used to store paging files. <br>
    * Default value is {@value org.hornetq.api.config.HornetQDefaultConfiguration#DEFAULT_PAGING_DIR}.
    */
   String getPagingDirectory();

   /**
    * Sets the file system directory used to store paging files.
    */
   void setPagingDirectory(String dir);

   // Large Messages Properties ------------------------------------------------------------

   /**
    * Returns the file system directory used to store large messages. <br>
    * Default value is {@value org.hornetq.api.config.HornetQDefaultConfiguration#DEFAULT_LARGE_MESSAGES_DIR}.
    */
   String getLargeMessagesDirectory();

   /**
    * Sets the file system directory used to store large messages.
    */
   void setLargeMessagesDirectory(String directory);

   // Other Properties ---------------------------------------------------------------------

   /**
    * Returns whether wildcard routing is supported by this server. <br>
    * Default value is {@value org.hornetq.api.config.HornetQDefaultConfiguration#DEFAULT_WILDCARD_ROUTING_ENABLED}.
    */
   boolean isWildcardRoutingEnabled();

   /**
    * Sets whether wildcard routing is supported by this server.
    */
   void setWildcardRoutingEnabled(boolean enabled);

   /**
    * Returns the timeout (in milliseconds) after which transactions is removed from the resource
    * manager after it was created. <br>
    * Default value is {@value org.hornetq.api.config.HornetQDefaultConfiguration#DEFAULT_TRANSACTION_TIMEOUT}.
    */
   long getTransactionTimeout();

   /**
    * Sets the timeout (in milliseconds) after which transactions is removed
    * from the resource manager after it was created.
    */
   void setTransactionTimeout(long timeout);

   /**
    * Returns whether message counter is enabled for this server. <br>
    * Default value is {@value org.hornetq.api.config.HornetQDefaultConfiguration#DEFAULT_MESSAGE_COUNTER_ENABLED}.
    */
   boolean isMessageCounterEnabled();

   /**
    * Sets whether message counter is enabled for this server.
    */
   void setMessageCounterEnabled(boolean enabled);

   /**
    * Returns the sample period (in milliseconds) to take message counter snapshot. <br>
    * Default value is {@value org.hornetq.api.config.HornetQDefaultConfiguration#DEFAULT_MESSAGE_COUNTER_SAMPLE_PERIOD}.
    */
   long getMessageCounterSamplePeriod();

   /**
    * Sets the sample period to take message counter snapshot.
    *
    * @param period value must be greater than 1000ms
    */
   void setMessageCounterSamplePeriod(long period);

   /**
    * Returns the maximum number of days kept in memory for message counter. <br>
    * Default value is {@value org.hornetq.api.config.HornetQDefaultConfiguration#DEFAULT_MESSAGE_COUNTER_MAX_DAY_HISTORY}.
    */
   int getMessageCounterMaxDayHistory();

   /**
    * Sets the maximum number of days kept in memory for message counter.
    *
    * @param maxDayHistory value must be greater than 0
    */
   void setMessageCounterMaxDayHistory(int maxDayHistory);

   /**
    * Returns the frequency (in milliseconds) to scan transactions to detect which transactions have
    * timed out. <br>
    * Default value is {@value org.hornetq.api.config.HornetQDefaultConfiguration#DEFAULT_TRANSACTION_TIMEOUT_SCAN_PERIOD}.
    */
   long getTransactionTimeoutScanPeriod();

   /**
    * Sets the frequency (in milliseconds)  to scan transactions to detect which transactions
    * have timed out.
    */
   void setTransactionTimeoutScanPeriod(long period);

   /**
    * Returns the frequency (in milliseconds) to scan messages to detect which messages have
    * expired. <br>
    * Default value is {@value org.hornetq.api.config.HornetQDefaultConfiguration#DEFAULT_MESSAGE_EXPIRY_SCAN_PERIOD}.
    */
   long getMessageExpiryScanPeriod();

   /**
    * Sets the frequency (in milliseconds)  to scan messages to detect which messages
    * have expired.
    */
   void setMessageExpiryScanPeriod(long messageExpiryScanPeriod);

   /**
    * Returns the priority of the thread used to scan message expiration. <br>
    * Default value is {@value org.hornetq.api.config.HornetQDefaultConfiguration#DEFAULT_MESSAGE_EXPIRY_THREAD_PRIORITY}.
    */
   int getMessageExpiryThreadPriority();

   /**
    * Sets the priority of the thread used to scan message expiration.
    */
   void setMessageExpiryThreadPriority(int messageExpiryThreadPriority);

   /**
    * @return A list of AddressSettings per matching to be deployed to the address settings repository
    */
   Map<String, AddressSettings> getAddressesSettings();

   /**
    * @param addressesSettings list of AddressSettings per matching to be deployed to the address
    *                          settings repository
    */
   void setAddressesSettings(Map<String, AddressSettings> addressesSettings);

   /**
    * @param roles a list of roles per matching
    */
   void setSecurityRoles(Map<String, Set<Role>> roles);

   /**
    * @return a list of roles per matching
    */
   Map<String, Set<Role>> getSecurityRoles();

   void setConnectorServiceConfigurations(List<ConnectorServiceConfiguration> configs);

   /**
    * @return list of {@link ConnectorServiceConfiguration}
    */
   List<ConnectorServiceConfiguration> getConnectorServiceConfigurations();

   /**
    * Returns the delay to wait before fail-back occurs on restart.
    *
    * @deprecated replaced by {@link org.hornetq.core.server.cluster.ha.HAPolicy#getFailbackDelay()}
    */
   @Deprecated
   long getFailbackDelay();

   /**
    * Sets the fail-back delay.
    *
    * @deprecated replaced by {@link org.hornetq.core.server.cluster.ha.HAPolicy#setFailbackDelay(long)}
    */
   @Deprecated
   void setFailbackDelay(long delay);

   /**
    * Whether to check if the cluster already has a (live) node with our node-ID.
    * <p/>
    * If the cluster does contain a server using this server's node-ID, then this server will assume
    * that fail-over has occurred and will try to trigger a fail-back.
    * <p/>
    * Enabling this check will slow down a server start-up slightly.
    *
    * @return true if we want to make the check
    */
   boolean isCheckForLiveServer();

   /**
    * Sets whether to check if the cluster already has a (live) node with our node-ID.
    * <p/>
    * If the cluster does contain a server using this server's node-ID, then this server will assume
    * that fail-over has occurred and will try to trigger a fail-back.
    * <p/>
    * Enabling this check will slow down a server start-up slightly.
    *
    * @param checkForLiveServer true if we want to make the check
    */
   void setCheckForLiveServer(boolean checkForLiveServer);

   /**
    * The default password decoder
    */
   void setPasswordCodec(String codec);

   /**
    * Gets the default password decoder
    */
   String getPasswordCodec();

   /**
    * Sets if passwords should be masked or not. True means the passwords should be masked.
    */
   void setMaskPassword(boolean maskPassword);

   /**
    * If passwords are masked. True means the passwords are masked.
    */
   boolean isMaskPassword();

   /**
    * Name of the cluster configuration to use for replication.
    * <p/>
    * Only applicable for servers with more than one cluster configuration. This value is only used
    * by replicating backups and live servers that attempt fail-back.
    *
    * @param clusterName
    *
    * @deprecated replaced by {@link org.hornetq.core.server.cluster.ha.HAPolicy#setReplicationClustername(String)}
    */
   @Deprecated
   void setReplicationClustername(String clusterName);

   /**
    * @return name of the cluster configuration to use
    * @see #setReplicationClustername(String)
    *
    * @deprecated replaced by {@link org.hornetq.core.server.cluster.ha.HAPolicy#getReplicationClustername()}
    */
   @Deprecated
   String getReplicationClustername();

   /**
    * Name of the cluster configuration to use for scaling down.
    * <p/>
    * Only applicable for servers with more than one cluster configuration.
    *
    * @param clusterName
    *
    * @deprecated replaced by {@link org.hornetq.core.server.cluster.ha.HAPolicy#setScaleDownClustername(String)}
    */
   @Deprecated
   void setScaleDownClustername(String clusterName);

   /**
    * @return name of the cluster configuration to use
    * @see #setScaleDownClustername(String)
    *
    * @deprecated replaced by {@link org.hornetq.core.server.cluster.ha.HAPolicy#getScaleDownClustername()}
    */
   @Deprecated
   String getScaleDownClustername();

   /*
   * Whether or not that HornetQ should use all protocols available on the classpath. If false only the core protocol will
   * be set, any other protocols will need to be set directly on the HornetQServer
   * */
   void setResolveProtocols(boolean resolveProtocols);

   /*
   * @see #setResolveProtocols()
   * @return whether HornetQ should resolve and use any Protocols available on the classpath
   * Default value is {@value org.hornetq.api.config.HornetQDefaultConfiguration#DEFAULT_RESOLVE_PROTOCOLS}.
   * */
   boolean isResolveProtocols();

   /**
    * How many backup journals to keep after failback occurs.
    * <p/>
    * This value is only used by replicating backups after a live server has failed back. Beofre the backup restarts
    * it will copy its journals into another directory to keep.
    *
    * @param maxSavedReplicatedJournalsSize
    *
    * @deprecated replaced by {@link org.hornetq.core.server.cluster.ha.HAPolicy#getMaxSavedReplicatedJournalsSize()}
    */
   @Deprecated
   void setMaxSavedReplicatedJournalSize(int maxSavedReplicatedJournalsSize);

   /**
    * @return the number of backup journals to keep after failback has occurred
    * @see #setMaxSavedReplicatedJournalSize(int)
    *
    * @deprecated replaced by {@link org.hornetq.core.server.cluster.ha.HAPolicy#getMaxSavedReplicatedJournalsSize()}
    */
   @Deprecated
   int getMaxSavedReplicatedJournalsSize();

   Set<Configuration> getBackupServerConfigurations();

   Configuration copy() throws Exception;

   /**
    * @deprecated replaced by {@link org.hornetq.core.server.cluster.ha.HAPolicy#setBackupStrategy(BackupStrategy)}
    */
   @Deprecated
   void setBackupStrategy(BackupStrategy strategy);

   /**
    * @deprecated replaced by {@link org.hornetq.core.server.cluster.ha.HAPolicy#getBackupStrategy()}
    */
   @Deprecated
   BackupStrategy getBackupStrategy();

   void setJournalLockAcquisitionTimeout(long journalLockAcquisitionTimeout);

   long getJournalLockAcquisitionTimeout();

   HAPolicy getHAPolicy();

   void setHAPolicy(HAPolicy haPolicy);
}
