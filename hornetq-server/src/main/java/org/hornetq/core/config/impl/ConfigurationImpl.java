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
package org.hornetq.core.config.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hornetq.api.config.HornetQDefaultConfiguration;
import org.hornetq.api.core.BroadcastGroupConfiguration;
import org.hornetq.api.core.DiscoveryGroupConfiguration;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.config.BackupStrategy;
import org.hornetq.core.config.BridgeConfiguration;
import org.hornetq.core.config.ClusterConnectionConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.ConnectorServiceConfiguration;
import org.hornetq.core.config.CoreQueueConfiguration;
import org.hornetq.core.config.DivertConfiguration;
import org.hornetq.core.security.Role;
import org.hornetq.core.server.JournalType;
import org.hornetq.core.server.cluster.ha.HAPolicy;
import org.hornetq.core.server.group.impl.GroupingHandlerConfiguration;
import org.hornetq.core.settings.impl.AddressSettings;

/**
 * @author <a href="mailto:ataylor@redhat.com>Andy Taylor</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class ConfigurationImpl implements Configuration
{
   // Constants ------------------------------------------------------------------------------

   public static final JournalType DEFAULT_JOURNAL_TYPE = JournalType.ASYNCIO;

   private static final long serialVersionUID = 4077088945050267843L;

   // Attributes -----------------------------------------------------------------------------

   private String name = "ConfigurationImpl::" + System.identityHashCode(this);

   protected boolean fileDeploymentEnabled = HornetQDefaultConfiguration.isDefaultFileDeploymentEnabled();

   private boolean persistenceEnabled = HornetQDefaultConfiguration.isDefaultPersistenceEnabled();

   protected long fileDeploymentScanPeriod = HornetQDefaultConfiguration.getDefaultFileDeployerScanPeriod();

   private boolean persistDeliveryCountBeforeDelivery =
            HornetQDefaultConfiguration.isDefaultPersistDeliveryCountBeforeDelivery();

   private int scheduledThreadPoolMaxSize = HornetQDefaultConfiguration.getDefaultScheduledThreadPoolMaxSize();

   private int threadPoolMaxSize = HornetQDefaultConfiguration.getDefaultThreadPoolMaxSize();

   private long securityInvalidationInterval = HornetQDefaultConfiguration.getDefaultSecurityInvalidationInterval();

   private boolean securityEnabled = HornetQDefaultConfiguration.isDefaultSecurityEnabled();

   protected boolean jmxManagementEnabled = HornetQDefaultConfiguration.isDefaultJmxManagementEnabled();

   protected String jmxDomain = HornetQDefaultConfiguration.getDefaultJmxDomain();

   protected long connectionTTLOverride = HornetQDefaultConfiguration.getDefaultConnectionTtlOverride();

   protected boolean asyncConnectionExecutionEnabled = HornetQDefaultConfiguration.isDefaultAsyncConnectionExecutionEnabled();

   private long messageExpiryScanPeriod = HornetQDefaultConfiguration.getDefaultMessageExpiryScanPeriod();

   private int messageExpiryThreadPriority = HornetQDefaultConfiguration.getDefaultMessageExpiryThreadPriority();

   protected int idCacheSize = HornetQDefaultConfiguration.getDefaultIdCacheSize();

   private boolean persistIDCache = HornetQDefaultConfiguration.isDefaultPersistIdCache();

   private List<String> incomingInterceptorClassNames = new ArrayList<String>();

   private List<String> outgoingInterceptorClassNames = new ArrayList<String>();

   protected Map<String, TransportConfiguration> connectorConfigs = new HashMap<String, TransportConfiguration>();

   private Set<TransportConfiguration> acceptorConfigs = new HashSet<TransportConfiguration>();

   protected List<BridgeConfiguration> bridgeConfigurations = new ArrayList<BridgeConfiguration>();

   protected List<DivertConfiguration> divertConfigurations = new ArrayList<DivertConfiguration>();

   protected List<ClusterConnectionConfiguration> clusterConfigurations = new ArrayList<ClusterConnectionConfiguration>();

   private List<CoreQueueConfiguration> queueConfigurations = new ArrayList<CoreQueueConfiguration>();

   protected List<BroadcastGroupConfiguration> broadcastGroupConfigurations = new ArrayList<BroadcastGroupConfiguration>();

   protected Map<String, DiscoveryGroupConfiguration> discoveryGroupConfigurations = new LinkedHashMap<String, DiscoveryGroupConfiguration>();

   // Paging related attributes ------------------------------------------------------------

   private String pagingDirectory = HornetQDefaultConfiguration.getDefaultPagingDir();

   // File related attributes -----------------------------------------------------------

   private int maxConcurrentPageIO = HornetQDefaultConfiguration.getDefaultMaxConcurrentPageIo();

   protected String largeMessagesDirectory = HornetQDefaultConfiguration.getDefaultLargeMessagesDir();

   protected String bindingsDirectory = HornetQDefaultConfiguration.getDefaultBindingsDirectory();

   protected boolean createBindingsDir = HornetQDefaultConfiguration.isDefaultCreateBindingsDir();

   protected String journalDirectory = HornetQDefaultConfiguration.getDefaultJournalDir();

   protected boolean createJournalDir = HornetQDefaultConfiguration.isDefaultCreateJournalDir();

   public JournalType journalType = ConfigurationImpl.DEFAULT_JOURNAL_TYPE;

   protected boolean journalSyncTransactional = HornetQDefaultConfiguration.isDefaultJournalSyncTransactional();

   protected boolean journalSyncNonTransactional = HornetQDefaultConfiguration.isDefaultJournalSyncNonTransactional();

   protected int journalCompactMinFiles = HornetQDefaultConfiguration.getDefaultJournalCompactMinFiles();

   protected int journalCompactPercentage = HornetQDefaultConfiguration.getDefaultJournalCompactPercentage();

   protected int journalFileSize = HornetQDefaultConfiguration.getDefaultJournalFileSize();

   protected int journalMinFiles = HornetQDefaultConfiguration.getDefaultJournalMinFiles();

   // AIO and NIO need different values for these attributes

   protected int journalMaxIO_AIO = HornetQDefaultConfiguration.getDefaultJournalMaxIoAio();

   protected int journalBufferTimeout_AIO = HornetQDefaultConfiguration.getDefaultJournalBufferTimeoutAio();

   protected int journalBufferSize_AIO = HornetQDefaultConfiguration.getDefaultJournalBufferSizeAio();

   protected int journalMaxIO_NIO = HornetQDefaultConfiguration.getDefaultJournalMaxIoNio();

   protected int journalBufferTimeout_NIO = HornetQDefaultConfiguration.getDefaultJournalBufferTimeoutNio();

   protected int journalBufferSize_NIO = HornetQDefaultConfiguration.getDefaultJournalBufferSizeNio();

   protected boolean logJournalWriteRate = HornetQDefaultConfiguration.isDefaultJournalLogWriteRate();

   protected int journalPerfBlastPages = HornetQDefaultConfiguration.getDefaultJournalPerfBlastPages();

   protected boolean runSyncSpeedTest = HornetQDefaultConfiguration.isDefaultRunSyncSpeedTest();

   private boolean wildcardRoutingEnabled = HornetQDefaultConfiguration.isDefaultWildcardRoutingEnabled();

   private boolean messageCounterEnabled = HornetQDefaultConfiguration.isDefaultMessageCounterEnabled();

   private long messageCounterSamplePeriod = HornetQDefaultConfiguration.getDefaultMessageCounterSamplePeriod();

   private int messageCounterMaxDayHistory = HornetQDefaultConfiguration.getDefaultMessageCounterMaxDayHistory();

   private long transactionTimeout = HornetQDefaultConfiguration.getDefaultTransactionTimeout();

   private long transactionTimeoutScanPeriod = HornetQDefaultConfiguration.getDefaultTransactionTimeoutScanPeriod();

   private SimpleString managementAddress = HornetQDefaultConfiguration.getDefaultManagementAddress();

   private SimpleString managementNotificationAddress = HornetQDefaultConfiguration.getDefaultManagementNotificationAddress();

   protected String clusterUser = HornetQDefaultConfiguration.getDefaultClusterUser();

   protected String clusterPassword = HornetQDefaultConfiguration.getDefaultClusterPassword();

   private long serverDumpInterval = HornetQDefaultConfiguration.getDefaultServerDumpInterval();

   protected boolean failoverOnServerShutdown = HornetQDefaultConfiguration.isDefaultFailoverOnServerShutdown();

   // percentage of free memory which triggers warning from the memory manager
   private int memoryWarningThreshold = HornetQDefaultConfiguration.getDefaultMemoryWarningThreshold();

   private long memoryMeasureInterval = HornetQDefaultConfiguration.getDefaultMemoryMeasureInterval();

   protected GroupingHandlerConfiguration groupingHandlerConfiguration;

   private Map<String, AddressSettings> addressesSettings = new HashMap<String, AddressSettings>();

   private Map<String, Set<Role>> securitySettings = new HashMap<String, Set<Role>>();

   protected List<ConnectorServiceConfiguration> connectorServiceConfigurations = new ArrayList<ConnectorServiceConfiguration>();

   private boolean checkForLiveServer = HornetQDefaultConfiguration.isDefaultCheckForLiveServer();

   private boolean maskPassword = HornetQDefaultConfiguration.isDefaultMaskPassword();

   private transient String passwordCodec;

   private boolean resolveProtocols = HornetQDefaultConfiguration.isDefaultResolveProtocols();

   private Set<Configuration> backupServerConfigurations = new HashSet<>();

   private BackupStrategy backupStrategy;

   private long journalLockAcquisitionTimeout = HornetQDefaultConfiguration.getDefaultJournalLockAcquisitionTimeout();

   private HAPolicy haPolicy = new HAPolicy();

   // Public -------------------------------------------------------------------------

   public boolean isClustered()
   {
      return !getClusterConfigurations().isEmpty();
   }

   @Deprecated
   public boolean isAllowAutoFailBack()
   {
      return haPolicy.isAllowAutoFailBack();
   }

   @Deprecated
   public void setAllowAutoFailBack(boolean allowAutoFailBack)
   {
      haPolicy.setAllowAutoFailBack(allowAutoFailBack);
   }

   @Deprecated
   public boolean isBackup()
   {
      return haPolicy.isBackup();
   }

   public boolean isFileDeploymentEnabled()
   {
      return fileDeploymentEnabled;
   }

   public void setFileDeploymentEnabled(final boolean enable)
   {
      fileDeploymentEnabled = enable;
   }

   public boolean isPersistenceEnabled()
   {
      return persistenceEnabled;
   }

   public void setPersistenceEnabled(final boolean enable)
   {
      persistenceEnabled = enable;
   }

   public long getFileDeployerScanPeriod()
   {
      return fileDeploymentScanPeriod;
   }

   public void setFileDeployerScanPeriod(final long period)
   {
      fileDeploymentScanPeriod = period;
   }

   /**
    * @return the persistDeliveryCountBeforeDelivery
    */
   public boolean isPersistDeliveryCountBeforeDelivery()
   {
      return persistDeliveryCountBeforeDelivery;
   }

   public void setPersistDeliveryCountBeforeDelivery(final boolean persistDeliveryCountBeforeDelivery)
   {
      this.persistDeliveryCountBeforeDelivery = persistDeliveryCountBeforeDelivery;
   }

   @Deprecated
   public void setBackup(final boolean backup)
   {
      // this is done via the haPolicy now
   }

   @Deprecated
   public boolean isSharedStore()
   {
      return haPolicy.isSharedStore();
   }

   @Deprecated
   public void setSharedStore(final boolean sharedStore)
   {
      // this is done via the haPolicy now
   }

   public int getScheduledThreadPoolMaxSize()
   {
      return scheduledThreadPoolMaxSize;
   }

   public void setScheduledThreadPoolMaxSize(final int maxSize)
   {
      scheduledThreadPoolMaxSize = maxSize;
   }

   public int getThreadPoolMaxSize()
   {
      return threadPoolMaxSize;
   }

   public void setThreadPoolMaxSize(final int maxSize)
   {
      threadPoolMaxSize = maxSize;
   }

   public long getSecurityInvalidationInterval()
   {
      return securityInvalidationInterval;
   }

   public void setSecurityInvalidationInterval(final long interval)
   {
      securityInvalidationInterval = interval;
   }

   public long getConnectionTTLOverride()
   {
      return connectionTTLOverride;
   }

   public void setConnectionTTLOverride(final long ttl)
   {
      connectionTTLOverride = ttl;
   }

   public boolean isAsyncConnectionExecutionEnabled()
   {
      return asyncConnectionExecutionEnabled;
   }

   public void setEnabledAsyncConnectionExecution(final boolean enabled)
   {
      asyncConnectionExecutionEnabled = enabled;
   }

   @Deprecated
   @Override
   public List<String> getInterceptorClassNames()
   {
      return getIncomingInterceptorClassNames();
   }

   @Deprecated
   @Override
   public void setInterceptorClassNames(final List<String> interceptors)
   {
      setIncomingInterceptorClassNames(interceptors);
   }

   public List<String> getIncomingInterceptorClassNames()
   {
      return incomingInterceptorClassNames;
   }

   public void setIncomingInterceptorClassNames(final List<String> interceptors)
   {
      incomingInterceptorClassNames = interceptors;
   }

   public List<String> getOutgoingInterceptorClassNames()
   {
      return outgoingInterceptorClassNames;
   }

   public void setOutgoingInterceptorClassNames(final List<String> interceptors)
   {
      outgoingInterceptorClassNames = interceptors;
   }

   public Set<TransportConfiguration> getAcceptorConfigurations()
   {
      return acceptorConfigs;
   }

   public void setAcceptorConfigurations(final Set<TransportConfiguration> infos)
   {
      acceptorConfigs = infos;
   }

   public Map<String, TransportConfiguration> getConnectorConfigurations()
   {
      return connectorConfigs;
   }

   public void setConnectorConfigurations(final Map<String, TransportConfiguration> infos)
   {
      connectorConfigs = infos;
   }

   public GroupingHandlerConfiguration getGroupingHandlerConfiguration()
   {
      return groupingHandlerConfiguration;
   }

   public void setGroupingHandlerConfiguration(final GroupingHandlerConfiguration groupingHandlerConfiguration)
   {
      this.groupingHandlerConfiguration = groupingHandlerConfiguration;
   }

   public List<BridgeConfiguration> getBridgeConfigurations()
   {
      return bridgeConfigurations;
   }

   public void setBridgeConfigurations(final List<BridgeConfiguration> configs)
   {
      bridgeConfigurations = configs;
   }

   public List<BroadcastGroupConfiguration> getBroadcastGroupConfigurations()
   {
      return broadcastGroupConfigurations;
   }

   public void setBroadcastGroupConfigurations(final List<BroadcastGroupConfiguration> configs)
   {
      broadcastGroupConfigurations = configs;
   }

   public List<ClusterConnectionConfiguration> getClusterConfigurations()
   {
      return clusterConfigurations;
   }

   public void setClusterConfigurations(final List<ClusterConnectionConfiguration> configs)
   {
      clusterConfigurations = configs;
   }

   public List<DivertConfiguration> getDivertConfigurations()
   {
      return divertConfigurations;
   }

   public void setDivertConfigurations(final List<DivertConfiguration> configs)
   {
      divertConfigurations = configs;
   }

   public List<CoreQueueConfiguration> getQueueConfigurations()
   {
      return queueConfigurations;
   }

   public void setQueueConfigurations(final List<CoreQueueConfiguration> configs)
   {
      queueConfigurations = configs;
   }

   public Map<String, DiscoveryGroupConfiguration> getDiscoveryGroupConfigurations()
   {
      return discoveryGroupConfigurations;
   }

   public void setDiscoveryGroupConfigurations(final Map<String, DiscoveryGroupConfiguration> discoveryGroupConfigurations)
   {
      this.discoveryGroupConfigurations = discoveryGroupConfigurations;
   }

   public int getIDCacheSize()
   {
      return idCacheSize;
   }

   public void setIDCacheSize(final int idCacheSize)
   {
      this.idCacheSize = idCacheSize;
   }

   public boolean isPersistIDCache()
   {
      return persistIDCache;
   }

   public void setPersistIDCache(final boolean persist)
   {
      persistIDCache = persist;
   }

   public String getBindingsDirectory()
   {
      return bindingsDirectory;
   }

   public void setBindingsDirectory(final String dir)
   {
      bindingsDirectory = dir;
   }


   @Override
   public int getPageMaxConcurrentIO()
   {
      return maxConcurrentPageIO;
   }

   @Override
   public void setPageMaxConcurrentIO(int maxIO)
   {
      this.maxConcurrentPageIO = maxIO;
   }


   public String getJournalDirectory()
   {
      return journalDirectory;
   }

   public void setJournalDirectory(final String dir)
   {
      journalDirectory = dir;
   }

   public JournalType getJournalType()
   {
      return journalType;
   }

   public void setPagingDirectory(final String dir)
   {
      pagingDirectory = dir;
   }

   public String getPagingDirectory()
   {
      return pagingDirectory;
   }

   public void setJournalType(final JournalType type)
   {
      journalType = type;
   }

   public boolean isJournalSyncTransactional()
   {
      return journalSyncTransactional;
   }

   public void setJournalSyncTransactional(final boolean sync)
   {
      journalSyncTransactional = sync;
   }

   public boolean isJournalSyncNonTransactional()
   {
      return journalSyncNonTransactional;
   }

   public void setJournalSyncNonTransactional(final boolean sync)
   {
      journalSyncNonTransactional = sync;
   }

   public int getJournalFileSize()
   {
      return journalFileSize;
   }

   public void setJournalFileSize(final int size)
   {
      journalFileSize = size;
   }

   public int getJournalMinFiles()
   {
      return journalMinFiles;
   }

   public void setJournalMinFiles(final int files)
   {
      journalMinFiles = files;
   }

   public boolean isLogJournalWriteRate()
   {
      return logJournalWriteRate;
   }

   public void setLogJournalWriteRate(final boolean logJournalWriteRate)
   {
      this.logJournalWriteRate = logJournalWriteRate;
   }

   public int getJournalPerfBlastPages()
   {
      return journalPerfBlastPages;
   }

   public void setJournalPerfBlastPages(final int journalPerfBlastPages)
   {
      this.journalPerfBlastPages = journalPerfBlastPages;
   }

   public boolean isRunSyncSpeedTest()
   {
      return runSyncSpeedTest;
   }

   public void setRunSyncSpeedTest(final boolean run)
   {
      runSyncSpeedTest = run;
   }

   public boolean isCreateBindingsDir()
   {
      return createBindingsDir;
   }

   public void setCreateBindingsDir(final boolean create)
   {
      createBindingsDir = create;
   }

   public boolean isCreateJournalDir()
   {
      return createJournalDir;
   }

   public void setCreateJournalDir(final boolean create)
   {
      createJournalDir = create;
   }

   public boolean isWildcardRoutingEnabled()
   {
      return wildcardRoutingEnabled;
   }

   public void setWildcardRoutingEnabled(final boolean enabled)
   {
      wildcardRoutingEnabled = enabled;
   }

   public long getTransactionTimeout()
   {
      return transactionTimeout;
   }

   public void setTransactionTimeout(final long timeout)
   {
      transactionTimeout = timeout;
   }

   public long getTransactionTimeoutScanPeriod()
   {
      return transactionTimeoutScanPeriod;
   }

   public void setTransactionTimeoutScanPeriod(final long period)
   {
      transactionTimeoutScanPeriod = period;
   }

   public long getMessageExpiryScanPeriod()
   {
      return messageExpiryScanPeriod;
   }

   public void setMessageExpiryScanPeriod(final long messageExpiryScanPeriod)
   {
      this.messageExpiryScanPeriod = messageExpiryScanPeriod;
   }

   public int getMessageExpiryThreadPriority()
   {
      return messageExpiryThreadPriority;
   }

   public void setMessageExpiryThreadPriority(final int messageExpiryThreadPriority)
   {
      this.messageExpiryThreadPriority = messageExpiryThreadPriority;
   }

   public boolean isSecurityEnabled()
   {
      return securityEnabled;
   }

   public void setSecurityEnabled(final boolean enabled)
   {
      securityEnabled = enabled;
   }

   public boolean isJMXManagementEnabled()
   {
      return jmxManagementEnabled;
   }

   public void setJMXManagementEnabled(final boolean enabled)
   {
      jmxManagementEnabled = enabled;
   }

   public String getJMXDomain()
   {
      return jmxDomain;
   }

   public void setJMXDomain(final String domain)
   {
      jmxDomain = domain;
   }

   public String getLargeMessagesDirectory()
   {
      return largeMessagesDirectory;
   }

   public void setLargeMessagesDirectory(final String directory)
   {
      largeMessagesDirectory = directory;
   }

   public boolean isMessageCounterEnabled()
   {
      return messageCounterEnabled;
   }

   public void setMessageCounterEnabled(final boolean enabled)
   {
      messageCounterEnabled = enabled;
   }

   public long getMessageCounterSamplePeriod()
   {
      return messageCounterSamplePeriod;
   }

   public void setMessageCounterSamplePeriod(final long period)
   {
      messageCounterSamplePeriod = period;
   }

   public int getMessageCounterMaxDayHistory()
   {
      return messageCounterMaxDayHistory;
   }

   public void setMessageCounterMaxDayHistory(final int maxDayHistory)
   {
      messageCounterMaxDayHistory = maxDayHistory;
   }

   public SimpleString getManagementAddress()
   {
      return managementAddress;
   }

   public void setManagementAddress(final SimpleString address)
   {
      managementAddress = address;
   }

   public SimpleString getManagementNotificationAddress()
   {
      return managementNotificationAddress;
   }

   public void setManagementNotificationAddress(final SimpleString address)
   {
      managementNotificationAddress = address;
   }

   public String getClusterUser()
   {
      return clusterUser;
   }

   public void setClusterUser(final String user)
   {
      clusterUser = user;
   }

   public String getClusterPassword()
   {
      return clusterPassword;
   }

   public boolean isFailoverOnServerShutdown()
   {
      return failoverOnServerShutdown;
   }

   public void setFailoverOnServerShutdown(boolean failoverOnServerShutdown)
   {
      this.failoverOnServerShutdown = failoverOnServerShutdown;
   }

   public void setClusterPassword(final String theclusterPassword)
   {
      clusterPassword = theclusterPassword;
   }

   public int getJournalCompactMinFiles()
   {
      return journalCompactMinFiles;
   }

   public int getJournalCompactPercentage()
   {
      return journalCompactPercentage;
   }

   public void setJournalCompactMinFiles(final int minFiles)
   {
      journalCompactMinFiles = minFiles;
   }

   public void setJournalCompactPercentage(final int percentage)
   {
      journalCompactPercentage = percentage;
   }

   public long getServerDumpInterval()
   {
      return serverDumpInterval;
   }

   public void setServerDumpInterval(final long intervalInMilliseconds)
   {
      serverDumpInterval = intervalInMilliseconds;
   }

   public int getMemoryWarningThreshold()
   {
      return memoryWarningThreshold;
   }

   public void setMemoryWarningThreshold(final int memoryWarningThreshold)
   {
      this.memoryWarningThreshold = memoryWarningThreshold;
   }

   public long getMemoryMeasureInterval()
   {
      return memoryMeasureInterval;
   }

   public void setMemoryMeasureInterval(final long memoryMeasureInterval)
   {
      this.memoryMeasureInterval = memoryMeasureInterval;
   }

   public int getJournalMaxIO_AIO()
   {
      return journalMaxIO_AIO;
   }

   public void setJournalMaxIO_AIO(final int journalMaxIO)
   {
      journalMaxIO_AIO = journalMaxIO;
   }

   public int getJournalBufferTimeout_AIO()
   {
      return journalBufferTimeout_AIO;
   }

   public void setJournalBufferTimeout_AIO(final int journalBufferTimeout)
   {
      journalBufferTimeout_AIO = journalBufferTimeout;
   }

   public int getJournalBufferSize_AIO()
   {
      return journalBufferSize_AIO;
   }

   public void setJournalBufferSize_AIO(final int journalBufferSize)
   {
      journalBufferSize_AIO = journalBufferSize;
   }

   public int getJournalMaxIO_NIO()
   {
      return journalMaxIO_NIO;
   }

   public void setJournalMaxIO_NIO(final int journalMaxIO)
   {
      journalMaxIO_NIO = journalMaxIO;
   }

   public int getJournalBufferTimeout_NIO()
   {
      return journalBufferTimeout_NIO;
   }

   public void setJournalBufferTimeout_NIO(final int journalBufferTimeout)
   {
      journalBufferTimeout_NIO = journalBufferTimeout;
   }

   public int getJournalBufferSize_NIO()
   {
      return journalBufferSize_NIO;
   }

   public void setJournalBufferSize_NIO(final int journalBufferSize)
   {
      journalBufferSize_NIO = journalBufferSize;
   }

   @Override
   public Map<String, AddressSettings> getAddressesSettings()
   {
      return addressesSettings;
   }

   @Override
   public void setAddressesSettings(final Map<String, AddressSettings> addressesSettings)
   {
      this.addressesSettings = addressesSettings;
   }

   @Override
   public Map<String, Set<Role>> getSecurityRoles()
   {
      return securitySettings;
   }

   @Override
   public void setSecurityRoles(final Map<String, Set<Role>> securitySettings)
   {
      this.securitySettings = securitySettings;
   }

   public List<ConnectorServiceConfiguration> getConnectorServiceConfigurations()
   {
      return this.connectorServiceConfigurations;
   }

   @Deprecated
   public long getFailbackDelay()
   {
      return haPolicy.getFailbackDelay();
   }

   @Deprecated
   public void setFailbackDelay(long failbackDelay)
   {
      this.haPolicy.setFailbackDelay(failbackDelay);
   }

   public boolean isCheckForLiveServer()
   {
      return checkForLiveServer;
   }

   public void setCheckForLiveServer(boolean checkForLiveServer)
   {
      this.checkForLiveServer = checkForLiveServer;
   }

   public void setConnectorServiceConfigurations(final List<ConnectorServiceConfiguration> configs)
   {
      this.connectorServiceConfigurations = configs;
   }

   @Override
   public String getName()
   {
      return name;
   }

   @Override
   public void setName(String name)
   {
      this.name = name;
   }

   @Deprecated
   public String getBackupGroupName()
   {
      return haPolicy.getBackupGroupName();
   }

   @Deprecated
   public void setBackupGroupName(String nodeGroupName)
   {
      haPolicy.setBackupGroupName(nodeGroupName);
   }

   @Override
   public String toString()
   {
      StringBuilder sb = new StringBuilder("HornetQ Configuration (");
      sb.append("clustered=").append(isClustered()).append(",");
      sb.append("ha-policy-type=").append(haPolicy.getPolicyType()).append(",");
      sb.append("journalDirectory=").append(journalDirectory).append(",");
      sb.append("bindingsDirectory=").append(bindingsDirectory).append(",");
      sb.append("largeMessagesDirectory=").append(largeMessagesDirectory).append(",");
      sb.append("pagingDirectory=").append(pagingDirectory);
      sb.append(")");
      return sb.toString();
   }

   public boolean isMaskPassword()
   {
      return maskPassword;
   }

   public void setMaskPassword(boolean maskPassword)
   {
      this.maskPassword = maskPassword;
   }

   public void setPasswordCodec(String codec)
   {
      passwordCodec = codec;
   }

   public String getPasswordCodec()
   {
      return passwordCodec;
   }

   @Override
   @Deprecated
   public void setReplicationClustername(String clusterName)
   {
      this.haPolicy.setReplicationClustername(clusterName);
   }

   @Override
   @Deprecated
   public String getReplicationClustername()
   {
      return haPolicy.getReplicationClustername();
   }

   @Override
   @Deprecated
   public void setScaleDownClustername(String clusterName)
   {
      this.haPolicy.setScaleDownClustername(clusterName);
   }

   @Override
   @Deprecated
   public String getScaleDownClustername()
   {
      return haPolicy.getScaleDownClustername();
   }

   @Override
   @Deprecated
   public void setMaxSavedReplicatedJournalSize(int maxSavedReplicatedJournalsSize)
   {
      this.haPolicy.setMaxSavedReplicatedJournalSize(maxSavedReplicatedJournalsSize);
   }

   @Override
   @Deprecated
   public int getMaxSavedReplicatedJournalsSize()
   {
      return haPolicy.getMaxSavedReplicatedJournalsSize();
   }

   @Override
   public void setResolveProtocols(boolean resolveProtocols)
   {
      this.resolveProtocols = resolveProtocols;
   }

   @Override
   public boolean isResolveProtocols()
   {
      return resolveProtocols;
   }

   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((acceptorConfigs == null) ? 0 : acceptorConfigs.hashCode());
      result = prime * result + ((addressesSettings == null) ? 0 : addressesSettings.hashCode());
      result = prime * result + (haPolicy.isAllowAutoFailBack() ? 1231 : 1237);
      result = prime * result + (asyncConnectionExecutionEnabled ? 1231 : 1237);
      result = prime * result + (haPolicy.isBackup() ? 1231 : 1237);
      result = prime * result + ((bindingsDirectory == null) ? 0 : bindingsDirectory.hashCode());
      result = prime * result + ((bridgeConfigurations == null) ? 0 : bridgeConfigurations.hashCode());
      result = prime * result + ((broadcastGroupConfigurations == null) ? 0 : broadcastGroupConfigurations.hashCode());
      result = prime * result + (checkForLiveServer ? 1231 : 1237);
      result = prime * result + ((clusterConfigurations == null) ? 0 : clusterConfigurations.hashCode());
      result = prime * result + ((clusterPassword == null) ? 0 : clusterPassword.hashCode());
      result = prime * result + ((clusterUser == null) ? 0 : clusterUser.hashCode());
      result = prime * result + (int)(connectionTTLOverride ^ (connectionTTLOverride >>> 32));
      result = prime * result + ((connectorConfigs == null) ? 0 : connectorConfigs.hashCode());
      result =
               prime * result +
                        ((connectorServiceConfigurations == null) ? 0 : connectorServiceConfigurations.hashCode());
      result = prime * result + (createBindingsDir ? 1231 : 1237);
      result = prime * result + (createJournalDir ? 1231 : 1237);
      result = prime * result + ((discoveryGroupConfigurations == null) ? 0 : discoveryGroupConfigurations.hashCode());
      result = prime * result + ((divertConfigurations == null) ? 0 : divertConfigurations.hashCode());
      result = prime * result + (int)(haPolicy.getFailbackDelay() ^ (haPolicy.getFailbackDelay() >>> 32));
      result = prime * result + (failoverOnServerShutdown ? 1231 : 1237);
      result = prime * result + (fileDeploymentEnabled ? 1231 : 1237);
      result = prime * result + (int)(fileDeploymentScanPeriod ^ (fileDeploymentScanPeriod >>> 32));
      result = prime * result + ((groupingHandlerConfiguration == null) ? 0 : groupingHandlerConfiguration.hashCode());
      result = prime * result + idCacheSize;
      result =
               prime * result +
                        ((incomingInterceptorClassNames == null) ? 0 : incomingInterceptorClassNames.hashCode());
      result = prime * result + ((jmxDomain == null) ? 0 : jmxDomain.hashCode());
      result = prime * result + (jmxManagementEnabled ? 1231 : 1237);
      result = prime * result + journalBufferSize_AIO;
      result = prime * result + journalBufferSize_NIO;
      result = prime * result + journalBufferTimeout_AIO;
      result = prime * result + journalBufferTimeout_NIO;
      result = prime * result + journalCompactMinFiles;
      result = prime * result + journalCompactPercentage;
      result = prime * result + ((journalDirectory == null) ? 0 : journalDirectory.hashCode());
      result = prime * result + journalFileSize;
      result = prime * result + journalMaxIO_AIO;
      result = prime * result + journalMaxIO_NIO;
      result = prime * result + journalMinFiles;
      result = prime * result + journalPerfBlastPages;
      result = prime * result + (journalSyncNonTransactional ? 1231 : 1237);
      result = prime * result + (journalSyncTransactional ? 1231 : 1237);
      result = prime * result + ((journalType == null) ? 0 : journalType.hashCode());
      result = prime * result + ((largeMessagesDirectory == null) ? 0 : largeMessagesDirectory.hashCode());
      result = prime * result + (logJournalWriteRate ? 1231 : 1237);
      result = prime * result + ((managementAddress == null) ? 0 : managementAddress.hashCode());
      result =
               prime * result +
                        ((managementNotificationAddress == null) ? 0 : managementNotificationAddress.hashCode());
      result = prime * result + (maskPassword ? 1231 : 1237);
      result = prime * result + maxConcurrentPageIO;
      result = prime * result + (int)(memoryMeasureInterval ^ (memoryMeasureInterval >>> 32));
      result = prime * result + memoryWarningThreshold;
      result = prime * result + (messageCounterEnabled ? 1231 : 1237);
      result = prime * result + messageCounterMaxDayHistory;
      result = prime * result + (int)(messageCounterSamplePeriod ^ (messageCounterSamplePeriod >>> 32));
      result = prime * result + (int)(messageExpiryScanPeriod ^ (messageExpiryScanPeriod >>> 32));
      result = prime * result + messageExpiryThreadPriority;
      result = prime * result + ((name == null) ? 0 : name.hashCode());
      result = prime * result + ((haPolicy.getBackupGroupName() == null) ? 0 : haPolicy.getBackupGroupName().hashCode());
      result =
               prime * result +
                        ((outgoingInterceptorClassNames == null) ? 0 : outgoingInterceptorClassNames.hashCode());
      result = prime * result + ((pagingDirectory == null) ? 0 : pagingDirectory.hashCode());
      result = prime * result + (persistDeliveryCountBeforeDelivery ? 1231 : 1237);
      result = prime * result + (persistIDCache ? 1231 : 1237);
      result = prime * result + (persistenceEnabled ? 1231 : 1237);
      result = prime * result + ((queueConfigurations == null) ? 0 : queueConfigurations.hashCode());
      result = prime * result + ((haPolicy.getReplicationClustername() == null) ? 0 : haPolicy.getReplicationClustername().hashCode());
      result = prime * result + ((haPolicy.getScaleDownClustername() == null) ? 0 : haPolicy.getScaleDownClustername().hashCode());
      result = prime * result + (runSyncSpeedTest ? 1231 : 1237);
      result = prime * result + scheduledThreadPoolMaxSize;
      result = prime * result + (securityEnabled ? 1231 : 1237);
      result = prime * result + (int)(securityInvalidationInterval ^ (securityInvalidationInterval >>> 32));
      result = prime * result + ((securitySettings == null) ? 0 : securitySettings.hashCode());
      result = prime * result + (int)(serverDumpInterval ^ (serverDumpInterval >>> 32));
      result = prime * result + (haPolicy.isSharedStore() ? 1231 : 1237);
      result = prime * result + threadPoolMaxSize;
      result = prime * result + (int)(transactionTimeout ^ (transactionTimeout >>> 32));
      result = prime * result + (int)(transactionTimeoutScanPeriod ^ (transactionTimeoutScanPeriod >>> 32));
      result = prime * result + (wildcardRoutingEnabled ? 1231 : 1237);
      result = prime * result + (resolveProtocols ? 1231 : 1237);
      result = prime * result + (int) (journalLockAcquisitionTimeout ^ (journalLockAcquisitionTimeout >>> 32));
      return result;
   }

   @Override
   public boolean equals(Object obj)
   {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (!(obj instanceof ConfigurationImpl))
         return false;
      ConfigurationImpl other = (ConfigurationImpl)obj;
      if (acceptorConfigs == null)
      {
         if (other.acceptorConfigs != null)
            return false;
      }
      else if (!acceptorConfigs.equals(other.acceptorConfigs))
         return false;
      if (addressesSettings == null)
      {
         if (other.addressesSettings != null)
            return false;
      }
      else if (!addressesSettings.equals(other.addressesSettings))
         return false;
      if (haPolicy.isAllowAutoFailBack() != other.getHAPolicy().isAllowAutoFailBack())
         return false;
      if (asyncConnectionExecutionEnabled != other.asyncConnectionExecutionEnabled)
         return false;
      if (haPolicy.isBackup() != other.getHAPolicy().isBackup())
         return false;
      if (bindingsDirectory == null)
      {
         if (other.bindingsDirectory != null)
            return false;
      }
      else if (!bindingsDirectory.equals(other.bindingsDirectory))
         return false;
      if (bridgeConfigurations == null)
      {
         if (other.bridgeConfigurations != null)
            return false;
      }
      else if (!bridgeConfigurations.equals(other.bridgeConfigurations))
         return false;
      if (broadcastGroupConfigurations == null)
      {
         if (other.broadcastGroupConfigurations != null)
            return false;
      }
      else if (!broadcastGroupConfigurations.equals(other.broadcastGroupConfigurations))
         return false;
      if (checkForLiveServer != other.checkForLiveServer)
         return false;
      if (clusterConfigurations == null)
      {
         if (other.clusterConfigurations != null)
            return false;
      }
      else if (!clusterConfigurations.equals(other.clusterConfigurations))
         return false;
      if (clusterPassword == null)
      {
         if (other.clusterPassword != null)
            return false;
      }
      else if (!clusterPassword.equals(other.clusterPassword))
         return false;
      if (clusterUser == null)
      {
         if (other.clusterUser != null)
            return false;
      }
      else if (!clusterUser.equals(other.clusterUser))
         return false;
      if (connectionTTLOverride != other.connectionTTLOverride)
         return false;
      if (connectorConfigs == null)
      {
         if (other.connectorConfigs != null)
            return false;
      }
      else if (!connectorConfigs.equals(other.connectorConfigs))
         return false;
      if (connectorServiceConfigurations == null)
      {
         if (other.connectorServiceConfigurations != null)
            return false;
      }
      else if (!connectorServiceConfigurations.equals(other.connectorServiceConfigurations))
         return false;
      if (createBindingsDir != other.createBindingsDir)
         return false;
      if (createJournalDir != other.createJournalDir)
         return false;
      if (discoveryGroupConfigurations == null)
      {
         if (other.discoveryGroupConfigurations != null)
            return false;
      }
      else if (!discoveryGroupConfigurations.equals(other.discoveryGroupConfigurations))
         return false;
      if (divertConfigurations == null)
      {
         if (other.divertConfigurations != null)
            return false;
      }
      else if (!divertConfigurations.equals(other.divertConfigurations))
         return false;
      if (haPolicy.getFailbackDelay() != other.getHAPolicy().getFailbackDelay())
         return false;
      if (failoverOnServerShutdown != other.failoverOnServerShutdown)
         return false;
      if (fileDeploymentEnabled != other.fileDeploymentEnabled)
         return false;
      if (fileDeploymentScanPeriod != other.fileDeploymentScanPeriod)
         return false;
      if (groupingHandlerConfiguration == null)
      {
         if (other.groupingHandlerConfiguration != null)
            return false;
      }
      else if (!groupingHandlerConfiguration.equals(other.groupingHandlerConfiguration))
         return false;
      if (idCacheSize != other.idCacheSize)
         return false;
      if (incomingInterceptorClassNames == null)
      {
         if (other.incomingInterceptorClassNames != null)
            return false;
      }
      else if (!incomingInterceptorClassNames.equals(other.incomingInterceptorClassNames))
         return false;
      if (jmxDomain == null)
      {
         if (other.jmxDomain != null)
            return false;
      }
      else if (!jmxDomain.equals(other.jmxDomain))
         return false;
      if (jmxManagementEnabled != other.jmxManagementEnabled)
         return false;
      if (journalBufferSize_AIO != other.journalBufferSize_AIO)
         return false;
      if (journalBufferSize_NIO != other.journalBufferSize_NIO)
         return false;
      if (journalBufferTimeout_AIO != other.journalBufferTimeout_AIO)
         return false;
      if (journalBufferTimeout_NIO != other.journalBufferTimeout_NIO)
         return false;
      if (journalCompactMinFiles != other.journalCompactMinFiles)
         return false;
      if (journalCompactPercentage != other.journalCompactPercentage)
         return false;
      if (journalDirectory == null)
      {
         if (other.journalDirectory != null)
            return false;
      }
      else if (!journalDirectory.equals(other.journalDirectory))
         return false;
      if (journalFileSize != other.journalFileSize)
         return false;
      if (journalMaxIO_AIO != other.journalMaxIO_AIO)
         return false;
      if (journalMaxIO_NIO != other.journalMaxIO_NIO)
         return false;
      if (journalMinFiles != other.journalMinFiles)
         return false;
      if (journalPerfBlastPages != other.journalPerfBlastPages)
         return false;
      if (journalSyncNonTransactional != other.journalSyncNonTransactional)
         return false;
      if (journalSyncTransactional != other.journalSyncTransactional)
         return false;
      if (journalType != other.journalType)
         return false;
      if (largeMessagesDirectory == null)
      {
         if (other.largeMessagesDirectory != null)
            return false;
      }
      else if (!largeMessagesDirectory.equals(other.largeMessagesDirectory))
         return false;
      if (logJournalWriteRate != other.logJournalWriteRate)
         return false;
      if (managementAddress == null)
      {
         if (other.managementAddress != null)
            return false;
      }
      else if (!managementAddress.equals(other.managementAddress))
         return false;
      if (managementNotificationAddress == null)
      {
         if (other.managementNotificationAddress != null)
            return false;
      }
      else if (!managementNotificationAddress.equals(other.managementNotificationAddress))
         return false;
      if (maskPassword != other.maskPassword)
         return false;
      if (maxConcurrentPageIO != other.maxConcurrentPageIO)
         return false;
      if (memoryMeasureInterval != other.memoryMeasureInterval)
         return false;
      if (memoryWarningThreshold != other.memoryWarningThreshold)
         return false;
      if (messageCounterEnabled != other.messageCounterEnabled)
         return false;
      if (messageCounterMaxDayHistory != other.messageCounterMaxDayHistory)
         return false;
      if (messageCounterSamplePeriod != other.messageCounterSamplePeriod)
         return false;
      if (messageExpiryScanPeriod != other.messageExpiryScanPeriod)
         return false;
      if (messageExpiryThreadPriority != other.messageExpiryThreadPriority)
         return false;
      if (name == null)
      {
         if (other.name != null)
            return false;
      }
      else if (!name.equals(other.name))
         return false;
      if (haPolicy.getBackupGroupName() == null)
      {
         if (other.getHAPolicy().getBackupGroupName() != null)
            return false;
      }
      else if (!haPolicy.getBackupGroupName().equals(other.getHAPolicy().getBackupGroupName()))
         return false;
      if (outgoingInterceptorClassNames == null)
      {
         if (other.outgoingInterceptorClassNames != null)
            return false;
      }
      else if (!outgoingInterceptorClassNames.equals(other.outgoingInterceptorClassNames))
         return false;
      if (pagingDirectory == null)
      {
         if (other.pagingDirectory != null)
            return false;
      }
      else if (!pagingDirectory.equals(other.pagingDirectory))
         return false;
      if (persistDeliveryCountBeforeDelivery != other.persistDeliveryCountBeforeDelivery)
         return false;
      if (persistIDCache != other.persistIDCache)
         return false;
      if (persistenceEnabled != other.persistenceEnabled)
         return false;
      if (queueConfigurations == null)
      {
         if (other.queueConfigurations != null)
            return false;
      }
      else if (!queueConfigurations.equals(other.queueConfigurations))
         return false;
      if (haPolicy.getReplicationClustername() == null)
      {
         if (other.getHAPolicy().getReplicationClustername() != null)
            return false;
      }
      else if (!haPolicy.getReplicationClustername().equals(other.getHAPolicy().getReplicationClustername()))
         return false;
      if (haPolicy.getScaleDownClustername() == null)
      {
         if (other.getHAPolicy().getScaleDownClustername() != null)
            return false;
      }
      else if (!haPolicy.getScaleDownClustername().equals(other.getHAPolicy().getScaleDownClustername()))
         return false;
      if (runSyncSpeedTest != other.runSyncSpeedTest)
         return false;
      if (scheduledThreadPoolMaxSize != other.scheduledThreadPoolMaxSize)
         return false;
      if (securityEnabled != other.securityEnabled)
         return false;
      if (securityInvalidationInterval != other.securityInvalidationInterval)
         return false;
      if (securitySettings == null)
      {
         if (other.securitySettings != null)
            return false;
      }
      else if (!securitySettings.equals(other.securitySettings))
         return false;
      if (serverDumpInterval != other.serverDumpInterval)
         return false;
      if (haPolicy.isSharedStore() != other.getHAPolicy().isSharedStore())
         return false;
      if (threadPoolMaxSize != other.threadPoolMaxSize)
         return false;
      if (transactionTimeout != other.transactionTimeout)
         return false;
      if (transactionTimeoutScanPeriod != other.transactionTimeoutScanPeriod)
         return false;
      if (wildcardRoutingEnabled != other.wildcardRoutingEnabled)
         return false;
      if (resolveProtocols != other.resolveProtocols)
         return false;
      if (journalLockAcquisitionTimeout != other.journalLockAcquisitionTimeout)
         return false;
      return true;
   }

   public Configuration copy() throws Exception
   {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      ObjectOutputStream os = new ObjectOutputStream(bos);
      os.writeObject(this);
      ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()));
      return (Configuration) ois.readObject();
   }

   @Override
   public Set<Configuration> getBackupServerConfigurations()
   {
      return backupServerConfigurations;
   }

   @Override
   public void setBackupStrategy(BackupStrategy backupStrategy)
   {
      this.backupStrategy = backupStrategy;
   }

   public BackupStrategy getBackupStrategy()
   {
      return backupStrategy;
   }

   @Override
   public void setJournalLockAcquisitionTimeout(long journalLockAcquisitionTimeout)
   {
      this.journalLockAcquisitionTimeout = journalLockAcquisitionTimeout;
   }

   @Override
   public long getJournalLockAcquisitionTimeout()
   {
      return journalLockAcquisitionTimeout;
   }

   @Override
   public HAPolicy getHAPolicy()
   {
      return haPolicy;
   }

   @Override
   public void setHAPolicy(HAPolicy haPolicy)
   {
      this.haPolicy = haPolicy;
   }
}
