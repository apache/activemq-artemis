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
package org.apache.activemq.artemis.core.server.impl;

import javax.management.MBeanServer;
import javax.security.cert.X509Certificate;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.net.URL;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.ActiveMQDeleteAddressException;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.client.impl.ClientSessionFactoryImpl;
import org.apache.activemq.artemis.core.config.BridgeConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.ConfigurationUtils;
import org.apache.activemq.artemis.core.config.CoreAddressConfiguration;
import org.apache.activemq.artemis.core.config.CoreQueueConfiguration;
import org.apache.activemq.artemis.core.config.DivertConfiguration;
import org.apache.activemq.artemis.core.config.StoreConfiguration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.deployers.impl.FileConfigurationParser;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.filter.impl.FilterImpl;
import org.apache.activemq.artemis.core.io.IOCriticalErrorListener;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.io.aio.AIOSequentialFileFactory;
import org.apache.activemq.artemis.core.journal.JournalLoadInformation;
import org.apache.activemq.artemis.core.management.impl.ActiveMQServerControlImpl;
import org.apache.activemq.artemis.core.paging.PagingManager;
import org.apache.activemq.artemis.core.paging.cursor.PageSubscription;
import org.apache.activemq.artemis.core.paging.impl.PagingManagerImpl;
import org.apache.activemq.artemis.core.paging.impl.PagingStoreFactoryNIO;
import org.apache.activemq.artemis.core.persistence.AddressBindingInfo;
import org.apache.activemq.artemis.core.persistence.GroupingInfo;
import org.apache.activemq.artemis.core.persistence.OperationContext;
import org.apache.activemq.artemis.core.persistence.QueueBindingInfo;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.persistence.config.PersistedAddressSetting;
import org.apache.activemq.artemis.core.persistence.config.PersistedRoles;
import org.apache.activemq.artemis.core.persistence.impl.PageCountPending;
import org.apache.activemq.artemis.core.persistence.impl.journal.JDBCJournalStorageManager;
import org.apache.activemq.artemis.core.persistence.impl.journal.JournalStorageManager;
import org.apache.activemq.artemis.core.persistence.impl.journal.OperationContextImpl;
import org.apache.activemq.artemis.core.persistence.impl.nullpm.NullStorageManager;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.BindingType;
import org.apache.activemq.artemis.core.postoffice.Bindings;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.postoffice.QueueBinding;
import org.apache.activemq.artemis.core.postoffice.impl.DivertBinding;
import org.apache.activemq.artemis.core.postoffice.impl.LocalQueueBinding;
import org.apache.activemq.artemis.core.postoffice.impl.PostOfficeImpl;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnection;
import org.apache.activemq.artemis.core.remoting.server.RemotingService;
import org.apache.activemq.artemis.core.remoting.server.impl.RemotingServiceImpl;
import org.apache.activemq.artemis.core.replication.ReplicationEndpoint;
import org.apache.activemq.artemis.core.replication.ReplicationManager;
import org.apache.activemq.artemis.core.security.CheckType;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.security.SecurityAuth;
import org.apache.activemq.artemis.core.security.SecurityStore;
import org.apache.activemq.artemis.core.security.impl.SecurityStoreImpl;
import org.apache.activemq.artemis.core.server.ActivateCallback;
import org.apache.activemq.artemis.core.server.ActivationFailureListener;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.core.server.ActiveMQMessageBundle;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.AddressQueryResult;
import org.apache.activemq.artemis.core.server.Bindable;
import org.apache.activemq.artemis.core.server.BindingQueryResult;
import org.apache.activemq.artemis.core.server.Divert;
import org.apache.activemq.artemis.core.server.JournalType;
import org.apache.activemq.artemis.core.server.LargeServerMessage;
import org.apache.activemq.artemis.core.server.MemoryManager;
import org.apache.activemq.artemis.core.server.NetworkHealthCheck;
import org.apache.activemq.artemis.core.server.NodeManager;
import org.apache.activemq.artemis.core.server.PostQueueCreationCallback;
import org.apache.activemq.artemis.core.server.PostQueueDeletionCallback;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.QueueConfig;
import org.apache.activemq.artemis.core.server.QueueFactory;
import org.apache.activemq.artemis.core.server.QueueQueryResult;
import org.apache.activemq.artemis.core.server.RoutingType;
import org.apache.activemq.artemis.core.server.SecuritySettingPlugin;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.core.server.ServiceRegistry;
import org.apache.activemq.artemis.core.server.cluster.BackupManager;
import org.apache.activemq.artemis.core.server.cluster.ClusterManager;
import org.apache.activemq.artemis.core.server.cluster.Transformer;
import org.apache.activemq.artemis.core.server.cluster.ha.HAPolicy;
import org.apache.activemq.artemis.core.server.files.FileMoveManager;
import org.apache.activemq.artemis.core.server.files.FileStoreMonitor;
import org.apache.activemq.artemis.core.server.group.GroupingHandler;
import org.apache.activemq.artemis.core.server.group.impl.GroupingHandlerConfiguration;
import org.apache.activemq.artemis.core.server.group.impl.LocalGroupingHandler;
import org.apache.activemq.artemis.core.server.group.impl.RemoteGroupingHandler;
import org.apache.activemq.artemis.core.server.management.ManagementService;
import org.apache.activemq.artemis.core.server.management.impl.ManagementServiceImpl;
import org.apache.activemq.artemis.core.server.reload.ReloadCallback;
import org.apache.activemq.artemis.core.server.reload.ReloadManager;
import org.apache.activemq.artemis.core.server.reload.ReloadManagerImpl;
import org.apache.activemq.artemis.core.settings.HierarchicalRepository;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.core.settings.impl.HierarchicalObjectRepository;
import org.apache.activemq.artemis.core.settings.impl.ResourceLimitSettings;
import org.apache.activemq.artemis.core.transaction.ResourceManager;
import org.apache.activemq.artemis.core.transaction.impl.ResourceManagerImpl;
import org.apache.activemq.artemis.core.version.Version;
import org.apache.activemq.artemis.jlibaio.LibaioContext;
import org.apache.activemq.artemis.spi.core.protocol.ProtocolManagerFactory;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.protocol.SessionCallback;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager;
import org.apache.activemq.artemis.utils.ActiveMQThreadFactory;
import org.apache.activemq.artemis.utils.ActiveMQThreadPoolExecutor;
import org.apache.activemq.artemis.utils.CertificateUtil;
import org.apache.activemq.artemis.utils.ConcurrentHashSet;
import org.apache.activemq.artemis.utils.ExecutorFactory;
import org.apache.activemq.artemis.utils.OrderedExecutorFactory;
import org.apache.activemq.artemis.utils.ReusableLatch;
import org.apache.activemq.artemis.utils.SecurityFormatter;
import org.apache.activemq.artemis.utils.TimeUtils;
import org.apache.activemq.artemis.utils.VersionLoader;
import org.jboss.logging.Logger;

/**
 * The ActiveMQ Artemis server implementation
 */
public class ActiveMQServerImpl implements ActiveMQServer {

   private static final Logger logger = Logger.getLogger(ActiveMQServerImpl.class);

   public static final String INTERNAL_NAMING_PREFIX = "$.artemis.internal";

   /**
    * JMS Topics (which are outside of the scope of the core API) will require a dumb subscription
    * with a dummy-filter at this current version as a way to keep its existence valid and TCK
    * tests. That subscription needs an invalid filter, however paging needs to ignore any
    * subscription with this filter. For that reason, this filter needs to be rejected on paging or
    * any other component on the system, and just be ignored for any purpose It's declared here as
    * this filter is considered a global ignore
    *
    * @deprecated Replaced by {@link org.apache.activemq.artemis.core.filter.Filter#GENERIC_IGNORED_FILTER}
    */
   @Deprecated
   public static final String GENERIC_IGNORED_FILTER = Filter.GENERIC_IGNORED_FILTER;

   private HAPolicy haPolicy;

   enum SERVER_STATE {
      /**
       * start() has been called but components are not initialized. The whole point of this state,
       * is to be in a state which is different from {@link SERVER_STATE#STARTED} and
       * {@link SERVER_STATE#STOPPED}, so that methods testing for these two values such as
       * {@link #stop(boolean)} worked as intended.
       */
      STARTING, /**
       * server is started. {@code server.isStarted()} returns {@code true}, and all assumptions
       * about it hold.
       */
      STARTED, /**
       * stop() was called but has not finished yet. Meant to avoids starting components while
       * stop() is executing.
       */
      STOPPING, /**
       * Stopped: either stop() has been called and has finished running, or start() has never been
       * called.
       */
      STOPPED
   }

   private volatile SERVER_STATE state = SERVER_STATE.STOPPED;

   private final Version version;

   private final ActiveMQSecurityManager securityManager;

   private final Configuration configuration;

   private MBeanServer mbeanServer;

   private volatile SecurityStore securityStore;

   private final HierarchicalRepository<AddressSettings> addressSettingsRepository;

   private volatile QueueFactory queueFactory;

   private volatile PagingManager pagingManager;

   private volatile PostOffice postOffice;

   private volatile ExecutorService threadPool;

   private volatile ScheduledExecutorService scheduledPool;

   private volatile ExecutorFactory executorFactory;

   private volatile ExecutorService ioExecutorPool;

   /**
    * This is a thread pool for io tasks only.
    * We can't use the same global executor to avoid starvations.
    */
   private volatile ExecutorFactory ioExecutorFactory;

   private final NetworkHealthCheck networkHealthCheck = new NetworkHealthCheck(ActiveMQDefaultConfiguration.getDefaultNetworkCheckNic(), ActiveMQDefaultConfiguration.getDefaultNetworkCheckPeriod(), ActiveMQDefaultConfiguration.getDefaultNetworkCheckTimeout());

   private final HierarchicalRepository<Set<Role>> securityRepository;

   private volatile ResourceManager resourceManager;

   private volatile ActiveMQServerControlImpl messagingServerControl;

   private volatile ClusterManager clusterManager;

   private volatile BackupManager backupManager;

   private volatile StorageManager storageManager;

   private volatile RemotingService remotingService;

   private final List<ProtocolManagerFactory> protocolManagerFactories = new ArrayList<>();

   private volatile ManagementService managementService;

   private volatile ConnectorsService connectorsService;

   private MemoryManager memoryManager;

   private ReloadManager reloadManager;

   private FileStoreMonitor fileStoreMonitor;

   private final Map<String, ServerSession> sessions = new ConcurrentHashMap<>();

   private final Semaphore activationLock = new Semaphore(1);
   /**
    * This class here has the same principle of CountDownLatch but you can reuse the counters.
    * It's based on the same super classes of {@code CountDownLatch}
    */
   private final ReusableLatch activationLatch = new ReusableLatch(0);

   private final Set<ActivateCallback> activateCallbacks = new ConcurrentHashSet<>();

   private final Set<ActivationFailureListener> activationFailureListeners = new ConcurrentHashSet<>();

   private final Set<PostQueueCreationCallback> postQueueCreationCallbacks = new ConcurrentHashSet<>();

   private final Set<PostQueueDeletionCallback> postQueueDeletionCallbacks = new ConcurrentHashSet<>();

   private volatile GroupingHandler groupingHandler;

   private NodeManager nodeManager;

   // Used to identify the server on tests... useful on debugging testcases
   private String identity;

   private Thread backupActivationThread;

   private Activation activation;

   private final Map<String, Object> activationParams = new HashMap<>();

   private final ShutdownOnCriticalErrorListener shutdownOnCriticalIO = new ShutdownOnCriticalErrorListener();

   private final ActiveMQServer parentServer;

   //todo think about moving this to the activation
   private final List<SimpleString> scaledDownNodeIDs = new ArrayList<>();

   private boolean threadPoolSupplied = false;

   private boolean scheduledPoolSupplied = false;

   private final ServiceRegistry serviceRegistry;

   private Date startDate;

   private final List<ActiveMQComponent> externalComponents = new ArrayList<>();

   private final ConcurrentMap<String, AtomicInteger> connectedClientIds = new ConcurrentHashMap();

   private final ActiveMQComponent networkCheckMonitor = new ActiveMQComponent() {
      @Override
      public void start() throws Exception {
         internalStart();
      }

      @Override
      public void stop() throws Exception {
         internalStop();
      }

      @Override
      public String toString() {
         return ActiveMQServerImpl.this.toString();
      }

      @Override
      public boolean isStarted() {
         return ActiveMQServerImpl.this.isStarted();
      }
   };

   // Constructors
   // ---------------------------------------------------------------------------------

   public ActiveMQServerImpl() {
      this(null, null, null);
   }

   public ActiveMQServerImpl(final Configuration configuration) {
      this(configuration, null, null);
   }

   public ActiveMQServerImpl(final Configuration configuration, ActiveMQServer parentServer) {
      this(configuration, null, null, parentServer);
   }

   public ActiveMQServerImpl(final Configuration configuration, final MBeanServer mbeanServer) {
      this(configuration, mbeanServer, null);
   }

   public ActiveMQServerImpl(final Configuration configuration, final ActiveMQSecurityManager securityManager) {
      this(configuration, null, securityManager);
   }

   public ActiveMQServerImpl(Configuration configuration,
                             MBeanServer mbeanServer,
                             final ActiveMQSecurityManager securityManager) {
      this(configuration, mbeanServer, securityManager, null);
   }

   public ActiveMQServerImpl(Configuration configuration,
                             MBeanServer mbeanServer,
                             final ActiveMQSecurityManager securityManager,
                             final ActiveMQServer parentServer) {
      this(configuration, mbeanServer, securityManager, parentServer, null);
   }

   public ActiveMQServerImpl(Configuration configuration,
                             MBeanServer mbeanServer,
                             final ActiveMQSecurityManager securityManager,
                             final ActiveMQServer parentServer,
                             final ServiceRegistry serviceRegistry) {
      if (configuration == null) {
         configuration = new ConfigurationImpl();
      } else {
         ConfigurationUtils.validateConfiguration(configuration);
      }

      if (mbeanServer == null) {
         // Just use JVM mbean server
         mbeanServer = ManagementFactory.getPlatformMBeanServer();
      }

      // We need to hard code the version information into a source file

      version = VersionLoader.getVersion();

      this.configuration = configuration;

      this.mbeanServer = mbeanServer;

      this.securityManager = securityManager;

      addressSettingsRepository = new HierarchicalObjectRepository<>();

      addressSettingsRepository.setDefault(new AddressSettings());

      securityRepository = new HierarchicalObjectRepository<>();

      securityRepository.setDefault(new HashSet<Role>());

      this.parentServer = parentServer;

      this.serviceRegistry = serviceRegistry == null ? new ServiceRegistryImpl() : serviceRegistry;
   }

   @Override
   public ReloadManager getReloadManager() {
      return reloadManager;
   }

   @Override
   public NetworkHealthCheck getNetworkHealthCheck() {
      return networkHealthCheck;
   }

   // life-cycle methods
   // ----------------------------------------------------------------

   /*
    * Can be overridden for tests
    */
   protected NodeManager createNodeManager(final File directory, boolean replicatingBackup) {
      NodeManager manager;
      if (!configuration.isPersistenceEnabled()) {
         manager = new InVMNodeManager(replicatingBackup);
      } else if (configuration.getJournalType() == JournalType.ASYNCIO && LibaioContext.isLoaded()) {
         manager = new AIOFileLockNodeManager(directory, replicatingBackup, configuration.getJournalLockAcquisitionTimeout());
      } else {
         manager = new FileLockNodeManager(directory, replicatingBackup, configuration.getJournalLockAcquisitionTimeout());
      }
      return manager;
   }

   @Override
   public OperationContext newOperationContext() {
      return getStorageManager().newContext(getExecutorFactory().getExecutor());
   }

   @Override
   public final synchronized void start() throws Exception {
      SERVER_STATE originalState = state;
      try {
         internalStart();
      } finally {
         if (originalState == SERVER_STATE.STOPPED) {
            networkHealthCheck.setTimeUnit(TimeUnit.MILLISECONDS).setPeriod(configuration.getNetworkCheckPeriod()).
               setNetworkTimeout(configuration.getNetworkCheckTimeout()).
               parseAddressList(configuration.getNetworkCheckList()).
               parseURIList(configuration.getNetworkCheckURLList()).
               setNICName(configuration.getNetworkCheckNIC()).
               setIpv4Command(configuration.getNetworkCheckPingCommand()).
               setIpv6Command(configuration.getNetworkCheckPing6Command());

            networkHealthCheck.addComponent(networkCheckMonitor);
         }
      }
   }

   private void internalStart() throws Exception {
      if (state != SERVER_STATE.STOPPED) {
         logger.debug("Server already started!");
         return;
      }

      configuration.parseSystemProperties();

      startDate = new Date();

      state = SERVER_STATE.STARTING;

      if (haPolicy == null) {
         haPolicy = ConfigurationUtils.getHAPolicy(configuration.getHAPolicyConfiguration(), this);
      }

      activationLatch.setCount(1);

      logger.debug("Starting server " + this);

      OperationContextImpl.clearContext();

      try {
         checkJournalDirectory();

         nodeManager = createNodeManager(configuration.getJournalLocation(), false);

         nodeManager.start();

         ActiveMQServerLogger.LOGGER.serverStarting((haPolicy.isBackup() ? "backup" : "live"), configuration);

         final boolean wasLive = !haPolicy.isBackup();
         if (!haPolicy.isBackup()) {
            activation = haPolicy.createActivation(this, false, activationParams, shutdownOnCriticalIO);

            activation.run();
         }
         // The activation on fail-back may change the value of isBackup, for that reason we are
         // checking again here
         if (haPolicy.isBackup()) {
            if (haPolicy.isSharedStore()) {
               activation = haPolicy.createActivation(this, false, activationParams, shutdownOnCriticalIO);
            } else {
               activation = haPolicy.createActivation(this, wasLive, activationParams, shutdownOnCriticalIO);
            }

            if (logger.isTraceEnabled()) {
               logger.trace("starting backupActivation");
            }
            backupActivationThread = new ActivationThread(activation, ActiveMQMessageBundle.BUNDLE.activationForServer(this));
            backupActivationThread.start();
         } else {
            ActiveMQServerLogger.LOGGER.serverStarted(getVersion().getFullVersion(), configuration.getName(), nodeManager.getNodeId(), identity != null ? identity : "");
         }
         // start connector service
         connectorsService = new ConnectorsService(configuration, storageManager, scheduledPool, postOffice, serviceRegistry);
         connectorsService.start();
      } finally {
         // this avoids embedded applications using dirty contexts from startup
         OperationContextImpl.clearContext();
      }
   }

   @Override
   public ReplicationEndpoint getReplicationEndpoint() {
      if (activation instanceof SharedNothingBackupActivation) {
         return ((SharedNothingBackupActivation) activation).getReplicationEndpoint();
      }
      return null;
   }

   @Override
   public void unlockActivation() {
      activationLock.release();
   }

   @Override
   public void lockActivation() {
      try {
         activationLock.acquire();
      } catch (Exception e) {
         logger.warn(e.getMessage(), e);
      }
   }

   @Override
   protected final void finalize() throws Throwable {
      if (state != SERVER_STATE.STOPPED) {
         ActiveMQServerLogger.LOGGER.serverFinalisedWIthoutBeingSTopped();

         stop();
      }

      super.finalize();
   }

   public void setState(SERVER_STATE state) {
      this.state = state;
   }

   public SERVER_STATE getState() {
      return state;
   }

   public void interrupBackupThread(NodeManager nodeManagerInUse) throws InterruptedException {
      long timeout = 30000;

      long start = System.currentTimeMillis();

      while (backupActivationThread.isAlive() && System.currentTimeMillis() - start < timeout) {
         if (nodeManagerInUse != null) {
            nodeManagerInUse.interrupt();
         }

         backupActivationThread.interrupt();

         backupActivationThread.join(1000);

      }

      if (System.currentTimeMillis() - start >= timeout) {
         ActiveMQServerLogger.LOGGER.backupActivationTimeout();
         threadDump();
      }
   }

   public void resetNodeManager() throws Exception {
      if (nodeManager != null) {
         nodeManager.stop();
      }
      nodeManager = createNodeManager(configuration.getJournalLocation(), true);
   }

   @Override
   public Activation getActivation() {
      return activation;
   }

   @Override
   public HAPolicy getHAPolicy() {
      return haPolicy;
   }

   @Override
   public void setHAPolicy(HAPolicy haPolicy) {
      if (logger.isTraceEnabled()) {
         logger.tracef("XXX @@@ Setting %s, isBackup=%s at %s", haPolicy, haPolicy.isBackup(), this);
      }
      this.haPolicy = haPolicy;
   }

   @Override
   public void setMBeanServer(MBeanServer mbeanServer) {
      if (state == SERVER_STATE.STARTING || state == SERVER_STATE.STARTED) {
         throw ActiveMQMessageBundle.BUNDLE.cannotSetMBeanserver();
      }
      this.mbeanServer = mbeanServer;
   }

   @Override
   public void addExternalComponent(ActiveMQComponent externalComponent) {
      externalComponents.add(externalComponent);
   }

   public ExecutorService getThreadPool() {
      return threadPool;
   }

   public void setActivation(SharedNothingLiveActivation activation) {
      this.activation = activation;
   }

   /**
    * Stops the server in a different thread.
    */
   public final void stopTheServer(final boolean criticalIOError) {
      Thread thread = new Thread() {
         @Override
         public void run() {
            try {
               ActiveMQServerImpl.this.stop(false, criticalIOError, false);
            } catch (Exception e) {
               ActiveMQServerLogger.LOGGER.errorStoppingServer(e);
            }
         }
      };

      thread.start();
   }

   @Override
   public final void stop() throws Exception {
      try {
         internalStop();
      } finally {
         networkHealthCheck.stop();
      }
   }

   private void internalStop() throws Exception {
      stop(false);
   }

   @Override
   public void addActivationParam(String key, Object val) {
      activationParams.put(key, val);
   }

   @Override
   public boolean isAddressBound(String address) throws Exception {
      return postOffice.isAddressBound(SimpleString.toSimpleString(address));
   }

   @Override
   public BindingQueryResult bindingQuery(SimpleString address) throws Exception {
      if (address == null) {
         throw ActiveMQMessageBundle.BUNDLE.addressIsNull();
      }

      AddressSettings addressSettings = getAddressSettingsRepository().getMatch(address.toString());

      boolean autoCreateQeueus = addressSettings.isAutoCreateQueues();
      boolean autoCreateAddresses = addressSettings.isAutoCreateAddresses();
      boolean defaultPurgeOnNoConsumers = addressSettings.isDefaultPurgeOnNoConsumers();
      int defaultMaxConsumers = addressSettings.getDefaultMaxConsumers();

      List<SimpleString> names = new ArrayList<>();

      // make an exception for the management address (see HORNETQ-29)
      ManagementService managementService = getManagementService();
      if (managementService != null) {
         if (address.equals(managementService.getManagementAddress())) {
            return new BindingQueryResult(true, names, autoCreateQeueus, autoCreateAddresses, defaultPurgeOnNoConsumers, defaultMaxConsumers);
         }
      }

      Bindings bindings = getPostOffice().getMatchingBindings(address);

      for (Binding binding : bindings.getBindings()) {
         if (binding.getType() == BindingType.LOCAL_QUEUE || binding.getType() == BindingType.REMOTE_QUEUE) {
            names.add(binding.getUniqueName());
         }
      }

      return new BindingQueryResult(getAddressInfo(address) != null, names, autoCreateQeueus, autoCreateAddresses, defaultPurgeOnNoConsumers, defaultMaxConsumers);
   }

   @Override
   public QueueQueryResult queueQuery(SimpleString name) {
      if (name == null) {
         throw ActiveMQMessageBundle.BUNDLE.queueNameIsNull();
      }

      boolean autoCreateQueues = getAddressSettingsRepository().getMatch(name.toString()).isAutoCreateQueues();
      boolean defaultPurgeOnNoConsumers = getAddressSettingsRepository().getMatch(name.toString()).isDefaultPurgeOnNoConsumers();
      int defaultMaxConsumers = getAddressSettingsRepository().getMatch(name.toString()).getDefaultMaxConsumers();

      QueueQueryResult response;

      Binding binding = getPostOffice().getBinding(name);

      SimpleString managementAddress = getManagementService() != null ? getManagementService().getManagementAddress() : null;

      if (binding != null && binding.getType() == BindingType.LOCAL_QUEUE) {
         Queue queue = (Queue) binding.getBindable();

         Filter filter = queue.getFilter();

         SimpleString filterString = filter == null ? null : filter.getFilterString();

         response = new QueueQueryResult(name, binding.getAddress(), queue.isDurable(), queue.isTemporary(), filterString, queue.getConsumerCount(), queue.getMessageCount(), autoCreateQueues, true, queue.isAutoCreated(), queue.isPurgeOnNoConsumers(), queue.getRoutingType(), queue.getMaxConsumers());
      } else if (name.equals(managementAddress)) {
         // make an exception for the management address (see HORNETQ-29)
         response = new QueueQueryResult(name, managementAddress, true, false, null, -1, -1, autoCreateQueues, true, false, false, RoutingType.MULTICAST, -1);
      } else if (autoCreateQueues) {
         response = new QueueQueryResult(name, name, true, false, null, 0, 0, true, false, false, defaultPurgeOnNoConsumers, RoutingType.MULTICAST, defaultMaxConsumers);
      } else {
         response = new QueueQueryResult(null, null, false, false, null, 0, 0, false, false, false, false, RoutingType.MULTICAST, 0);
      }

      return response;
   }

   @Override
   public AddressQueryResult addressQuery(SimpleString name) throws Exception {
      if (name == null) {
         throw ActiveMQMessageBundle.BUNDLE.queueNameIsNull();
      }

      AddressSettings addressSettings = getAddressSettingsRepository().getMatch(name.toString());

      boolean autoCreateAddresses = addressSettings.isAutoCreateAddresses();
      boolean defaultPurgeOnNoConsumers = addressSettings.isDefaultPurgeOnNoConsumers();
      int defaultMaxConsumers = addressSettings.getDefaultMaxConsumers();

      AddressInfo addressInfo = postOffice.getAddressInfo(name);
      AddressQueryResult response;
      if (addressInfo != null) {
         response = new AddressQueryResult(addressInfo.getName(), addressInfo.getRoutingTypes(), addressInfo.getId(), addressInfo.isAutoCreated(), true, autoCreateAddresses, defaultPurgeOnNoConsumers, defaultMaxConsumers);
      } else {
         response = new AddressQueryResult(name, null, -1, false, false, autoCreateAddresses, defaultPurgeOnNoConsumers, defaultMaxConsumers);
      }
      return response;
   }

   @Override
   public void threadDump() {
      StringWriter str = new StringWriter();
      PrintWriter out = new PrintWriter(str);

      Map<Thread, StackTraceElement[]> stackTrace = Thread.getAllStackTraces();

      out.println(ActiveMQMessageBundle.BUNDLE.generatingThreadDump());
      out.println("*******************************************************************************");

      for (Map.Entry<Thread, StackTraceElement[]> el : stackTrace.entrySet()) {
         out.println("===============================================================================");
         out.println(ActiveMQMessageBundle.BUNDLE.threadDump(el.getKey(), el.getKey().getName(), el.getKey().getId(), el.getKey().getThreadGroup()));
         out.println();
         for (StackTraceElement traceEl : el.getValue()) {
            out.println(traceEl);
         }
      }

      out.println("===============================================================================");
      out.println(ActiveMQMessageBundle.BUNDLE.endThreadDump());
      out.println("*******************************************************************************");

      ActiveMQServerLogger.LOGGER.threadDump(str.toString());
   }

   @Override
   public final void stop(boolean failoverOnServerShutdown) throws Exception {
      stop(failoverOnServerShutdown, false, false);
   }

   @Override
   public boolean isReplicaSync() {
      if (activation instanceof SharedNothingLiveActivation) {
         ReplicationManager replicationManager = getReplicationManager();

         if (replicationManager == null) {
            return false;
         } else {
            return !replicationManager.isSynchronizing();
         }
      } else if (activation instanceof SharedNothingBackupActivation) {
         return ((SharedNothingBackupActivation) activation).isRemoteBackupUpToDate();
      } else {
         throw ActiveMQMessageBundle.BUNDLE.methodNotApplicable();
      }
   }

   /**
    * Stops the server
    *
    * @param criticalIOError whether we have encountered an IO error with the journal etc
    */
   void stop(boolean failoverOnServerShutdown, final boolean criticalIOError, boolean restarting) {

      synchronized (this) {
         if (state == SERVER_STATE.STOPPED || state == SERVER_STATE.STOPPING) {
            return;
         }
         state = SERVER_STATE.STOPPING;

         if (fileStoreMonitor != null) {
            fileStoreMonitor.stop();
            fileStoreMonitor = null;
         }

         if (failoverOnServerShutdown) {
            activation.sendLiveIsStopping();
         }

         stopComponent(connectorsService);

         // we stop the groupingHandler before we stop the cluster manager so binding mappings
         // aren't removed in case of failover
         if (groupingHandler != null) {
            managementService.removeNotificationListener(groupingHandler);
            stopComponent(groupingHandler);
         }
         stopComponent(clusterManager);

         if (remotingService != null) {
            remotingService.pauseAcceptors();
         }

         // allows for graceful shutdown
         if (remotingService != null && configuration.isGracefulShutdownEnabled()) {
            long timeout = configuration.getGracefulShutdownTimeout();
            try {
               if (timeout == -1) {
                  remotingService.getConnectionCountLatch().await();
               } else {
                  remotingService.getConnectionCountLatch().await(timeout);
               }
            } catch (InterruptedException e) {
               ActiveMQServerLogger.LOGGER.interruptWhilstStoppingComponent(remotingService.getClass().getName());
            }
         }

         freezeConnections();
      }

      activation.postConnectionFreeze();

      closeAllServerSessions(criticalIOError);

      // *************************************************************************************************************
      // There's no need to sync this part of the method, since the state stopped | stopping is checked within the sync
      //
      // we can't synchronized the whole method here as that would cause a deadlock
      // so stop is checking for stopped || stopping inside the lock
      // which will be already enough to guarantee that no other thread will be accessing this method here.
      //
      // *************************************************************************************************************

      if (storageManager != null)
         storageManager.clearContext();

      //before we stop any components deactivate any callbacks
      callDeActiveCallbacks();

      stopComponent(backupManager);

      try {
         activation.preStorageClose();
      } catch (Throwable t) {
         ActiveMQServerLogger.LOGGER.errorStoppingComponent(t, activation.getClass().getName());
      }

      stopComponent(pagingManager);

      if (storageManager != null)
         try {
            storageManager.stop(criticalIOError, failoverOnServerShutdown);
         } catch (Throwable t) {
            ActiveMQServerLogger.LOGGER.errorStoppingComponent(t, storageManager.getClass().getName());
         }

      // We stop remotingService before otherwise we may lock the system in case of a critical IO
      // error shutdown
      if (remotingService != null)
         try {
            remotingService.stop(criticalIOError);
         } catch (Throwable t) {
            ActiveMQServerLogger.LOGGER.errorStoppingComponent(t, remotingService.getClass().getName());
         }

      // Stop the management service after the remoting service to ensure all acceptors are deregistered with JMX
      if (managementService != null)
         try {
            managementService.unregisterServer();
         } catch (Throwable t) {
            ActiveMQServerLogger.LOGGER.errorStoppingComponent(t, managementService.getClass().getName());
         }

      stopComponent(managementService);
      stopComponent(resourceManager);
      stopComponent(postOffice);

      if (scheduledPool != null && !scheduledPoolSupplied) {
         // we just interrupt all running tasks, these are supposed to be pings and the like.
         scheduledPool.shutdownNow();
      }

      stopComponent(memoryManager);

      for (SecuritySettingPlugin securitySettingPlugin : configuration.getSecuritySettingPlugins()) {
         securitySettingPlugin.stop();
      }

      if (threadPool != null && !threadPoolSupplied) {
         shutdownPool(threadPool);
      }

      if (ioExecutorPool != null) {
         shutdownPool(ioExecutorPool);
      }

      if (!threadPoolSupplied)
         threadPool = null;
      if (!scheduledPoolSupplied)
         scheduledPool = null;

      if (securityStore != null) {
         try {
            securityStore.stop();
         } catch (Throwable t) {
            ActiveMQServerLogger.LOGGER.errorStoppingComponent(t, managementService.getClass().getName());
         }
      }

      pagingManager = null;
      securityStore = null;
      resourceManager = null;
      postOffice = null;
      queueFactory = null;
      resourceManager = null;
      messagingServerControl = null;
      memoryManager = null;
      backupManager = null;
      storageManager = null;

      sessions.clear();

      state = SERVER_STATE.STOPPED;

      activationLatch.setCount(1);

      // to display in the log message
      SimpleString tempNodeID = getNodeID();
      if (activation != null) {
         try {
            activation.close(failoverOnServerShutdown, restarting);
         } catch (Throwable t) {
            ActiveMQServerLogger.LOGGER.errorStoppingComponent(t, activation.getClass().getName());
         }
      }

      if (backupActivationThread != null) {
         try {
            backupActivationThread.join(30000);
         } catch (InterruptedException e) {
            ActiveMQServerLogger.LOGGER.interruptWhilstStoppingComponent(backupActivationThread.getClass().getName());
         }

         if (backupActivationThread.isAlive()) {
            ActiveMQServerLogger.LOGGER.backupActivationDidntFinish(this);
            backupActivationThread.interrupt();
         }
      }

      stopComponent(nodeManager);

      nodeManager = null;

      addressSettingsRepository.clearListeners();

      addressSettingsRepository.clearCache();

      scaledDownNodeIDs.clear();

      for (ActiveMQComponent externalComponent : externalComponents) {
         try {
            externalComponent.stop();
         } catch (Exception e) {
            ActiveMQServerLogger.LOGGER.errorStoppingComponent(e, externalComponent.getClass().getName());
         }
      }

      if (identity != null) {
         ActiveMQServerLogger.LOGGER.serverStopped("identity=" + identity + ",version=" + getVersion().getFullVersion(), tempNodeID, getUptime());
      } else {
         ActiveMQServerLogger.LOGGER.serverStopped(getVersion().getFullVersion(), tempNodeID, getUptime());
      }
   }

   private void shutdownPool(ExecutorService executorService) {
      executorService.shutdown();
      try {
         if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
            ActiveMQServerLogger.LOGGER.timedOutStoppingThreadpool(threadPool);
            for (Runnable r : executorService.shutdownNow()) {
               logger.debug("Cancelled the execution of " + r);
            }
         }
      } catch (InterruptedException e) {
         ActiveMQServerLogger.LOGGER.interruptWhilstStoppingComponent(threadPool.getClass().getName());
      }
   }

   public boolean checkLiveIsNotColocated(String nodeId) {
      if (parentServer == null) {
         return true;
      } else {
         return !parentServer.getNodeID().toString().equals(nodeId);
      }
   }

   /**
    * Freeze all connections.
    * <p>
    * If replicating, avoid freezing the replication connection. Helper method for
    * {@link #stop(boolean, boolean, boolean)}.
    */
   private void freezeConnections() {
      activation.freezeConnections(remotingService);

      // after disconnecting all the clients close all the server sessions so any messages in delivery will be cancelled back to the queue
      for (ServerSession serverSession : sessions.values()) {
         try {
            serverSession.close(true);
         } catch (Exception e) {
            ActiveMQServerLogger.LOGGER.errorClosingSession(e);
         }
      }
   }

   /**
    * We close all the exception in an attempt to let any pending IO to finish to avoid scenarios
    * where the send or ACK got to disk but the response didn't get to the client It may still be
    * possible to have this scenario on a real failure (without the use of XA) But at least we will
    * do our best to avoid it on regular shutdowns
    */
   private void closeAllServerSessions(final boolean criticalIOError) {
      if (state != SERVER_STATE.STOPPING) {
         return;
      }
      for (ServerSession session : sessions.values()) {
         try {
            session.close(true);
         } catch (Exception e) {
            // If anything went wrong with closing sessions.. we should ignore it
            // such as transactions.. etc.
            ActiveMQServerLogger.LOGGER.errorClosingSessionsWhileStoppingServer(e);
         }
      }
      if (!criticalIOError) {
         for (ServerSession session : sessions.values()) {
            try {
               session.waitContextCompletion();
            } catch (Exception e) {
               ActiveMQServerLogger.LOGGER.errorClosingSessionsWhileStoppingServer(e);
            }
         }
      }

   }

   static void stopComponent(ActiveMQComponent component) {
      try {
         if (component != null) {
            component.stop();
         }
      } catch (Throwable t) {
         ActiveMQServerLogger.LOGGER.errorStoppingComponent(t, component.getClass().getName());
      }
   }

   // ActiveMQServer implementation
   // -----------------------------------------------------------

   @Override
   public String describe() {
      StringWriter str = new StringWriter();
      PrintWriter out = new PrintWriter(str);

      out.println(ActiveMQMessageBundle.BUNDLE.serverDescribe(identity, getClusterManager().describe()));

      return str.toString();
   }

   @Override
   public String destroyConnectionWithSessionMetadata(String metaKey, String parameterValue) throws Exception {
      StringBuffer operationsExecuted = new StringBuffer();

      try {
         operationsExecuted.append("**************************************************************************************************\n");
         operationsExecuted.append(ActiveMQMessageBundle.BUNDLE.destroyConnectionWithSessionMetadataHeader(metaKey, parameterValue) + "\n");

         Set<ServerSession> allSessions = getSessions();

         ServerSession sessionFound = null;
         for (ServerSession session : allSessions) {
            try {
               String value = session.getMetaData(metaKey);
               if (value != null && value.equals(parameterValue)) {
                  sessionFound = session;
                  operationsExecuted.append(ActiveMQMessageBundle.BUNDLE.destroyConnectionWithSessionMetadataClosingConnection(sessionFound.toString()) + "\n");
                  RemotingConnection conn = session.getRemotingConnection();
                  if (conn != null) {
                     conn.fail(ActiveMQMessageBundle.BUNDLE.destroyConnectionWithSessionMetadataSendException(metaKey, parameterValue));
                  }
                  session.close(true);
                  sessions.remove(session.getName());
               }
            } catch (Throwable e) {
               ActiveMQServerLogger.LOGGER.warn(e.getMessage(), e);
            }
         }

         if (sessionFound == null) {
            operationsExecuted.append(ActiveMQMessageBundle.BUNDLE.destroyConnectionWithSessionMetadataNoSessionFound(metaKey, parameterValue) + "\n");
         }

         operationsExecuted.append("**************************************************************************************************");

         return operationsExecuted.toString();
      } finally {
         // This operation is critical for the knowledge of the admin, so we need to add info logs for later knowledge
         ActiveMQServerLogger.LOGGER.info(operationsExecuted.toString());
      }

   }

   @Override
   public void setIdentity(String identity) {
      this.identity = identity;
   }

   @Override
   public String getIdentity() {
      return identity;
   }

   @Override
   public ScheduledExecutorService getScheduledPool() {
      return scheduledPool;
   }

   @Override
   public Configuration getConfiguration() {
      return configuration;
   }

   @Override
   public PagingManager getPagingManager() {
      return pagingManager;
   }

   @Override
   public RemotingService getRemotingService() {
      return remotingService;
   }

   @Override
   public StorageManager getStorageManager() {
      return storageManager;
   }

   @Override
   public ActiveMQSecurityManager getSecurityManager() {
      return securityManager;
   }

   @Override
   public ManagementService getManagementService() {
      return managementService;
   }

   @Override
   public HierarchicalRepository<Set<Role>> getSecurityRepository() {
      return securityRepository;
   }

   @Override
   public NodeManager getNodeManager() {
      return nodeManager;
   }

   @Override
   public HierarchicalRepository<AddressSettings> getAddressSettingsRepository() {
      return addressSettingsRepository;
   }

   @Override
   public ResourceManager getResourceManager() {
      return resourceManager;
   }

   @Override
   public Version getVersion() {
      return version;
   }

   @Override
   public boolean isStarted() {
      return state == SERVER_STATE.STARTED;
   }

   @Override
   public ClusterManager getClusterManager() {
      return clusterManager;
   }

   public BackupManager getBackupManager() {
      return backupManager;
   }

   @Override
   public ServerSession createSession(final String name,
                                      final String username,
                                      final String password,
                                      final int minLargeMessageSize,
                                      final RemotingConnection connection,
                                      final boolean autoCommitSends,
                                      final boolean autoCommitAcks,
                                      final boolean preAcknowledge,
                                      final boolean xa,
                                      final String defaultAddress,
                                      final SessionCallback callback,
                                      final boolean autoCreateQueues,
                                      final OperationContext context,
                                      final Map<SimpleString, RoutingType> prefixes) throws Exception {
      String validatedUser = "";

      if (securityStore != null) {
         X509Certificate[] certificates = null;
         if (connection.getTransportConnection() instanceof NettyConnection) {
            certificates = CertificateUtil.getCertsFromChannel(((NettyConnection) connection.getTransportConnection()).getChannel());
         }
         validatedUser = securityStore.authenticate(username, password, certificates);
      }

      checkSessionLimit(validatedUser);

      final ServerSessionImpl session = internalCreateSession(name, username, password, validatedUser, minLargeMessageSize, connection, autoCommitSends, autoCommitAcks, preAcknowledge, xa, defaultAddress, callback, context, autoCreateQueues, prefixes);

      sessions.put(name, session);

      return session;
   }

   private void checkSessionLimit(String username) throws Exception {
      if (configuration.getResourceLimitSettings() != null && configuration.getResourceLimitSettings().containsKey(username)) {
         ResourceLimitSettings limits = configuration.getResourceLimitSettings().get(username);

         if (limits.getMaxConnections() == -1) {
            return;
         } else if (limits.getMaxConnections() == 0 || getSessionCountForUser(username) >= limits.getMaxConnections()) {
            throw ActiveMQMessageBundle.BUNDLE.sessionLimitReached(username, limits.getMaxConnections());
         }
      }
   }

   private int getSessionCountForUser(String username) {
      int sessionCount = 0;

      for (Entry<String, ServerSession> sessionEntry : sessions.entrySet()) {
         if (sessionEntry.getValue().getUsername().equals(username)) {
            sessionCount++;
         }
      }

      return sessionCount;
   }

   @Override
   public void checkQueueCreationLimit(String username) throws Exception {
      if (configuration.getResourceLimitSettings() != null && configuration.getResourceLimitSettings().containsKey(username)) {
         ResourceLimitSettings limits = configuration.getResourceLimitSettings().get(username);

         if (limits.getMaxQueues() == -1) {
            return;
         } else if (limits.getMaxQueues() == 0 || getQueueCountForUser(username) >= limits.getMaxQueues()) {
            throw ActiveMQMessageBundle.BUNDLE.queueLimitReached(username, limits.getMaxQueues());
         }
      }
   }

   public int getQueueCountForUser(String username) throws Exception {
      Map<SimpleString, Binding> bindings = postOffice.getAllBindings();

      int queuesForUser = 0;

      for (Binding binding : bindings.values()) {
         if (binding instanceof LocalQueueBinding && ((LocalQueueBinding) binding).getQueue().getUser().equals(SimpleString.toSimpleString(username))) {
            queuesForUser++;
         }
      }

      return queuesForUser;

   }

   protected ServerSessionImpl internalCreateSession(String name,
                                                     String username,
                                                     String password,
                                                     String validatedUser,
                                                     int minLargeMessageSize,
                                                     RemotingConnection connection,
                                                     boolean autoCommitSends,
                                                     boolean autoCommitAcks,
                                                     boolean preAcknowledge,
                                                     boolean xa,
                                                     String defaultAddress,
                                                     SessionCallback callback,
                                                     OperationContext context,
                                                     boolean autoCreateJMSQueues,
                                                     Map<SimpleString, RoutingType> prefixes) throws Exception {
      return new ServerSessionImpl(name, username, password, validatedUser, minLargeMessageSize, autoCommitSends, autoCommitAcks, preAcknowledge, configuration.isPersistDeliveryCountBeforeDelivery(), xa, connection, storageManager, postOffice, resourceManager, securityStore, managementService, this, configuration.getManagementAddress(), defaultAddress == null ? null : new SimpleString(defaultAddress), callback, context, pagingManager, prefixes);
   }

   @Override
   public SecurityStore getSecurityStore() {
      return securityStore;
   }

   @Override
   public void removeSession(final String name) throws Exception {
      sessions.remove(name);
   }

   @Override
   public ServerSession lookupSession(String key, String value) {
      // getSessions is called here in a try to minimize locking the Server while this check is being done
      Set<ServerSession> allSessions = getSessions();

      for (ServerSession session : allSessions) {
         String metaValue = session.getMetaData(key);
         if (metaValue != null && metaValue.equals(value)) {
            return session;
         }
      }

      return null;
   }

   @Override
   public synchronized List<ServerSession> getSessions(final String connectionID) {
      Set<Entry<String, ServerSession>> sessionEntries = sessions.entrySet();
      List<ServerSession> matchingSessions = new ArrayList<>();
      for (Entry<String, ServerSession> sessionEntry : sessionEntries) {
         ServerSession serverSession = sessionEntry.getValue();
         if (serverSession.getConnectionID().toString().equals(connectionID)) {
            matchingSessions.add(serverSession);
         }
      }
      return matchingSessions;
   }

   @Override
   public synchronized Set<ServerSession> getSessions() {
      return new HashSet<>(sessions.values());
   }

   @Override
   public boolean isActive() {
      return activationLatch.getCount() < 1;
   }

   @Override
   public boolean waitForActivation(long timeout, TimeUnit unit) throws InterruptedException {
      return activationLatch.await(timeout, unit);
   }

   @Override
   public ActiveMQServerControlImpl getActiveMQServerControl() {
      return messagingServerControl;
   }

   @Override
   public int getConnectionCount() {
      return remotingService.getConnections().size();
   }

   @Override
   public long getTotalConnectionCount() {
      return remotingService.getTotalConnectionCount();
   }

   @Override
   public long getTotalMessageCount() {
      long total = 0;

      for (Binding binding : postOffice.getAllBindings().values()) {
         if (binding.getType() == BindingType.LOCAL_QUEUE) {
            total += ((LocalQueueBinding) binding).getQueue().getMessageCount();
         }
      }

      return total;
   }

   @Override
   public long getTotalMessagesAdded() {
      long total = 0;

      for (Binding binding : postOffice.getAllBindings().values()) {
         if (binding.getType() == BindingType.LOCAL_QUEUE) {
            total += ((LocalQueueBinding) binding).getQueue().getMessagesAdded();
         }
      }

      return total;
   }

   @Override
   public long getTotalMessagesAcknowledged() {
      long total = 0;

      for (Binding binding : postOffice.getAllBindings().values()) {
         if (binding.getType() == BindingType.LOCAL_QUEUE) {
            total += ((LocalQueueBinding) binding).getQueue().getMessagesAcknowledged();
         }
      }

      return total;
   }

   @Override
   public long getTotalConsumerCount() {
      long total = 0;

      for (Binding binding : postOffice.getAllBindings().values()) {
         if (binding.getType() == BindingType.LOCAL_QUEUE) {
            total += ((LocalQueueBinding) binding).getQueue().getConsumerCount();
         }
      }

      return total;
   }

   @Override
   public PostOffice getPostOffice() {
      return postOffice;
   }

   @Override
   public QueueFactory getQueueFactory() {
      return queueFactory;
   }

   @Override
   public SimpleString getNodeID() {
      return nodeManager == null ? null : nodeManager.getNodeId();
   }

   @Override
   public Queue createQueue(final SimpleString address,
                            final RoutingType routingType,
                            final SimpleString queueName,
                            final SimpleString filterString,
                            final boolean durable,
                            final boolean temporary) throws Exception {
      AddressSettings as = getAddressSettingsRepository().getMatch(address.toString());
      return createQueue(address, routingType, queueName, filterString, durable, temporary, as.getDefaultMaxConsumers(), as.isDefaultPurgeOnNoConsumers(), as.isAutoCreateAddresses());
   }

   @Override
   public Queue createQueue(final SimpleString address,
                            final RoutingType routingType,
                            final SimpleString queueName,
                            final SimpleString filter,
                            final boolean durable,
                            final boolean temporary,
                            final int maxConsumers,
                            final boolean purgeOnNoConsumers,
                            final boolean autoCreateAddress) throws Exception {
      return createQueue(address, routingType, queueName, filter, null, durable, temporary, false, maxConsumers, purgeOnNoConsumers, autoCreateAddress);
   }

   @Override
   public Queue createQueue(SimpleString address,
                            RoutingType routingType,
                            SimpleString queueName,
                            SimpleString filter,
                            SimpleString user,
                            boolean durable,
                            boolean temporary,
                            boolean autoCreated,
                            Integer maxConsumers,
                            Boolean deleteOnNoConsumers,
                            boolean autoCreateAddress) throws Exception {
      return createQueue(address, routingType, queueName, filter, user, durable, temporary, false, false, autoCreated, maxConsumers, deleteOnNoConsumers, autoCreateAddress);
   }

   @Deprecated
   @Override
   public Queue createQueue(final SimpleString address,
                            final SimpleString queueName,
                            final SimpleString filterString,
                            final boolean durable,
                            final boolean temporary) throws Exception {
      return createQueue(address, getAddressSettingsRepository().getMatch(address.toString()).getDefaultQueueRoutingType(), queueName, filterString, durable, temporary);
   }

   @Override
   public void createSharedQueue(final SimpleString address,
                                 RoutingType routingType,
                                 final SimpleString name,
                                 final SimpleString filterString,
                                 final SimpleString user,
                                 boolean durable) throws Exception {
      //force the old contract about address
      if (address == null) {
         throw new NullPointerException("address can't be null!");
      }

      if (routingType == null) {
         AddressInfo addressInfo = getAddressInfo(address);
         routingType = addressInfo.getRoutingTypes().size() == 1 ? addressInfo.getRoutingType() : getAddressSettingsRepository().getMatch(address.toString()).getDefaultQueueRoutingType();
         if (routingType == null) {
            // TODO (mtaylor) throw exception Can not determine routing type info from address
         }
      }

      final Queue queue = createQueue(address, routingType, name, filterString, user, durable, !durable, true, !durable, false, Queue.MAX_CONSUMERS_UNLIMITED, false, true);

      if (!queue.getAddress().equals(address)) {
         throw ActiveMQMessageBundle.BUNDLE.queueSubscriptionBelongsToDifferentAddress(name);
      }

      if (filterString != null && (queue.getFilter() == null || !queue.getFilter().getFilterString().equals(filterString)) || filterString == null && queue.getFilter() != null) {
         throw ActiveMQMessageBundle.BUNDLE.queueSubscriptionBelongsToDifferentFilter(name);
      }

      if (logger.isDebugEnabled()) {
         logger.debug("Transient Queue " + name + " created on address " + name + " with filter=" + filterString);
      }

   }

   @Override
   public Queue locateQueue(SimpleString queueName) {
      Binding binding = postOffice.getBinding(queueName);

      if (binding == null) {
         return null;
      }

      Bindable queue = binding.getBindable();

      if (!(queue instanceof Queue)) {
         throw new IllegalStateException("locateQueue should only be used to locate queues");
      }

      return (Queue) binding.getBindable();
   }

   @Deprecated
   @Override
   public Queue deployQueue(final SimpleString address,
                            final SimpleString resourceName,
                            final SimpleString filterString,
                            final boolean durable,
                            final boolean temporary) throws Exception {
      return createQueue(address, getAddressSettingsRepository().getMatch(address.toString()).getDefaultQueueRoutingType(), resourceName, filterString, durable, temporary);
   }

   @Deprecated
   @Override
   public Queue deployQueue(final String address,
                            final String resourceName,
                            final String filterString,
                            final boolean durable,
                            final boolean temporary) throws Exception {
      return deployQueue(SimpleString.toSimpleString(address), SimpleString.toSimpleString(resourceName), SimpleString.toSimpleString(filterString), durable, temporary);
   }

   @Override
   public void destroyQueue(final SimpleString queueName) throws Exception {
      // The session is passed as an argument to verify if the user has authorization to delete the queue
      // in some cases (such as temporary queues) this should happen regardless of the authorization
      // since that will only happen during a session close, which will be used to cleanup on temporary queues
      destroyQueue(queueName, null, true);
   }

   @Override
   public void destroyQueue(final SimpleString queueName, final SecurityAuth session) throws Exception {
      destroyQueue(queueName, session, true);
   }

   @Override
   public void destroyQueue(final SimpleString queueName,
                            final SecurityAuth session,
                            final boolean checkConsumerCount) throws Exception {
      destroyQueue(queueName, session, checkConsumerCount, false);
   }

   @Override
   public void destroyQueue(final SimpleString queueName,
                            final SecurityAuth session,
                            final boolean checkConsumerCount,
                            final boolean removeConsumers) throws Exception {
      if (postOffice == null) {
         return;
      }

      Binding binding = postOffice.getBinding(queueName);

      if (binding == null) {
         throw ActiveMQMessageBundle.BUNDLE.noSuchQueue(queueName);
      }

      String address = binding.getAddress().toString();

      destroyQueue(queueName, session, checkConsumerCount, removeConsumers, addressSettingsRepository.getMatch(address).isAutoDeleteAddresses());
   }

   @Override
   public void destroyQueue(final SimpleString queueName,
                            final SecurityAuth session,
                            final boolean checkConsumerCount,
                            final boolean removeConsumers,
                            final boolean autoDeleteAddress) throws Exception {
      if (postOffice == null) {
         return;
      }

      addressSettingsRepository.clearCache();

      Binding binding = postOffice.getBinding(queueName);

      if (binding == null) {
         throw ActiveMQMessageBundle.BUNDLE.noSuchQueue(queueName);
      }

      SimpleString address = binding.getAddress();

      Queue queue = (Queue) binding.getBindable();

      // This check is only valid if checkConsumerCount == true
      if (checkConsumerCount && queue.getConsumerCount() != 0) {
         throw ActiveMQMessageBundle.BUNDLE.cannotDeleteQueue(queue.getName(), queueName, binding.getClass().getName());
      }

      if (session != null) {

         if (queue.isDurable()) {
            // make sure the user has privileges to delete this queue
            securityStore.check(address, CheckType.DELETE_DURABLE_QUEUE, session);
         } else {
            securityStore.check(address, CheckType.DELETE_NON_DURABLE_QUEUE, session);
         }
      }

      queue.deleteQueue(removeConsumers);

      if (autoDeleteAddress && postOffice != null && getAddressInfo(address).isAutoCreated()) {
         try {
            removeAddressInfo(address, session);
         } catch (ActiveMQDeleteAddressException e) {
            // Could be thrown if the address has bindings or is not deletable.
         }
      }

      callPostQueueDeletionCallbacks(address, queueName);
   }

   @Override
   public void registerActivateCallback(final ActivateCallback callback) {
      activateCallbacks.add(callback);
   }

   @Override
   public void unregisterActivateCallback(final ActivateCallback callback) {
      activateCallbacks.remove(callback);
   }

   @Override
   public void registerActivationFailureListener(final ActivationFailureListener listener) {
      activationFailureListeners.add(listener);
   }

   @Override
   public void unregisterActivationFailureListener(final ActivationFailureListener listener) {
      activationFailureListeners.remove(listener);
   }

   @Override
   public void callActivationFailureListeners(final Exception e) {
      for (ActivationFailureListener listener : activationFailureListeners) {
         listener.activationFailed(e);
      }
   }

   @Override
   public void registerPostQueueCreationCallback(final PostQueueCreationCallback callback) {
      postQueueCreationCallbacks.add(callback);
   }

   @Override
   public void unregisterPostQueueCreationCallback(final PostQueueCreationCallback callback) {
      postQueueCreationCallbacks.remove(callback);
   }

   @Override
   public void callPostQueueCreationCallbacks(final SimpleString queueName) throws Exception {
      for (PostQueueCreationCallback callback : postQueueCreationCallbacks) {
         callback.callback(queueName);
      }
   }

   @Override
   public void registerPostQueueDeletionCallback(final PostQueueDeletionCallback callback) {
      postQueueDeletionCallbacks.add(callback);
   }

   @Override
   public void unregisterPostQueueDeletionCallback(final PostQueueDeletionCallback callback) {
      postQueueDeletionCallbacks.remove(callback);
   }

   @Override
   public void callPostQueueDeletionCallbacks(final SimpleString address,
                                              final SimpleString queueName) throws Exception {
      for (PostQueueDeletionCallback callback : postQueueDeletionCallbacks) {
         callback.callback(address, queueName);
      }
   }

   @Override
   public ExecutorFactory getExecutorFactory() {
      return executorFactory;
   }

   @Override
   public ExecutorFactory getIOExecutorFactory() {
      return ioExecutorFactory;
   }

   @Override
   public void setGroupingHandler(final GroupingHandler groupingHandler) {
      if (this.groupingHandler != null && managementService != null) {
         // Removing old groupNotification
         managementService.removeNotificationListener(this.groupingHandler);
      }
      this.groupingHandler = groupingHandler;
      if (managementService != null) {
         managementService.addNotificationListener(this.groupingHandler);
      }

   }

   @Override
   public GroupingHandler getGroupingHandler() {
      return groupingHandler;
   }

   @Override
   public ReplicationManager getReplicationManager() {
      return activation.getReplicationManager();
   }

   @Override
   public ConnectorsService getConnectorsService() {
      return connectorsService;
   }

   @Override
   public void deployDivert(DivertConfiguration config) throws Exception {
      if (config.getName() == null) {
         throw ActiveMQMessageBundle.BUNDLE.divertWithNoName();
      }

      if (config.getAddress() == null) {
         ActiveMQServerLogger.LOGGER.divertWithNoAddress();

         return;
      }

      if (config.getForwardingAddress() == null) {
         ActiveMQServerLogger.LOGGER.divertWithNoForwardingAddress();

         return;
      }

      SimpleString sName = new SimpleString(config.getName());

      if (postOffice.getBinding(sName) != null) {
         ActiveMQServerLogger.LOGGER.divertBindingAlreadyExists(sName);

         return;
      }

      SimpleString sAddress = new SimpleString(config.getAddress());

      Transformer transformer = getServiceRegistry().getDivertTransformer(config.getName(), config.getTransformerClassName());

      Filter filter = FilterImpl.createFilter(config.getFilterString());

      Divert divert = new DivertImpl(new SimpleString(config.getForwardingAddress()), sName, new SimpleString(config.getRoutingName()), config.isExclusive(), filter, transformer, postOffice, storageManager, config.getRoutingType());

      Binding binding = new DivertBinding(storageManager.generateID(), sAddress, divert);

      postOffice.addBinding(binding);

      managementService.registerDivert(divert, config);
   }

   @Override
   public void destroyDivert(SimpleString name) throws Exception {
      Binding binding = postOffice.getBinding(name);
      if (binding == null) {
         throw ActiveMQMessageBundle.BUNDLE.noBindingForDivert(name);
      }
      if (!(binding instanceof DivertBinding)) {
         throw ActiveMQMessageBundle.BUNDLE.bindingNotDivert(name);
      }

      postOffice.removeBinding(name, null, true);
   }

   @Override
   public void deployBridge(BridgeConfiguration config) throws Exception {
      if (clusterManager != null) {
         clusterManager.deployBridge(config);
      }
   }

   @Override
   public void destroyBridge(String name) throws Exception {
      if (clusterManager != null) {
         clusterManager.destroyBridge(name);
      }
   }

   @Override
   public ServerSession getSessionByID(String sessionName) {
      return sessions.get(sessionName);
   }

   // PUBLIC -------

   @Override
   public String toString() {
      if (identity != null) {
         return "ActiveMQServerImpl::" + identity;
      }
      return "ActiveMQServerImpl::" + (nodeManager != null ? "serverUUID=" + nodeManager.getUUID() : "");
   }

   /**
    * For tests only, don't use this method as it's not part of the API
    *
    * @param factory
    */
   public void replaceQueueFactory(QueueFactory factory) {
      this.queueFactory = factory;
   }

   protected PagingManager createPagingManager() {
      return new PagingManagerImpl(getPagingStoreFactory(), addressSettingsRepository, configuration.getGlobalMaxSize());
   }

   protected PagingStoreFactoryNIO getPagingStoreFactory() {
      return new PagingStoreFactoryNIO(storageManager, configuration.getPagingLocation(), configuration.getJournalBufferTimeout_NIO(), scheduledPool, executorFactory, configuration.isJournalSyncNonTransactional(), shutdownOnCriticalIO);
   }

   /**
    * This method is protected as it may be used as a hook for creating a custom storage manager (on tests for instance)
    */
   private StorageManager createStorageManager() {
      if (configuration.isPersistenceEnabled()) {
         if (configuration.getStoreConfiguration() != null && configuration.getStoreConfiguration().getStoreType() == StoreConfiguration.StoreType.DATABASE) {
            return new JDBCJournalStorageManager(configuration, getScheduledPool(), executorFactory, ioExecutorFactory, shutdownOnCriticalIO);
         } else {
            // Default to File Based Storage Manager, (Legacy default configuration).
            return new JournalStorageManager(configuration, executorFactory, scheduledPool, ioExecutorFactory, shutdownOnCriticalIO);
         }
      }
      return new NullStorageManager();
   }

   private void callActivateCallbacks() {
      for (ActivateCallback callback : activateCallbacks) {
         callback.activated();
      }
   }

   private void callPreActiveCallbacks() {
      for (ActivateCallback callback : activateCallbacks) {
         callback.preActivate();
      }
   }

   private void callDeActiveCallbacks() {
      for (ActivateCallback callback : activateCallbacks) {
         try {
            callback.deActivate();
         } catch (Throwable e) {
            // https://bugzilla.redhat.com/show_bug.cgi?id=1009530:
            // we won't interrupt the shutdown sequence because of a failed callback here
            ActiveMQServerLogger.LOGGER.warn(e.getMessage(), e);
         }
      }
   }

   private void callActivationCompleteCallbacks() {
      for (ActivateCallback callback : activateCallbacks) {
         callback.activationComplete();
      }
   }

   /**
    * Sets up ActiveMQ Artemis Executor Services.
    */
   private void initializeExecutorServices() {
      /* We check to see if a Thread Pool is supplied in the InjectedObjectRegistry.  If so we created a new Ordered
       * Executor based on the provided Thread pool.  Otherwise we create a new ThreadPool.
       */
      if (serviceRegistry.getExecutorService() == null) {
         ThreadFactory tFactory = AccessController.doPrivileged(new PrivilegedAction<ThreadFactory>() {
            @Override
            public ThreadFactory run() {
               return new ActiveMQThreadFactory("ActiveMQ-server-" + this.toString(), false, ClientSessionFactoryImpl.class.getClassLoader());
            }
         });

         if (configuration.getThreadPoolMaxSize() == -1) {
            threadPool = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(), tFactory);
         } else {
            threadPool = new ActiveMQThreadPoolExecutor(0, configuration.getThreadPoolMaxSize(), 60L, TimeUnit.SECONDS, tFactory);
         }
      } else {
         threadPool = serviceRegistry.getExecutorService();
         this.threadPoolSupplied = true;
      }
      this.executorFactory = new OrderedExecutorFactory(threadPool);

      if (serviceRegistry.getIOExecutorService() != null) {
         this.ioExecutorFactory = new OrderedExecutorFactory(serviceRegistry.getIOExecutorService());
      } else {
         ThreadFactory tFactory = AccessController.doPrivileged(new PrivilegedAction<ThreadFactory>() {
            @Override
            public ThreadFactory run() {
               return new ActiveMQThreadFactory("ActiveMQ-IO-server-" + this.toString(), false, ClientSessionFactoryImpl.class.getClassLoader());
            }
         });

         this.ioExecutorPool = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(), tFactory);
         this.ioExecutorFactory = new OrderedExecutorFactory(ioExecutorPool);
      }

       /* We check to see if a Scheduled Executor Service is provided in the InjectedObjectRegistry.  If so we use this
       * Scheduled ExecutorService otherwise we create a new one.
       */
      if (serviceRegistry.getScheduledExecutorService() == null) {
         ThreadFactory tFactory = AccessController.doPrivileged(new PrivilegedAction<ThreadFactory>() {
            @Override
            public ThreadFactory run() {
               return new ActiveMQThreadFactory("ActiveMQ-scheduled-threads", false, ClientSessionFactoryImpl.class.getClassLoader());
            }
         });
         scheduledPool = new ScheduledThreadPoolExecutor(configuration.getScheduledThreadPoolMaxSize(), tFactory);
      } else {
         this.scheduledPoolSupplied = true;
         this.scheduledPool = serviceRegistry.getScheduledExecutorService();
      }
   }

   @Override
   public ServiceRegistry getServiceRegistry() {
      return serviceRegistry;
   }

   /**
    * Starts everything apart from RemotingService and loading the data.
    * <p>
    * After optional intermediary steps, Part 1 is meant to be followed by part 2
    * {@link #initialisePart2(boolean)}.
    *
    * @param scalingDown
    */
   synchronized boolean initialisePart1(boolean scalingDown) throws Exception {
      if (state == SERVER_STATE.STOPPED)
         return false;

      // Create the pools - we have two pools - one for non scheduled - and another for scheduled
      initializeExecutorServices();

      if (configuration.getJournalType() == JournalType.ASYNCIO) {
         if (!AIOSequentialFileFactory.isSupported()) {
            ActiveMQServerLogger.LOGGER.switchingNIO();
            configuration.setJournalType(JournalType.NIO);
         } else if (!AIOSequentialFileFactory.isSupported(configuration.getJournalLocation())) {
            ActiveMQServerLogger.LOGGER.switchingNIOonPath(configuration.getJournalLocation().getAbsolutePath());
            configuration.setJournalType(JournalType.NIO);
         }
      }

      managementService = new ManagementServiceImpl(mbeanServer, configuration);

      if (configuration.getMemoryMeasureInterval() != -1) {
         memoryManager = new MemoryManager(configuration.getMemoryWarningThreshold(), configuration.getMemoryMeasureInterval());

         memoryManager.start();
      }

      // Create the hard-wired components

      callPreActiveCallbacks();

      // startReplication();

      storageManager = createStorageManager();

      if (configuration.getClusterConfigurations().size() > 0 && ActiveMQDefaultConfiguration.getDefaultClusterUser().equals(configuration.getClusterUser()) && ActiveMQDefaultConfiguration.getDefaultClusterPassword().equals(configuration.getClusterPassword())) {
         ActiveMQServerLogger.LOGGER.clusterSecurityRisk();
      }

      securityStore = new SecurityStoreImpl(securityRepository, securityManager, configuration.getSecurityInvalidationInterval(), configuration.isSecurityEnabled(), configuration.getClusterUser(), configuration.getClusterPassword(), managementService);

      queueFactory = new QueueFactoryImpl(executorFactory, scheduledPool, addressSettingsRepository, storageManager);

      pagingManager = createPagingManager();

      resourceManager = new ResourceManagerImpl((int) (configuration.getTransactionTimeout() / 1000), configuration.getTransactionTimeoutScanPeriod(), scheduledPool);
      postOffice = new PostOfficeImpl(this, storageManager, pagingManager, queueFactory, managementService, configuration.getMessageExpiryScanPeriod(), configuration.getMessageExpiryThreadPriority(), configuration.getWildcardConfiguration(), configuration.getIDCacheSize(), configuration.isPersistIDCache(), addressSettingsRepository);

      // This can't be created until node id is set
      clusterManager = new ClusterManager(executorFactory, this, postOffice, scheduledPool, managementService, configuration, nodeManager, haPolicy.isBackup());

      backupManager = new BackupManager(this, executorFactory, scheduledPool, nodeManager, configuration, clusterManager);

      clusterManager.deploy();

      remotingService = new RemotingServiceImpl(clusterManager, configuration, this, managementService, scheduledPool, protocolManagerFactories, executorFactory.getExecutor(), serviceRegistry);

      messagingServerControl = managementService.registerServer(postOffice, securityStore, storageManager, configuration, addressSettingsRepository, securityRepository, resourceManager, remotingService, this, queueFactory, scheduledPool, pagingManager, haPolicy.isBackup());

      // Address settings need to deployed initially, since they're require on paging manager.start()

      if (!scalingDown) {
         deployAddressSettingsFromConfiguration();
      }

      storageManager.start();

      postOffice.start();

      pagingManager.start();

      managementService.start();

      resourceManager.start();

      deploySecurityFromConfiguration();

      deployGroupingHandlerConfiguration(configuration.getGroupingHandlerConfiguration());

      this.reloadManager = new ReloadManagerImpl(getScheduledPool(), executorFactory.getExecutor(), configuration.getConfigurationFileRefreshPeriod());

      if (configuration.getConfigurationUrl() != null && getScheduledPool() != null) {
         reloadManager.addCallback(configuration.getConfigurationUrl(), new ConfigurationFileReloader());
      }

      return true;
   }

   /*
    * Load the data, and start remoting service so clients can connect
    */
   synchronized void initialisePart2(boolean scalingDown) throws Exception {
      // Load the journal and populate queues, transactions and caches in memory

      if (state == SERVER_STATE.STOPPED || state == SERVER_STATE.STOPPING) {
         return;
      }

      pagingManager.reloadStores();

      JournalLoadInformation[] journalInfo = loadJournals();

      final ServerInfo dumper = new ServerInfo(this, pagingManager);

      long dumpInfoInterval = configuration.getServerDumpInterval();

      if (dumpInfoInterval > 0) {
         scheduledPool.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
               ActiveMQServerLogger.LOGGER.dumpServerInfo(dumper.dump());
            }
         }, 0, dumpInfoInterval, TimeUnit.MILLISECONDS);
      }

      // Deploy the rest of the stuff

      // Deploy predefined addresses
      deployAddressesFromConfiguration();

      // Deploy any predefined queues
      deployQueuesFromConfiguration();

      // We need to call this here, this gives any dependent server a chance to deploy its own addresses
      // this needs to be done before clustering is fully activated
      callActivateCallbacks();

      checkForPotentialOOMEInAddressConfiguration();

      if (!scalingDown) {
         // Deploy any pre-defined diverts
         deployDiverts();

         if (groupingHandler != null) {
            groupingHandler.start();
         }

         // We do this at the end - we don't want things like MDBs or other connections connecting to a backup server until
         // it is activated

         if (groupingHandler != null && groupingHandler instanceof LocalGroupingHandler) {
            clusterManager.start();

            groupingHandler.awaitBindings();

            remotingService.start();
         } else {
            remotingService.start();

            clusterManager.start();
         }

         if (nodeManager.getNodeId() == null) {
            throw ActiveMQMessageBundle.BUNDLE.nodeIdNull();
         }

         // We can only do this after everything is started otherwise we may get nasty races with expired messages
         postOffice.startExpiryScanner();
      }

      try {
         injectMonitor(new FileStoreMonitor(getScheduledPool(), executorFactory.getExecutor(), configuration.getDiskScanPeriod(), TimeUnit.MILLISECONDS, configuration.getMaxDiskUsage() / 100f));
      } catch (Exception e) {
         logger.warn(e.getMessage(), e);
      }
   }

   /**
    * This method exists for a possibility of test cases replacing the FileStoreMonitor for an extension that would for instance pretend a disk full on certain tests.
    */
   public void injectMonitor(FileStoreMonitor storeMonitor) throws Exception {
      this.fileStoreMonitor = storeMonitor;
      pagingManager.injectMonitor(storeMonitor);
      storageManager.injectMonitor(storeMonitor);
      fileStoreMonitor.start();
   }

   public FileStoreMonitor getMonitor() {
      return fileStoreMonitor;
   }

   public void completeActivation() throws Exception {
      setState(ActiveMQServerImpl.SERVER_STATE.STARTED);
      getRemotingService().startAcceptors();
      activationLatch.countDown();
      callActivationCompleteCallbacks();
   }

   private void deploySecurityFromConfiguration() {
      for (Map.Entry<String, Set<Role>> entry : configuration.getSecurityRoles().entrySet()) {
         securityRepository.addMatch(entry.getKey(), entry.getValue(), true);
      }

      for (SecuritySettingPlugin securitySettingPlugin : configuration.getSecuritySettingPlugins()) {
         securitySettingPlugin.setSecurityRepository(securityRepository);
      }
   }

   private void deployAddressesFromConfiguration() throws Exception {
      for (CoreAddressConfiguration config : configuration.getAddressConfigurations()) {
         AddressInfo info = new AddressInfo(SimpleString.toSimpleString(config.getName()), config.getRoutingTypes());
         addOrUpdateAddressInfo(info);
         deployQueuesFromListCoreQueueConfiguration(config.getQueueConfigurations());
      }
   }

   private void deployQueuesFromListCoreQueueConfiguration(List<CoreQueueConfiguration> queues) throws Exception {
      for (CoreQueueConfiguration config : queues) {
         ActiveMQServerLogger.LOGGER.deployQueue(SimpleString.toSimpleString(config.getName()));

         createQueue(SimpleString.toSimpleString(config.getAddress()), config.getRoutingType(), SimpleString.toSimpleString(config.getName()), SimpleString.toSimpleString(config.getFilterString()), null, config.isDurable(), false, true, false, false, config.getMaxConsumers(), config.getPurgeOnNoConsumers(), true);
      }
   }

   private void deployQueuesFromConfiguration() throws Exception {
      deployQueuesFromListCoreQueueConfiguration(configuration.getQueueConfigurations());
   }

   private void checkForPotentialOOMEInAddressConfiguration() {
      long totalMaxSizeBytes = 0;
      long addressCount = 0;
      for (SimpleString address : postOffice.getAddresses()) {
         totalMaxSizeBytes += addressSettingsRepository.getMatch(address.toString()).getMaxSizeBytes();
         addressCount++;
      }

      long maxMemory = Runtime.getRuntime().maxMemory();
      if (totalMaxSizeBytes >= maxMemory && configuration.getGlobalMaxSize() < 0) {
         ActiveMQServerLogger.LOGGER.potentialOOME(addressCount, totalMaxSizeBytes, maxMemory);
      }
   }

   private void deployAddressSettingsFromConfiguration() {
      for (Map.Entry<String, AddressSettings> entry : configuration.getAddressesSettings().entrySet()) {
         addressSettingsRepository.addMatch(entry.getKey(), entry.getValue(), true);
      }
   }

   private JournalLoadInformation[] loadJournals() throws Exception {
      JournalLoader journalLoader = activation.createJournalLoader(postOffice, pagingManager, storageManager, queueFactory, nodeManager, managementService, groupingHandler, configuration, parentServer);

      JournalLoadInformation[] journalInfo = new JournalLoadInformation[2];

      List<QueueBindingInfo> queueBindingInfos = new ArrayList<>();

      List<GroupingInfo> groupingInfos = new ArrayList<>();

      List<AddressBindingInfo> addressBindingInfos = new ArrayList<>();

      journalInfo[0] = storageManager.loadBindingJournal(queueBindingInfos, groupingInfos, addressBindingInfos);

      recoverStoredConfigs();

      Map<Long, AddressBindingInfo> addressBindingInfosMap = new HashMap<>();

      journalLoader.initAddresses(addressBindingInfosMap, addressBindingInfos);

      Map<Long, QueueBindingInfo> queueBindingInfosMap = new HashMap<>();

      journalLoader.initQueues(queueBindingInfosMap, queueBindingInfos);

      journalLoader.handleGroupingBindings(groupingInfos);

      Map<SimpleString, List<Pair<byte[], Long>>> duplicateIDMap = new HashMap<>();

      HashSet<Pair<Long, Long>> pendingLargeMessages = new HashSet<>();

      List<PageCountPending> pendingNonTXPageCounter = new LinkedList<>();

      journalInfo[1] = storageManager.loadMessageJournal(postOffice, pagingManager, resourceManager, queueBindingInfosMap, duplicateIDMap, pendingLargeMessages, pendingNonTXPageCounter, journalLoader);

      journalLoader.handleDuplicateIds(duplicateIDMap);

      for (Pair<Long, Long> msgToDelete : pendingLargeMessages) {
         ActiveMQServerLogger.LOGGER.deletingPendingMessage(msgToDelete);
         LargeServerMessage msg = storageManager.createLargeMessage();
         msg.setMessageID(msgToDelete.getB());
         msg.setPendingRecordID(msgToDelete.getA());
         msg.setDurable(true);
         msg.deleteFile();
      }

      if (pendingNonTXPageCounter.size() != 0) {
         try {
            journalLoader.recoverPendingPageCounters(pendingNonTXPageCounter);
         } catch (Throwable e) {
            ActiveMQServerLogger.LOGGER.errorRecoveringPageCounter(e);
         }
      }

      journalLoader.cleanUp();

      return journalInfo;
   }

   /**
    * @throws Exception
    */
   private void recoverStoredConfigs() throws Exception {
      List<PersistedAddressSetting> adsettings = storageManager.recoverAddressSettings();
      for (PersistedAddressSetting set : adsettings) {
         addressSettingsRepository.addMatch(set.getAddressMatch().toString(), set.getSetting());
      }

      List<PersistedRoles> roles = storageManager.recoverPersistedRoles();

      for (PersistedRoles roleItem : roles) {
         Set<Role> setRoles = SecurityFormatter.createSecurity(roleItem.getSendRoles(), roleItem.getConsumeRoles(), roleItem.getCreateDurableQueueRoles(), roleItem.getDeleteDurableQueueRoles(), roleItem.getCreateNonDurableQueueRoles(), roleItem.getDeleteNonDurableQueueRoles(), roleItem.getManageRoles(), roleItem.getBrowseRoles(), roleItem.getCreateAddressRoles(), roleItem.getDeleteAddressRoles());

         securityRepository.addMatch(roleItem.getAddressMatch().toString(), setRoles);
      }
   }

   @Override
   public boolean updateAddressInfo(SimpleString address, Collection<RoutingType> routingTypes) throws Exception {
      if (getAddressInfo(address) == null) {
         return false;
      }

      //after the postOffice call, updatedAddressInfo could change further (concurrently)!
      final AddressInfo updatedAddressInfo = postOffice.updateAddressInfo(address, routingTypes);
      //it change the address info without any lock!
      final long txID = storageManager.generateID();
      try {
         storageManager.deleteAddressBinding(txID, updatedAddressInfo.getId());
         storageManager.addAddressBinding(txID, updatedAddressInfo);
      } finally {
         storageManager.commitBindings(txID);
      }

      return true;
   }

   @Override
   public boolean addAddressInfo(AddressInfo addressInfo) throws Exception {
      boolean result = postOffice.addAddressInfo(addressInfo);

      if (result) {
         long txID = storageManager.generateID();
         storageManager.addAddressBinding(txID, addressInfo);
         storageManager.commitBindings(txID);
      } else {
         result = false;
      }

      return result;
   }

   @Override
   public AddressInfo addOrUpdateAddressInfo(AddressInfo addressInfo) throws Exception {
      if (!addAddressInfo(addressInfo)) {
         updateAddressInfo(addressInfo.getName(), addressInfo.getRoutingTypes());
      }

      return getAddressInfo(addressInfo.getName());
   }

   @Override
   public void removeAddressInfo(final SimpleString address, final SecurityAuth auth) throws Exception {
      if (auth != null) {
         securityStore.check(address, CheckType.DELETE_ADDRESS, auth);
      }

      AddressInfo addressInfo = getAddressInfo(address);
      if (postOffice.removeAddressInfo(address) == null) {
         throw ActiveMQMessageBundle.BUNDLE.addressDoesNotExist(address);
      }

      long txID = storageManager.generateID();
      storageManager.deleteAddressBinding(txID, addressInfo.getId());
      storageManager.commitBindings(txID);
   }

   @Override
   public String getInternalNamingPrefix() {
      return configuration.getInternalNamingPrefix();
   }

   @Override
   public AddressInfo getAddressInfo(SimpleString address) {
      return postOffice.getAddressInfo(address);
   }

   @Override
   public Queue createQueue(final SimpleString address,
                            final RoutingType routingType,
                            final SimpleString queueName,
                            final SimpleString filterString,
                            final SimpleString user,
                            final boolean durable,
                            final boolean temporary,
                            final boolean ignoreIfExists,
                            final boolean transientQueue,
                            final boolean autoCreated,
                            final int maxConsumers,
                            final boolean purgeOnNoConsumers,
                            final boolean autoCreateAddress) throws Exception {

      final QueueBinding binding = (QueueBinding) postOffice.getBinding(queueName);
      if (binding != null) {
         if (ignoreIfExists) {
            return binding.getQueue();
         } else {
            throw ActiveMQMessageBundle.BUNDLE.queueAlreadyExists(queueName);
         }
      }

      final Filter filter = FilterImpl.createFilter(filterString);

      final long txID = storageManager.generateID();
      final long queueID = storageManager.generateID();

      final QueueConfig.Builder queueConfigBuilder;
      if (address == null) {
         queueConfigBuilder = QueueConfig.builderWith(queueID, queueName);
      } else {
         queueConfigBuilder = QueueConfig.builderWith(queueID, queueName, address);
      }

      AddressInfo info = postOffice.getAddressInfo(address);

      if (autoCreateAddress) {
         RoutingType rt = (routingType == null ? ActiveMQDefaultConfiguration.getDefaultRoutingType() : routingType);
         if (info == null) {
            final AddressInfo addressInfo = new AddressInfo(address, rt);
            addressInfo.setAutoCreated(true);
            addAddressInfo(addressInfo);
         } else if (!info.getRoutingTypes().contains(routingType)) {
            Set<RoutingType> routingTypes = new HashSet<>();
            routingTypes.addAll(info.getRoutingTypes());
            routingTypes.add(routingType);
            updateAddressInfo(info.getName(), routingTypes);
         }
      } else if (info == null) {
         throw ActiveMQMessageBundle.BUNDLE.addressDoesNotExist(address);
      } else if (!info.getRoutingTypes().contains(routingType)) {
         throw ActiveMQMessageBundle.BUNDLE.invalidRoutingTypeForAddress(routingType, info.getName().toString(), info.getRoutingTypes());
      }

      final QueueConfig queueConfig = queueConfigBuilder.filter(filter).pagingManager(pagingManager).user(user).durable(durable).temporary(temporary).autoCreated(autoCreated).routingType(routingType).maxConsumers(maxConsumers).purgeOnNoConsumers(purgeOnNoConsumers).build();

      final Queue queue = queueFactory.createQueueWith(queueConfig);

      if (transientQueue) {
         queue.setConsumersRefCount(new TransientQueueManagerImpl(this, queue.getName()));
      } else if (queue.isAutoCreated()) {
         queue.setConsumersRefCount(new AutoCreatedQueueManagerImpl(this, queue.getName()));
      }

      final QueueBinding localQueueBinding = new LocalQueueBinding(queue.getAddress(), queue, nodeManager.getNodeId());

      if (queue.isDurable()) {
         storageManager.addQueueBinding(txID, localQueueBinding);
      }

      try {
         postOffice.addBinding(localQueueBinding);
         if (queue.isDurable()) {
            storageManager.commitBindings(txID);
         }
      } catch (Exception e) {
         try {
            if (durable) {
               storageManager.rollbackBindings(txID);
            }
            final PageSubscription pageSubscription = queue.getPageSubscription();
            try {
               queue.close();
            } finally {
               if (pageSubscription != null) {
                  pageSubscription.destroy();
               }
            }
         } catch (Throwable ignored) {
            logger.debug(ignored.getMessage(), ignored);
         }
         throw e;
      }

      managementService.registerQueue(queue, queue.getAddress(), storageManager);

      callPostQueueCreationCallbacks(queue.getName());

      return queue;
   }

   @Override
   public Queue updateQueue(String name,
                            RoutingType routingType,
                            Integer maxConsumers,
                            Boolean purgeOnNoConsumers) throws Exception {
      final QueueBinding queueBinding = this.postOffice.updateQueue(new SimpleString(name), routingType, maxConsumers, purgeOnNoConsumers);
      if (queueBinding != null) {
         final Queue queue = queueBinding.getQueue();
         if (queue.isDurable()) {
            final long txID = storageManager.generateID();
            try {
               storageManager.deleteQueueBinding(txID, queueBinding.getID());
               storageManager.addQueueBinding(txID, queueBinding);
               storageManager.commitBindings(txID);
            } catch (Throwable throwable) {
               storageManager.rollbackBindings(txID);
               throw throwable;
            }
         }
         return queue;
      } else {
         return null;
      }
   }

   private void deployDiverts() throws Exception {
      for (DivertConfiguration config : configuration.getDivertConfigurations()) {
         deployDivert(config);
      }
   }

   private void deployGroupingHandlerConfiguration(final GroupingHandlerConfiguration config) throws Exception {
      if (config != null) {
         GroupingHandler groupingHandler1;
         if (config.getType() == GroupingHandlerConfiguration.TYPE.LOCAL) {
            groupingHandler1 = new LocalGroupingHandler(executorFactory, scheduledPool, managementService, config.getName(), config.getAddress(), getStorageManager(), config.getTimeout(), config.getGroupTimeout(), config.getReaperPeriod());
         } else {
            groupingHandler1 = new RemoteGroupingHandler(executorFactory, managementService, config.getName(), config.getAddress(), config.getTimeout(), config.getGroupTimeout());
         }

         this.groupingHandler = groupingHandler1;

         managementService.addNotificationListener(groupingHandler1);
      }
   }

   /**
    * Check if journal directory exists or create it (if configured to do so)
    */
   public void checkJournalDirectory() {
      File journalDir = configuration.getJournalLocation();

      if (!journalDir.exists() && configuration.isPersistenceEnabled()) {
         if (configuration.isCreateJournalDir()) {
            journalDir.mkdirs();
         } else {
            throw ActiveMQMessageBundle.BUNDLE.cannotCreateDir(journalDir.getAbsolutePath());
         }
      }
   }

   // Inner classes
   // --------------------------------------------------------------------------------

   public final class ShutdownOnCriticalErrorListener implements IOCriticalErrorListener {

      boolean failedAlready = false;

      @Override
      public synchronized void onIOException(Throwable cause, String message, SequentialFile file) {
         if (!failedAlready) {
            failedAlready = true;

            if (file == null) {
               ActiveMQServerLogger.LOGGER.ioCriticalIOError(message, "NULL", cause);
            } else {
               ActiveMQServerLogger.LOGGER.ioCriticalIOError(message, file.toString(), cause);
            }

            stopTheServer(true);
         }
      }
   }

   @Override
   public void addProtocolManagerFactory(ProtocolManagerFactory factory) {
      protocolManagerFactories.add(factory);
   }

   @Override
   public void removeProtocolManagerFactory(ProtocolManagerFactory factory) {
      protocolManagerFactories.remove(factory);
   }

   @Override
   public ActiveMQServer createBackupServer(Configuration configuration) {
      return new ActiveMQServerImpl(configuration, null, securityManager, this);
   }

   @Override
   public void addScaledDownNode(SimpleString scaledDownNodeId) {
      synchronized (scaledDownNodeIDs) {
         scaledDownNodeIDs.add(scaledDownNodeId);
         if (scaledDownNodeIDs.size() > 10) {
            scaledDownNodeIDs.remove(10);
         }
      }
   }

   @Override
   public boolean hasScaledDown(SimpleString scaledDownNodeId) {
      return scaledDownNodeIDs.contains(scaledDownNodeId);
   }

   /**
    * Move data away before starting data synchronization for fail-back.
    * <p>
    * Use case is a server, upon restarting, finding a former backup running in its place. It will
    * move any older data away and log a warning about it.
    */
   void moveServerData(int maxSavedReplicated) throws IOException {
      File[] dataDirs = new File[]{configuration.getBindingsLocation(), configuration.getJournalLocation(), configuration.getPagingLocation(), configuration.getLargeMessagesLocation()};

      for (File data : dataDirs) {
         FileMoveManager moveManager = new FileMoveManager(data, maxSavedReplicated);
         moveManager.doMove();
      }
   }

   @Override
   public String getUptime() {
      long delta = getUptimeMillis();

      if (delta == 0) {
         return "not started";
      }

      return TimeUtils.printDuration(delta);
   }

   @Override
   public long getUptimeMillis() {
      if (startDate == null) {
         return 0;
      }

      return new Date().getTime() - startDate.getTime();
   }

   @Override
   public boolean addClientConnection(String clientId, boolean unique) {
      final AtomicInteger i = connectedClientIds.putIfAbsent(clientId, new AtomicInteger(1));
      if (i != null) {
         if (unique && i.get() != 0) {
            return false;
         } else if (i.incrementAndGet() > 0) {
            connectedClientIds.put(clientId, i);
         }
      }
      return true;
   }

   @Override
   public void removeClientConnection(String clientId) {
      AtomicInteger i = connectedClientIds.get(clientId);
      if (i != null && i.decrementAndGet() == 0) {
         connectedClientIds.remove(clientId);
      }
   }

   private final class ActivationThread extends Thread {

      final Runnable runnable;

      ActivationThread(Runnable runnable, String name) {
         super(name);
         this.runnable = runnable;
      }

      @Override
      public void run() {
         lockActivation();
         try {
            runnable.run();
         } finally {
            unlockActivation();
         }
      }

   }

   private final class ConfigurationFileReloader implements ReloadCallback {

      @Override
      public void reload(URL uri) throws Exception {
         Configuration config = new FileConfigurationParser().parseMainConfig(uri.openStream());
         ActiveMQServerLogger.LOGGER.reloadingConfiguration("security");
         securityRepository.swap(config.getSecurityRoles().entrySet());
         ActiveMQServerLogger.LOGGER.reloadingConfiguration("address settings");
         addressSettingsRepository.swap(config.getAddressesSettings().entrySet());
         ActiveMQServerLogger.LOGGER.reloadingConfiguration("diverts");
         for (DivertConfiguration divertConfig : config.getDivertConfigurations()) {
            if (postOffice.getBinding(new SimpleString(divertConfig.getName())) == null) {
               deployDivert(divertConfig);
            }
         }
      }
   }
}
