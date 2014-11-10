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
package org.hornetq.core.server.impl;

import javax.management.MBeanServer;

import java.io.File;
import java.io.FilenameFilter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.hornetq.api.config.HornetQDefaultConfiguration;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.asyncio.impl.AsynchronousFileImpl;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.config.BridgeConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.ConfigurationUtils;
import org.hornetq.core.config.CoreQueueConfiguration;
import org.hornetq.core.config.DivertConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.deployers.Deployer;
import org.hornetq.core.deployers.DeploymentManager;
import org.hornetq.core.deployers.impl.AddressSettingsDeployer;
import org.hornetq.core.deployers.impl.BasicUserCredentialsDeployer;
import org.hornetq.core.deployers.impl.FileDeploymentManager;
import org.hornetq.core.deployers.impl.QueueDeployer;
import org.hornetq.core.deployers.impl.SecurityDeployer;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.filter.impl.FilterImpl;
import org.hornetq.core.journal.IOCriticalErrorListener;
import org.hornetq.core.journal.JournalLoadInformation;
import org.hornetq.core.journal.SequentialFile;
import org.hornetq.core.journal.impl.AIOSequentialFileFactory;
import org.hornetq.core.journal.impl.SyncSpeedTest;
import org.hornetq.core.management.impl.HornetQServerControlImpl;
import org.hornetq.core.paging.PagingManager;
import org.hornetq.core.paging.cursor.PageSubscription;
import org.hornetq.core.paging.impl.PagingManagerImpl;
import org.hornetq.core.paging.impl.PagingStoreFactoryNIO;
import org.hornetq.core.persistence.GroupingInfo;
import org.hornetq.core.persistence.OperationContext;
import org.hornetq.core.persistence.QueueBindingInfo;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.persistence.config.PersistedAddressSetting;
import org.hornetq.core.persistence.config.PersistedRoles;
import org.hornetq.core.persistence.impl.PageCountPending;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager;
import org.hornetq.core.persistence.impl.journal.OperationContextImpl;
import org.hornetq.core.persistence.impl.nullpm.NullStorageManager;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.postoffice.QueueBinding;
import org.hornetq.core.postoffice.impl.DivertBinding;
import org.hornetq.core.postoffice.impl.LocalQueueBinding;
import org.hornetq.core.postoffice.impl.PostOfficeImpl;
import org.hornetq.core.remoting.server.RemotingService;
import org.hornetq.core.remoting.server.impl.RemotingServiceImpl;
import org.hornetq.core.replication.ReplicationManager;
import org.hornetq.core.security.CheckType;
import org.hornetq.core.security.Role;
import org.hornetq.core.security.SecurityStore;
import org.hornetq.core.security.impl.SecurityStoreImpl;
import org.hornetq.core.server.ActivateCallback;
import org.hornetq.core.server.Bindable;
import org.hornetq.core.server.Divert;
import org.hornetq.core.server.HornetQComponent;
import org.hornetq.core.server.HornetQMessageBundle;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServerLogger;
import org.hornetq.core.server.JournalType;
import org.hornetq.core.server.LargeServerMessage;
import org.hornetq.core.server.MemoryManager;
import org.hornetq.core.server.NodeManager;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.QueueFactory;
import org.hornetq.core.server.ServerSession;
import org.hornetq.core.server.ServerSessionFactory;
import org.hornetq.core.server.cluster.BackupManager;
import org.hornetq.core.server.cluster.ClusterManager;
import org.hornetq.core.server.cluster.Transformer;
import org.hornetq.core.server.cluster.ha.HAPolicy;
import org.hornetq.core.server.group.GroupingHandler;
import org.hornetq.core.server.group.impl.GroupingHandlerConfiguration;
import org.hornetq.core.server.group.impl.LocalGroupingHandler;
import org.hornetq.core.server.group.impl.RemoteGroupingHandler;
import org.hornetq.core.server.management.ManagementService;
import org.hornetq.core.server.management.impl.ManagementServiceImpl;
import org.hornetq.core.settings.HierarchicalRepository;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.core.settings.impl.HierarchicalObjectRepository;
import org.hornetq.core.transaction.ResourceManager;
import org.hornetq.core.transaction.impl.ResourceManagerImpl;
import org.hornetq.core.version.Version;
import org.hornetq.spi.core.protocol.ProtocolManagerFactory;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.spi.core.protocol.SessionCallback;
import org.hornetq.spi.core.security.HornetQSecurityManager;
import org.hornetq.utils.ClassloadingUtil;
import org.hornetq.utils.ConcurrentHashSet;
import org.hornetq.utils.ExecutorFactory;
import org.hornetq.utils.HornetQThreadFactory;
import org.hornetq.utils.OrderedExecutorFactory;
import org.hornetq.utils.ReusableLatch;
import org.hornetq.utils.SecurityFormatter;
import org.hornetq.utils.VersionLoader;

/**
 * The HornetQ server implementation
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ataylor@redhat.com>Andy Taylor</a>
 * @author <a href="mailto:mtaylor@redhat.com>Martyn Taylor</a>
 */
public class HornetQServerImpl implements HornetQServer
{
   /**
    * JMS Topics (which are outside of the scope of the core API) will require a dumb subscription
    * with a dummy-filter at this current version as a way to keep its existence valid and TCK
    * tests. That subscription needs an invalid filter, however paging needs to ignore any
    * subscription with this filter. For that reason, this filter needs to be rejected on paging or
    * any other component on the system, and just be ignored for any purpose It's declared here as
    * this filter is considered a global ignore
    */
   public static final String GENERIC_IGNORED_FILTER = "__HQX=-1";

   private HAPolicy haPolicy;

   enum SERVER_STATE
   {
      /**
       * start() has been called but components are not initialized. The whole point of this state,
       * is to be in a state which is different from {@link SERVER_STATE#STARTED} and
       * {@link SERVER_STATE#STOPPED}, so that methods testing for these two values such as
       * {@link #stop(boolean)} worked as intended.
       */
      STARTING,
      /**
       * server is started. {@code server.isStarted()} returns {@code true}, and all assumptions
       * about it hold.
       */
      STARTED,
      /**
       * stop() was called but has not finished yet. Meant to avoids starting components while
       * stop() is executing.
       */
      STOPPING,
      /**
       * Stopped: either stop() has been called and has finished running, or start() has never been
       * called.
       */
      STOPPED;
   }

   private volatile SERVER_STATE state = SERVER_STATE.STOPPED;

   private final Version version;

   private final HornetQSecurityManager securityManager;

   private final Configuration configuration;

   private final MBeanServer mbeanServer;

   private volatile SecurityStore securityStore;

   private final HierarchicalRepository<AddressSettings> addressSettingsRepository;

   private volatile QueueFactory queueFactory;

   private volatile PagingManager pagingManager;

   private volatile PostOffice postOffice;

   private volatile ExecutorService threadPool;

   private volatile ScheduledExecutorService scheduledPool;

   private volatile ExecutorFactory executorFactory;

   private final HierarchicalRepository<Set<Role>> securityRepository;

   private volatile ResourceManager resourceManager;

   private volatile HornetQServerControlImpl messagingServerControl;

   private volatile ClusterManager clusterManager;

   private volatile BackupManager backupManager;

   private volatile StorageManager storageManager;

   private volatile RemotingService remotingService;

   private final List<ProtocolManagerFactory> protocolManagerFactories = new ArrayList<>();

   private volatile ManagementService managementService;

   private volatile ConnectorsService connectorsService;

   private MemoryManager memoryManager;

   private volatile DeploymentManager deploymentManager;

   private Deployer basicUserCredentialsDeployer;
   private Deployer addressSettingsDeployer;
   private Deployer queueDeployer;
   private Deployer securityDeployer;

   private final Map<String, ServerSession> sessions = new ConcurrentHashMap<String, ServerSession>();

   /**
    * This class here has the same principle of CountDownLatch but you can reuse the counters.
    * It's based on the same super classes of {@code CountDownLatch}
    */
   private final ReusableLatch activationLatch = new ReusableLatch(0);

   private final Set<ActivateCallback> activateCallbacks = new ConcurrentHashSet<ActivateCallback>();

   private volatile GroupingHandler groupingHandler;

   private NodeManager nodeManager;

   // Used to identify the server on tests... useful on debugging testcases
   private String identity;

   private Thread backupActivationThread;

   private Activation activation;

   private Map<String, Object> activationParams = new HashMap<>();

   private final ShutdownOnCriticalErrorListener shutdownOnCriticalIO = new ShutdownOnCriticalErrorListener();

   private final HornetQServer parentServer;

   //todo think about moving this to the activation
   private final List<SimpleString> scaledDownNodeIDs = new ArrayList<>();

   private boolean threadPoolSupplied = false;

   private boolean scheduledPoolSupplied = false;

   private ServiceRegistry serviceRegistry;

   // Constructors
   // ---------------------------------------------------------------------------------

   public HornetQServerImpl()
   {
      this(null, null, null);
   }

   public HornetQServerImpl(final Configuration configuration)
   {
      this(configuration, null, null);
   }

   public HornetQServerImpl(final Configuration configuration, HornetQServer parentServer)
   {
      this(configuration, null, null, parentServer);
   }

   public HornetQServerImpl(final Configuration configuration, final MBeanServer mbeanServer)
   {
      this(configuration, mbeanServer, null);
   }

   public HornetQServerImpl(final Configuration configuration, final HornetQSecurityManager securityManager)
   {
      this(configuration, null, securityManager);
   }

   public HornetQServerImpl(Configuration configuration,
                            MBeanServer mbeanServer,
                            final HornetQSecurityManager securityManager)
   {
      this(configuration, mbeanServer, securityManager, null);
   }

   public HornetQServerImpl(Configuration configuration,
                            MBeanServer mbeanServer,
                            final HornetQSecurityManager securityManager,
                            final HornetQServer parentServer)
   {
      this(configuration, mbeanServer, securityManager, parentServer, null);
   }

   public HornetQServerImpl(Configuration configuration,
                            MBeanServer mbeanServer,
                            final HornetQSecurityManager securityManager,
                            final HornetQServer parentServer,
                            final ServiceRegistry serviceRegistry)
   {
      if (configuration == null)
      {
         configuration = new ConfigurationImpl();
      }
      if (mbeanServer == null)
      {
         // Just use JVM mbean server
         mbeanServer = ManagementFactory.getPlatformMBeanServer();
      }

      // We need to hard code the version information into a source file

      version = VersionLoader.getVersion();

      this.configuration = configuration;

      this.mbeanServer = mbeanServer;

      this.securityManager = securityManager;

      addressSettingsRepository = new HierarchicalObjectRepository<AddressSettings>();

      addressSettingsRepository.setDefault(new AddressSettings());

      securityRepository = new HierarchicalObjectRepository<Set<Role>>();

      securityRepository.setDefault(new HashSet<Role>());

      this.parentServer = parentServer;

      this.serviceRegistry = serviceRegistry == null ?  new ServiceRegistry() : serviceRegistry;
   }

   // life-cycle methods
   // ----------------------------------------------------------------

   /*
    * Can be overridden for tests
    */
   protected NodeManager createNodeManager(final String directory, boolean replicatingBackup)
   {
      NodeManager manager;
      if (!configuration.isPersistenceEnabled())
      {
         manager = new InVMNodeManager(replicatingBackup);
      }
      else if (configuration.getJournalType() == JournalType.ASYNCIO && AsynchronousFileImpl.isLoaded())
      {
         manager = new AIOFileLockNodeManager(directory, replicatingBackup, configuration.getJournalLockAcquisitionTimeout());
      }
      else
      {
         manager = new FileLockNodeManager(directory, replicatingBackup, configuration.getJournalLockAcquisitionTimeout());
      }
      return manager;
   }

   public final synchronized void start() throws Exception
   {
      if (state != SERVER_STATE.STOPPED)
      {
         HornetQServerLogger.LOGGER.debug("Server already started!");
         return;
      }

      state = SERVER_STATE.STARTING;

      if (haPolicy == null)
      {
         haPolicy = ConfigurationUtils.getHAPolicy(configuration.getHAPolicyConfiguration());
      }

      activationLatch.setCount(1);

      HornetQServerLogger.LOGGER.debug("Starting server " + this);

      OperationContextImpl.clearContext();

      try
      {
         checkJournalDirectory();

         nodeManager = createNodeManager(configuration.getJournalDirectory(), false);

         nodeManager.start();

         HornetQServerLogger.LOGGER.serverStarting((haPolicy.isBackup() ? "backup" : "live"), configuration);

         if (configuration.isRunSyncSpeedTest())
         {
            SyncSpeedTest test = new SyncSpeedTest();

            test.run();
         }

         final boolean wasLive = !haPolicy.isBackup();
         if (!haPolicy.isBackup())
         {
            activation = haPolicy.createActivation(this, false, activationParams, shutdownOnCriticalIO);

            activation.run();
         }
         // The activation on fail-back may change the value of isBackup, for that reason we are
         // checking again here
         if (haPolicy.isBackup())
         {
            if (haPolicy.isSharedStore())
            {
               activation = haPolicy.createActivation(this, false, activationParams, shutdownOnCriticalIO);
            }
            else
            {
               activation = haPolicy.createActivation(this, wasLive, activationParams, shutdownOnCriticalIO);
            }

            backupActivationThread = new Thread(activation, HornetQMessageBundle.BUNDLE.activationForServer(this));
            backupActivationThread.start();
         }
         else
         {
            state = SERVER_STATE.STARTED;
            HornetQServerLogger.LOGGER.serverStarted(getVersion().getFullVersion(), nodeManager.getNodeId(),
                                                     identity != null ? identity : "");
         }
         // start connector service
         connectorsService = new ConnectorsService(configuration, storageManager, scheduledPool, postOffice, serviceRegistry);
         connectorsService.start();
      }
      finally
      {
         // this avoids embedded applications using dirty contexts from startup
         OperationContextImpl.clearContext();
      }
   }

   @Override
   protected final void finalize() throws Throwable
   {
      if (state != SERVER_STATE.STOPPED)
      {
         HornetQServerLogger.LOGGER.serverFinalisedWIthoutBeingSTopped();

         stop();
      }

      super.finalize();
   }

   public void setState(SERVER_STATE state)
   {
      this.state = state;
   }

   public SERVER_STATE getState()
   {
      return state;
   }

   public void interrupBackupThread(NodeManager nodeManagerInUse) throws InterruptedException
   {
      long timeout = 30000;

      long start = System.currentTimeMillis();

      while (backupActivationThread.isAlive() && System.currentTimeMillis() - start < timeout)
      {
         if (nodeManagerInUse != null)
         {
            nodeManagerInUse.interrupt();
         }

         backupActivationThread.interrupt();

         backupActivationThread.join(1000);

      }

      if (System.currentTimeMillis() - start >= timeout)
      {
         threadDump("Timed out waiting for backup activation to exit");
      }
   }

   public void resetNodeManager() throws Exception
   {
      nodeManager.stop();
      nodeManager =
            createNodeManager(configuration.getJournalDirectory(), true);
   }

   public Activation getActivation()
   {
      return activation;
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

   public ExecutorService getThreadPool()
   {
      return threadPool;
   }

   public void setActivation(SharedNothingLiveActivation activation)
   {
      this.activation = activation;
   }
   /**
    * Stops the server in a different thread.
    */
   public final void stopTheServer(final boolean criticalIOError)
   {
      ExecutorService executor = Executors.newSingleThreadExecutor();
      executor.submit(new Runnable()
      {
         @Override
         public void run()
         {
            try
            {
               stop(false, criticalIOError, false);
            }
            catch (Exception e)
            {
               HornetQServerLogger.LOGGER.errorStoppingServer(e);
            }
         }
      });
   }

   public final void stop() throws Exception
   {
      stop(false);
   }

   public void addActivationParam(String key, Object val)
   {
      activationParams.put(key, val);
   }
   @Override
   public boolean isAddressBound(String address) throws Exception
   {
      return postOffice.isAddressBound(SimpleString.toSimpleString(address));
   }

   public void threadDump(final String reason)
   {
      StringWriter str = new StringWriter();
      PrintWriter out = new PrintWriter(str);

      Map<Thread, StackTraceElement[]> stackTrace = Thread.getAllStackTraces();

      out.println(HornetQMessageBundle.BUNDLE.generatingThreadDump(reason));
      out.println("*******************************************************************************");

      for (Map.Entry<Thread, StackTraceElement[]> el : stackTrace.entrySet())
      {
         out.println("===============================================================================");
         out.println(HornetQMessageBundle.BUNDLE.threadDump(el.getKey(), el.getKey().getName(), el.getKey().getId(), el.getKey().getThreadGroup()));
         out.println();
         for (StackTraceElement traceEl : el.getValue())
         {
            out.println(traceEl);
         }
      }

      out.println("===============================================================================");
      out.println(HornetQMessageBundle.BUNDLE.endThreadDump());
      out.println("*******************************************************************************");

      HornetQServerLogger.LOGGER.warn(str.toString());
   }

   public final void stop(boolean failoverOnServerShutdown) throws Exception
   {
      stop(failoverOnServerShutdown, false, false);
   }

   /**
    * Stops the server
    * @param criticalIOError          whether we have encountered an IO error with the journal etc
    */
   void stop(boolean failoverOnServerShutdown, final boolean criticalIOError, boolean restarting) throws Exception
   {
      synchronized (this)
      {
         if (state == SERVER_STATE.STOPPED || state == SERVER_STATE.STOPPING)
         {
            return;
         }
         state = SERVER_STATE.STOPPING;

         activation.sendLiveIsStopping();

         stopComponent(connectorsService);

         // we stop the groupingHandler before we stop the cluster manager so binding mappings
         // aren't removed in case of failover
         if (groupingHandler != null)
         {
            managementService.removeNotificationListener(groupingHandler);
            groupingHandler.stop();
         }
         stopComponent(clusterManager);

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

      // Stop the deployers
      if (configuration.isFileDeploymentEnabled())
      {
         stopComponent(basicUserCredentialsDeployer);
         stopComponent(addressSettingsDeployer);
         stopComponent(queueDeployer);
         stopComponent(securityDeployer);
         stopComponent(deploymentManager);
      }

      stopComponent(backupManager);
      activation.preStorageClose();
      stopComponent(pagingManager);

      if (storageManager != null)
         storageManager.stop(criticalIOError);

      // We stop remotingService before otherwise we may lock the system in case of a critical IO
      // error shutdown
      if (remotingService != null)
         remotingService.stop(criticalIOError);

      // Stop the management service after the remoting service to ensure all acceptors are deregistered with JMX
      if (managementService != null)
         managementService.unregisterServer();
      stopComponent(managementService);

      stopComponent(securityManager);
      stopComponent(resourceManager);

      stopComponent(postOffice);

      if (scheduledPool != null && !scheduledPoolSupplied)
      {
         // we just interrupt all running tasks, these are supposed to be pings and the like.
         scheduledPool.shutdownNow();
      }

      stopComponent(memoryManager);

      if (threadPool != null && !threadPoolSupplied)
      {
         threadPool.shutdown();
         try
         {
            if (!threadPool.awaitTermination(10, TimeUnit.SECONDS))
            {
               HornetQServerLogger.LOGGER.timedOutStoppingThreadpool(threadPool);
               for (Runnable r : threadPool.shutdownNow())
               {
                  HornetQServerLogger.LOGGER.debug("Cancelled the execution of " + r);
               }
            }
         }
         catch (InterruptedException e)
         {
            // Ignore
         }
      }

      if (!threadPoolSupplied) threadPool = null;
      if (!scheduledPoolSupplied) scheduledPool = null;

      if (securityStore != null)
         securityStore.stop();

      pagingManager = null;
      securityStore = null;
      resourceManager = null;
      postOffice = null;
      queueFactory = null;
      resourceManager = null;
      messagingServerControl = null;
      memoryManager = null;
      backupManager = null;

      sessions.clear();

      state = SERVER_STATE.STOPPED;

      activationLatch.setCount(1);

      // to display in the log message
      SimpleString tempNodeID = getNodeID();
      if (activation != null)
      {
         activation.close(failoverOnServerShutdown, restarting);
      }
      if (backupActivationThread != null)
      {

         backupActivationThread.join(30000);
         if (backupActivationThread.isAlive())
         {
            HornetQServerLogger.LOGGER.backupActivationDidntFinish(this);
            backupActivationThread.interrupt();
         }
      }

      stopComponent(nodeManager);

      nodeManager = null;

      addressSettingsRepository.clearListeners();

      addressSettingsRepository.clearCache();

      scaledDownNodeIDs.clear();

      if (identity != null)
      {
         HornetQServerLogger.LOGGER.serverStopped("identity=" + identity + ",version=" + getVersion().getFullVersion(),
                                                  tempNodeID);
      }
      else
      {
         HornetQServerLogger.LOGGER.serverStopped(getVersion().getFullVersion(), tempNodeID);
      }
   }



   public boolean checkLiveIsNotColocated(String nodeId)
   {
      if (parentServer == null)
      {
         return true;
      }
      else
      {
         return !parentServer.getNodeID().toString().equals(nodeId);
      }
   }

   /**
    * Freeze all connections.
    * <p/>
    * If replicating, avoid freezing the replication connection. Helper method for
    * {@link #stop(boolean, boolean, boolean)}.
    */
   private void freezeConnections()
   {
      activation.freezeConnections(remotingService);

      // after disconnecting all the clients close all the server sessions so any messages in delivery will be cancelled back to the queue
      for (ServerSession serverSession : sessions.values())
      {
         try
         {
            serverSession.close(true);
         }
         catch (Exception e)
         {
            e.printStackTrace();
         }
      }
   }

   /**
    * We close all the exception in an attempt to let any pending IO to finish to avoid scenarios
    * where the send or ACK got to disk but the response didn't get to the client It may still be
    * possible to have this scenario on a real failure (without the use of XA) But at least we will
    * do our best to avoid it on regular shutdowns
    */
   private void closeAllServerSessions(final boolean criticalIOError)
   {
      if (state != SERVER_STATE.STOPPING)
      {
         return;
      }
      for (ServerSession session : sessions.values())
      {
         try
         {
            session.close(true);
         }
         catch (Exception e)
         {
            // If anything went wrong with closing sessions.. we should ignore it
            // such as transactions.. etc.
            HornetQServerLogger.LOGGER.errorClosingSessionsWhileStoppingServer(e);
         }
      }
      if (!criticalIOError)
      {
         for (ServerSession session : sessions.values())
         {
            try
            {
               session.waitContextCompletion();
            }
            catch (Exception e)
            {
               HornetQServerLogger.LOGGER.errorClosingSessionsWhileStoppingServer(e);
            }
         }
      }

   }

   static void stopComponent(HornetQComponent component) throws Exception
   {
      if (component != null)
         component.stop();
   }

   // HornetQServer implementation
   // -----------------------------------------------------------

   public String describe()
   {
      StringWriter str = new StringWriter();
      PrintWriter out = new PrintWriter(str);

      out.println(HornetQMessageBundle.BUNDLE.serverDescribe(identity, getClusterManager().describe()));

      return str.toString();
   }

   public String destroyConnectionWithSessionMetadata(String metaKey, String parameterValue) throws Exception
   {
      StringBuffer operationsExecuted = new StringBuffer();

      try
      {
         operationsExecuted.append("**************************************************************************************************\n");
         operationsExecuted.append(HornetQMessageBundle.BUNDLE.destroyConnectionWithSessionMetadataHeader(metaKey, parameterValue) + "\n");

         Set<ServerSession> allSessions = getSessions();

         ServerSession sessionFound = null;
         for (ServerSession session : allSessions)
         {
            try
            {
               String value = session.getMetaData(metaKey);
               if (value != null && value.equals(parameterValue))
               {
                  sessionFound = session;
                  operationsExecuted.append(HornetQMessageBundle.BUNDLE.destroyConnectionWithSessionMetadataClosingConnection(sessionFound.toString()) + "\n");
                  RemotingConnection conn = session.getRemotingConnection();
                  if (conn != null)
                  {
                     conn.fail(HornetQMessageBundle.BUNDLE.destroyConnectionWithSessionMetadataSendException(metaKey, parameterValue));
                  }
                  session.close(true);
                  sessions.remove(session.getName());
               }
            }
            catch (Throwable e)
            {
               HornetQServerLogger.LOGGER.warn(e.getMessage(), e);
            }
         }

         if (sessionFound == null)
         {
            operationsExecuted.append(HornetQMessageBundle.BUNDLE.destroyConnectionWithSessionMetadataNoSessionFound(metaKey, parameterValue) + "\n");
         }

         operationsExecuted.append("**************************************************************************************************");

         return operationsExecuted.toString();
      }
      finally
      {
         // This operation is critical for the knowledge of the admin, so we need to add info logs for later knowledge
         HornetQServerLogger.LOGGER.info(operationsExecuted.toString());
      }

   }

   public void setIdentity(String identity)
   {
      this.identity = identity;
   }

   public String getIdentity()
   {
      return identity;
   }

   public ScheduledExecutorService getScheduledPool()
   {
      return scheduledPool;
   }

   public Configuration getConfiguration()
   {
      return configuration;
   }

   public PagingManager getPagingManager()
   {
      return pagingManager;
   }

   public RemotingService getRemotingService()
   {
      return remotingService;
   }

   public StorageManager getStorageManager()
   {
      return storageManager;
   }

   public HornetQSecurityManager getSecurityManager()
   {
      return securityManager;
   }

   public ManagementService getManagementService()
   {
      return managementService;
   }

   public HierarchicalRepository<Set<Role>> getSecurityRepository()
   {
      return securityRepository;
   }

   public NodeManager getNodeManager()
   {
      return nodeManager;
   }

   public HierarchicalRepository<AddressSettings> getAddressSettingsRepository()
   {
      return addressSettingsRepository;
   }

   public DeploymentManager getDeploymentManager()
   {
      return deploymentManager;
   }

   public ResourceManager getResourceManager()
   {
      return resourceManager;
   }

   public Version getVersion()
   {
      return version;
   }

   public boolean isStarted()
   {
      return state == SERVER_STATE.STARTED;
   }

   public ClusterManager getClusterManager()
   {
      return clusterManager;
   }

   public BackupManager getBackupManager()
   {
      return backupManager;
   }

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
                                      final ServerSessionFactory sessionFactory) throws Exception
   {

      if (securityStore != null)
      {
         securityStore.authenticate(username, password);
      }
      final OperationContext context = storageManager.newContext(getExecutorFactory().getExecutor());
      final ServerSessionImpl session = internalCreateSession(name, username, password, minLargeMessageSize, connection, autoCommitSends, autoCommitAcks, preAcknowledge, xa, defaultAddress, callback, context, sessionFactory);

      sessions.put(name, session);

      return session;
   }

   protected ServerSessionImpl internalCreateSession(String name, String username, String password, int minLargeMessageSize, RemotingConnection connection, boolean autoCommitSends, boolean autoCommitAcks, boolean preAcknowledge, boolean xa, String defaultAddress, SessionCallback callback, OperationContext context, ServerSessionFactory sessionFactory) throws Exception
   {
      if (sessionFactory == null)
      {
         return new ServerSessionImpl(name,
                                   username,
                                   password,
                                   minLargeMessageSize,
                                   autoCommitSends,
                                   autoCommitAcks,
                                   preAcknowledge,
                                   configuration.isPersistDeliveryCountBeforeDelivery(),
                                   xa,
                                   connection,
                                   storageManager,
                                   postOffice,
                                   resourceManager,
                                   securityStore,
                                   managementService,
                                   this,
                                   configuration.getManagementAddress(),
                                   defaultAddress == null ? null
                                      : new SimpleString(defaultAddress),
                                   callback,
                                   context);
      }
      else
      {
         return sessionFactory.createCoreSession(name,
                                   username,
                                   password,
                                   minLargeMessageSize,
                                   autoCommitSends,
                                   autoCommitAcks,
                                   preAcknowledge,
                                   configuration.isPersistDeliveryCountBeforeDelivery(),
                                   xa,
                                   connection,
                                   storageManager,
                                   postOffice,
                                   resourceManager,
                                   securityStore,
                                   managementService,
                                   this,
                                   configuration.getManagementAddress(),
                                   defaultAddress == null ? null
                                      : new SimpleString(defaultAddress),
                                   callback,
                                   context);
      }
   }

   @Override
   public SecurityStore getSecurityStore()
   {
      return securityStore;
   }

   public void removeSession(final String name) throws Exception
   {
      sessions.remove(name);
   }

   public ServerSession lookupSession(String key, String value)
   {
      // getSessions is called here in a try to minimize locking the Server while this check is being done
      Set<ServerSession> allSessions = getSessions();

      for (ServerSession session : allSessions)
      {
         String metaValue = session.getMetaData(key);
         if (metaValue != null && metaValue.equals(value))
         {
            return session;
         }
      }

      return null;
   }

   public synchronized List<ServerSession> getSessions(final String connectionID)
   {
      Set<Entry<String, ServerSession>> sessionEntries = sessions.entrySet();
      List<ServerSession> matchingSessions = new ArrayList<ServerSession>();
      for (Entry<String, ServerSession> sessionEntry : sessionEntries)
      {
         ServerSession serverSession = sessionEntry.getValue();
         if (serverSession.getConnectionID().toString().equals(connectionID))
         {
            matchingSessions.add(serverSession);
         }
      }
      return matchingSessions;
   }

   public synchronized Set<ServerSession> getSessions()
   {
      return new HashSet<ServerSession>(sessions.values());
   }

   @Override
   public boolean isActive()
   {
      return activationLatch.getCount() < 1;
   }

   @Override
   public boolean waitForActivation(long timeout, TimeUnit unit) throws InterruptedException
   {
      return activationLatch.await(timeout, unit);
   }


   public HornetQServerControlImpl getHornetQServerControl()
   {
      return messagingServerControl;
   }

   public int getConnectionCount()
   {
      return remotingService.getConnections().size();
   }

   public PostOffice getPostOffice()
   {
      return postOffice;
   }

   public QueueFactory getQueueFactory()
   {
      return queueFactory;
   }

   public SimpleString getNodeID()
   {
      return nodeManager == null ? null : nodeManager.getNodeId();
   }

   public Queue createQueue(final SimpleString address,
                            final SimpleString queueName,
                            final SimpleString filterString,
                            final boolean durable,
                            final boolean temporary) throws Exception
   {
      return createQueue(address, queueName, filterString, durable, temporary, false, false);
   }


   /**
    * Creates a transient queue. A queue that will exist as long as there are consumers.
    * The queue will be deleted as soon as all the consumers are removed.
    * <p/>
    * Notice: the queue won't be deleted until the first consumer arrives.
    *
    * @param address
    * @param name
    * @param filterString
    * @param durable
    * @throws Exception
    */
   public void createSharedQueue(final SimpleString address,
                                 final SimpleString name,
                                 final SimpleString filterString,
                                 boolean durable) throws Exception
   {
      Queue queue = createQueue(address, name, filterString, durable, !durable, true, !durable);

      if (!queue.getAddress().equals(address))
      {
         throw HornetQMessageBundle.BUNDLE.queueSubscriptionBelongsToDifferentAddress(name);
      }

      if (filterString != null && (queue.getFilter() == null || !queue.getFilter().getFilterString().equals(filterString)) ||
         filterString == null && queue.getFilter() != null)
      {
         throw HornetQMessageBundle.BUNDLE.queueSubscriptionBelongsToDifferentFilter(name);
      }

      if (HornetQServerLogger.LOGGER.isDebugEnabled())
      {
         HornetQServerLogger.LOGGER.debug("Transient Queue " + name + " created on address " + name +
                                             " with filter=" + filterString);
      }

   }


   public Queue locateQueue(SimpleString queueName) throws Exception
   {
      Binding binding = postOffice.getBinding(queueName);

      if (binding == null)
      {
         return null;
      }

      Bindable queue = binding.getBindable();

      if (!(queue instanceof Queue))
      {
         throw new IllegalStateException("locateQueue should only be used to locate queues");
      }

      return (Queue) binding.getBindable();
   }

   public Queue deployQueue(final SimpleString address,
                            final SimpleString queueName,
                            final SimpleString filterString,
                            final boolean durable,
                            final boolean temporary) throws Exception
   {
      HornetQServerLogger.LOGGER.deployQueue(queueName);

      return createQueue(address, queueName, filterString, durable, temporary, true, false);
   }

   public void destroyQueue(final SimpleString queueName) throws Exception
   {
      // The session is passed as an argument to verify if the user has authorization to delete the queue
      // in some cases (such as temporary queues) this should happen regardless of the authorization
      // since that will only happen during a session close, which will be used to cleanup on temporary queues
      destroyQueue(queueName, null, true);
   }

   public void destroyQueue(final SimpleString queueName, final ServerSession session) throws Exception
   {
      destroyQueue(queueName, session, true);
   }

   public void destroyQueue(final SimpleString queueName, final ServerSession session, final boolean checkConsumerCount) throws Exception
   {
      destroyQueue(queueName, session, checkConsumerCount, false);
   }

   public void destroyQueue(final SimpleString queueName, final ServerSession session, final boolean checkConsumerCount, final boolean removeConsumers) throws Exception
   {
      addressSettingsRepository.clearCache();

      Binding binding = postOffice.getBinding(queueName);

      if (binding == null)
      {
         throw HornetQMessageBundle.BUNDLE.noSuchQueue(queueName);
      }

      Queue queue = (Queue) binding.getBindable();

      // This check is only valid if checkConsumerCount == true
      if (checkConsumerCount && queue.getConsumerCount() != 0)
      {
         throw HornetQMessageBundle.BUNDLE.cannotDeleteQueue(queue.getName(), queueName, binding.getClass().getName());
      }

      if (session != null)
      {

         if (queue.isDurable())
         {
            // make sure the user has privileges to delete this queue
            securityStore.check(binding.getAddress(), CheckType.DELETE_DURABLE_QUEUE, session);
         }
         else
         {
            securityStore.check(binding.getAddress(), CheckType.DELETE_NON_DURABLE_QUEUE, session);
         }
      }

      queue.deleteQueue(removeConsumers);
   }


   public void registerActivateCallback(final ActivateCallback callback)
   {
      activateCallbacks.add(callback);
   }

   public void unregisterActivateCallback(final ActivateCallback callback)
   {
      activateCallbacks.remove(callback);
   }

   public ExecutorFactory getExecutorFactory()
   {
      return executorFactory;
   }

   public void setGroupingHandler(final GroupingHandler groupingHandler)
   {
      if (this.groupingHandler != null && managementService != null)
      {
         // Removing old groupNotification
         managementService.removeNotificationListener(this.groupingHandler);
      }
      this.groupingHandler = groupingHandler;
      if (managementService != null)
      {
         managementService.addNotificationListener(this.groupingHandler);
      }

   }

   public GroupingHandler getGroupingHandler()
   {
      return groupingHandler;
   }

   public ReplicationManager getReplicationManager()
   {
      return activation.getReplicationManager();
   }

   public ConnectorsService getConnectorsService()
   {
      return connectorsService;
   }

   public void deployDivert(DivertConfiguration config) throws Exception
   {
      if (config.getName() == null)
      {
         HornetQServerLogger.LOGGER.divertWithNoName();

         return;
      }

      if (config.getAddress() == null)
      {
         HornetQServerLogger.LOGGER.divertWithNoAddress();

         return;
      }

      if (config.getForwardingAddress() == null)
      {
         HornetQServerLogger.LOGGER.divertWithNoForwardingAddress();

         return;
      }

      SimpleString sName = new SimpleString(config.getName());

      if (postOffice.getBinding(sName) != null)
      {
         HornetQServerLogger.LOGGER.divertBindingNotExists(sName);

         return;
      }

      SimpleString sAddress = new SimpleString(config.getAddress());

      Transformer transformer = instantiateTransformer(config.getTransformerClassName());

      Filter filter = FilterImpl.createFilter(config.getFilterString());

      Divert divert = new DivertImpl(new SimpleString(config.getForwardingAddress()),
                                     sName,
                                     new SimpleString(config.getRoutingName()),
                                     config.isExclusive(),
                                     filter,
                                     transformer,
                                     postOffice,
                                     storageManager);

      Binding binding = new DivertBinding(storageManager.generateID(), sAddress, divert);

      postOffice.addBinding(binding);

      managementService.registerDivert(divert, config);
   }

   public void destroyDivert(SimpleString name) throws Exception
   {
      Binding binding = postOffice.getBinding(name);
      if (binding == null)
      {
         throw HornetQMessageBundle.BUNDLE.noBindingForDivert(name);
      }
      if (!(binding instanceof DivertBinding))
      {
         throw HornetQMessageBundle.BUNDLE.bindingNotDivert(name);
      }

      postOffice.removeBinding(name, null);
   }

   public void deployBridge(BridgeConfiguration config) throws Exception
   {
      if (clusterManager != null)
      {
         clusterManager.deployBridge(config);
      }
   }

   public void destroyBridge(String name) throws Exception
   {
      if (clusterManager != null)
      {
         clusterManager.destroyBridge(name);
      }
   }

   public ServerSession getSessionByID(String sessionName)
   {
      return sessions.get(sessionName);
   }

   // PUBLIC -------

   @Override
   public String toString()
   {
      if (identity != null)
      {
         return "HornetQServerImpl::" + identity;
      }
      return "HornetQServerImpl::" + (nodeManager != null ? "serverUUID=" + nodeManager.getUUID() : "");
   }

   /**
    * For tests only, don't use this method as it's not part of the API
    *
    * @param factory
    */
   public void replaceQueueFactory(QueueFactory factory)
   {
      this.queueFactory = factory;
   }


   private PagingManager createPagingManager()
   {

      return new PagingManagerImpl(new PagingStoreFactoryNIO(storageManager, configuration.getPagingDirectory(),
                                                             configuration.getJournalBufferTimeout_NIO(),
                                                             scheduledPool,
                                                             executorFactory,
                                                             configuration.isJournalSyncNonTransactional(),
                                                             shutdownOnCriticalIO),
                                   addressSettingsRepository);
   }

   /**
    * This method is protected as it may be used as a hook for creating a custom storage manager (on tests for instance)
    */
   private StorageManager createStorageManager()
   {
      if (configuration.isPersistenceEnabled())
      {
         return new JournalStorageManager(configuration, executorFactory, shutdownOnCriticalIO);
      }
      return new NullStorageManager();
   }

   private void callActivateCallbacks()
   {
      for (ActivateCallback callback : activateCallbacks)
      {
         callback.activated();
      }
   }

   private void callPreActiveCallbacks()
   {
      for (ActivateCallback callback : activateCallbacks)
      {
         callback.preActivate();
      }
   }

   private void callDeActiveCallbacks()
   {
      for (ActivateCallback callback : activateCallbacks)
      {
         try
         {
            callback.deActivate();
         }
         catch (Throwable e)
         {
            // https://bugzilla.redhat.com/show_bug.cgi?id=1009530:
            // we won't interrupt the shutdown sequence because of a failed callback here
            HornetQServerLogger.LOGGER.warn(e.getMessage(), e);
         }
      }
   }

   private void callActivationCompleteCallbacks()
   {
      for (ActivateCallback callback : activateCallbacks)
      {
         callback.activationComplete();
      }
   }

   /**
    * Sets up HornetQ Executor Services.
    */
   private void initializeExecutorServices()
   {
      /* We check to see if a Thread Pool is supplied in the InjectedObjectRegistry.  If so we created a new Ordered
       * Executor based on the provided Thread pool.  Otherwise we create a new ThreadPool.
       */
      if (serviceRegistry.getExecutorService() == null)
      {
         ThreadFactory tFactory = new HornetQThreadFactory("HornetQ-server-" + this.toString(), false, getThisClassLoader());
         if (configuration.getThreadPoolMaxSize() == -1)
         {
            threadPool = Executors.newCachedThreadPool(tFactory);
         }
         else
         {
            threadPool = Executors.newFixedThreadPool(configuration.getThreadPoolMaxSize(), tFactory);
         }
      }
      else
      {
         threadPool = serviceRegistry.getExecutorService();
         this.threadPoolSupplied = true;
      }
      this.executorFactory = new OrderedExecutorFactory(threadPool);

       /* We check to see if a Scheduled Executor Service is provided in the InjectedObjectRegistry.  If so we use this
       * Scheduled ExecutorService otherwise we create a new one.
       */
      if (serviceRegistry.getScheduledExecutorService() == null)
      {
         ThreadFactory tFactory = new HornetQThreadFactory("HornetQ-scheduled-threads", false, getThisClassLoader());
         scheduledPool = new ScheduledThreadPoolExecutor(configuration.getScheduledThreadPoolMaxSize(), tFactory);
      }
      else
      {
         this.scheduledPoolSupplied = true;
         this.scheduledPool = serviceRegistry.getScheduledExecutorService();
      }
   }

   public ServiceRegistry getServiceRegistry()
   {
      return serviceRegistry;
   }

   /**
    * Starts everything apart from RemotingService and loading the data.
    * <p/>
    * After optional intermediary steps, Part 1 is meant to be followed by part 2
    * {@link #initialisePart2(boolean)}.
    * @param scalingDown
    */
   synchronized boolean initialisePart1(boolean scalingDown) throws Exception
   {
      if (state == SERVER_STATE.STOPPED)
         return false;

      // Create the pools - we have two pools - one for non scheduled - and another for scheduled
      initializeExecutorServices();

      if (configuration.getJournalType() == JournalType.ASYNCIO && !AIOSequentialFileFactory.isSupported())
      {
         HornetQServerLogger.LOGGER.switchingNIO();
         configuration.setJournalType(JournalType.NIO);
      }

      managementService = new ManagementServiceImpl(mbeanServer, configuration);

      if (configuration.getMemoryMeasureInterval() != -1)
      {
         memoryManager = new MemoryManager(configuration.getMemoryWarningThreshold(),
                                           configuration.getMemoryMeasureInterval());

         memoryManager.start();
      }

      // Create the hard-wired components

      if (configuration.isFileDeploymentEnabled())
      {
         deploymentManager = new FileDeploymentManager(configuration.getFileDeployerScanPeriod());
      }

      callPreActiveCallbacks();

      // startReplication();

      storageManager = createStorageManager();

      if (HornetQDefaultConfiguration.getDefaultClusterUser().equals(configuration.getClusterUser()) && HornetQDefaultConfiguration.getDefaultClusterPassword().equals(configuration.getClusterPassword()))
      {
         HornetQServerLogger.LOGGER.clusterSecurityRisk();
      }

      securityStore = new SecurityStoreImpl(securityRepository,
                                            securityManager,
                                            configuration.getSecurityInvalidationInterval(),
                                            configuration.isSecurityEnabled(),
                                            configuration.getClusterUser(),
                                            configuration.getClusterPassword(),
                                            managementService);

      queueFactory = new QueueFactoryImpl(executorFactory, scheduledPool, addressSettingsRepository, storageManager);

      pagingManager = createPagingManager();

      resourceManager = new ResourceManagerImpl((int) (configuration.getTransactionTimeout() / 1000),
                                                configuration.getTransactionTimeoutScanPeriod(),
                                                scheduledPool);
      postOffice = new PostOfficeImpl(this,
                                      storageManager,
                                      pagingManager,
                                      queueFactory,
                                      managementService,
                                      configuration.getMessageExpiryScanPeriod(),
                                      configuration.getMessageExpiryThreadPriority(),
                                      configuration.isWildcardRoutingEnabled(),
                                      configuration.getIDCacheSize(),
                                      configuration.isPersistIDCache(),
                                      addressSettingsRepository);

      // This can't be created until node id is set
      clusterManager =
         new ClusterManager(executorFactory, this, postOffice, scheduledPool, managementService, configuration,
                            nodeManager, haPolicy.isBackup());

      backupManager = new BackupManager(this, executorFactory, scheduledPool, nodeManager, configuration, clusterManager);

      clusterManager.deploy();

      remotingService = new RemotingServiceImpl(clusterManager,
                                                configuration,
                                                this,
                                                managementService,
                                                scheduledPool,
                                                protocolManagerFactories,
                                                executorFactory.getExecutor(),
                                                serviceRegistry);

      messagingServerControl = managementService.registerServer(postOffice,
                                                                storageManager,
                                                                configuration,
                                                                addressSettingsRepository,
                                                                securityRepository,
                                                                resourceManager,
                                                                remotingService,
                                                                this,
                                                                queueFactory,
                                                                scheduledPool,
                                                                pagingManager,
                                                                haPolicy.isBackup());

      // Address settings need to deployed initially, since they're require on paging manager.start()

      if (!scalingDown)
      {
         if (configuration.isFileDeploymentEnabled())
         {
            addressSettingsDeployer = new AddressSettingsDeployer(deploymentManager, addressSettingsRepository);

            addressSettingsDeployer.start();
         }

         deployAddressSettingsFromConfiguration();
      }

      storageManager.start();


      if (securityManager != null)
      {
         securityManager.start();
      }

      postOffice.start();

      pagingManager.start();

      managementService.start();

      resourceManager.start();

      // Deploy all security related config
      if (configuration.isFileDeploymentEnabled())
      {
         basicUserCredentialsDeployer = new BasicUserCredentialsDeployer(deploymentManager, securityManager);

         basicUserCredentialsDeployer.start();

         if (securityManager != null)
         {
            securityDeployer = new SecurityDeployer(deploymentManager, securityRepository);

            securityDeployer.start();
         }
      }

      deploySecurityFromConfiguration();

      deployGroupingHandlerConfiguration(configuration.getGroupingHandlerConfiguration());

      return true;
   }

   /*
    * Load the data, and start remoting service so clients can connect
    */
   synchronized void initialisePart2(boolean scalingDown) throws Exception
   {
      // Load the journal and populate queues, transactions and caches in memory

      if (state == SERVER_STATE.STOPPED || state == SERVER_STATE.STOPPING)
      {
         return;
      }

      pagingManager.reloadStores();

      JournalLoadInformation[] journalInfo = loadJournals();


      final ServerInfo dumper = new ServerInfo(this, pagingManager);

      long dumpInfoInterval = configuration.getServerDumpInterval();

      if (dumpInfoInterval > 0)
      {
         scheduledPool.scheduleWithFixedDelay(new Runnable()
         {
            public void run()
            {
               HornetQServerLogger.LOGGER.dumpServerInfo(dumper.dump());
            }
         }, 0, dumpInfoInterval, TimeUnit.MILLISECONDS);
      }

      // Deploy the rest of the stuff

      // Deploy any predefined queues
      if (configuration.isFileDeploymentEnabled())
      {
         queueDeployer = new QueueDeployer(deploymentManager, this);

         queueDeployer.start();
      }
      else
      {
         deployQueuesFromConfiguration();
      }


      // We need to call this here, this gives any dependent server a chance to deploy its own addresses
      // this needs to be done before clustering is fully activated
      callActivateCallbacks();

      if (!scalingDown)
      {
         // Deploy any pre-defined diverts
         deployDiverts();

         if (groupingHandler != null)
         {
            groupingHandler.start();
         }

      // We do this at the end - we don't want things like MDBs or other connections connecting to a backup server until
      // it is activated

         if (groupingHandler != null && groupingHandler instanceof LocalGroupingHandler)
         {
            clusterManager.start();

            groupingHandler.awaitBindings();

            remotingService.start();
         }
         else
         {
            remotingService.start();

            clusterManager.start();
         }

         if (nodeManager.getNodeId() == null)
         {
            throw HornetQMessageBundle.BUNDLE.nodeIdNull();
         }
         activationLatch.countDown();

         // We can only do this after everything is started otherwise we may get nasty races with expired messages
         postOffice.startExpiryScanner();
      }
      else
      {
         activationLatch.countDown();
      }

      callActivationCompleteCallbacks();
   }

   private void deploySecurityFromConfiguration()
   {
      for (Map.Entry<String, Set<Role>> entry : configuration.getSecurityRoles().entrySet())
      {
         securityRepository.addMatch(entry.getKey(), entry.getValue(), true);
      }
   }

   private void deployQueuesFromConfiguration() throws Exception
   {
      for (CoreQueueConfiguration config : configuration.getQueueConfigurations())
      {
         deployQueue(SimpleString.toSimpleString(config.getAddress()),
                     SimpleString.toSimpleString(config.getName()),
                     SimpleString.toSimpleString(config.getFilterString()),
                     config.isDurable(),
                     false);
      }
   }

   private void deployAddressSettingsFromConfiguration()
   {
      for (Map.Entry<String, AddressSettings> entry : configuration.getAddressesSettings().entrySet())
      {
         addressSettingsRepository.addMatch(entry.getKey(), entry.getValue(), true);
      }
   }

   private JournalLoadInformation[] loadJournals() throws Exception
   {
      JournalLoader journalLoader = activation.createJournalLoader(postOffice,
            pagingManager,
            storageManager,
            queueFactory,
            nodeManager,
            managementService,
            groupingHandler,
            configuration,
            parentServer);

      JournalLoadInformation[] journalInfo = new JournalLoadInformation[2];

      List<QueueBindingInfo> queueBindingInfos = new ArrayList();

      List<GroupingInfo> groupingInfos = new ArrayList();

      journalInfo[0] = storageManager.loadBindingJournal(queueBindingInfos, groupingInfos);

      recoverStoredConfigs();

      Map<Long, QueueBindingInfo> queueBindingInfosMap = new HashMap();


      journalLoader.initQueues(queueBindingInfosMap, queueBindingInfos);

      journalLoader.handleGroupingBindings(groupingInfos);

      Map<SimpleString, List<Pair<byte[], Long>>> duplicateIDMap = new HashMap<SimpleString, List<Pair<byte[], Long>>>();

      HashSet<Pair<Long, Long>> pendingLargeMessages = new HashSet<Pair<Long, Long>>();

      List<PageCountPending> pendingNonTXPageCounter = new LinkedList<PageCountPending>();

      journalInfo[1] = storageManager.loadMessageJournal(postOffice,
                                                         pagingManager,
                                                         resourceManager,
                                                         queueBindingInfosMap,
                                                         duplicateIDMap,
                                                         pendingLargeMessages,
                                                         pendingNonTXPageCounter,
                                                         journalLoader);

      journalLoader.handleDuplicateIds(duplicateIDMap);

      for (Pair<Long, Long> msgToDelete : pendingLargeMessages)
      {
         HornetQServerLogger.LOGGER.deletingPendingMessage(msgToDelete);
         LargeServerMessage msg = storageManager.createLargeMessage();
         msg.setMessageID(msgToDelete.getB());
         msg.setPendingRecordID(msgToDelete.getA());
         msg.setDurable(true);
         msg.deleteFile();
      }

      if (pendingNonTXPageCounter.size() != 0)
      {
         try
         {
            journalLoader.recoverPendingPageCounters(pendingNonTXPageCounter);
         }
         catch (Throwable e)
         {
            HornetQServerLogger.LOGGER.errorRecoveringPageCounter(e);
         }
      }

      journalLoader.cleanUp();

      return journalInfo;
   }


   /**
    * @throws Exception
    */
   private void recoverStoredConfigs() throws Exception
   {
      List<PersistedAddressSetting> adsettings = storageManager.recoverAddressSettings();
      for (PersistedAddressSetting set : adsettings)
      {
         addressSettingsRepository.addMatch(set.getAddressMatch().toString(), set.getSetting());
      }

      List<PersistedRoles> roles = storageManager.recoverPersistedRoles();

      for (PersistedRoles roleItem : roles)
      {
         Set<Role> setRoles = SecurityFormatter.createSecurity(roleItem.getSendRoles(),
                                                               roleItem.getConsumeRoles(),
                                                               roleItem.getCreateDurableQueueRoles(),
                                                               roleItem.getDeleteDurableQueueRoles(),
                                                               roleItem.getCreateNonDurableQueueRoles(),
                                                               roleItem.getDeleteNonDurableQueueRoles(),
                                                               roleItem.getManageRoles());

         securityRepository.addMatch(roleItem.getAddressMatch().toString(), setRoles);
      }
   }

   private Queue createQueue(final SimpleString address,
                             final SimpleString queueName,
                             final SimpleString filterString,
                             final boolean durable,
                             final boolean temporary,
                             final boolean ignoreIfExists,
                             final boolean transientQueue) throws Exception
   {
      QueueBinding binding = (QueueBinding) postOffice.getBinding(queueName);

      if (binding != null)
      {
         if (ignoreIfExists)
         {
            return binding.getQueue();
         }
         else
         {
            throw HornetQMessageBundle.BUNDLE.queueAlreadyExists(queueName);
         }
      }

      Filter filter = FilterImpl.createFilter(filterString);

      long txID = storageManager.generateID();
      long queueID = storageManager.generateID();

      PageSubscription pageSubscription;

      if (filterString != null && filterString.toString().equals(GENERIC_IGNORED_FILTER))
      {
         pageSubscription = null;
      }
      else
      {
         pageSubscription = pagingManager.getPageStore(address)
            .getCursorProvider()
            .createSubscription(queueID, filter, durable);
      }

      final Queue queue = queueFactory.createQueue(queueID,
                                                   address,
                                                   queueName,
                                                   filter,
                                                   pageSubscription,
                                                   durable,
                                                   temporary);

      if (transientQueue)
      {
         queue.setConsumersRefCount(this);
      }

      binding = new LocalQueueBinding(address, queue, nodeManager.getNodeId());

      if (durable)
      {
         storageManager.addQueueBinding(txID, binding);
      }

      try
      {
         postOffice.addBinding(binding);
         if (durable)
         {
            storageManager.commitBindings(txID);
         }
      }
      catch (Exception e)
      {
         try
         {
            if (durable)
            {
               storageManager.rollbackBindings(txID);
            }
            if (queue != null)
            {
               queue.close();
            }
            if (pageSubscription != null)
            {
               pageSubscription.destroy();
            }
         }
         catch (Throwable ignored)
         {
            HornetQServerLogger.LOGGER.debug(ignored.getMessage(), ignored);
         }
         throw e;
      }


      managementService.registerAddress(address);
      managementService.registerQueue(queue, address, storageManager);

      return queue;
   }

   private void deployDiverts() throws Exception
   {
      for (DivertConfiguration config : configuration.getDivertConfigurations())
      {
         deployDivert(config);
      }
   }

   private void deployGroupingHandlerConfiguration(final GroupingHandlerConfiguration config) throws Exception
   {
      if (config != null)
      {
         GroupingHandler groupingHandler1;
         if (config.getType() == GroupingHandlerConfiguration.TYPE.LOCAL)
         {
            groupingHandler1 =
               new LocalGroupingHandler(executorFactory,
                                        scheduledPool,
                                        managementService,
                                        config.getName(),
                                        config.getAddress(),
                                        getStorageManager(),
                                        config.getTimeout(),
                                        config.getGroupTimeout(),
                                        config.getReaperPeriod());
         }
         else
         {
            groupingHandler1 =
                     new RemoteGroupingHandler(executorFactory, managementService,
                        config.getName(),
                        config.getAddress(),
                        config.getTimeout(),
                        config.getGroupTimeout());
         }

         this.groupingHandler = groupingHandler1;

         managementService.addNotificationListener(groupingHandler1);
      }
   }

   private Transformer instantiateTransformer(final String transformerClassName)
   {
      Transformer transformer = null;

      if (transformerClassName != null)
      {
         transformer = (Transformer) instantiateInstance(transformerClassName);
      }

      return transformer;
   }

   private Object instantiateInstance(final String className)
   {
      return safeInitNewInstance(className);
   }

   private static ClassLoader getThisClassLoader()
   {
      return AccessController.doPrivileged(new PrivilegedAction<ClassLoader>()
      {
         public ClassLoader run()
         {
            return ClientSessionFactoryImpl.class.getClassLoader();
         }
      });

   }

   /**
    * Check if journal directory exists or create it (if configured to do so)
    */
   void checkJournalDirectory()
   {
      File journalDir = new File(configuration.getJournalDirectory());

      if (!journalDir.exists())
      {
         if (configuration.isCreateJournalDir())
         {
            journalDir.mkdirs();
         }
         else
         {
            throw HornetQMessageBundle.BUNDLE.cannotCreateDir(journalDir.getAbsolutePath());
         }
      }
   }


   // Inner classes
   // --------------------------------------------------------------------------------



   public final class ShutdownOnCriticalErrorListener implements IOCriticalErrorListener
   {
      boolean failedAlready = false;

      public synchronized void onIOException(Exception cause, String message, SequentialFile file)
      {
         if (!failedAlready)
         {
            failedAlready = true;

            HornetQServerLogger.LOGGER.ioCriticalIOError(message, file.toString(), cause);

            stopTheServer(true);
         }
      }
   }


   /**
    * This seems duplicate code all over the place, but for security reasons we can't let something like this to be open in a
    * utility class, as it would be a door to load anything you like in a safe VM.
    * For that reason any class trying to do a privileged block should do with the AccessController directly.
    */
   private static Object safeInitNewInstance(final String className)
   {
      return AccessController.doPrivileged(new PrivilegedAction<Object>()
      {
         public Object run()
         {
            return ClassloadingUtil.newInstanceFromClassLoader(className);
         }
      });
   }

   public void addProtocolManagerFactory(ProtocolManagerFactory factory)
   {
      protocolManagerFactories.add(factory);
   }

   public void removeProtocolManagerFactory(ProtocolManagerFactory factory)
   {
      protocolManagerFactories.remove(factory);
   }

   @Override
   public HornetQServer createBackupServer(Configuration configuration)
   {
      return new HornetQServerImpl(configuration, null, securityManager, this);
   }

   @Override
   public void addScaledDownNode(SimpleString scaledDownNodeId)
   {
      synchronized (scaledDownNodeIDs)
      {
         scaledDownNodeIDs.add(scaledDownNodeId);
         if (scaledDownNodeIDs.size() > 10)
         {
            scaledDownNodeIDs.remove(10);
         }
      }
   }

   @Override
   public boolean hasScaledDown(SimpleString scaledDownNodeId)
   {
      return scaledDownNodeIDs.contains(scaledDownNodeId);
   }


   int countNumberOfCopiedJournals()
   {
      //will use the main journal to check for how many backups have been kept
      File journalDir = new File(configuration.getJournalDirectory());
      final String fileName = journalDir.getName();
      int numberOfbackupsSaved = 0;
      //fine if it doesn't exist, we aren't using file based persistence so it's no issue
      if (journalDir.exists())
      {
         File parentFile = new File(journalDir.getParent());
         String[] backupJournals = parentFile.list(new FilenameFilter()
         {
            @Override
            public boolean accept(File dir, String name)
            {
               return name.startsWith(fileName) && !name.matches(fileName);
            }
         });
         numberOfbackupsSaved = backupJournals != null ? backupJournals.length : 0;
      }
      return numberOfbackupsSaved;
   }

   /**
    * Move data away before starting data synchronization for fail-back.
    * <p/>
    * Use case is a server, upon restarting, finding a former backup running in its place. It will
    * move any older data away and log a warning about it.
    */
   void moveServerData()
   {
      String[] dataDirs =
         new String[]{configuration.getBindingsDirectory(),
            configuration.getJournalDirectory(),
            configuration.getPagingDirectory(),
            configuration.getLargeMessagesDirectory()};
      boolean allEmpty = true;
      int lowestSuffixForMovedData = 1;
      boolean redo = true;

      while (redo)
      {
         redo = false;
         for (String dir : dataDirs)
         {
            File fDir = new File(dir);
            if (fDir.exists())
            {
               if (!fDir.isDirectory())
               {
                  throw HornetQMessageBundle.BUNDLE.journalDirIsFile(fDir);
               }

               if (fDir.list().length > 0)
                  allEmpty = false;
            }

            String sanitizedPath = fDir.getPath();
            while (new File(sanitizedPath + lowestSuffixForMovedData).exists())
            {
               lowestSuffixForMovedData++;
               redo = true;
            }
         }
      }
      if (allEmpty)
         return;

      for (String dir0 : dataDirs)
      {
         File dir = new File(dir0);
         File newPath = new File(dir.getPath() + lowestSuffixForMovedData);
         if (dir.exists())
         {
            if (!dir.renameTo(newPath))
            {
               throw HornetQMessageBundle.BUNDLE.couldNotMoveJournal(dir);
            }

            HornetQServerLogger.LOGGER.backupMovingDataAway(dir0, newPath.getPath());
         }
         /*
         * sometimes OS's can hold on to file handles for a while so we need to check this actually qorks and then wait
         * a while and try again if it doesn't
         * */

         File dirToRecreate = new File(dir0);
         int count = 0;
         while (!dirToRecreate.exists() && !dirToRecreate.mkdir())
         {
            try
            {
               Thread.sleep(1000);
            }
            catch (InterruptedException e)
            {
            }
            count++;
            if (count == 5)
            {
               throw HornetQMessageBundle.BUNDLE.cannotCreateDir(dir.getPath());
            }
         }
      }
   }
}
