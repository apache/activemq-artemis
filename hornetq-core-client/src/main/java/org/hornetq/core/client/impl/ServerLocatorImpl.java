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
package org.hornetq.core.client.impl;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.hornetq.api.core.DiscoveryGroupConfiguration;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.HornetQExceptionType;
import org.hornetq.api.core.HornetQIllegalStateException;
import org.hornetq.api.core.HornetQInterruptedException;
import org.hornetq.api.core.Interceptor;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ClusterTopologyListener;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.TopologyMember;
import org.hornetq.api.core.client.loadbalance.ConnectionLoadBalancingPolicy;
import org.hornetq.core.client.HornetQClientLogger;
import org.hornetq.core.client.HornetQClientMessageBundle;
import org.hornetq.core.cluster.DiscoveryEntry;
import org.hornetq.core.cluster.DiscoveryGroup;
import org.hornetq.core.cluster.DiscoveryListener;
import org.hornetq.core.protocol.ClientPacketDecoder;
import org.hornetq.core.protocol.core.impl.PacketDecoder;
import org.hornetq.core.remoting.FailureListener;
import org.hornetq.spi.core.remoting.Connector;
import org.hornetq.utils.ClassloadingUtil;
import org.hornetq.utils.HornetQThreadFactory;
import org.hornetq.utils.UUIDGenerator;

/**
 * This is the implementation of {@link org.hornetq.api.core.client.ServerLocator} and all
 * the proper javadoc is located on that interface.
 *
 * @author Tim Fox
 */
public final class ServerLocatorImpl implements ServerLocatorInternal, DiscoveryListener, Serializable
{
   /*needed for backward compatibility*/
   @SuppressWarnings("unused")
   private final Set<ClusterTopologyListener> topologyListeners = new HashSet<ClusterTopologyListener>();

   /*end of compatibility fixes*/
   private enum STATE
   {
      INITIALIZED, CLOSED, CLOSING
   }

   private static final long serialVersionUID = -1615857864410205260L;

   private final boolean ha;

   private boolean finalizeCheck = true;

   private boolean clusterConnection;

   private transient String identity;

   private final Set<ClientSessionFactoryInternal> factories = new HashSet<ClientSessionFactoryInternal>();

   private final Set<ClientSessionFactoryInternal> connectingFactories = new HashSet<ClientSessionFactoryInternal>();

   private volatile TransportConfiguration[] initialConnectors;

   private final DiscoveryGroupConfiguration discoveryGroupConfiguration;

   private final StaticConnector staticConnector = new StaticConnector();

   private final Topology topology;

   //needs to be serializable and not final for retrocompatibility
   private String topologyArrayGuard = new String();

   private volatile Pair<TransportConfiguration, TransportConfiguration>[] topologyArray;

   private volatile boolean receivedTopology;

   private boolean compressLargeMessage;

   // if the system should shutdown the pool when shutting down
   private transient boolean shutdownPool;

   private transient ExecutorService threadPool;

   private transient ScheduledExecutorService scheduledThreadPool;

   private transient DiscoveryGroup discoveryGroup;

   private transient ConnectionLoadBalancingPolicy loadBalancingPolicy;

   // Settable attributes:

   private boolean cacheLargeMessagesClient;

   private long clientFailureCheckPeriod;

   private long connectionTTL;

   private long callTimeout;

   private long callFailoverTimeout;

   private int minLargeMessageSize;

   private int consumerWindowSize;

   private int consumerMaxRate;

   private int confirmationWindowSize;

   private int producerWindowSize;

   private int producerMaxRate;

   private boolean blockOnAcknowledge;

   private boolean blockOnDurableSend;

   private boolean blockOnNonDurableSend;

   private boolean autoGroup;

   private boolean preAcknowledge;

   private String connectionLoadBalancingPolicyClassName;

   private int ackBatchSize;

   private boolean useGlobalPools;

   private int scheduledThreadPoolMaxSize;

   private int threadPoolMaxSize;

   private long retryInterval;

   private double retryIntervalMultiplier;

   private long maxRetryInterval;

   private int reconnectAttempts;

   private int initialConnectAttempts;

   private boolean failoverOnInitialConnection;

   private int initialMessagePacketSize;

   //needs to be serializable and not final for retrocompatibility
   private String stateGuard = new String();
   private transient STATE state;
   private transient CountDownLatch latch;

   private final List<Interceptor> incomingInterceptors = new CopyOnWriteArrayList<Interceptor>();

   private final List<Interceptor> outgoingInterceptors = new CopyOnWriteArrayList<Interceptor>();

   private static ExecutorService globalThreadPool;

   private Executor startExecutor;

   private static ScheduledExecutorService globalScheduledThreadPool;

   private AfterConnectInternalListener afterConnectListener;

   private String groupID;

   private String nodeID;

   private TransportConfiguration clusterTransportConfiguration;

   /*
   * *************WARNING***************
   * remember that when adding any new classes that we have to support serialization with previous clients.
   * If you need to, make them transient and handle the serialization yourself
   * */


   /*
   * we use the client decoder by default but there are times when we want to use the server packet decoder
   */
   private transient PacketDecoder packetDecoder = ClientPacketDecoder.INSTANCE;

   private final Exception traceException = new Exception();

   // To be called when there are ServerLocator being finalized.
   // To be used on test assertions
   public static Runnable finalizeCallback = null;

   public static synchronized void clearThreadPools()
   {

      if (globalThreadPool != null)
      {
         globalThreadPool.shutdown();
         try
         {
            if (!globalThreadPool.awaitTermination(10, TimeUnit.SECONDS))
            {
               throw new IllegalStateException("Couldn't finish the globalThreadPool");
            }
         }
         catch (InterruptedException e)
         {
            throw new HornetQInterruptedException(e);
         }
         finally
         {
            globalThreadPool = null;
         }
      }

      if (globalScheduledThreadPool != null)
      {
         globalScheduledThreadPool.shutdown();
         try
         {
            if (!globalScheduledThreadPool.awaitTermination(10, TimeUnit.SECONDS))
            {
               throw new IllegalStateException("Couldn't finish the globalScheduledThreadPool");
            }
         }
         catch (InterruptedException e)
         {
            throw new HornetQInterruptedException(e);
         }
         finally
         {
            globalScheduledThreadPool = null;
         }
      }
   }

   private static synchronized ExecutorService getGlobalThreadPool()
   {
      if (globalThreadPool == null)
      {
         ThreadFactory factory = new HornetQThreadFactory("HornetQ-client-global-threads", true, getThisClassLoader());

         globalThreadPool = Executors.newCachedThreadPool(factory);
      }

      return globalThreadPool;
   }

   private static synchronized ScheduledExecutorService getGlobalScheduledThreadPool()
   {
      if (globalScheduledThreadPool == null)
      {
         ThreadFactory factory = new HornetQThreadFactory("HornetQ-client-global-scheduled-threads",
                                                          true,
                                                          getThisClassLoader());

         globalScheduledThreadPool = Executors.newScheduledThreadPool(HornetQClient.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE,

                                                                      factory);
      }

      return globalScheduledThreadPool;
   }

   private synchronized void setThreadPools()
   {
      if (threadPool != null)
      {
         return;
      }
      else if (useGlobalPools)
      {
         threadPool = getGlobalThreadPool();

         scheduledThreadPool = getGlobalScheduledThreadPool();
      }
      else
      {
         this.shutdownPool = true;

         ThreadFactory factory = new HornetQThreadFactory("HornetQ-client-factory-threads-" + System.identityHashCode(this),
                                                          true,
                                                          getThisClassLoader());

         if (threadPoolMaxSize == -1)
         {
            threadPool = Executors.newCachedThreadPool(factory);
         }
         else
         {
            threadPool = Executors.newFixedThreadPool(threadPoolMaxSize, factory);
         }

         factory = new HornetQThreadFactory("HornetQ-client-factory-pinger-threads-" + System.identityHashCode(this),
                                            true,
                                            getThisClassLoader());

         scheduledThreadPool = Executors.newScheduledThreadPool(scheduledThreadPoolMaxSize, factory);
      }
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


   private void instantiateLoadBalancingPolicy()
   {
      if (connectionLoadBalancingPolicyClassName == null)
      {
         throw new IllegalStateException("Please specify a load balancing policy class name on the session factory");
      }
      AccessController.doPrivileged(new PrivilegedAction<Object>()
      {
         public Object run()
         {
            loadBalancingPolicy = (ConnectionLoadBalancingPolicy) ClassloadingUtil.newInstanceFromClassLoader(connectionLoadBalancingPolicyClassName);
            return null;
         }
      });
   }

   private synchronized void initialise() throws HornetQException
   {
      if (state == STATE.INITIALIZED)
         return;
      synchronized (stateGuard)
      {
         if (state == STATE.CLOSING)
            throw new HornetQIllegalStateException();
         try
         {
            state = STATE.INITIALIZED;
            latch = new CountDownLatch(1);

            setThreadPools();

            instantiateLoadBalancingPolicy();

            if (discoveryGroupConfiguration != null)
            {
               discoveryGroup = createDiscoveryGroup(nodeID, discoveryGroupConfiguration);

               discoveryGroup.registerListener(this);

               discoveryGroup.start();
            }
         }
         catch (Exception e)
         {
            state = null;
            throw HornetQClientMessageBundle.BUNDLE.failedToInitialiseSessionFactory(e);
         }
      }
   }

   private static DiscoveryGroup createDiscoveryGroup(String nodeID, DiscoveryGroupConfiguration config) throws Exception
   {
      DiscoveryGroup group = new DiscoveryGroup(nodeID, config.getName(),
                                                config.getRefreshTimeout(), config.getBroadcastEndpointFactoryConfiguration().createBroadcastEndpointFactory(), null);
      return group;
   }

   private ServerLocatorImpl(final Topology topology,
                             final boolean useHA,
                             final DiscoveryGroupConfiguration discoveryGroupConfiguration,
                             final TransportConfiguration[] transportConfigs)
   {
      traceException.fillInStackTrace();

      this.topology = topology == null ? new Topology(this) : topology;

      this.ha = useHA;

      this.discoveryGroupConfiguration = discoveryGroupConfiguration;

      this.initialConnectors = transportConfigs != null ? transportConfigs : null;

      this.nodeID = UUIDGenerator.getInstance().generateStringUUID();

      clientFailureCheckPeriod = HornetQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD;

      connectionTTL = HornetQClient.DEFAULT_CONNECTION_TTL;

      callTimeout = HornetQClient.DEFAULT_CALL_TIMEOUT;

      callFailoverTimeout = HornetQClient.DEFAULT_CALL_FAILOVER_TIMEOUT;

      minLargeMessageSize = HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE;

      consumerWindowSize = HornetQClient.DEFAULT_CONSUMER_WINDOW_SIZE;

      consumerMaxRate = HornetQClient.DEFAULT_CONSUMER_MAX_RATE;

      confirmationWindowSize = HornetQClient.DEFAULT_CONFIRMATION_WINDOW_SIZE;

      producerWindowSize = HornetQClient.DEFAULT_PRODUCER_WINDOW_SIZE;

      producerMaxRate = HornetQClient.DEFAULT_PRODUCER_MAX_RATE;

      blockOnAcknowledge = HornetQClient.DEFAULT_BLOCK_ON_ACKNOWLEDGE;

      blockOnDurableSend = HornetQClient.DEFAULT_BLOCK_ON_DURABLE_SEND;

      blockOnNonDurableSend = HornetQClient.DEFAULT_BLOCK_ON_NON_DURABLE_SEND;

      autoGroup = HornetQClient.DEFAULT_AUTO_GROUP;

      preAcknowledge = HornetQClient.DEFAULT_PRE_ACKNOWLEDGE;

      ackBatchSize = HornetQClient.DEFAULT_ACK_BATCH_SIZE;

      connectionLoadBalancingPolicyClassName = HornetQClient.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME;

      useGlobalPools = HornetQClient.DEFAULT_USE_GLOBAL_POOLS;

      scheduledThreadPoolMaxSize = HornetQClient.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE;

      threadPoolMaxSize = HornetQClient.DEFAULT_THREAD_POOL_MAX_SIZE;

      retryInterval = HornetQClient.DEFAULT_RETRY_INTERVAL;

      retryIntervalMultiplier = HornetQClient.DEFAULT_RETRY_INTERVAL_MULTIPLIER;

      maxRetryInterval = HornetQClient.DEFAULT_MAX_RETRY_INTERVAL;

      reconnectAttempts = HornetQClient.DEFAULT_RECONNECT_ATTEMPTS;

      initialConnectAttempts = HornetQClient.INITIAL_CONNECT_ATTEMPTS;

      failoverOnInitialConnection = HornetQClient.DEFAULT_FAILOVER_ON_INITIAL_CONNECTION;

      cacheLargeMessagesClient = HornetQClient.DEFAULT_CACHE_LARGE_MESSAGE_CLIENT;

      initialMessagePacketSize = HornetQClient.DEFAULT_INITIAL_MESSAGE_PACKET_SIZE;

      cacheLargeMessagesClient = HornetQClient.DEFAULT_CACHE_LARGE_MESSAGE_CLIENT;

      compressLargeMessage = HornetQClient.DEFAULT_COMPRESS_LARGE_MESSAGES;

      clusterConnection = false;
   }

   /**
    * Create a ServerLocatorImpl using UDP discovery to lookup cluster
    */
   public ServerLocatorImpl(final boolean useHA, final DiscoveryGroupConfiguration groupConfiguration)
   {
      this(new Topology(null), useHA, groupConfiguration, null);
      if (useHA)
      {
         // We only set the owner at where the Topology was created.
         // For that reason we can't set it at the main constructor
         topology.setOwner(this);
      }
   }

   /**
    * Create a ServerLocatorImpl using a static list of live servers
    *
    * @param transportConfigs
    */
   public ServerLocatorImpl(final boolean useHA, final TransportConfiguration... transportConfigs)
   {
      this(new Topology(null), useHA, null, transportConfigs);
      if (useHA)
      {
         // We only set the owner at where the Topology was created.
         // For that reason we can't set it at the main constructor
         topology.setOwner(this);
      }
   }

   /**
    * Create a ServerLocatorImpl using UDP discovery to lookup cluster
    */
   public ServerLocatorImpl(final Topology topology,
                            final boolean useHA,
                            final DiscoveryGroupConfiguration groupConfiguration)
   {
      this(topology, useHA, groupConfiguration, null);
   }

   /**
    * Create a ServerLocatorImpl using a static list of live servers
    *
    * @param transportConfigs
    */
   public ServerLocatorImpl(final Topology topology,
                            final boolean useHA,
                            final TransportConfiguration... transportConfigs)
   {
      this(topology, useHA, null, transportConfigs);
   }

   public void resetToInitialConnectors()
   {
      synchronized (topologyArrayGuard)
      {
         receivedTopology = false;
         topologyArray = null;
         topology.clear();
      }
   }

   private ServerLocatorImpl(ServerLocatorImpl locator)
   {
      ha = locator.ha;
      finalizeCheck = locator.finalizeCheck;
      clusterConnection = locator.clusterConnection;
      initialConnectors = locator.initialConnectors;
      discoveryGroupConfiguration = locator.discoveryGroupConfiguration;
      topology = locator.topology;
      topologyArray = locator.topologyArray;
      receivedTopology = locator.receivedTopology;
      compressLargeMessage = locator.compressLargeMessage;
      cacheLargeMessagesClient = locator.cacheLargeMessagesClient;
      clientFailureCheckPeriod = locator.clientFailureCheckPeriod;
      connectionTTL = locator.connectionTTL;
      callTimeout = locator.callTimeout;
      callFailoverTimeout = locator.callFailoverTimeout;
      minLargeMessageSize = locator.minLargeMessageSize;
      consumerWindowSize = locator.consumerWindowSize;
      consumerMaxRate = locator.consumerMaxRate;
      confirmationWindowSize = locator.confirmationWindowSize;
      producerWindowSize = locator.producerWindowSize;
      producerMaxRate = locator.producerMaxRate;
      blockOnAcknowledge = locator.blockOnAcknowledge;
      blockOnDurableSend = locator.blockOnDurableSend;
      blockOnNonDurableSend = locator.blockOnNonDurableSend;
      autoGroup = locator.autoGroup;
      preAcknowledge = locator.preAcknowledge;
      connectionLoadBalancingPolicyClassName = locator.connectionLoadBalancingPolicyClassName;
      ackBatchSize = locator.ackBatchSize;
      useGlobalPools = locator.useGlobalPools;
      scheduledThreadPoolMaxSize = locator.scheduledThreadPoolMaxSize;
      threadPoolMaxSize = locator.threadPoolMaxSize;
      retryInterval = locator.retryInterval;
      retryIntervalMultiplier = locator.retryIntervalMultiplier;
      maxRetryInterval = locator.maxRetryInterval;
      reconnectAttempts = locator.reconnectAttempts;
      initialConnectAttempts = locator.initialConnectAttempts;
      failoverOnInitialConnection = locator.failoverOnInitialConnection;
      initialMessagePacketSize = locator.initialMessagePacketSize;
      startExecutor = locator.startExecutor;
      afterConnectListener = locator.afterConnectListener;
      groupID = locator.groupID;
      nodeID = locator.nodeID;
      clusterTransportConfiguration = locator.clusterTransportConfiguration;
   }

   private synchronized TransportConfiguration selectConnector()
   {
      Pair<TransportConfiguration, TransportConfiguration>[] usedTopology;

      synchronized (topologyArrayGuard)
      {
         usedTopology = topologyArray;
      }

      // if the topologyArray is null, we will use the initialConnectors
      if (usedTopology != null)
      {
         int pos = loadBalancingPolicy.select(usedTopology.length);
         Pair<TransportConfiguration, TransportConfiguration> pair = usedTopology[pos];

         return pair.getA();
      }
      else
      {
         // Get from initialconnectors

         int pos = loadBalancingPolicy.select(initialConnectors.length);

         return initialConnectors[pos];
      }
   }

   public void start(Executor executor) throws Exception
   {
      initialise();

      this.startExecutor = executor;

      if (executor != null)
      {
         executor.execute(new Runnable()
         {
            public void run()
            {
               try
               {
                  connect();
               }
               catch (Exception e)
               {
                  if (!isClosed())
                  {
                     HornetQClientLogger.LOGGER.errorConnectingToNodes(e);
                  }
               }
            }
         });
      }
   }

   public void disableFinalizeCheck()
   {
      finalizeCheck = false;
   }

   @Override
   public ClientSessionFactoryInternal connect() throws HornetQException
   {
      return connect(false);
   }

   private ClientSessionFactoryInternal connect(final boolean skipWarnings) throws HornetQException
   {
      synchronized (this)
      {
         // static list of initial connectors
         if (getNumInitialConnectors() > 0 && discoveryGroup == null)
         {
            ClientSessionFactoryInternal sf = (ClientSessionFactoryInternal) staticConnector.connect(skipWarnings);
            addFactory(sf);
            return sf;
         }
      }
      // wait for discovery group to get the list of initial connectors
      return (ClientSessionFactoryInternal) createSessionFactory();
   }

   @Override
   public ClientSessionFactoryInternal connectNoWarnings() throws HornetQException
   {
      return connect(true);
   }

   public void setAfterConnectionInternalListener(AfterConnectInternalListener listener)
   {
      this.afterConnectListener = listener;
   }

   public AfterConnectInternalListener getAfterConnectInternalListener()
   {
      return afterConnectListener;
   }

   public ClientSessionFactory createSessionFactory(String nodeID) throws Exception
   {
      TopologyMember topologyMember = topology.getMember(nodeID);

      if (HornetQClientLogger.LOGGER.isTraceEnabled())
      {
         HornetQClientLogger.LOGGER.trace("Creating connection factory towards " + nodeID + " = " + topologyMember + ", topology=" + topology.describe());
      }

      if (topologyMember == null)
      {
         return null;
      }
      if (topologyMember.getLive() != null)
      {
         ClientSessionFactoryInternal factory = (ClientSessionFactoryInternal) createSessionFactory(topologyMember.getLive());
         if (topologyMember.getBackup() != null)
         {
            factory.setBackupConnector(topologyMember.getLive(), topologyMember.getBackup());
         }
         return factory;
      }
      if (topologyMember.getLive() == null && topologyMember.getBackup() != null)
      {
         // This shouldn't happen, however I wanted this to consider all possible cases
         ClientSessionFactoryInternal factory = (ClientSessionFactoryInternal) createSessionFactory(topologyMember.getBackup());
         return factory;
      }
      // it shouldn't happen
      return null;
   }

   public ClientSessionFactory createSessionFactory(final TransportConfiguration transportConfiguration) throws Exception
   {
      assertOpen();

      initialise();

      ClientSessionFactoryInternal factory = new ClientSessionFactoryImpl(this,
                                                                          transportConfiguration,
                                                                          callTimeout,
                                                                          callFailoverTimeout,
                                                                          clientFailureCheckPeriod,
                                                                          connectionTTL,
                                                                          retryInterval,
                                                                          retryIntervalMultiplier,
                                                                          maxRetryInterval,
                                                                          reconnectAttempts,
                                                                          threadPool,
                                                                          scheduledThreadPool,
                                                                          incomingInterceptors,
                                                                          outgoingInterceptors,
                                                                          packetDecoder);

      addToConnecting(factory);
      try
      {
         try
         {
            factory.connect(reconnectAttempts, failoverOnInitialConnection);
         }
         catch (HornetQException e1)
         {
            //we need to make sure is closed just for garbage collection
            factory.close();
            throw e1;
         }
         addFactory(factory);
         return factory;
      }
      finally
      {
         removeFromConnecting(factory);
      }
   }

   public ClientSessionFactory createSessionFactory(final TransportConfiguration transportConfiguration, int reconnectAttempts, boolean failoverOnInitialConnection) throws Exception
   {
      assertOpen();

      initialise();

      ClientSessionFactoryInternal factory = new ClientSessionFactoryImpl(this,
                                                                          transportConfiguration,
                                                                          callTimeout,
                                                                          callFailoverTimeout,
                                                                          clientFailureCheckPeriod,
                                                                          connectionTTL,
                                                                          retryInterval,
                                                                          retryIntervalMultiplier,
                                                                          maxRetryInterval,
                                                                          reconnectAttempts,
                                                                          threadPool,
                                                                          scheduledThreadPool,
                                                                          incomingInterceptors,
                                                                          outgoingInterceptors,
                                                                          packetDecoder);

      addToConnecting(factory);
      try
      {
         try
         {
            factory.connect(reconnectAttempts, failoverOnInitialConnection);
         }
         catch (HornetQException e1)
         {
            //we need to make sure is closed just for garbage collection
            factory.close();
            throw e1;
         }
         addFactory(factory);
         return factory;
      }
      finally
      {
         removeFromConnecting(factory);
      }
   }

   private void removeFromConnecting(ClientSessionFactoryInternal factory)
   {
      synchronized (connectingFactories)
      {
         connectingFactories.remove(factory);
      }
   }

   private void addToConnecting(ClientSessionFactoryInternal factory)
   {
      synchronized (connectingFactories)
      {
         assertOpen();
         connectingFactories.add(factory);
      }
   }

   public ClientSessionFactory createSessionFactory() throws HornetQException
   {
      assertOpen();

      initialise();

      if (this.getNumInitialConnectors() == 0 && discoveryGroup != null)
      {
         // Wait for an initial broadcast to give us at least one node in the cluster
         long timeout = clusterConnection ? 0 : discoveryGroupConfiguration.getDiscoveryInitialWaitTimeout();
         boolean ok = discoveryGroup.waitForBroadcast(timeout);

         if (!ok)
         {
            throw HornetQClientMessageBundle.BUNDLE.connectionTimedOutInInitialBroadcast();
         }
      }

      ClientSessionFactoryInternal factory = null;

      synchronized (this)
      {
         boolean retry;
         int attempts = 0;
         do
         {
            retry = false;

            TransportConfiguration tc = selectConnector();
            if (tc == null)
            {
               throw HornetQClientMessageBundle.BUNDLE.noTCForSessionFactory();
            }

            // try each factory in the list until we find one which works

            try
            {
               factory = new ClientSessionFactoryImpl(this,
                                                      tc,
                                                      callTimeout,
                                                      callFailoverTimeout,
                                                      clientFailureCheckPeriod,
                                                      connectionTTL,
                                                      retryInterval,
                                                      retryIntervalMultiplier,
                                                      maxRetryInterval,
                                                      reconnectAttempts,
                                                      threadPool,
                                                      scheduledThreadPool,
                                                      incomingInterceptors,
                                                      outgoingInterceptors,
                                                      packetDecoder);
               try
               {
                  addToConnecting(factory);
                  factory.connect(initialConnectAttempts, failoverOnInitialConnection);
               }
               finally
               {
                  removeFromConnecting(factory);
               }
            }
            catch (HornetQException e)
            {
               factory.close();
               factory = null;
               if (e.getType() == HornetQExceptionType.NOT_CONNECTED)
               {
                  attempts++;

                  synchronized (topologyArrayGuard)
                  {

                     if (topologyArray != null && attempts == topologyArray.length)
                     {
                        throw HornetQClientMessageBundle.BUNDLE.cannotConnectToServers();
                     }
                     if (topologyArray == null && attempts == this.getNumInitialConnectors())
                     {
                        throw HornetQClientMessageBundle.BUNDLE.cannotConnectToServers();
                     }
                  }
                  retry = true;
               }
               else
               {
                  throw e;
               }
            }
         }
         while (retry);

         // We always wait for the topology, as the server
         // will send a single element if not cluster
         // so clients can know the id of the server they are connected to
         final long timeout = System.currentTimeMillis() + callTimeout;
         while (!isClosed() && !receivedTopology && timeout > System.currentTimeMillis())
         {
            // Now wait for the topology
            try
            {
               wait(1000);
            }
            catch (InterruptedException e)
            {
               throw new HornetQInterruptedException(e);
            }
         }


         // We are waiting for the topology here,
         // however to avoid a race where the connection is closed (and receivedtopology set to true)
         // between the wait and this timeout here, we redo the check for timeout.
         // if this becomes false there's no big deal and we will just ignore the issue
         // notice that we can't add more locks here otherwise there wouldn't be able to avoid a deadlock
         final boolean hasTimedOut = timeout > System.currentTimeMillis();
         if (!hasTimedOut && !receivedTopology)
         {
            if (factory != null)
               factory.cleanup();
            throw HornetQClientMessageBundle.BUNDLE.connectionTimedOutOnReceiveTopology(discoveryGroup);
         }

         addFactory(factory);

         return factory;
      }

   }

   public boolean isHA()
   {
      return ha;
   }

   public boolean isCacheLargeMessagesClient()
   {
      return cacheLargeMessagesClient;
   }

   public void setCacheLargeMessagesClient(final boolean cached)
   {
      cacheLargeMessagesClient = cached;
   }

   public long getClientFailureCheckPeriod()
   {
      return clientFailureCheckPeriod;
   }

   public void setClientFailureCheckPeriod(final long clientFailureCheckPeriod)
   {
      checkWrite();
      this.clientFailureCheckPeriod = clientFailureCheckPeriod;
   }

   public long getConnectionTTL()
   {
      return connectionTTL;
   }

   public void setConnectionTTL(final long connectionTTL)
   {
      checkWrite();
      this.connectionTTL = connectionTTL;
   }

   public long getCallTimeout()
   {
      return callTimeout;
   }

   public void setCallTimeout(final long callTimeout)
   {
      checkWrite();
      this.callTimeout = callTimeout;
   }

   public long getCallFailoverTimeout()
   {
      return callFailoverTimeout;
   }

   public void setCallFailoverTimeout(long callFailoverTimeout)
   {
      checkWrite();
      this.callFailoverTimeout = callFailoverTimeout;
   }

   public int getMinLargeMessageSize()
   {
      return minLargeMessageSize;
   }

   public void setMinLargeMessageSize(final int minLargeMessageSize)
   {
      checkWrite();
      this.minLargeMessageSize = minLargeMessageSize;
   }

   public int getConsumerWindowSize()
   {
      return consumerWindowSize;
   }

   public void setConsumerWindowSize(final int consumerWindowSize)
   {
      checkWrite();
      this.consumerWindowSize = consumerWindowSize;
   }

   public int getConsumerMaxRate()
   {
      return consumerMaxRate;
   }

   public void setConsumerMaxRate(final int consumerMaxRate)
   {
      checkWrite();
      this.consumerMaxRate = consumerMaxRate;
   }

   public int getConfirmationWindowSize()
   {
      return confirmationWindowSize;
   }

   public void setConfirmationWindowSize(final int confirmationWindowSize)
   {
      checkWrite();
      this.confirmationWindowSize = confirmationWindowSize;
   }

   public int getProducerWindowSize()
   {
      return producerWindowSize;
   }

   public void setProducerWindowSize(final int producerWindowSize)
   {
      checkWrite();
      this.producerWindowSize = producerWindowSize;
   }

   public int getProducerMaxRate()
   {
      return producerMaxRate;
   }

   public void setProducerMaxRate(final int producerMaxRate)
   {
      checkWrite();
      this.producerMaxRate = producerMaxRate;
   }

   public boolean isBlockOnAcknowledge()
   {
      return blockOnAcknowledge;
   }

   public void setBlockOnAcknowledge(final boolean blockOnAcknowledge)
   {
      checkWrite();
      this.blockOnAcknowledge = blockOnAcknowledge;
   }

   public boolean isBlockOnDurableSend()
   {
      return blockOnDurableSend;
   }

   public void setBlockOnDurableSend(final boolean blockOnDurableSend)
   {
      checkWrite();
      this.blockOnDurableSend = blockOnDurableSend;
   }

   public boolean isBlockOnNonDurableSend()
   {
      return blockOnNonDurableSend;
   }

   public void setBlockOnNonDurableSend(final boolean blockOnNonDurableSend)
   {
      checkWrite();
      this.blockOnNonDurableSend = blockOnNonDurableSend;
   }

   public boolean isAutoGroup()
   {
      return autoGroup;
   }

   public void setAutoGroup(final boolean autoGroup)
   {
      checkWrite();
      this.autoGroup = autoGroup;
   }

   public boolean isPreAcknowledge()
   {
      return preAcknowledge;
   }

   public void setPreAcknowledge(final boolean preAcknowledge)
   {
      checkWrite();
      this.preAcknowledge = preAcknowledge;
   }

   public int getAckBatchSize()
   {
      return ackBatchSize;
   }

   public void setAckBatchSize(final int ackBatchSize)
   {
      checkWrite();
      this.ackBatchSize = ackBatchSize;
   }

   public boolean isUseGlobalPools()
   {
      return useGlobalPools;
   }

   public void setUseGlobalPools(final boolean useGlobalPools)
   {
      checkWrite();
      this.useGlobalPools = useGlobalPools;
   }

   public int getScheduledThreadPoolMaxSize()
   {
      return scheduledThreadPoolMaxSize;
   }

   public void setScheduledThreadPoolMaxSize(final int scheduledThreadPoolMaxSize)
   {
      checkWrite();
      this.scheduledThreadPoolMaxSize = scheduledThreadPoolMaxSize;
   }

   public int getThreadPoolMaxSize()
   {
      return threadPoolMaxSize;
   }

   public void setThreadPoolMaxSize(final int threadPoolMaxSize)
   {
      checkWrite();
      this.threadPoolMaxSize = threadPoolMaxSize;
   }

   public long getRetryInterval()
   {
      return retryInterval;
   }

   public void setRetryInterval(final long retryInterval)
   {
      checkWrite();
      this.retryInterval = retryInterval;
   }

   public long getMaxRetryInterval()
   {
      return maxRetryInterval;
   }

   public void setMaxRetryInterval(final long retryInterval)
   {
      checkWrite();
      maxRetryInterval = retryInterval;
   }

   public double getRetryIntervalMultiplier()
   {
      return retryIntervalMultiplier;
   }

   public void setRetryIntervalMultiplier(final double retryIntervalMultiplier)
   {
      checkWrite();
      this.retryIntervalMultiplier = retryIntervalMultiplier;
   }

   public int getReconnectAttempts()
   {
      return reconnectAttempts;
   }

   public void setReconnectAttempts(final int reconnectAttempts)
   {
      checkWrite();
      this.reconnectAttempts = reconnectAttempts;
   }

   public void setInitialConnectAttempts(int initialConnectAttempts)
   {
      checkWrite();
      this.initialConnectAttempts = initialConnectAttempts;
   }

   public int getInitialConnectAttempts()
   {
      return initialConnectAttempts;
   }

   public boolean isFailoverOnInitialConnection()
   {
      return this.failoverOnInitialConnection;
   }

   public void setFailoverOnInitialConnection(final boolean failover)
   {
      checkWrite();
      this.failoverOnInitialConnection = failover;
   }

   public String getConnectionLoadBalancingPolicyClassName()
   {
      return connectionLoadBalancingPolicyClassName;
   }

   public void setConnectionLoadBalancingPolicyClassName(final String loadBalancingPolicyClassName)
   {
      checkWrite();
      connectionLoadBalancingPolicyClassName = loadBalancingPolicyClassName;
   }

   public TransportConfiguration[] getStaticTransportConfigurations()
   {
      if (initialConnectors == null) return new TransportConfiguration[]{};
      return Arrays.copyOf(initialConnectors, initialConnectors.length);
   }

   public DiscoveryGroupConfiguration getDiscoveryGroupConfiguration()
   {
      return discoveryGroupConfiguration;
   }

   @Override
   @Deprecated
   public void addInterceptor(final Interceptor interceptor)
   {
      addIncomingInterceptor(interceptor);
   }

   public void addIncomingInterceptor(final Interceptor interceptor)
   {
      incomingInterceptors.add(interceptor);
   }

   public void addOutgoingInterceptor(final Interceptor interceptor)
   {
      outgoingInterceptors.add(interceptor);
   }

   @Override
   @Deprecated
   public boolean removeInterceptor(final Interceptor interceptor)
   {
      return removeIncomingInterceptor(interceptor);
   }

   public boolean removeIncomingInterceptor(final Interceptor interceptor)
   {
      return incomingInterceptors.remove(interceptor);
   }

   public boolean removeOutgoingInterceptor(final Interceptor interceptor)
   {
      return outgoingInterceptors.remove(interceptor);
   }

   public int getInitialMessagePacketSize()
   {
      return initialMessagePacketSize;
   }

   public void setInitialMessagePacketSize(final int size)
   {
      checkWrite();
      initialMessagePacketSize = size;
   }

   public void setGroupID(final String groupID)
   {
      checkWrite();
      this.groupID = groupID;
   }

   public String getGroupID()
   {
      return groupID;
   }

   public boolean isCompressLargeMessage()
   {
      return compressLargeMessage;
   }

   public void setCompressLargeMessage(boolean avoid)
   {
      this.compressLargeMessage = avoid;
   }

   private void checkWrite()
   {
      synchronized (stateGuard)
      {
         if (state != null && state != STATE.CLOSED)
         {
            throw new IllegalStateException("Cannot set attribute on SessionFactory after it has been used");
         }
      }
   }

   private int getNumInitialConnectors()
   {
      if (initialConnectors == null) return 0;
      return initialConnectors.length;
   }

   public void setIdentity(String identity)
   {
      this.identity = identity;
   }

   public void setNodeID(String nodeID)
   {
      this.nodeID = nodeID;
   }

   public String getNodeID()
   {
      return nodeID;
   }

   public void setClusterConnection(boolean clusterConnection)
   {
      this.clusterConnection = clusterConnection;
   }

   public boolean isClusterConnection()
   {
      return clusterConnection;
   }

   public TransportConfiguration getClusterTransportConfiguration()
   {
      return clusterTransportConfiguration;
   }

   public void setClusterTransportConfiguration(TransportConfiguration tc)
   {
      this.clusterTransportConfiguration = tc;
   }

   @Override
   protected void finalize() throws Throwable
   {
      if (finalizeCheck)
      {
         close();
      }

      super.finalize();
   }

   public void cleanup()
   {
      doClose(false);
   }

   public void close()
   {
      doClose(true);
   }

   private void doClose(final boolean sendClose)
   {
      synchronized (stateGuard)
      {
         if (state == STATE.CLOSED)
         {
            if (HornetQClientLogger.LOGGER.isDebugEnabled())
            {
               HornetQClientLogger.LOGGER.debug(this + " is already closed when calling closed");
            }
            return;
         }

         state = STATE.CLOSING;
      }
      if (latch != null)
         latch.countDown();

      synchronized (connectingFactories)
      {
         for (ClientSessionFactoryInternal csf : connectingFactories)
         {
            csf.causeExit();
         }
      }

      if (discoveryGroup != null)
      {
         synchronized (this)
         {
            try
            {
               discoveryGroup.stop();
            }
            catch (Exception e)
            {
               HornetQClientLogger.LOGGER.failedToStopDiscovery(e);
            }
         }
      }
      else
      {
         staticConnector.disconnect();
      }

      synchronized (connectingFactories)
      {
         for (ClientSessionFactoryInternal csf : connectingFactories)
         {
            csf.causeExit();
         }
         for (ClientSessionFactoryInternal csf : connectingFactories)
         {
            csf.close();
         }
         connectingFactories.clear();
      }

      Set<ClientSessionFactoryInternal> clonedFactory;
      synchronized (factories)
      {
         clonedFactory = new HashSet<ClientSessionFactoryInternal>(factories);

         factories.clear();
      }

      for (ClientSessionFactoryInternal factory : clonedFactory)
      {
         factory.causeExit();
      }
      for (ClientSessionFactory factory : clonedFactory)
      {
         if (sendClose)
         {
            factory.close();
         }
         else
         {
            factory.cleanup();
         }
      }

      if (shutdownPool)
      {
         if (threadPool != null)
         {
            threadPool.shutdown();

            try
            {
               if (!threadPool.awaitTermination(10000, TimeUnit.MILLISECONDS))
               {
                  HornetQClientLogger.LOGGER.timedOutWaitingForTermination();
               }
            }
            catch (InterruptedException e)
            {
               throw new HornetQInterruptedException(e);
            }
         }

         if (scheduledThreadPool != null)
         {
            scheduledThreadPool.shutdown();

            try
            {
               if (!scheduledThreadPool.awaitTermination(10000, TimeUnit.MILLISECONDS))
               {
                  HornetQClientLogger.LOGGER.timedOutWaitingForScheduledPoolTermination();
               }
            }
            catch (InterruptedException e)
            {
               throw new HornetQInterruptedException(e);
            }
         }
      }
      synchronized (stateGuard)
      {
         state = STATE.CLOSED;
      }
   }

   /**
    * This is directly called when the connection to the node is gone,
    * or when the node sends a disconnection.
    * Look for callers of this method!
    */
   @Override
   public void notifyNodeDown(final long eventTime, final String nodeID)
   {

      if (!ha)
      {
         // there's no topology here
         return;
      }

      if (HornetQClientLogger.LOGGER.isTraceEnabled())
      {
         HornetQClientLogger.LOGGER.trace("nodeDown " + this + " nodeID=" + nodeID + " as being down", new Exception("trace"));
      }

      topology.removeMember(eventTime, nodeID);

      if (clusterConnection)
      {
         updateArraysAndPairs();
      }
      else
      {
         synchronized (topologyArrayGuard)
         {
            if (topology.isEmpty())
            {
               // Resetting the topology to its original condition as it was brand new
               receivedTopology = false;
               topologyArray = null;
            }
            else
            {
               updateArraysAndPairs();

               if (topology.nodes() == 1 && topology.getMember(this.nodeID) != null)
               {
                  // Resetting the topology to its original condition as it was brand new
                  receivedTopology = false;
               }
            }
         }
      }

   }

   public void notifyNodeUp(long uniqueEventID,
                            final String nodeID,
                            final String backupGroupName,
                            final String scaleDownGroupName,
                            final Pair<TransportConfiguration, TransportConfiguration> connectorPair,
                            final boolean last)
   {
      if (HornetQClientLogger.LOGGER.isTraceEnabled())
      {
         HornetQClientLogger.LOGGER.trace("NodeUp " + this + "::nodeID=" + nodeID + ", connectorPair=" + connectorPair, new Exception("trace"));
      }

      TopologyMemberImpl member = new TopologyMemberImpl(nodeID, backupGroupName, scaleDownGroupName, connectorPair.getA(), connectorPair.getB());

      topology.updateMember(uniqueEventID, nodeID, member);

      TopologyMember actMember = topology.getMember(nodeID);

      if (actMember != null && actMember.getLive() != null && actMember.getBackup() != null)
      {
         HashSet<ClientSessionFactory> clonedFactories = new HashSet<ClientSessionFactory>();
         synchronized (factories)
         {
            clonedFactories.addAll(factories);
         }

         for (ClientSessionFactory factory : clonedFactories)
         {
            ((ClientSessionFactoryInternal) factory).setBackupConnector(actMember.getLive(), actMember.getBackup());
         }
      }

      updateArraysAndPairs();

      if (last)
      {
         synchronized (this)
         {
            receivedTopology = true;
            // Notify if waiting on getting topology
            notifyAll();
         }
      }
   }

   @Override
   public String toString()
   {
      if (identity != null)
      {
         return "ServerLocatorImpl (identity=" + identity +
            ") [initialConnectors=" +
            Arrays.toString(initialConnectors == null ? new TransportConfiguration[0] : initialConnectors) +
            ", discoveryGroupConfiguration=" +
            discoveryGroupConfiguration +
            "]";
      }
      return "ServerLocatorImpl [initialConnectors=" + Arrays.toString(initialConnectors == null ? new TransportConfiguration[0] : initialConnectors) +
         ", discoveryGroupConfiguration=" +
         discoveryGroupConfiguration +
         "]";
   }

   @SuppressWarnings("unchecked")
   private void updateArraysAndPairs()
   {
      synchronized (topologyArrayGuard)
      {
         Collection<TopologyMemberImpl> membersCopy = topology.getMembers();

         Pair<TransportConfiguration, TransportConfiguration>[] topologyArrayLocal =
            (Pair<TransportConfiguration, TransportConfiguration>[]) Array.newInstance(Pair.class,
                                                                                       membersCopy.size());

         int count = 0;
         for (TopologyMemberImpl pair : membersCopy)
         {
            topologyArrayLocal[count++] = pair.getConnector();
         }

         this.topologyArray = topologyArrayLocal;
      }
   }

   public synchronized void connectorsChanged(List<DiscoveryEntry> newConnectors)
   {
      if (receivedTopology)
      {
         return;
      }
      TransportConfiguration[] newInitialconnectors = (TransportConfiguration[]) Array.newInstance(TransportConfiguration.class,
                                                                                                   newConnectors.size());

      int count = 0;
      for (DiscoveryEntry entry : newConnectors)
      {
         newInitialconnectors[count++] = entry.getConnector();

         if (ha && topology.getMember(entry.getNodeID()) == null)
         {
            TopologyMemberImpl member = new TopologyMemberImpl(entry.getNodeID(), null, null, entry.getConnector(), null);
            // on this case we set it as zero as any update coming from server should be accepted
            topology.updateMember(0, entry.getNodeID(), member);
         }
      }

      this.initialConnectors = newInitialconnectors.length == 0 ? null : newInitialconnectors;

      if (clusterConnection && !receivedTopology && this.getNumInitialConnectors() > 0)
      {
         // The node is alone in the cluster. We create a connection to the new node
         // to trigger the node notification to form the cluster.

         Runnable connectRunnable = new Runnable()
         {
            public void run()
            {
               try
               {
                  connect();
               }
               catch (HornetQException e)
               {
                  HornetQClientLogger.LOGGER.errorConnectingToNodes(e);
               }
            }
         };
         if (startExecutor != null)
         {
            startExecutor.execute(connectRunnable);
         }
         else
         {
            connectRunnable.run();
         }
      }
   }

   public void factoryClosed(final ClientSessionFactory factory)
   {
      synchronized (factories)
      {
         factories.remove(factory);

         if (!clusterConnection && factories.isEmpty())
         {
            // Go back to using the broadcast or static list
            synchronized (topologyArrayGuard)
            {
               receivedTopology = false;

               topologyArray = null;
            }
         }
      }
   }

   public Topology getTopology()
   {
      return topology;
   }

   @Override
   public void setPacketDecoder(PacketDecoder packetDecoder)
   {
      this.packetDecoder = packetDecoder;
   }

   @Override
   public boolean isConnectable()
   {
      return getNumInitialConnectors() > 0 || getDiscoveryGroupConfiguration() != null;
   }

   public void addClusterTopologyListener(final ClusterTopologyListener listener)
   {
      topology.addClusterTopologyListener(listener);
   }

   public void removeClusterTopologyListener(final ClusterTopologyListener listener)
   {
      topology.removeClusterTopologyListener(listener);
   }

   private void addFactory(ClientSessionFactoryInternal factory)
   {
      if (factory == null)
      {
         return;
      }

      if (isClosed())
      {
         factory.close();
         return;
      }

      TransportConfiguration backup = null;

      if (ha)
      {
         backup = topology.getBackupForConnector((Connector) factory.getConnector());
      }

      factory.setBackupConnector(factory.getConnectorConfiguration(), backup);

      synchronized (factories)
      {
         factories.add(factory);
      }
   }

   private void readObject(ObjectInputStream is) throws ClassNotFoundException, IOException
   {
      is.defaultReadObject();
      if (stateGuard == null)
      {
         stateGuard = new String();
      }
      if (topologyArrayGuard == null)
      {
         topologyArrayGuard = new String();
      }
      //is transient so need to create, for compatibility issues
      packetDecoder = ClientPacketDecoder.INSTANCE;
   }

   private final class StaticConnector implements Serializable
   {
      private static final long serialVersionUID = 6772279632415242634L;

      private List<Connector> connectors;

      public ClientSessionFactory connect(boolean skipWarnings) throws HornetQException
      {
         assertOpen();

         initialise();

         ClientSessionFactory csf = null;

         createConnectors();

         try
         {

            int retryNumber = 0;
            while (csf == null && !isClosed())
            {
               retryNumber++;
               for (Connector conn : connectors)
               {
                  if (HornetQClientLogger.LOGGER.isDebugEnabled())
                  {
                     HornetQClientLogger.LOGGER.debug(this + "::Submitting connect towards " + conn);
                  }

                  csf = conn.tryConnect();

                  if (csf != null)
                  {
                     csf.getConnection().addFailureListener(new FailureListener()
                     {
                        // Case the node where the cluster connection was connected is gone, we need to restart the
                        // connection
                        @Override
                        public void connectionFailed(HornetQException exception, boolean failedOver)
                        {
                           if (clusterConnection && exception.getType() == HornetQExceptionType.DISCONNECTED)
                           {
                              try
                              {
                                 ServerLocatorImpl.this.start(startExecutor);
                              }
                              catch (Exception e)
                              {
                                 // There isn't much to be done if this happens here
                                 HornetQClientLogger.LOGGER.errorStartingLocator(e);
                              }
                           }
                        }

                        @Override
                        public void connectionFailed(final HornetQException me, boolean failedOver, String scaleDownTargetNodeID)
                        {
                           connectionFailed(me, failedOver);
                        }

                        @Override
                        public String toString()
                        {
                           return "FailureListener('restarts cluster connections')";
                        }
                     });

                     if (HornetQClientLogger.LOGGER.isDebugEnabled())
                     {
                        HornetQClientLogger.LOGGER.debug("Returning " + csf +
                                                            " after " +
                                                            retryNumber +
                                                            " retries on StaticConnector " +
                                                            ServerLocatorImpl.this);
                     }

                     return csf;
                  }
               }

               if (initialConnectAttempts >= 0 && retryNumber > initialConnectAttempts)
               {
                  break;
               }

               if (latch.await(retryInterval, TimeUnit.MILLISECONDS))
                  return null;
            }

         }
         catch (RejectedExecutionException e)
         {
            if (isClosed() || skipWarnings)
               return null;
            HornetQClientLogger.LOGGER.debug("Rejected execution", e);
            throw e;
         }
         catch (Exception e)
         {
            if (isClosed() || skipWarnings)
               return null;
            HornetQClientLogger.LOGGER.errorConnectingToNodes(e);
            throw HornetQClientMessageBundle.BUNDLE.cannotConnectToStaticConnectors(e);
         }

         if (isClosed() || skipWarnings)
         {
            return null;
         }

         HornetQClientLogger.LOGGER.errorConnectingToNodes(traceException);
         throw HornetQClientMessageBundle.BUNDLE.cannotConnectToStaticConnectors2();
      }

      private synchronized void createConnectors()
      {
         if (connectors != null)
         {
            for (Connector conn : connectors)
            {
               if (conn != null)
               {
                  conn.disconnect();
               }
            }
         }
         connectors = new ArrayList<Connector>();
         if (initialConnectors != null)
         {
            for (TransportConfiguration initialConnector : initialConnectors)
            {
               ClientSessionFactoryInternal factory = new ClientSessionFactoryImpl(ServerLocatorImpl.this,
                                                                                   initialConnector,
                                                                                   callTimeout,
                                                                                   callFailoverTimeout,
                                                                                   clientFailureCheckPeriod,
                                                                                   connectionTTL,
                                                                                   retryInterval,
                                                                                   retryIntervalMultiplier,
                                                                                   maxRetryInterval,
                                                                                   reconnectAttempts,
                                                                                   threadPool,
                                                                                   scheduledThreadPool,
                                                                                   incomingInterceptors,
                                                                                   outgoingInterceptors,
                                                                                   packetDecoder);

               factory.disableFinalizeCheck();

               connectors.add(new Connector(initialConnector, factory));
            }
         }
      }

      public synchronized void disconnect()
      {
         if (connectors != null)
         {
            for (Connector connector : connectors)
            {
               connector.disconnect();
            }
         }
      }

      @Override
      protected void finalize() throws Throwable
      {
         if (!isClosed() && finalizeCheck)
         {
            HornetQClientLogger.LOGGER.serverLocatorNotClosed(traceException, System.identityHashCode(this));

            if (ServerLocatorImpl.finalizeCallback != null)
            {
               ServerLocatorImpl.finalizeCallback.run();
            }

            close();
         }

         super.finalize();
      }

      private final class Connector
      {
         private final TransportConfiguration initialConnector;

         private volatile ClientSessionFactoryInternal factory;

         public Connector(TransportConfiguration initialConnector, ClientSessionFactoryInternal factory)
         {
            this.initialConnector = initialConnector;
            this.factory = factory;
         }

         public ClientSessionFactory tryConnect() throws HornetQException
         {
            if (HornetQClientLogger.LOGGER.isDebugEnabled())
            {
               HornetQClientLogger.LOGGER.debug(this + "::Trying to connect to " + factory);
            }
            try
            {
               ClientSessionFactoryInternal factoryToUse = factory;
               if (factoryToUse != null)
               {
                  addToConnecting(factoryToUse);

                  try
                  {
                     factoryToUse.connect(1, false);
                  }
                  finally
                  {
                     removeFromConnecting(factoryToUse);
                  }
               }
               return factoryToUse;
            }
            catch (HornetQException e)
            {
               HornetQClientLogger.LOGGER.debug(this + "::Exception on establish connector initial connection", e);
               return null;
            }
         }

         public void disconnect()
         {
            if (factory != null)
            {
               factory.causeExit();
               factory.cleanup();
               factory = null;
            }
         }

         @Override
         public String toString()
         {
            return "Connector [initialConnector=" + initialConnector + "]";
         }

      }
   }

   private void assertOpen()
   {
      synchronized (stateGuard)
      {
         if (state != null && state != STATE.INITIALIZED)
         {
            throw new IllegalStateException("Server locator is closed (maybe it was garbage collected)");
         }
      }
   }

   public boolean isClosed()
   {
      synchronized (stateGuard)
      {
         return state != STATE.INITIALIZED;
      }
   }

   private Object writeReplace() throws ObjectStreamException
   {
      ServerLocatorImpl clone = new ServerLocatorImpl(this);
      return clone;
   }
}
