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
package org.apache.activemq.artemis.core.client.impl;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.net.URI;
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

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.api.core.ActiveMQIllegalStateException;
import org.apache.activemq.artemis.api.core.ActiveMQInterruptedException;
import org.apache.activemq.artemis.api.core.DiscoveryGroupConfiguration;
import org.apache.activemq.artemis.api.core.Interceptor;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ClusterTopologyListener;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.api.core.client.TopologyMember;
import org.apache.activemq.artemis.api.core.client.loadbalance.ConnectionLoadBalancingPolicy;
import org.apache.activemq.artemis.core.client.ActiveMQClientLogger;
import org.apache.activemq.artemis.core.client.ActiveMQClientMessageBundle;
import org.apache.activemq.artemis.core.cluster.DiscoveryEntry;
import org.apache.activemq.artemis.core.cluster.DiscoveryGroup;
import org.apache.activemq.artemis.core.cluster.DiscoveryListener;
import org.apache.activemq.artemis.core.protocol.core.impl.ActiveMQClientProtocolManagerFactory;
import org.apache.activemq.artemis.core.remoting.FailureListener;
import org.apache.activemq.artemis.spi.core.remoting.ClientProtocolManager;
import org.apache.activemq.artemis.spi.core.remoting.ClientProtocolManagerFactory;
import org.apache.activemq.artemis.spi.core.remoting.Connector;
import org.apache.activemq.artemis.uri.ServerLocatorParser;
import org.apache.activemq.artemis.utils.ActiveMQThreadFactory;
import org.apache.activemq.artemis.utils.ActiveMQThreadPoolExecutor;
import org.apache.activemq.artemis.utils.ClassloadingUtil;
import org.apache.activemq.artemis.utils.UUIDGenerator;
import org.apache.activemq.artemis.utils.actors.Actor;
import org.apache.activemq.artemis.utils.actors.OrderedExecutor;
import org.apache.activemq.artemis.utils.uri.FluentPropertyBeanIntrospectorWithIgnores;
import org.jboss.logging.Logger;

/**
 * This is the implementation of {@link org.apache.activemq.artemis.api.core.client.ServerLocator} and all
 * the proper javadoc is located on that interface.
 */
public final class ServerLocatorImpl implements ServerLocatorInternal, DiscoveryListener {

   private static final Logger logger = Logger.getLogger(ServerLocatorImpl.class);

   private enum STATE {
      INITIALIZED, CLOSED, CLOSING
   }

   static {
      // this is not really a property, needs to be ignored
      FluentPropertyBeanIntrospectorWithIgnores.addIgnore(ServerLocatorImpl.class.getName(), "setThreadPools");
   }

   private static final long serialVersionUID = -1615857864410205260L;

   // This is the default value
   private ClientProtocolManagerFactory protocolManagerFactory = ActiveMQClientProtocolManagerFactory.getInstance(this);

   private final boolean ha;

   private boolean finalizeCheck = true;

   private boolean clusterConnection;

   private transient String identity;

   private final Set<ClientSessionFactoryInternal> factories = new HashSet<>();

   private final Set<ClientSessionFactoryInternal> connectingFactories = new HashSet<>();

   private volatile TransportConfiguration[] initialConnectors;

   private final DiscoveryGroupConfiguration discoveryGroupConfiguration;

   private final StaticConnector staticConnector = new StaticConnector();

   private Topology topology;

   private final Object topologyArrayGuard = new Object();

   private volatile Pair<TransportConfiguration, TransportConfiguration>[] topologyArray;

   private volatile boolean receivedTopology;

   private boolean compressLargeMessage;

   // if the system should shutdown the pool when shutting down
   private transient boolean shutdownPool;

   private transient Executor threadPool;

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

   private final Object stateGuard = new Object();
   private transient STATE state;
   private transient CountDownLatch latch;

   private final List<Interceptor> incomingInterceptors = new CopyOnWriteArrayList<>();

   private final List<Interceptor> outgoingInterceptors = new CopyOnWriteArrayList<>();

   private Executor startExecutor;

   private Actor<Long> updateArrayActor;

   private AfterConnectInternalListener afterConnectListener;

   private String groupID;

   private String nodeID;

   private TransportConfiguration clusterTransportConfiguration;

   private boolean useTopologyForLoadBalancing;

   private final Exception traceException = new Exception();

   public static synchronized void clearThreadPools() {
      ActiveMQClient.clearThreadPools();
   }

   private synchronized void setThreadPools() {
      if (threadPool != null) {
         return;
      } else if (useGlobalPools) {
         threadPool = ActiveMQClient.getGlobalThreadPool();

         scheduledThreadPool = ActiveMQClient.getGlobalScheduledThreadPool();
      } else {
         this.shutdownPool = true;

         ThreadFactory factory = AccessController.doPrivileged(new PrivilegedAction<ThreadFactory>() {
            @Override
            public ThreadFactory run() {
               return new ActiveMQThreadFactory("ActiveMQ-client-factory-threads-" + System.identityHashCode(this), true, ClientSessionFactoryImpl.class.getClassLoader());
            }
         });

         if (threadPoolMaxSize == -1) {
            threadPool = Executors.newCachedThreadPool(factory);
         } else {
            threadPool = new ActiveMQThreadPoolExecutor(0, threadPoolMaxSize, 60L, TimeUnit.SECONDS, factory);
         }

         factory = AccessController.doPrivileged(new PrivilegedAction<ThreadFactory>() {
            @Override
            public ThreadFactory run() {
               return new ActiveMQThreadFactory("ActiveMQ-client-factory-pinger-threads-" + System.identityHashCode(this), true, ClientSessionFactoryImpl.class.getClassLoader());
            }
         });

         scheduledThreadPool = Executors.newScheduledThreadPool(scheduledThreadPoolMaxSize, factory);
      }
      this.updateArrayActor = new Actor<>(threadPool, this::internalUpdateArray);
   }

   @Override
   public synchronized boolean setThreadPools(Executor threadPool, ScheduledExecutorService scheduledThreadPool) {

      if (threadPool == null || scheduledThreadPool == null)
         return false;

      if (this.threadPool == null && this.scheduledThreadPool == null) {
         useGlobalPools = false;
         shutdownPool = false;
         this.threadPool = threadPool;
         this.scheduledThreadPool = scheduledThreadPool;
         return true;
      } else {
         return false;
      }
   }

   private void instantiateLoadBalancingPolicy() {
      if (connectionLoadBalancingPolicyClassName == null) {
         throw new IllegalStateException("Please specify a load balancing policy class name on the session factory");
      }
      AccessController.doPrivileged(new PrivilegedAction<Object>() {
         @Override
         public Object run() {
            loadBalancingPolicy = (ConnectionLoadBalancingPolicy) ClassloadingUtil.newInstanceFromClassLoader(connectionLoadBalancingPolicyClassName);
            return null;
         }
      });
   }

   @Override
   public synchronized void initialize() throws ActiveMQException {
      if (state == STATE.INITIALIZED)
         return;
      synchronized (stateGuard) {
         if (state == STATE.CLOSING)
            throw new ActiveMQIllegalStateException();
         try {
            state = STATE.INITIALIZED;
            latch = new CountDownLatch(1);

            setThreadPools();

            topology.setExecutor(new OrderedExecutor(threadPool));

            instantiateLoadBalancingPolicy();

            if (discoveryGroupConfiguration != null) {
               discoveryGroup = createDiscoveryGroup(nodeID, discoveryGroupConfiguration);

               discoveryGroup.registerListener(this);

               discoveryGroup.start();
            }
         } catch (Exception e) {
            state = null;
            throw ActiveMQClientMessageBundle.BUNDLE.failedToInitialiseSessionFactory(e);
         }
      }
   }

   private static DiscoveryGroup createDiscoveryGroup(String nodeID,
                                                      DiscoveryGroupConfiguration config) throws Exception {
      DiscoveryGroup group = new DiscoveryGroup(nodeID, config.getName(), config.getRefreshTimeout(), config.getBroadcastEndpointFactory(), null);
      return group;
   }

   private ServerLocatorImpl(final Topology topology,
                             final boolean useHA,
                             final DiscoveryGroupConfiguration discoveryGroupConfiguration,
                             final TransportConfiguration[] transportConfigs) {
      traceException.fillInStackTrace();

      this.topology = topology == null ? new Topology(this) : topology;

      this.ha = useHA;

      this.discoveryGroupConfiguration = discoveryGroupConfiguration;

      this.initialConnectors = transportConfigs != null ? transportConfigs : null;

      this.nodeID = UUIDGenerator.getInstance().generateStringUUID();

      clientFailureCheckPeriod = ActiveMQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD;

      connectionTTL = ActiveMQClient.DEFAULT_CONNECTION_TTL;

      callTimeout = ActiveMQClient.DEFAULT_CALL_TIMEOUT;

      callFailoverTimeout = ActiveMQClient.DEFAULT_CALL_FAILOVER_TIMEOUT;

      minLargeMessageSize = ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE;

      consumerWindowSize = ActiveMQClient.DEFAULT_CONSUMER_WINDOW_SIZE;

      consumerMaxRate = ActiveMQClient.DEFAULT_CONSUMER_MAX_RATE;

      confirmationWindowSize = ActiveMQClient.DEFAULT_CONFIRMATION_WINDOW_SIZE;

      producerWindowSize = ActiveMQClient.DEFAULT_PRODUCER_WINDOW_SIZE;

      producerMaxRate = ActiveMQClient.DEFAULT_PRODUCER_MAX_RATE;

      blockOnAcknowledge = ActiveMQClient.DEFAULT_BLOCK_ON_ACKNOWLEDGE;

      blockOnDurableSend = ActiveMQClient.DEFAULT_BLOCK_ON_DURABLE_SEND;

      blockOnNonDurableSend = ActiveMQClient.DEFAULT_BLOCK_ON_NON_DURABLE_SEND;

      autoGroup = ActiveMQClient.DEFAULT_AUTO_GROUP;

      preAcknowledge = ActiveMQClient.DEFAULT_PRE_ACKNOWLEDGE;

      ackBatchSize = ActiveMQClient.DEFAULT_ACK_BATCH_SIZE;

      connectionLoadBalancingPolicyClassName = ActiveMQClient.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME;

      useGlobalPools = ActiveMQClient.DEFAULT_USE_GLOBAL_POOLS;

      threadPoolMaxSize = ActiveMQClient.DEFAULT_THREAD_POOL_MAX_SIZE;

      scheduledThreadPoolMaxSize = ActiveMQClient.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE;

      retryInterval = ActiveMQClient.DEFAULT_RETRY_INTERVAL;

      retryIntervalMultiplier = ActiveMQClient.DEFAULT_RETRY_INTERVAL_MULTIPLIER;

      maxRetryInterval = ActiveMQClient.DEFAULT_MAX_RETRY_INTERVAL;

      reconnectAttempts = ActiveMQClient.DEFAULT_RECONNECT_ATTEMPTS;

      initialConnectAttempts = ActiveMQClient.INITIAL_CONNECT_ATTEMPTS;

      failoverOnInitialConnection = ActiveMQClient.DEFAULT_FAILOVER_ON_INITIAL_CONNECTION;

      cacheLargeMessagesClient = ActiveMQClient.DEFAULT_CACHE_LARGE_MESSAGE_CLIENT;

      initialMessagePacketSize = ActiveMQClient.DEFAULT_INITIAL_MESSAGE_PACKET_SIZE;

      cacheLargeMessagesClient = ActiveMQClient.DEFAULT_CACHE_LARGE_MESSAGE_CLIENT;

      compressLargeMessage = ActiveMQClient.DEFAULT_COMPRESS_LARGE_MESSAGES;

      clusterConnection = false;

      useTopologyForLoadBalancing = ActiveMQClient.DEFAULT_USE_TOPOLOGY_FOR_LOADBALANCING;
   }

   public static ServerLocator newLocator(String uri) {
      try {
         ServerLocatorParser parser = new ServerLocatorParser();
         URI newURI = parser.expandURI(uri);
         return parser.newObject(newURI, null);
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   public static ServerLocator newLocator(URI uri) {
      try {
         ServerLocatorParser parser = new ServerLocatorParser();
         return parser.newObject(uri, null);
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   /**
    * Create a ServerLocatorImpl using UDP discovery to lookup cluster
    */
   public ServerLocatorImpl(final boolean useHA, final DiscoveryGroupConfiguration groupConfiguration) {
      this(new Topology(null), useHA, groupConfiguration, null);
      if (useHA) {
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
   public ServerLocatorImpl(final boolean useHA, final TransportConfiguration... transportConfigs) {
      this(new Topology(null), useHA, null, transportConfigs);
      if (useHA) {
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
                            final DiscoveryGroupConfiguration groupConfiguration) {
      this(topology, useHA, groupConfiguration, null);
   }

   /**
    * Create a ServerLocatorImpl using a static list of live servers
    *
    * @param transportConfigs
    */
   public ServerLocatorImpl(final Topology topology,
                            final boolean useHA,
                            final TransportConfiguration... transportConfigs) {
      this(topology, useHA, null, transportConfigs);
   }

   @Override
   public void resetToInitialConnectors() {
      receivedTopology = false;
      topologyArray = null;
      topology.clear();
   }

   /*
    * I'm not using isAllInVM here otherwsie BeanProperties would translate this as a property for the URL
    */
   @Override
   public boolean allInVM() {
      for (TransportConfiguration config : getStaticTransportConfigurations()) {
         if (!config.getFactoryClassName().contains("InVMConnectorFactory")) {
            return false;
         }
      }

      return true;
   }

   private ServerLocatorImpl(ServerLocatorImpl locator) {
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
      useTopologyForLoadBalancing = locator.useTopologyForLoadBalancing;
   }

   private TransportConfiguration selectConnector() {
      Pair<TransportConfiguration, TransportConfiguration>[] usedTopology;

      flushTopology();

      synchronized (topologyArrayGuard) {
         usedTopology = topologyArray;
      }

      synchronized (this) {
         if (usedTopology != null && useTopologyForLoadBalancing) {
            if (logger.isTraceEnabled()) {
               logger.trace("Selecting connector from topology.");
            }
            int pos = loadBalancingPolicy.select(usedTopology.length);
            Pair<TransportConfiguration, TransportConfiguration> pair = usedTopology[pos];

            return pair.getA();
         } else {
            if (logger.isTraceEnabled()) {
               logger.trace("Selecting connector from initial connectors.");
            }

            int pos = loadBalancingPolicy.select(initialConnectors.length);

            return initialConnectors[pos];
         }
      }
   }

   @Override
   public void start(Executor executor) throws Exception {
      initialize();

      this.startExecutor = executor;

      if (executor != null) {
         executor.execute(new Runnable() {
            @Override
            public void run() {
               try {
                  connect();
               } catch (Exception e) {
                  if (!isClosed()) {
                     ActiveMQClientLogger.LOGGER.errorConnectingToNodes(e);
                  }
               }
            }
         });
      }
   }

   @Override
   public ClientProtocolManager newProtocolManager() {
      return getProtocolManagerFactory().newProtocolManager();
   }

   @Override
   public ClientProtocolManagerFactory getProtocolManagerFactory() {
      if (protocolManagerFactory == null) {
         // Default one in case it's null
         protocolManagerFactory = ActiveMQClientProtocolManagerFactory.getInstance(this);
      }
      return protocolManagerFactory;
   }

   @Override
   public ServerLocator setProtocolManagerFactory(ClientProtocolManagerFactory protocolManagerFactory) {
      this.protocolManagerFactory = protocolManagerFactory;
      protocolManagerFactory.setLocator(this);
      return this;
   }

   @Override
   public void disableFinalizeCheck() {
      finalizeCheck = false;
   }

   @Override
   public ClientSessionFactoryInternal connect() throws ActiveMQException {
      return connect(false);
   }

   private ClientSessionFactoryInternal connect(final boolean skipWarnings) throws ActiveMQException {
      ClientSessionFactoryInternal returnFactory = null;

      synchronized (this) {
         // static list of initial connectors
         if (getNumInitialConnectors() > 0 && discoveryGroup == null) {
            returnFactory = (ClientSessionFactoryInternal) staticConnector.connect(skipWarnings);
         }
      }

      if (returnFactory != null) {
         addFactory(returnFactory);
         return returnFactory;
      } else {
         // wait for discovery group to get the list of initial connectors
         return (ClientSessionFactoryInternal) createSessionFactory();
      }
   }

   @Override
   public ClientSessionFactoryInternal connectNoWarnings() throws ActiveMQException {
      return connect(true);
   }

   @Override
   public ServerLocatorImpl setAfterConnectionInternalListener(AfterConnectInternalListener listener) {
      this.afterConnectListener = listener;
      return this;
   }

   @Override
   public AfterConnectInternalListener getAfterConnectInternalListener() {
      return afterConnectListener;
   }

   @Override
   public ClientSessionFactory createSessionFactory(String nodeID) throws Exception {
      TopologyMember topologyMember = topology.getMember(nodeID);

      if (logger.isTraceEnabled()) {
         logger.trace("Creating connection factory towards " + nodeID + " = " + topologyMember + ", topology=" + topology.describe());
      }

      if (topologyMember == null) {
         return null;
      }
      if (topologyMember.getLive() != null) {
         ClientSessionFactoryInternal factory = (ClientSessionFactoryInternal) createSessionFactory(topologyMember.getLive());
         if (topologyMember.getBackup() != null) {
            factory.setBackupConnector(topologyMember.getLive(), topologyMember.getBackup());
         }
         return factory;
      }
      if (topologyMember.getLive() == null && topologyMember.getBackup() != null) {
         // This shouldn't happen, however I wanted this to consider all possible cases
         ClientSessionFactoryInternal factory = (ClientSessionFactoryInternal) createSessionFactory(topologyMember.getBackup());
         return factory;
      }
      // it shouldn't happen
      return null;
   }

   @Override
   public ClientSessionFactory createSessionFactory(final TransportConfiguration transportConfiguration) throws Exception {
      assertOpen();

      initialize();

      ClientSessionFactoryInternal factory = new ClientSessionFactoryImpl(this, transportConfiguration, callTimeout, callFailoverTimeout, clientFailureCheckPeriod, connectionTTL, retryInterval, retryIntervalMultiplier, maxRetryInterval, reconnectAttempts, threadPool, scheduledThreadPool, incomingInterceptors, outgoingInterceptors);

      addToConnecting(factory);
      try {
         try {
            factory.connect(reconnectAttempts, failoverOnInitialConnection);
         } catch (ActiveMQException e1) {
            //we need to make sure is closed just for garbage collection
            factory.close();
            throw e1;
         }
         addFactory(factory);
         return factory;
      } finally {
         removeFromConnecting(factory);
      }
   }

   @Override
   public ClientSessionFactory createSessionFactory(final TransportConfiguration transportConfiguration,
                                                    int reconnectAttempts,
                                                    boolean failoverOnInitialConnection) throws Exception {
      assertOpen();

      initialize();

      ClientSessionFactoryInternal factory = new ClientSessionFactoryImpl(this, transportConfiguration, callTimeout, callFailoverTimeout, clientFailureCheckPeriod, connectionTTL, retryInterval, retryIntervalMultiplier, maxRetryInterval, reconnectAttempts, threadPool, scheduledThreadPool, incomingInterceptors, outgoingInterceptors);

      addToConnecting(factory);
      try {
         try {
            factory.connect(reconnectAttempts, failoverOnInitialConnection);
         } catch (ActiveMQException e1) {
            //we need to make sure is closed just for garbage collection
            factory.close();
            throw e1;
         }
         addFactory(factory);
         return factory;
      } finally {
         removeFromConnecting(factory);
      }
   }

   private void removeFromConnecting(ClientSessionFactoryInternal factory) {
      synchronized (connectingFactories) {
         connectingFactories.remove(factory);
      }
   }

   private void addToConnecting(ClientSessionFactoryInternal factory) {
      synchronized (connectingFactories) {
         assertOpen();
         connectingFactories.add(factory);
      }
   }

   @Override
   public ClientSessionFactory createSessionFactory() throws ActiveMQException {
      assertOpen();

      initialize();

      flushTopology();

      if (this.getNumInitialConnectors() == 0 && discoveryGroup != null) {
         // Wait for an initial broadcast to give us at least one node in the cluster
         long timeout = clusterConnection ? 0 : discoveryGroupConfiguration.getDiscoveryInitialWaitTimeout();
         boolean ok = discoveryGroup.waitForBroadcast(timeout);

         if (!ok) {
            throw ActiveMQClientMessageBundle.BUNDLE.connectionTimedOutInInitialBroadcast();
         }
      }

      ClientSessionFactoryInternal factory = null;

      synchronized (this) {
         boolean retry;
         int attempts = 0;
         do {
            retry = false;

            TransportConfiguration tc = selectConnector();
            if (tc == null) {
               throw ActiveMQClientMessageBundle.BUNDLE.noTCForSessionFactory();
            }

            // try each factory in the list until we find one which works

            try {
               factory = new ClientSessionFactoryImpl(this, tc, callTimeout, callFailoverTimeout, clientFailureCheckPeriod, connectionTTL, retryInterval, retryIntervalMultiplier, maxRetryInterval, reconnectAttempts, threadPool, scheduledThreadPool, incomingInterceptors, outgoingInterceptors);
               try {
                  addToConnecting(factory);
                  factory.connect(initialConnectAttempts, failoverOnInitialConnection);
               } finally {
                  removeFromConnecting(factory);
               }
            } catch (ActiveMQException e) {
               factory.close();
               if (e.getType() == ActiveMQExceptionType.NOT_CONNECTED) {
                  attempts++;

                  synchronized (topologyArrayGuard) {

                     if (topologyArray != null && attempts == topologyArray.length) {
                        throw ActiveMQClientMessageBundle.BUNDLE.cannotConnectToServers();
                     }
                     if (topologyArray == null && attempts == this.getNumInitialConnectors()) {
                        throw ActiveMQClientMessageBundle.BUNDLE.cannotConnectToServers();
                     }
                  }
                  retry = true;
               } else {
                  throw e;
               }
            }
         }
         while (retry);
      }

      // ATM topology is never != null. Checking here just to be consistent with
      // how the sendSubscription happens.
      // in case this ever changes.
      if (topology != null && !factory.waitForTopology(callTimeout, TimeUnit.MILLISECONDS)) {
         factory.cleanup();
         throw ActiveMQClientMessageBundle.BUNDLE.connectionTimedOutOnReceiveTopology(discoveryGroup);
      }

      addFactory(factory);

      return factory;
   }

   public void flushTopology() {
      if (updateArrayActor != null) {
         updateArrayActor.flush(10, TimeUnit.SECONDS);
      }
   }

   @Override
   public boolean isHA() {
      return ha;
   }

   /**
    * @param interceptorList a comma separated string of incoming interceptor class names to be used. Each interceptor needs a default Constructor to be used with this method.
    * @return this
    */
   @Override
   public ServerLocator setIncomingInterceptorList(String interceptorList) {
      feedInterceptors(incomingInterceptors, interceptorList);
      return this;
   }

   @Override
   public String getIncomingInterceptorList() {
      return fromInterceptors(incomingInterceptors);
   }

   /**
    * @param interceptorList a comma separated string of incoming interceptor class names to be used. Each interceptor needs a default Constructor to be used with this method.
    * @return this
    */
   @Override
   public ServerLocator setOutgoingInterceptorList(String interceptorList) {
      feedInterceptors(outgoingInterceptors, interceptorList);
      return this;
   }

   @Override
   public String getOutgoingInterceptorList() {
      return fromInterceptors(outgoingInterceptors);
   }

   @Override
   public boolean isCacheLargeMessagesClient() {
      return cacheLargeMessagesClient;
   }

   @Override
   public ServerLocatorImpl setCacheLargeMessagesClient(final boolean cached) {
      cacheLargeMessagesClient = cached;
      return this;
   }

   @Override
   public long getClientFailureCheckPeriod() {
      return clientFailureCheckPeriod;
   }

   @Override
   public ServerLocatorImpl setClientFailureCheckPeriod(final long clientFailureCheckPeriod) {
      checkWrite();
      this.clientFailureCheckPeriod = clientFailureCheckPeriod;
      return this;
   }

   @Override
   public long getConnectionTTL() {
      return connectionTTL;
   }

   @Override
   public ServerLocatorImpl setConnectionTTL(final long connectionTTL) {
      checkWrite();
      this.connectionTTL = connectionTTL;
      return this;
   }

   @Override
   public long getCallTimeout() {
      return callTimeout;
   }

   @Override
   public ServerLocatorImpl setCallTimeout(final long callTimeout) {
      checkWrite();
      this.callTimeout = callTimeout;
      return this;
   }

   @Override
   public long getCallFailoverTimeout() {
      return callFailoverTimeout;
   }

   @Override
   public ServerLocatorImpl setCallFailoverTimeout(long callFailoverTimeout) {
      checkWrite();
      this.callFailoverTimeout = callFailoverTimeout;
      return this;
   }

   @Override
   public int getMinLargeMessageSize() {
      return minLargeMessageSize;
   }

   @Override
   public ServerLocatorImpl setMinLargeMessageSize(final int minLargeMessageSize) {
      checkWrite();
      this.minLargeMessageSize = minLargeMessageSize;
      return this;
   }

   @Override
   public int getConsumerWindowSize() {
      return consumerWindowSize;
   }

   @Override
   public ServerLocatorImpl setConsumerWindowSize(final int consumerWindowSize) {
      checkWrite();
      this.consumerWindowSize = consumerWindowSize;
      return this;
   }

   @Override
   public int getConsumerMaxRate() {
      return consumerMaxRate;
   }

   @Override
   public ServerLocatorImpl setConsumerMaxRate(final int consumerMaxRate) {
      checkWrite();
      this.consumerMaxRate = consumerMaxRate;
      return this;
   }

   @Override
   public int getConfirmationWindowSize() {
      return confirmationWindowSize;
   }

   @Override
   public ServerLocatorImpl setConfirmationWindowSize(final int confirmationWindowSize) {
      checkWrite();
      this.confirmationWindowSize = confirmationWindowSize;
      return this;
   }

   @Override
   public int getProducerWindowSize() {
      return producerWindowSize;
   }

   @Override
   public ServerLocatorImpl setProducerWindowSize(final int producerWindowSize) {
      checkWrite();
      this.producerWindowSize = producerWindowSize;
      return this;
   }

   @Override
   public int getProducerMaxRate() {
      return producerMaxRate;
   }

   @Override
   public ServerLocatorImpl setProducerMaxRate(final int producerMaxRate) {
      checkWrite();
      this.producerMaxRate = producerMaxRate;
      return this;
   }

   @Override
   public boolean isBlockOnAcknowledge() {
      return blockOnAcknowledge;
   }

   @Override
   public ServerLocatorImpl setBlockOnAcknowledge(final boolean blockOnAcknowledge) {
      checkWrite();
      this.blockOnAcknowledge = blockOnAcknowledge;
      return this;
   }

   @Override
   public boolean isBlockOnDurableSend() {
      return blockOnDurableSend;
   }

   @Override
   public ServerLocatorImpl setBlockOnDurableSend(final boolean blockOnDurableSend) {
      checkWrite();
      this.blockOnDurableSend = blockOnDurableSend;
      return this;
   }

   @Override
   public boolean isBlockOnNonDurableSend() {
      return blockOnNonDurableSend;
   }

   @Override
   public ServerLocatorImpl setBlockOnNonDurableSend(final boolean blockOnNonDurableSend) {
      checkWrite();
      this.blockOnNonDurableSend = blockOnNonDurableSend;
      return this;
   }

   @Override
   public boolean isAutoGroup() {
      return autoGroup;
   }

   @Override
   public ServerLocatorImpl setAutoGroup(final boolean autoGroup) {
      checkWrite();
      this.autoGroup = autoGroup;
      return this;
   }

   @Override
   public boolean isPreAcknowledge() {
      return preAcknowledge;
   }

   @Override
   public ServerLocatorImpl setPreAcknowledge(final boolean preAcknowledge) {
      checkWrite();
      this.preAcknowledge = preAcknowledge;
      return this;
   }

   @Override
   public int getAckBatchSize() {
      return ackBatchSize;
   }

   @Override
   public ServerLocatorImpl setAckBatchSize(final int ackBatchSize) {
      checkWrite();
      this.ackBatchSize = ackBatchSize;
      return this;
   }

   @Override
   public boolean isUseGlobalPools() {
      return useGlobalPools;
   }

   @Override
   public ServerLocatorImpl setUseGlobalPools(final boolean useGlobalPools) {
      checkWrite();
      this.useGlobalPools = useGlobalPools;
      return this;
   }

   @Override
   public int getScheduledThreadPoolMaxSize() {
      return scheduledThreadPoolMaxSize;
   }

   @Override
   public ServerLocatorImpl setScheduledThreadPoolMaxSize(final int scheduledThreadPoolMaxSize) {
      checkWrite();
      this.scheduledThreadPoolMaxSize = scheduledThreadPoolMaxSize;
      return this;
   }

   @Override
   public int getThreadPoolMaxSize() {
      return threadPoolMaxSize;
   }

   @Override
   public ServerLocatorImpl setThreadPoolMaxSize(final int threadPoolMaxSize) {
      checkWrite();
      this.threadPoolMaxSize = threadPoolMaxSize;
      return this;
   }

   @Override
   public long getRetryInterval() {
      return retryInterval;
   }

   @Override
   public ServerLocatorImpl setRetryInterval(final long retryInterval) {
      checkWrite();
      this.retryInterval = retryInterval;
      return this;
   }

   @Override
   public long getMaxRetryInterval() {
      return maxRetryInterval;
   }

   @Override
   public ServerLocatorImpl setMaxRetryInterval(final long retryInterval) {
      checkWrite();
      maxRetryInterval = retryInterval;
      return this;
   }

   @Override
   public double getRetryIntervalMultiplier() {
      return retryIntervalMultiplier;
   }

   @Override
   public ServerLocatorImpl setRetryIntervalMultiplier(final double retryIntervalMultiplier) {
      checkWrite();
      this.retryIntervalMultiplier = retryIntervalMultiplier;
      return this;
   }

   @Override
   public int getReconnectAttempts() {
      return reconnectAttempts;
   }

   @Override
   public ServerLocatorImpl setReconnectAttempts(final int reconnectAttempts) {
      checkWrite();
      this.reconnectAttempts = reconnectAttempts;
      return this;
   }

   @Override
   public ServerLocatorImpl setInitialConnectAttempts(int initialConnectAttempts) {
      checkWrite();
      this.initialConnectAttempts = initialConnectAttempts;
      return this;
   }

   @Override
   public int getInitialConnectAttempts() {
      return initialConnectAttempts;
   }

   @Override
   public boolean isFailoverOnInitialConnection() {
      return this.failoverOnInitialConnection;
   }

   @Override
   public ServerLocatorImpl setFailoverOnInitialConnection(final boolean failover) {
      checkWrite();
      this.failoverOnInitialConnection = failover;
      return this;
   }

   @Override
   public String getConnectionLoadBalancingPolicyClassName() {
      return connectionLoadBalancingPolicyClassName;
   }

   @Override
   public ServerLocatorImpl setConnectionLoadBalancingPolicyClassName(final String loadBalancingPolicyClassName) {
      checkWrite();
      connectionLoadBalancingPolicyClassName = loadBalancingPolicyClassName;
      return this;
   }

   @Override
   public TransportConfiguration[] getStaticTransportConfigurations() {
      if (initialConnectors == null)
         return new TransportConfiguration[]{};
      return Arrays.copyOf(initialConnectors, initialConnectors.length);
   }

   @Override
   public DiscoveryGroupConfiguration getDiscoveryGroupConfiguration() {
      return discoveryGroupConfiguration;
   }

   @Override
   public ServerLocatorImpl addIncomingInterceptor(final Interceptor interceptor) {
      incomingInterceptors.add(interceptor);
      return this;
   }

   @Override
   public ServerLocatorImpl addOutgoingInterceptor(final Interceptor interceptor) {
      outgoingInterceptors.add(interceptor);
      return this;
   }

   @Override
   public boolean removeIncomingInterceptor(final Interceptor interceptor) {
      return incomingInterceptors.remove(interceptor);
   }

   @Override
   public boolean removeOutgoingInterceptor(final Interceptor interceptor) {
      return outgoingInterceptors.remove(interceptor);
   }

   @Override
   public int getInitialMessagePacketSize() {
      return initialMessagePacketSize;
   }

   @Override
   public ServerLocatorImpl setInitialMessagePacketSize(final int size) {
      checkWrite();
      initialMessagePacketSize = size;
      return this;
   }

   @Override
   public ServerLocatorImpl setGroupID(final String groupID) {
      checkWrite();
      this.groupID = groupID;
      return this;
   }

   @Override
   public String getGroupID() {
      return groupID;
   }

   @Override
   public boolean isCompressLargeMessage() {
      return compressLargeMessage;
   }

   @Override
   public ServerLocatorImpl setCompressLargeMessage(boolean avoid) {
      this.compressLargeMessage = avoid;
      return this;
   }

   private void checkWrite() {
      synchronized (stateGuard) {
         if (state != null && state != STATE.CLOSED) {
            throw new IllegalStateException("Cannot set attribute on SessionFactory after it has been used");
         }
      }
   }

   private int getNumInitialConnectors() {
      if (initialConnectors == null)
         return 0;
      return initialConnectors.length;
   }

   @Override
   public ServerLocatorImpl setIdentity(String identity) {
      this.identity = identity;
      return this;
   }

   @Override
   public ServerLocatorImpl setNodeID(String nodeID) {
      this.nodeID = nodeID;
      return this;
   }

   @Override
   public String getNodeID() {
      return nodeID;
   }

   @Override
   public ServerLocatorImpl setClusterConnection(boolean clusterConnection) {
      this.clusterConnection = clusterConnection;
      return this;
   }

   @Override
   public boolean isClusterConnection() {
      return clusterConnection;
   }

   @Override
   public TransportConfiguration getClusterTransportConfiguration() {
      return clusterTransportConfiguration;
   }

   @Override
   public ServerLocatorImpl setClusterTransportConfiguration(TransportConfiguration tc) {
      this.clusterTransportConfiguration = tc;
      return this;
   }

   @Override
   protected void finalize() throws Throwable {
      if (finalizeCheck) {
         close();
      }

      super.finalize();
   }

   @Override
   public void cleanup() {
      doClose(false);
   }

   @Override
   public void close() {
      doClose(true);
   }

   private void doClose(final boolean sendClose) {
      synchronized (stateGuard) {
         if (state == STATE.CLOSED) {
            if (logger.isDebugEnabled()) {
               logger.debug(this + " is already closed when calling closed");
            }
            return;
         }

         state = STATE.CLOSING;
      }
      if (latch != null)
         latch.countDown();

      synchronized (connectingFactories) {
         for (ClientSessionFactoryInternal csf : connectingFactories) {
            csf.causeExit();
         }
      }

      if (discoveryGroup != null) {
         synchronized (this) {
            try {
               discoveryGroup.stop();
            } catch (Exception e) {
               ActiveMQClientLogger.LOGGER.failedToStopDiscovery(e);
            }
         }
      } else {
         staticConnector.disconnect();
      }

      synchronized (connectingFactories) {
         for (ClientSessionFactoryInternal csf : connectingFactories) {
            csf.causeExit();
         }
         for (ClientSessionFactoryInternal csf : connectingFactories) {
            csf.close();
         }
         connectingFactories.clear();
      }

      Set<ClientSessionFactoryInternal> clonedFactory;
      synchronized (factories) {
         clonedFactory = new HashSet<>(factories);

         factories.clear();
      }

      for (ClientSessionFactoryInternal factory : clonedFactory) {
         factory.causeExit();
      }
      for (ClientSessionFactory factory : clonedFactory) {
         if (sendClose) {
            try {
               factory.close();
            } catch (Throwable e) {
               logger.debug(e.getMessage(), e);
               factory.cleanup();
            }
         } else {
            factory.cleanup();
         }
      }

      if (shutdownPool) {
         if (threadPool != null) {
            ExecutorService executorService = (ExecutorService) threadPool;
            executorService.shutdown();

            try {
               if (!executorService.awaitTermination(10000, TimeUnit.MILLISECONDS)) {
                  ActiveMQClientLogger.LOGGER.timedOutWaitingForTermination();
               }
            } catch (InterruptedException e) {
               throw new ActiveMQInterruptedException(e);
            }
         }

         if (scheduledThreadPool != null) {
            scheduledThreadPool.shutdown();

            try {
               if (!scheduledThreadPool.awaitTermination(10000, TimeUnit.MILLISECONDS)) {
                  ActiveMQClientLogger.LOGGER.timedOutWaitingForScheduledPoolTermination();
               }
            } catch (InterruptedException e) {
               throw new ActiveMQInterruptedException(e);
            }
         }
      }
      synchronized (stateGuard) {
         state = STATE.CLOSED;
      }
   }

   /**
    * This is directly called when the connection to the node is gone,
    * or when the node sends a disconnection.
    * Look for callers of this method!
    */
   @Override
   public void notifyNodeDown(final long eventTime, final String nodeID) {

      if (!ha) {
         // there's no topology here
         return;
      }

      if (logger.isTraceEnabled()) {
         logger.trace("nodeDown " + this + " nodeID=" + nodeID + " as being down", new Exception("trace"));
      }

      topology.removeMember(eventTime, nodeID);

      if (clusterConnection) {
         updateArraysAndPairs(eventTime);
      } else {
         if (topology.isEmpty()) {
            // Resetting the topology to its original condition as it was brand new
            receivedTopology = false;
            topologyArray = null;
         } else {
            updateArraysAndPairs(eventTime);

            if (topology.nodes() == 1 && topology.getMember(this.nodeID) != null) {
               // Resetting the topology to its original condition as it was brand new
               receivedTopology = false;
            }
         }
      }

   }

   @Override
   public void notifyNodeUp(long uniqueEventID,
                            final String nodeID,
                            final String backupGroupName,
                            final String scaleDownGroupName,
                            final Pair<TransportConfiguration, TransportConfiguration> connectorPair,
                            final boolean last) {
      if (logger.isTraceEnabled()) {
         logger.trace("NodeUp " + this + "::nodeID=" + nodeID + ", connectorPair=" + connectorPair, new Exception("trace"));
      }

      TopologyMemberImpl member = new TopologyMemberImpl(nodeID, backupGroupName, scaleDownGroupName, connectorPair.getA(), connectorPair.getB());

      topology.updateMember(uniqueEventID, nodeID, member);

      TopologyMember actMember = topology.getMember(nodeID);

      if (actMember != null && actMember.getLive() != null && actMember.getBackup() != null) {
         HashSet<ClientSessionFactory> clonedFactories = new HashSet<>();
         synchronized (factories) {
            clonedFactories.addAll(factories);
         }

         for (ClientSessionFactory factory : clonedFactories) {
            ((ClientSessionFactoryInternal) factory).setBackupConnector(actMember.getLive(), actMember.getBackup());
         }
      }

      updateArraysAndPairs(uniqueEventID);

      if (last) {
         receivedTopology = true;
      }
   }

   @Override
   public String toString() {
      if (identity != null) {
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
   private void updateArraysAndPairs(long time) {
      if (updateArrayActor == null) {
         // if for some reason we don't have an actor, just go straight
         internalUpdateArray(time);
      } else {
         updateArrayActor.act(time);
      }
   }

   private void internalUpdateArray(long time) {
      synchronized (topologyArrayGuard) {
         Collection<TopologyMemberImpl> membersCopy = topology.getMembers();

         Pair<TransportConfiguration, TransportConfiguration>[] topologyArrayLocal = (Pair<TransportConfiguration, TransportConfiguration>[]) Array.newInstance(Pair.class, membersCopy.size());

         int count = 0;
         for (TopologyMemberImpl pair : membersCopy) {
            Pair<TransportConfiguration, TransportConfiguration> transportConfigs = pair.getConnector();
            topologyArrayLocal[count++] = new Pair<>(protocolManagerFactory.adaptTransportConfiguration(transportConfigs.getA()),
                                                     protocolManagerFactory.adaptTransportConfiguration(transportConfigs.getB()));
         }

         this.topologyArray = topologyArrayLocal;
      }
   }

   @Override
   public synchronized void connectorsChanged(List<DiscoveryEntry> newConnectors) {
      if (receivedTopology) {
         return;
      }
      TransportConfiguration[] newInitialconnectors = (TransportConfiguration[]) Array.newInstance(TransportConfiguration.class, newConnectors.size());

      int count = 0;
      for (DiscoveryEntry entry : newConnectors) {
         newInitialconnectors[count++] = entry.getConnector();

         if (ha && topology.getMember(entry.getNodeID()) == null) {
            TopologyMemberImpl member = new TopologyMemberImpl(entry.getNodeID(), null, null, entry.getConnector(), null);
            // on this case we set it as zero as any update coming from server should be accepted
            topology.updateMember(0, entry.getNodeID(), member);
         }
      }

      this.initialConnectors = newInitialconnectors.length == 0 ? null : newInitialconnectors;

      if (clusterConnection && !receivedTopology && this.getNumInitialConnectors() > 0) {
         // The node is alone in the cluster. We create a connection to the new node
         // to trigger the node notification to form the cluster.

         Runnable connectRunnable = new Runnable() {
            @Override
            public void run() {
               try {
                  connect();
               } catch (ActiveMQException e) {
                  ActiveMQClientLogger.LOGGER.errorConnectingToNodes(e);
               }
            }
         };
         if (startExecutor != null) {
            startExecutor.execute(connectRunnable);
         } else {
            connectRunnable.run();
         }
      }
   }

   @Override
   public void factoryClosed(final ClientSessionFactory factory) {
      boolean isEmpty;
      synchronized (factories) {
         factories.remove(factory);
         isEmpty = factories.isEmpty();
      }

      if (!clusterConnection && isEmpty) {
         receivedTopology = false;
         topologyArray = null;
      }
   }

   @Override
   public ServerLocator setUseTopologyForLoadBalancing(boolean useTopologyForLoadBalancing) {
      this.useTopologyForLoadBalancing = useTopologyForLoadBalancing;
      return this;
   }

   @Override
   public boolean getUseTopologyForLoadBalancing() {
      return useTopologyForLoadBalancing;
   }

   @Override
   public Topology getTopology() {
      return topology;
   }

   @Override
   public boolean isConnectable() {
      return getNumInitialConnectors() > 0 || getDiscoveryGroupConfiguration() != null;
   }

   @Override
   public ServerLocatorImpl addClusterTopologyListener(final ClusterTopologyListener listener) {
      topology.addClusterTopologyListener(listener);
      return this;
   }

   @Override
   public void removeClusterTopologyListener(final ClusterTopologyListener listener) {
      topology.removeClusterTopologyListener(listener);
   }

   /**
    * for tests only and not part of the public interface. Do not use it.
    *
    * @return
    */
   public TransportConfiguration[] getInitialConnectors() {
      return initialConnectors;
   }

   private void addFactory(ClientSessionFactoryInternal factory) {
      if (factory == null) {
         return;
      }

      if (isClosed()) {
         factory.close();
         return;
      }

      TransportConfiguration backup = null;

      if (ha) {
         backup = topology.getBackupForConnector((Connector) factory.getConnector());
      }

      factory.setBackupConnector(factory.getConnectorConfiguration(), backup);

      synchronized (factories) {
         factories.add(factory);
      }
   }

   private final class StaticConnector implements Serializable {

      private static final long serialVersionUID = 6772279632415242634L;

      private List<Connector> connectors;

      public ClientSessionFactory connect(boolean skipWarnings) throws ActiveMQException {
         assertOpen();

         initialize();

         createConnectors();

         try {

            int retryNumber = 0;
            while (!isClosed()) {
               retryNumber++;
               for (Connector conn : connectors) {
                  if (logger.isDebugEnabled()) {
                     logger.debug(this + "::Submitting connect towards " + conn);
                  }

                  ClientSessionFactory csf = conn.tryConnect();

                  if (csf != null) {
                     csf.getConnection().addFailureListener(new FailureListener() {
                        // Case the node where the cluster connection was connected is gone, we need to restart the
                        // connection
                        @Override
                        public void connectionFailed(ActiveMQException exception, boolean failedOver) {
                           if (clusterConnection && exception.getType() == ActiveMQExceptionType.DISCONNECTED) {
                              try {
                                 ServerLocatorImpl.this.start(startExecutor);
                              } catch (Exception e) {
                                 // There isn't much to be done if this happens here
                                 ActiveMQClientLogger.LOGGER.errorStartingLocator(e);
                              }
                           }
                        }

                        @Override
                        public void connectionFailed(final ActiveMQException me,
                                                     boolean failedOver,
                                                     String scaleDownTargetNodeID) {
                           connectionFailed(me, failedOver);
                        }

                        @Override
                        public String toString() {
                           return "FailureListener('restarts cluster connections')";
                        }
                     });

                     if (logger.isDebugEnabled()) {
                        logger.debug("Returning " + csf +
                                        " after " +
                                        retryNumber +
                                        " retries on StaticConnector " +
                                        ServerLocatorImpl.this);
                     }

                     return csf;
                  }
               }

               if (initialConnectAttempts >= 0 && retryNumber > initialConnectAttempts) {
                  break;
               }

               if (latch.await(retryInterval, TimeUnit.MILLISECONDS))
                  return null;
            }

         } catch (RejectedExecutionException e) {
            if (isClosed() || skipWarnings)
               return null;
            logger.debug("Rejected execution", e);
            throw e;
         } catch (Exception e) {
            if (isClosed() || skipWarnings)
               return null;
            ActiveMQClientLogger.LOGGER.errorConnectingToNodes(e);
            throw ActiveMQClientMessageBundle.BUNDLE.cannotConnectToStaticConnectors(e);
         }

         if (isClosed() || skipWarnings) {
            return null;
         }

         ActiveMQClientLogger.LOGGER.errorConnectingToNodes(traceException);
         throw ActiveMQClientMessageBundle.BUNDLE.cannotConnectToStaticConnectors2();
      }

      private synchronized void createConnectors() {
         if (connectors != null) {
            for (Connector conn : connectors) {
               if (conn != null) {
                  conn.disconnect();
               }
            }
         }
         connectors = new ArrayList<>();
         if (initialConnectors != null) {
            for (TransportConfiguration initialConnector : initialConnectors) {
               ClientSessionFactoryInternal factory = new ClientSessionFactoryImpl(ServerLocatorImpl.this, initialConnector, callTimeout, callFailoverTimeout, clientFailureCheckPeriod, connectionTTL, retryInterval, retryIntervalMultiplier, maxRetryInterval, reconnectAttempts, threadPool, scheduledThreadPool, incomingInterceptors, outgoingInterceptors);

               factory.disableFinalizeCheck();

               connectors.add(new Connector(initialConnector, factory));
            }
         }
      }

      public synchronized void disconnect() {
         if (connectors != null) {
            for (Connector connector : connectors) {
               connector.disconnect();
            }
         }
      }

      @Override
      protected void finalize() throws Throwable {
         if (!isClosed() && finalizeCheck) {
            ActiveMQClientLogger.LOGGER.serverLocatorNotClosed(traceException, System.identityHashCode(this));

            close();
         }

         super.finalize();
      }

      private final class Connector {

         private final TransportConfiguration initialConnector;

         private volatile ClientSessionFactoryInternal factory;

         private Connector(TransportConfiguration initialConnector, ClientSessionFactoryInternal factory) {
            this.initialConnector = initialConnector;
            this.factory = factory;
         }

         public ClientSessionFactory tryConnect() throws ActiveMQException {
            if (logger.isDebugEnabled()) {
               logger.debug(this + "::Trying to connect to " + factory);
            }
            try {
               ClientSessionFactoryInternal factoryToUse = factory;
               if (factoryToUse != null) {
                  addToConnecting(factoryToUse);

                  try {
                     factoryToUse.connect(1, false);
                  } finally {
                     removeFromConnecting(factoryToUse);
                  }
               }
               return factoryToUse;
            } catch (ActiveMQException e) {
               logger.debug(this + "::Exception on establish connector initial connection", e);
               return null;
            }
         }

         public void disconnect() {
            if (factory != null) {
               factory.causeExit();
               factory.cleanup();
               factory = null;
            }
         }

         @Override
         public String toString() {
            return "Connector [initialConnector=" + initialConnector + "]";
         }

      }
   }

   private void assertOpen() {
      synchronized (stateGuard) {
         if (state != null && state != STATE.INITIALIZED) {
            throw new IllegalStateException("Server locator is closed (maybe it was garbage collected)");
         }
      }
   }

   @Override
   public boolean isClosed() {
      synchronized (stateGuard) {
         return state != STATE.INITIALIZED;
      }
   }

   private Object writeReplace() throws ObjectStreamException {
      ServerLocatorImpl clone = new ServerLocatorImpl(this);
      return clone;
   }

   public boolean isReceivedTopology() {
      return receivedTopology;
   }

   private String fromInterceptors(final List<Interceptor> interceptors) {
      StringBuffer buffer = new StringBuffer();
      boolean first = true;
      for (Interceptor value : interceptors) {
         if (!first) {
            buffer.append(",");
         }
         first = false;
         buffer.append(value.getClass().getName());
      }

      return buffer.toString();
   }

   private void feedInterceptors(final List<Interceptor> interceptors, final String interceptorList) {
      interceptors.clear();

      if (interceptorList == null || interceptorList.trim().equals("")) {
         return;
      }
      AccessController.doPrivileged(new PrivilegedAction<Object>() {
         @Override
         public Object run() {

            String[] arrayInterceptor = interceptorList.split(",");
            for (String strValue : arrayInterceptor) {
               Interceptor interceptor = (Interceptor) ClassloadingUtil.newInstanceFromClassLoader(strValue.trim());
               interceptors.add(interceptor);
            }
            return null;
         }
      });

   }

}
