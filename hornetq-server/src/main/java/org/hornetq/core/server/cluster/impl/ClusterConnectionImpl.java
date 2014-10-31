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
package org.hornetq.core.server.cluster.impl;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.hornetq.api.core.DiscoveryGroupConfiguration;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClusterTopologyListener;
import org.hornetq.api.core.client.TopologyMember;
import org.hornetq.api.core.management.ManagementHelper;
import org.hornetq.api.core.management.NotificationType;
import org.hornetq.core.client.impl.AfterConnectInternalListener;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.client.impl.ServerLocatorImpl;
import org.hornetq.core.client.impl.ServerLocatorInternal;
import org.hornetq.core.client.impl.Topology;
import org.hornetq.core.client.impl.TopologyMemberImpl;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.Bindings;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.postoffice.impl.PostOfficeImpl;
import org.hornetq.core.protocol.ServerPacketDecoder;
import org.hornetq.core.server.HornetQMessageBundle;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServerLogger;
import org.hornetq.core.server.NodeManager;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.cluster.Bridge;
import org.hornetq.core.server.cluster.ClusterConnection;
import org.hornetq.core.server.cluster.ClusterControl;
import org.hornetq.core.server.cluster.ClusterManager;
import org.hornetq.core.server.cluster.ClusterManager.IncomingInterceptorLookingForExceptionMessage;
import org.hornetq.core.server.cluster.MessageFlowRecord;
import org.hornetq.core.server.cluster.RemoteQueueBinding;
import org.hornetq.core.server.group.impl.Proposal;
import org.hornetq.core.server.group.impl.Response;
import org.hornetq.core.server.management.ManagementService;
import org.hornetq.core.server.management.Notification;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.utils.ExecutorFactory;
import org.hornetq.utils.FutureLatch;
import org.hornetq.utils.TypedProperties;

import static org.hornetq.api.core.management.NotificationType.CONSUMER_CLOSED;
import static org.hornetq.api.core.management.NotificationType.CONSUMER_CREATED;

/**
 * A ClusterConnectionImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author Clebert Suconic
 */
public final class ClusterConnectionImpl implements ClusterConnection, AfterConnectInternalListener
{
   private static final boolean isTrace = HornetQServerLogger.LOGGER.isTraceEnabled();

   private final ExecutorFactory executorFactory;

   private final Executor executor;

   private final HornetQServer server;

   private final PostOffice postOffice;

   private final ManagementService managementService;

   private final SimpleString name;

   private final SimpleString address;

   private final long clientFailureCheckPeriod;

   private final long connectionTTL;

   private final long retryInterval;

   private final long callTimeout;

   private final long callFailoverTimeout;

   private final double retryIntervalMultiplier;

   private final long maxRetryInterval;

   private final int initialConnectAttempts;

   private final int reconnectAttempts;

   private final boolean useDuplicateDetection;

   private final boolean routeWhenNoConsumers;

   private final int confirmationWindowSize;

   /**
    * Guard for the field {@link #records}. Note that the field is {@link ConcurrentHashMap},
    * however we need the guard to synchronize multiple step operations during topology updates.
    */
   private final Object recordsGuard = new Object();
   private final Map<String, MessageFlowRecord> records = new ConcurrentHashMap<String, MessageFlowRecord>();

   private final Map<String, MessageFlowRecord> disconnectedRecords = new ConcurrentHashMap<String, MessageFlowRecord>();

   private final ScheduledExecutorService scheduledExecutor;

   private final int maxHops;

   private final NodeManager nodeManager;

   private volatile boolean started;

   private final String clusterUser;

   private final String clusterPassword;

   private final ClusterConnector clusterConnector;

   private ServerLocatorInternal serverLocator;

   private final TransportConfiguration connector;

   private final boolean allowDirectConnectionsOnly;

   private final Set<TransportConfiguration> allowableConnections = new HashSet<TransportConfiguration>();

   private final ClusterManager manager;

   private final int minLargeMessageSize;


   // Stuff that used to be on the ClusterManager

   private final Topology topology = new Topology(this);

   private volatile boolean stopping = false;

   private LiveNotifier liveNotifier = null;

   private final long clusterNotificationInterval;

   private final int clusterNotificationAttempts;

   public ClusterConnectionImpl(final ClusterManager manager,
                                final TransportConfiguration[] staticTranspConfigs,
                                final TransportConfiguration connector,
                                final SimpleString name,
                                final SimpleString address,
                                final int minLargeMessageSize,
                                final long clientFailureCheckPeriod,
                                final long connectionTTL,
                                final long retryInterval,
                                final double retryIntervalMultiplier,
                                final long maxRetryInterval,
                                final int initialConnectAttempts,
                                final int reconnectAttempts,
                                final long callTimeout,
                                final long callFailoverTimeout,
                                final boolean useDuplicateDetection,
                                final boolean routeWhenNoConsumers,
                                final int confirmationWindowSize,
                                final ExecutorFactory executorFactory,
                                final HornetQServer server,
                                final PostOffice postOffice,
                                final ManagementService managementService,
                                final ScheduledExecutorService scheduledExecutor,
                                final int maxHops,
                                final NodeManager nodeManager,
                                final String clusterUser,
                                final String clusterPassword,
                                final boolean allowDirectConnectionsOnly,
                                final long clusterNotificationInterval,
                                final int clusterNotificationAttempts) throws Exception
   {
      this.nodeManager = nodeManager;

      this.connector = connector;

      this.name = name;

      this.address = address;

      this.clientFailureCheckPeriod = clientFailureCheckPeriod;

      this.connectionTTL = connectionTTL;

      this.retryInterval = retryInterval;

      this.retryIntervalMultiplier = retryIntervalMultiplier;

      this.maxRetryInterval = maxRetryInterval;

      this.initialConnectAttempts = initialConnectAttempts;

      this.reconnectAttempts = reconnectAttempts;

      this.useDuplicateDetection = useDuplicateDetection;

      this.routeWhenNoConsumers = routeWhenNoConsumers;

      this.confirmationWindowSize = confirmationWindowSize;

      this.executorFactory = executorFactory;

      this.clusterNotificationInterval = clusterNotificationInterval;

      this.clusterNotificationAttempts = clusterNotificationAttempts;

      this.executor = executorFactory.getExecutor();

      this.topology.setExecutor(executor);

      this.server = server;

      this.postOffice = postOffice;

      this.managementService = managementService;

      this.scheduledExecutor = scheduledExecutor;

      this.maxHops = maxHops;

      this.clusterUser = clusterUser;

      this.clusterPassword = clusterPassword;

      this.allowDirectConnectionsOnly = allowDirectConnectionsOnly;

      this.manager = manager;

      this.callTimeout = callTimeout;

      this.callFailoverTimeout = callFailoverTimeout;

      this.minLargeMessageSize = minLargeMessageSize;

      clusterConnector = new StaticClusterConnector(staticTranspConfigs);

      if (staticTranspConfigs != null && staticTranspConfigs.length > 0)
      {
         // a cluster connection will connect to other nodes only if they are directly connected
         // through a static list of connectors or broadcasting using UDP.
         if (allowDirectConnectionsOnly)
         {
            allowableConnections.addAll(Arrays.asList(staticTranspConfigs));
         }
      }

   }

   public ClusterConnectionImpl(final ClusterManager manager,
                                DiscoveryGroupConfiguration dg,
                                final TransportConfiguration connector,
                                final SimpleString name,
                                final SimpleString address,
                                final int minLargeMessageSize,
                                final long clientFailureCheckPeriod,
                                final long connectionTTL,
                                final long retryInterval,
                                final double retryIntervalMultiplier,
                                final long maxRetryInterval,
                                final int initialConnectAttempts,
                                final int reconnectAttempts,
                                final long callTimeout,
                                final long callFailoverTimeout,
                                final boolean useDuplicateDetection,
                                final boolean routeWhenNoConsumers,
                                final int confirmationWindowSize,
                                final ExecutorFactory executorFactory,
                                final HornetQServer server,
                                final PostOffice postOffice,
                                final ManagementService managementService,
                                final ScheduledExecutorService scheduledExecutor,
                                final int maxHops,
                                final NodeManager nodeManager,
                                final String clusterUser,
                                final String clusterPassword,
                                final boolean allowDirectConnectionsOnly,
                                final long clusterNotificationInterval,
                                final int clusterNotificationAttempts) throws Exception
   {
      this.nodeManager = nodeManager;

      this.connector = connector;

      this.name = name;

      this.address = address;

      this.clientFailureCheckPeriod = clientFailureCheckPeriod;

      this.connectionTTL = connectionTTL;

      this.retryInterval = retryInterval;

      this.retryIntervalMultiplier = retryIntervalMultiplier;

      this.maxRetryInterval = maxRetryInterval;

      this.minLargeMessageSize = minLargeMessageSize;

      this.initialConnectAttempts = initialConnectAttempts;

      this.reconnectAttempts = reconnectAttempts;

      this.callTimeout = callTimeout;

      this.callFailoverTimeout = callFailoverTimeout;

      this.useDuplicateDetection = useDuplicateDetection;

      this.routeWhenNoConsumers = routeWhenNoConsumers;

      this.confirmationWindowSize = confirmationWindowSize;

      this.executorFactory = executorFactory;

      this.clusterNotificationInterval = clusterNotificationInterval;

      this.clusterNotificationAttempts = clusterNotificationAttempts;

      this.executor = executorFactory.getExecutor();

      this.topology.setExecutor(executor);

      this.server = server;

      this.postOffice = postOffice;

      this.managementService = managementService;

      this.scheduledExecutor = scheduledExecutor;

      this.maxHops = maxHops;

      this.clusterUser = clusterUser;

      this.clusterPassword = clusterPassword;

      this.allowDirectConnectionsOnly = allowDirectConnectionsOnly;

      clusterConnector = new DiscoveryClusterConnector(dg);

      this.manager = manager;
   }

   public void start() throws Exception
   {
      synchronized (this)
      {
         if (started)
         {
            return;
         }

         stopping = false;
         started = true;
         activate();
      }

   }

   public void flushExecutor()
   {
      FutureLatch future = new FutureLatch();
      executor.execute(future);
      if (!future.await(10000))
      {
         server.threadDump("Couldn't finish executor on " + this);
      }
   }

   public void stop() throws Exception
   {
      if (!started)
      {
         return;
      }
      stopping = true;
      if (HornetQServerLogger.LOGGER.isDebugEnabled())
      {
         HornetQServerLogger.LOGGER.debug(this + "::stopping ClusterConnection");
      }

      if (serverLocator != null)
      {
         serverLocator.removeClusterTopologyListener(this);
      }

      HornetQServerLogger.LOGGER.debug("Cluster connection being stopped for node" + nodeManager.getNodeId() +
                                          ", server = " +
                                          this.server +
                                          " serverLocator = " +
                                          serverLocator);

      synchronized (this)
      {
         for (MessageFlowRecord record : records.values())
         {
            try
            {
               record.close();
            }
            catch (Exception ignore)
            {
            }
         }
      }

      if (managementService != null)
      {
         TypedProperties props = new TypedProperties();
         props.putSimpleStringProperty(new SimpleString("name"), name);
         Notification notification = new Notification(nodeManager.getNodeId().toString(),
                                                      NotificationType.CLUSTER_CONNECTION_STOPPED,
                                                      props);
         managementService.sendNotification(notification);
      }
      executor.execute(new Runnable()
      {
         public void run()
         {
            synchronized (ClusterConnectionImpl.this)
            {
               closeLocator(serverLocator);
               serverLocator = null;
            }

         }
      });

      started = false;
   }

   /**
    * @param locator
    */
   private void closeLocator(final ServerLocatorInternal locator)
   {
      if (locator != null)
         locator.close();
   }

   private TopologyMember getLocalMember()
   {
      return topology.getMember(manager.getNodeId());
   }

   public void addClusterTopologyListener(final ClusterTopologyListener listener)
   {
      topology.addClusterTopologyListener(listener);
   }

   public void removeClusterTopologyListener(final ClusterTopologyListener listener)
   {
      topology.removeClusterTopologyListener(listener);
   }

   public Topology getTopology()
   {
      return topology;
   }

   public void nodeAnnounced(final long uniqueEventID,
                             final String nodeID,
                             final String backupGroupName,
                             final String scaleDownGroupName,
                             final Pair<TransportConfiguration, TransportConfiguration> connectorPair,
                             final boolean backup)
   {
      if (HornetQServerLogger.LOGGER.isDebugEnabled())
      {
         HornetQServerLogger.LOGGER.debug(this + "::NodeAnnounced, backup=" + backup + nodeID + connectorPair);
      }

      TransportConfiguration live = connectorPair.getA();
      TransportConfiguration backupTC = connectorPair.getB();
      TopologyMemberImpl newMember = new TopologyMemberImpl(nodeID, backupGroupName, scaleDownGroupName, live, backupTC);
      newMember.setUniqueEventID(uniqueEventID);
      if (backup)
      {
         topology.updateBackup(new TopologyMemberImpl(nodeID, backupGroupName, scaleDownGroupName, live, backupTC));
      }
      else
      {
         topology.updateMember(uniqueEventID, nodeID, newMember);
      }
   }

   @Override
   public void onConnection(ClientSessionFactoryInternal sf)
   {
      TopologyMember localMember = getLocalMember();
      if (localMember != null)
      {
         ClusterControl clusterControl = manager.getClusterController().connectToNodeInCluster(sf);
         try
         {
            clusterControl.authorize();
            clusterControl.sendNodeAnnounce(localMember.getUniqueEventID(),
                                            manager.getNodeId(),
                                            manager.getBackupGroupName(),
                                            manager.getScaleDownGroupName(),
                                            false,
                                            localMember.getLive(), localMember.getBackup());
         }
         catch (HornetQException e)
         {
            HornetQServerLogger.LOGGER.clusterControlAuthfailure();
         }
      }
      else
      {
         HornetQServerLogger.LOGGER.noLocalMemborOnClusterConnection(this);
      }

      // TODO: shouldn't we send the current time here? and change the current topology?
      // sf.sendNodeAnnounce(System.currentTimeMillis(),
      // manager.getNodeId(),
      // false,
      // localMember.getConnector().a,
      // localMember.getConnector().b);
   }

   public boolean isStarted()
   {
      return started;
   }

   public SimpleString getName()
   {
      return name;
   }

   public String getNodeID()
   {
      return nodeManager.getNodeId().toString();
   }

   public HornetQServer getServer()
   {
      return server;
   }

   public boolean isNodeActive(String nodeId)
   {
      MessageFlowRecord rec = records.get(nodeId);
      if (rec == null)
      {
         return false;
      }
      return rec.getBridge().isConnected();
   }

   public Map<String, String> getNodes()
   {
      synchronized (recordsGuard)
      {
         Map<String, String> nodes = new HashMap<String, String>();
         for (Entry<String, MessageFlowRecord> entry : records.entrySet())
         {
            RemotingConnection fwdConnection = entry.getValue().getBridge().getForwardingConnection();
            if (fwdConnection != null)
            {
               nodes.put(entry.getKey(), fwdConnection.getRemoteAddress());
            }
         }
         return nodes;
      }
   }

   private synchronized void activate() throws Exception
   {
      if (!started)
      {
         return;
      }

      if (HornetQServerLogger.LOGGER.isDebugEnabled())
      {
         HornetQServerLogger.LOGGER.debug("Activating cluster connection nodeID=" + nodeManager.getNodeId() + " for server=" + this.server);
      }

      liveNotifier = new LiveNotifier();
      liveNotifier.updateAsLive();
      liveNotifier.schedule();

      serverLocator = clusterConnector.createServerLocator();

      if (serverLocator != null)
      {

         if (!useDuplicateDetection)
         {
            HornetQServerLogger.LOGGER.debug("DuplicateDetection is disabled, sending clustered messages blocked");
         }

         final TopologyMember currentMember = topology.getMember(manager.getNodeId());

         if (currentMember == null)
         {
            // sanity check only
            throw new IllegalStateException("InternalError! The ClusterConnection doesn't know about its own node = " + this);
         }

         serverLocator.setNodeID(nodeManager.getNodeId().toString());
         serverLocator.setIdentity("(main-ClusterConnection::" + server.toString() + ")");
         serverLocator.setReconnectAttempts(0);
         serverLocator.setClusterConnection(true);
         serverLocator.setClusterTransportConfiguration(connector);
         serverLocator.setInitialConnectAttempts(-1);
         serverLocator.setClientFailureCheckPeriod(clientFailureCheckPeriod);
         serverLocator.setConnectionTTL(connectionTTL);
         serverLocator.setConfirmationWindowSize(confirmationWindowSize);
         // if not using duplicate detection, we will send blocked
         serverLocator.setBlockOnDurableSend(!useDuplicateDetection);
         serverLocator.setBlockOnNonDurableSend(!useDuplicateDetection);
         serverLocator.setCallTimeout(callTimeout);
         serverLocator.setCallFailoverTimeout(callFailoverTimeout);
         // No producer flow control on the bridges, as we don't want to lock the queues
         serverLocator.setProducerWindowSize(-1);

         if (retryInterval > 0)
         {
            this.serverLocator.setRetryInterval(retryInterval);
         }

         serverLocator.setAfterConnectionInternalListener(this);

         serverLocator.setPacketDecoder(ServerPacketDecoder.INSTANCE);

         serverLocator.start(server.getExecutorFactory().getExecutor());
      }

      if (managementService != null)
      {
         TypedProperties props = new TypedProperties();
         props.putSimpleStringProperty(new SimpleString("name"), name);
         Notification notification = new Notification(nodeManager.getNodeId().toString(),
                                                      NotificationType.CLUSTER_CONNECTION_STARTED,
                                                      props);
         HornetQServerLogger.LOGGER.debug("sending notification: " + notification);
         managementService.sendNotification(notification);
      }
      //we add as a listener after we have sent the cluster start notif as the listener may start sending notifs before
      addClusterTopologyListener(this);
   }

   public TransportConfiguration getConnector()
   {
      return connector;
   }

   // ClusterTopologyListener implementation ------------------------------------------------------------------

   public void nodeDown(final long eventUID, final String nodeID)
   {
      /*
      * we dont do anything when a node down is received. The bridges will take care themselves when they should disconnect
      * and/or clear their bindings. This is to avoid closing a record when we don't want to.
      * */
   }

   @Override
   public void nodeUP(final TopologyMember topologyMember, final boolean last)
   {
      if (stopping)
      {
         return;
      }
      final String nodeID = topologyMember.getNodeId();
      if (HornetQServerLogger.LOGGER.isDebugEnabled())
      {
         String ClusterTestBase = "receiving nodeUP for nodeID=";
         HornetQServerLogger.LOGGER.debug(this + ClusterTestBase + nodeID + " connectionPair=" + topologyMember);
      }
      // discard notifications about ourselves unless its from our backup

      if (nodeID.equals(nodeManager.getNodeId().toString()))
      {
         if (HornetQServerLogger.LOGGER.isTraceEnabled())
         {
            HornetQServerLogger.LOGGER.trace(this + "::informing about backup to itself, nodeUUID=" +
                                                nodeManager.getNodeId() + ", connectorPair=" + topologyMember + ", this = " + this);
         }
         return;
      }

      // if the node is more than 1 hop away, we do not create a bridge for direct cluster connection
      if (allowDirectConnectionsOnly && !allowableConnections.contains(topologyMember.getLive()))
      {
         return;
      }

      // FIXME required to prevent cluster connections w/o discovery group
      // and empty static connectors to create bridges... ulgy!
      if (serverLocator == null)
      {
         return;
      }
      /*we don't create bridges to backups*/
      if (topologyMember.getLive() == null)
      {
         if (isTrace)
         {
            HornetQServerLogger.LOGGER.trace(this + " ignoring call with nodeID=" + nodeID + ", topologyMember=" +
                                                topologyMember + ", last=" + last);
         }
         return;
      }

      synchronized (recordsGuard)
      {
         try
         {
            MessageFlowRecord record = records.get(nodeID);

            if (record == null)
            {
               if (HornetQServerLogger.LOGGER.isDebugEnabled())
               {
                  HornetQServerLogger.LOGGER.debug(this + "::Creating record for nodeID=" + nodeID + ", topologyMember=" +
                                                      topologyMember);
               }

               // New node - create a new flow record

               final SimpleString queueName = new SimpleString("sf." + name + "." + nodeID);

               Binding queueBinding = postOffice.getBinding(queueName);

               Queue queue;

               if (queueBinding != null)
               {
                  queue = (Queue) queueBinding.getBindable();
               }
               else
               {
                  // Add binding in storage so the queue will get reloaded on startup and we can find it - it's never
                  // actually routed to at that address though
                  queue = server.createQueue(queueName, queueName, null, true, false);
               }

               // There are a few things that will behave differently when it's an internal queue
               // such as we don't hold groupIDs inside the SnF queue
               queue.setInternalQueue(true);

               createNewRecord(topologyMember.getUniqueEventID(), nodeID, topologyMember.getLive(), queueName, queue, true);
            }
            else
            {
               if (isTrace)
               {
                  HornetQServerLogger.LOGGER.trace(this + " ignored nodeUp record for " + topologyMember + " on nodeID=" +
                                                      nodeID + " as the record already existed");
               }
            }
         }
         catch (Exception e)
         {
            HornetQServerLogger.LOGGER.errorUpdatingTopology(e);
         }
      }
   }

   public synchronized void informClusterOfBackup()
   {
      String nodeID = server.getNodeID().toString();

      TopologyMemberImpl localMember = new TopologyMemberImpl(nodeID, null, null, null, connector);

      topology.updateAsLive(nodeID, localMember);
   }

   private void createNewRecord(final long eventUID,
                                final String targetNodeID,
                                final TransportConfiguration connector,
                                final SimpleString queueName,
                                final Queue queue,
                                final boolean start) throws Exception
   {
      String nodeId;

      synchronized (this)
      {
         if (!started)
         {
            return;
         }

         if (serverLocator == null)
         {
            return;
         }

         nodeId = serverLocator.getNodeID();
      }

      final ServerLocatorInternal targetLocator = new ServerLocatorImpl(topology, true, connector);

      targetLocator.setReconnectAttempts(0);

      targetLocator.setInitialConnectAttempts(0);
      targetLocator.setClientFailureCheckPeriod(clientFailureCheckPeriod);
      targetLocator.setConnectionTTL(connectionTTL);
      targetLocator.setInitialConnectAttempts(0);

      targetLocator.setConfirmationWindowSize(confirmationWindowSize);
      targetLocator.setBlockOnDurableSend(!useDuplicateDetection);
      targetLocator.setBlockOnNonDurableSend(!useDuplicateDetection);

      targetLocator.setRetryInterval(retryInterval);
      targetLocator.setMaxRetryInterval(maxRetryInterval);
      targetLocator.setRetryIntervalMultiplier(retryIntervalMultiplier);
      targetLocator.setMinLargeMessageSize(minLargeMessageSize);

      // No producer flow control on the bridges, as we don't want to lock the queues
      targetLocator.setProducerWindowSize(-1);

      targetLocator.setAfterConnectionInternalListener(this);

      targetLocator.setPacketDecoder(ServerPacketDecoder.INSTANCE);

      targetLocator.setNodeID(nodeId);

      targetLocator.setClusterTransportConfiguration(serverLocator.getClusterTransportConfiguration());

      if (retryInterval > 0)
      {
         targetLocator.setRetryInterval(retryInterval);
      }

      targetLocator.disableFinalizeCheck();
      targetLocator.addIncomingInterceptor(new IncomingInterceptorLookingForExceptionMessage(manager,
                                                                                             executorFactory.getExecutor()));
      MessageFlowRecordImpl record = new MessageFlowRecordImpl(targetLocator,
                                                               eventUID,
                                                               targetNodeID,
                                                               connector,
                                                               queueName,
                                                               queue);

      ClusterConnectionBridge bridge = new ClusterConnectionBridge(this,
                                                                   manager,
                                                                   targetLocator,
                                                                   serverLocator,
                                                                   initialConnectAttempts,
                                                                   reconnectAttempts,
                                                                   retryInterval,
                                                                   retryIntervalMultiplier,
                                                                   maxRetryInterval,
                                                                   nodeManager.getUUID(),
                                                                   record.getEventUID(),
                                                                   record.getTargetNodeID(),
                                                                   record.getQueueName(),
                                                                   record.getQueue(),
                                                                   executorFactory.getExecutor(),
                                                                   null,
                                                                   null,
                                                                   scheduledExecutor,
                                                                   null,
                                                                   useDuplicateDetection,
                                                                   clusterUser,
                                                                   clusterPassword,
                                                                   server.getStorageManager(),
                                                                   managementService.getManagementAddress(),
                                                                   managementService.getManagementNotificationAddress(),
                                                                   record,
                                                                   record.getConnector());

      targetLocator.setIdentity("(Cluster-connection-bridge::" + bridge.toString() + "::" + this.toString() + ")");

      if (HornetQServerLogger.LOGGER.isDebugEnabled())
      {
         HornetQServerLogger.LOGGER.debug("creating record between " + this.connector + " and " + connector + bridge);
      }

      record.setBridge(bridge);

      records.put(targetNodeID, record);

      if (start)
      {
         bridge.start();
      }
   }

   // Inner classes -----------------------------------------------------------------------------------

   private class MessageFlowRecordImpl implements MessageFlowRecord
   {
      private BridgeImpl bridge;

      private final long eventUID;

      private final String targetNodeID;

      private final TransportConfiguration connector;

      private final ServerLocatorInternal targetLocator;

      private final SimpleString queueName;

      private boolean disconnected = false;

      private final Queue queue;

      private final Map<SimpleString, RemoteQueueBinding> bindings = new HashMap<SimpleString, RemoteQueueBinding>();

      private volatile boolean isClosed = false;

      private volatile boolean reset = false;

      public MessageFlowRecordImpl(final ServerLocatorInternal targetLocator,
                                   final long eventUID,
                                   final String targetNodeID,
                                   final TransportConfiguration connector,
                                   final SimpleString queueName,
                                   final Queue queue)
      {
         this.targetLocator = targetLocator;
         this.queue = queue;
         this.targetNodeID = targetNodeID;
         this.connector = connector;
         this.queueName = queueName;
         this.eventUID = eventUID;
      }

      /* (non-Javadoc)
       * @see java.lang.Object#toString()
       */
      @Override
      public String toString()
      {
         return "MessageFlowRecordImpl [nodeID=" + targetNodeID +
            ", connector=" +
            connector +
            ", queueName=" +
            queueName +
            ", queue=" +
            queue +
            ", isClosed=" +
            isClosed +
            ", reset=" +
            reset +
            "]";
      }

      public void serverDisconnected()
      {
         this.disconnected = true;
      }

      public String getAddress()
      {
         return address.toString();
      }

      /**
       * @return the eventUID
       */
      public long getEventUID()
      {
         return eventUID;
      }

      /**
       * @return the nodeID
       */
      public String getTargetNodeID()
      {
         return targetNodeID;
      }

      /**
       * @return the connector
       */
      public TransportConfiguration getConnector()
      {
         return connector;
      }

      /**
       * @return the queueName
       */
      public SimpleString getQueueName()
      {
         return queueName;
      }

      /**
       * @return the queue
       */
      public Queue getQueue()
      {
         return queue;
      }

      public int getMaxHops()
      {
         return maxHops;
      }

      /*
      * we should only ever close a record when the node itself has gone down or in the case of scale down where we know
      * the node is being completely destroyed and in this case we will migrate to another server/Bridge.
      * */
      public void close() throws Exception
      {
         if (isTrace)
         {
            HornetQServerLogger.LOGGER.trace("Stopping bridge " + bridge);
         }

         isClosed = true;

         clearBindings();

         if (disconnected)
         {
            bridge.disconnect();
         }

         bridge.stop();

         bridge.getExecutor().execute(new Runnable()
         {
            public void run()
            {
               try
               {
                  if (disconnected)
                  {
                     targetLocator.cleanup();
                  }
                  else
                  {
                     targetLocator.close();
                  }
               }
               catch (Exception ignored)
               {
                  HornetQServerLogger.LOGGER.debug(ignored.getMessage(), ignored);
               }
            }
         });
      }

      public boolean isClosed()
      {
         return isClosed;
      }

      public void reset() throws Exception
      {
         resetBindings();
      }

      public void setBridge(final BridgeImpl bridge)
      {
         this.bridge = bridge;
      }

      public Bridge getBridge()
      {
         return bridge;
      }

      public synchronized void onMessage(final ClientMessage message)
      {
         if (HornetQServerLogger.LOGGER.isDebugEnabled())
         {
            HornetQServerLogger.LOGGER.debug("ClusterCommunication::Flow record on " + clusterConnector + " Receiving message " + message);
         }
         try
         {
            // Reset the bindings
            if (message.containsProperty(PostOfficeImpl.HDR_RESET_QUEUE_DATA))
            {
               reset = true;

               return;
            }
            else if (message.containsProperty(PostOfficeImpl.HDR_RESET_QUEUE_DATA_COMPLETE))
            {
               clearDisconnectedBindings();
               return;
            }

            if (!reset)
            {
               HornetQServerLogger.LOGGER.debug("Notification being ignored since first reset wasn't received yet: " + message);
               return;
            }

            handleNotificationMessage(message);
         }
         catch (Exception e)
         {
            HornetQServerLogger.LOGGER.errorHandlingMessage(e);
         }
      }

      private void handleNotificationMessage(ClientMessage message) throws Exception
      {
         // TODO - optimised this by just passing int in header - but filter needs to be extended to support IN with
         // a list of integers
         SimpleString type = message.getSimpleStringProperty(ManagementHelper.HDR_NOTIFICATION_TYPE);

         NotificationType ntype = NotificationType.valueOf(type.toString());

         switch (ntype)
         {
            case BINDING_ADDED:
            {
               doBindingAdded(message);

               break;
            }
            case BINDING_REMOVED:
            {
               doBindingRemoved(message);

               break;
            }
            case CONSUMER_CREATED:
            {
               doConsumerCreated(message);

               break;
            }
            case CONSUMER_CLOSED:
            {
               doConsumerClosed(message);

               break;
            }
            case PROPOSAL:
            {
               doProposalReceived(message);

               break;
            }
            case PROPOSAL_RESPONSE:
            {
               doProposalResponseReceived(message);
               break;
            }
            case UNPROPOSAL:
            {
               doUnProposalReceived(message);
               break;
            }
            default:
            {
               throw HornetQMessageBundle.BUNDLE.invalidType(ntype);
            }
         }
      }

      /*
      * Inform the grouping handler of a proposal
      * */
      private synchronized void doProposalReceived(final ClientMessage message) throws Exception
      {
         if (!message.containsProperty(ManagementHelper.HDR_PROPOSAL_GROUP_ID))
         {
            throw new IllegalStateException("proposal type is null");
         }

         SimpleString type = message.getSimpleStringProperty(ManagementHelper.HDR_PROPOSAL_GROUP_ID);

         SimpleString val = message.getSimpleStringProperty(ManagementHelper.HDR_PROPOSAL_VALUE);

         Integer hops = message.getIntProperty(ManagementHelper.HDR_DISTANCE);

         if (server.getGroupingHandler() == null)
         {
            throw new IllegalStateException("grouping handler is null");
         }

         Response response = server.getGroupingHandler().receive(new Proposal(type, val), hops + 1);

         if (response != null)
         {
            server.getGroupingHandler().sendProposalResponse(response, 0);
         }
      }

      /*
      * Inform the grouping handler of a proposal(groupid) being removed
      * */
      private synchronized void doUnProposalReceived(final ClientMessage message) throws Exception
      {
         if (!message.containsProperty(ManagementHelper.HDR_PROPOSAL_GROUP_ID))
         {
            throw new IllegalStateException("proposal type is null");
         }

         SimpleString groupId = message.getSimpleStringProperty(ManagementHelper.HDR_PROPOSAL_GROUP_ID);

         SimpleString clusterName = message.getSimpleStringProperty(ManagementHelper.HDR_PROPOSAL_VALUE);

         Integer hops = message.getIntProperty(ManagementHelper.HDR_DISTANCE);

         if (server.getGroupingHandler() == null)
         {
            throw new IllegalStateException("grouping handler is null");
         }

         server.getGroupingHandler().remove(groupId, clusterName, hops + 1);

      }

      /*
      * Inform the grouping handler of a response from a proposal
      *
      * */
      private synchronized void doProposalResponseReceived(final ClientMessage message) throws Exception
      {
         if (!message.containsProperty(ManagementHelper.HDR_PROPOSAL_GROUP_ID))
         {
            throw new IllegalStateException("proposal type is null");
         }

         SimpleString type = message.getSimpleStringProperty(ManagementHelper.HDR_PROPOSAL_GROUP_ID);
         SimpleString val = message.getSimpleStringProperty(ManagementHelper.HDR_PROPOSAL_VALUE);
         SimpleString alt = message.getSimpleStringProperty(ManagementHelper.HDR_PROPOSAL_ALT_VALUE);
         Integer hops = message.getIntProperty(ManagementHelper.HDR_DISTANCE);
         Response response = new Response(type, val, alt);

         if (server.getGroupingHandler() == null)
         {
            throw new IllegalStateException("grouping handler is null while sending response " + response);
         }

         server.getGroupingHandler().proposed(response);
         server.getGroupingHandler().sendProposalResponse(response, hops + 1);
      }

      private synchronized void clearBindings() throws Exception
      {
         HornetQServerLogger.LOGGER.debug(ClusterConnectionImpl.this + " clearing bindings");
         for (RemoteQueueBinding binding : new HashSet<RemoteQueueBinding>(bindings.values()))
         {
            removeBinding(binding.getClusterName());
         }
      }

      private synchronized void resetBindings() throws Exception
      {
         HornetQServerLogger.LOGGER.debug(ClusterConnectionImpl.this + " reset bindings");
         for (RemoteQueueBinding binding : new HashSet<>(bindings.values()))
         {
            resetBinding(binding.getClusterName());
         }
      }

      private synchronized void clearDisconnectedBindings() throws Exception
      {
         HornetQServerLogger.LOGGER.debug(ClusterConnectionImpl.this + " reset bindings");
         for (RemoteQueueBinding binding : new HashSet<>(bindings.values()))
         {
            if (!binding.isConnected())
            {
               removeBinding(binding.getClusterName());
            }
         }
      }


      public synchronized void disconnectBindings() throws Exception
      {
         HornetQServerLogger.LOGGER.debug(ClusterConnectionImpl.this + " disconnect bindings");
         reset = false;
         for (RemoteQueueBinding binding : new HashSet<>(bindings.values()))
         {
            disconnectBinding(binding.getClusterName());
         }
      }

      private synchronized void doBindingAdded(final ClientMessage message) throws Exception
      {
         if (HornetQServerLogger.LOGGER.isTraceEnabled())
         {
            HornetQServerLogger.LOGGER.trace(ClusterConnectionImpl.this + " Adding binding " + message);
         }
         if (!message.containsProperty(ManagementHelper.HDR_DISTANCE))
         {
            throw new IllegalStateException("distance is null");
         }

         if (!message.containsProperty(ManagementHelper.HDR_ADDRESS))
         {
            throw new IllegalStateException("queueAddress is null");
         }

         if (!message.containsProperty(ManagementHelper.HDR_CLUSTER_NAME))
         {
            throw new IllegalStateException("clusterName is null");
         }

         if (!message.containsProperty(ManagementHelper.HDR_ROUTING_NAME))
         {
            throw new IllegalStateException("routingName is null");
         }

         if (!message.containsProperty(ManagementHelper.HDR_BINDING_ID))
         {
            throw new IllegalStateException("queueID is null");
         }

         Integer distance = message.getIntProperty(ManagementHelper.HDR_DISTANCE);

         SimpleString queueAddress = message.getSimpleStringProperty(ManagementHelper.HDR_ADDRESS);

         SimpleString clusterName = message.getSimpleStringProperty(ManagementHelper.HDR_CLUSTER_NAME);

         SimpleString routingName = message.getSimpleStringProperty(ManagementHelper.HDR_ROUTING_NAME);

         SimpleString filterString = message.getSimpleStringProperty(ManagementHelper.HDR_FILTERSTRING);

         Long queueID = message.getLongProperty(ManagementHelper.HDR_BINDING_ID);

         RemoteQueueBinding existingBinding = (RemoteQueueBinding) postOffice.getBinding(clusterName);

         if (existingBinding != null)
         {
            if (!existingBinding.isConnected())
            {
               existingBinding.connect();
               return;
            }
            // Sanity check - this means the binding has already been added via another bridge, probably max
            // hops is too high
            // or there are multiple cluster connections for the same address

            HornetQServerLogger.LOGGER.remoteQueueAlreadyBoundOnClusterConnection(this, clusterName);

            return;
         }

         RemoteQueueBinding binding = new RemoteQueueBindingImpl(server.getStorageManager().generateUniqueID(),
                                                                 queueAddress,
                                                                 clusterName,
                                                                 routingName,
                                                                 queueID,
                                                                 filterString,
                                                                 queue,
                                                                 bridge.getName(),
                                                                 distance + 1);

         if (isTrace)
         {
            HornetQServerLogger.LOGGER.trace("Adding binding " + clusterName + " into " + ClusterConnectionImpl.this);
         }

         bindings.put(clusterName, binding);

         try
         {
            postOffice.addBinding(binding);
         }
         catch (Exception ignore)
         {
         }

         Bindings theBindings = postOffice.getBindingsForAddress(queueAddress);

         theBindings.setRouteWhenNoConsumers(routeWhenNoConsumers);

      }

      private void doBindingRemoved(final ClientMessage message) throws Exception
      {
         if (HornetQServerLogger.LOGGER.isTraceEnabled())
         {
            HornetQServerLogger.LOGGER.trace(ClusterConnectionImpl.this + " Removing binding " + message);
         }
         if (!message.containsProperty(ManagementHelper.HDR_CLUSTER_NAME))
         {
            throw new IllegalStateException("clusterName is null");
         }

         SimpleString clusterName = message.getSimpleStringProperty(ManagementHelper.HDR_CLUSTER_NAME);

         removeBinding(clusterName);
      }

      private synchronized void removeBinding(final SimpleString clusterName) throws Exception
      {
         RemoteQueueBinding binding = bindings.remove(clusterName);

         if (binding == null)
         {
            throw new IllegalStateException("Cannot find binding for queue " + clusterName);
         }

         postOffice.removeBinding(binding.getUniqueName(), null);
      }

      private synchronized void resetBinding(final SimpleString clusterName) throws Exception
      {
         RemoteQueueBinding binding = bindings.get(clusterName);
         if (binding == null)
         {
            throw new IllegalStateException("Cannot find binding for queue " + clusterName);
         }
         binding.reset();
      }


      private synchronized void disconnectBinding(final SimpleString clusterName) throws Exception
      {
         RemoteQueueBinding binding = bindings.get(clusterName);

         if (binding == null)
         {
            throw new IllegalStateException("Cannot find binding for queue " + clusterName);
         }

         binding.disconnect();
      }

      private synchronized void doConsumerCreated(final ClientMessage message) throws Exception
      {
         if (HornetQServerLogger.LOGGER.isTraceEnabled())
         {
            HornetQServerLogger.LOGGER.trace(ClusterConnectionImpl.this + " Consumer created " + message);
         }
         if (!message.containsProperty(ManagementHelper.HDR_DISTANCE))
         {
            throw new IllegalStateException("distance is null");
         }

         if (!message.containsProperty(ManagementHelper.HDR_CLUSTER_NAME))
         {
            throw new IllegalStateException("clusterName is null");
         }

         Integer distance = message.getIntProperty(ManagementHelper.HDR_DISTANCE);

         SimpleString clusterName = message.getSimpleStringProperty(ManagementHelper.HDR_CLUSTER_NAME);

         message.putIntProperty(ManagementHelper.HDR_DISTANCE, distance + 1);

         SimpleString filterString = message.getSimpleStringProperty(ManagementHelper.HDR_FILTERSTRING);

         RemoteQueueBinding binding = bindings.get(clusterName);

         if (binding == null)
         {
            throw new IllegalStateException("Cannot find binding for " + clusterName +
                                               " on " +
                                               ClusterConnectionImpl.this);
         }

         binding.addConsumer(filterString);

         // Need to propagate the consumer add
         TypedProperties props = new TypedProperties();

         props.putSimpleStringProperty(ManagementHelper.HDR_ADDRESS, binding.getAddress());

         props.putSimpleStringProperty(ManagementHelper.HDR_CLUSTER_NAME, clusterName);

         props.putSimpleStringProperty(ManagementHelper.HDR_ROUTING_NAME, binding.getRoutingName());

         props.putIntProperty(ManagementHelper.HDR_DISTANCE, distance + 1);

         Queue theQueue = (Queue) binding.getBindable();

         props.putIntProperty(ManagementHelper.HDR_CONSUMER_COUNT, theQueue.getConsumerCount());

         if (filterString != null)
         {
            props.putSimpleStringProperty(ManagementHelper.HDR_FILTERSTRING, filterString);
         }

         Notification notification = new Notification(null, CONSUMER_CREATED, props);

         managementService.sendNotification(notification);
      }

      private synchronized void doConsumerClosed(final ClientMessage message) throws Exception
      {
         if (HornetQServerLogger.LOGGER.isTraceEnabled())
         {
            HornetQServerLogger.LOGGER.trace(ClusterConnectionImpl.this + " Consumer closed " + message);
         }
         if (!message.containsProperty(ManagementHelper.HDR_DISTANCE))
         {
            throw new IllegalStateException("distance is null");
         }

         if (!message.containsProperty(ManagementHelper.HDR_CLUSTER_NAME))
         {
            throw new IllegalStateException("clusterName is null");
         }

         Integer distance = message.getIntProperty(ManagementHelper.HDR_DISTANCE);

         SimpleString clusterName = message.getSimpleStringProperty(ManagementHelper.HDR_CLUSTER_NAME);

         message.putIntProperty(ManagementHelper.HDR_DISTANCE, distance + 1);

         SimpleString filterString = message.getSimpleStringProperty(ManagementHelper.HDR_FILTERSTRING);

         RemoteQueueBinding binding = bindings.get(clusterName);

         if (binding == null)
         {
            throw new IllegalStateException("Cannot find binding for " + clusterName);
         }

         binding.removeConsumer(filterString);

         // Need to propagate the consumer close
         TypedProperties props = new TypedProperties();

         props.putSimpleStringProperty(ManagementHelper.HDR_ADDRESS, binding.getAddress());

         props.putSimpleStringProperty(ManagementHelper.HDR_CLUSTER_NAME, clusterName);

         props.putSimpleStringProperty(ManagementHelper.HDR_ROUTING_NAME, binding.getRoutingName());

         props.putIntProperty(ManagementHelper.HDR_DISTANCE, distance + 1);

         Queue theQueue = (Queue) binding.getBindable();

         props.putIntProperty(ManagementHelper.HDR_CONSUMER_COUNT, theQueue.getConsumerCount());

         if (filterString != null)
         {
            props.putSimpleStringProperty(ManagementHelper.HDR_FILTERSTRING, filterString);
         }
         Notification notification = new Notification(null, CONSUMER_CLOSED, props);

         managementService.sendNotification(notification);
      }

   }

   // for testing only
   public Map<String, MessageFlowRecord> getRecords()
   {
      return records;
   }

   @Override
   public String toString()
   {
      return "ClusterConnectionImpl@" + System.identityHashCode(this) +
         "[nodeUUID=" +
         nodeManager.getNodeId() +
         ", connector=" +
         connector +
         ", address=" +
         address +
         ", server=" +
         server +
         "]";
   }

   public String describe()
   {
      StringWriter str = new StringWriter();
      PrintWriter out = new PrintWriter(str);

      out.println(this);
      out.println("***************************************");
      out.println(name + " connected to");
      for (MessageFlowRecord messageFlow : records.values())
      {
         out.println("\t Bridge = " + messageFlow.getBridge());
         out.println("\t Flow Record = " + messageFlow);
      }
      out.println("***************************************");

      return str.toString();
   }

   private interface ClusterConnector
   {
      ServerLocatorInternal createServerLocator();
   }

   private final class StaticClusterConnector implements ClusterConnector
   {
      private final TransportConfiguration[] tcConfigs;

      public StaticClusterConnector(TransportConfiguration[] tcConfigs)
      {
         this.tcConfigs = tcConfigs;
      }

      public ServerLocatorInternal createServerLocator()
      {
         if (tcConfigs != null && tcConfigs.length > 0)
         {
            if (HornetQServerLogger.LOGGER.isDebugEnabled())
            {
               HornetQServerLogger.LOGGER.debug(ClusterConnectionImpl.this + "Creating a serverLocator for " + Arrays.toString(tcConfigs));
            }
            ServerLocatorImpl locator = new ServerLocatorImpl(topology, true, tcConfigs);
            locator.setClusterConnection(true);
            return locator;
         }
         return null;
      }

      @Override
      public String toString()
      {
         return "StaticClusterConnector [tcConfigs=" + Arrays.toString(tcConfigs) + "]";
      }

   }

   private final class DiscoveryClusterConnector implements ClusterConnector
   {
      private final DiscoveryGroupConfiguration dg;

      public DiscoveryClusterConnector(DiscoveryGroupConfiguration dg)
      {
         this.dg = dg;
      }

      public ServerLocatorInternal createServerLocator()
      {
         return new ServerLocatorImpl(topology, true, dg);
      }
   }

   @Override
   public boolean verify(String clusterUser0, String clusterPassword0)
   {
      return clusterUser.equals(clusterUser0) && clusterPassword.equals(clusterPassword0);
   }

   @Override
   public void removeRecord(String targetNodeID)
   {
      HornetQServerLogger.LOGGER.debug("Removing record for: " + targetNodeID);
      MessageFlowRecord record = records.remove(targetNodeID);
      try
      {
         record.close();
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }

   @Override
   public void disconnectRecord(String targetNodeID)
   {
      HornetQServerLogger.LOGGER.debug("Disconnecting record for: " + targetNodeID);
      MessageFlowRecord record = records.get(targetNodeID);
      try
      {
         if (record != null)
         {
            record.disconnectBindings();
         }
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }

   private final class LiveNotifier implements Runnable
   {
      int notificationsSent = 0;


      @Override
      public void run()
      {
         resendLive();

         schedule();
      }

      public void schedule()
      {
         if (started && !stopping && notificationsSent++ < clusterNotificationAttempts)
         {
            scheduledExecutor.schedule(this, clusterNotificationInterval, TimeUnit.MILLISECONDS);
         }
      }

      public void updateAsLive()
      {
         if (!stopping && started)
         {
            topology.updateAsLive(manager.getNodeId(), new TopologyMemberImpl(manager.getNodeId(),
                                                                              manager.getBackupGroupName(),
                                                                              manager.getScaleDownGroupName(),
                                                                              connector,
                                                                              null));
         }
      }

      public void resendLive()
      {
         if (!stopping && started)
         {
            topology.resendNode(manager.getNodeId());
         }
      }
   }
}
