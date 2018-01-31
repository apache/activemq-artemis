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
package org.apache.activemq.artemis.core.protocol.core.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

import io.netty.channel.ChannelPipeline;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.api.core.BaseInterceptor;
import org.apache.activemq.artemis.api.core.Interceptor;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClusterTopologyListener;
import org.apache.activemq.artemis.api.core.client.TopologyMember;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.protocol.ServerPacketDecoder;
import org.apache.activemq.artemis.core.protocol.core.Channel;
import org.apache.activemq.artemis.core.protocol.core.ChannelHandler;
import org.apache.activemq.artemis.core.protocol.core.CoreRemotingConnection;
import org.apache.activemq.artemis.core.protocol.core.Packet;
import org.apache.activemq.artemis.core.protocol.core.ServerSessionPacketHandler;
import org.apache.activemq.artemis.core.protocol.core.impl.ChannelImpl.CHANNEL_ID;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ClusterTopologyChangeMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ClusterTopologyChangeMessage_V2;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ClusterTopologyChangeMessage_V3;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.Ping;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SubscribeClusterTopologyUpdatesMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SubscribeClusterTopologyUpdatesMessageV2;
import org.apache.activemq.artemis.core.remoting.CloseListener;
import org.apache.activemq.artemis.core.remoting.impl.netty.ActiveMQFrameDecoder2;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyServerConnection;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.spi.core.protocol.ConnectionEntry;
import org.apache.activemq.artemis.spi.core.protocol.ProtocolManager;
import org.apache.activemq.artemis.spi.core.protocol.ProtocolManagerFactory;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.remoting.Acceptor;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.jboss.logging.Logger;

public class CoreProtocolManager implements ProtocolManager<Interceptor> {

   private static final Logger logger = Logger.getLogger(CoreProtocolManager.class);

   private static final List<String> websocketRegistryNames = Collections.EMPTY_LIST;

   protected final ActiveMQServer server;

   private final List<Interceptor> incomingInterceptors;

   private final List<Interceptor> outgoingInterceptors;

   private final CoreProtocolManagerFactory protocolManagerFactory;

   private final Map<SimpleString, RoutingType> prefixes = new HashMap<>();

   public CoreProtocolManager(final CoreProtocolManagerFactory factory,
                              final ActiveMQServer server,
                              final List<Interceptor> incomingInterceptors,
                              List<Interceptor> outgoingInterceptors) {
      this.protocolManagerFactory = factory;

      this.server = server;

      this.incomingInterceptors = incomingInterceptors;

      this.outgoingInterceptors = outgoingInterceptors;
   }

   @Override
   public ProtocolManagerFactory<Interceptor> getFactory() {
      return protocolManagerFactory;
   }

   @Override
   public void updateInterceptors(List<BaseInterceptor> incoming, List<BaseInterceptor> outgoing) {
      this.incomingInterceptors.clear();
      this.incomingInterceptors.addAll(getFactory().filterInterceptors(incoming));

      this.outgoingInterceptors.clear();
      this.outgoingInterceptors.addAll(getFactory().filterInterceptors(outgoing));
   }

   @Override
   public boolean acceptsNoHandshake() {
      return false;
   }

   @Override
   public ConnectionEntry createConnectionEntry(final Acceptor acceptorUsed, final Connection connection) {
      final Configuration config = server.getConfiguration();

      Executor connectionExecutor = server.getExecutorFactory().getExecutor();

      final CoreRemotingConnection rc = new RemotingConnectionImpl(new ServerPacketDecoder(), connection, incomingInterceptors, outgoingInterceptors, config.isAsyncConnectionExecutionEnabled() ? connectionExecutor : null, server.getNodeID());

      Channel channel1 = rc.getChannel(CHANNEL_ID.SESSION.id, -1);

      ChannelHandler handler = new ActiveMQPacketHandler(this, server, channel1, rc);

      channel1.setHandler(handler);

      long ttl = ActiveMQClient.DEFAULT_CONNECTION_TTL;

      if (config.getConnectionTTLOverride() != -1) {
         ttl = config.getConnectionTTLOverride();
      }

      final ConnectionEntry entry = new ConnectionEntry(rc, connectionExecutor, System.currentTimeMillis(), ttl);

      final Channel channel0 = rc.getChannel(ChannelImpl.CHANNEL_ID.PING.id, -1);

      channel0.setHandler(new LocalChannelHandler(config, entry, channel0, acceptorUsed, rc));

      server.getClusterManager().addClusterChannelHandler(rc.getChannel(CHANNEL_ID.CLUSTER.id, -1), acceptorUsed, rc, server.getActivation());

      return entry;
   }

   private final Map<String, ServerSessionPacketHandler> sessionHandlers = new ConcurrentHashMap<>();

   ServerSessionPacketHandler getSessionHandler(final String sessionName) {
      return sessionHandlers.get(sessionName);
   }

   void addSessionHandler(final String name, final ServerSessionPacketHandler handler) {
      sessionHandlers.put(name, handler);
   }

   @Override
   public void removeHandler(final String name) {
      sessionHandlers.remove(name);
   }

   @Override
   public void handleBuffer(RemotingConnection connection, ActiveMQBuffer buffer) {
   }

   @Override
   public void addChannelHandlers(ChannelPipeline pipeline) {
      pipeline.addLast("activemq-decoder", new ActiveMQFrameDecoder2());
   }

   @Override
   public boolean isProtocol(byte[] array) {
      return isArtemis(ActiveMQBuffers.wrappedBuffer(array));
   }

   @Override
   public void handshake(NettyServerConnection connection, ActiveMQBuffer buffer) {
      //if we are not an old client then handshake
      if (isArtemis(buffer)) {
         buffer.skipBytes(7);
      }
   }

   @Override
   public List<String> websocketSubprotocolIdentifiers() {
      return websocketRegistryNames;
   }

   @Override
   public void setAnycastPrefix(String anycastPrefix) {
      for (String prefix : anycastPrefix.split(",")) {
         prefixes.put(SimpleString.toSimpleString(prefix), RoutingType.ANYCAST);
      }
   }

   @Override
   public void setMulticastPrefix(String multicastPrefix) {
      for (String prefix : multicastPrefix.split(",")) {
         prefixes.put(SimpleString.toSimpleString(prefix), RoutingType.MULTICAST);
      }
   }

   @Override
   public Map<SimpleString, RoutingType> getPrefixes() {
      return prefixes;
   }

   private boolean isArtemis(ActiveMQBuffer buffer) {
      return buffer.getByte(0) == 'A' &&
         buffer.getByte(1) == 'R' &&
         buffer.getByte(2) == 'T' &&
         buffer.getByte(3) == 'E' &&
         buffer.getByte(4) == 'M' &&
         buffer.getByte(5) == 'I' &&
         buffer.getByte(6) == 'S';
   }

   @Override
   public String toString() {
      return "CoreProtocolManager(server=" + server + ")";
   }

   private class LocalChannelHandler implements ChannelHandler {

      private final Configuration config;
      private final ConnectionEntry entry;
      private final Channel channel0;
      private final Acceptor acceptorUsed;
      private final CoreRemotingConnection rc;

      private LocalChannelHandler(final Configuration config,
                                  final ConnectionEntry entry,
                                  final Channel channel0,
                                  final Acceptor acceptorUsed,
                                  final CoreRemotingConnection rc) {
         this.config = config;
         this.entry = entry;
         this.channel0 = channel0;
         this.acceptorUsed = acceptorUsed;
         this.rc = rc;
      }

      @Override
      public void handlePacket(final Packet packet) {
         if (packet.getType() == PacketImpl.PING) {
            Ping ping = (Ping) packet;

            if (config.getConnectionTTLOverride() == -1) {
               // Allow clients to specify connection ttl
               entry.ttl = ping.getConnectionTTL();
            }

            // Just send a ping back
            channel0.send(packet);
         } else if (packet.getType() == PacketImpl.SUBSCRIBE_TOPOLOGY || packet.getType() == PacketImpl.SUBSCRIBE_TOPOLOGY_V2) {
            SubscribeClusterTopologyUpdatesMessage msg = (SubscribeClusterTopologyUpdatesMessage) packet;

            if (packet.getType() == PacketImpl.SUBSCRIBE_TOPOLOGY_V2) {
               channel0.getConnection().setChannelVersion(((SubscribeClusterTopologyUpdatesMessageV2) msg).getClientVersion());
            }

            final ClusterTopologyListener listener = new ClusterTopologyListener() {
               @Override
               public void nodeUP(final TopologyMember topologyMember, final boolean last) {
                  try {
                     final Pair<TransportConfiguration, TransportConfiguration> connectorPair = BackwardsCompatibilityUtils.getTCPair(channel0.getConnection().getChannelVersion(), topologyMember);

                     final String nodeID = topologyMember.getNodeId();
                     // Using an executor as most of the notifications on the Topology
                     // may come from a channel itself
                     // What could cause deadlocks
                     entry.connectionExecutor.execute(new Runnable() {
                        @Override
                        public void run() {
                           if (channel0.supports(PacketImpl.CLUSTER_TOPOLOGY_V3)) {
                              channel0.send(new ClusterTopologyChangeMessage_V3(topologyMember.getUniqueEventID(), nodeID, topologyMember.getBackupGroupName(), topologyMember.getScaleDownGroupName(), connectorPair, last));
                           } else if (channel0.supports(PacketImpl.CLUSTER_TOPOLOGY_V2)) {
                              channel0.send(new ClusterTopologyChangeMessage_V2(topologyMember.getUniqueEventID(), nodeID, topologyMember.getBackupGroupName(), connectorPair, last));
                           } else {
                              channel0.send(new ClusterTopologyChangeMessage(nodeID, connectorPair, last));
                           }
                        }
                     });
                  } catch (RejectedExecutionException ignored) {
                     // this could happen during a shutdown and we don't care, if we lost a nodeDown during a shutdown
                     // what can we do anyways?
                  }

               }

               @Override
               public void nodeDown(final long uniqueEventID, final String nodeID) {
                  // Using an executor as most of the notifications on the Topology
                  // may come from a channel itself
                  // What could cause deadlocks
                  try {
                     entry.connectionExecutor.execute(new Runnable() {
                        @Override
                        public void run() {
                           if (channel0.supports(PacketImpl.CLUSTER_TOPOLOGY_V2)) {
                              channel0.send(new ClusterTopologyChangeMessage_V2(uniqueEventID, nodeID));
                           } else {
                              channel0.send(new ClusterTopologyChangeMessage(nodeID));
                           }
                        }
                     });
                  } catch (RejectedExecutionException ignored) {
                     // this could happen during a shutdown and we don't care, if we lost a nodeDown during a shutdown
                     // what can we do anyways?
                  }
               }

               @Override
               public String toString() {
                  return "Remote Proxy on channel " + Integer.toHexString(System.identityHashCode(this));
               }
            };

            if (acceptorUsed.getClusterConnection() != null) {
               acceptorUsed.getClusterConnection().addClusterTopologyListener(listener);

               rc.addCloseListener(new CloseListener() {
                  @Override
                  public void connectionClosed() {
                     acceptorUsed.getClusterConnection().removeClusterTopologyListener(listener);
                  }
               });
            } else {
               // if not clustered, we send a single notification to the client containing the node-id where the server is connected to
               // This is done this way so Recovery discovery could also use the node-id for non-clustered setups
               entry.connectionExecutor.execute(new Runnable() {
                  @Override
                  public void run() {
                     String nodeId = server.getNodeID().toString();
                     Pair<TransportConfiguration, TransportConfiguration> emptyConfig = new Pair<>(null, null);
                     if (channel0.supports(PacketImpl.CLUSTER_TOPOLOGY_V2)) {
                        channel0.send(new ClusterTopologyChangeMessage_V2(System.currentTimeMillis(), nodeId, null, emptyConfig, true));
                     } else {
                        channel0.send(new ClusterTopologyChangeMessage(nodeId, emptyConfig, true));
                     }
                  }
               });
            }
         }
      }

      private Pair<TransportConfiguration, TransportConfiguration> getPair(TransportConfiguration conn,
                                                                           boolean isBackup) {
         if (isBackup) {
            return new Pair<>(null, conn);
         }
         return new Pair<>(conn, null);
      }
   }
}
