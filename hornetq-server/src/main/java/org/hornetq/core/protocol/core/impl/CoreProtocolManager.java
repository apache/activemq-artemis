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
package org.hornetq.core.protocol.core.impl;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

import io.netty.channel.ChannelPipeline;
import org.hornetq.api.core.HornetQAlreadyReplicatingException;
import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Interceptor;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClusterTopologyListener;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.TopologyMember;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.protocol.ServerPacketDecoder;
import org.hornetq.core.protocol.core.Channel;
import org.hornetq.core.protocol.core.ChannelHandler;
import org.hornetq.core.protocol.core.CoreRemotingConnection;
import org.hornetq.core.protocol.core.Packet;
import org.hornetq.core.protocol.core.ServerSessionPacketHandler;
import org.hornetq.core.protocol.core.impl.ChannelImpl.CHANNEL_ID;
import org.hornetq.core.protocol.core.impl.wireformat.BackupRegistrationMessage;
import org.hornetq.core.protocol.core.impl.wireformat.BackupReplicationStartFailedMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ClusterTopologyChangeMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ClusterTopologyChangeMessage_V2;
import org.hornetq.core.protocol.core.impl.wireformat.ClusterTopologyChangeMessage_V3;
import org.hornetq.core.protocol.core.impl.wireformat.Ping;
import org.hornetq.core.protocol.core.impl.wireformat.SubscribeClusterTopologyUpdatesMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SubscribeClusterTopologyUpdatesMessageV2;
import org.hornetq.core.remoting.CloseListener;
import org.hornetq.core.remoting.impl.netty.HornetQFrameDecoder2;
import org.hornetq.core.remoting.impl.netty.NettyServerConnection;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServerLogger;
import org.hornetq.core.server.cluster.ClusterConnection;
import org.hornetq.spi.core.protocol.ConnectionEntry;
import org.hornetq.spi.core.protocol.ProtocolManager;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.spi.core.remoting.Acceptor;
import org.hornetq.spi.core.remoting.Connection;

/**
 * A CoreProtocolManager
 *
 * @author Tim Fox
 */
class CoreProtocolManager implements ProtocolManager
{
   private static final boolean isTrace = HornetQServerLogger.LOGGER.isTraceEnabled();

   private final HornetQServer server;

   private final List<Interceptor> incomingInterceptors;

   private final List<Interceptor> outgoingInterceptors;

   CoreProtocolManager(final HornetQServer server, final List<Interceptor> incomingInterceptors, List<Interceptor> outgoingInterceptors)
   {
      this.server = server;

      this.incomingInterceptors = incomingInterceptors;

      this.outgoingInterceptors = outgoingInterceptors;
   }

   public ConnectionEntry createConnectionEntry(final Acceptor acceptorUsed, final Connection connection)
   {
      final Configuration config = server.getConfiguration();

      Executor connectionExecutor = server.getExecutorFactory().getExecutor();

      final CoreRemotingConnection rc = new RemotingConnectionImpl(ServerPacketDecoder.INSTANCE,
                                                                   connection,
                                                                   incomingInterceptors,
                                                                   outgoingInterceptors,
                                                                   config.isAsyncConnectionExecutionEnabled() ? connectionExecutor : null,
                                                                   server.getNodeID());

      Channel channel1 = rc.getChannel(CHANNEL_ID.SESSION.id, -1);

      ChannelHandler handler = new HornetQPacketHandler(this, server, channel1, rc);

      channel1.setHandler(handler);

      long ttl = HornetQClient.DEFAULT_CONNECTION_TTL;

      if (config.getConnectionTTLOverride() != -1)
      {
         ttl = config.getConnectionTTLOverride();
      }

      final ConnectionEntry entry = new ConnectionEntry(rc, connectionExecutor, System.currentTimeMillis(), ttl);

      final Channel channel0 = rc.getChannel(ChannelImpl.CHANNEL_ID.PING.id, -1);

      channel0.setHandler(new LocalChannelHandler(config, entry, channel0, acceptorUsed, rc));

      server.getClusterManager().addClusterChannelHandler(rc.getChannel(CHANNEL_ID.CLUSTER.id, -1), acceptorUsed, rc);

      return entry;
   }

   private final Map<String, ServerSessionPacketHandler> sessionHandlers = new ConcurrentHashMap<String, ServerSessionPacketHandler>();

   ServerSessionPacketHandler getSessionHandler(final String sessionName)
   {
      return sessionHandlers.get(sessionName);
   }

   void addSessionHandler(final String name, final ServerSessionPacketHandler handler)
   {
      sessionHandlers.put(name, handler);
   }

   public void removeHandler(final String name)
   {
      sessionHandlers.remove(name);
   }

   public void handleBuffer(RemotingConnection connection, HornetQBuffer buffer)
   {
   }

   @Override
   public void addChannelHandlers(ChannelPipeline pipeline)
   {
      pipeline.addLast("hornetq-decoder", new HornetQFrameDecoder2());
   }

   @Override
   public boolean isProtocol(byte[] array)
   {
      String frameStart = new String(array, StandardCharsets.US_ASCII);
      return frameStart.startsWith("HORNETQ");
   }

   @Override
   public void handshake(NettyServerConnection connection, HornetQBuffer buffer)
   {
      //if we are not an old client then handshake
      if (buffer.getByte(0) == 'H' &&
         buffer.getByte(1) == 'O' &&
         buffer.getByte(2) == 'R' &&
         buffer.getByte(3) == 'N' &&
         buffer.getByte(4) == 'E' &&
         buffer.getByte(5) == 'T' &&
         buffer.getByte(6) == 'Q')
      {
         //todo add some handshaking
         buffer.readBytes(7);
      }
   }

   @Override
   public String toString()
   {
      return "CoreProtocolManager(server=" + server + ")";
   }

   private class LocalChannelHandler implements ChannelHandler
   {
      private final Configuration config;
      private final ConnectionEntry entry;
      private final Channel channel0;
      private final Acceptor acceptorUsed;
      private final CoreRemotingConnection rc;

      public LocalChannelHandler(final Configuration config, final ConnectionEntry entry,
                                 final Channel channel0, final Acceptor acceptorUsed, final CoreRemotingConnection rc)
      {
         this.config = config;
         this.entry = entry;
         this.channel0 = channel0;
         this.acceptorUsed = acceptorUsed;
         this.rc = rc;
      }

      public void handlePacket(final Packet packet)
      {
         if (packet.getType() == PacketImpl.PING)
         {
            Ping ping = (Ping)packet;

            if (config.getConnectionTTLOverride() == -1)
            {
               // Allow clients to specify connection ttl
               entry.ttl = ping.getConnectionTTL();
            }

            // Just send a ping back
            channel0.send(packet);
         }
         else if (packet.getType() == PacketImpl.SUBSCRIBE_TOPOLOGY || packet.getType() == PacketImpl.SUBSCRIBE_TOPOLOGY_V2)
         {
            SubscribeClusterTopologyUpdatesMessage msg = (SubscribeClusterTopologyUpdatesMessage)packet;

            if (packet.getType() == PacketImpl.SUBSCRIBE_TOPOLOGY_V2)
            {
               channel0.getConnection().setClientVersion(((SubscribeClusterTopologyUpdatesMessageV2)msg).getClientVersion());
            }

            final ClusterTopologyListener listener = new ClusterTopologyListener()
            {
               @Override
               public void nodeUP(final TopologyMember topologyMember, final boolean last)
               {
                  try
                  {
                     final Pair<TransportConfiguration, TransportConfiguration> connectorPair =
                        new Pair<TransportConfiguration, TransportConfiguration>(topologyMember.getLive(),
                                                                                 topologyMember.getBackup());
                     final String nodeID = topologyMember.getNodeId();
                     // Using an executor as most of the notifications on the Topology
                     // may come from a channel itself
                     // What could cause deadlocks
                     entry.connectionExecutor.execute(new Runnable()
                     {
                        public void run()
                        {
                           if (channel0.supports(PacketImpl.CLUSTER_TOPOLOGY_V3))
                           {
                              channel0.send(new ClusterTopologyChangeMessage_V3(topologyMember.getUniqueEventID(),
                                                                                nodeID, topologyMember.getBackupGroupName(),
                                                                                topologyMember.getScaleDownGroupName(),
                                                                                connectorPair, last));
                           }
                           else if (channel0.supports(PacketImpl.CLUSTER_TOPOLOGY_V2))
                           {
                              channel0.send(new ClusterTopologyChangeMessage_V2(topologyMember.getUniqueEventID(),
                                                                                nodeID, topologyMember.getBackupGroupName(),
                                                                                connectorPair, last));
                           }
                           else
                           {
                              channel0.send(new ClusterTopologyChangeMessage(nodeID, connectorPair, last));
                           }
                        }
                     });
                  }
                  catch (RejectedExecutionException ignored)
                  {
                     // this could happen during a shutdown and we don't care, if we lost a nodeDown during a shutdown
                     // what can we do anyways?
                  }

               }

               @Override
               public void nodeDown(final long uniqueEventID, final String nodeID)
               {
                  // Using an executor as most of the notifications on the Topology
                  // may come from a channel itself
                  // What could cause deadlocks
                  try
                  {
                     entry.connectionExecutor.execute(new Runnable()
                     {
                        public void run()
                        {
                           if (channel0.supports(PacketImpl.CLUSTER_TOPOLOGY_V2))
                           {
                              channel0.send(new ClusterTopologyChangeMessage_V2(uniqueEventID, nodeID));
                           }
                           else
                           {
                              channel0.send(new ClusterTopologyChangeMessage(nodeID));
                           }
                        }
                     });
                  }
                  catch (RejectedExecutionException ignored)
                  {
                     // this could happen during a shutdown and we don't care, if we lost a nodeDown during a shutdown
                     // what can we do anyways?
                  }
               }

               @Override
               public String toString()
               {
                  return "Remote Proxy on channel " + Integer.toHexString(System.identityHashCode(this));
               }
            };

            if (acceptorUsed.getClusterConnection() != null)
            {
               acceptorUsed.getClusterConnection().addClusterTopologyListener(listener);

               rc.addCloseListener(new CloseListener()
               {
                  public void connectionClosed()
                  {
                     acceptorUsed.getClusterConnection().removeClusterTopologyListener(listener);
                  }
               });
            }
            else
            {
               // if not clustered, we send a single notification to the client containing the node-id where the server is connected to
               // This is done this way so Recovery discovery could also use the node-id for non-clustered setups
               entry.connectionExecutor.execute(new Runnable()
               {
                  public void run()
                  {
                     String nodeId = server.getNodeID().toString();
                     Pair<TransportConfiguration, TransportConfiguration> emptyConfig = new Pair<TransportConfiguration, TransportConfiguration>(null, null);
                     if (channel0.supports(PacketImpl.CLUSTER_TOPOLOGY_V2))
                     {
                        channel0.send(new ClusterTopologyChangeMessage_V2(System.currentTimeMillis(), nodeId, null, emptyConfig, true));
                     }
                     else
                     {
                        channel0.send(new ClusterTopologyChangeMessage(nodeId, emptyConfig, true));
                     }
                  }
               });

            }
         }
         else if (packet.getType() == PacketImpl.BACKUP_REGISTRATION)
         {
            BackupRegistrationMessage msg = (BackupRegistrationMessage)packet;
            ClusterConnection clusterConnection = acceptorUsed.getClusterConnection();

            if (!config.isSecurityEnabled() || clusterConnection.verify(msg.getClusterUser(), msg.getClusterPassword()))
            {
               try
               {
                  server.startReplication(rc, clusterConnection, getPair(msg.getConnector(), true),
                                          msg.isFailBackRequest());
               }
               catch (HornetQAlreadyReplicatingException are)
               {
                  channel0.send(new BackupReplicationStartFailedMessage(BackupReplicationStartFailedMessage.BackupRegistrationProblem.ALREADY_REPLICATING));
               }
               catch (HornetQException e)
               {
                  channel0.send(new BackupReplicationStartFailedMessage(BackupReplicationStartFailedMessage.BackupRegistrationProblem.EXCEPTION));
               }
            }
            else
            {
               channel0.send(new BackupReplicationStartFailedMessage(BackupReplicationStartFailedMessage.BackupRegistrationProblem.AUTHENTICATION));
            }
         }
      }

      private Pair<TransportConfiguration, TransportConfiguration> getPair(TransportConfiguration conn,
                                                                           boolean isBackup)
      {
         if (isBackup)
         {
            return new Pair<TransportConfiguration, TransportConfiguration>(null, conn);
         }
         return new Pair<TransportConfiguration, TransportConfiguration>(conn, null);
      }
   }
}
