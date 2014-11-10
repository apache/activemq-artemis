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

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.client.impl.Topology;
import org.hornetq.core.client.impl.TopologyMemberImpl;
import org.hornetq.core.protocol.core.Channel;
import org.hornetq.core.protocol.core.ChannelHandler;
import org.hornetq.core.protocol.core.Packet;
import org.hornetq.core.protocol.core.impl.PacketImpl;
import org.hornetq.core.protocol.core.impl.wireformat.BackupRequestMessage;
import org.hornetq.core.protocol.core.impl.wireformat.BackupResponseMessage;
import org.hornetq.core.remoting.server.RemotingService;
import org.hornetq.core.replication.ReplicationManager;
import org.hornetq.core.server.cluster.ha.ColocatedHAManager;
import org.hornetq.core.server.cluster.ha.ColocatedPolicy;
import org.hornetq.core.server.cluster.ha.HAManager;
import org.hornetq.core.server.cluster.qourum.QuorumVote;
import org.hornetq.core.server.cluster.qourum.QuorumVoteHandler;
import org.hornetq.core.server.cluster.qourum.Vote;
import org.hornetq.spi.core.remoting.Acceptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ColocatedActivation extends LiveActivation
{
   private static final SimpleString REQUEST_BACKUP_QUORUM_VOTE = new SimpleString("RequestBackupQuorumVote");

   private final ColocatedHAManager colocatedHAManager;

   private final ColocatedPolicy colocatedPolicy;

   LiveActivation liveActivation;

   private final HornetQServerImpl server;

   public ColocatedActivation(HornetQServerImpl hornetQServer, ColocatedPolicy colocatedPolicy, LiveActivation liveActivation)
   {
      server = hornetQServer;
      this.colocatedPolicy = colocatedPolicy;
      this.liveActivation = liveActivation;
      colocatedHAManager = new ColocatedHAManager(colocatedPolicy, server);
   }


   @Override
   public void haStarted()
   {
      server.getClusterManager().getQuorumManager().registerQuorumHandler(new RequestBackupQuorumVoteHandler());
      //vote for a backup if required
      if (colocatedPolicy.isRequestBackup())
      {
         server.getClusterManager().getQuorumManager().vote(new RequestBackupQuorumVote());
      }
   }

   @Override
   public void freezeConnections(RemotingService remotingService)
   {
      liveActivation.freezeConnections(remotingService);
   }

   @Override
   public void postConnectionFreeze()
   {
      liveActivation.postConnectionFreeze();
   }

   @Override
   public void preStorageClose() throws Exception
   {
      liveActivation.preStorageClose();
   }

   @Override
   public void sendLiveIsStopping()
   {
      liveActivation.sendLiveIsStopping();
   }

   @Override
   public ReplicationManager getReplicationManager()
   {
      return liveActivation.getReplicationManager();
   }

   @Override
   public HAManager getHAManager()
   {
      return colocatedHAManager;
   }

   @Override
   public void run()
   {
      liveActivation.run();
   }

   @Override
   public void close(boolean permanently, boolean restarting) throws Exception
   {
      liveActivation.close(permanently, restarting);
   }

   @Override
   public ChannelHandler getActivationChannelHandler(final Channel channel, final Acceptor acceptorUsed)
   {
      final ChannelHandler activationChannelHandler = liveActivation.getActivationChannelHandler(channel, acceptorUsed);
      return new ChannelHandler()
      {
         @Override
         public void handlePacket(Packet packet)
         {
            if (packet.getType() == PacketImpl.BACKUP_REQUEST)
            {
               BackupRequestMessage backupRequestMessage = (BackupRequestMessage) packet;
               boolean started = false;
               try
               {
                  started = colocatedHAManager.activateBackup(backupRequestMessage.getBackupSize(),
                        backupRequestMessage.getJournalDirectory(),
                        backupRequestMessage.getBindingsDirectory(),
                        backupRequestMessage.getLargeMessagesDirectory(),
                        backupRequestMessage.getPagingDirectory(),
                        backupRequestMessage.getNodeID());
               }
               catch (Exception e)
               {
                  e.printStackTrace();
               }
               channel.send(new BackupResponseMessage(started));
            }
            else if (activationChannelHandler != null)
            {
               activationChannelHandler.handlePacket(packet);
            }
         }
      };
   }

   /**
    * A vote handler for incoming backup request votes
    */
   private final class RequestBackupQuorumVoteHandler implements QuorumVoteHandler
   {
      @Override
      public Vote vote(Vote vote)
      {
         int size = colocatedHAManager.getBackupServers().size();
         return new RequestBackupVote(size, server.getNodeID().toString(), size < colocatedPolicy.getMaxBackups());
      }

      @Override
      public SimpleString getQuorumName()
      {
         return REQUEST_BACKUP_QUORUM_VOTE;
      }

      @Override
      public Vote decode(HornetQBuffer voteBuffer)
      {
         RequestBackupVote requestBackupVote = new RequestBackupVote();
         requestBackupVote.decode(voteBuffer);
         return requestBackupVote;
      }
   }
   /**
    * a quorum vote for backup requests
    */
   private final class RequestBackupQuorumVote extends QuorumVote<RequestBackupVote, Pair<String, Integer>>
   {
      //the available nodes that we can request
      private final List<Pair<String, Integer>> nodes = new ArrayList<>();

      public RequestBackupQuorumVote()
      {
         super(REQUEST_BACKUP_QUORUM_VOTE);
      }

      @Override
      public Vote connected()
      {
         return new RequestBackupVote();
      }

      @Override
      public Vote notConnected()
      {
         return new RequestBackupVote();
      }

      @Override
      public void vote(RequestBackupVote vote)
      {
         //if the returned vote is available add it to the nodes we can request
         if (vote.backupAvailable)
         {
            nodes.add(vote.getVote());
         }
      }

      @Override
      public Pair<String, Integer> getDecision()
      {
         //sort the nodes by how many backups they have and choose the first
         Collections.sort(nodes, new Comparator<Pair<String, Integer>>()
         {
            @Override
            public int compare(Pair<String, Integer> o1, Pair<String, Integer> o2)
            {
               return o1.getB().compareTo(o2.getB());
            }
         });
         return nodes.get(0);
      }

      @Override
      public void allVotesCast(Topology voteTopology)
      {
         //if we have any nodes that we can request then send a request
         if (nodes.size() > 0)
         {
            Pair<String, Integer> decision = getDecision();
            TopologyMemberImpl member = voteTopology.getMember(decision.getA());
            try
            {
               boolean backupStarted = colocatedHAManager.requestBackup(member.getConnector(), decision.getB().intValue(), !colocatedPolicy.isSharedStore());
               if (!backupStarted)
               {
                  nodes.clear();
                  server.getScheduledPool().schedule(new Runnable()
                  {
                     @Override
                     public void run()
                     {
                        server.getClusterManager().getQuorumManager().vote(new RequestBackupQuorumVote());
                     }
                  }, colocatedPolicy.getBackupRequestRetryInterval(), TimeUnit.MILLISECONDS);
               }
            }
            catch (Exception e)
            {
               e.printStackTrace();
               //todo
            }
         }
         else
         {
            nodes.clear();
            server.getScheduledPool().schedule(new Runnable()
            {
               @Override
               public void run()
               {
                  server.getClusterManager().getQuorumManager().vote(RequestBackupQuorumVote.this);
               }
            }, colocatedPolicy.getBackupRequestRetryInterval(), TimeUnit.MILLISECONDS);
         }
      }

      @Override
      public SimpleString getName()
      {
         return REQUEST_BACKUP_QUORUM_VOTE;
      }
   }

   class RequestBackupVote extends Vote<Pair<String, Integer>>
   {
      private int backupsSize;
      private String nodeID;
      private boolean backupAvailable;

      public RequestBackupVote()
      {
         backupsSize = -1;
      }

      public RequestBackupVote(int backupsSize, String nodeID, boolean backupAvailable)
      {
         this.backupsSize = backupsSize;
         this.nodeID = nodeID;
         this.backupAvailable = backupAvailable;
      }

      @Override
      public void encode(HornetQBuffer buff)
      {
         buff.writeInt(backupsSize);
         buff.writeNullableString(nodeID);
         buff.writeBoolean(backupAvailable);
      }

      @Override
      public void decode(HornetQBuffer buff)
      {
         backupsSize = buff.readInt();
         nodeID = buff.readNullableString();
         backupAvailable = buff.readBoolean();
      }

      @Override
      public boolean isRequestServerVote()
      {
         return true;
      }

      @Override
      public Pair<String, Integer> getVote()
      {
         return new Pair<>(nodeID, backupsSize);
      }
   }
}
