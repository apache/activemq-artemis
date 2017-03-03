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
package org.apache.activemq.artemis.core.protocol;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.core.protocol.core.Packet;
import org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.BackupRegistrationMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.BackupReplicationStartFailedMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.BackupRequestMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.BackupResponseMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ClusterConnectMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ClusterConnectReplyMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.NodeAnnounceMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.QuorumVoteMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.QuorumVoteReplyMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationAddMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationAddTXMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationCommitMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationDeleteMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationDeleteTXMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationLargeMessageBeginMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationLargeMessageEndMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationLargeMessageWriteMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationLiveIsStoppingMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationPageEventMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationPageWriteMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationPrepareMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationResponseMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationResponseMessageV2;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationStartSyncMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ReplicationSyncFileMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ScaleDownAnnounceMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionSendLargeMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.SessionSendMessage;

import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.BACKUP_REQUEST;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.BACKUP_REQUEST_RESPONSE;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.CLUSTER_CONNECT;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.CLUSTER_CONNECT_REPLY;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.NODE_ANNOUNCE;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.QUORUM_VOTE;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.QUORUM_VOTE_REPLY;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.REPLICATION_APPEND;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.REPLICATION_APPEND_TX;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.REPLICATION_COMMIT_ROLLBACK;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.REPLICATION_DELETE;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.REPLICATION_DELETE_TX;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.REPLICATION_LARGE_MESSAGE_BEGIN;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.REPLICATION_LARGE_MESSAGE_END;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.REPLICATION_LARGE_MESSAGE_WRITE;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.REPLICATION_PAGE_EVENT;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.REPLICATION_PAGE_WRITE;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.REPLICATION_PREPARE;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.REPLICATION_RESPONSE;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.REPLICATION_RESPONSE_V2;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.SCALEDOWN_ANNOUNCEMENT;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.SESS_SEND;
import static org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl.SESS_SEND_LARGE;

public class ServerPacketDecoder extends ClientPacketDecoder {

   private static final long serialVersionUID = 3348673114388400766L;
   public static final ServerPacketDecoder INSTANCE = new ServerPacketDecoder();

   @Override
   public Packet decode(final ActiveMQBuffer in) {
      final byte packetType = in.readByte();

      Packet packet;

      switch (packetType) {

         case SESS_SEND: {
            packet = new SessionSendMessage(new CoreMessage());
            break;
         }
         case SESS_SEND_LARGE: {
            packet = new SessionSendLargeMessage(new CoreMessage());
            break;
         }
         case REPLICATION_APPEND: {
            packet = new ReplicationAddMessage();
            break;
         }
         case REPLICATION_APPEND_TX: {
            packet = new ReplicationAddTXMessage();
            break;
         }
         case REPLICATION_DELETE: {
            packet = new ReplicationDeleteMessage();
            break;
         }
         case REPLICATION_DELETE_TX: {
            packet = new ReplicationDeleteTXMessage();
            break;
         }
         case REPLICATION_PREPARE: {
            packet = new ReplicationPrepareMessage();
            break;
         }
         case REPLICATION_COMMIT_ROLLBACK: {
            packet = new ReplicationCommitMessage();
            break;
         }
         case REPLICATION_RESPONSE: {
            packet = new ReplicationResponseMessage();
            break;
         }
         case REPLICATION_RESPONSE_V2: {
            packet = new ReplicationResponseMessageV2();
            break;
         }
         case REPLICATION_PAGE_WRITE: {
            packet = new ReplicationPageWriteMessage();
            break;
         }
         case REPLICATION_PAGE_EVENT: {
            packet = new ReplicationPageEventMessage();
            break;
         }
         case REPLICATION_LARGE_MESSAGE_BEGIN: {
            packet = new ReplicationLargeMessageBeginMessage();
            break;
         }
         case REPLICATION_LARGE_MESSAGE_END: {
            packet = new ReplicationLargeMessageEndMessage();
            break;
         }
         case REPLICATION_LARGE_MESSAGE_WRITE: {
            packet = new ReplicationLargeMessageWriteMessage();
            break;
         }
         case PacketImpl.BACKUP_REGISTRATION: {
            packet = new BackupRegistrationMessage();
            break;
         }
         case PacketImpl.BACKUP_REGISTRATION_FAILED: {
            packet = new BackupReplicationStartFailedMessage();
            break;
         }
         case PacketImpl.REPLICATION_START_FINISH_SYNC: {
            packet = new ReplicationStartSyncMessage();
            break;
         }
         case PacketImpl.REPLICATION_SYNC_FILE: {
            packet = new ReplicationSyncFileMessage();
            break;
         }
         case PacketImpl.REPLICATION_SCHEDULED_FAILOVER: {
            packet = new ReplicationLiveIsStoppingMessage();
            break;
         }
         case CLUSTER_CONNECT: {
            packet = new ClusterConnectMessage();
            break;
         }
         case CLUSTER_CONNECT_REPLY: {
            packet = new ClusterConnectReplyMessage();
            break;
         }
         case NODE_ANNOUNCE: {
            packet = new NodeAnnounceMessage();
            break;
         }
         case BACKUP_REQUEST: {
            packet = new BackupRequestMessage();
            break;
         }
         case BACKUP_REQUEST_RESPONSE: {
            packet = new BackupResponseMessage();
            break;
         }
         case QUORUM_VOTE: {
            packet = new QuorumVoteMessage();
            break;
         }
         case QUORUM_VOTE_REPLY: {
            packet = new QuorumVoteReplyMessage();
            break;
         }
         case SCALEDOWN_ANNOUNCEMENT: {
            packet = new ScaleDownAnnounceMessage();
            break;
         }
         default: {
            packet = super.decode(packetType);
         }
      }

      packet.decode(in);

      return packet;
   }

}
