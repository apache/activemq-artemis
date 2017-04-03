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

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.core.protocol.core.Packet;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.utils.DataConstants;

public class PacketImpl implements Packet {
   // Constants -------------------------------------------------------------------------

   // The minimal size for all the packets, Common data for all the packets (look at
   // PacketImpl.encode)
   public static final int PACKET_HEADERS_SIZE = DataConstants.SIZE_INT + DataConstants.SIZE_BYTE +
      DataConstants.SIZE_LONG;

   private static final int INITIAL_PACKET_SIZE = 1500;

   protected long channelID;

   private final byte type;

   protected int size = -1;

   // The packet types
   // -----------------------------------------------------------------------------------

   public static final byte PING = 10;

   public static final byte DISCONNECT = 11;

   public static final byte DISCONNECT_CONSUMER = 12;

   public static final byte DISCONNECT_CONSUMER_KILL = 13;

   // Miscellaneous
   public static final byte EXCEPTION = 20;

   public static final byte NULL_RESPONSE = 21;

   public static final byte PACKETS_CONFIRMED = 22;

   // Server
   public static final byte CREATESESSION = 30;

   public static final byte CREATESESSION_RESP = 31;

   public static final byte REATTACH_SESSION = 32;

   public static final byte REATTACH_SESSION_RESP = 33;

   public static final byte CREATE_QUEUE = 34;

   public static final byte DELETE_QUEUE = 35;

   public static final byte CREATE_SHARED_QUEUE = 36;

   // Session

   public static final byte SESS_XA_FAILED = 39;

   public static final byte SESS_CREATECONSUMER = 40;

   public static final byte SESS_ACKNOWLEDGE = 41;

   public static final byte SESS_EXPIRED = 42;

   public static final byte SESS_COMMIT = 43;

   public static final byte SESS_ROLLBACK = 44;

   public static final byte SESS_QUEUEQUERY = 45;

   public static final byte SESS_QUEUEQUERY_RESP = 46;

   public static final byte SESS_BINDINGQUERY = 49;

   public static final byte SESS_BINDINGQUERY_RESP = 50;

   public static final byte SESS_XA_START = 51;

   public static final byte SESS_XA_END = 52;

   public static final byte SESS_XA_COMMIT = 53;

   public static final byte SESS_XA_PREPARE = 54;

   public static final byte SESS_XA_RESP = 55;

   public static final byte SESS_XA_ROLLBACK = 56;

   public static final byte SESS_XA_JOIN = 57;

   public static final byte SESS_XA_SUSPEND = 58;

   public static final byte SESS_XA_RESUME = 59;

   public static final byte SESS_XA_FORGET = 60;

   public static final byte SESS_XA_INDOUBT_XIDS = 61;

   public static final byte SESS_XA_INDOUBT_XIDS_RESP = 62;

   public static final byte SESS_XA_SET_TIMEOUT = 63;

   public static final byte SESS_XA_SET_TIMEOUT_RESP = 64;

   public static final byte SESS_XA_GET_TIMEOUT = 65;

   public static final byte SESS_XA_GET_TIMEOUT_RESP = 66;

   public static final byte SESS_START = 67;

   public static final byte SESS_STOP = 68;

   public static final byte SESS_CLOSE = 69;

   public static final byte SESS_FLOWTOKEN = 70;

   public static final byte SESS_SEND = 71;

   public static final byte SESS_SEND_LARGE = 72;

   public static final byte SESS_SEND_CONTINUATION = 73;

   public static final byte SESS_CONSUMER_CLOSE = 74;

   public static final byte SESS_RECEIVE_MSG = 75;

   public static final byte SESS_RECEIVE_LARGE_MSG = 76;

   public static final byte SESS_RECEIVE_CONTINUATION = 77;

   public static final byte SESS_FORCE_CONSUMER_DELIVERY = 78;

   public static final byte SESS_PRODUCER_REQUEST_CREDITS = 79;

   public static final byte SESS_PRODUCER_CREDITS = 80;

   public static final byte SESS_INDIVIDUAL_ACKNOWLEDGE = 81;

   public static final byte SESS_PRODUCER_FAIL_CREDITS = 82;

   // Replication

   public static final byte REPLICATION_RESPONSE = 90;

   public static final byte REPLICATION_APPEND = 91;

   public static final byte REPLICATION_APPEND_TX = 92;

   public static final byte REPLICATION_DELETE = 93;

   public static final byte REPLICATION_DELETE_TX = 94;

   public static final byte REPLICATION_PREPARE = 95;

   public static final byte REPLICATION_COMMIT_ROLLBACK = 96;

   public static final byte REPLICATION_PAGE_WRITE = 97;

   public static final byte REPLICATION_PAGE_EVENT = 98;

   public static final byte REPLICATION_LARGE_MESSAGE_BEGIN = 99;

   public static final byte REPLICATION_LARGE_MESSAGE_END = 100;

   public static final byte REPLICATION_LARGE_MESSAGE_WRITE = 101;

   /*
    * code 102 was REPLICATION_COMPARE_DATA, released into production as a message, but as part of
    * the (then) non-function replication system.
    */

   public static final byte REPLICATION_SYNC_FILE = 103;

   public static final byte SESS_ADD_METADATA = 104;

   public static final byte SESS_ADD_METADATA2 = 105;

   public static final byte SESS_UNIQUE_ADD_METADATA = 106;

   // HA

   public static final byte CLUSTER_TOPOLOGY = 110;

   public static final byte NODE_ANNOUNCE = 111;

   public static final byte SUBSCRIBE_TOPOLOGY = 112;

   // For newer versions

   public static final byte SUBSCRIBE_TOPOLOGY_V2 = 113;

   public static final byte CLUSTER_TOPOLOGY_V2 = 114;

   public static final byte BACKUP_REGISTRATION = 115;
   public static final byte BACKUP_REGISTRATION_FAILED = 116;

   public static final byte REPLICATION_START_FINISH_SYNC = 120;
   public static final byte REPLICATION_SCHEDULED_FAILOVER = 121;

   public static final byte CLUSTER_TOPOLOGY_V3 = 122;

   //do not reuse
   //public static final byte NODE_ANNOUNCE_V2 = 123;

   public static final byte DISCONNECT_V2 = 124;

   public static final byte CLUSTER_CONNECT = 125;

   public static final byte CLUSTER_CONNECT_REPLY = 126;

   public static final byte BACKUP_REQUEST = 127;

   //oops ran out of positive bytes
   public static final byte BACKUP_REQUEST_RESPONSE = -1;

   public static final byte QUORUM_VOTE = -2;

   public static final byte QUORUM_VOTE_REPLY = -3;

   public static final byte CHECK_FOR_FAILOVER = -4;

   public static final byte CHECK_FOR_FAILOVER_REPLY = -5;

   public static final byte SCALEDOWN_ANNOUNCEMENT = -6;

   public static final byte SESS_QUEUEQUERY_RESP_V2 = -7;

   public static final byte SESS_BINDINGQUERY_RESP_V2 = -8;

   public static final byte REPLICATION_RESPONSE_V2 = -9;

   public static final byte SESS_BINDINGQUERY_RESP_V3 = -10;

   // Static --------------------------------------------------------

   public PacketImpl(final byte type) {
      this.type = type;
   }

   // Public --------------------------------------------------------

   @Override
   public byte getType() {
      return type;
   }

   @Override
   public long getChannelID() {
      return channelID;
   }

   @Override
   public void setChannelID(final long channelID) {
      this.channelID = channelID;
   }

   @Override
   public ActiveMQBuffer encode(final RemotingConnection connection) {
      ActiveMQBuffer buffer = connection.createTransportBuffer(PacketImpl.INITIAL_PACKET_SIZE);

      // The standard header fields

      buffer.writeInt(0); // The length gets filled in at the end
      buffer.writeByte(type);
      buffer.writeLong(channelID);

      encodeRest(buffer);

      size = buffer.writerIndex();

      // The length doesn't include the actual length byte
      int len = size - DataConstants.SIZE_INT;

      buffer.setInt(0, len);

      return buffer;
   }

   @Override
   public void decode(final ActiveMQBuffer buffer) {
      channelID = buffer.readLong();

      decodeRest(buffer);

      size = buffer.readerIndex();
   }

   @Override
   public int getPacketSize() {
      if (size == -1) {
         throw new IllegalStateException("Packet hasn't been encoded/decoded yet");
      }

      return size;
   }

   @Override
   public boolean isResponse() {
      return false;
   }

   public void encodeRest(final ActiveMQBuffer buffer) {
   }

   public void decodeRest(final ActiveMQBuffer buffer) {
   }

   @Override
   public boolean isRequiresConfirmations() {
      return true;
   }

   @Override
   public String toString() {
      return getParentString() + "]";
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + (int) (channelID ^ (channelID >>> 32));
      result = prime * result + size;
      result = prime * result + type;
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj) {
         return true;
      }
      if (!(obj instanceof PacketImpl)) {
         return false;
      }
      PacketImpl other = (PacketImpl) obj;
      return (channelID == other.channelID) && (size == other.size) && (type != other.type);
   }

   protected String getParentString() {
      return "PACKET(" + this.getClass().getSimpleName() + ")[type=" + type + ", channelID=" + channelID + ", packetObject=" + this.getClass().getSimpleName();
   }

   private int stringEncodeSize(final String str) {
      return DataConstants.SIZE_INT + str.length() * 2;
   }

   protected int nullableStringEncodeSize(final String str) {
      return DataConstants.SIZE_BOOLEAN + (str != null ? stringEncodeSize(str) : 0);
   }
}
