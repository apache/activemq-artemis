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
package org.apache.activemq.artemis.core.protocol.core.impl.wireformat;

import java.util.Objects;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl;
import org.apache.activemq.artemis.utils.DataConstants;
import org.apache.activemq.artemis.utils.UUID;

/**
 * Ping is sent on the client side by {@link org.apache.activemq.artemis.core.client.impl.ClientSessionFactoryImpl}. At the server's
 * side it is handled by org.apache.activemq.artemis.core.remoting.server.impl.RemotingServiceImpl
 */
public final class Ping extends PacketImpl {

   private long connectionTTL;
   /**
    * the UUID of the node which sent the ping
    */
   private UUID nodeUUID;

   /**
    * Used for sending pings.
    *
    * @param nodeUUID the UUID of the node which sent the ping
    */
   public Ping(UUID nodeUUID, final long connectionTTL) {
      super(PING);

      this.connectionTTL = connectionTTL;
      this.nodeUUID = nodeUUID;

   }

   /**
    * Used for decoding of incoming pings.
    */
   public Ping() {
      super(PING);
   }

   public long getConnectionTTL() {
      return connectionTTL;
   }

   public UUID getNodeUUID() {
      return nodeUUID;
   }

   @Override
   public void encodeRest(final ActiveMQBuffer buffer) {
      buffer.writeLong(connectionTTL);
      if (nodeUUID != null) {
         buffer.writeBytes(nodeUUID.asBytes());
      }
   }

   @Override
   public void decodeRest(final ActiveMQBuffer buffer) {
      connectionTTL = buffer.readLong();
      if (buffer.readable()) {
         byte[] bytes = new byte[16];
         buffer.readBytes(bytes);
         nodeUUID =  new UUID(UUID.TYPE_TIME_BASED, bytes);
      }
   }

   @Override
   public boolean isRequiresConfirmations() {
      return false;
   }

   @Override
   public int expectedEncodeSize() {
      int numLongs = nodeUUID == null ? 1 : 3;
      return PACKET_HEADERS_SIZE + numLongs * DataConstants.SIZE_LONG;
   }

   @Override
   protected String getPacketString() {
      return super.getPacketString()
             + ", connectionTTL=" + connectionTTL
             + ", nodeUUID=" + nodeUUID;
   }

   @Override
   public int hashCode() {
      return Objects.hash(connectionTTL, nodeUUID);
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj) {
         return true;
      }
      if (!super.equals(obj)) {
         return false;
      }
      if (!(obj instanceof Ping)) {
         return false;
      }
      Ping other = (Ping) obj;
      if (connectionTTL != other.connectionTTL) {
         return false;
      }
      return Objects.equals(nodeUUID, other.nodeUUID);
   }
}
