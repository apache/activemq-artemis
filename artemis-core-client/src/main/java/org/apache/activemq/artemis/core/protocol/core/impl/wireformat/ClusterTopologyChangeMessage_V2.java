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

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.TransportConfiguration;

/**
 * Clebert Suconic
 */
public class ClusterTopologyChangeMessage_V2 extends ClusterTopologyChangeMessage {

   protected long uniqueEventID;
   protected String backupGroupName;

   public ClusterTopologyChangeMessage_V2(final long uniqueEventID,
                                          final String nodeID,
                                          final String backupGroupName,
                                          final Pair<TransportConfiguration, TransportConfiguration> pair,
                                          final boolean last) {
      super(CLUSTER_TOPOLOGY_V2);

      this.nodeID = nodeID;

      this.pair = pair;

      this.last = last;

      this.exit = false;

      this.uniqueEventID = uniqueEventID;

      this.backupGroupName = backupGroupName;
   }

   public ClusterTopologyChangeMessage_V2(final long uniqueEventID, final String nodeID) {
      super(CLUSTER_TOPOLOGY_V2);

      this.exit = true;

      this.nodeID = nodeID;

      this.uniqueEventID = uniqueEventID;
   }

   public ClusterTopologyChangeMessage_V2() {
      super(CLUSTER_TOPOLOGY_V2);
   }

   public ClusterTopologyChangeMessage_V2(byte clusterTopologyV3) {
      super(clusterTopologyV3);
   }

   /**
    * @return the uniqueEventID
    */
   public long getUniqueEventID() {
      return uniqueEventID;
   }

   public String getBackupGroupName() {
      return backupGroupName;
   }

   @Override
   public void encodeRest(final ActiveMQBuffer buffer) {
      buffer.writeBoolean(exit);
      buffer.writeString(nodeID);
      buffer.writeLong(uniqueEventID);
      if (!exit) {
         if (pair.getA() != null) {
            buffer.writeBoolean(true);
            pair.getA().encode(buffer);
         } else {
            buffer.writeBoolean(false);
         }
         if (pair.getB() != null) {
            buffer.writeBoolean(true);
            pair.getB().encode(buffer);
         } else {
            buffer.writeBoolean(false);
         }
         buffer.writeBoolean(last);
      }
      buffer.writeNullableString(backupGroupName);
   }

   @Override
   public void decodeRest(final ActiveMQBuffer buffer) {
      exit = buffer.readBoolean();
      nodeID = buffer.readString();
      uniqueEventID = buffer.readLong();
      if (!exit) {
         boolean hasLive = buffer.readBoolean();
         TransportConfiguration a;
         if (hasLive) {
            a = new TransportConfiguration();
            a.decode(buffer);
         } else {
            a = null;
         }
         boolean hasBackup = buffer.readBoolean();
         TransportConfiguration b;
         if (hasBackup) {
            b = new TransportConfiguration();
            b.decode(buffer);
         } else {
            b = null;
         }
         pair = new Pair<>(a, b);
         last = buffer.readBoolean();
      }
      if (buffer.readableBytes() > 0) {
         backupGroupName = buffer.readNullableString();
      }
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + ((backupGroupName == null) ? 0 : backupGroupName.hashCode());
      result = prime * result + (int) (uniqueEventID ^ (uniqueEventID >>> 32));
      return result;
   }

   @Override
   public String toString() {
      StringBuffer buff = new StringBuffer(getParentString());
      buff.append(", exit=" + exit);
      buff.append(", last=" + last);
      buff.append(", nodeID=" + nodeID);
      buff.append(", pair=" + pair);
      buff.append(", backupGroupName=" + backupGroupName);
      buff.append(", uniqueEventID=" + uniqueEventID);
      buff.append("]");
      return buff.toString();
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj) {
         return true;
      }
      if (!super.equals(obj)) {
         return false;
      }
      if (!(obj instanceof ClusterTopologyChangeMessage_V2)) {
         return false;
      }
      ClusterTopologyChangeMessage_V2 other = (ClusterTopologyChangeMessage_V2) obj;
      if (uniqueEventID != other.uniqueEventID) {
         return false;
      }
      if (backupGroupName == null) {
         if (other.backupGroupName != null) {
            return false;
         }
      } else if (!backupGroupName.equals(other.backupGroupName)) {
         return false;
      }
      return true;
   }
}
