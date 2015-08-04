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
import org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl;

public final class ReplicationCommitMessage extends PacketImpl {

   /**
    * 0 - BindingsImpl, 1 - MessagesJournal
    */
   private byte journalID;

   private boolean rollback;

   private long txId;

   public ReplicationCommitMessage() {
      super(PacketImpl.REPLICATION_COMMIT_ROLLBACK);
   }

   public ReplicationCommitMessage(final byte journalID, final boolean rollback, final long txId) {
      this();
      this.journalID = journalID;
      this.rollback = rollback;
      this.txId = txId;
   }

   @Override
   public void encodeRest(final ActiveMQBuffer buffer) {
      buffer.writeByte(journalID);
      buffer.writeBoolean(rollback);
      buffer.writeLong(txId);
   }

   @Override
   public void decodeRest(final ActiveMQBuffer buffer) {
      journalID = buffer.readByte();
      rollback = buffer.readBoolean();
      txId = buffer.readLong();
   }

   public boolean isRollback() {
      return rollback;
   }

   public long getTxId() {
      return txId;
   }

   /**
    * @return the journalID
    */
   public byte getJournalID() {
      return journalID;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + journalID;
      result = prime * result + (rollback ? 1231 : 1237);
      result = prime * result + (int) (txId ^ (txId >>> 32));
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (!super.equals(obj))
         return false;
      if (!(obj instanceof ReplicationCommitMessage))
         return false;
      ReplicationCommitMessage other = (ReplicationCommitMessage) obj;
      if (journalID != other.journalID)
         return false;
      if (rollback != other.rollback)
         return false;
      if (txId != other.txId)
         return false;
      return true;
   }

   @Override
   public String toString() {
      String txOperation = rollback ? "rollback" : "commmit";
      return ReplicationCommitMessage.class.getSimpleName() + "[type=" + getType() + ", channel=" + getChannelID() +
         ", journalID=" + journalID + ", txAction='" + txOperation + "']";
   }
}
