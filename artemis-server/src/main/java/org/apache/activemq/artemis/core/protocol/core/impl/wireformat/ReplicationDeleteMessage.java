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

public final class ReplicationDeleteMessage extends PacketImpl {

   private long id;

   /**
    * 0 - BindingsImpl, 1 - MessagesJournal
    */
   private byte journalID;

   public ReplicationDeleteMessage() {
      super(PacketImpl.REPLICATION_DELETE);
   }

   public ReplicationDeleteMessage(final byte journalID, final long id) {
      this();
      this.journalID = journalID;
      this.id = id;
   }

   @Override
   public void encodeRest(final ActiveMQBuffer buffer) {
      buffer.writeByte(journalID);
      buffer.writeLong(id);
   }

   @Override
   public void decodeRest(final ActiveMQBuffer buffer) {
      journalID = buffer.readByte();
      id = buffer.readLong();
   }

   /**
    * @return the id
    */
   public long getId() {
      return id;
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
      result = prime * result + (int) (id ^ (id >>> 32));
      result = prime * result + journalID;
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (!super.equals(obj))
         return false;
      if (!(obj instanceof ReplicationDeleteMessage))
         return false;
      ReplicationDeleteMessage other = (ReplicationDeleteMessage) obj;
      if (id != other.id)
         return false;
      if (journalID != other.journalID)
         return false;
      return true;
   }
}
