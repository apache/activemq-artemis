/**
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
package org.apache.activemq.core.protocol.core.impl.wireformat;

import java.security.InvalidParameterException;
import java.util.Arrays;
import java.util.List;

import org.apache.activemq.api.core.ActiveMQBuffer;
import org.apache.activemq.core.journal.impl.JournalFile;
import org.apache.activemq.core.persistence.impl.journal.JournalStorageManager.JournalContent;
import org.apache.activemq.core.protocol.core.impl.PacketImpl;

/**
 * This message may signal start or end of the replication synchronization.
 * <p>
 * At start, it sends all fileIDs used in a given journal live server to the backup, so the backup
 * can reserve those IDs.
 */
public class ReplicationStartSyncMessage extends PacketImpl
{
   private long[] ids;
   private SyncDataType dataType;
   private boolean synchronizationIsFinished;
   private String nodeID;
   private boolean allowsAutoFailBack;

   public enum SyncDataType
   {
      JournalBindings(JournalContent.BINDINGS.typeByte),
      JournalMessages(JournalContent.MESSAGES.typeByte),
      LargeMessages((byte)2);

      private byte code;

      private SyncDataType(byte code)
      {
         this.code = code;
      }

      public static JournalContent getJournalContentType(SyncDataType dataType)
      {
         return JournalContent.getType(dataType.code);
      }

      public static SyncDataType getDataType(byte code)
      {
         if (code == JournalBindings.code)
            return JournalBindings;
         if (code == JournalMessages.code)
            return JournalMessages;
         if (code == LargeMessages.code)
            return LargeMessages;
         throw new InvalidParameterException("invalid byte: " + code);
      }
   }

   public ReplicationStartSyncMessage()
   {
      super(REPLICATION_START_FINISH_SYNC);
   }

   public ReplicationStartSyncMessage(List<Long> filenames)
   {
      this();
      ids = new long[filenames.size()];
      for (int i = 0; i < filenames.size(); i++)
      {
         ids[i] = filenames.get(i);
      }
      dataType = SyncDataType.LargeMessages;
      nodeID = ""; // this value will be ignored
   }

   public ReplicationStartSyncMessage(String nodeID)
   {
      this();
      synchronizationIsFinished = true;
      this.nodeID = nodeID;
   }

   public ReplicationStartSyncMessage(JournalFile[] datafiles, JournalContent contentType, String nodeID,
                                      boolean allowsAutoFailBack)
   {
      this();
      this.nodeID = nodeID;
      this.allowsAutoFailBack = allowsAutoFailBack;
      synchronizationIsFinished = false;
      ids = new long[datafiles.length];
      for (int i = 0; i < datafiles.length; i++)
      {
         ids[i] = datafiles[i].getFileID();
      }
      switch (contentType)
      {
         case MESSAGES:
            dataType = SyncDataType.JournalMessages;
            break;
         case BINDINGS:
            dataType = SyncDataType.JournalBindings;
            break;
         default:
            throw new IllegalArgumentException();
      }
   }

   @Override
   public void encodeRest(final ActiveMQBuffer buffer)
   {
      buffer.writeBoolean(synchronizationIsFinished);
      buffer.writeBoolean(allowsAutoFailBack);
      buffer.writeString(nodeID);
      if (synchronizationIsFinished)
         return;
      buffer.writeByte(dataType.code);
      buffer.writeInt(ids.length);
      for (long id : ids)
      {
         buffer.writeLong(id);
      }
   }

   @Override
   public void decodeRest(final ActiveMQBuffer buffer)
   {
      synchronizationIsFinished = buffer.readBoolean();
      allowsAutoFailBack = buffer.readBoolean();
      nodeID = buffer.readString();
      if (synchronizationIsFinished)
      {
         return;
      }
      dataType = SyncDataType.getDataType(buffer.readByte());
      int length = buffer.readInt();
      ids = new long[length];
      for (int i = 0; i < length; i++)
      {
         ids[i] = buffer.readLong();
      }
   }

   /**
    * @return whether the server is configured to allow for fail-back
    * @see Configuration#isAllowAutoFailBack()
    */
   public boolean isServerToFailBack()
   {
      return allowsAutoFailBack;
   }
   /**
    * @return {@code true} if the live has finished synchronizing its data and the backup is
    *         therefore up-to-date, {@code false} otherwise.
    */
   public boolean isSynchronizationFinished()
   {
      return synchronizationIsFinished;
   }

   public SyncDataType getDataType()
   {
      return dataType;
   }

   public long[] getFileIds()
   {
      return ids;
   }

   public String getNodeID()
   {
      return nodeID;
   }

   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + (allowsAutoFailBack ? 1231 : 1237);
      result = prime * result + ((dataType == null) ? 0 : dataType.hashCode());
      result = prime * result + Arrays.hashCode(ids);
      result = prime * result + ((nodeID == null) ? 0 : nodeID.hashCode());
      result = prime * result + (synchronizationIsFinished ? 1231 : 1237);
      return result;
   }

   @Override
   public boolean equals(Object obj)
   {
      if (this == obj)
         return true;
      if (!super.equals(obj))
         return false;
      if (!(obj instanceof ReplicationStartSyncMessage))
         return false;
      ReplicationStartSyncMessage other = (ReplicationStartSyncMessage)obj;
      if (allowsAutoFailBack != other.allowsAutoFailBack)
         return false;
      if (dataType != other.dataType)
         return false;
      if (!Arrays.equals(ids, other.ids))
         return false;
      if (nodeID == null)
      {
         if (other.nodeID != null)
            return false;
      }
      else if (!nodeID.equals(other.nodeID))
         return false;
      if (synchronizationIsFinished != other.synchronizationIsFinished)
         return false;
      return true;
   }
}
