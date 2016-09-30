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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Set;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.persistence.impl.journal.AbstractJournalStorageManager;
import org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl;

/**
 * Message is used to sync {@link org.apache.activemq.artemis.core.io.SequentialFile}s to a backup server. The {@link FileType} controls
 * which extra information is sent.
 */
public final class ReplicationSyncFileMessage extends PacketImpl {

   /**
    * The JournalType or {@code null} if sync'ing large-messages.
    */
   private AbstractJournalStorageManager.JournalContent journalType;
   /**
    * This value refers to {@link org.apache.activemq.artemis.core.journal.impl.JournalFile#getFileID()}, or the
    * message id if we are sync'ing a large-message.
    */
   private long fileId;
   private int dataSize;
   private ByteBuffer byteBuffer;
   private byte[] byteArray;
   private SimpleString pageStoreName;
   private FileType fileType;

   public enum FileType {
      JOURNAL(0), PAGE(1), LARGE_MESSAGE(2);

      private byte code;
      private static final Set<FileType> ALL_OF = EnumSet.allOf(FileType.class);

      FileType(int code) {
         this.code = (byte) code;
      }

      /**
       * @param readByte
       * @return {@link FileType} corresponding to the byte code.
       */
      public static FileType getFileType(byte readByte) {
         for (FileType type : ALL_OF) {
            if (type.code == readByte)
               return type;
         }
         throw new InternalError("Unsupported byte value for " + FileType.class);
      }
   }

   public ReplicationSyncFileMessage() {
      super(REPLICATION_SYNC_FILE);
   }

   public ReplicationSyncFileMessage(AbstractJournalStorageManager.JournalContent content,
                                     SimpleString storeName,
                                     long id,
                                     int size,
                                     ByteBuffer buffer) {
      this();
      this.byteBuffer = buffer;
      this.pageStoreName = storeName;
      this.dataSize = size;
      this.fileId = id;
      this.journalType = content;
      determineType();
   }

   private void determineType() {
      if (journalType != null) {
         fileType = FileType.JOURNAL;
      } else if (pageStoreName != null) {
         fileType = FileType.PAGE;
      } else {
         fileType = FileType.LARGE_MESSAGE;
      }
   }

   @Override
   public void encodeRest(final ActiveMQBuffer buffer) {
      buffer.writeLong(fileId);
      if (fileId == -1)
         return;
      buffer.writeByte(fileType.code);
      switch (fileType) {
         case JOURNAL: {
            buffer.writeByte(journalType.typeByte);
            break;
         }
         case PAGE: {
            buffer.writeSimpleString(pageStoreName);
            break;
         }
         case LARGE_MESSAGE:
         default:
            // no-op
      }

      buffer.writeInt(dataSize);
      /*
       * sending -1 will close the file in case of a journal, but not in case of a largeMessage
       * (which might receive appends)
       */
      if (dataSize > 0) {
         buffer.writeBytes(byteBuffer);
      }
   }

   @Override
   public void decodeRest(final ActiveMQBuffer buffer) {
      fileId = buffer.readLong();
      switch (FileType.getFileType(buffer.readByte())) {
         case JOURNAL: {
            journalType = AbstractJournalStorageManager.JournalContent.getType(buffer.readByte());
            fileType = FileType.JOURNAL;
            break;
         }
         case PAGE: {
            pageStoreName = buffer.readSimpleString();
            fileType = FileType.PAGE;
            break;
         }
         case LARGE_MESSAGE: {
            fileType = FileType.LARGE_MESSAGE;
            break;
         }
      }
      int size = buffer.readInt();
      if (size > 0) {
         byteArray = new byte[size];
         buffer.readBytes(byteArray);
      }
   }

   public long getId() {
      return fileId;
   }

   public AbstractJournalStorageManager.JournalContent getJournalContent() {
      return journalType;
   }

   public byte[] getData() {
      return byteArray;
   }

   public FileType getFileType() {
      return fileType;
   }

   public SimpleString getPageStore() {
      return pageStoreName;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + Arrays.hashCode(byteArray);
      result = prime * result + ((byteBuffer == null) ? 0 : byteBuffer.hashCode());
      result = prime * result + dataSize;
      result = prime * result + (int) (fileId ^ (fileId >>> 32));
      result = prime * result + ((fileType == null) ? 0 : fileType.hashCode());
      result = prime * result + ((journalType == null) ? 0 : journalType.hashCode());
      result = prime * result + ((pageStoreName == null) ? 0 : pageStoreName.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj) {
         return true;
      }
      if (!super.equals(obj)) {
         return false;
      }
      if (!(obj instanceof ReplicationSyncFileMessage)) {
         return false;
      }
      ReplicationSyncFileMessage other = (ReplicationSyncFileMessage) obj;
      if (!Arrays.equals(byteArray, other.byteArray)) {
         return false;
      }
      if (byteBuffer == null) {
         if (other.byteBuffer != null) {
            return false;
         }
      } else if (!byteBuffer.equals(other.byteBuffer)) {
         return false;
      }
      if (dataSize != other.dataSize) {
         return false;
      }
      if (fileId != other.fileId) {
         return false;
      }
      if (fileType != other.fileType) {
         return false;
      }
      if (journalType != other.journalType) {
         return false;
      }
      if (pageStoreName == null) {
         if (other.pageStoreName != null) {
            return false;
         }
      } else if (!pageStoreName.equals(other.pageStoreName)) {
         return false;
      }
      return true;
   }

   @Override
   public String toString() {
      return ReplicationSyncFileMessage.class.getSimpleName() + "(" + fileType +
         (journalType != null ? ", " + journalType : "") + ", id=" + fileId + ")";
   }
}
