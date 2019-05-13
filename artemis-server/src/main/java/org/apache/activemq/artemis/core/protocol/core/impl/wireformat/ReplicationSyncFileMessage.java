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

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.persistence.impl.journal.AbstractJournalStorageManager;
import org.apache.activemq.artemis.core.protocol.core.CoreRemotingConnection;
import org.apache.activemq.artemis.core.protocol.core.impl.PacketImpl;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnection;
import org.apache.activemq.artemis.utils.DataConstants;
import org.jboss.logging.Logger;

/**
 * Message is used to sync {@link org.apache.activemq.artemis.core.io.SequentialFile}s to a backup server. The {@link FileType} controls
 * which extra information is sent.
 */
public final class ReplicationSyncFileMessage extends PacketImpl {
   private static final Logger logger = Logger.getLogger(ReplicationSyncFileMessage.class);

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
   private byte[] byteArray;
   private SimpleString pageStoreName;
   private FileType fileType;
   private RandomAccessFile raf;
   private FileChannel fileChannel;
   private long offset;

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
                                     RandomAccessFile raf,
                                     FileChannel fileChannel,
                                     long offset,
                                     int size) {
      this();
      this.pageStoreName = storeName;
      this.dataSize = size;
      this.fileId = id;
      this.raf = raf;
      this.fileChannel = fileChannel;
      this.journalType = content;
      this.offset = offset;
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

   public long getFileId() {
      return fileId;
   }

   public int getDataSize() {
      return dataSize;
   }

   public RandomAccessFile getRaf() {
      return raf;
   }

   public FileChannel getFileChannel() {
      return fileChannel;
   }

   public long getOffset() {
      return offset;
   }

   @Override
   public int expectedEncodeSize() {
      int size = PACKET_HEADERS_SIZE +
         DataConstants.SIZE_LONG; // buffer.writeLong(fileId);

      if (fileId == -1)
         return size;

      size += DataConstants.SIZE_BYTE; // buffer.writeByte(fileType.code);
      switch (fileType) {
         case JOURNAL: {
            size += DataConstants.SIZE_BYTE; // buffer.writeByte(journalType.typeByte);
            break;
         }
         case PAGE: {
            size += SimpleString.sizeofString(pageStoreName);
            break;
         }
         case LARGE_MESSAGE:
         default:
            // no-op
      }

      size += DataConstants.SIZE_INT; // buffer.writeInt(dataSize);

      if (dataSize > 0) {
         size += dataSize;
      }

      return size;
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
   }

   @Override
   public ActiveMQBuffer encode(CoreRemotingConnection connection) {
      if (fileId != -1 && dataSize > 0) {
         ActiveMQBuffer buffer;
         int bufferSize = expectedEncodeSize();
         int encodedSize = bufferSize;
         boolean isNetty = false;
         if (connection != null && connection.getTransportConnection() instanceof NettyConnection) {
            bufferSize -= dataSize;
            isNetty = true;
         }
         buffer = createPacket(connection, bufferSize);
         encodeHeader(buffer);
         encodeRest(buffer, connection);
         if (!isNetty) {
            ByteBuffer byteBuffer;
            if (buffer.byteBuf() != null && buffer.byteBuf().nioBufferCount() == 1) {
               byteBuffer = buffer.byteBuf().internalNioBuffer(buffer.writerIndex(), buffer.writableBytes());
            } else {
               byteBuffer = buffer.toByteBuffer(buffer.writerIndex(), buffer.writableBytes());
            }
            readFile(byteBuffer);
            buffer.writerIndex(buffer.capacity());
         }
         encodeSize(buffer, encodedSize);
         return buffer;
      } else {
         return super.encode(connection);
      }
   }

   @Override
   public void release() {
      if (raf != null) {
         try {
            raf.close();
         } catch (IOException e) {
            logger.error("Close file " + this + " failed", e);
         }
      }
   }

   @Override
   public void decodeRest(final ActiveMQBuffer buffer) {
      fileId = buffer.readLong();
      if (fileId == -1) return;
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

   private void readFile(ByteBuffer buffer) {
      try {
         fileChannel.read(buffer, offset);
      } catch (IOException e) {
         throw new RuntimeException(e);
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
   public boolean equals(Object o) {
      if (this == o)
         return true;
      if (o == null || getClass() != o.getClass())
         return false;
      if (!super.equals(o))
         return false;
      ReplicationSyncFileMessage that = (ReplicationSyncFileMessage) o;
      return fileId == that.fileId && dataSize == that.dataSize && offset == that.offset && journalType == that.journalType && Arrays.equals(byteArray, that.byteArray) && Objects.equals(pageStoreName, that.pageStoreName) && fileType == that.fileType && Objects.equals(raf, that.raf) && Objects.equals(fileChannel, that.fileChannel);
   }

   @Override
   public int hashCode() {
      int result = Objects.hash(super.hashCode(), journalType, fileId, dataSize, pageStoreName, fileType, raf, fileChannel, offset);
      result = 31 * result + Arrays.hashCode(byteArray);
      return result;
   }

   @Override
   public String toString() {
      return ReplicationSyncFileMessage.class.getSimpleName() + "(" + fileType +
         (journalType != null ? ", " + journalType : "") + ", id=" + fileId + ")";
   }
}
