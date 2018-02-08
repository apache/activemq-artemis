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
package org.apache.activemq.artemis.core.paging.impl;

import java.util.Arrays;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.paging.PagedMessage;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.persistence.impl.journal.codec.LargeMessagePersister;
import org.apache.activemq.artemis.core.server.LargeServerMessage;
import org.apache.activemq.artemis.spi.core.protocol.MessagePersister;
import org.apache.activemq.artemis.utils.DataConstants;

/**
 * This class represents a paged message
 */
public class PagedMessageImpl implements PagedMessage {

   /**
    * Large messages will need to be instantiated lazily during getMessage when the StorageManager
    * is available
    */
   private byte[] largeMessageLazyData;

   private Message message;

   private long[] queueIDs;

   private long transactionID = 0;

   private volatile StorageManager storageManager;

   public PagedMessageImpl(final Message message, final long[] queueIDs, final long transactionID) {
      this(message, queueIDs);
      this.transactionID = transactionID;
   }

   public PagedMessageImpl(final Message message, final long[] queueIDs) {
      this.queueIDs = queueIDs;
      this.message = message;
   }

   public PagedMessageImpl(StorageManager storageManager) {
      this.storageManager = storageManager;
   }

   @Override
   public Message getMessage() {
      return message;
   }

   @Override
   public void initMessage(StorageManager storage) {
      if (largeMessageLazyData != null) {
         LargeServerMessage lgMessage = storage.createLargeMessage();

         ActiveMQBuffer buffer = ActiveMQBuffers.wrappedBuffer(largeMessageLazyData);
         lgMessage = LargeMessagePersister.getInstance().decode(buffer, lgMessage);
         lgMessage.incrementDelayDeletionCount();
         lgMessage.setPaged();
         this.message = lgMessage;
         largeMessageLazyData = null;
      }
   }

   @Override
   public long getTransactionID() {
      return transactionID;
   }

   @Override
   public long[] getQueueIDs() {
      return queueIDs;
   }

   // EncodingSupport implementation --------------------------------

   @Override
   public void decode(final ActiveMQBuffer buffer) {
      transactionID = buffer.readLong();

      boolean isLargeMessage = buffer.readBoolean();

      if (isLargeMessage) {
         int largeMessageHeaderSize = buffer.readInt();

         if (storageManager == null) {
            largeMessageLazyData = new byte[largeMessageHeaderSize];
            buffer.readBytes(largeMessageLazyData);
         } else {
            this.message = storageManager.createLargeMessage();
            LargeMessagePersister.getInstance().decode(buffer, (LargeServerMessage) message);
            ((LargeServerMessage) message).incrementDelayDeletionCount();
         }
      } else {
         this.message = MessagePersister.getInstance().decode(buffer, null);
      }

      int queueIDsSize = buffer.readInt();

      queueIDs = new long[queueIDsSize];

      for (int i = 0; i < queueIDsSize; i++) {
         queueIDs[i] = buffer.readLong();
      }
   }

   @Override
   public void encode(final ActiveMQBuffer buffer) {
      buffer.writeLong(transactionID);

      boolean isLargeMessage = isLargeMessage();

      buffer.writeBoolean(isLargeMessage);

      if (isLargeMessage) {
         buffer.writeInt(LargeMessagePersister.getInstance().getEncodeSize((LargeServerMessage)message));
         LargeMessagePersister.getInstance().encode(buffer, (LargeServerMessage) message);
      } else {
         message.getPersister().encode(buffer, message);
      }

      buffer.writeInt(queueIDs.length);

      for (long queueID : queueIDs) {
         buffer.writeLong(queueID);
      }
   }

   public boolean isLargeMessage() {
      return message instanceof ICoreMessage && ((ICoreMessage)message).isLargeMessage();
   }

   @Override
   public int getEncodeSize() {
      if (isLargeMessage()) {
         return DataConstants.SIZE_LONG + DataConstants.SIZE_BYTE + DataConstants.SIZE_INT + LargeMessagePersister.getInstance().getEncodeSize((LargeServerMessage)message) +
            DataConstants.SIZE_INT + queueIDs.length * DataConstants.SIZE_LONG;
      } else {
         return DataConstants.SIZE_LONG + DataConstants.SIZE_BYTE + message.getPersister().getEncodeSize(message) +
            DataConstants.SIZE_INT + queueIDs.length * DataConstants.SIZE_LONG;
      }
   }

   @Override
   public String toString() {
      return "PagedMessageImpl [queueIDs=" + Arrays.toString(queueIDs) +
         ", transactionID=" +
         transactionID +
         ", message=" +
         message +
         "]";
   }

   @Override
   public long getPersistentSize() throws ActiveMQException {
      return message.getPersistentSize();
   }
}
