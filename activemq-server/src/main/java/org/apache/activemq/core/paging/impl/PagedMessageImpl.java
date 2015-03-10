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
package org.apache.activemq.core.paging.impl;

import java.util.Arrays;

import org.apache.activemq.api.core.ActiveMQBuffer;
import org.apache.activemq.api.core.ActiveMQBuffers;
import org.apache.activemq.core.paging.PagedMessage;
import org.apache.activemq.core.persistence.StorageManager;
import org.apache.activemq.core.server.LargeServerMessage;
import org.apache.activemq.core.server.ServerMessage;
import org.apache.activemq.core.server.impl.ServerMessageImpl;
import org.apache.activemq.utils.DataConstants;

/**
 * This class represents a paged message
 */
public class PagedMessageImpl implements PagedMessage
{
   /**
    * Large messages will need to be instantiated lazily during getMessage when the StorageManager
    * is available
    */
   private byte[] largeMessageLazyData;

   private ServerMessage message;

   private long[] queueIDs;

   private long transactionID = 0;

   public PagedMessageImpl(final ServerMessage message, final long[] queueIDs, final long transactionID)
   {
      this(message, queueIDs);
      this.transactionID = transactionID;
   }

   public PagedMessageImpl(final ServerMessage message, final long[] queueIDs)
   {
      this.queueIDs = queueIDs;
      this.message = message;
   }

   public PagedMessageImpl()
   {
   }

   public ServerMessage getMessage()
   {
      return message;
   }

   public void initMessage(StorageManager storage)
   {
      if (largeMessageLazyData != null)
      {
         LargeServerMessage lgMessage = storage.createLargeMessage();
         ActiveMQBuffer buffer = ActiveMQBuffers.dynamicBuffer(largeMessageLazyData);
         lgMessage.decodeHeadersAndProperties(buffer);
         lgMessage.incrementDelayDeletionCount();
         lgMessage.setPaged();
         message = lgMessage;
         largeMessageLazyData = null;
      }
   }

   public long getTransactionID()
   {
      return transactionID;
   }

   public long[] getQueueIDs()
   {
      return queueIDs;
   }

   // EncodingSupport implementation --------------------------------

   public void decode(final ActiveMQBuffer buffer)
   {
      transactionID = buffer.readLong();

      boolean isLargeMessage = buffer.readBoolean();

      if (isLargeMessage)
      {
         int largeMessageHeaderSize = buffer.readInt();

         largeMessageLazyData = new byte[largeMessageHeaderSize];

         buffer.readBytes(largeMessageLazyData);
      }
      else
      {
         buffer.readInt(); // This value is only used on LargeMessages for now

         message = new ServerMessageImpl(-1, 50);

         message.decode(buffer);
      }

      int queueIDsSize = buffer.readInt();

      queueIDs = new long[queueIDsSize];

      for (int i = 0; i < queueIDsSize; i++)
      {
         queueIDs[i] = buffer.readLong();
      }
   }

   public void encode(final ActiveMQBuffer buffer)
   {
      buffer.writeLong(transactionID);

      buffer.writeBoolean(message instanceof LargeServerMessage);

      buffer.writeInt(message.getEncodeSize());

      message.encode(buffer);

      buffer.writeInt(queueIDs.length);

      for (long queueID : queueIDs)
      {
         buffer.writeLong(queueID);
      }
   }

   public int getEncodeSize()
   {
      return DataConstants.SIZE_LONG + DataConstants.SIZE_BYTE + DataConstants.SIZE_INT + message.getEncodeSize() +
         DataConstants.SIZE_INT + queueIDs.length * DataConstants.SIZE_LONG;
   }

   @Override
   public String toString()
   {
      return "PagedMessageImpl [queueIDs=" + Arrays.toString(queueIDs) +
         ", transactionID=" +
         transactionID +
         ", message=" +
         message +
         "]";
   }
}
