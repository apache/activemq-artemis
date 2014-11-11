/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.activemq6.core.paging.impl;

import java.util.Arrays;

import org.apache.activemq6.api.core.HornetQBuffer;
import org.apache.activemq6.api.core.HornetQBuffers;
import org.apache.activemq6.core.paging.PagedMessage;
import org.apache.activemq6.core.persistence.StorageManager;
import org.apache.activemq6.core.server.LargeServerMessage;
import org.apache.activemq6.core.server.ServerMessage;
import org.apache.activemq6.core.server.impl.ServerMessageImpl;
import org.apache.activemq6.utils.DataConstants;

/**
 * This class represents a paged message
 *
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 * @author <a href="mailto:andy.taylor@jboss.org>Andy Taylor</a>
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
         HornetQBuffer buffer = HornetQBuffers.dynamicBuffer(largeMessageLazyData);
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

   public void decode(final HornetQBuffer buffer)
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

   public void encode(final HornetQBuffer buffer)
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
