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

package org.apache.activemq.artemis.core.settings.impl;

import java.io.Serializable;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.journal.EncodingSupport;
import org.apache.activemq.artemis.utils.BufferHelper;

public class ResourceLimitSettings implements Serializable, EncodingSupport {

   private static final long serialVersionUID = -110638321333856932L;

   public static final SimpleString DEFAULT_MATCH = null;

   public static final Integer DEFAULT_MAX_CONNECTIONS = -1;

   public static final Integer DEFAULT_MAX_QUEUES = -1;

   //   public static final Long DEFAULT_MAX_QUEUE_SIZE_BYTES = -1L;

   //   public static final SimpleString DEFAULT_QUEUE_NAME_REGEX = new SimpleString(".+");

   SimpleString match = null;

   Integer maxConnections = null;

   Integer maxQueues = null;

   //   Long maxQueueSizeBytes = null;

   //   SimpleString queueNameRegex = null;

   public SimpleString getMatch() {
      return match != null ? match : DEFAULT_MATCH;
   }

   public int getMaxConnections() {
      return maxConnections != null ? maxConnections : DEFAULT_MAX_CONNECTIONS;
   }

   public int getMaxQueues() {
      return maxQueues != null ? maxQueues : DEFAULT_MAX_QUEUES;
   }

   //   public long getMaxQueueSizeBytes()
   //   {
   //      return maxQueueSizeBytes != null ? maxQueueSizeBytes : DEFAULT_MAX_QUEUE_SIZE_BYTES;
   //   }
   //
   //   public SimpleString getQueueNameRegex()
   //   {
   //      return queueNameRegex != null ? queueNameRegex : DEFAULT_QUEUE_NAME_REGEX;
   //   }

   public void setMatch(SimpleString match) {
      this.match = match;
   }

   public void setMaxConnections(int maxConnections) {
      this.maxConnections = maxConnections;
   }

   public void setMaxQueues(int maxQueues) {
      this.maxQueues = maxQueues;
   }

   //   public void setMaxQueueSizeBytes(long maxQueueSizeBytes)
   //   {
   //      this.maxQueueSizeBytes = maxQueueSizeBytes;
   //   }
   //
   //   public void setQueueNameRegex(SimpleString queueNameRegex)
   //   {
   //      this.queueNameRegex = queueNameRegex;
   //   }

   @Override
   public int getEncodeSize() {
      return SimpleString.sizeofNullableString(match) +
         BufferHelper.sizeOfNullableInteger(maxConnections) +
         BufferHelper.sizeOfNullableInteger(maxQueues);
      //              BufferHelper.sizeOfNullableLong(maxQueueSizeBytes) +
      //              SimpleString.sizeofNullableString(queueNameRegex);
   }

   @Override
   public void encode(ActiveMQBuffer buffer) {
      buffer.writeNullableSimpleString(match);

      BufferHelper.writeNullableInteger(buffer, maxConnections);

      BufferHelper.writeNullableInteger(buffer, maxQueues);

      //      BufferHelper.writeNullableLong(buffer, maxQueueSizeBytes);

      //      buffer.writeNullableSimpleString(queueNameRegex);
   }

   @Override
   public void decode(ActiveMQBuffer buffer) {
      match = buffer.readNullableSimpleString();

      maxConnections = BufferHelper.readNullableInteger(buffer);

      maxQueues = BufferHelper.readNullableInteger(buffer);

      //      maxQueueSizeBytes = BufferHelper.readNullableLong(buffer);

      //      queueNameRegex = buffer.readNullableSimpleString();
   }

   /* (non-Javadoc)
    * @see java.lang.Object#hashCode()
    */
   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((match == null) ? 0 : match.hashCode());
      result = prime * result + ((maxConnections == null) ? 0 : maxConnections.hashCode());
      result = prime * result + ((maxQueues == null) ? 0 : maxQueues.hashCode());
      //      result = prime * result + ((maxQueueSizeBytes == null) ? 0 : maxQueueSizeBytes.hashCode());
      //      result = prime * result + ((queueNameRegex == null) ? 0 : queueNameRegex.hashCode());
      return result;
   }

   /* (non-Javadoc)
    * @see java.lang.Object#toString()
    */
   @Override
   public String toString() {
      return "ResourceLimitSettings [match=" + match +
         ", maxConnections=" +
         maxConnections +
         ", maxQueues=" +
         maxQueues +
         //              ", maxQueueSizeBytes=" +
         //              maxQueueSizeBytes +
         //              ", queueNameRegex=" +
         //              queueNameRegex +
         "]";
   }
}
