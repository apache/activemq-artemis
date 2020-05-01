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
package org.apache.activemq.artemis.core.persistence.impl.journal;

import java.util.Objects;

import it.unimi.dsi.fastutil.longs.Long2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;

/**
 * Data structure used to hold an insertion-ordered map of {@link Message}s
 * with additional information on {@code scheduledDeliveryTime} and {@code deliveryCount}.
 * This is meant to be used by {@link org.apache.activemq.artemis.core.server.impl.JournalLoader}
 * while re-routing the loaded {@link Message}s into the {@link Queue} they belong.
 */
public final class MessageRecordOrderedMap {

   private static final class AddMessageRecord {

      private long scheduledDeliveryTime = 0;
      private int deliveryCount = 0;
   }

   private Long2ObjectMap<AddMessageRecord> records;
   private final Long2ObjectLinkedOpenHashMap<Message> messages;

   public MessageRecordOrderedMap() {
      records = null;
      messages = new Long2ObjectLinkedOpenHashMap<>();
   }

   public void addMessage(long messageID, Message message) {
      assert message.getMessageID() == messageID;
      messages.put(messageID, message);
   }

   public boolean removeMessage(long messageID) {
      if (messages.remove(messageID) != null) {
         if (records != null) {
            records.remove(messageID);
         }
         return true;
      }
      return false;
   }

   private AddMessageRecord getOrAddRecord(long messageID) {
      if (!messages.containsKey(messageID)) {
         return null;
      }
      Long2ObjectMap<AddMessageRecord> records = this.records;
      AddMessageRecord record = null;
      if (records == null) {
         records = new Long2ObjectOpenHashMap<>();
         this.records = records;
      } else {
         record = records.get(messageID);
      }
      if (record == null) {
         record = new AddMessageRecord();
         records.put(messageID, record);
      }
      return record;
   }

   public boolean setDeliveryCount(long messageID, int value) {
      final AddMessageRecord record = getOrAddRecord(messageID);
      if (record == null) {
         return false;
      }
      record.deliveryCount = value;
      return true;
   }

   public boolean setScheduledDeliveryTime(long messageID, long value) {
      final AddMessageRecord record = getOrAddRecord(messageID);
      if (record == null) {
         return false;
      }
      record.scheduledDeliveryTime = value;
      return true;
   }

   private static final Long ZERO = Long.valueOf(0);

   private static long updateMessageScheduledDeliveryTime(Message message,
                                                          AddMessageRecord record,
                                                          long currentTimeMillis) {
      if (record == null) {
         return 0;
      }
      final long scheduledDeliveryTime = record.scheduledDeliveryTime;
      if (scheduledDeliveryTime == 0) {
         return 0;
      }
      if (scheduledDeliveryTime <= currentTimeMillis) {
         message.setScheduledDeliveryTime(ZERO);
         return 0;
      }
      assert scheduledDeliveryTime != 0;
      message.setScheduledDeliveryTime(scheduledDeliveryTime);
      return scheduledDeliveryTime;
   }

   public void pauseReloadMessages(PostOffice postOffice, Queue queue, long currentTimeMillis) throws Exception {
      Objects.requireNonNull(postOffice, "postOffice");
      Objects.requireNonNull(queue, "queue");
      if (currentTimeMillis < 0) {
         throw new IllegalArgumentException("currentTimeMillis cannot be < 0");
      }
      final Long2ObjectLinkedOpenHashMap<Message> messages = this.messages;
      if (messages.isEmpty()) {
         return;
      }
      final Long2ObjectMap<AddMessageRecord> records = this.records;
      queue.pause();
      for (Long2ObjectMap.Entry<Message> entry : messages.long2ObjectEntrySet()) {
         // not using entry.getValue().getMessageID() to save a data dependency from message
         final long messageID = entry.getLongKey();
         final AddMessageRecord record = records == null ? null : records.get(messageID);
         final Message message = entry.getValue();
         final long scheduledDeliveryTime = updateMessageScheduledDeliveryTime(message, record, currentTimeMillis);
         final MessageReference ref = postOffice.reload(message, queue, null);
         final int deliveryCount = record == null ? 0 : record.deliveryCount;
         // setDeliveryCount is a CPU intensive ops, let's try to save it
         if (ref.getDeliveryCount() != deliveryCount) {
            ref.setDeliveryCount(deliveryCount);
         }
         if (scheduledDeliveryTime != 0) {
            message.setScheduledDeliveryTime(ZERO);
         }
      }
   }

   public boolean isEmpty() {
      return messages.isEmpty();
   }
}