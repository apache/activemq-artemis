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

package org.apache.activemq.artemis.core.journal.collections;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.core.persistence.CoreMessageObjectPools;
import org.apache.activemq.artemis.core.persistence.Persister;
import org.apache.activemq.artemis.utils.DataConstants;

public abstract class AbstractHashMapPersister<K, V> implements Persister<JournalHashMap.MapRecord<K, V>> {

   private final byte VERSION = 0;

   @Override
   public byte getID() {
      return 0;
   }

   @Override
   public final int getEncodeSize(JournalHashMap.MapRecord<K, V> record) {
      return DataConstants.SIZE_LONG + // recordID
             DataConstants.SIZE_BYTE + // Version
             DataConstants.SIZE_LONG + // collectionID
             getKeySize(record.key) +
             getValueSize(record.value);
   }

   protected abstract int getKeySize(K key);

   protected abstract void encodeKey(ActiveMQBuffer buffer, K key);

   protected abstract K decodeKey(ActiveMQBuffer buffer);

   protected abstract int getValueSize(V value);

   protected abstract void encodeValue(ActiveMQBuffer buffer, V value);

   protected abstract V decodeValue(ActiveMQBuffer buffer, K key);

   @Override
   public final void encode(ActiveMQBuffer buffer, JournalHashMap.MapRecord<K, V> record) {
      buffer.writeLong(record.id);
      buffer.writeByte(VERSION);
      buffer.writeLong(record.collectionID);
      encodeKey(buffer, record.key);
      encodeValue(buffer, record.value);
   }

   @Override
   public final JournalHashMap.MapRecord<K, V> decode(ActiveMQBuffer buffer,
                                                JournalHashMap.MapRecord<K, V> record,
                                                CoreMessageObjectPools pool) {
      long id = buffer.readLong();

      byte version = buffer.readByte();
      assert version == VERSION;

      long collectionID = buffer.readLong();
      K key = decodeKey(buffer);
      V value = decodeValue(buffer, key);

      JournalHashMap.MapRecord<K, V> mapRecord = new JournalHashMap.MapRecord<>(collectionID, id, key, value);
      return mapRecord;
   }
}
