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
package org.apache.activemq.artemis.core.journal.impl.dataformat;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.core.journal.EncodingSupport;
import org.apache.activemq.artemis.core.journal.impl.JournalImpl;

public class JournalAddRecord extends JournalInternalRecord {

   protected final long id;

   protected final EncodingSupport record;

   protected final byte recordType;

   protected final boolean add;

   /**
    * @param id
    * @param recordType
    * @param record
    */
   public JournalAddRecord(final boolean add, final long id, final byte recordType, final EncodingSupport record) {
      this.id = id;

      this.record = record;

      this.recordType = recordType;

      this.add = add;
   }

   @Override
   public void encode(final ActiveMQBuffer buffer) {
      if (add) {
         buffer.writeByte(JournalImpl.ADD_RECORD);
      } else {
         buffer.writeByte(JournalImpl.UPDATE_RECORD);
      }

      buffer.writeInt(fileID);

      buffer.writeByte(compactCount);

      buffer.writeLong(id);

      buffer.writeInt(record.getEncodeSize());

      buffer.writeByte(recordType);

      record.encode(buffer);

      buffer.writeInt(getEncodeSize());
   }

   @Override
   public int getEncodeSize() {
      return JournalImpl.SIZE_ADD_RECORD + record.getEncodeSize() + 1;
   }
}
