/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.core.persistence.impl.journal.codec;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.core.persistence.QueueStatus;
import org.apache.activemq.artemis.utils.DataConstants;

public class QueueStatusEncoding extends QueueEncoding {

   private QueueStatus status;

   private long id;

   public QueueStatusEncoding(long queueID, QueueStatus status) {
      super(queueID);
      this.status = status;
   }

   public QueueStatusEncoding() {
      super();
   }

   @Override
   public void decode(final ActiveMQBuffer buffer) {
      super.decode(buffer);
      short shortStatus = buffer.readShort();
      this.status = QueueStatus.fromID(shortStatus);
   }

   @Override
   public void encode(final ActiveMQBuffer buffer) {
      super.encode(buffer);
      buffer.writeShort(status.id);
   }

   public QueueStatus getStatus() {
      return status;
   }

   public long getId() {
      return id;
   }

   public QueueStatusEncoding setId(long id) {
      this.id = id;
      return this;
   }

   @Override
   public int getEncodeSize() {
      return super.getEncodeSize() + DataConstants.SIZE_SHORT;
   }

   @Override
   public String toString() {
      return "QueueStatusEncoding [id=" + id + ", queueID=" + queueID + ", status=" + status + "]";
   }

}
