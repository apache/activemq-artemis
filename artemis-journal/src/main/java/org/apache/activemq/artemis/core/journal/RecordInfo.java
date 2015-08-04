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
package org.apache.activemq.artemis.core.journal;

public class RecordInfo {

   public RecordInfo(final long id,
                     final byte userRecordType,
                     final byte[] data,
                     final boolean isUpdate,
                     final short compactCount) {
      this.id = id;

      this.userRecordType = userRecordType;

      this.data = data;

      this.isUpdate = isUpdate;

      this.compactCount = compactCount;
   }

   /**
    * How many times this record was compacted (up to 7 times)
    * After the record has reached 7 times, it will always be 7
    * As we only store up to 0x7 binary, as part of the recordID (binary 111)
    */
   public final short compactCount;

   public final long id;

   public final byte userRecordType;

   public final byte[] data;

   public boolean isUpdate;

   public byte getUserRecordType() {
      return userRecordType;
   }

   @Override
   public int hashCode() {
      return (int) (id >>> 32 ^ id);
   }

   @Override
   public boolean equals(final Object other) {
      if (!(other instanceof RecordInfo)) {
         return false;
      }
      RecordInfo r = (RecordInfo) other;

      return r.id == id;
   }

   @Override
   public String toString() {
      return "RecordInfo (id=" + id +
         ", userRecordType = " +
         userRecordType +
         ", data.length = " +
         data.length +
         ", isUpdate = " +
         isUpdate;
   }

}
