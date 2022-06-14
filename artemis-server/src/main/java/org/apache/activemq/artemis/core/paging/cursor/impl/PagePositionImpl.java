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
package org.apache.activemq.artemis.core.paging.cursor.impl;

import org.apache.activemq.artemis.core.paging.cursor.PagePosition;

public class PagePositionImpl implements PagePosition {

   private final long pageNr;

   /**
    * The index of the message on the page file.
    *
    * This can be used as -1 in cases where the message is irrelevant,
    * for instance when a cursor is storing the next message to be received
    * or when a page is marked as fully complete (as the ACKs are removed)
    */
   private final int messageNr;

   /**
    * ID used for storage
    */
   private long recordID = -1;

   /**
    * Optional size value that can be set to specify the peristent size of the message
    * for metrics tracking purposes
    */
   private long persistentSize;

   public PagePositionImpl(long pageNr, int messageNr) {
      this.pageNr = pageNr;
      this.messageNr = messageNr;
   }


   /**
    * @return the recordID
    */
   @Override
   public long getRecordID() {
      return recordID;
   }

   /**
    * @param recordID the recordID to set
    */
   @Override
   public void setRecordID(long recordID) {
      this.recordID = recordID;
   }

   /**
    * @return the pageNr
    */
   @Override
   public long getPageNr() {
      return pageNr;
   }

   /**
    * @return the messageNr
    */
   @Override
   public int getMessageNr() {
      return messageNr;
   }

   /**
    * @return the persistentSize
    */
   @Override
   public long getPersistentSize() {
      return persistentSize;
   }

   /**
    * @param persistentSize the persistentSize to set
    */
   @Override
   public void setPersistentSize(long persistentSize) {
      this.persistentSize = persistentSize;
   }

   @Override
   public int compareTo(PagePosition o) {
      if (pageNr > o.getPageNr()) {
         return 1;
      } else if (pageNr < o.getPageNr()) {
         return -1;
      } else return Long.compare(recordID, o.getRecordID());
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + messageNr;
      result = prime * result + (int) (pageNr ^ (pageNr >>> 32));
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      PagePositionImpl other = (PagePositionImpl) obj;
      if (messageNr != other.messageNr)
         return false;
      if (pageNr != other.pageNr)
         return false;
      return true;
   }

   @Override
   public String toString() {
      return "PagePositionImpl [pageNr=" + pageNr + ", messageNr=" + messageNr + ", recordID=" + recordID + "]";
   }

}
