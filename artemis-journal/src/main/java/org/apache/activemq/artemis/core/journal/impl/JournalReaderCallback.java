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
package org.apache.activemq.artemis.core.journal.impl;

import org.apache.activemq.artemis.core.journal.RecordInfo;

public interface JournalReaderCallback {

   void onReadAddRecord(RecordInfo info) throws Exception;

   /**
    * @param recordInfo
    * @throws Exception
    */
   void onReadUpdateRecord(RecordInfo recordInfo) throws Exception;

   /**
    * @param recordID
    */
   void onReadDeleteRecord(long recordID) throws Exception;

   /**
    * @param transactionID
    * @param recordInfo
    * @throws Exception
    */
   void onReadAddRecordTX(long transactionID, RecordInfo recordInfo) throws Exception;

   /**
    * @param transactionID
    * @param recordInfo
    * @throws Exception
    */
   void onReadUpdateRecordTX(long transactionID, RecordInfo recordInfo) throws Exception;

   /**
    * @param transactionID
    * @param recordInfo
    */
   void onReadDeleteRecordTX(long transactionID, RecordInfo recordInfo) throws Exception;

   /**
    * @param transactionID
    * @param extraData
    * @param numberOfRecords
    */
   void onReadPrepareRecord(long transactionID, byte[] extraData, int numberOfRecords) throws Exception;

   /**
    * @param transactionID
    * @param numberOfRecords
    */
   void onReadCommitRecord(long transactionID, int numberOfRecords) throws Exception;

   /**
    * @param transactionID
    */
   void onReadRollbackRecord(long transactionID) throws Exception;

   void markAsDataFile(JournalFile file);

}
