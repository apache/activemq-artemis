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
package org.apache.activemq6.core.journal.impl;

import org.apache.activemq6.core.journal.RecordInfo;

/**
 * A JournalReaderCallbackAbstract
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class JournalReaderCallbackAbstract implements JournalReaderCallback
{

   public void markAsDataFile(final JournalFile file)
   {
   }

   public void onReadAddRecord(final RecordInfo info) throws Exception
   {
   }

   public void onReadAddRecordTX(final long transactionID, final RecordInfo recordInfo) throws Exception
   {
   }

   public void onReadCommitRecord(final long transactionID, final int numberOfRecords) throws Exception
   {
   }

   public void onReadDeleteRecord(final long recordID) throws Exception
   {
   }

   public void onReadDeleteRecordTX(final long transactionID, final RecordInfo recordInfo) throws Exception
   {
   }

   public void onReadPrepareRecord(final long transactionID, final byte[] extraData, final int numberOfRecords) throws Exception
   {
   }

   public void onReadRollbackRecord(final long transactionID) throws Exception
   {
   }

   public void onReadUpdateRecord(final RecordInfo recordInfo) throws Exception
   {
   }

   public void onReadUpdateRecordTX(final long transactionID, final RecordInfo recordInfo) throws Exception
   {
   }

}
