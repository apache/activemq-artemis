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
package org.apache.activemq.artemis.journal;

import org.apache.activemq.artemis.core.journal.impl.JournalFile;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * Logger Code 14
 *
 * each message id must be 6 digits long starting with 14, the 3rd digit donates the level so
 *
 * INF0  1
 * WARN  2
 * DEBUG 3
 * ERROR 4
 * TRACE 5
 * FATAL 6
 *
 * so an INFO message would be 141000 to 141999
 */
@MessageLogger(projectCode = "AMQ")
public interface ActiveMQJournalLogger extends BasicLogger {

   /**
    * The journal logger.
    */
   ActiveMQJournalLogger LOGGER = Logger.getMessageLogger(ActiveMQJournalLogger.class, ActiveMQJournalLogger.class.getPackage().getName());

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 141000, value = "*** running direct journal blast: {0}", format = Message.Format.MESSAGE_FORMAT)
   void runningJournalBlast(Integer numIts);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 141002, value = "starting thread for sync speed test", format = Message.Format.MESSAGE_FORMAT)
   void startingThread();

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 141003, value = "Write rate = {0} bytes / sec or {1} MiB / sec", format = Message.Format.MESSAGE_FORMAT)
   void writeRate(Double rate, Long l);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 141004, value = "Flush rate = {0} flushes / sec", format = Message.Format.MESSAGE_FORMAT)
   void flushRate(Double rate);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 141005, value = "Check Data Files:", format = Message.Format.MESSAGE_FORMAT)
   void checkFiles();

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 141006, value = "Sequence out of order on journal", format = Message.Format.MESSAGE_FORMAT)
   void seqOutOfOrder();

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 141007, value = "Current File on the journal is <= the sequence file.getFileID={0} on the dataFiles" +
      "\nCurrentfile.getFileId={1} while the file.getFileID()={2}" +
      "\nIs same = ({3})",
      format = Message.Format.MESSAGE_FORMAT)
   void currentFile(Long fileID, Long id, Long fileFileID, Boolean b);

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 141008, value = "Free File ID out of order", format = Message.Format.MESSAGE_FORMAT)
   void fileIdOutOfOrder();

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 141009, value = "A Free File is less than the maximum data", format = Message.Format.MESSAGE_FORMAT)
   void fileTooSmall();

   @LogMessage(level = Logger.Level.INFO)
   @Message(id = 141010, value = "Initialising JDBC data source {0} with properties {1}", format = Message.Format.MESSAGE_FORMAT)
   void initializingJdbcDataSource(String dataSourceClassName, String dataSourceProperties);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 142000, value = "You have a native library with a different version than expected", format = Message.Format.MESSAGE_FORMAT)
   void incompatibleNativeLibrary();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 142001, value = "Could not get lock after 60 seconds on closing Asynchronous File: {0}",
      format = Message.Format.MESSAGE_FORMAT)
   void couldNotGetLock(String fileName);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 142002, value = "Asynchronous File: {0} being finalized with opened state", format = Message.Format.MESSAGE_FORMAT)
   void fileFinalizedWhileOpen(String fileName);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 142003, value = "AIO Callback Error: {0}", format = Message.Format.MESSAGE_FORMAT)
   void callbackError(String error);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 142004, value = "Inconsistency during compacting: CommitRecord ID = {0} for an already committed transaction during compacting",
      format = Message.Format.MESSAGE_FORMAT)
   void inconsistencyDuringCompacting(Long transactionID);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 142005, value = "Inconsistency during compacting: Delete record being read on an existent record (id={0})",
      format = Message.Format.MESSAGE_FORMAT)
   void inconsistencyDuringCompactingDelete(Long recordID);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 142006, value = "Could not find add Record information for record {0} during compacting",
      format = Message.Format.MESSAGE_FORMAT)
   void compactingWithNoAddRecord(Long id);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 142007, value = "Can not find record {0} during compact replay",
      format = Message.Format.MESSAGE_FORMAT)
   void noRecordDuringCompactReplay(Long id);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 142008, value = "Could not remove file {0} from the list of data files",
      format = Message.Format.MESSAGE_FORMAT)
   void couldNotRemoveFile(JournalFile file);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 142009, value = "*******************************************************************************************************************************\nThe File Storage Attic is full, as the file {0}  does not have the configured size, and the file will be removed\n*******************************************************************************************************************************",
      format = Message.Format.MESSAGE_FORMAT)
   void deletingFile(JournalFile file);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 142010, value = "Failed to add file to opened files queue: {0}. This should NOT happen!",
      format = Message.Format.MESSAGE_FORMAT)
   void failedToAddFile(JournalFile nextOpenedFile);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 142011, value = "Error on reading compacting for {0}",
      format = Message.Format.MESSAGE_FORMAT)
   void compactReadError(JournalFile file);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 142012, value = "Couldn''t find tx={0} to merge after compacting",
      format = Message.Format.MESSAGE_FORMAT)
   void compactMergeError(Long id);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 142013, value = "Prepared transaction {0} was not considered completed, it will be ignored",
      format = Message.Format.MESSAGE_FORMAT)
   void preparedTXIncomplete(Long id);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 142014, value = "Transaction {0} is missing elements so the transaction is being ignored",
      format = Message.Format.MESSAGE_FORMAT)
   void txMissingElements(Long id);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 142015, value = "Uncommitted transaction with id {0} found and discarded",
      format = Message.Format.MESSAGE_FORMAT)
   void uncomittedTxFound(Long id);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 142016, value = "Could not stop compactor executor after 120 seconds",
      format = Message.Format.MESSAGE_FORMAT)
   void couldNotStopCompactor();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 142017, value = "Could not stop journal executor after 60 seconds",
      format = Message.Format.MESSAGE_FORMAT)
   void couldNotStopJournalExecutor();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 142018, value = "Temporary files were left unattended after a crash on journal directory, deleting invalid files now",
      format = Message.Format.MESSAGE_FORMAT)
   void tempFilesLeftOpen();

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 142019, value = "Deleting orphaned file {0}", format = Message.Format.MESSAGE_FORMAT)
   void deletingOrphanedFile(String fileToDelete);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 142020, value = "Could not get lock after 60 seconds on closing Asynchronous File: {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorClosingFile(String fileToDelete);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 142021, value = "Error on IO callback, {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorOnIOCallback(String errorMessage);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 142022, value = "Timed out on AIO poller shutdown", format = Message.Format.MESSAGE_FORMAT)
   void timeoutOnPollerShutdown(@Cause Exception e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 142023, value = "Executor on file {0} couldn''t complete its tasks in 60 seconds.", format = Message.Format.MESSAGE_FORMAT)
   void couldNotCompleteTask(@Cause Exception e, String name);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 142024, value = "Error completing callback", format = Message.Format.MESSAGE_FORMAT)
   void errorCompletingCallback(@Cause Throwable e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 142025, value = "Error calling onError callback", format = Message.Format.MESSAGE_FORMAT)
   void errorCallingErrorCallback(@Cause Throwable e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 142026, value = "Timed out on AIO writer shutdown", format = Message.Format.MESSAGE_FORMAT)
   void timeoutOnWriterShutdown(@Cause Throwable e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 142027, value = "Error on writing data! {0} code - {1}", format = Message.Format.MESSAGE_FORMAT)
   void errorWritingData(@Cause Throwable e, String errorMessage, Integer errorCode);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 142028, value = "Error replaying pending commands after compacting", format = Message.Format.MESSAGE_FORMAT)
   void errorReplayingCommands(@Cause Throwable e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 142029, value = "Error closing file", format = Message.Format.MESSAGE_FORMAT)
   void errorClosingFile(@Cause Throwable e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 142030, value = "Could not open a file in 60 Seconds", format = Message.Format.MESSAGE_FORMAT)
   void errorOpeningFile(@Cause Throwable e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 142031, value = "Error retrieving ID part of the file name {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorRetrievingID(@Cause Throwable e, String fileName);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 142032, value = "Error reading journal file", format = Message.Format.MESSAGE_FORMAT)
   void errorReadingFile(@Cause Throwable e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 142033, value = "Error reinitializing file {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorReinitializingFile(@Cause Throwable e, JournalFile file);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 142034, value = "Exception on submitting write", format = Message.Format.MESSAGE_FORMAT)
   void errorSubmittingWrite(@Cause Throwable e);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 142035, value = "Could not stop journal append executor after 60 seconds", format = Message.Format.MESSAGE_FORMAT)
   void couldNotStopJournalAppendExecutor();

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 144000, value = "Failed to delete file {0}", format = Message.Format.MESSAGE_FORMAT)
   void errorDeletingFile(Object e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 144001, value = "Error starting poller", format = Message.Format.MESSAGE_FORMAT)
   void errorStartingPoller(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 144002, value = "Error pushing opened file", format = Message.Format.MESSAGE_FORMAT)
   void errorPushingFile(@Cause Exception e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 144003, value = "Error compacting", format = Message.Format.MESSAGE_FORMAT)
   void errorCompacting(@Cause Throwable e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 144004, value = "Error scheduling compacting", format = Message.Format.MESSAGE_FORMAT)
   void errorSchedulingCompacting(@Cause Throwable e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 144005, value = "Failed to performance blast", format = Message.Format.MESSAGE_FORMAT)
   void failedToPerfBlast(@Cause Throwable e);

   @LogMessage(level = Logger.Level.ERROR)
   @Message(id = 144006, value = "IOError code {0}, {1}", format = Message.Format.MESSAGE_FORMAT)
   void ioError(int errorCode, String errorMessage);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 144007, value = "Ignoring journal file {0}: file is shorter then minimum header size. This file is being removed.", format = Message.Format.MESSAGE_FORMAT)
   void ignoringShortFile(String fileName);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 144008, value = "*******************************************************************************************************************************\nFile {0}: was moved under attic, please review it and remove it.\n*******************************************************************************************************************************", format = Message.Format.MESSAGE_FORMAT)
   void movingFileToAttic(String fileName);

   @LogMessage(level = Logger.Level.WARN)
   @Message(id = 144009, value = "Could not get a file in {0} seconds, System will retry the open but you may see increased latency in your system", format = Message.Format.MESSAGE_FORMAT)
   void cantOpenFileTimeout(long timeout);
}
