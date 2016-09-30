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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.core.journal.impl.dataformat.ByteArrayEncoding;
import org.apache.activemq.artemis.core.journal.impl.dataformat.JournalAddRecord;
import org.apache.activemq.artemis.core.journal.impl.dataformat.JournalInternalRecord;
import org.apache.activemq.artemis.utils.ConcurrentHashSet;

/**
 * Super class for Journal maintenances such as clean up and Compactor
 */
public abstract class AbstractJournalUpdateTask implements JournalReaderCallback {

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------
   protected static final String FILE_COMPACT_CONTROL = "journal-rename-control.ctr";

   protected final JournalImpl journal;

   protected final SequentialFileFactory fileFactory;

   protected JournalFile currentFile;

   protected SequentialFile sequentialFile;

   protected final JournalFilesRepository filesRepository;

   protected long nextOrderingID;

   private ActiveMQBuffer writingChannel;

   private final Set<Long> recordsSnapshot = new ConcurrentHashSet<>();

   protected final List<JournalFile> newDataFiles = new ArrayList<>();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   protected AbstractJournalUpdateTask(final SequentialFileFactory fileFactory,
                                       final JournalImpl journal,
                                       final JournalFilesRepository filesRepository,
                                       final Set<Long> recordsSnapshot,
                                       final long nextOrderingID) {
      super();
      this.journal = journal;
      this.filesRepository = filesRepository;
      this.fileFactory = fileFactory;
      this.nextOrderingID = nextOrderingID;
      this.recordsSnapshot.addAll(recordsSnapshot);
   }

   // Public --------------------------------------------------------

   public static SequentialFile writeControlFile(final SequentialFileFactory fileFactory,
                                                 final List<JournalFile> files,
                                                 final List<JournalFile> newFiles,
                                                 final List<Pair<String, String>> renames) throws Exception {

      SequentialFile controlFile = fileFactory.createSequentialFile(AbstractJournalUpdateTask.FILE_COMPACT_CONTROL);

      try {
         controlFile.open(1, false);

         JournalImpl.initFileHeader(fileFactory, controlFile, 0, 0);

         ActiveMQBuffer filesToRename = ActiveMQBuffers.dynamicBuffer(1);

         // DataFiles first

         if (files == null) {
            filesToRename.writeInt(0);
         } else {
            filesToRename.writeInt(files.size());

            for (JournalFile file : files) {
               filesToRename.writeUTF(file.getFile().getFileName());
            }
         }

         // New Files second

         if (newFiles == null) {
            filesToRename.writeInt(0);
         } else {
            filesToRename.writeInt(newFiles.size());

            for (JournalFile file : newFiles) {
               filesToRename.writeUTF(file.getFile().getFileName());
            }
         }

         // Renames from clean up third
         if (renames == null) {
            filesToRename.writeInt(0);
         } else {
            filesToRename.writeInt(renames.size());
            for (Pair<String, String> rename : renames) {
               filesToRename.writeUTF(rename.getA());
               filesToRename.writeUTF(rename.getB());
            }
         }

         JournalInternalRecord controlRecord = new JournalAddRecord(true, 1, (byte) 0, new ByteArrayEncoding(filesToRename.toByteBuffer().array()));

         ActiveMQBuffer renameBuffer = ActiveMQBuffers.dynamicBuffer(filesToRename.writerIndex());

         controlRecord.setFileID(0);

         controlRecord.encode(renameBuffer);

         ByteBuffer writeBuffer = fileFactory.newBuffer(renameBuffer.writerIndex());

         writeBuffer.put(renameBuffer.toByteBuffer().array(), 0, renameBuffer.writerIndex());

         writeBuffer.rewind();

         controlFile.writeDirect(writeBuffer, true);

         return controlFile;
      } finally {
         controlFile.close();
      }
   }

   /**
    * Write pending output into file
    */
   public void flush() throws Exception {
      if (writingChannel != null) {
         sequentialFile.position(0);

         // To Fix the size of the file
         writingChannel.writerIndex(writingChannel.capacity());

         sequentialFile.writeDirect(writingChannel.toByteBuffer(), true);
         sequentialFile.close();
         newDataFiles.add(currentFile);
      }

      writingChannel = null;
   }

   public boolean lookupRecord(final long id) {
      return recordsSnapshot.contains(id);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   /**
    * @throws Exception
    */

   protected void openFile() throws Exception {
      flush();

      ByteBuffer bufferWrite = fileFactory.newBuffer(journal.getFileSize());

      writingChannel = ActiveMQBuffers.wrappedBuffer(bufferWrite);

      currentFile = filesRepository.takeFile(false, false, false, true);

      sequentialFile = currentFile.getFile();

      sequentialFile.open(1, false);

      currentFile = new JournalFileImpl(sequentialFile, nextOrderingID++, JournalImpl.FORMAT_VERSION);

      JournalImpl.writeHeader(writingChannel, journal.getUserVersion(), currentFile.getFileID());
   }

   protected void addToRecordsSnaptshot(final long id) {
      recordsSnapshot.add(id);
   }

   /**
    * @return the writingChannel
    */
   protected ActiveMQBuffer getWritingChannel() {
      return writingChannel;
   }

   protected void writeEncoder(final JournalInternalRecord record) throws Exception {
      record.setFileID(currentFile.getRecordID());
      record.encode(getWritingChannel());
   }

   protected void writeEncoder(final JournalInternalRecord record, final int txcounter) throws Exception {
      record.setNumberOfRecords(txcounter);
      writeEncoder(record);
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
