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

import org.apache.activemq.artemis.core.journal.impl.JournalFile;

public interface TestableJournal extends Journal
{
   int getDataFilesCount();

   int getFreeFilesCount();

   int getOpenedFilesCount();

   int getIDMapSize();

   String debug() throws Exception;

   void debugWait() throws Exception;

   int getFileSize();

   int getMinFiles();

   String getFilePrefix();

   String getFileExtension();

   int getMaxAIO();

   void forceMoveNextFile() throws Exception;

   void setAutoReclaim(boolean autoReclaim);

   boolean isAutoReclaim();

   void testCompact();

   JournalFile getCurrentFile();

   /**
    * This method is called automatically when a new file is opened.
    * <p>
    * It will among other things, remove stale files and make them available for reuse.
    * <p>
    * This method locks the journal.
    * @return true if it needs to re-check due to cleanup or other factors
    */
   boolean checkReclaimStatus() throws Exception;

   JournalFile[] getDataFiles();
}
