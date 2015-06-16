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

import java.io.File;

import org.apache.activemq.artemis.core.journal.IOCriticalErrorListener;

/**
 * This is an undocumented class, that will open a journal and force compacting on it.
 * <p>
 * It may be used under special cases, but it shouldn't be needed under regular circumstances as the
 * system should detect the need for compacting. The regular use is to configure min-compact
 * parameters.
 */
public final class CompactJournal // NO_UCD
{

   public static void main(final String[] arg)
   {
      if (arg.length != 4)
      {
         System.err.println("Use: java -cp activemq-core.jar org.apache.activemq.artemis.core.journal.impl.CompactJournal <JournalDirectory> <JournalPrefix> <FileExtension> <FileSize>");
         return;
      }

      try
      {
         CompactJournal.compactJournal(new File(arg[0]), arg[1], arg[2], 2, Integer.parseInt(arg[3]), null);
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }

   }

   static void compactJournal(final File directory,
                                     final String journalPrefix,
                                     final String journalSuffix,
                                     final int minFiles,
                                     final int fileSize,
                                     final IOCriticalErrorListener listener) throws Exception
   {
      NIOSequentialFileFactory nio = new NIOSequentialFileFactory(directory, listener);

      JournalImpl journal = new JournalImpl(fileSize, minFiles, 0, 0, nio, journalPrefix, journalSuffix, 1);

      journal.start();

      journal.loadInternalOnly();

      journal.compact();

      journal.stop();
   }
}
