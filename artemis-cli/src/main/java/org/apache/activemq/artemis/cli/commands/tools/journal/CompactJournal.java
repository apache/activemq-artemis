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
package org.apache.activemq.artemis.cli.commands.tools.journal;

import java.io.File;

import io.airlift.airline.Command;
import org.apache.activemq.artemis.cli.commands.ActionContext;
import org.apache.activemq.artemis.cli.commands.tools.LockAbstract;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.io.IOCriticalErrorListener;
import org.apache.activemq.artemis.core.io.nio.NIOSequentialFileFactory;
import org.apache.activemq.artemis.core.journal.impl.JournalImpl;

@Command(name = "compact", description = "Compacts the journal of a non running server")
public final class CompactJournal extends LockAbstract {

   @Override
   public Object execute(ActionContext context) throws Exception {
      super.execute(context);
      try {
         Configuration configuration = getFileConfiguration();
         compactJournal(new File(getJournal()), "activemq-data", "amq", configuration.getJournalMinFiles(), configuration.getJournalFileSize(), null);
         System.out.println("Compactation succeeded for " + getJournal());
         compactJournal(new File(getBinding()), "activemq-bindings", "bindings", 2, 1048576, null);
         System.out.println("Compactation succeeded for " + getBinding());

      } catch (Exception e) {
         treatError(e, "data", "compact");
      }
      return null;
   }

   private void compactJournal(final File directory,
                               final String journalPrefix,
                               final String journalSuffix,
                               final int minFiles,
                               final int fileSize,
                               final IOCriticalErrorListener listener) throws Exception {
      NIOSequentialFileFactory nio = new NIOSequentialFileFactory(directory, listener, 1);

      JournalImpl journal = new JournalImpl(fileSize, minFiles, minFiles, 0, 0, nio, journalPrefix, journalSuffix, 1);

      journal.start();

      journal.loadInternalOnly();

      journal.compact();

      journal.stop();
   }
}
