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

import java.util.ArrayList;
import java.util.List;

import org.apache.activemq6.api.core.Pair;

/**
 * This holds the relationship a record has with other files in regard to reference counting.
 * Note: This class used to be called PosFiles
 *
 * Used on the ref-count for reclaiming
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * */
public class JournalRecord
{
   private final JournalFile addFile;

   private final int size;

   private List<Pair<JournalFile, Integer>> updateFiles;

   public JournalRecord(final JournalFile addFile, final int size)
   {
      this.addFile = addFile;

      this.size = size;

      addFile.incPosCount();

      addFile.addSize(size);
   }

   void addUpdateFile(final JournalFile updateFile, final int size)
   {
      if (updateFiles == null)
      {
         updateFiles = new ArrayList<Pair<JournalFile, Integer>>();
      }

      updateFiles.add(new Pair<JournalFile, Integer>(updateFile, size));

      updateFile.incPosCount();

      updateFile.addSize(size);
   }

   void delete(final JournalFile file)
   {
      file.incNegCount(addFile);
      addFile.decSize(size);

      if (updateFiles != null)
      {
         for (Pair<JournalFile, Integer> updFile : updateFiles)
         {
            file.incNegCount(updFile.getA());
            updFile.getA().decSize(updFile.getB());
         }
      }
   }

   @Override
   public String toString()
   {
      StringBuilder buffer = new StringBuilder();
      buffer.append("JournalRecord(add=" + addFile.getFile().getFileName());

      if (updateFiles != null)
      {

         for (Pair<JournalFile, Integer> update : updateFiles)
         {
            buffer.append(", update=" + update.getA().getFile().getFileName());
         }

      }

      buffer.append(")");

      return buffer.toString();
   }
}
