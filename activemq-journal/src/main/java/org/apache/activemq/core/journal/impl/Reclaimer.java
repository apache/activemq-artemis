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
package org.apache.activemq.core.journal.impl;


import org.apache.activemq.journal.ActiveMQJournalLogger;

/**
 *
 * <p>The journal consists of an ordered list of journal files Fn where 0 <= n <= N</p>
 *
 * <p>A journal file can contain either positives (pos) or negatives (neg)</p>
 *
 * <p>(Positives correspond either to adds or updates, and negatives correspond to deletes).</p>
 *
 * <p>A file Fn can be deleted if, and only if the following criteria are satisified</p>
 *
 * <p>1) All pos in a file Fn, must have corresponding neg in any file Fm where m >= n.</p>
 *
 * <p>2) All pos that correspond to any neg in file Fn, must all live in any file Fm where 0 <= m <= n
 * which are also marked for deletion in the same pass of the algorithm.</p>
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class Reclaimer
{
   private static boolean trace = ActiveMQJournalLogger.LOGGER.isTraceEnabled();

   private static void trace(final String message)
   {
      ActiveMQJournalLogger.LOGGER.trace(message);
   }

   public void scan(final JournalFile[] files)
   {
      for (int i = 0; i < files.length; i++)
      {
         // First we evaluate criterion 1)

         JournalFile currentFile = files[i];

         int posCount = currentFile.getPosCount();

         int totNeg = 0;

         if (Reclaimer.trace)
         {
            Reclaimer.trace("posCount on " + currentFile + " = " + posCount);
         }

         for (int j = i; j < files.length; j++)
         {
            if (Reclaimer.trace)
            {
               if (files[j].getNegCount(currentFile) != 0)
               {
                  Reclaimer.trace("Negative from " + files[j] +
                                  " into " +
                                  currentFile +
                                  " = " +
                                  files[j].getNegCount(currentFile));
               }
            }

            totNeg += files[j].getNegCount(currentFile);
         }

         currentFile.setCanReclaim(true);

         if (posCount <= totNeg)
         {
            // Now we evaluate criterion 2)

            for (int j = 0; j <= i; j++)
            {
               JournalFile file = files[j];

               int negCount = currentFile.getNegCount(file);

               if (negCount != 0)
               {
                  if (file.isCanReclaim())
                  {
                     // Ok
                  }
                  else
                  {
                     if (Reclaimer.trace)
                     {
                        Reclaimer.trace(currentFile + " Can't be reclaimed because " + file + " has negative values");
                     }

                     currentFile.setCanReclaim(false);

                     break;
                  }
               }
            }
         }
         else
         {
            currentFile.setCanReclaim(false);
         }
      }
   }
}
