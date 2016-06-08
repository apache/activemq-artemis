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

import org.jboss.logging.Logger;

/**
 * <p>The journal consists of an ordered list of journal files Fn where {@code 0 <= n <= N}</p>
 *
 * <p>A journal file can contain either positives (pos) or negatives (neg)</p>
 *
 * <p>(Positives correspond either to adds or updates, and negatives correspond to deletes).</p>
 *
 * <p>A file Fn can be deleted if, and only if the following criteria are satisfied</p>
 *
 * <p>1) All pos in a file Fn, must have corresponding neg in any file Fm where {@code m >= n}.</p>
 *
 * <p>2) All pos that correspond to any neg in file Fn, must all live in any file Fm where {@code 0 <= m <= n}
 * which are also marked for deletion in the same pass of the algorithm.</p>
 */
public class Reclaimer {

   private static final Logger logger = Logger.getLogger(Reclaimer.class);

   public void scan(final JournalFile[] files) {
      for (int i = 0; i < files.length; i++) {
         JournalFile currentFile = files[i];
         currentFile.setCanReclaim(true);

         // First we evaluate criterion 2)
         for (int j = i - 1; j >= 0; j--) {
            JournalFile file = files[j];
            if (currentFile.getNegCount(file) != 0 && !file.isCanReclaim()) {
               logger.tracef("%s can't be reclaimed because %s has negative values", currentFile, file);
               currentFile.setCanReclaim(false);
               break;
            }
         }
         if (!currentFile.isCanReclaim()) {
            continue; // Move to next file as we already know that this file can't be reclaimed because criterion 2)
         }

         // Now we evaluate criterion 1)
         int negCount = 0, posCount = currentFile.getPosCount();
         logger.tracef("posCount on %s = %d", currentFile, posCount);

         for (int j = i; j < files.length && negCount < posCount; j++) {
            int toNeg = files[j].getNegCount(currentFile);
            negCount += toNeg;

            if (logger.isTraceEnabled() && toNeg != 0) {
               logger.tracef("Negative from %s into %s = %d", files[j], currentFile, toNeg);
            }
         }

         if (negCount < posCount ) {
            logger.tracef("%s can't be reclaimed because there are not enough negatives %d", currentFile, negCount);
            currentFile.setCanReclaim(false);
         }
         else {
            logger.tracef("%s can be reclaimed", currentFile);
         }
      }
   }
}
