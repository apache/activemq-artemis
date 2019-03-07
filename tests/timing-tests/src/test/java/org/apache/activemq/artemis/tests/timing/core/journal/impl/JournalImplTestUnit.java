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
package org.apache.activemq.artemis.tests.timing.core.journal.impl;

import java.util.ArrayList;

import org.apache.activemq.artemis.core.journal.PreparedTransactionInfo;
import org.apache.activemq.artemis.core.journal.RecordInfo;
import org.apache.activemq.artemis.nativo.jlibaio.LibaioContext;
import org.apache.activemq.artemis.tests.unit.UnitTestLogger;
import org.apache.activemq.artemis.tests.unit.core.journal.impl.JournalImplTestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public abstract class JournalImplTestUnit extends JournalImplTestBase {

   private static final UnitTestLogger log = UnitTestLogger.LOGGER;

   @Override
   @After
   public void tearDown() throws Exception {
      super.tearDown();

      Assert.assertEquals(0, LibaioContext.getTotalMaxIO());
   }

   @Test
   public void testAddUpdateDeleteManyLargeFileSize() throws Exception {
      final int numberAdds = 10000;

      final int numberUpdates = 5000;

      final int numberDeletes = 3000;

      long[] adds = new long[numberAdds];

      for (int i = 0; i < numberAdds; i++) {
         adds[i] = i;
      }

      long[] updates = new long[numberUpdates];

      for (int i = 0; i < numberUpdates; i++) {
         updates[i] = i;
      }

      long[] deletes = new long[numberDeletes];

      for (int i = 0; i < numberDeletes; i++) {
         deletes[i] = i;
      }

      setup(10, 10 * 1024 * 1024, true);
      createJournal();
      startJournal();
      load();
      add(adds);
      update(updates);
      delete(deletes);
      stopJournal();
      createJournal();
      startJournal();
      loadAndCheck();

   }

   @Test
   public void testAddUpdateDeleteManySmallFileSize() throws Exception {
      final int numberAdds = 10000;

      final int numberUpdates = 5000;

      final int numberDeletes = 3000;

      long[] adds = new long[numberAdds];

      for (int i = 0; i < numberAdds; i++) {
         adds[i] = i;
      }

      long[] updates = new long[numberUpdates];

      for (int i = 0; i < numberUpdates; i++) {
         updates[i] = i;
      }

      long[] deletes = new long[numberDeletes];

      for (int i = 0; i < numberDeletes; i++) {
         deletes[i] = i;
      }

      setup(10, 10 * 1024, true);
      createJournal();
      startJournal();
      load();
      add(adds);
      update(updates);
      delete(deletes);

      stopJournal(false);
      createJournal();
      startJournal();
      loadAndCheck();

   }

   @Test
   public void testReclaimAndReload() throws Exception {
      setup(2, 10 * 1024 * 1024, false);
      createJournal();
      startJournal();
      load();

      long start = System.currentTimeMillis();

      byte[] record = generateRecord(recordLength);

      int NUMBER_OF_RECORDS = 1000;

      for (int count = 0; count < NUMBER_OF_RECORDS; count++) {
         journal.appendAddRecord(count, (byte) 0, record, false);

         if (count >= NUMBER_OF_RECORDS / 2) {
            journal.appendDeleteRecord(count - NUMBER_OF_RECORDS / 2, false);
         }

         if (count % 100 == 0) {
            JournalImplTestUnit.log.debug("Done: " + count);
         }
      }

      long end = System.currentTimeMillis();

      double rate = 1000 * (double) NUMBER_OF_RECORDS / (end - start);

      JournalImplTestUnit.log.debug("Rate of " + rate + " adds/removes per sec");

      JournalImplTestUnit.log.debug("Reclaim status = " + debugJournal());

      stopJournal();
      createJournal();
      startJournal();
      journal.load(new ArrayList<RecordInfo>(), new ArrayList<PreparedTransactionInfo>(), null);

      Assert.assertEquals(NUMBER_OF_RECORDS / 2, journal.getIDMapSize());

      stopJournal();
   }

}
