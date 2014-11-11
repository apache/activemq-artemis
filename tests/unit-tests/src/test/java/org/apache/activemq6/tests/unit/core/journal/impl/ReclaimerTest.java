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
package org.apache.activemq6.tests.unit.core.journal.impl;
import org.junit.Before;

import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;

import org.apache.activemq6.core.journal.SequentialFile;
import org.apache.activemq6.core.journal.impl.JournalFile;
import org.apache.activemq6.core.journal.impl.JournalImpl;
import org.apache.activemq6.core.journal.impl.Reclaimer;
import org.apache.activemq6.tests.util.UnitTestCase;

/**
 *
 * A ReclaimerTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ReclaimerTest extends UnitTestCase
{
   private JournalFile[] files;

   private Reclaimer reclaimer;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      reclaimer = new Reclaimer();
   }

   @Test
   public void testOneFilePosNegAll() throws Exception
   {
      setup(1);

      setupPosNeg(0, 10, 10);

      reclaimer.scan(files);

      assertCanDelete(0);
   }

   @Test
   public void testOneFilePosNegNotAll() throws Exception
   {
      setup(1);

      setupPosNeg(0, 10, 7);

      reclaimer.scan(files);

      assertCantDelete(0);
   }

   @Test
   public void testOneFilePosOnly() throws Exception
   {
      setup(1);

      setupPosNeg(0, 10);

      reclaimer.scan(files);

      assertCantDelete(0);
   }

   @Test
   public void testOneFileNegOnly() throws Exception
   {
      setup(1);

      setupPosNeg(0, 0, 10);

      reclaimer.scan(files);

      assertCanDelete(0);
   }

   @Test
   public void testTwoFilesPosNegAllDifferentFiles() throws Exception
   {
      setup(2);

      setupPosNeg(0, 10);
      setupPosNeg(1, 0, 10);

      reclaimer.scan(files);

      assertCanDelete(0);
      assertCanDelete(1);

   }

   @Test
   public void testTwoFilesPosNegAllSameFiles() throws Exception
   {
      setup(2);

      setupPosNeg(0, 10, 10);
      setupPosNeg(1, 10, 0, 10);

      reclaimer.scan(files);

      assertCanDelete(0);
      assertCanDelete(1);

   }

   @Test
   public void testTwoFilesPosNegMixedFiles() throws Exception
   {
      setup(2);

      setupPosNeg(0, 10, 7);
      setupPosNeg(1, 10, 3, 10);

      reclaimer.scan(files);

      assertCanDelete(0);
      assertCanDelete(1);
   }

   @Test
   public void testTwoFilesPosNegAllFirstFile() throws Exception
   {
      setup(2);

      setupPosNeg(0, 10, 10);
      setupPosNeg(1, 10);

      reclaimer.scan(files);

      assertCanDelete(0);
      assertCantDelete(1);
   }

   @Test
   public void testTwoFilesPosNegAllSecondFile() throws Exception
   {
      setup(2);

      setupPosNeg(0, 10);
      setupPosNeg(1, 10, 0, 10);

      reclaimer.scan(files);

      assertCantDelete(0);
      assertCanDelete(1);
   }

   @Test
   public void testTwoFilesPosOnly() throws Exception
   {
      setup(2);

      setupPosNeg(0, 10);
      setupPosNeg(1, 10);

      reclaimer.scan(files);

      assertCantDelete(0);
      assertCantDelete(1);
   }

   @Test
   public void testTwoFilesxyz() throws Exception
   {
      setup(2);

      setupPosNeg(0, 10);
      setupPosNeg(1, 10, 10);

      reclaimer.scan(files);

      assertCanDelete(0);
      assertCantDelete(1);
   }

   // Can-can-can

   @Test
   public void testThreeFiles1() throws Exception
   {
      setup(3);

      setupPosNeg(0, 10, 10, 0, 0);
      setupPosNeg(1, 10, 0, 10, 0);
      setupPosNeg(2, 10, 0, 0, 10);

      reclaimer.scan(files);

      assertCanDelete(0);
      assertCanDelete(1);
      assertCanDelete(2);
   }

   @Test
   public void testThreeFiles2() throws Exception
   {
      setup(3);

      setupPosNeg(0, 10, 7, 0, 0);
      setupPosNeg(1, 10, 3, 5, 0);
      setupPosNeg(2, 10, 0, 5, 10);

      reclaimer.scan(files);

      assertCanDelete(0);
      assertCanDelete(1);
      assertCanDelete(2);
   }

   @Test
   public void testThreeFiles3() throws Exception
   {
      setup(3);

      setupPosNeg(0, 10, 1, 0, 0);
      setupPosNeg(1, 10, 6, 5, 0);
      setupPosNeg(2, 10, 3, 5, 10);

      reclaimer.scan(files);

      assertCanDelete(0);
      assertCanDelete(1);
      assertCanDelete(2);
   }

   @Test
   public void testThreeFiles3_1() throws Exception
   {
      setup(3);

      setupPosNeg(0, 10, 1, 0, 0);
      setupPosNeg(1, 10, 6, 5, 0);
      setupPosNeg(2, 0, 3, 5, 0);

      reclaimer.scan(files);

      assertCanDelete(0);
      assertCanDelete(1);
      assertCanDelete(2);
   }

   @Test
   public void testThreeFiles3_2() throws Exception
   {
      setup(3);

      setupPosNeg(0, 10, 1, 0, 0);
      setupPosNeg(1, 0, 6, 0, 0);
      setupPosNeg(2, 0, 3, 0, 0);

      reclaimer.scan(files);

      assertCanDelete(0);
      assertCanDelete(1);
      assertCanDelete(2);
   }

   // Cant-can-can

   @Test
   public void testThreeFiles4() throws Exception
   {
      setup(3);

      setupPosNeg(0, 10, 3, 0, 0);
      setupPosNeg(1, 10, 0, 5, 0);
      setupPosNeg(2, 10, 0, 5, 10);

      reclaimer.scan(files);

      assertCantDelete(0);
      assertCanDelete(1);
      assertCanDelete(2);
   }

   @Test
   public void testThreeFiles5() throws Exception
   {
      setup(3);

      setupPosNeg(0, 10, 3, 0, 0);
      setupPosNeg(1, 10, 0, 5, 0);
      setupPosNeg(2, 0, 0, 5, 0);

      reclaimer.scan(files);

      assertCantDelete(0);
      assertCanDelete(1);
      assertCanDelete(2);
   }

   @Test
   public void testThreeFiles6() throws Exception
   {
      setup(3);

      setupPosNeg(0, 10, 0, 0, 0);
      setupPosNeg(1, 10, 0, 5, 0);
      setupPosNeg(2, 0, 0, 5, 10);

      reclaimer.scan(files);

      assertCantDelete(0);
      assertCanDelete(1);
      assertCanDelete(2);
   }

   @Test
   public void testThreeFiles7() throws Exception
   {
      setup(3);

      setupPosNeg(0, 10, 0, 0, 0);
      setupPosNeg(1, 10, 0, 5, 0);
      setupPosNeg(2, 0, 0, 5, 0);

      reclaimer.scan(files);

      assertCantDelete(0);
      assertCanDelete(1);
      assertCanDelete(2);
   }

   // Cant can cant

   @Test
   public void testThreeFiles8() throws Exception
   {
      setup(3);

      setupPosNeg(0, 10, 3, 0, 0);
      setupPosNeg(1, 10, 0, 10, 0);
      setupPosNeg(2, 10, 0, 0, 2);

      reclaimer.scan(files);

      assertCantDelete(0);
      assertCanDelete(1);
      assertCantDelete(2);
   }

   @Test
   public void testThreeFiles9() throws Exception
   {
      setup(3);

      setupPosNeg(0, 10, 3, 0, 0);
      setupPosNeg(1, 10, 0, 10, 0);
      setupPosNeg(2, 10, 1, 0, 2);

      reclaimer.scan(files);

      assertCantDelete(0);
      assertCanDelete(1);
      assertCantDelete(2);
   }

   @Test
   public void testThreeFiles10() throws Exception
   {
      setup(3);

      setupPosNeg(0, 10, 3, 0, 0);
      setupPosNeg(1, 10, 0, 10, 0);
      setupPosNeg(2, 10, 1, 0, 0);

      reclaimer.scan(files);

      assertCantDelete(0);
      assertCanDelete(1);
      assertCantDelete(2);
   }

   @Test
   public void testThreeFiles11() throws Exception
   {
      setup(3);

      setupPosNeg(0, 10, 0, 0, 0);
      setupPosNeg(1, 10, 0, 10, 0);
      setupPosNeg(2, 10, 0, 0, 0);

      reclaimer.scan(files);

      assertCantDelete(0);
      assertCanDelete(1);
      assertCantDelete(2);
   }

   @Test
   public void testThreeFiles12() throws Exception
   {
      setup(3);

      setupPosNeg(0, 10, 0, 0, 0);
      setupPosNeg(1, 10, 0, 10, 0);
      setupPosNeg(2, 0, 3, 0, 0);

      reclaimer.scan(files);

      assertCantDelete(0);
      assertCanDelete(1);
      assertCantDelete(2);
   }

   // Cant-cant-cant

   @Test
   public void testThreeFiles13() throws Exception
   {
      setup(3);

      setupPosNeg(0, 10, 3, 0, 0);
      setupPosNeg(1, 10, 2, 3, 0);
      setupPosNeg(2, 10, 1, 5, 7);

      reclaimer.scan(files);

      assertCantDelete(0);
      assertCantDelete(1);
      assertCantDelete(2);
   }

   @Test
   public void testThreeFiles14() throws Exception
   {
      setup(3);

      setupPosNeg(0, 10, 3, 0, 0);
      setupPosNeg(1, 0, 2, 0, 0);
      setupPosNeg(2, 10, 1, 0, 7);

      reclaimer.scan(files);

      assertCantDelete(0);
      assertCantDelete(1);
      assertCantDelete(2);
   }

   @Test
   public void testThreeFiles15() throws Exception
   {
      setup(3);

      setupPosNeg(0, 10, 3, 0, 0);
      setupPosNeg(1, 10, 2, 3, 0);
      setupPosNeg(2, 0, 1, 5, 0);

      reclaimer.scan(files);

      assertCantDelete(0);
      assertCantDelete(1);
      assertCantDelete(2);
   }

   @Test
   public void testThreeFiles16() throws Exception
   {
      setup(3);

      setupPosNeg(0, 10, 3, 0, 0);
      setupPosNeg(1, 0, 2, 0, 0);
      setupPosNeg(2, 0, 1, 0, 0);

      reclaimer.scan(files);

      assertCantDelete(0);
      assertCantDelete(1);
      assertCantDelete(2);
   }

   @Test
   public void testThreeFiles17() throws Exception
   {
      setup(3);

      setupPosNeg(0, 10, 3, 0, 0);
      setupPosNeg(1, 10, 0, 3, 0);
      setupPosNeg(2, 10, 1, 5, 7);

      reclaimer.scan(files);

      assertCantDelete(0);
      assertCantDelete(1);
      assertCantDelete(2);
   }

   @Test
   public void testThreeFiles18() throws Exception
   {
      setup(3);

      setupPosNeg(0, 10, 3, 0, 0);
      setupPosNeg(1, 10, 0, 3, 0);
      setupPosNeg(2, 10, 1, 0, 7);

      reclaimer.scan(files);

      assertCantDelete(0);
      assertCantDelete(1);
      assertCantDelete(2);
   }

   @Test
   public void testThreeFiles19() throws Exception
   {
      setup(3);

      setupPosNeg(0, 10, 3, 0, 0);
      setupPosNeg(1, 10, 0, 3, 0);
      setupPosNeg(2, 10, 1, 0, 0);

      reclaimer.scan(files);

      assertCantDelete(0);
      assertCantDelete(1);
      assertCantDelete(2);
   }

   @Test
   public void testThreeFiles20() throws Exception
   {
      setup(3);

      setupPosNeg(0, 10, 3, 0, 0);
      setupPosNeg(1, 10, 0, 0, 0);
      setupPosNeg(2, 10, 1, 0, 0);

      reclaimer.scan(files);

      assertCantDelete(0);
      assertCantDelete(1);
      assertCantDelete(2);
   }

   @Test
   public void testThreeFiles21() throws Exception
   {
      setup(3);

      setupPosNeg(0, 10, 0, 0, 0);
      setupPosNeg(1, 10, 0, 0, 0);
      setupPosNeg(2, 10, 0, 0, 0);

      reclaimer.scan(files);

      assertCantDelete(0);
      assertCantDelete(1);
      assertCantDelete(2);
   }

   // Can-can-cant

   @Test
   public void testThreeFiles22() throws Exception
   {
      setup(3);

      setupPosNeg(0, 10, 10, 0, 0);
      setupPosNeg(1, 10, 0, 10, 0);
      setupPosNeg(2, 10, 0, 0, 0);

      reclaimer.scan(files);

      assertCanDelete(0);
      assertCanDelete(1);
      assertCantDelete(2);
   }

   @Test
   public void testThreeFiles23() throws Exception
   {
      setup(3);

      setupPosNeg(0, 10, 10, 0, 0);
      setupPosNeg(1, 10, 0, 10, 0);
      setupPosNeg(2, 10, 3, 0, 0);

      reclaimer.scan(files);

      assertCanDelete(0);
      assertCanDelete(1);
      assertCantDelete(2);
   }

   @Test
   public void testThreeFiles24() throws Exception
   {
      setup(3);

      setupPosNeg(0, 10, 7, 0, 0);
      setupPosNeg(1, 10, 3, 10, 0);
      setupPosNeg(2, 10, 3, 0, 0);

      reclaimer.scan(files);

      assertCanDelete(0);
      assertCanDelete(1);
      assertCantDelete(2);
   }

   @Test
   public void testThreeFiles25() throws Exception
   {
      setup(3);

      setupPosNeg(0, 10, 7, 0, 0);
      setupPosNeg(1, 0, 3, 10, 0);
      setupPosNeg(2, 10, 3, 0, 0);

      reclaimer.scan(files);

      assertCanDelete(0);
      assertCanDelete(1);
      assertCantDelete(2);
   }

   @Test
   public void testThreeFiles26() throws Exception
   {
      setup(3);

      setupPosNeg(0, 10, 7, 0, 0);
      setupPosNeg(1, 0, 3, 10, 0);
      setupPosNeg(2, 10, 0, 0, 0);

      reclaimer.scan(files);

      assertCanDelete(0);
      assertCanDelete(1);
      assertCantDelete(2);
   }

   // Can-cant-cant

   @Test
   public void testThreeFiles27() throws Exception
   {
      setup(3);

      setupPosNeg(0, 10, 10, 0, 0);
      setupPosNeg(1, 10, 0, 0, 0);
      setupPosNeg(2, 10, 0, 0, 0);

      reclaimer.scan(files);

      assertCanDelete(0);
      assertCantDelete(1);
      assertCantDelete(2);
   }

   @Test
   public void testThreeFiles28() throws Exception
   {
      setup(3);

      setupPosNeg(0, 10, 10, 0, 0);
      setupPosNeg(1, 10, 0, 3, 0);
      setupPosNeg(2, 10, 0, 0, 5);

      reclaimer.scan(files);

      assertCanDelete(0);
      assertCantDelete(1);
      assertCantDelete(2);
   }

   @Test
   public void testThreeFiles29() throws Exception
   {
      setup(3);

      setupPosNeg(0, 10, 10, 0, 0);
      setupPosNeg(1, 10, 0, 3, 0);
      setupPosNeg(2, 10, 0, 6, 5);

      reclaimer.scan(files);

      assertCanDelete(0);
      assertCantDelete(1);
      assertCantDelete(2);
   }

   @Test
   public void testThreeFiles30() throws Exception
   {
      setup(3);

      setupPosNeg(0, 10, 10, 0, 0);
      setupPosNeg(1, 10, 0, 3, 0);
      setupPosNeg(2, 0, 0, 6, 0);

      reclaimer.scan(files);

      assertCanDelete(0);
      assertCantDelete(1);
      assertCantDelete(2);
   }

   // Private
   // ------------------------------------------------------------------------

   private void setup(final int numFiles)
   {
      files = new JournalFile[numFiles];

      for (int i = 0; i < numFiles; i++)
      {
         files[i] = new MockJournalFile();
      }
   }

   private void setupPosNeg(final int fileNumber, final int pos, final int... neg)
   {
      JournalFile file = files[fileNumber];

      int totalDep = file.getTotalNegativeToOthers();

      for (int i = 0; i < pos; i++)
      {
         file.incPosCount();
      }

      for (int i = 0; i < neg.length; i++)
      {
         JournalFile reclaimable2 = files[i];

         for (int j = 0; j < neg[i]; j++)
         {
            file.incNegCount(reclaimable2);
            totalDep++;
         }
      }

      assertEquals(totalDep, file.getTotalNegativeToOthers());
   }

   private void assertCanDelete(final int... fileNumber)
   {
      for (int num : fileNumber)
      {
         Assert.assertTrue(files[num].isCanReclaim());
      }
   }

   private void assertCantDelete(final int... fileNumber)
   {
      for (int num : fileNumber)
      {
         Assert.assertFalse(files[num].isCanReclaim());
      }
   }

   static final class MockJournalFile implements JournalFile
   {
      private final Set<Long> transactionIDs = new HashSet<Long>();

      private final Set<Long> transactionTerminationIDs = new HashSet<Long>();

      private final Set<Long> transactionPrepareIDs = new HashSet<Long>();

      private final Map<JournalFile, Integer> negCounts = new HashMap<JournalFile, Integer>();

      private int posCount;

      private boolean canDelete;

      private boolean needCleanup;

      private int totalDep;

      public void extendOffset(final int delta)
      {
      }

      public SequentialFile getFile()
      {
         return null;
      }

      public long getOffset()
      {
         return 0;
      }

      public long getFileID()
      {
         return 0;
      }

      public void setOffset(final long offset)
      {
      }

      public int getNegCount(final JournalFile file)
      {
         Integer count = negCounts.get(file);

         if (count != null)
         {
            return count.intValue();
         }
         else
         {
            return 0;
         }
      }

      public void incNegCount(final JournalFile file)
      {
         Integer count = negCounts.get(file);

         int c = count == null ? 1 : count.intValue() + 1;

         negCounts.put(file, c);

         totalDep++;
      }

      public int getPosCount()
      {
         return posCount;
      }

      public void incPosCount()
      {
         posCount++;
      }

      public void decPosCount()
      {
         posCount--;
      }

      public boolean isCanReclaim()
      {
         return canDelete;
      }

      public void setCanReclaim(final boolean canDelete)
      {
         this.canDelete = canDelete;
      }

      public void addTransactionID(final long id)
      {
         transactionIDs.add(id);
      }

      public void addTransactionPrepareID(final long id)
      {
         transactionPrepareIDs.add(id);
      }

      public void addTransactionTerminationID(final long id)
      {
         transactionTerminationIDs.add(id);
      }

      public boolean containsTransactionID(final long id)
      {
         return transactionIDs.contains(id);
      }

      public boolean containsTransactionPrepareID(final long id)
      {
         return transactionPrepareIDs.contains(id);
      }

      public boolean containsTransactionTerminationID(final long id)
      {
         return transactionTerminationIDs.contains(id);
      }

      public Set<Long> getTranactionTerminationIDs()
      {
         return transactionTerminationIDs;
      }

      public Set<Long> getTransactionPrepareIDs()
      {
         return transactionPrepareIDs;
      }

      public Set<Long> getTransactionsIDs()
      {
         return transactionIDs;
      }

      /* (non-Javadoc)
       * @see org.apache.activemq6.core.journal.impl.JournalFile#decPendingTransaction()
       */
      public void decPendingTransaction()
      {
      }

      /* (non-Javadoc)
       * @see org.apache.activemq6.core.journal.impl.JournalFile#getPendingTransactions()
       */
      public int getPendingTransactions()
      {
         return 0;
      }

      /* (non-Javadoc)
       * @see org.apache.activemq6.core.journal.impl.JournalFile#incPendingTransaction()
       */
      public void incPendingTransaction()
      {
      }

      /* (non-Javadoc)
       * @see org.apache.activemq6.core.journal.impl.JournalFile#getOrderingID()
       */
      public int getOrderingID()
      {
         return 0;
      }

      public void addSize(final int bytes)
      {
      }

      @Override
      public void decSize(final int bytes)
      {
      }

      /* (non-Javadoc)
       * @see org.apache.activemq6.core.journal.impl.JournalFile#getSize()
       */
      public int getLiveSize()
      {
         return 0;
      }

      /* (non-Javadoc)
       * @see org.apache.activemq6.core.journal.impl.JournalFile#isNeedCleanup()
       */
      public boolean isNeedCleanup()
      {
         return needCleanup;
      }

      /* (non-Javadoc)
       * @see org.apache.activemq6.core.journal.impl.JournalFile#resetNegCount(org.apache.activemq6.core.journal.impl.JournalFile)
       */
      public boolean resetNegCount(final JournalFile file)
      {
         return false;
      }

      /* (non-Javadoc)
       * @see org.apache.activemq6.core.journal.impl.JournalFile#setNeedCleanup(boolean)
       */
      public void setNeedCleanup(final boolean needCleanup)
      {
         this.needCleanup = needCleanup;

      }

      /* (non-Javadoc)
       * @see org.apache.activemq6.core.journal.impl.JournalFile#getRecordID()
       */
      public int getRecordID()
      {
         return 0;
      }

      /* (non-Javadoc)
       * @see org.apache.activemq6.core.journal.impl.JournalFile#getTotalNegativeToOthers()
       */
      public int getTotalNegativeToOthers()
      {
         return totalDep;
      }

      /* (non-Javadoc)
       * @see org.apache.activemq6.core.journal.impl.JournalFile#getJournalVersion()
       */
      public int getJournalVersion()
      {
         return JournalImpl.FORMAT_VERSION;
      }

      /* (non-Javadoc)
       * @see org.apache.activemq6.core.journal.impl.JournalFile#getTotNeg()
       */
      public int getTotNeg()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      /* (non-Javadoc)
       * @see org.apache.activemq6.core.journal.impl.JournalFile#setTotNeg(int)
       */
      public void setTotNeg(int totNeg)
      {
         // TODO Auto-generated method stub

      }
   }
}
