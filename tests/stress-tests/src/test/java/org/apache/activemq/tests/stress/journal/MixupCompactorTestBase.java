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
package org.apache.activemq.tests.stress.journal;

import java.io.File;
import java.io.FilenameFilter;

import org.apache.activemq.core.journal.SequentialFileFactory;
import org.apache.activemq.core.journal.impl.JournalImpl;
import org.apache.activemq.core.journal.impl.NIOSequentialFileFactory;
import org.apache.activemq.tests.unit.core.journal.impl.JournalImplTestBase;
import org.apache.activemq.utils.ReusableLatch;
import org.apache.activemq.utils.SimpleIDGenerator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * This class will control mix up compactor between each operation of a test
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public abstract class MixupCompactorTestBase extends JournalImplTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private ReusableLatch startedCompactingLatch = null;

   private ReusableLatch releaseCompactingLatch = null;

   private Thread tCompact = null;

   int startCompactAt;

   int joinCompactAt;

   int secondCompactAt;

   int currentOperation;

   SimpleIDGenerator idGen = new SimpleIDGenerator(1000);

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------
   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      tCompact = null;

      startedCompactingLatch = new ReusableLatch(1);

      releaseCompactingLatch = new ReusableLatch(1);

      setup(2, 60 * 1024, false);

   }

   @Override
   @After
   public void tearDown() throws Exception
   {

      File testDir = new File(getTestDir());

      File[] files = testDir.listFiles(new FilenameFilter()
      {

         public boolean accept(final File dir, final String name)
         {
            return name.startsWith(filePrefix) && name.endsWith(fileExtension);
         }
      });

      for (File file : files)
      {
         Assert.assertEquals("File " + file + " doesn't have the expected number of bytes", fileSize, file.length());
      }

      super.tearDown();
   }

   @Override
   public void createJournal() throws Exception
   {
      journal = new JournalImpl(fileSize, minFiles, 0, 0, fileFactory, filePrefix, fileExtension, maxAIO)
      {

         @Override
         public void onCompactDone()
         {
            startedCompactingLatch.countDown();
            try
            {
               releaseCompactingLatch.await();
            }
            catch (InterruptedException e)
            {
               e.printStackTrace();
            }
         }
      };

      journal.setAutoReclaim(false);
   }

   @Test
   public void testMixOperations() throws Exception
   {


      currentOperation = 0;
      internalTest();
      int MAX_OPERATIONS = testMix(-1, -1, -1);

      System.out.println("Using MAX_OPERATIONS = " + MAX_OPERATIONS);

      for (int startAt = 0; startAt < MAX_OPERATIONS; startAt++)
      {
         for (int joinAt = startAt; joinAt < MAX_OPERATIONS; joinAt++)
         {
            for (int secondAt = joinAt; secondAt < MAX_OPERATIONS; secondAt++)
            {
               System.out.println("start=" + startAt + ", join=" + joinAt + ", second=" + secondAt);

               try
               {
                  tearDown();
                  setUp();
                  testMix(startAt, joinAt, secondAt);
               }
               catch (Throwable e)
               {
                  throw new Exception("Error at compact=" + startCompactAt +
                                         ", joinCompactAt=" +
                                         joinCompactAt +
                                         ", secondCompactAt=" +
                                         secondCompactAt, e);
               }
            }
         }
      }
   }

   protected int testMix(final int startAt, final int joinAt, final int secondAt) throws Exception
   {
      startCompactAt = startAt;
      joinCompactAt = joinAt;
      secondCompactAt = secondAt;

      currentOperation = 0;

      internalTest();

      return currentOperation;
   }

   @Override
   protected void beforeJournalOperation() throws Exception
   {
      checkJournalOperation();
   }

   /**
    * @throws InterruptedException
    * @throws Exception
    */
   protected void checkJournalOperation() throws Exception
   {
      if (startCompactAt == currentOperation)
      {
         threadCompact();
      }
      if (joinCompactAt == currentOperation)
      {
         joinCompact();
      }
      if (secondCompactAt == currentOperation)
      {
         journal.testCompact();
      }

      currentOperation++;
   }

   protected abstract void internalTest() throws Exception;

   private void joinCompact() throws InterruptedException
   {
      releaseCompactingLatch.countDown();

      tCompact.join();

      tCompact = null;
   }

   private void threadCompact() throws InterruptedException
   {
      tCompact = new Thread()
      {
         @Override
         public void run()
         {
            try
            {
               journal.testCompact();
            }
            catch (Exception e)
            {
               e.printStackTrace();
            }
         }
      };

      tCompact.start();

      startedCompactingLatch.await();
   }

   @Override
   protected SequentialFileFactory getFileFactory() throws Exception
   {
      return new NIOSequentialFileFactory(getTestDir());
   }
}
