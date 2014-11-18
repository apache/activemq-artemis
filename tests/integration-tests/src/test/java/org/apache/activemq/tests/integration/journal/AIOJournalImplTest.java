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
package org.apache.activemq.tests.integration.journal;
import java.io.File;

import org.apache.activemq.core.asyncio.impl.AsynchronousFileImpl;
import org.apache.activemq.core.journal.SequentialFileFactory;
import org.apache.activemq.core.journal.impl.AIOSequentialFileFactory;
import org.apache.activemq.core.journal.impl.JournalConstants;
import org.apache.activemq.tests.unit.core.journal.impl.JournalImplTestUnit;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 *
 * A RealJournalImplTest
 * you need to define -Djava.library.path=${project-root}/native/src/.libs when calling the JVM
 * If you are running this test in eclipse you should do:
 *   I - Run->Open Run Dialog
 *   II - Find the class on the list (you will find it if you already tried running this testcase before)
 *   III - Add -Djava.library.path=<your project place>/native/src/.libs
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class AIOJournalImplTest extends JournalImplTestUnit
{
   @BeforeClass
   public static void hasAIO()
   {
      org.junit.Assume.assumeTrue("Test case needs AIO to run", AIOSequentialFileFactory.isSupported());
   }

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      if (!AsynchronousFileImpl.isLoaded())
      {
         Assert.fail(String.format("libAIO is not loaded on %s %s %s",
                                   System.getProperty("os.name"),
                                   System.getProperty("os.arch"),
                                   System.getProperty("os.version")));
      }
   }

   @Override
   protected SequentialFileFactory getFileFactory() throws Exception
   {
      File file = new File(getTestDir());

      deleteDirectory(file);

      file.mkdir();

      return new AIOSequentialFileFactory(getTestDir(),
            JournalConstants.DEFAULT_JOURNAL_BUFFER_SIZE_AIO, 1000000,
            false);
   }

   @Override
   protected int getAlignment()
   {
      return 512;
   }

}
