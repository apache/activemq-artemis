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
package org.apache.activemq6.tests.performance.journal;
import java.io.File;

import org.apache.activemq6.core.journal.SequentialFileFactory;
import org.apache.activemq6.core.journal.impl.AIOSequentialFileFactory;
import org.apache.activemq6.tests.unit.UnitTestLogger;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 *
 * A RealJournalImplTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class RealJournalImplAIOTest extends JournalImplTestUnit
{
   private static final UnitTestLogger log = UnitTestLogger.LOGGER;

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
   }

   @Override
   protected SequentialFileFactory getFileFactory() throws Exception
   {
      File file = new File(getTestDir());

      RealJournalImplAIOTest.log.debug("deleting directory " + file);

      deleteDirectory(file);

      file.mkdir();

      return new AIOSequentialFileFactory(getTestDir());
   }

}
