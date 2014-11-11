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
package org.apache.activemq6.tests.integration.journal;

import java.io.File;

import org.apache.activemq6.core.journal.SequentialFileFactory;
import org.apache.activemq6.core.journal.impl.NIOSequentialFileFactory;

/**
 * A NIOBufferedJournalCompactTest
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class NIOBufferedJournalCompactTest extends NIOJournalCompactTest
{

   @Override
   protected SequentialFileFactory getFileFactory() throws Exception
   {
      File file = new File(getTestDir());

      deleteDirectory(file);

      file.mkdir();

      return new NIOSequentialFileFactory(getTestDir(), true);
   }

}
