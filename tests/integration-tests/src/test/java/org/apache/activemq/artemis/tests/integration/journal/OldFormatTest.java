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
package org.apache.activemq.artemis.tests.integration.journal;

import java.nio.ByteBuffer;

import org.apache.activemq.artemis.core.journal.RecordInfo;
import org.apache.activemq.artemis.core.journal.SequentialFile;
import org.apache.activemq.artemis.core.journal.SequentialFileFactory;
import org.apache.activemq.artemis.core.journal.impl.JournalImpl;
import org.apache.activemq.artemis.core.journal.impl.NIOSequentialFileFactory;
import org.apache.activemq.artemis.tests.unit.core.journal.impl.JournalImplTestBase;
import org.apache.activemq.artemis.utils.DataConstants;
import org.junit.Test;

public class OldFormatTest extends JournalImplTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // This will generate records using the Version 1 format, and reading at the current version
   @Test
   public void testFormatOne() throws Exception
   {
      setup(2, 100 * 1024, true);

      SequentialFile file = fileFactory.createSequentialFile("amq-1.amq", 1);

      ByteBuffer buffer = ByteBuffer.allocateDirect(100 * 1024);

      initHeader(buffer, 1);

      byte[] record = new byte[1];

      for (long i = 0; i < 10; i++)
      {
         add(buffer, 1, i, record);

         update(buffer, 1, i, record);
      }

      file.open(1, false);

      buffer.rewind();

      file.writeDirect(buffer, true);

      file.close();

      createJournal();
      startJournal();
      loadAndCheck();

      startCompact();
      finishCompact();

      stopJournal();
      createJournal();
      startJournal();
      loadAndCheck();
   }

   private void add(ByteBuffer buffer, int fileID, long id, byte[] record)
   {
      int pos = buffer.position();

      buffer.put(JournalImpl.ADD_RECORD);

      buffer.putInt(fileID);

      buffer.putLong(id);

      buffer.putInt(record.length);

      buffer.put((byte) 0);

      buffer.put(record);

      buffer.putInt(buffer.position() - pos + DataConstants.SIZE_INT);

      records.add(new RecordInfo(id, (byte) 0, record, false, (short) 0));
   }

   private void update(ByteBuffer buffer, int fileID, long id, byte[] record)
   {
      int pos = buffer.position();

      buffer.put(JournalImpl.UPDATE_RECORD);

      buffer.putInt(fileID);

      buffer.putLong(id);

      buffer.putInt(record.length);

      buffer.put((byte) 0);

      buffer.put(record);

      buffer.putInt(buffer.position() - pos + DataConstants.SIZE_INT);

      records.add(new RecordInfo(id, (byte) 0, record, true, (short) 0));

   }

   /**
    * @param buffer
    */
   private void initHeader(ByteBuffer buffer, int fileID)
   {
      buffer.putInt(1);

      buffer.putInt(0);

      buffer.putLong(fileID);
   }

   /* (non-Javadoc)
    * @see JournalImplTestBase#getFileFactory()
    */
   @Override
   protected SequentialFileFactory getFileFactory() throws Exception
   {
      return new NIOSequentialFileFactory(getTestDir());
   }
}
