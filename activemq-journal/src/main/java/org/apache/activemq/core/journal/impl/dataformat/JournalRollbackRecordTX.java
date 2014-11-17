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
package org.apache.activemq.core.journal.impl.dataformat;

import org.apache.activemq.api.core.HornetQBuffer;
import org.apache.activemq.core.journal.impl.JournalImpl;

/**
 * A JournalRollbackRecordTX
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class JournalRollbackRecordTX extends JournalInternalRecord
{
   private final long txID;

   public JournalRollbackRecordTX(final long txID)
   {
      this.txID = txID;
   }

   @Override
   public void encode(final HornetQBuffer buffer)
   {
      buffer.writeByte(JournalImpl.ROLLBACK_RECORD);
      buffer.writeInt(fileID);
      buffer.writeByte(compactCount);
      buffer.writeLong(txID);
      buffer.writeInt(JournalImpl.SIZE_ROLLBACK_RECORD + 1);

   }

   @Override
   public int getEncodeSize()
   {
      return JournalImpl.SIZE_ROLLBACK_RECORD + 1;
   }
}
