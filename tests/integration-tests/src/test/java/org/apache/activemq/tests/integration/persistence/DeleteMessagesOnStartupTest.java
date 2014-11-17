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
package org.apache.activemq.tests.integration.persistence;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.core.persistence.GroupingInfo;
import org.apache.activemq.core.persistence.QueueBindingInfo;
import org.apache.activemq.core.persistence.impl.journal.JournalStorageManager;
import org.apache.activemq.core.server.Queue;
import org.apache.activemq.core.server.ServerMessage;
import org.apache.activemq.core.server.impl.PostOfficeJournalLoader;
import org.apache.activemq.core.server.impl.ServerMessageImpl;
import org.apache.activemq.tests.unit.core.postoffice.impl.FakeQueue;
import org.apache.activemq.tests.unit.core.server.impl.fakes.FakePostOffice;
import org.junit.Test;

/**
 * A DeleteMessagesOnStartupTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class DeleteMessagesOnStartupTest extends StorageManagerTestBase
{

   volatile boolean deleteMessages = false;

   ArrayList<Long> deletedMessage = new ArrayList<Long>();

   @Test
   public void testDeleteMessagesOnStartup() throws Exception
   {
      createStorage();

      Queue theQueue = new FakeQueue(new SimpleString(""));
      HashMap<Long, Queue> queues = new HashMap<Long, Queue>();
      queues.put(100L, theQueue);

      ServerMessage msg = new ServerMessageImpl(1, 100);

      journal.storeMessage(msg);

      for (int i = 2; i < 100; i++)
      {
         journal.storeMessage(new ServerMessageImpl(i, 100));
      }

      journal.storeReference(100, 1, true);

      journal.stop();

      journal.start();

      journal.loadBindingJournal(new ArrayList<QueueBindingInfo>(), new ArrayList<GroupingInfo>());

      FakePostOffice postOffice = new FakePostOffice();

      journal.loadMessageJournal(postOffice, null, null, null, null, null, null, new PostOfficeJournalLoader(postOffice, null, journal, null, null, null, null, null, queues));

      assertEquals(98, deletedMessage.size());

      for (Long messageID : deletedMessage)
      {
         assertTrue("messageID = " + messageID, messageID.longValue() >= 2 && messageID <= 99);
      }
   }

   @Override
   protected JournalStorageManager createJournalStorageManager(Configuration configuration)
   {
      return new JournalStorageManager(configuration, execFactory, null)
      {
         @Override
         public void deleteMessage(final long messageID) throws Exception
         {
            deletedMessage.add(messageID);
            super.deleteMessage(messageID);
         }

      };
   }
}
