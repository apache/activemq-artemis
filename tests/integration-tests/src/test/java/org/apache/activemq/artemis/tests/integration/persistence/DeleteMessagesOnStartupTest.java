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
package org.apache.activemq.artemis.tests.integration.persistence;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.StoreConfiguration;
import org.apache.activemq.artemis.core.persistence.AddressBindingInfo;
import org.apache.activemq.artemis.core.persistence.GroupingInfo;
import org.apache.activemq.artemis.core.persistence.QueueBindingInfo;
import org.apache.activemq.artemis.core.persistence.impl.journal.JournalStorageManager;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.ServerMessage;
import org.apache.activemq.artemis.core.server.impl.PostOfficeJournalLoader;
import org.apache.activemq.artemis.core.server.impl.ServerMessageImpl;
import org.apache.activemq.artemis.tests.unit.core.postoffice.impl.FakeQueue;
import org.apache.activemq.artemis.tests.unit.core.server.impl.fakes.FakePostOffice;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Parameterized;

public class DeleteMessagesOnStartupTest extends StorageManagerTestBase {

   volatile boolean deleteMessages = false;

   ArrayList<Long> deletedMessage = new ArrayList<>();

   public DeleteMessagesOnStartupTest(StoreConfiguration.StoreType storeType) {
      super(storeType);
   }

   // This is only applicable for FILE based store, as the database storage manager will automatically delete records.
   @Parameterized.Parameters(name = "storeType")
   public static Collection<Object[]> data() {
      Object[][] params = new Object[][]{{StoreConfiguration.StoreType.FILE}};
      return Arrays.asList(params);
   }

   @Test
   public void testDeleteMessagesOnStartup() throws Exception {
      createStorage();

      Queue theQueue = new FakeQueue(new SimpleString(""));
      HashMap<Long, Queue> queues = new HashMap<>();
      queues.put(100L, theQueue);

      ServerMessage msg = new ServerMessageImpl(1, 100);

      journal.storeMessage(msg);

      for (int i = 2; i < 100; i++) {
         journal.storeMessage(new ServerMessageImpl(i, 100));
      }

      journal.storeReference(100, 1, true);

      journal.stop();

      journal.start();

      journal.loadBindingJournal(new ArrayList<QueueBindingInfo>(), new ArrayList<GroupingInfo>(), new ArrayList<AddressBindingInfo>());

      FakePostOffice postOffice = new FakePostOffice();

      journal.loadMessageJournal(postOffice, null, null, null, null, null, null, new PostOfficeJournalLoader(postOffice, null, journal, null, null, null, null, null, queues));

      Assert.assertEquals(98, deletedMessage.size());

      for (Long messageID : deletedMessage) {
         Assert.assertTrue("messageID = " + messageID, messageID.longValue() >= 2 && messageID <= 99);
      }
   }

   @Override
   protected JournalStorageManager createJournalStorageManager(Configuration configuration) {
      return new JournalStorageManager(configuration, execFactory, execFactory) {
         @Override
         public void deleteMessage(final long messageID) throws Exception {
            deletedMessage.add(messageID);
            super.deleteMessage(messageID);
         }
      };
   }
}
