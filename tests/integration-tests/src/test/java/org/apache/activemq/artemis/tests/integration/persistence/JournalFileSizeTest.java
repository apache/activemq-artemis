/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.persistence;


import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.persistence.impl.journal.JournalStorageManager;
import org.apache.activemq.artemis.utils.OrderedExecutorFactory;
import org.junit.Assert;
import org.junit.Test;

public class JournalFileSizeTest {

   @Test
   public void testIncorrectFileSizeLower() {
      ConfigurationImpl config = new ConfigurationImpl();
      int origFileSize = config.getJournalFileSize();
      config.setJournalFileSize(origFileSize + (512 / 2 - 1));
      JournalStorageManager manager = new JournalStorageManager(config,
            new OrderedExecutorFactory(null),
            new OrderedExecutorFactory(null));
      int fileSize = manager.getMessageJournal().getFileSize();
      Assert.assertEquals(origFileSize, fileSize);
   }

   @Test
   public void testIncorrectFileSizeHigher() {
      ConfigurationImpl config = new ConfigurationImpl();
      int origFileSize = config.getJournalFileSize();
      config.setJournalFileSize(origFileSize + (512 / 2 + 1));
      JournalStorageManager manager = new JournalStorageManager(config,
            new OrderedExecutorFactory(null),
            new OrderedExecutorFactory(null));
      int fileSize = manager.getMessageJournal().getFileSize();
      Assert.assertEquals(origFileSize + 512, fileSize);
   }

   @Test
   public void testIncorrectFileSizeHalf() {
      ConfigurationImpl config = new ConfigurationImpl();
      int origFileSize = config.getJournalFileSize();
      config.setJournalFileSize(origFileSize + (512 / 2));
      JournalStorageManager manager = new JournalStorageManager(config,
            new OrderedExecutorFactory(null),
            new OrderedExecutorFactory(null));
      int fileSize = manager.getMessageJournal().getFileSize();
      Assert.assertEquals(origFileSize + 512, fileSize);
   }
}
