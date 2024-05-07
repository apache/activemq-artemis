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

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.persistence.impl.journal.JournalRecordIds;
import org.apache.activemq.artemis.core.postoffice.DuplicateIDCache;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.Assert;
import org.junit.Test;

public class ResizeDuplicateCacheTest extends ActiveMQTestBase {

   @Test
   public void testResizeCache() throws Exception {
      int duplicateSize = 30;
      SimpleString randomString = RandomUtil.randomSimpleString();

      ActiveMQServer server = createServer(true, false);
      server.start();

      DuplicateIDCache duplicateIDCache = server.getPostOffice().getDuplicateIDCache(randomString, duplicateSize);

      for (int i = 0; i < duplicateSize * 2; i++) {
         duplicateIDCache.addToCache(("a" + i).getBytes(StandardCharsets.UTF_8));
      }

      server.stop();
      server.start();

      duplicateIDCache = server.getPostOffice().getDuplicateIDCache(randomString, duplicateSize);

      Assert.assertEquals(duplicateSize, duplicateIDCache.getSize());

      server.getStorageManager().getMessageJournal().scheduleCompactAndBlock(10_000);
      HashMap<Integer, AtomicInteger> records = countJournal(server.getConfiguration());

      AtomicInteger duplicateRecordsCount = records.get((int) JournalRecordIds.DUPLICATE_ID);
      Assert.assertNotNull(duplicateRecordsCount);
      Assert.assertEquals(duplicateSize, duplicateRecordsCount.get());

      server.stop();
   }
}