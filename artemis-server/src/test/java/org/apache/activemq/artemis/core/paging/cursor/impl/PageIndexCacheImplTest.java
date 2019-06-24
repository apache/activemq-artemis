/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.core.paging.cursor.impl;

import java.util.TreeMap;

import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.core.io.nio.NIOSequentialFileFactory;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.core.paging.impl.Page;
import org.apache.activemq.artemis.core.paging.impl.PagedMessageImpl;
import org.apache.activemq.artemis.core.persistence.impl.nullpm.NullStorageManager;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Assert;
import org.junit.Test;

public class PageIndexCacheImplTest extends ActiveMQTestBase {
   @Test
   public void testPageReadMessage() throws Exception {
      recreateDirectory(getTestDir());

      SequentialFileFactory factory = new NIOSequentialFileFactory(getTestDirfile(), 1);
      SequentialFile file = factory.createSequentialFile("00010.page");
      Page page = new Page(new SimpleString("something"), new NullStorageManager(), factory, file, 10);
      page.open();
      SimpleString simpleDestination = new SimpleString("Test");
      for (int i = 0; i < 100; i++) {
         ICoreMessage msg = new CoreMessage().setMessageID(i).initBuffer(1024);

         for (int j = 0; j < 1000; j++) {
            msg.getBodyBuffer().writeByte((byte) 'b');
         }

         msg.setAddress(simpleDestination);

         page.write(new PagedMessageImpl(msg, new long[0]));

         Assert.assertEquals(i + 1, page.getNumberOfMessages());
      }
      page.sync();
      page.close();

      file = factory.createSequentialFile("00010.page");
      file.open();
      page = new Page(new SimpleString("something"), new NullStorageManager(), factory, file, 10);
      TreeMap<Integer, Integer> indexMap = new TreeMap<>();
      page.read(new NullStorageManager(), indexMap);

      assertTrue(indexMap.size() > 0);
      assertEquals(indexMap.firstKey().intValue(), 4);
      assertTrue(indexMap.firstEntry().getValue().intValue() > 4096);

      PageIndexCacheImpl pageIndexCache = new PageIndexCacheImpl(10);
      pageIndexCache.setNumberOfMessages(page.getNumberOfMessages());
      pageIndexCache.setMessageIndex(indexMap);
      pageIndexCache.setPage(page);
      for (int i = 0; i < 10; i++) {
         for (int j = 0; j < 5; j++) {
            int num = i * 5 + j;
            assertEquals(pageIndexCache.getMessage(num).getMessage().getMessageID(), num);
         }
         for (int j = 0; j < 5; j++) {
            int num = 50 + i * 5 + j;
            assertEquals(pageIndexCache.getMessage(num).getMessage().getMessageID(), num);
         }
      }
      try {
         pageIndexCache.getMessage(100);
         assertTrue("message num out of index", false);
      } catch (Exception e) {
      }
      pageIndexCache.close();
   }
}