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

import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.core.io.nio.NIOSequentialFileFactory;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.core.paging.PagedMessage;
import org.apache.activemq.artemis.core.paging.cursor.NonExistentPage;
import org.apache.activemq.artemis.core.paging.cursor.PagePosition;
import org.apache.activemq.artemis.core.paging.impl.Page;
import org.apache.activemq.artemis.core.paging.impl.PagedMessageImpl;
import org.apache.activemq.artemis.core.persistence.impl.nullpm.NullStorageManager;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.activemq.artemis.core.paging.cursor.impl.PageSubscriptionImpl.PagePositionAndFileOffset;
import static org.apache.activemq.artemis.utils.RandomUtil.randomBoolean;

public class PageReaderTest extends ActiveMQTestBase {

   @Test
   public void testPageReadMessage() throws Exception {
      recreateDirectory(getTestDir());

      int num = 50;
      int[] offsets = createPage(num);
      PageReader pageReader = getPageReader();

      PagedMessage[] pagedMessages = pageReader.getMessages();
      assertEquals(pagedMessages.length, num);

      PagedMessage pagedMessage = null;
      for (int i = 0; i < num; i++) {
         if (randomBoolean()) {
            PagePosition pagePosition = new PagePositionImpl(10, i);
            pagedMessage = pageReader.getMessage(pagePosition);
         } else {
            int nextFileOffset = pagedMessage == null ? -1 : offsets[i - 1] + pagedMessage.getEncodeSize() + Page.SIZE_RECORD;
            PagePositionAndFileOffset startPosition = new PagePositionAndFileOffset(nextFileOffset, new PagePositionImpl(10, i - 1));
            PagePosition pagePosition = startPosition.nextPagePostion();
            assertEquals(offsets[i], pagePosition.getFileOffset());
            pagedMessage = pageReader.getMessage(pagePosition);
         }
         assertNotNull(pagedMessage);
         assertEquals(pagedMessage.getMessage().getMessageID(), i);
         assertEquals(pagedMessages[i].getMessage().getMessageID(), i);
      }

      pageReader.close();
   }

   @Test
   public void testPageReadMessageBeyondPage() throws Exception {
      recreateDirectory(getTestDir());

      int num = 10;
      createPage(num);
      PageReader pageReader = getPageReader();

      assertNull(pageReader.getMessage(new PagePositionImpl(10, num)));
      try {
         pageReader.getMessage(new PagePositionImpl(10, num), true, true);
         assertFalse("Expect exception since message number is beyond page ", true);
      } catch (NonExistentPage e) {
      }

      pageReader.close();
   }

   @Test
   public void testPageReadMessageKeepOpen() throws Exception {
      recreateDirectory(getTestDir());

      int num = 10;
      createPage(num);
      PageReader pageReader = getPageReader();

      pageReader.getMessage(new PagePositionImpl(10, 1), true, true);
      assertFalse("Page file should keep open", pageReader.openPage());
      pageReader.getMessage(new PagePositionImpl(10, 1), true, false);
      assertFalse("Page file should preserve previous state", pageReader.openPage());

      pageReader.close();
      pageReader.getMessage(new PagePositionImpl(10, 1), true, false);
      assertTrue("Page file should preserve previous state", pageReader.openPage());

      pageReader.close();
   }

   private int[] createPage(int num) throws Exception {
      SequentialFileFactory factory = new NIOSequentialFileFactory(getTestDirfile(), 1);
      SequentialFile file = factory.createSequentialFile("00010.page");
      Page page = new Page(new SimpleString("something"), new NullStorageManager(), factory, file, 10);
      page.open();
      SimpleString simpleDestination = new SimpleString("Test");
      int[] offsets = new int[num];
      for (int i = 0; i < num; i++) {
         ICoreMessage msg = new CoreMessage().setMessageID(i).initBuffer(1024);

         for (int j = 0; j < 100; j++) {
            msg.getBodyBuffer().writeByte((byte) 'b');
         }

         msg.setAddress(simpleDestination);
         offsets[i] = (int)page.getFile().position();
         page.write(new PagedMessageImpl(msg, new long[0]));

         Assert.assertEquals(i + 1, page.getNumberOfMessages());
      }
      page.close(false, false);
      return offsets;
   }

   private PageReader getPageReader() throws Exception {
      SequentialFileFactory factory = new NIOSequentialFileFactory(getTestDirfile(), 1);
      SequentialFile file = factory.createSequentialFile("00010.page");
      file.open();
      Page page = new Page(new SimpleString("something"), new NullStorageManager(), factory, file, 10);
      page.open();
      page.read(new NullStorageManager());
      PageReader pageReader = new PageReader(page, page.getNumberOfMessages());
      return pageReader;
   }

}