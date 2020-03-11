/*
 * Copyright The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.core.paging.cursor.impl;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.paging.cursor.PageCache;
import org.apache.activemq.artemis.core.paging.impl.Page;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.utils.actors.ArtemisExecutor;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PageCursorProviderImplTest {

   @Test(timeout = 30_000)
   public void shouldAllowConcurrentPageReads() throws Exception {
      final PagingStore pagingStore = mock(PagingStore.class);
      final StorageManager storageManager = mock(StorageManager.class);
      when(storageManager.beforePageRead(anyLong(), any(TimeUnit.class))).thenReturn(true);
      final int pages = 2;
      final ArtemisExecutor artemisExecutor = mock(ArtemisExecutor.class);
      final PageCursorProviderImpl pageCursorProvider = new PageCursorProviderImpl(pagingStore, storageManager, artemisExecutor, 2);
      when(pagingStore.getCurrentWritingPage()).thenReturn(pages);
      when(pagingStore.checkPageFileExists(anyInt())).thenReturn(true);
      SequentialFileFactory sequentialFileFactory = mock(SequentialFileFactory.class);
      SequentialFile firstPageFile = mock(SequentialFile.class);
      when(firstPageFile.size()).thenReturn(0L);
      Page firstPage = new Page(new SimpleString("StorageManager"), storageManager, sequentialFileFactory, firstPageFile, 1);
      when(pagingStore.createPage(1)).thenReturn(firstPage);
      SequentialFile secondPageFile = mock(SequentialFile.class);
      when(secondPageFile.size()).thenReturn(0L);
      when(secondPageFile.isOpen()).thenReturn(true);
      Page secondPage = new Page(new SimpleString("StorageManager"), storageManager, sequentialFileFactory, secondPageFile, 2);
      when(pagingStore.createPage(2)).thenReturn(secondPage);
      final CountDownLatch finishFirstPageRead = new CountDownLatch(1);
      final Thread concurrentRead = new Thread(() -> {
         try {
            final PageCache cache = pageCursorProvider.getPageCache(2);
            Assert.assertNotNull(cache);
         } finally {
            finishFirstPageRead.countDown();
         }
      });
      try {
         when(firstPageFile.isOpen()).then(invocationOnMock -> {
            boolean pageReading = false;
            for (StackTraceElement element : Thread.currentThread().getStackTrace()) {
               if (element.getClassName().compareTo(Page.class.getName()) == 0 && element.getMethodName().compareTo("read") == 0) {
                  pageReading = true;
                  break;
               }
            }
            if (pageReading) {
               concurrentRead.start();
               finishFirstPageRead.await();
            }
            return true;
         });
         Assert.assertNotNull(pageCursorProvider.getPageCache(1));
      } finally {
         pageCursorProvider.stop();
         concurrentRead.interrupt();
         concurrentRead.join();
      }
   }

   @Test(timeout = 30_000)
   public void returnPageCacheImplIfEvicted() throws Exception {
      returnCacheIfEvicted(true);
   }

   @Test(timeout = 30_000)
   public void returnPageReaderIfEvicted() throws Exception {
      returnCacheIfEvicted(false);
   }

   private void returnCacheIfEvicted(boolean readWholePage) throws Exception {
      final PagingStore pagingStore = mock(PagingStore.class);
      final StorageManager storageManager = mock(StorageManager.class);
      when(storageManager.beforePageRead(anyLong(), any(TimeUnit.class))).thenReturn(true);
      final int pages = 2;
      final ArtemisExecutor artemisExecutor = mock(ArtemisExecutor.class);
      final PageCursorProviderImpl pageCursorProvider = new PageCursorProviderImpl(pagingStore, storageManager, artemisExecutor, 1, readWholePage);
      when(pagingStore.getCurrentWritingPage()).thenReturn(pages);
      when(pagingStore.checkPageFileExists(anyInt())).thenReturn(true);
      final Page firstPage = mock(Page.class);
      when(firstPage.getPageId()).thenReturn(1);
      when(pagingStore.createPage(1)).thenReturn(firstPage);
      final Page secondPage = mock(Page.class);
      when(secondPage.getPageId()).thenReturn(2);
      when(pagingStore.createPage(2)).thenReturn(secondPage);

      Assert.assertTrue(pageCursorProvider.getPageCache(1) instanceof PageCacheImpl);
      Assert.assertTrue(pageCursorProvider.getPageCache(2) instanceof PageCacheImpl);
      if (readWholePage) {
         Assert.assertTrue(pageCursorProvider.getPageCache(1) instanceof PageCacheImpl);
      } else {
         Assert.assertTrue(pageCursorProvider.getPageCache(1) instanceof PageReader);
      }
      Assert.assertEquals(pageCursorProvider.getCacheSize(), 1);
      Assert.assertTrue(pageCursorProvider.getPageCache(2) instanceof PageCacheImpl);
      pageCursorProvider.stop();
   }
}
