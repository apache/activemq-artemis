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

import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.paging.cursor.PageCache;
import org.apache.activemq.artemis.core.paging.impl.Page;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.utils.actors.ArtemisExecutor;
import org.junit.Assert;
import org.junit.Test;

import static java.util.Collections.emptyList;
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
      final Page firstPage = mock(Page.class);
      when(firstPage.getPageId()).thenReturn(1);
      when(pagingStore.createPage(1)).thenReturn(firstPage);
      final Page secondPage = mock(Page.class);
      when(secondPage.getPageId()).thenReturn(2);
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
         when(firstPage.read(storageManager)).then(invocationOnMock -> {
            concurrentRead.start();
            finishFirstPageRead.await();
            return emptyList();
         });
         Assert.assertNotNull(pageCursorProvider.getPageCache(1));
      } finally {
         pageCursorProvider.stop();
         concurrentRead.interrupt();
         concurrentRead.join();
      }
   }

}
