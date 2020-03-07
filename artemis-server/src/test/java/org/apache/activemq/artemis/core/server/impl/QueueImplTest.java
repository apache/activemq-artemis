/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.core.server.impl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.paging.cursor.PageIterator;
import org.apache.activemq.artemis.core.paging.cursor.PagePosition;
import org.apache.activemq.artemis.core.paging.cursor.PageSubscription;
import org.apache.activemq.artemis.core.paging.cursor.PagedReferenceImpl;
import org.apache.activemq.artemis.core.paging.impl.PagedMessageImpl;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.QueueFactory;
import org.apache.activemq.artemis.utils.actors.ArtemisExecutor;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class QueueImplTest {

   @Test
   public void deleteAllPagedReferencesTest() throws Exception {
      SimpleString address = new SimpleString("test");
      AtomicInteger pageIteratorIndex = new AtomicInteger(0);
      PageIterator pageIterator = Mockito.mock(PageIterator.class);
      PageSubscription pageSubscription = Mockito.mock(PageSubscription.class);
      ExecutorService executorService = Executors.newSingleThreadExecutor();
      StorageManager storageManager = Mockito.mock(StorageManager.class);

      final int flushLimit = 100;
      final int pagedReferences = 5 * flushLimit;
      Semaphore availableTransactions = new Semaphore(3);

      //Mock pageIterator.
      Mockito.doAnswer(invocationOnMock -> pageIteratorIndex.get() < pagedReferences).
         when(pageIterator).hasNext();
      Mockito.doAnswer(invocationOnMock -> {
         pageIteratorIndex.incrementAndGet();
         return new PagedReferenceImpl(Mockito.mock(PagePosition.class), new PagedMessageImpl(
            Mockito.mock(Message.class), new long[]{0}), pageSubscription);
      }).when(pageIterator).next();
      Mockito.doReturn(pageIterator).when(pageSubscription).iterator();

      //Mock storageManager.
      Mockito.doAnswer(invocationOnMock -> {
         Assert.assertTrue("Too transactions locked on afterCommit.",
                           availableTransactions.tryAcquire(3000, TimeUnit.MILLISECONDS));
         executorService.execute(() -> {
            ((IOCallback) invocationOnMock.getArgument(0)).done();
            availableTransactions.release();
         });
         return null;
      }).when(storageManager).afterCompleteOperations(Mockito.any(IOCallback.class));

      QueueImpl queue = new QueueImpl(0, address, address, null, null, pageSubscription, null, false,
                                      false, false, Mockito.mock(ScheduledExecutorService.class),
                                      Mockito.mock(PostOffice.class), storageManager, null,
                                      Mockito.mock(ArtemisExecutor.class), Mockito.mock(ActiveMQServer.class),
                                      Mockito.mock(QueueFactory.class));

      Mockito.doReturn(queue).when(pageSubscription).getQueue();

      Assert.assertEquals(pagedReferences, queue.deleteAllReferences(flushLimit));
   }
}