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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.StoreConfiguration;
import org.apache.activemq.artemis.core.io.IOCallback;
import org.apache.activemq.artemis.core.persistence.impl.journal.OperationContextImpl;
import org.apache.activemq.artemis.core.postoffice.DuplicateIDCache;
import org.apache.activemq.artemis.core.postoffice.impl.DuplicateIDCaches;
import org.apache.activemq.artemis.core.transaction.impl.TransactionImpl;
import org.apache.activemq.artemis.tests.extensions.parameterized.ParameterizedTestExtension;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

//Parameters set in super class
@ExtendWith(ParameterizedTestExtension.class)
public class DuplicateCacheTest extends StorageManagerTestBase {

   public DuplicateCacheTest(StoreConfiguration.StoreType storeType) {
      super(storeType);
   }

   @AfterEach
   @Override
   public void tearDown() throws Exception {
      super.tearDown();
   }

   @TestTemplate
   public void testDuplicate() throws Exception {
      createStorage();

      DuplicateIDCache cache = DuplicateIDCaches.persistent(new SimpleString("test"), 2000, journal);

      TransactionImpl tx = new TransactionImpl(journal);

      for (int i = 0; i < 5000; i++) {
         byte[] bytes = RandomUtil.randomBytes();

         cache.addToCache(bytes, tx);
      }

      tx.commit();

      tx = new TransactionImpl(journal);

      for (int i = 0; i < 5000; i++) {
         byte[] bytes = RandomUtil.randomBytes();

         cache.addToCache(bytes, tx);
      }

      tx.commit();

      byte[] id = RandomUtil.randomBytes();

      assertFalse(cache.contains(id));

      cache.addToCache(id, null);

      assertTrue(cache.contains(id));

      cache.deleteFromCache(id);

      final CountDownLatch latch = new CountDownLatch(1);
      OperationContextImpl.getContext().executeOnCompletion(new IOCallback() {
         @Override
         public void done() {
            latch.countDown();
         }

         @Override
         public void onError(int errorCode, String errorMessage) {

         }
      }, true);

      assertTrue(latch.await(1, TimeUnit.MINUTES));

      assertFalse(cache.contains(id));

      cache.clear();
   }

   @TestTemplate
   public void testDuplicateNonPersistent() throws Exception {
      createStorage();

      DuplicateIDCache cache = DuplicateIDCaches.inMemory(new SimpleString("test"), 2000);

      TransactionImpl tx = new TransactionImpl(journal);

      for (int i = 0; i < 5000; i++) {
         byte[] bytes = RandomUtil.randomBytes();

         cache.addToCache(bytes, tx);
      }

      tx.commit();

      for (int i = 0; i < 5000; i++) {
         byte[] bytes = RandomUtil.randomBytes();

         cache.addToCache(bytes, null);
      }

      cache.clear();
   }

   @TestTemplate
   public void testDisabledPersistentCache() throws Exception {
      createStorage();
      DuplicateIDCache cache = DuplicateIDCaches.persistent(new SimpleString("test"), 0, journal);
      byte[] bytes = RandomUtil.randomBytes();
      // Previously this would throw an ArrayIndexOutOfBoundsException
      cache.addToCache(bytes);
   }

   @TestTemplate
   public void testDisabledInMemoryCache() throws Exception {
      createStorage();
      DuplicateIDCache cache = DuplicateIDCaches.inMemory(new SimpleString("test"), 0);
      byte[] bytes = RandomUtil.randomBytes();
      // Previously this would throw an ArrayIndexOutOfBoundsException
      cache.addToCache(bytes);
   }
}
