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

package org.apache.activemq.artemis.utils;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

public class SimpleFutureTest {

   @Rule
   public ThreadLeakCheckRule threadLeakCheckRule = new ThreadLeakCheckRule();

   @Test
   public void testFuture() throws Exception {
      final long randomStart = System.currentTimeMillis();
      final SimpleFuture<Long> simpleFuture = new SimpleFutureImpl<>();
      Thread t = new Thread() {
         @Override
         public void run() {
            simpleFuture.set(randomStart);
         }
      };
      t.start();

      Assert.assertEquals(randomStart, simpleFuture.get().longValue());
   }


   @Test
   public void testException() throws Exception {
      final SimpleFuture<Long> simpleFuture = new SimpleFutureImpl<>();
      Thread t = new Thread() {
         @Override
         public void run() {
            simpleFuture.fail(new Exception("hello"));
         }
      };
      t.start();

      boolean failed = false;
      try {
         simpleFuture.get();
      } catch (Exception e) {
         failed = true;
      }


      Assert.assertTrue(failed);
   }



}
