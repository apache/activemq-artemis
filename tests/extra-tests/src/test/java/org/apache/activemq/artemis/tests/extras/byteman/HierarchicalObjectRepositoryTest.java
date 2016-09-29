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
package org.apache.activemq.artemis.tests.extras.byteman;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.core.settings.impl.HierarchicalObjectRepository;
import org.apache.activemq.artemis.utils.ActiveMQThreadFactory;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(BMUnitRunner.class)
@BMRules(rules = {@BMRule(name = "modify map during iteration",
   targetClass = "org.apache.activemq.artemis.core.settings.impl.HierarchicalObjectRepository",
   targetMethod = "getPossibleMatches(String)", targetLocation = "AT INVOKE java.util.HashMap.put",
   action = "org.apache.activemq.artemis.tests.extras.byteman.HierarchicalObjectRepositoryTest.bum()"),})
public class HierarchicalObjectRepositoryTest {

   private static final String A = "a.";
   private static final int TOTAL = 100;
   private static CountDownLatch latch;
   private static CountDownLatch latch2;
   private ExecutorService executor;
   private HierarchicalObjectRepository<String> repo;

   @Before
   public void setUp() {
      latch = new CountDownLatch(1);
      latch2 = new CountDownLatch(1);
      executor = Executors.newSingleThreadExecutor(ActiveMQThreadFactory.defaultThreadFactory());
      repo = new HierarchicalObjectRepository<>();
      addToRepo(repo, A);
   }

   static void addToRepo(HierarchicalObjectRepository<String> repo0, String pattern) {
      for (int i = 0; i < TOTAL; i++) {
         repo0.addMatch(pattern + i + ".*", String.valueOf(i));
      }
   }

   @After
   public void tearDown() throws InterruptedException {
      latch.countDown();
      latch2.countDown();
      executor.shutdown();
      executor.awaitTermination(1, TimeUnit.SECONDS);
   }

   private class Clearer implements Runnable {

      private final int code;

      private Clearer(int code) {
         this.code = code;
      }

      @Override
      public void run() {
         try {
            latch.await();
         } catch (InterruptedException e) {
            throw new RuntimeException(e);
         }

         switch (code) {
            case 0:
               repo.clear();
               break;
            case 1:
               addToRepo(repo, "bb.");
               break;
            case 2:
               for (int i = TOTAL / 2; i < TOTAL; i++) {
                  repo.removeMatch(A + i + ".*");
               }
               break;
            default:
               throw new RuntimeException();
         }

         latch2.countDown();
      }
   }

   @Test
   public void testConcurrentModificationsClear() {
      executor.execute(new Clearer(0));
      repo.getMatch(A + (TOTAL - 10) + ".foobar");
      Assert.assertEquals("Byteman rule failed?", 0, latch.getCount());
   }

   @Test
   public void testConcurrentModificationsAdd() {
      executor.execute(new Clearer(1));
      repo.getMatch(A + (TOTAL - 10) + ".foobar");
      Assert.assertEquals("Byteman rule failed?", 0, latch.getCount());
   }

   @Test
   public void testConcurrentModificationsRemove() {
      executor.execute(new Clearer(2));
      repo.getMatch(A + (TOTAL - 10) + ".foobar");
      Assert.assertEquals("Byteman rule failed?", 0, latch.getCount());
   }

   public static void bum() {
      latch.countDown();
      try {
         latch2.await(3, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
         // no op
      }
   }
}
