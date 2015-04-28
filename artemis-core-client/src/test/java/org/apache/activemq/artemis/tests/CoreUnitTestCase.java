/**
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
package org.apache.activemq.artemis.tests;


import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.core.client.ActiveMQClientLogger;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

public abstract class CoreUnitTestCase extends Assert
{
   public static void assertEqualsByteArrays(final byte[] expected, final byte[] actual)
   {
      for (int i = 0; i < expected.length; i++)
      {
         Assert.assertEquals("byte at index " + i, expected[i], actual[i]);
      }
   }

   private static final ActiveMQClientLogger log = ActiveMQClientLogger.LOGGER;

   @Rule
   public TestRule watcher = new TestWatcher()
   {
      @Override
      protected void starting(Description description)
      {
         log.info(String.format("#*#*# Starting test: %s()...", description.getMethodName()));
      }

      @Override
      protected void finished(Description description)
      {
         log.info(String.format("#*#*# Finished test: %s()...", description.getMethodName()));
      }
   };

   /**
    * Asserts that latch completes within a (rather large interval).
    * <p/>
    * Use this instead of just calling {@code latch.await()}. Otherwise your test may hang the whole
    * test run if it fails to count-down the latch.
    *
    * @param latch
    * @throws InterruptedException
    */
   public static void waitForLatch(CountDownLatch latch) throws InterruptedException
   {
      assertTrue("Latch has got to return within a minute", latch.await(1, TimeUnit.MINUTES));
   }

   public static int countOccurrencesOf(String str, String sub)
   {
      if (str == null || sub == null || str.length() == 0 || sub.length() == 0)
      {
         return 0;
      }
      int count = 0;
      int pos = 0;
      int idx;
      while ((idx = str.indexOf(sub, pos)) != -1)
      {
         ++count;
         pos = idx + sub.length();
      }
      return count;
   }

}
