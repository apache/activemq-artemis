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
package org.apache.activemq.artemis.junit;

import java.util.concurrent.TimeUnit;

import org.junit.Assert;

/**
 * Utility adapted from: org.apache.activemq.util.Wait
 */
public class Wait {


   public static final long MAX_WAIT_MILLIS = 30 * 1000;
   public static final int SLEEP_MILLIS = 1000;

   public interface Condition {

      boolean isSatisfied() throws Exception;
   }

   public interface LongCondition {
      long getCount() throws Exception;
   }

   public interface IntCondition {
      int getCount() throws Exception;
   }

   public static boolean waitFor(Condition condition) throws Exception {
      return waitFor(condition, MAX_WAIT_MILLIS);
   }


   public static void assertEquals(long size, LongCondition condition) throws Exception {
      assertEquals(size, condition, MAX_WAIT_MILLIS);
   }

   public static void assertEquals(long size, LongCondition condition, long timeout) throws Exception {
      assertEquals(size, condition, timeout, SLEEP_MILLIS);
   }

   public static void assertEquals(Long size, LongCondition condition, long timeout, long sleepMillis) throws Exception {
      boolean result = waitFor(() -> condition.getCount() == size, timeout, sleepMillis);

      if (!result) {
         Assert.fail(size + " != " + condition.getCount());
      }
   }


   public static void assertEquals(int size, IntCondition condition) throws Exception {
      assertEquals(size, condition, MAX_WAIT_MILLIS);
   }

   public static void assertEquals(int size, IntCondition condition, long timeout) throws Exception {
      assertEquals(size, condition, timeout, SLEEP_MILLIS);
   }

   public static void assertEquals(int size, IntCondition condition, long timeout, long sleepMillis) throws Exception {
      boolean result = waitFor(() -> condition.getCount() == size, timeout, sleepMillis);

      if (!result) {
         Assert.fail(size + " != " + condition.getCount());
      }
   }

   public static void assertTrue(Condition condition) throws Exception {
      assertTrue("Condition wasn't met", condition);
   }

   public static void assertFalse(Condition condition) throws Exception {
      assertTrue(() -> !condition.isSatisfied());
   }

   public static void assertFalse(String failureMessage, Condition condition) throws Exception {
      assertTrue(failureMessage, () -> !condition.isSatisfied());
   }


   public static void assertTrue(String failureMessage, Condition condition) throws Exception {
      boolean result = waitFor(condition);

      if (!result) {
         Assert.fail(failureMessage);
      }
   }

   public static boolean waitFor(final Condition condition, final long duration) throws Exception {
      return waitFor(condition, duration, SLEEP_MILLIS);
   }

   public static boolean waitFor(final Condition condition,
                                 final long durationMillis,
                                 final long sleepMillis) throws Exception {

      final long expiry = System.currentTimeMillis() + durationMillis;
      boolean conditionSatisified = condition.isSatisfied();
      while (!conditionSatisified && System.currentTimeMillis() < expiry) {
         TimeUnit.MILLISECONDS.sleep(sleepMillis);
         conditionSatisified = condition.isSatisfied();
      }
      return conditionSatisified;
   }


}
