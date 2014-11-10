/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.hornetq.tests.integration.openwire.util;

import java.util.concurrent.TimeUnit;

/**
 * Utility adapted from: org.apache.activemq.util.Wait
 *
 * @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
 *
 */
public class Wait
{
   public static final long MAX_WAIT_MILLIS = 30 * 1000;
   public static final int SLEEP_MILLIS = 1000;

   public interface Condition
   {
      boolean isSatisified() throws Exception;
   }

   public static boolean waitFor(Condition condition) throws Exception
   {
      return waitFor(condition, MAX_WAIT_MILLIS);
   }

   public static boolean waitFor(final Condition condition, final long duration) throws Exception
   {
      return waitFor(condition, duration, SLEEP_MILLIS);
   }

   public static boolean waitFor(final Condition condition, final long duration, final int sleepMillis) throws Exception
   {

      final long expiry = System.currentTimeMillis() + duration;
      boolean conditionSatisified = condition.isSatisified();
      while (!conditionSatisified && System.currentTimeMillis() < expiry)
      {
         TimeUnit.MILLISECONDS.sleep(sleepMillis);
         conditionSatisified = condition.isSatisified();
      }
      return conditionSatisified;
   }

}
