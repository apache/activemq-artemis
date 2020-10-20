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

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.apache.activemq.artemis.core.server.impl.FileLockNodeManager;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.jboss.logging.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(BMUnitRunner.class)
public class FileLockNodeManagerTest {
   private static final Logger log = Logger.getLogger(FileLockNodeManagerTest.class);

   private static final int TIMEOUT_TOLERANCE = 50;

   private File sharedDir;

   public FileLockNodeManagerTest() throws IOException {
      sharedDir = File.createTempFile("shared-dir", "");
      sharedDir.delete();
      Assert.assertTrue(sharedDir.mkdir());
   }

   @Test
   @BMRules(
         rules = {@BMRule(
               name = "throw IOException during activation",
               targetClass = "org.apache.activemq.artemis.core.server.impl.FileLockNodeManager",
               targetMethod = "tryLock",
               targetLocation = "AT ENTRY",
               action = "THROW new IOException(\"IO Error\");")
         })
   public void test() throws Exception {
      measureLockAcquisisionTimeout(100); // warm-up

      assertMeasuredTimeoutFor(100);
      assertMeasuredTimeoutFor(200);
   }

   protected void assertMeasuredTimeoutFor(long lockAcquisitionTimeout) throws Exception {
      long realTimeout = measureLockAcquisisionTimeout(lockAcquisitionTimeout);
      log.debug(String.format("lockAcquisisionTimeout: %d ms, measured timeout: %d ms", lockAcquisitionTimeout, realTimeout));
      Assert.assertTrue(String.format("Timeout %d ms was larger than expected %d ms", realTimeout, lockAcquisitionTimeout + TIMEOUT_TOLERANCE),
            lockAcquisitionTimeout + TIMEOUT_TOLERANCE >= realTimeout);
      Assert.assertTrue(String.format("Timeout %d ms was lower than expected %d ms", realTimeout, lockAcquisitionTimeout),
            lockAcquisitionTimeout + TIMEOUT_TOLERANCE >= realTimeout);
   }

   private long measureLockAcquisisionTimeout(long lockAcquisitionTimeout) throws Exception {
      FileLockNodeManager manager = new FileLockNodeManager(sharedDir, false, lockAcquisitionTimeout, new ScheduledThreadPoolExecutor(1));
      manager.start();

      // try to lock and measure real timeout
      long start = System.currentTimeMillis();
      try {
         manager.awaitLiveNode();
      } catch (Exception e) {
         long stop = System.currentTimeMillis();
         if (!"timed out waiting for lock".equals(e.getCause().getMessage())) {
            throw e;
         }
         return stop - start;
      }
      Assert.fail("Exception expected");
      return 0;
   }
}
