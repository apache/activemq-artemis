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
package org.apache.activemq.artemis.tests.extras.byteman.critical.analyzer;

import java.lang.reflect.Field;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.activemq.artemis.core.persistence.impl.journal.AbstractJournalStorageManager;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(BMUnitRunner.class)
public class DeadlockStorageManagerTest extends CriticalAnalyzerFaultInjectionTestBase {

   private static Thread lockT;

   @BMRules(
      rules = {
         @BMRule(
            name = "Deadlock on Storage Manager",
            targetClass = "org.apache.activemq.artemis.core.persistence.impl.journal.AbstractJournalStorageManager",
            targetMethod = "commit",
            targetLocation = "ENTRY",
            condition = "!flagged(\"testDeadlockOnStorageManager\")",
            action = "org.apache.activemq.artemis.tests.extras.byteman.critical.analyzer.DeadlockStorageManagerTest.acquireStorageManagerLock($0);"),
         @BMRule(
            name = "Release Suspended Thread during Server Shutdown", // Releases wakes up suspended threads to allow shutdown to complete
            targetClass = "org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl",
            targetMethod = "stop",
            targetLocation = "ENTRY",
            action = "flag(\"testDeadlockOnStorageManager\"); org.apache.activemq.artemis.tests.extras.byteman.critical.analyzer.DeadlockStorageManagerTest.releaseStorageManagerLock()" )
      })
   @Test(timeout = 60000)
   public void testDeadlockOnStorageManager() throws Exception {
      testSendDurableMessage();
   }

   public static void acquireStorageManagerLock(final Object o) throws Exception {
      // We're attempting to cause a deadlock on the readlock.  This means we need to grab the write lock
      // in a separate thread.
      if (lockT == null) {
         lockT = new Thread(() -> {
            try {
               Field field = AbstractJournalStorageManager.class.getDeclaredField("storageManagerLock");
               field.setAccessible(true);
               ReentrantReadWriteLock lock = ((ReentrantReadWriteLock) field.get(o));
               lock.writeLock().lock();
               try {
                  Thread.sleep(10000);
               } catch (InterruptedException ie) {
                  lock.writeLock().unlock();
               }
            } catch (Exception e) {
               fail();
            }
         });
         lockT.start();
      }
   }

   public static void releaseStorageManagerLock() throws NoSuchFieldException, IllegalAccessException {
      lockT.interrupt();
   }
}
