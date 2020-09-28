/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.extras.byteman;

import java.io.File;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.apache.activemq.artemis.core.server.ActivateCallback;
import org.apache.activemq.artemis.core.server.NodeManager;
import org.apache.activemq.artemis.core.server.NodeManager.LockListener;
import org.apache.activemq.artemis.core.server.impl.FileLockNodeManager;
import org.apache.activemq.artemis.utils.Wait;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(BMUnitRunner.class)
public class FileLockMonitorTest {

   private File sharedDir;
   private volatile boolean lostLock = false;
   private volatile FileLockNodeManager nodeManager;
   private ScheduledThreadPoolExecutor executor;

   @Before
   public void handleLockFile() throws Exception {
      sharedDir = File.createTempFile("shared-dir", "");
      sharedDir.delete();
      Assert.assertTrue(sharedDir.mkdir());
   }

   @Test
   @BMRules(rules = {
         @BMRule(name = "lock is invalid", targetClass = "org.apache.activemq.artemis.core.server.impl.FileLockNodeManager", targetMethod = "isLiveLockLost", action = "return true;") })
   public void testLockMonitorInvalid() throws Exception {
      lostLock = false;
      startServer();
      Wait.assertTrue("The FileLockNodeManager should have lost the lock", () -> lostLock, 20_000, 100);
      nodeManager.isStarted();
      nodeManager.crashLiveServer();
      executor.shutdown();
   }

   public static void throwNodeManagerException(String msg) {
      throw new NodeManager.NodeManagerException(msg);
   }

   @Test
   @BMRules(rules = {
         @BMRule(name = "lock is invalid", targetClass = "org.apache.activemq.artemis.core.server.impl.FileLockNodeManager",
            targetMethod = "getState",
            action = "org.apache.activemq.artemis.tests.extras.byteman.FileLockMonitorTest.throwNodeManagerException(\"EFS is disconnected\");") })
   public void testLockMonitorIOException() throws Exception {
      lostLock = false;
      startServer();
      Wait.assertTrue("The FileLockNodeManager should have lost the lock", () -> lostLock, 5000, 100);
      nodeManager.crashLiveServer();
      executor.shutdown();
   }

   @Test
   public void testLockMonitorHasCorrectLockAndState() throws Exception {
      lostLock = false;
      startServer();
      Assert.assertFalse("The FileLockNodeManager should not have lost the lock", Wait.waitFor(() -> lostLock, 5000, 100));
      nodeManager.crashLiveServer();
      executor.shutdown();
   }

   @Test
   @BMRules(rules = {
         @BMRule(name = "lock is invalid", targetClass = "org.apache.activemq.artemis.core.server.impl.FileLockNodeManager", targetMethod = "getState", action = "return 70;") })
   public void testLockMonitorHasLockWrongState() throws Exception {
      lostLock = false;
      startServer();
      Assert.assertFalse("The FileLockNodeManager should not have lost the lock", Wait.waitFor(() -> lostLock, 5000, 100));
      nodeManager.crashLiveServer();
      executor.shutdown();
   }

   public LockListener startServer() throws Exception {
      executor = new ScheduledThreadPoolExecutor(2);
      nodeManager = new FileLockNodeManager(sharedDir, false, executor);
      LockListener listener = () -> {
         lostLock = true;
         try {
            nodeManager.crashLiveServer();
         } catch (Throwable t) {
            t.printStackTrace();
         }
      };
      nodeManager.registerLockListener(listener);

      try {
         nodeManager.start();
         ActivateCallback startLiveNode = nodeManager.startLiveNode();
         startLiveNode.activationComplete();

      } catch (Exception exception) {
         exception.printStackTrace();
      }

      return listener;
   }
}
