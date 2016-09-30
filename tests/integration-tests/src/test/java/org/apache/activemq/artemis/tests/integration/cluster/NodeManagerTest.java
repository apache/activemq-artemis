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
package org.apache.activemq.artemis.tests.integration.cluster;

import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.artemis.core.server.NodeManager;
import org.apache.activemq.artemis.core.server.impl.InVMNodeManager;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Test;

import static org.apache.activemq.artemis.tests.integration.cluster.NodeManagerAction.AWAIT_LIVE;
import static org.apache.activemq.artemis.tests.integration.cluster.NodeManagerAction.CRASH_LIVE;
import static org.apache.activemq.artemis.tests.integration.cluster.NodeManagerAction.DOESNT_HAVE_BACKUP;
import static org.apache.activemq.artemis.tests.integration.cluster.NodeManagerAction.DOESNT_HAVE_LIVE;
import static org.apache.activemq.artemis.tests.integration.cluster.NodeManagerAction.HAS_BACKUP;
import static org.apache.activemq.artemis.tests.integration.cluster.NodeManagerAction.HAS_LIVE;
import static org.apache.activemq.artemis.tests.integration.cluster.NodeManagerAction.PAUSE_LIVE;
import static org.apache.activemq.artemis.tests.integration.cluster.NodeManagerAction.RELEASE_BACKUP;
import static org.apache.activemq.artemis.tests.integration.cluster.NodeManagerAction.START_BACKUP;
import static org.apache.activemq.artemis.tests.integration.cluster.NodeManagerAction.START_LIVE;
import static org.apache.activemq.artemis.tests.integration.cluster.NodeManagerAction.STOP_BACKUP;

public class NodeManagerTest extends ActiveMQTestBase {

   @Test
   public void testLive() throws Exception {
      NodeManagerAction live1 = new NodeManagerAction(START_LIVE, HAS_LIVE, DOESNT_HAVE_BACKUP, CRASH_LIVE, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_LIVE, HAS_LIVE, DOESNT_HAVE_BACKUP, CRASH_LIVE, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      performWork(live1);
   }

   @Test
   public void testSimpleLiveAndBackup() throws Exception {
      NodeManagerAction live1 = new NodeManagerAction(START_LIVE, HAS_LIVE, DOESNT_HAVE_BACKUP, CRASH_LIVE, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      NodeManagerAction backup1 = new NodeManagerAction(DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_BACKUP, HAS_BACKUP, AWAIT_LIVE, RELEASE_BACKUP, HAS_LIVE, PAUSE_LIVE, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      performWork(live1, backup1);
   }

   @Test
   public void testSimpleBackupAndLive() throws Exception {
      NodeManagerAction live1 = new NodeManagerAction(START_LIVE, HAS_LIVE, DOESNT_HAVE_BACKUP, CRASH_LIVE, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      NodeManagerAction backup1 = new NodeManagerAction(DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_BACKUP, HAS_BACKUP, AWAIT_LIVE, RELEASE_BACKUP, HAS_LIVE, PAUSE_LIVE, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      performWork(backup1, live1);
   }

   @Test
   public void testSimpleLiveAnd2Backups() throws Exception {
      NodeManagerAction live1 = new NodeManagerAction(START_LIVE, HAS_LIVE, DOESNT_HAVE_BACKUP, CRASH_LIVE, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      NodeManagerAction backup1 = new NodeManagerAction(DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_BACKUP, HAS_BACKUP, AWAIT_LIVE, RELEASE_BACKUP, HAS_LIVE, CRASH_LIVE, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      NodeManagerAction backup2 = new NodeManagerAction(DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_BACKUP, HAS_BACKUP, AWAIT_LIVE, RELEASE_BACKUP, HAS_LIVE, CRASH_LIVE, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      performWork(live1, backup1, backup2);
   }

   @Test
   public void testSimple2BackupsAndLive() throws Exception {
      NodeManagerAction live1 = new NodeManagerAction(START_LIVE, HAS_LIVE, DOESNT_HAVE_BACKUP, CRASH_LIVE, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      NodeManagerAction backup1 = new NodeManagerAction(DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_BACKUP, HAS_BACKUP, AWAIT_LIVE, RELEASE_BACKUP, HAS_LIVE, CRASH_LIVE, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      NodeManagerAction backup2 = new NodeManagerAction(DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_BACKUP, HAS_BACKUP, AWAIT_LIVE, RELEASE_BACKUP, HAS_LIVE, CRASH_LIVE, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      performWork(backup1, backup2, live1);
   }

   @Test
   public void testSimpleLiveAnd2BackupsPaused() throws Exception {
      NodeManagerAction live1 = new NodeManagerAction(START_LIVE, HAS_LIVE, DOESNT_HAVE_BACKUP, CRASH_LIVE, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      NodeManagerAction backup1 = new NodeManagerAction(DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_BACKUP, HAS_BACKUP, AWAIT_LIVE, RELEASE_BACKUP, HAS_LIVE, PAUSE_LIVE, START_LIVE, HAS_LIVE, DOESNT_HAVE_BACKUP, CRASH_LIVE, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      NodeManagerAction backup2 = new NodeManagerAction(DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_BACKUP, HAS_BACKUP, AWAIT_LIVE, RELEASE_BACKUP, HAS_LIVE, PAUSE_LIVE, START_LIVE, HAS_LIVE, DOESNT_HAVE_BACKUP, CRASH_LIVE, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      performWork(live1, backup1, backup2);
   }

   @Test
   public void testSimple2BackupsPausedAndLive() throws Exception {
      NodeManagerAction live1 = new NodeManagerAction(START_LIVE, HAS_LIVE, DOESNT_HAVE_BACKUP, CRASH_LIVE, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      NodeManagerAction backup1 = new NodeManagerAction(DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_BACKUP, HAS_BACKUP, AWAIT_LIVE, RELEASE_BACKUP, HAS_LIVE, PAUSE_LIVE, START_LIVE, HAS_LIVE, DOESNT_HAVE_BACKUP, CRASH_LIVE, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      NodeManagerAction backup2 = new NodeManagerAction(DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_BACKUP, HAS_BACKUP, AWAIT_LIVE, RELEASE_BACKUP, HAS_LIVE, PAUSE_LIVE, START_LIVE, HAS_LIVE, DOESNT_HAVE_BACKUP, CRASH_LIVE, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      performWork(backup1, backup2, live1);
   }

   @Test
   public void testBackupsOnly() throws Exception {
      NodeManagerAction backup1 = new NodeManagerAction(DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_BACKUP, HAS_BACKUP, STOP_BACKUP, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      NodeManagerAction backup2 = new NodeManagerAction(DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_BACKUP, HAS_BACKUP, STOP_BACKUP, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      NodeManagerAction backup3 = new NodeManagerAction(DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_BACKUP, HAS_BACKUP, STOP_BACKUP, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      NodeManagerAction backup4 = new NodeManagerAction(DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_BACKUP, HAS_BACKUP, STOP_BACKUP, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      NodeManagerAction backup5 = new NodeManagerAction(DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_BACKUP, HAS_BACKUP, STOP_BACKUP, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      NodeManagerAction backup6 = new NodeManagerAction(DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_BACKUP, HAS_BACKUP, STOP_BACKUP, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      NodeManagerAction backup7 = new NodeManagerAction(DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_BACKUP, HAS_BACKUP, STOP_BACKUP, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      NodeManagerAction backup8 = new NodeManagerAction(DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_BACKUP, HAS_BACKUP, STOP_BACKUP, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      NodeManagerAction backup9 = new NodeManagerAction(DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_BACKUP, HAS_BACKUP, STOP_BACKUP, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      NodeManagerAction backup10 = new NodeManagerAction(DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_BACKUP, HAS_BACKUP, STOP_BACKUP, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      NodeManagerAction backup11 = new NodeManagerAction(DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_BACKUP, HAS_BACKUP, STOP_BACKUP, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE);
      performWork(backup1, backup2, backup3, backup4, backup5, backup6, backup7, backup8, backup9, backup10, backup11);
   }

   @Test
   public void testLiveAndBackupLiveForcesFailback() throws Exception {
      NodeManagerAction live1 = new NodeManagerAction(START_LIVE, HAS_LIVE, DOESNT_HAVE_BACKUP, CRASH_LIVE, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_LIVE, HAS_LIVE, DOESNT_HAVE_BACKUP, CRASH_LIVE);
      NodeManagerAction backup1 = new NodeManagerAction(DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_BACKUP, HAS_BACKUP, AWAIT_LIVE, RELEASE_BACKUP, HAS_LIVE, CRASH_LIVE, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, AWAIT_LIVE, HAS_LIVE, PAUSE_LIVE);
      performWork(live1, backup1);
   }

   @Test
   public void testLiveAnd2BackupsLiveForcesFailback() throws Exception {
      NodeManagerAction live1 = new NodeManagerAction(START_LIVE, HAS_LIVE, DOESNT_HAVE_BACKUP, CRASH_LIVE, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_LIVE, HAS_LIVE, DOESNT_HAVE_BACKUP, CRASH_LIVE);
      NodeManagerAction backup1 = new NodeManagerAction(DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_BACKUP, HAS_BACKUP, AWAIT_LIVE, RELEASE_BACKUP, HAS_LIVE, CRASH_LIVE, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, AWAIT_LIVE, RELEASE_BACKUP, HAS_LIVE, CRASH_LIVE);
      NodeManagerAction backup2 = new NodeManagerAction(DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, START_BACKUP, HAS_BACKUP, AWAIT_LIVE, RELEASE_BACKUP, HAS_LIVE, CRASH_LIVE, DOESNT_HAVE_BACKUP, DOESNT_HAVE_LIVE, AWAIT_LIVE, RELEASE_BACKUP, HAS_LIVE, CRASH_LIVE);
      performWork(live1, backup1, backup2);
   }

   public void performWork(NodeManagerAction... actions) throws Exception {
      NodeManager nodeManager = new InVMNodeManager(false);
      List<NodeRunner> nodeRunners = new ArrayList<>();
      Thread[] threads = new Thread[actions.length];
      for (NodeManagerAction action : actions) {
         NodeRunner nodeRunner = new NodeRunner(nodeManager, action);
         nodeRunners.add(nodeRunner);
      }
      for (int i = 0, nodeRunnersSize = nodeRunners.size(); i < nodeRunnersSize; i++) {
         NodeRunner nodeRunner = nodeRunners.get(i);
         threads[i] = new Thread(nodeRunner);
         threads[i].start();
      }

      for (Thread thread : threads) {
         try {
            thread.join(5000);
         } catch (InterruptedException e) {
            //
         }
         if (thread.isAlive()) {
            thread.interrupt();
            fail("thread still running");
         }
      }

      for (NodeRunner nodeRunner : nodeRunners) {
         if (nodeRunner.e != null) {
            nodeRunner.e.printStackTrace();
            fail(nodeRunner.e.getMessage());
         }
      }
   }

   static class NodeRunner implements Runnable {

      private NodeManagerAction action;
      private NodeManager manager;
      Throwable e;

      NodeRunner(NodeManager nodeManager, NodeManagerAction action) {
         this.manager = nodeManager;
         this.action = action;
      }

      @Override
      public void run() {
         try {
            action.performWork(manager);
         } catch (Throwable e) {
            this.e = e;
         }
      }
   }

}
