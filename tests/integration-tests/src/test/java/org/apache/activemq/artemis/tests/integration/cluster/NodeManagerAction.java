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

import java.io.File;

import org.apache.activemq.artemis.core.server.NodeManager;
import org.apache.activemq.artemis.core.server.impl.FileLockNodeManager;

public class NodeManagerAction {

   public static final int START_LIVE = 0;
   public static final int START_BACKUP = 1;
   public static final int CRASH_LIVE = 2;
   public static final int PAUSE_LIVE = 3;
   public static final int STOP_BACKUP = 4;
   public static final int AWAIT_LIVE = 5;
   public static final int RELEASE_BACKUP = 6;

   public static final int HAS_LIVE = 10;
   public static final int HAS_BACKUP = 11;
   public static final int DOESNT_HAVE_LIVE = 12;
   public static final int DOESNT_HAVE_BACKUP = 13;

   private final int[] work;

   boolean hasLiveLock = false;
   boolean hasBackupLock = false;

   public NodeManagerAction(int... work) {
      this.work = work;
   }

   public void performWork(NodeManager nodeManager) throws Exception {
      for (int action : work) {
         switch (action) {
            case START_LIVE:
               nodeManager.startLiveNode().activationComplete();
               hasLiveLock = true;
               hasBackupLock = false;
               break;
            case START_BACKUP:
               nodeManager.startBackup();
               hasBackupLock = true;
               break;
            case CRASH_LIVE:
               nodeManager.crashLiveServer();
               hasLiveLock = false;
               break;
            case PAUSE_LIVE:
               nodeManager.pauseLiveServer();
               hasLiveLock = false;
               break;
            case STOP_BACKUP:
               nodeManager.stopBackup();
               hasBackupLock = false;
               break;
            case AWAIT_LIVE:
               nodeManager.awaitLiveNode();
               hasLiveLock = true;
               break;
            case RELEASE_BACKUP:
               nodeManager.releaseBackup();
               hasBackupLock = false;
            case HAS_LIVE:
               if (!hasLiveLock) {
                  throw new IllegalStateException("live lock not held");
               }
               break;
            case HAS_BACKUP:

               if (!hasBackupLock) {
                  throw new IllegalStateException("backup lock not held");
               }
               break;
            case DOESNT_HAVE_LIVE:
               if (hasLiveLock) {
                  throw new IllegalStateException("live lock held");
               }
               break;
            case DOESNT_HAVE_BACKUP:

               if (hasBackupLock) {
                  throw new IllegalStateException("backup lock held");
               }
               break;
         }
      }
   }

   public String[] getWork() {
      String[] strings = new String[work.length];
      for (int i = 0, stringsLength = strings.length; i < stringsLength; i++) {
         strings[i] = "" + work[i];
      }
      return strings;
   }

   public static void main(String[] args) throws Exception {
      int[] work1 = new int[args.length];
      for (int i = 0; i < args.length; i++) {
         work1[i] = Integer.parseInt(args[i]);

      }
      NodeManagerAction nodeManagerAction = new NodeManagerAction(work1);
      FileLockNodeManager nodeManager = new FileLockNodeManager(new File("."), false);
      nodeManager.start();
      try {
         nodeManagerAction.performWork(nodeManager);
      } catch (Exception e) {
         e.printStackTrace();
         System.exit(9);
      }
      System.out.println("work performed");
   }

}
