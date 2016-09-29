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

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.ReplicatedBackupUtils;
import org.apache.activemq.artemis.tests.util.TransportConfigurationUtils;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(BMUnitRunner.class)
public class ReplicationBackupTest extends ActiveMQTestBase {

   private static final CountDownLatch ruleFired = new CountDownLatch(1);
   private ActiveMQServer backupServer;
   private ActiveMQServer liveServer;

   /*
   * simple test to induce a potential race condition where the server's acceptors are active, but the server's
   * state != STARTED
   */
   @Test
   @BMRules(
      rules = {@BMRule(
         name = "prevent backup annoucement",
         targetClass = "org.apache.activemq.artemis.core.server.impl.SharedNothingLiveActivation",
         targetMethod = "run",
         targetLocation = "AT EXIT",
         action = "org.apache.activemq.artemis.tests.extras.byteman.ReplicationBackupTest.breakIt();")})
   public void testReplicatedBackupAnnouncement() throws Exception {
      TransportConfiguration liveConnector = TransportConfigurationUtils.getNettyConnector(true, 0);
      TransportConfiguration liveAcceptor = TransportConfigurationUtils.getNettyAcceptor(true, 0);
      TransportConfiguration backupConnector = TransportConfigurationUtils.getNettyConnector(false, 0);
      TransportConfiguration backupAcceptor = TransportConfigurationUtils.getNettyAcceptor(false, 0);

      Configuration backupConfig = createDefaultInVMConfig().setBindingsDirectory(getBindingsDir(0, true)).setJournalDirectory(getJournalDir(0, true)).setPagingDirectory(getPageDir(0, true)).setLargeMessagesDirectory(getLargeMessagesDir(0, true));

      Configuration liveConfig = createDefaultInVMConfig();

      ReplicatedBackupUtils.configureReplicationPair(backupConfig, backupConnector, backupAcceptor, liveConfig, liveConnector, liveAcceptor);

      liveServer = createServer(liveConfig);

      // start the live server in a new thread so we can start the backup simultaneously to induce a potential race
      Thread startThread = new Thread(new Runnable() {
         @Override
         public void run() {
            try {
               liveServer.start();
            } catch (Exception e) {
               e.printStackTrace();
            }
         }
      });
      startThread.start();

      ruleFired.await();

      backupServer = createServer(backupConfig);
      backupServer.start();
      ActiveMQTestBase.waitForRemoteBackup(null, 3, true, backupServer);
   }

   public static void breakIt() {
      ruleFired.countDown();
      try {
         /* before the fix this sleep would put the "live" server into a state where the acceptors were started
          * but the server's state != STARTED which would cause the backup to fail to announce
          */
         Thread.sleep(2000);
      } catch (InterruptedException e) {
         e.printStackTrace();
      }
   }
}
