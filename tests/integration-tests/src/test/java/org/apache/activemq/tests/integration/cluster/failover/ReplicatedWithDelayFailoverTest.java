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
package org.apache.activemq.tests.integration.cluster.failover;
import org.junit.Before;

import org.apache.activemq.api.core.client.ClientSession;
import org.apache.activemq.tests.integration.cluster.util.BackupSyncDelay;

/**
 * See {@link BackupSyncDelay} for the rationale about these 'WithDelay' tests.
 */
public class ReplicatedWithDelayFailoverTest extends ReplicatedFailoverTest
{

   private BackupSyncDelay syncDelay;

   @Override
   @Before
   public void setUp() throws Exception
   {
      startBackupServer = false;
      super.setUp();
      syncDelay = new BackupSyncDelay(backupServer, liveServer);
      backupServer.start();
      waitForServer(backupServer.getServer());
   }

   @Override
   protected void beforeWaitForRemoteBackupSynchronization()
   {
      syncDelay.deliverUpToDateMsg();
   }

   @Override
   protected void crash(ClientSession... sessions) throws Exception
   {
      syncDelay.deliverUpToDateMsg();
      super.crash(sessions);
   }

   @Override
   protected void crash(boolean waitFailure, ClientSession... sessions) throws Exception
   {
      syncDelay.deliverUpToDateMsg();
      waitForBackup(null, 5);
      super.crash(waitFailure, sessions);
   }
}
