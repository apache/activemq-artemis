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
package org.hornetq.tests.integration.cluster.failover;

import org.hornetq.api.core.client.ClientSession;
import org.junit.Test;

public class ReplicatedFailoverTest extends FailoverTest
{

   @Test
   /*
   * default maxSavedReplicatedJournalsSize is 2, this means the backup will fall back to replicated only twice, after this
   * it is stopped permanently
   *
   * */
   public void testReplicatedFailback() throws Exception
   {
      try
      {
         backupServer.getServer().getConfiguration().getHAPolicy().setFailbackDelay(2000);
         backupServer.getServer().getConfiguration().setMaxSavedReplicatedJournalSize(2);
         createSessionFactory();

         ClientSession session = createSession(sf, true, true);

         session.createQueue(FailoverTestBase.ADDRESS, FailoverTestBase.ADDRESS, null, true);

         crash(session);

         liveServer.getServer().getConfiguration().setCheckForLiveServer(true);

         liveServer.start();

         waitForRemoteBackupSynchronization(liveServer.getServer());

         waitForRemoteBackupSynchronization(backupServer.getServer());

         waitForServer(liveServer.getServer());

         session = createSession(sf, true, true);

         crash(session);

         liveServer.getServer().getConfiguration().setCheckForLiveServer(true);

         liveServer.start();

         waitForRemoteBackupSynchronization(liveServer.getServer());

         waitForRemoteBackupSynchronization(backupServer.getServer());

         waitForServer(liveServer.getServer());

         session = createSession(sf, true, true);

         crash(session);

         liveServer.getServer().getConfiguration().setCheckForLiveServer(true);

         liveServer.start();

         waitForRemoteBackupSynchronization(liveServer.getServer());

         waitForServer(liveServer.getServer());

         //this will give the backup time to stop fully
         waitForServerToStop(backupServer.getServer());

         assertFalse(backupServer.getServer().isStarted());

         //the server wouldnt have reset to backup
         assertFalse(backupServer.getServer().getConfiguration().getHAPolicy().isBackup());
      }
      finally
      {
         sf.close();
      }
   }

   @Override
   protected void createConfigs() throws Exception
   {
      createReplicatedConfigs();
   }

   @Override
   protected void crash(boolean waitFailure, ClientSession... sessions) throws Exception
   {
      if (sessions.length > 0)
      {
         for (ClientSession session : sessions)
         {
            waitForRemoteBackup(session.getSessionFactory(), 5, true, backupServer.getServer());
         }
      }
      else
      {
         waitForRemoteBackup(null, 5, true, backupServer.getServer());
      }
      super.crash(waitFailure, sessions);
   }

   @Override
   protected void crash(ClientSession... sessions) throws Exception
   {
      if (sessions.length > 0)
      {
         for (ClientSession session : sessions)
         {
            waitForRemoteBackup(session.getSessionFactory(), 5, true, backupServer.getServer());
         }
      }
      else
      {
         waitForRemoteBackup(null, 5, true, backupServer.getServer());
      }
      super.crash(sessions);
   }
}
