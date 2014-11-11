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
/**
 *
 */
package org.apache.activemq6.tests.integration.cluster.failover;
import org.junit.Before;

import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq6.api.core.HornetQException;
import org.apache.activemq6.api.core.Interceptor;
import org.apache.activemq6.api.core.TransportConfiguration;
import org.apache.activemq6.core.protocol.core.Packet;
import org.apache.activemq6.core.protocol.core.impl.PacketImpl;
import org.apache.activemq6.spi.core.protocol.RemotingConnection;
import org.apache.activemq6.tests.util.TransportConfigurationUtils;

public class BackupAuthenticationTest extends FailoverTestBase
{
   private static CountDownLatch latch;
   @Override
   @Before
   public void setUp() throws Exception
   {
      startBackupServer = false;
      latch = new CountDownLatch(1);
      super.setUp();
   }

   @Test
   public void testPasswordSetting() throws Exception
   {
      waitForServer(liveServer.getServer());
      backupServer.start();
      assertTrue(latch.await(5, TimeUnit.SECONDS));
      /*
       * can't intercept the message at the backup, so we intercept the registration message at the
       * live.
       */
      Thread.sleep(2000);
      assertFalse("backup should have stopped", backupServer.isStarted());
      backupConfig.setClusterPassword(CLUSTER_PASSWORD);
      backupServer.start();
      waitForRemoteBackup(null, 5, true, backupServer.getServer());
   }

   @Override
   protected void createConfigs() throws Exception
   {
      createReplicatedConfigs();
      backupConfig.setClusterPassword("crocodile");
      liveConfig.setIncomingInterceptorClassNames(Arrays.asList(NotifyingInterceptor.class.getName()));
      backupConfig.setSecurityEnabled(true);
      liveConfig.setSecurityEnabled(true);
   }

   @Override
   protected TransportConfiguration getAcceptorTransportConfiguration(boolean live)
   {
      return TransportConfigurationUtils.getInVMAcceptor(live);
   }

   @Override
   protected TransportConfiguration getConnectorTransportConfiguration(boolean live)
   {
      return TransportConfigurationUtils.getInVMConnector(live);
   }

   public static final class NotifyingInterceptor implements Interceptor
   {

      @Override
      public boolean intercept(Packet packet, RemotingConnection connection) throws HornetQException
      {
         if (packet.getType() == PacketImpl.BACKUP_REGISTRATION)
         {
            latch.countDown();
         }
         else if (packet.getType() == PacketImpl.CLUSTER_CONNECT)
         {
            latch.countDown();
         }
         return true;
      }
   }
}
