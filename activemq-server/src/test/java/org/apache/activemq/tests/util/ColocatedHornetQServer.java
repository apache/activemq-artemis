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
package org.apache.activemq6.tests.util;

import javax.management.MBeanServer;

import org.apache.activemq6.core.asyncio.impl.AsynchronousFileImpl;
import org.apache.activemq6.core.config.Configuration;
import org.apache.activemq6.core.config.impl.FileConfiguration;
import org.apache.activemq6.core.server.HornetQServer;
import org.apache.activemq6.core.server.JournalType;
import org.apache.activemq6.core.server.NodeManager;
import org.apache.activemq6.core.server.impl.AIOFileLockNodeManager;
import org.apache.activemq6.core.server.impl.FileLockNodeManager;
import org.apache.activemq6.core.server.impl.HornetQServerImpl;
import org.apache.activemq6.spi.core.security.HornetQSecurityManager;


public class ColocatedHornetQServer extends HornetQServerImpl
{
   private final NodeManager nodeManagerLive;
   private final NodeManager nodeManagerBackup;
   boolean backup = false;
   public ColocatedHornetQServer backupServer;

   public ColocatedHornetQServer(FileConfiguration fc, HornetQSecurityManager sm, NodeManager nodeManagerLive, NodeManager nodeManagerBackup)
   {
      super(fc, sm);
      this.nodeManagerLive = nodeManagerLive;
      this.nodeManagerBackup = nodeManagerBackup;
   }

   public ColocatedHornetQServer(Configuration backupServerConfiguration, HornetQServer parentServer, NodeManager nodeManagerBackup, NodeManager nodeManagerLive)
   {
      super(backupServerConfiguration, null, null, parentServer);
      this.nodeManagerLive = nodeManagerLive;
      this.nodeManagerBackup = nodeManagerBackup;
   }

   public ColocatedHornetQServer(Configuration configuration, MBeanServer platformMBeanServer, HornetQSecurityManager securityManager,
                                 NodeManager nodeManagerLive, NodeManager nodeManagerBackup)
   {
      super(configuration, platformMBeanServer, securityManager);
      this.nodeManagerLive = nodeManagerLive;
      this.nodeManagerBackup = nodeManagerBackup;
   }


   @Override
   protected NodeManager
   createNodeManager(final String directory, boolean replicatingBackup)
   {
      if (replicatingBackup)
      {
         NodeManager manager;
         if (getConfiguration().getJournalType() == JournalType.ASYNCIO && AsynchronousFileImpl.isLoaded())
         {
            return new AIOFileLockNodeManager(directory, replicatingBackup, getConfiguration().getJournalLockAcquisitionTimeout());
         }
         else
         {
            return new FileLockNodeManager(directory, replicatingBackup, getConfiguration().getJournalLockAcquisitionTimeout());
         }
      }
      else
      {
         if (backup)
         {
            return nodeManagerBackup;
         }
         else
         {
            return nodeManagerLive;
         }
      }
   }

   @Override
   public HornetQServer createBackupServer(Configuration configuration)
   {
      ColocatedHornetQServer backup = new ColocatedHornetQServer(configuration, this, nodeManagerBackup, nodeManagerLive);
      backup.backup = true;
      this.backupServer = backup;
      return backup;
   }
}
