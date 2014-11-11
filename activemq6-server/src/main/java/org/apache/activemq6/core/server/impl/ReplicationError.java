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
package org.apache.activemq6.core.server.impl;

import org.apache.activemq6.api.core.HornetQException;
import org.apache.activemq6.api.core.HornetQInternalErrorException;
import org.apache.activemq6.api.core.Interceptor;
import org.apache.activemq6.core.protocol.core.Packet;
import org.apache.activemq6.core.protocol.core.impl.PacketImpl;
import org.apache.activemq6.core.protocol.core.impl.wireformat.BackupReplicationStartFailedMessage;
import org.apache.activemq6.core.server.HornetQServerLogger;
import org.apache.activemq6.core.server.HornetQServer;
import org.apache.activemq6.core.server.LiveNodeLocator;
import org.apache.activemq6.spi.core.protocol.RemotingConnection;

/**
 * Stops the backup in case of an error at the start of Replication.
 * <p>
 * Using an interceptor for the task to avoid a server reference inside of the 'basic' channel-0
 * handler at {@link org.apache.activemq6.core.protocol.core.impl.HornetQClientProtocolManager.Channel0Handler}. As {@link org.hornetq.core.protocol.core.impl.HornetQClientProtocolManager}
 * is also shipped in the HQ-client JAR (which does not include {@link HornetQServer}).
 */
final class ReplicationError implements Interceptor
{
   private final HornetQServer server;
   private LiveNodeLocator nodeLocator;

   public ReplicationError(HornetQServer server, LiveNodeLocator nodeLocator)
   {
      this.server = server;
      this.nodeLocator = nodeLocator;
   }

   @Override
   public boolean intercept(Packet packet, RemotingConnection connection) throws HornetQException
   {
      if (packet.getType() != PacketImpl.BACKUP_REGISTRATION_FAILED)
         return true;
      BackupReplicationStartFailedMessage message = (BackupReplicationStartFailedMessage) packet;
      switch (message.getRegistrationProblem())
      {
         case ALREADY_REPLICATING:
            tryNext();
            break;
         case AUTHENTICATION:
            failed();
            break;
         case EXCEPTION:
            failed();
            break;
         default:
            failed();

      }
      return false;
   }

   private void failed() throws HornetQInternalErrorException
   {
      HornetQServerLogger.LOGGER.errorRegisteringBackup();
      nodeLocator.notifyRegistrationFailed(false);
   }

   private void tryNext()
   {
      nodeLocator.notifyRegistrationFailed(true);
   }

}
