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
package org.apache.activemq.core.protocol.core.impl.wireformat;

import org.apache.activemq.api.core.HornetQBuffer;
import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.core.protocol.core.impl.PacketImpl;

/**
 * Registers a given backup-server as the replicating backup of a live server (i.e. a regular
 * HornetQ).
 * <p>
 * If it succeeds the backup will start synchronization of its state with the new backup node, and
 * replicating any new data. If it fails the backup server will receive a message indicating
 * failure, and should shutdown.
 * @see BackupReplicationStartFailedMessage
 */
public final class BackupRegistrationMessage extends PacketImpl
{

   private TransportConfiguration connector;
   private String clusterUser;
   private String clusterPassword;
   private boolean backupWantsFailBack;

   public BackupRegistrationMessage(TransportConfiguration tc, String user, String password, boolean backupWantsFailBack)
   {
      this();
      connector = tc;
      clusterUser = user;
      clusterPassword = password;
      this.backupWantsFailBack = backupWantsFailBack;
   }

   public BackupRegistrationMessage()
   {
      super(BACKUP_REGISTRATION);
   }

   public TransportConfiguration getConnector()
   {
      return connector;
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeString(clusterUser);
      buffer.writeString(clusterPassword);
      buffer.writeBoolean(backupWantsFailBack);
      connector.encode(buffer);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      clusterUser = buffer.readString();
      clusterPassword = buffer.readString();
      backupWantsFailBack = buffer.readBoolean();
      connector = new TransportConfiguration();
      connector.decode(buffer);
   }

   public String getClusterUser()
   {
      return clusterUser;
   }

   public String getClusterPassword()
   {
      return clusterPassword;
   }

   public boolean isFailBackRequest()
   {
      return backupWantsFailBack;
   }

   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + (backupWantsFailBack ? 1231 : 1237);
      result = prime * result + ((clusterPassword == null) ? 0 : clusterPassword.hashCode());
      result = prime * result + ((clusterUser == null) ? 0 : clusterUser.hashCode());
      result = prime * result + ((connector == null) ? 0 : connector.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj)
   {
      if (this == obj)
         return true;
      if (!super.equals(obj))
         return false;
      if (!(obj instanceof BackupRegistrationMessage))
         return false;
      BackupRegistrationMessage other = (BackupRegistrationMessage)obj;
      if (backupWantsFailBack != other.backupWantsFailBack)
         return false;
      if (clusterPassword == null)
      {
         if (other.clusterPassword != null)
            return false;
      }
      else if (!clusterPassword.equals(other.clusterPassword))
         return false;
      if (clusterUser == null)
      {
         if (other.clusterUser != null)
            return false;
      }
      else if (!clusterUser.equals(other.clusterUser))
         return false;
      if (connector == null)
      {
         if (other.connector != null)
            return false;
      }
      else if (!connector.equals(other.connector))
         return false;
      return true;
   }
}
