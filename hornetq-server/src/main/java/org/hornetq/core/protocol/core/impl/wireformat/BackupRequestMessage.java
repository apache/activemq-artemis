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

package org.hornetq.core.protocol.core.impl.wireformat;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.protocol.core.impl.PacketImpl;
import org.hornetq.core.server.cluster.ha.HAPolicy;

public class BackupRequestMessage extends PacketImpl
{
   private int backupSize;
   private SimpleString nodeID;
   private HAPolicy.POLICY_TYPE backupType;
   private String journalDirectory;
   private String bindingsDirectory;
   private String largeMessagesDirectory;
   private String pagingDirectory;


   public BackupRequestMessage()
   {
      super(BACKUP_REQUEST);
   }

   public BackupRequestMessage(int backupSize, String journalDirectory, String bindingsDirectory, String largeMessagesDirectory, String pagingDirectory)
   {
      super(BACKUP_REQUEST);
      this.backupSize = backupSize;
      backupType = HAPolicy.POLICY_TYPE.COLOCATED_SHARED_STORE;
      this.journalDirectory = journalDirectory;
      this.bindingsDirectory = bindingsDirectory;
      this.largeMessagesDirectory = largeMessagesDirectory;
      this.pagingDirectory = pagingDirectory;
   }

   public BackupRequestMessage(int backupSize, SimpleString nodeID)
   {
      super(BACKUP_REQUEST);
      this.backupSize = backupSize;
      this.nodeID = nodeID;
      backupType = HAPolicy.POLICY_TYPE.COLOCATED_REPLICATED;
   }

   @Override
   public void encodeRest(HornetQBuffer buffer)
   {
      super.encodeRest(buffer);
      buffer.writeInt(backupSize);
      buffer.writeByte(backupType.getType());
      if (backupType == HAPolicy.POLICY_TYPE.COLOCATED_SHARED_STORE)
      {
         buffer.writeString(journalDirectory);
         buffer.writeString(bindingsDirectory);
         buffer.writeString(largeMessagesDirectory);
         buffer.writeString(pagingDirectory);
      }
      else
      {
         buffer.writeSimpleString(nodeID);
      }
   }

   @Override
   public void decodeRest(HornetQBuffer buffer)
   {
      super.decodeRest(buffer);
      backupSize = buffer.readInt();
      backupType = HAPolicy.POLICY_TYPE.toBackupType(buffer.readByte());
      if (backupType == HAPolicy.POLICY_TYPE.COLOCATED_SHARED_STORE)
      {
         journalDirectory = buffer.readString();
         bindingsDirectory = buffer.readString();
         largeMessagesDirectory = buffer.readString();
         pagingDirectory = buffer.readString();
      }
      else
      {
         nodeID = buffer.readSimpleString();
      }
   }

   public int getBackupSize()
   {
      return backupSize;
   }

   public SimpleString getNodeID()
   {
      return nodeID;
   }

   public HAPolicy.POLICY_TYPE getBackupType()
   {
      return backupType;
   }

   public String getJournalDirectory()
   {
      return journalDirectory;
   }

   public String getBindingsDirectory()
   {
      return bindingsDirectory;
   }

   public String getLargeMessagesDirectory()
   {
      return largeMessagesDirectory;
   }

   public String getPagingDirectory()
   {
      return pagingDirectory;
   }
}
