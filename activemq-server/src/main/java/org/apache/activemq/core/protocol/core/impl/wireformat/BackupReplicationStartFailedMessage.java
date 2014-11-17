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
package org.apache.activemq.core.protocol.core.impl.wireformat;

import org.apache.activemq.api.core.HornetQBuffer;
import org.apache.activemq.core.protocol.core.impl.PacketImpl;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Informs the Backup trying to start replicating of an error.
 * @see org.apache.activemq.core.server.impl.ReplicationError
 */
public final class BackupReplicationStartFailedMessage extends PacketImpl
{

   public enum BackupRegistrationProblem
   {
      EXCEPTION(0), AUTHENTICATION(1), ALREADY_REPLICATING(2);

      private static final Map<Integer, BackupRegistrationProblem> TYPE_MAP;

      final int code;

      private BackupRegistrationProblem(int code)
      {
         this.code = code;
      }

      static
      {
         HashMap<Integer, BackupRegistrationProblem> map = new HashMap<Integer, BackupRegistrationProblem>();
         for (BackupRegistrationProblem type : EnumSet.allOf(BackupRegistrationProblem.class))
         {
            map.put(type.code, type);
         }
         TYPE_MAP = Collections.unmodifiableMap(map);
      }
   }

   private BackupRegistrationProblem problem;

   private static BackupRegistrationProblem getType(int type)
   {
      return BackupRegistrationProblem.TYPE_MAP.get(type);
   }

   public BackupReplicationStartFailedMessage(BackupRegistrationProblem registrationProblem)
   {
      super(BACKUP_REGISTRATION_FAILED);
      problem = registrationProblem;
   }

   public BackupReplicationStartFailedMessage()
   {
      super(BACKUP_REGISTRATION_FAILED);
   }

   public BackupRegistrationProblem getRegistrationProblem()
   {
      return problem;
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeInt(problem.code);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      problem = getType(buffer.readInt());
   }

   @Override
   public boolean equals(Object o)
   {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;

      BackupReplicationStartFailedMessage that = (BackupReplicationStartFailedMessage) o;

      if (problem != that.problem) return false;

      return true;
   }

   @Override
   public int hashCode()
   {
      int result = super.hashCode();
      result = 31 * result + (problem != null ? problem.hashCode() : 0);
      return result;
   }
}
