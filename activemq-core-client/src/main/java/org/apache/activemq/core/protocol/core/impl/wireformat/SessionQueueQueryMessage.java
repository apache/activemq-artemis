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
import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.core.protocol.core.impl.PacketImpl;

/**
 *
 * A SessionQueueQueryMessage
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionQueueQueryMessage extends PacketImpl
{
   private SimpleString queueName;

   public SessionQueueQueryMessage(final SimpleString queueName)
   {
      super(SESS_QUEUEQUERY);

      this.queueName = queueName;
   }

   public SessionQueueQueryMessage()
   {
      super(SESS_QUEUEQUERY);
   }

   public SimpleString getQueueName()
   {
      return queueName;
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeSimpleString(queueName);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      queueName = buffer.readSimpleString();
   }

   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + ((queueName == null) ? 0 : queueName.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj)
   {
      if (this == obj)
         return true;
      if (!super.equals(obj))
         return false;
      if (!(obj instanceof SessionQueueQueryMessage))
         return false;
      SessionQueueQueryMessage other = (SessionQueueQueryMessage)obj;
      if (queueName == null)
      {
         if (other.queueName != null)
            return false;
      }
      else if (!queueName.equals(other.queueName))
         return false;
      return true;
   }
}
