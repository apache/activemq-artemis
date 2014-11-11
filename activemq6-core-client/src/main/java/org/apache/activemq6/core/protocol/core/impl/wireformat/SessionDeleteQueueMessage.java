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
package org.apache.activemq6.core.protocol.core.impl.wireformat;

import org.apache.activemq6.api.core.HornetQBuffer;
import org.apache.activemq6.api.core.SimpleString;
import org.apache.activemq6.core.protocol.core.impl.PacketImpl;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>

 */
public class SessionDeleteQueueMessage extends PacketImpl
{
   private SimpleString queueName;

   public SessionDeleteQueueMessage(final SimpleString queueName)
   {
      super(DELETE_QUEUE);

      this.queueName = queueName;
   }

   public SessionDeleteQueueMessage()
   {
      super(DELETE_QUEUE);
   }

   @Override
   public String toString()
   {
      StringBuffer buff = new StringBuffer(getParentString());
      buff.append(", queueName=" + queueName);
      buff.append("]");
      return buff.toString();
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
      if (!(obj instanceof SessionDeleteQueueMessage))
         return false;
      SessionDeleteQueueMessage other = (SessionDeleteQueueMessage)obj;
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
