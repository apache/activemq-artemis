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
import org.apache.activemq.api.core.HornetQException;
import org.apache.activemq.api.core.HornetQExceptionType;
import org.apache.activemq.core.protocol.core.impl.PacketImpl;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class HornetQExceptionMessage extends PacketImpl
{

   private HornetQException exception;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public HornetQExceptionMessage(final HornetQException exception)
   {
      super(EXCEPTION);

      this.exception = exception;
   }

   public HornetQExceptionMessage()
   {
      super(EXCEPTION);
   }

   // Public --------------------------------------------------------

   @Override
   public boolean isResponse()
   {
      return true;
   }

   public HornetQException getException()
   {
      return exception;
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeInt(exception.getType().getCode());
      buffer.writeNullableString(exception.getMessage());
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      int code = buffer.readInt();
      String msg = buffer.readNullableString();

      exception = HornetQExceptionType.createException(code, msg);
   }

   @Override
   public String toString()
   {
      return getParentString() + ", exception= " + exception + "]";
   }

   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + ((exception == null) ? 0 : exception.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj)
   {
      if (this == obj)
      {
         return true;
      }
      if (!super.equals(obj))
      {
         return false;
      }
      if (!(obj instanceof HornetQExceptionMessage))
      {
         return false;
      }
      HornetQExceptionMessage other = (HornetQExceptionMessage)obj;
      if (exception == null)
      {
         if (other.exception != null)
         {
            return false;
         }
      }
      else if (!exception.equals(other.exception))
      {
         return false;
      }
      return true;
   }
}
