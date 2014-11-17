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
import org.apache.activemq.core.paging.PagedMessage;
import org.apache.activemq.core.paging.impl.PagedMessageImpl;
import org.apache.activemq.core.protocol.core.impl.PacketImpl;

/**
 * A ReplicationPageWrite
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class ReplicationPageWriteMessage extends PacketImpl
{

   private int pageNumber;

   private PagedMessage pagedMessage;


   public ReplicationPageWriteMessage()
   {
      super(PacketImpl.REPLICATION_PAGE_WRITE);
   }

   public ReplicationPageWriteMessage(final PagedMessage pagedMessage, final int pageNumber)
   {
      this();
      this.pageNumber = pageNumber;
      this.pagedMessage = pagedMessage;
   }

   // Public --------------------------------------------------------

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeInt(pageNumber);
      pagedMessage.encode(buffer);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      pageNumber = buffer.readInt();
      pagedMessage = new PagedMessageImpl();
      pagedMessage.decode(buffer);
   }

   /**
    * @return the pageNumber
    */
   public int getPageNumber()
   {
      return pageNumber;
   }

   /**
    * @return the pagedMessage
    */
   public PagedMessage getPagedMessage()
   {
      return pagedMessage;
   }

   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + pageNumber;
      result = prime * result + ((pagedMessage == null) ? 0 : pagedMessage.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj)
   {
      if (this == obj)
         return true;
      if (!super.equals(obj))
         return false;
      if (getClass() != obj.getClass())
         return false;
      ReplicationPageWriteMessage other = (ReplicationPageWriteMessage)obj;
      if (pageNumber != other.pageNumber)
         return false;
      if (pagedMessage == null)
      {
         if (other.pagedMessage != null)
            return false;
      }
      else if (!pagedMessage.equals(other.pagedMessage))
         return false;
      return true;
   }

}
