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
package org.apache.activemq.tests.unit.core.server.impl.fakes;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.activemq.core.filter.Filter;
import org.apache.activemq.core.server.Consumer;
import org.apache.activemq.core.server.HandleStatus;
import org.apache.activemq.core.server.MessageReference;

/**
 *
 * A FakeConsumer
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class FakeConsumer implements Consumer
{
   private HandleStatus statusToReturn = HandleStatus.HANDLED;

   private HandleStatus newStatus;

   private int delayCountdown = 0;

   private final LinkedList<MessageReference> references = new LinkedList<MessageReference>();

   private final Filter filter;

   public FakeConsumer()
   {
      filter = null;
   }

   public FakeConsumer(final Filter filter)
   {
      this.filter = filter;
   }

   public Filter getFilter()
   {
      return filter;
   }

   public String debug()
   {
      return toString();
   }

   public synchronized MessageReference waitForNextReference(long timeout)
   {
      while (references.isEmpty() && timeout > 0)
      {
         long start = System.currentTimeMillis();
         try
         {
            wait();
         }
         catch (InterruptedException e)
         {
         }
         timeout -= System.currentTimeMillis() - start;
      }

      if (timeout <= 0)
      {
         throw new IllegalStateException("Timed out waiting for reference");
      }

      return references.removeFirst();
   }

   public synchronized void setStatusImmediate(final HandleStatus newStatus)
   {
      statusToReturn = newStatus;
   }

   public synchronized void setStatusDelayed(final HandleStatus newStatus, final int numReferences)
   {
      this.newStatus = newStatus;

      delayCountdown = numReferences;
   }

   public synchronized List<MessageReference> getReferences()
   {
      return references;
   }

   public synchronized void clearReferences()
   {
      references.clear();
   }

   public synchronized HandleStatus handle(final MessageReference reference)
   {
      if (statusToReturn == HandleStatus.BUSY)
      {
         return HandleStatus.BUSY;
      }

      if (filter != null)
      {
         if (filter.match(reference.getMessage()))
         {
            references.addLast(reference);
            reference.getQueue().referenceHandled();
            notify();

            return HandleStatus.HANDLED;
         }
         else
         {
            return HandleStatus.NO_MATCH;
         }
      }

      if (newStatus != null)
      {
         if (delayCountdown == 0)
         {
            statusToReturn = newStatus;

            newStatus = null;
         }
         else
         {
            delayCountdown--;
         }
      }

      if (statusToReturn == HandleStatus.HANDLED)
      {
         reference.getQueue().referenceHandled();
         references.addLast(reference);
         notify();
      }

      return statusToReturn;
   }

   @Override
   public void proceedDeliver(MessageReference ref) throws Exception
   {
      // no op
   }

   @Override
   public String toManagementString()
   {
      return toString();
   }

   @Override
   public void disconnect()
   {
      //To change body of implemented methods use File | Settings | File Templates.
   }

   public List<MessageReference>  getDeliveringMessages()
   {
      return Collections.emptyList();
   }



}
