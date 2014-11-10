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
package org.hornetq.core.protocol.openwire.amq;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.state.ProducerState;

public class AMQProducerBrokerExchange
{
   private AMQConnectionContext connectionContext;
   private AMQDestination regionDestination;
   private ProducerState producerState;
   private boolean mutable = true;
   private AtomicLong lastSendSequenceNumber = new AtomicLong(-1);
   private boolean auditProducerSequenceIds;
   private boolean isNetworkProducer;
   private final FlowControlInfo flowControlInfo = new FlowControlInfo();

   public AMQProducerBrokerExchange()
   {
   }

   public AMQProducerBrokerExchange copy()
   {
      AMQProducerBrokerExchange rc = new AMQProducerBrokerExchange();
      rc.connectionContext = connectionContext.copy();
      rc.regionDestination = regionDestination;
      rc.producerState = producerState;
      rc.mutable = mutable;
      return rc;
   }

   /**
    * @return the connectionContext
    */
   public AMQConnectionContext getConnectionContext()
   {
      return this.connectionContext;
   }

   /**
    * @param connectionContext
    *           the connectionContext to set
    */
   public void setConnectionContext(AMQConnectionContext connectionContext)
   {
      this.connectionContext = connectionContext;
   }

   /**
    * @return the mutable
    */
   public boolean isMutable()
   {
      return this.mutable;
   }

   /**
    * @param mutable
    *           the mutable to set
    */
   public void setMutable(boolean mutable)
   {
      this.mutable = mutable;
   }

   /**
    * @return the regionDestination
    */
   public AMQDestination getRegionDestination()
   {
      return this.regionDestination;
   }

   /**
    * @param regionDestination
    *           the regionDestination to set
    */
   public void setRegionDestination(AMQDestination regionDestination)
   {
      this.regionDestination = regionDestination;
   }

   /**
    * @return the producerState
    */
   public ProducerState getProducerState()
   {
      return this.producerState;
   }

   /**
    * @param producerState
    *           the producerState to set
    */
   public void setProducerState(ProducerState producerState)
   {
      this.producerState = producerState;
   }

   /**
    * Enforce duplicate suppression using info from persistence adapter
    *
    * @return false if message should be ignored as a duplicate
    */
   public boolean canDispatch(Message messageSend)
   {
      boolean canDispatch = true;
      if (auditProducerSequenceIds && messageSend.isPersistent())
      {
         final long producerSequenceId = messageSend.getMessageId()
               .getProducerSequenceId();
         if (isNetworkProducer)
         {
            // messages are multiplexed on this producer so we need to query the
            // persistenceAdapter
            long lastStoredForMessageProducer = getStoredSequenceIdForMessage(messageSend
                  .getMessageId());
            if (producerSequenceId <= lastStoredForMessageProducer)
            {
               canDispatch = false;
            }
         }
         else if (producerSequenceId <= lastSendSequenceNumber.get())
         {
            canDispatch = false;
            if (messageSend.isInTransaction())
            {
            }
            else
            {
            }
         }
         else
         {
            // track current so we can suppress duplicates later in the stream
            lastSendSequenceNumber.set(producerSequenceId);
         }
      }
      return canDispatch;
   }

   private long getStoredSequenceIdForMessage(MessageId messageId)
   {
      return -1;
   }

   public void setLastStoredSequenceId(long l)
   {
   }

   public void incrementSend()
   {
      flowControlInfo.incrementSend();
   }

   public void blockingOnFlowControl(boolean blockingOnFlowControl)
   {
      flowControlInfo.setBlockingOnFlowControl(blockingOnFlowControl);
   }

   public void incrementTimeBlocked(AMQDestination destination, long timeBlocked)
   {
      flowControlInfo.incrementTimeBlocked(timeBlocked);
   }

   public boolean isBlockedForFlowControl()
   {
      return flowControlInfo.isBlockingOnFlowControl();
   }

   public void resetFlowControl()
   {
      flowControlInfo.reset();
   }

   public long getTotalTimeBlocked()
   {
      return flowControlInfo.getTotalTimeBlocked();
   }

   public int getPercentageBlocked()
   {
      double value = flowControlInfo.getSendsBlocked()
            / flowControlInfo.getTotalSends();
      return (int) value * 100;
   }

   public static class FlowControlInfo
   {
      private AtomicBoolean blockingOnFlowControl = new AtomicBoolean();
      private AtomicLong totalSends = new AtomicLong();
      private AtomicLong sendsBlocked = new AtomicLong();
      private AtomicLong totalTimeBlocked = new AtomicLong();

      public boolean isBlockingOnFlowControl()
      {
         return blockingOnFlowControl.get();
      }

      public void setBlockingOnFlowControl(boolean blockingOnFlowControl)
      {
         this.blockingOnFlowControl.set(blockingOnFlowControl);
         if (blockingOnFlowControl)
         {
            incrementSendBlocked();
         }
      }

      public long getTotalSends()
      {
         return totalSends.get();
      }

      public void incrementSend()
      {
         this.totalSends.incrementAndGet();
      }

      public long getSendsBlocked()
      {
         return sendsBlocked.get();
      }

      public void incrementSendBlocked()
      {
         this.sendsBlocked.incrementAndGet();
      }

      public long getTotalTimeBlocked()
      {
         return totalTimeBlocked.get();
      }

      public void incrementTimeBlocked(long time)
      {
         this.totalTimeBlocked.addAndGet(time);
      }

      public void reset()
      {
         blockingOnFlowControl.set(false);
         totalSends.set(0);
         sendsBlocked.set(0);
         totalTimeBlocked.set(0);

      }
   }

}
