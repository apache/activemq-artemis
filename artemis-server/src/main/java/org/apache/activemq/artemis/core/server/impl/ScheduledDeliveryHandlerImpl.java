/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.core.server.impl;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.ScheduledDeliveryHandler;
import org.jboss.logging.Logger;

/**
 * Handles scheduling deliveries to a queue at the correct time.
 */
public class ScheduledDeliveryHandlerImpl implements ScheduledDeliveryHandler {

   private static final Logger logger = Logger.getLogger(ScheduledDeliveryHandlerImpl.class);

   private final ScheduledExecutorService scheduledExecutor;

   private final Map<Long, Runnable> runnables = new ConcurrentHashMap<>();

   // This contains RefSchedules which are delegates to the real references
   // just adding some information to keep it in order accordingly to the initial operations
   private final TreeSet<RefScheduled> scheduledReferences = new TreeSet<>(new MessageReferenceComparator());

   public ScheduledDeliveryHandlerImpl(final ScheduledExecutorService scheduledExecutor) {
      this.scheduledExecutor = scheduledExecutor;
   }

   @Override
   public boolean checkAndSchedule(final MessageReference ref, final boolean tail) {
      long deliveryTime = ref.getScheduledDeliveryTime();

      if (deliveryTime > 0 && scheduledExecutor != null) {
         if (logger.isTraceEnabled()) {
            logger.trace("Scheduling delivery for " + ref + " to occur at " + deliveryTime);
         }

         addInPlace(deliveryTime, ref, tail);

         scheduleDelivery(deliveryTime);

         return true;
      }
      return false;
   }

   public void addInPlace(final long deliveryTime, final MessageReference ref, final boolean tail) {
      synchronized (scheduledReferences) {
         scheduledReferences.add(new RefScheduled(ref, tail));
      }
   }

   @Override
   public int getScheduledCount() {
      synchronized (scheduledReferences) {
         return scheduledReferences.size();
      }
   }

   @Override
   public List<MessageReference> getScheduledReferences() {
      List<MessageReference> refs = new LinkedList<>();

      synchronized (scheduledReferences) {
         for (RefScheduled ref : scheduledReferences) {
            refs.add(ref.getRef());
         }
      }
      return refs;
   }

   @Override
   public List<MessageReference> cancel(final Filter filter) throws ActiveMQException {
      List<MessageReference> refs = new ArrayList<>();

      synchronized (scheduledReferences) {
         Iterator<RefScheduled> iter = scheduledReferences.iterator();

         while (iter.hasNext()) {
            MessageReference ref = iter.next().getRef();
            if (filter == null || filter.match(ref.getMessage())) {
               iter.remove();
               refs.add(ref);
            }
         }
      }
      return refs;
   }

   @Override
   public MessageReference removeReferenceWithID(final long id) throws ActiveMQException {
      synchronized (scheduledReferences) {
         Iterator<RefScheduled> iter = scheduledReferences.iterator();
         while (iter.hasNext()) {
            MessageReference ref = iter.next().getRef();
            if (ref.getMessage().getMessageID() == id) {
               iter.remove();
               return ref;
            }
         }
      }

      return null;
   }

   private void scheduleDelivery(final long deliveryTime) {
      final long now = System.currentTimeMillis();

      final long delay = deliveryTime - now;

      if (delay < 0) {
         if (logger.isTraceEnabled()) {
            logger.trace("calling another scheduler now as deliverTime " + deliveryTime + " < now=" + now);
         }
         // if delay == 0 we will avoid races between adding the scheduler and finishing it
         ScheduledDeliveryRunnable runnable = new ScheduledDeliveryRunnable(deliveryTime);
         scheduledExecutor.schedule(runnable, 0, TimeUnit.MILLISECONDS);
      } else if (!runnables.containsKey(deliveryTime)) {
         ScheduledDeliveryRunnable runnable = new ScheduledDeliveryRunnable(deliveryTime);

         if (logger.isTraceEnabled()) {
            logger.trace("Setting up scheduler for " + deliveryTime + " with a delay of " + delay + " as now=" + now);
         }

         runnables.put(deliveryTime, runnable);
         scheduledExecutor.schedule(runnable, delay, TimeUnit.MILLISECONDS);
      } else {
         if (logger.isTraceEnabled()) {
            logger.trace("Couldn't make another scheduler as " + deliveryTime + " is already set, now is " + now);
         }
      }
   }

   private class ScheduledDeliveryRunnable implements Runnable {

      long deliveryTime;

      private ScheduledDeliveryRunnable(final long deliveryTime) {
         this.deliveryTime = deliveryTime;
      }

      @Override
      public void run() {
         HashMap<Queue, LinkedList<MessageReference>> refs = new HashMap<>();

         runnables.remove(deliveryTime);

         final long now = System.currentTimeMillis();

         if (now < deliveryTime) {
            // Ohhhh... blame it on the OS
            // on some OSes (so far Windows only) the precision of the scheduled executor could eventually give
            // an executor call earlier than it was supposed...
            // for that reason we will schedule it again so no messages are lost!
            // we can't just assume deliveryTime here as we could deliver earlier than what we are supposed to
            // this is basically a hack to work around an OS or JDK bug!
            if (logger.isTraceEnabled()) {
               logger.trace("Scheduler is working around OS imprecisions on " +
                               "timing and re-scheduling an executor. now=" + now +
                               " and deliveryTime=" + deliveryTime);
            }
            ScheduledDeliveryHandlerImpl.this.scheduleDelivery(deliveryTime);
         }

         if (logger.isTraceEnabled()) {
            logger.trace("Is it " + System.currentTimeMillis() + " now and we are running deliveryTime = " + deliveryTime);
         }

         synchronized (scheduledReferences) {

            Iterator<RefScheduled> iter = scheduledReferences.iterator();
            while (iter.hasNext()) {
               MessageReference reference = iter.next().getRef();
               if (reference.getScheduledDeliveryTime() > now) {
                  // We will delivery as long as there are messages to be delivered
                  break;
               }

               iter.remove();

               reference.setScheduledDeliveryTime(0);

               LinkedList<MessageReference> references = refs.get(reference.getQueue());

               if (references == null) {
                  references = new LinkedList<>();
                  refs.put(reference.getQueue(), references);
               }

               if (logger.isTraceEnabled()) {
                  logger.trace("sending message " + reference + " to delivery, deliveryTime =  " + deliveryTime);
               }

               references.addFirst(reference);
            }
            if (logger.isTraceEnabled()) {
               logger.trace("Finished loop on deliveryTime = " + deliveryTime);
            }
         }

         for (Map.Entry<Queue, LinkedList<MessageReference>> entry : refs.entrySet()) {

            Queue queue = entry.getKey();
            LinkedList<MessageReference> list = entry.getValue();
            if (logger.isTraceEnabled()) {
               logger.trace("Delivering " + list.size() + " elements on list to queue " + queue);
            }
            queue.addHead(list, true);
         }

         // Just to speed up GC
         refs.clear();
      }
   }

   // We need a treeset ordered, but we need to order tail operations as well.
   // So, this will serve as a delegate to the object
   class RefScheduled {

      private final MessageReference ref;
      private final boolean tail;

      RefScheduled(MessageReference ref, boolean tail) {
         this.ref = ref;
         this.tail = tail;
      }

      public MessageReference getRef() {
         return ref;
      }

      public boolean isTail() {
         return tail;
      }

   }

   static class MessageReferenceComparator implements Comparator<RefScheduled> {

      @Override
      public int compare(RefScheduled ref1, RefScheduled ref2) {
         long diff = ref1.getRef().getScheduledDeliveryTime() - ref2.getRef().getScheduledDeliveryTime();

         if (diff < 0L) {
            return -1;
         }
         if (diff > 0L) {
            return 1;
         }

         // Even if ref1 and ref2 have the same delivery time, we only want to return 0 if they are identical
         if (ref1 == ref2) {
            return 0;
         } else {

            if (ref1.isTail() && !ref2.isTail()) {
               return 1;
            } else if (!ref1.isTail() && ref2.isTail()) {
               return -1;
            }
            if (!ref1.isTail() && !ref2.isTail()) {
               return -1;
            } else {
               return 1;
            }
         }
      }
   }

}
