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
package org.apache.activemq.artemis.core.protocol.openwire.amq;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.client.impl.ClientConsumerImpl;
import org.apache.activemq.artemis.core.protocol.openwire.OpenWireMessageConverter;
import org.apache.activemq.artemis.core.protocol.openwire.OpenWireUtil;
import org.apache.activemq.artemis.core.server.QueueQueryResult;
import org.apache.activemq.artemis.core.server.ServerMessage;
import org.apache.activemq.artemis.core.server.SlowConsumerDetectionListener;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.command.ConsumerControl;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.MessagePull;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.wireformat.WireFormat;

public class AMQConsumer {
   private AMQSession session;
   private org.apache.activemq.command.ActiveMQDestination openwireDestination;
   private ConsumerInfo info;
   private final ScheduledExecutorService scheduledPool;
   private long nativeId = -1;

   private int prefetchSize;
   private AtomicInteger windowAvailable;
   private final java.util.Queue<MessageInfo> deliveringRefs = new ConcurrentLinkedQueue<>();
   private long messagePullSequence = 0;
   private MessagePullHandler messagePullHandler;

   public AMQConsumer(AMQSession amqSession,
                      org.apache.activemq.command.ActiveMQDestination d,
                      ConsumerInfo info,
                      ScheduledExecutorService scheduledPool) {
      this.session = amqSession;
      this.openwireDestination = d;
      this.info = info;
      this.scheduledPool = scheduledPool;
      this.prefetchSize = info.getPrefetchSize();
      this.windowAvailable = new AtomicInteger(prefetchSize);
      if (prefetchSize == 0) {
         messagePullHandler = new MessagePullHandler();
      }
   }

   public void init(SlowConsumerDetectionListener slowConsumerDetectionListener, long nativeId) throws Exception {
      this.nativeId = nativeId;
      AMQServerConsumer serverConsumer = createServerConsumer(info, slowConsumerDetectionListener);
      serverConsumer.setAmqConsumer(this);
   }


   private AMQServerConsumer createServerConsumer(ConsumerInfo info, SlowConsumerDetectionListener slowConsumerDetectionListener) throws Exception {

      SimpleString selector = info.getSelector() == null ? null : new SimpleString(info.getSelector());

      String physicalName = OpenWireUtil.convertWildcard(openwireDestination.getPhysicalName());

      SimpleString address;

      if (openwireDestination.isTopic()) {
         address = new SimpleString("jms.topic." + physicalName);

         SimpleString queueName = createTopicSubscription(info.isDurable(), info.getClientId(), physicalName, info.getSubscriptionName(), selector, address);

         AMQServerConsumer serverConsumer = (AMQServerConsumer) session.getCoreSession().createConsumer(nativeId, queueName, null, info.isBrowser(), false, -1);
         serverConsumer.setlowConsumerDetection(slowConsumerDetectionListener);
         return serverConsumer;
      }
      else {
         SimpleString queueName = new SimpleString("jms.queue." + physicalName);
         AMQServerConsumer serverConsumer = (AMQServerConsumer) session.getCoreSession().createConsumer(nativeId, queueName, selector, info.isBrowser(), false, -1);
         serverConsumer.setlowConsumerDetection(slowConsumerDetectionListener);
         AddressSettings addrSettings = session.getCoreServer().getAddressSettingsRepository().getMatch(queueName.toString());
         if (addrSettings != null) {
            //see PolicyEntry
            if (info.getPrefetchSize() != 0 && addrSettings.getQueuePrefetch() == 0) {
               //sends back a ConsumerControl
               ConsumerControl cc = new ConsumerControl();
               cc.setConsumerId(info.getConsumerId());
               cc.setPrefetch(0);
               session.getConnection().dispatch(cc);
            }
         }

         return serverConsumer;

      }

   }

   private SimpleString createTopicSubscription(boolean isDurable,
                                                String clientID,
                                                String physicalName,
                                                String subscriptionName,
                                                SimpleString selector,
                                                SimpleString address) throws Exception {

      SimpleString queueName;

      if (isDurable) {
         queueName = new SimpleString(org.apache.activemq.artemis.jms.client.ActiveMQDestination.createQueueNameForDurableSubscription(true, clientID, subscriptionName));
         QueueQueryResult result = session.getCoreSession().executeQueueQuery(queueName);
         if (result.isExists()) {
            // Already exists
            if (result.getConsumerCount() > 0) {
               throw new IllegalStateException("Cannot create a subscriber on the durable subscription since it already has subscriber(s)");
            }

            SimpleString oldFilterString = result.getFilterString();

            boolean selectorChanged = selector == null && oldFilterString != null || oldFilterString == null && selector != null || oldFilterString != null && selector != null && !oldFilterString.equals(selector);

            SimpleString oldTopicName = result.getAddress();

            boolean topicChanged = !oldTopicName.equals(address);

            if (selectorChanged || topicChanged) {
               // Delete the old durable sub
               session.getCoreSession().deleteQueue(queueName);

               // Create the new one
               session.getCoreSession().createQueue(address, queueName, selector, false, true);
            }
         }
         else {
            session.getCoreSession().createQueue(address, queueName, selector, false, true);
         }
      }
      else {
         queueName = new SimpleString(UUID.randomUUID().toString());

         session.getCoreSession().createQueue(address, queueName, selector, true, false);

      }

      return queueName;
   }



   public long getNativeId() {
      return this.nativeId;
   }

   public ConsumerId getId() {
      return info.getConsumerId();
   }

   public WireFormat getMarshaller() {
      return this.session.getMarshaller();
   }

   public void acquireCredit(int n) throws Exception {
      boolean promptDelivery = windowAvailable.get() == 0;
      if (windowAvailable.get() < prefetchSize) {
         this.windowAvailable.addAndGet(n);
      }
      if (promptDelivery) {
         session.getCoreSession().promptDelivery(nativeId);
      }
   }

   public int handleDeliver(ServerMessage message, int deliveryCount) {
      MessageDispatch dispatch;
      try {
         if (messagePullHandler != null && !messagePullHandler.checkForcedConsumer(message)) {
            return 0;
         }
         //decrement deliveryCount as AMQ client tends to add 1.
         dispatch = OpenWireMessageConverter.createMessageDispatch(message, deliveryCount - 1, this);
         int size = dispatch.getMessage().getSize();
         this.deliveringRefs.add(new MessageInfo(dispatch.getMessage().getMessageId(), message.getMessageID(), size));
         session.deliverMessage(dispatch);
         windowAvailable.decrementAndGet();
         return size;
      }
      catch (IOException e) {
         return 0;
      }
      catch (Throwable t) {
         return 0;
      }
   }

   public void handleDeliverNullDispatch() {
      MessageDispatch md = new MessageDispatch();
      md.setConsumerId(getId());
      md.setDestination(openwireDestination);
      session.deliverMessage(md);
      windowAvailable.decrementAndGet();
   }

   public void acknowledge(MessageAck ack) throws Exception {
      MessageId first = ack.getFirstMessageId();
      MessageId lastm = ack.getLastMessageId();
      TransactionId tid = ack.getTransactionId();
      boolean isLocalTx = (tid != null) && tid.isLocalTransaction();
      boolean single = lastm.equals(first);

      MessageInfo mi = null;
      int n = 0;

      if (ack.isIndividualAck()) {
         Iterator<MessageInfo> iter = deliveringRefs.iterator();
         while (iter.hasNext()) {
            mi = iter.next();
            if (mi.amqId.equals(lastm)) {
               n++;
               if (!isLocalTx) {
                  iter.remove();
                  session.getCoreSession().individualAcknowledge(nativeId, mi.nativeId);
               }
               else {
                  mi.setLocalAcked(true);
               }
               if (tid == null) {
                  session.getCoreSession().commit();
               }
               break;
            }
         }
      }
      else if (ack.isRedeliveredAck()) {
         //client tells that this message is for redlivery.
         //do nothing until poisoned.
         n = ack.getMessageCount();
      }
      else if (ack.isPoisonAck()) {
         //send to dlq
         Iterator<MessageInfo> iter = deliveringRefs.iterator();
         boolean firstFound = false;
         while (iter.hasNext()) {
            mi = iter.next();
            if (mi.amqId.equals(first)) {
               n++;
               iter.remove();
               session.getCoreSession().moveToDeadLetterAddress(nativeId, mi.nativeId, ack.getPoisonCause());
               session.getCoreSession().commit();
               if (single) {
                  break;
               }
               firstFound = true;
            }
            else if (firstFound || first == null) {
               n++;
               iter.remove();
               session.getCoreSession().moveToDeadLetterAddress(nativeId, mi.nativeId, ack.getPoisonCause());
               session.getCoreSession().commit();
               if (mi.amqId.equals(lastm)) {
                  break;
               }
            }
         }
      }
      else if (ack.isDeliveredAck() || ack.isExpiredAck()) {
         //ToDo: implement with tests
         n = ack.getMessageCount();
      }
      else {
         Iterator<MessageInfo> iter = deliveringRefs.iterator();
         boolean firstFound = false;
         while (iter.hasNext()) {
            MessageInfo ami = iter.next();
            if (ami.amqId.equals(first)) {
               n++;
               if (!isLocalTx) {
                  iter.remove();
               }
               else {
                  ami.setLocalAcked(true);
               }
               if (single) {
                  mi = ami;
                  break;
               }
               firstFound = true;
            }
            else if (firstFound || first == null) {
               n++;
               if (!isLocalTx) {
                  iter.remove();
               }
               else {
                  ami.setLocalAcked(true);
               }
               if (ami.amqId.equals(lastm)) {
                  mi = ami;
                  break;
               }
            }
         }
         if (mi != null && !isLocalTx) {
            session.getCoreSession().acknowledge(nativeId, mi.nativeId);
         }
      }

      acquireCredit(n);
   }

   public void browseFinished() {
      MessageDispatch md = new MessageDispatch();
      md.setConsumerId(info.getConsumerId());
      md.setMessage(null);
      md.setDestination(null);

      session.deliverMessage(md);
   }

   //this is called before session commit a local tx
   public void finishTx() throws Exception {
      MessageInfo lastMi = null;

      MessageInfo mi = null;
      Iterator<MessageInfo> iter = deliveringRefs.iterator();
      while (iter.hasNext()) {
         mi = iter.next();
         if (mi.isLocalAcked()) {
            iter.remove();
            lastMi = mi;
         }
      }

      if (lastMi != null) {
         session.getCoreSession().acknowledge(nativeId, lastMi.nativeId);
      }
   }

   public void rollbackTx(Set<Long> acked) throws Exception {
      MessageInfo lastMi = null;

      MessageInfo mi = null;
      Iterator<MessageInfo> iter = deliveringRefs.iterator();
      while (iter.hasNext()) {
         mi = iter.next();
         if (mi.isLocalAcked()) {
            acked.add(mi.nativeId);
            lastMi = mi;
         }
      }

      if (lastMi != null) {
         session.getCoreSession().acknowledge(nativeId, lastMi.nativeId);
      }
   }

   public ConsumerInfo getInfo() {
      return info;
   }

   public boolean hasCredits() {
      return windowAvailable.get() > 0;
   }

   public void processMessagePull(MessagePull messagePull) throws Exception {
      windowAvailable.incrementAndGet();

      if (messagePullHandler != null) {
         messagePullHandler.nextSequence(messagePullSequence++, messagePull.getTimeout());
      }
   }

   public void removeConsumer() throws Exception {
      session.removeConsumer(nativeId);
   }

   public org.apache.activemq.command.ActiveMQDestination getOpenwireDestination() {
      return openwireDestination;
   }

   public void setPrefetchSize(int prefetchSize) {
      this.prefetchSize = prefetchSize;
      this.windowAvailable.set(prefetchSize);
      this.info.setPrefetchSize(prefetchSize);
      if (this.prefetchSize > 0) {
         session.getCoreSession().promptDelivery(nativeId);
      }
   }

   /**
    * The MessagePullHandler is used with slow consumer policies.
    * */
   private class MessagePullHandler {

      private long next = -1;
      private long timeout;
      private CountDownLatch latch = new CountDownLatch(1);
      private ScheduledFuture<?> messagePullFuture;

      public void nextSequence(long next, long timeout) throws Exception {
         this.next = next;
         this.timeout = timeout;
         latch = new CountDownLatch(1);
         session.getCoreSession().forceConsumerDelivery(nativeId, messagePullSequence);
         //if we are 0 timeout or less we need to wait to get either the forced message or a real message.
         if (timeout <= 0) {
            latch.await(10, TimeUnit.SECONDS);
            //this means we have received no message just the forced delivery message
            if (this.next >= 0) {
               handleDeliverNullDispatch();
            }
         }
      }

      public boolean checkForcedConsumer(ServerMessage message) {
         if (message.containsProperty(ClientConsumerImpl.FORCED_DELIVERY_MESSAGE)) {
            System.out.println("MessagePullHandler.checkForcedConsumer");
            if (next >= 0) {
               if (timeout <= 0) {
                  latch.countDown();
               }
               else {
                  messagePullFuture = scheduledPool.schedule(new Runnable() {
                     @Override
                     public void run() {
                        if (next >= 0) {
                           handleDeliverNullDispatch();
                        }
                     }
                  }, timeout, TimeUnit.MILLISECONDS);
               }
            }
            return false;
         }
         else {
            next = -1;
            if (messagePullFuture != null) {
               messagePullFuture.cancel(true);
            }
            latch.countDown();
            return true;
         }
      }
   }
}
