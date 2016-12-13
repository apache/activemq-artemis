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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.ActiveMQQueueExistsException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.client.impl.ClientConsumerImpl;
import org.apache.activemq.artemis.core.protocol.openwire.OpenWireMessageConverter;
import org.apache.activemq.artemis.core.protocol.openwire.util.OpenWireUtil;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.QueueQueryResult;
import org.apache.activemq.artemis.core.server.RoutingType;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.ServerMessage;
import org.apache.activemq.artemis.core.server.SlowConsumerDetectionListener;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.command.ConsumerControl;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.MessagePull;
import org.apache.activemq.command.RemoveInfo;
import org.apache.activemq.wireformat.WireFormat;

public class AMQConsumer {
   private AMQSession session;
   private org.apache.activemq.command.ActiveMQDestination openwireDestination;
   private ConsumerInfo info;
   private final ScheduledExecutorService scheduledPool;
   private ServerConsumer serverConsumer;

   private int prefetchSize;
   private AtomicInteger currentWindow;
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
      this.currentWindow = new AtomicInteger(prefetchSize);
      if (prefetchSize == 0) {
         messagePullHandler = new MessagePullHandler();
      }
   }

   public void init(SlowConsumerDetectionListener slowConsumerDetectionListener, long nativeId) throws Exception {

      SimpleString selector = info.getSelector() == null ? null : new SimpleString(info.getSelector());

      String physicalName = OpenWireUtil.convertWildcard(openwireDestination.getPhysicalName());

      SimpleString address;

      if (openwireDestination.isTopic()) {
         if (openwireDestination.isTemporary()) {
            address = new SimpleString(physicalName);
         } else {
            address = new SimpleString(physicalName);
         }

         SimpleString queueName = createTopicSubscription(info.isDurable(), info.getClientId(), physicalName, info.getSubscriptionName(), selector, address);

         serverConsumer = session.getCoreSession().createConsumer(nativeId, queueName, null, info.isBrowser(), false, -1);
         serverConsumer.setlowConsumerDetection(slowConsumerDetectionListener);
      } else {
         SimpleString queueName = new SimpleString(OpenWireUtil.convertWildcard(openwireDestination.getPhysicalName()));
         try {
            session.getCoreServer().createQueue(queueName, RoutingType.ANYCAST, queueName, null, true, false);
         } catch (ActiveMQQueueExistsException e) {
            // ignore
         }
         serverConsumer = session.getCoreSession().createConsumer(nativeId, queueName, selector, info.isBrowser(), false, -1);
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

      }

      serverConsumer.setProtocolData(this);
   }

   private SimpleString createTopicSubscription(boolean isDurable,
                                                String clientID,
                                                String physicalName,
                                                String subscriptionName,
                                                SimpleString selector,
                                                SimpleString address) throws Exception {

      SimpleString queueName;

      AddressInfo addressInfo = session.getCoreServer().getAddressInfo(address);
      if (addressInfo != null) {
         addressInfo.addRoutingType(RoutingType.MULTICAST);
      }
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
               session.getCoreSession().createQueue(address, queueName, RoutingType.MULTICAST, selector, false, true);
            }
         } else {
            session.getCoreSession().createQueue(address, queueName, RoutingType.MULTICAST, selector, false, true);
         }
      } else {
         queueName = new SimpleString(UUID.randomUUID().toString());

         session.getCoreSession().createQueue(address, queueName, RoutingType.MULTICAST, selector, true, false);

      }

      return queueName;
   }

   public ConsumerId getId() {
      return info.getConsumerId();
   }

   public WireFormat getMarshaller() {
      return this.session.getMarshaller();
   }

   public void acquireCredit(int n) throws Exception {
      if (messagePullHandler != null) {
         //don't acquire any credits when the pull handler controls it!!
         return;
      }
      int oldwindow = currentWindow.getAndAdd(n);

      boolean promptDelivery = oldwindow < prefetchSize;

      if (promptDelivery) {
         serverConsumer.promptDelivery();
      }

   }

   public int handleDeliver(MessageReference reference, ServerMessage message, int deliveryCount) {
      MessageDispatch dispatch;
      try {
         if (messagePullHandler != null && !messagePullHandler.checkForcedConsumer(message)) {
            return 0;
         }

         dispatch = OpenWireMessageConverter.createMessageDispatch(reference, message, this);
         int size = dispatch.getMessage().getSize();
         reference.setProtocolData(dispatch.getMessage().getMessageId());
         session.deliverMessage(dispatch);
         currentWindow.decrementAndGet();
         return size;
      } catch (IOException e) {
         ActiveMQServerLogger.LOGGER.warn("Error during message dispatch", e);
         return 0;
      } catch (Throwable t) {
         ActiveMQServerLogger.LOGGER.warn("Error during message dispatch", t);
         return 0;
      }
   }

   public void handleDeliverNullDispatch() {
      MessageDispatch md = new MessageDispatch();
      md.setConsumerId(getId());
      md.setDestination(openwireDestination);
      session.deliverMessage(md);
   }

   /**
    * The acknowledgement in openwire is done based on intervals.
    * We will iterate through the list of delivering messages at {@link ServerConsumer#getDeliveringReferencesBasedOnProtocol(boolean, Object, Object)}
    * and add those to the Transaction.
    * Notice that we will start a new transaction on the cases where there is no transaction.
    */
   public void acknowledge(MessageAck ack) throws Exception {

      MessageId first = ack.getFirstMessageId();
      MessageId last = ack.getLastMessageId();

      if (first == null) {
         first = last;
      }

      boolean removeReferences = !serverConsumer.isBrowseOnly(); // if it's browse only, nothing to be acked, we just remove the lists

      if (ack.isRedeliveredAck() || ack.isDeliveredAck() || ack.isExpiredAck()) {
         removeReferences = false;
      }

      List<MessageReference> ackList = serverConsumer.getDeliveringReferencesBasedOnProtocol(removeReferences, first, last);

      acquireCredit(ack.getMessageCount());

      if (removeReferences) {

         Transaction originalTX = session.getCoreSession().getCurrentTransaction();
         Transaction transaction;

         if (originalTX == null) {
            transaction = session.getCoreSession().newTransaction();
         } else {
            transaction = originalTX;
         }

         if (ack.isIndividualAck() || ack.isStandardAck()) {
            for (MessageReference ref : ackList) {
               ref.acknowledge(transaction);
            }
         } else if (ack.isPoisonAck()) {
            for (MessageReference ref : ackList) {
               Throwable poisonCause = ack.getPoisonCause();
               if (poisonCause != null) {
                  ref.getMessage().putStringProperty(OpenWireMessageConverter.AMQ_MSG_DLQ_DELIVERY_FAILURE_CAUSE_PROPERTY, poisonCause.toString());
               }
               ref.getQueue().sendToDeadLetterAddress(transaction, ref);
            }
         }

         if (originalTX == null) {
            transaction.commit(true);
         }
      }
      if (ack.isExpiredAck()) {
         //adjust delivering count for expired messages
         this.serverConsumer.getQueue().decDelivering(ackList.size());
      }
   }

   public void browseFinished() {
      MessageDispatch md = new MessageDispatch();
      md.setConsumerId(info.getConsumerId());
      md.setMessage(null);
      md.setDestination(null);

      session.deliverMessage(md);
   }

   public ConsumerInfo getInfo() {
      return info;
   }

   public boolean hasCredits() {
      return currentWindow.get() > 0;
   }

   public void processMessagePull(MessagePull messagePull) throws Exception {
      currentWindow.incrementAndGet();
      if (messagePullHandler != null) {
         messagePullHandler.nextSequence(messagePullSequence++, messagePull.getTimeout());
      }
   }

   public void removeConsumer() throws Exception {
      serverConsumer.close(false);
   }

   public org.apache.activemq.command.ActiveMQDestination getOpenwireDestination() {
      return openwireDestination;
   }

   public void setPrefetchSize(int prefetchSize) {
      this.prefetchSize = prefetchSize;
      this.currentWindow.set(prefetchSize);
      this.info.setPrefetchSize(prefetchSize);
      if (this.prefetchSize > 0) {
         serverConsumer.promptDelivery();
      }
   }

   public boolean updateDeliveryCountAfterCancel(MessageReference ref) {
      long seqId = ref.getMessage().getMessageID();
      long lastDelSeqId = info.getLastDeliveredSequenceId();

      //in activemq5, closing a durable subscription won't close the consumer
      //at broker. Messages will be treated as if being redelivered to
      //the same consumer.
      if (this.info.isDurable() && this.getOpenwireDestination().isTopic()) {
         return true;
      }

      //because delivering count is always one greater than redelivery count
      //we adjust it down before further calculating.
      ref.decrementDeliveryCount();

      // This is a specific rule of the protocol
      if (lastDelSeqId == RemoveInfo.LAST_DELIVERED_UNKNOWN) {
         // this takes care of un-acked messages in non-tx deliveries
         // tx cases are handled by
         // org.apache.activemq.artemis.core.protocol.openwire.OpenWireConnection.CommandProcessor.processRollbackTransaction()
         ref.incrementDeliveryCount();
      }

      return true;
   }

   /**
    * The MessagePullHandler is used with slow consumer policies.
    */
   private class MessagePullHandler {

      private long next = -1;
      private long timeout;
      private CountDownLatch latch = new CountDownLatch(1);
      private ScheduledFuture<?> messagePullFuture;

      public void nextSequence(long next, long timeout) throws Exception {
         this.next = next;
         this.timeout = timeout;
         latch = new CountDownLatch(1);
         serverConsumer.forceDelivery(messagePullSequence);
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
            if (next >= 0) {
               if (timeout <= 0) {
                  latch.countDown();
               } else {
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
         } else {
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
