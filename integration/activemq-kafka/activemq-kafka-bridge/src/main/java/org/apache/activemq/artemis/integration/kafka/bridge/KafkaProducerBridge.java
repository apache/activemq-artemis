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
package org.apache.activemq.artemis.integration.kafka.bridge;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.filter.impl.FilterImpl;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.ConnectorService;
import org.apache.activemq.artemis.core.server.Consumer;
import org.apache.activemq.artemis.core.server.HandleStatus;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.integration.kafka.protocol.core.CoreMessageSerializer;
import org.apache.activemq.artemis.utils.ConfigurationHelper;
import org.apache.activemq.artemis.utils.ReusableLatch;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringSerializer;

class KafkaProducerBridge implements Consumer, ConnectorService {

   private final String connectorName;

   private final String queueName;

   private final String topicName;

   private final PostOffice postOffice;

   private Queue queue = null;

   private Filter filter = null;

   private String filterString;

   private AtomicBoolean isStarted = new AtomicBoolean();

   private boolean isConnected = false;

   private Producer<String, Message> kafkaProducer;

   private Map<String, Object> configuration;

   private long sequentialID;

   private final ReusableLatch pendingAcks = new ReusableLatch(0);

   private final java.util.Map<Long, MessageReference> refs = new LinkedHashMap<>();

   private final ScheduledExecutorService scheduledExecutorService;

   private final int retryAttempts;

   private final long retryInterval;

   private final double retryMultiplier;

   private final long retryMaxInterval;

   private final AtomicInteger retryCount = new AtomicInteger();

   private final AtomicBoolean awaitingReconnect = new AtomicBoolean();

   private final KafkaProducerFactory<String, Message> kafkaProducerFactory;


   KafkaProducerBridge(String connectorName, KafkaProducerFactory<String, Message> kafkaProducerFactory, Map<String, Object> configuration, StorageManager storageManager, PostOffice postOffice, ScheduledExecutorService scheduledExecutorService) {
      this.sequentialID = storageManager.generateID();

      this.kafkaProducerFactory = kafkaProducerFactory;

      this.connectorName = connectorName;
      this.queueName = ConfigurationHelper.getStringProperty(KafkaConstants.QUEUE_NAME, null, configuration);
      this.topicName = ConfigurationHelper.getStringProperty(KafkaConstants.TOPIC_NAME, null, configuration);

      this.retryAttempts = ConfigurationHelper.getIntProperty(KafkaConstants.RETRY_ATTEMPTS_NAME, -1, configuration);
      this.retryInterval = ConfigurationHelper.getLongProperty(KafkaConstants.RETRY_INTERVAL_NAME, 2000, configuration);
      this.retryMultiplier = ConfigurationHelper.getDoubleProperty(KafkaConstants.RETRY_MULTIPLIER_NAME, 2, configuration);
      this.retryMaxInterval = ConfigurationHelper.getLongProperty(KafkaConstants.RETRY_MAX_INTERVAL_NAME, 30000, configuration);

      this.filterString = ConfigurationHelper.getStringProperty(KafkaConstants.FILTER_STRING, null, configuration);

      if (!configuration.containsKey(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)) {
         configuration.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
      }
      if (!configuration.containsKey(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)) {
         configuration.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CoreMessageSerializer.class.getName());
      }
      if (!configuration.containsKey(ProducerConfig.PARTITIONER_CLASS_CONFIG)) {
         configuration.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, GroupIdPartitioner.class.getName());
      }

      this.postOffice = postOffice;
      this.configuration = configuration;
      this.scheduledExecutorService = scheduledExecutorService;
   }

   private Map<String, Object> kafkaConfig(Map<String, Object> configuration) {
      Map<String, Object> filteredConfig = new HashMap<>(configuration);
      KafkaConstants.OPTIONAL_OUTGOING_CONNECTOR_KEYS.forEach(filteredConfig::remove);
      KafkaConstants.REQUIRED_OUTGOING_CONNECTOR_KEYS.forEach(filteredConfig::remove);
      return filteredConfig;
   }

   @Override
   public void start() throws Exception {
      synchronized (this) {
         if (this.isStarted.get()) {
            return;
         }
         if (this.connectorName == null || this.connectorName.trim().equals("")) {
            throw new Exception("invalid connector name: " + this.connectorName);
         }

         if (this.topicName == null || this.topicName.trim().equals("")) {
            throw new Exception("invalid topic name: " + topicName);
         }

         if (this.queueName == null || this.queueName.trim().equals("")) {
            throw new Exception("invalid queue name: " + queueName);
         }

         this.filter = FilterImpl.createFilter(filterString);

         SimpleString name = new SimpleString(this.queueName);
         Binding b = this.postOffice.getBinding(name);
         if (b == null) {
            throw new Exception(connectorName + ": queue " + queueName + " not found");
         }
         this.queue = (Queue) b.getBindable();

         this.kafkaProducer = kafkaProducerFactory.create(kafkaConfig(configuration));

         List<PartitionInfo> topicPartitions = kafkaProducer.partitionsFor(topicName);
         if (topicPartitions == null || topicPartitions.size() == 0) {
            throw new Exception(connectorName + ": topic " + topicName + " not found");
         }

         this.retryCount.set(0);
         this.isStarted.set(true);
         connect();
         ActiveMQKafkaLogger.LOGGER.bridgeStarted(connectorName);
      }
   }

   public void connect() throws Exception {
      synchronized (this) {
         if (!isConnected && isStarted.get()) {
            isConnected = true;
            this.queue.addConsumer(this);
            this.queue.deliverAsync();
            ActiveMQKafkaLogger.LOGGER.bridgeConnected(connectorName);
         }
      }
   }

   @Override
   public void disconnect() {
      synchronized (this) {
         if (isConnected) {
            if (queue != null) {
               this.queue.removeConsumer(this);
            }

            cancelRefs();
            if (queue != null) {
               queue.deliverAsync();
            }
            isConnected = false;
            ActiveMQKafkaLogger.LOGGER.bridgeDisconnected(connectorName);
         }
      }
   }

   @Override
   public void stop() {
      synchronized (this) {
         if (!this.isStarted.get()) {
            return;
         }
         ActiveMQKafkaLogger.LOGGER.bridgeReceivedStopRequest(connectorName);

         disconnect();

         kafkaProducer.close();
         kafkaProducer = null;
         this.isStarted.set(false);
         ActiveMQKafkaLogger.LOGGER.bridgeStopped(connectorName);
      }
   }

   @Override
   public boolean isStarted() {
      return this.isStarted.get();
   }

   @Override
   public String getName() {
      return this.connectorName;
   }

   @Override
   public boolean supportsDirectDelivery() {
      return false;
   }

   @Override
   public HandleStatus handle(MessageReference ref) throws Exception {
      if (filter != null && !filter.match(ref.getMessage())) {
         return HandleStatus.NO_MATCH;
      }

      synchronized (this) {
         ref.handled();

         Message message = ref.getMessage();

         synchronized (refs) {
            refs.put(ref.getMessage().getMessageID(), ref);
         }

         //Compaction/Record Key (equivalent to Artemis LVQ)
         SimpleString key = message.getLastValueProperty();
         ProducerRecord<String, Message> producerRecord =
             new ProducerRecord<>(
                   topicName,
                   null,
                   message.getTimestamp(),
                   key == null ? null : key.toString(),
                   message);

         if (ActiveMQServerLogger.LOGGER.isTraceEnabled()) {
            ActiveMQServerLogger.LOGGER.trace("KafkaProducerBridge::going to send message: " + message + " from " + this.queue);
         }

         pendingAcks.countUp();

         try {
            kafkaProducer.send(producerRecord, new MessageAwareCallback(ref));
         } catch (Exception e) {
            // If an exception happened, we must count down immediately
            pendingAcks.countDown();
            throw e;
         }

         return HandleStatus.HANDLED;
      }
   }

   private void cancelRefs() {
      LinkedList<MessageReference> list = new LinkedList<>();

      synchronized (refs) {
         list.addAll(refs.values());
         refs.clear();
      }

      if (ActiveMQKafkaLogger.LOGGER.isTraceEnabled()) {
         ActiveMQKafkaLogger.LOGGER.trace("KafkaProducerBridge::cancelRefs cancelling " + list.size() + " references");
      }

      if (ActiveMQKafkaLogger.LOGGER.isTraceEnabled() && list.isEmpty()) {
         ActiveMQKafkaLogger.LOGGER.trace("KafkaProducerBridge::didn't have any references to cancel on bridge " + this);
         return;
      }

      ListIterator<MessageReference> listIterator = list.listIterator(list.size());

      Queue refqueue;

      long timeBase = System.currentTimeMillis();

      while (listIterator.hasPrevious()) {
         MessageReference ref = listIterator.previous();

         if (ActiveMQKafkaLogger.LOGGER.isTraceEnabled()) {
            ActiveMQKafkaLogger.LOGGER.trace("KafkaProducerBridge::cancelRefs Cancelling reference " + ref + " on bridge " + this);
         }

         refqueue = ref.getQueue();

         try {
            refqueue.cancel(ref, timeBase);
         } catch (Exception e) {
            // There isn't much we can do besides log an error
            ActiveMQKafkaLogger.LOGGER.errorCancellingRefOnBridge(e, ref);
         }
      }
   }

   @Override
   public void proceedDeliver(MessageReference reference) throws Exception {
      // no op
   }

   @Override
   public Filter getFilter() {
      return this.filter;
   }

   @Override
   public String debug() {
      return null;
   }

   @Override
   public String toManagementString() {
      return null;
   }

   @Override
   public List<MessageReference> getDeliveringMessages() {
      return null;
   }

   @Override
   public long sequentialID() {
      return sequentialID;
   }

   protected void scheduleReconnect() {

      if (!isStarted.get()) {
         ActiveMQServerLogger.LOGGER.bridgeStopping();
         return;
      }

      int retryCountInt = retryCount.get();
      if (retryCountInt > retryAttempts && retryAttempts >= 0) {
         ActiveMQKafkaLogger.LOGGER.bridgeAbortStart(SimpleString.toSimpleString(connectorName), retryCountInt, retryAttempts);
         stop();
         return;
      }

      long timeout = (long) (this.retryInterval * Math.pow(this.retryMultiplier, retryCountInt));
      if (timeout == 0) {
         timeout = this.retryInterval;
      }
      if (timeout > retryMaxInterval) {
         timeout = retryMaxInterval;
      }

      if (ActiveMQKafkaLogger.LOGGER.isDebugEnabled()) {
         ActiveMQKafkaLogger.LOGGER.debug("KafkaProducerBridge:: " + connectorName +
                                          " retrying connection #" +
                                          retryCount +
                                          ", maxRetry=" +
                                          retryAttempts +
                                          ", timeout=" +
                                          timeout);
      }

      if (awaitingReconnect.compareAndSet(false, true)) {
         scheduleReconnectFixedTimeout(timeout);
         retryCount.incrementAndGet();
      }
   }

   // Inner classes -------------------------------------------------

   protected void scheduleReconnectFixedTimeout(final long milliseconds) {
      if (ActiveMQKafkaLogger.LOGGER.isDebugEnabled()) {
         ActiveMQKafkaLogger.LOGGER.debug("KafkaProducerBridge::Scheduling retry for bridge " + this.connectorName + " in " + milliseconds + " milliseconds");
      }

      scheduledExecutorService.schedule(new ConnectRunnable(this), milliseconds, TimeUnit.MILLISECONDS);
   }

   private final class ConnectRunnable implements Runnable {

      private final KafkaProducerBridge bridge;

      private ConnectRunnable(KafkaProducerBridge bridge2) {
         bridge = bridge2;
      }

      @Override
      public void run() {
         try {
            bridge.connect();
            awaitingReconnect.set(false);
         } catch (Exception e) {
            ActiveMQKafkaLogger.LOGGER.bridgeFailedToStart(e, connectorName);
         }
      }
   }

   private final class MessageAwareCallback implements Callback {

      private MessageReference messageReference;

      private MessageAwareCallback(MessageReference messageReference) {
         this.messageReference = messageReference;
      }

      @Override
      public void onCompletion(RecordMetadata recordMetadata, Exception e) {
         if (e == null) {
            onSuccess(recordMetadata);
         } else {
            onException(e);
         }
      }

      void onException(Exception e) {
         ActiveMQKafkaLogger.LOGGER.bridgeUnableToSendMessage(e, messageReference);
         pendingAcks.countDown();
         disconnect();
         scheduleReconnect();
      }

      void onSuccess(RecordMetadata recordMetadata) {
         if (ActiveMQKafkaLogger.LOGGER.isTraceEnabled()) {
            ActiveMQKafkaLogger.LOGGER.trace(
                "KafkaProducerBridge::sendAcknowledged received confirmation for message " + messageReference.getMessage().getMessageID());
         }
         retryCount.lazySet(0);
         try {

            final MessageReference ref;

            synchronized (refs) {
               ref = refs.remove(messageReference.getMessage().getMessageID());
            }

            if (ref != null) {
               if (ActiveMQKafkaLogger.LOGGER.isTraceEnabled()) {
                  ActiveMQKafkaLogger.LOGGER
                      .trace("KafkaProducerBridge::sendAcknowledged bridge " + this + " Acking " + ref + " on queue " + ref.getQueue());
               }
               ref.getQueue().acknowledge(ref);
               pendingAcks.countDown();
            } else {
               if (ActiveMQKafkaLogger.LOGGER.isTraceEnabled()) {
                  ActiveMQKafkaLogger.LOGGER.trace(
                      "KafkaProducerBridge::sendAcknowledged bridge " + this + " could not find reference for message " +
                      messageReference.getMessage());
               }
            }
         } catch (Exception e) {
            ActiveMQKafkaLogger.LOGGER.bridgeFailedToAck(e);
         }
      }
   }
}
