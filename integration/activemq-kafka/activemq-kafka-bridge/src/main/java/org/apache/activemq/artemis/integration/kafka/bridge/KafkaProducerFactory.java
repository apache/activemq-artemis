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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.utils.UUID;
import org.apache.activemq.artemis.utils.UUIDGenerator;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;

public class KafkaProducerFactory<K, V> {

   private Map<Map<String, Object>, RefCountedKafkaProducerFactory> kafkaProducerFactoryMap = new ConcurrentHashMap<>();

   public Producer<K, V> create(Map<String, Object> configuration) {
      Map<String, Object> filteredConfig = new HashMap<>(configuration);
      KafkaConstants.OPTIONAL_OUTGOING_CONNECTOR_KEYS.forEach(filteredConfig::remove);
      KafkaConstants.REQUIRED_OUTGOING_CONNECTOR_KEYS.forEach(filteredConfig::remove);
      RefCountedKafkaProducerFactory refCountedKafkaProducer = kafkaProducerFactoryMap.computeIfAbsent(filteredConfig, RefCountedKafkaProducerFactory::new);
      return refCountedKafkaProducer.create();
   }

   private class RefCountedKafkaProducerFactory {

      private final Map<String, Object> configs;
      private Producer<K, V> kafkaProducer;
      private Map<UUID, DelegatedProducer> referencedProducers = new HashMap<>();
      private UUIDGenerator uuidGenerator = UUIDGenerator.getInstance();

      private RefCountedKafkaProducerFactory(Map<String, Object> configs) {
         this.configs = configs;
      }

      public Producer<K, V> create() {
         synchronized (this) {
            if (kafkaProducer == null) {
               kafkaProducer = new KafkaProducer<>(configs);
            }
            UUID uuid = uuidGenerator.generateUUID();
            DelegatedProducer delegatedProducer = new DelegatedProducer(uuid, kafkaProducer);
            referencedProducers.put(uuid, delegatedProducer);
            return delegatedProducer;
         }
      }

      private void close(UUID uuid) {
         synchronized (this) {
            DelegatedProducer delegatedProducer = referencedProducers.remove(uuid);
            if (delegatedProducer != null && referencedProducers.isEmpty()) {
               if (kafkaProducer != null) {
                  kafkaProducer.close();
                  kafkaProducer = null;
               }
            }
         }
      }

      private void close(UUID uuid, long timeout, TimeUnit timeUnit) {
         synchronized (this) {
            DelegatedProducer delegatedProducer = referencedProducers.remove(uuid);
            if (delegatedProducer != null && referencedProducers.isEmpty()) {
               if (kafkaProducer != null) {
                  kafkaProducer.close(timeout, timeUnit);
                  kafkaProducer = null;
               }
            }
         }
      }


      private class DelegatedProducer implements Producer<K, V> {

         private final Producer<K, V> producer;
         private boolean isClosed = false;
         private final UUID uuid;

         private DelegatedProducer(UUID uuid, Producer<K, V> producer) {
            this.uuid = uuid;
            this.producer = producer;
         }

         @Override
         public void initTransactions() {
            throw new UnsupportedOperationException();
         }

         @Override
         public void beginTransaction() throws ProducerFencedException {
            throw new UnsupportedOperationException();
         }

         @Override
         public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> map, String s) throws ProducerFencedException {
            throw new UnsupportedOperationException();
         }

         @Override
         public void commitTransaction() throws ProducerFencedException {
            throw new UnsupportedOperationException();
         }

         @Override
         public void abortTransaction() throws ProducerFencedException {
            throw new UnsupportedOperationException();
         }

         @Override
         public Future<RecordMetadata> send(ProducerRecord<K, V> producerRecord) {
            return producer.send(producerRecord);
         }

         @Override
         public Future<RecordMetadata> send(ProducerRecord<K, V> producerRecord, Callback callback) {
            return producer.send(producerRecord, callback);
         }

         @Override
         public void flush() {
            producer.flush();
         }

         @Override
         public List<PartitionInfo> partitionsFor(String s) {
            return producer.partitionsFor(s);
         }

         @Override
         public Map<MetricName, ? extends Metric> metrics() {
            return producer.metrics();
         }

         @Override
         public void close() {
            synchronized (this) {
               if (isClosed) {
                  return;
               } else {
                  RefCountedKafkaProducerFactory.this.close(uuid);
               }
            }
         }

         @Override
         public void close(long timeout, TimeUnit timeUnit) {
            synchronized (this) {
               if (isClosed) {
                  return;
               } else {
                  RefCountedKafkaProducerFactory.this.close(uuid, timeout, timeUnit);
               }
            }
         }
      }
   }
}
