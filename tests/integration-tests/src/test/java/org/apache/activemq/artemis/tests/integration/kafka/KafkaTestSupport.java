/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.kafka;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.activemq.artemis.tests.integration.amqp.JMSClientTestSupport;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.Before;

public class KafkaTestSupport extends JMSClientTestSupport {

   Properties properties = new Properties();
   {
      properties.put("group.initial.rebalance.delay.ms", 0);
   }

   public LocalKafkaBroker localKafkaBroker;

   @Before
   @Override
   public void setUp() throws Exception {
      localKafkaBroker = LocalKafkaBroker.create(-1, -1, properties);
      localKafkaBroker.start().get();
      super.setUp();
   }

   @After
   @Override
   public void tearDown() throws Exception {
      super.tearDown();
      localKafkaBroker.stop();
   }

   /**
    * Create a consumer that can read from this broker
    *
    * @param topic             Topic to subscribe
    * @param keyDeserializer   Key deserializer
    * @param valueDeserializer Value deserializer
    * @param overrideConfig    Consumer config to override. Pass null if there aren't any
    * @param <K>               Type of Key
    * @param <V>               Type of Value
    * @return KafkaConsumer
    */
   public <K, V> KafkaConsumer<K, V> createConsumer(String topic, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer,
                                                    Map<String, Object> overrideConfig) {
      return subscribe(localKafkaBroker.createConsumer(keyDeserializer, valueDeserializer, overrideConfig), topic);
   }

   /**
    * Create a consumer that reads strings
    *
    * @return KafkaConsumer
    */
   public KafkaConsumer<String, String> createStringConsumer(String topic) {
      return createConsumer(topic, new StringDeserializer(), new StringDeserializer(), null);
   }

   /**
    * Create a consumer that reads bytes
    *
    * @return KafkaConsumer
    */
   public KafkaConsumer<byte[], byte[]> createByteConsumer(String topic) {
      return createConsumer(topic, new ByteArrayDeserializer(), new ByteArrayDeserializer(), null);
   }

   private <K, V> KafkaConsumer<K, V> subscribe(KafkaConsumer<K, V> consumer, String topic) {
      consumer.subscribe(Collections.singleton(topic));
      return consumer;
   }

   protected <K, V> ConsumerRecord<K, V> consumeOneRecord(KafkaConsumer<K, V> consumer) {
      List<ConsumerRecord<K, V>> records = consumeRecords(consumer, 1);
      if (records.isEmpty())
         return null;
      return records.get(0);
   }

   protected  <K, V> List<ConsumerRecord<K, V>> consumeRecords(KafkaConsumer<K, V> consumer, int numMessagesToConsume) {
      return consume(consumer, numMessagesToConsume);
   }

   /**
    * Attempt to consume the specified number of messages
    *
    * @param consumer             Consumer to use
    * @param numRecordsToConsume Number of messages to consume
    * @param <K>                  Type of Key
    * @param <V>                  Type of Value
    * @return ListenableFuture
    */
   public <K, V> List<ConsumerRecord<K, V>> consume(KafkaConsumer<K, V> consumer, int numRecordsToConsume) {
      Map<TopicPartition, OffsetAndMetadata> commitBuffer = new HashMap<>();
      List<ConsumerRecord<K, V>> polledMessages = new ArrayList<>(numRecordsToConsume);
      while ((polledMessages.size() < numRecordsToConsume) && (!Thread.currentThread().isInterrupted())) {
         ConsumerRecords<K, V> records = consumer.poll(0);
         for (ConsumerRecord<K, V> rec : records) {
            polledMessages.add(rec);
            commitBuffer.put(
                new TopicPartition(rec.topic(), rec.partition()),
                new OffsetAndMetadata(rec.offset() + 1)
            );

            if (polledMessages.size() == numRecordsToConsume) {
               consumer.commitSync(commitBuffer);
               break;
            }
         }
      }
      return polledMessages;
   }
}
