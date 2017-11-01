/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.integration.kafka;

import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import org.jboss.logging.Logger;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;


public class LocalKafkaBroker {
   private static final Logger LOGGER = Logger.getLogger(LocalKafkaBroker.class);
   private static final int ALLOCATE_RANDOM_PORT = -1;
   private static final String LOCALHOST = "localhost";

   private int kafkaPort;
   private int zookeeperPort;
   private Properties overrideBrokerProperties;

   private TestingServer zookeeper;
   private KafkaServerStartable kafkaServer;
   private Path kafkaLogDir;

   private volatile boolean brokerStarted = false;

   /**
    * Create a new ephemeral Kafka broker with random broker port and Zookeeper port
    *
    * @return EphemeralKafkaBroker
    */
   public static LocalKafkaBroker create() {
      return create(ALLOCATE_RANDOM_PORT);
   }

   /**
    * Create a new ephemeral Kafka broker with the specified broker port and random Zookeeper port
    *
    * @param kafkaPort Port the broker should listen on
    * @return EphemeralKafkaBroker
    */
   public static LocalKafkaBroker create(int kafkaPort) {
      return create(kafkaPort, ALLOCATE_RANDOM_PORT);
   }

   /**
    * Create a new ephemeral Kafka broker with the specified broker port and Zookeeper port
    *
    * @param kafkaPort    Port the broker should listen on
    * @param zookeeperPort Port the Zookeeper should listen on
    * @return EphemeralKafkaBroker
    */
   public static LocalKafkaBroker create(int kafkaPort, int zookeeperPort) {
      return create(kafkaPort, zookeeperPort, null);
   }

   /**
    * Create a new ephemeral Kafka broker with the specified broker port, Zookeeper port and config overrides.
    *
    * @param kafkaPort            Port the broker should listen on
    * @param zookeeperPort         Port the Zookeeper should listen on
    * @param overrideBrokerProperties Broker properties to override. Pass null if there aren't any.
    * @return EphemeralKafkaBroker
    */
   public static LocalKafkaBroker create(int kafkaPort, int zookeeperPort, Properties overrideBrokerProperties) {
      return new LocalKafkaBroker(kafkaPort, zookeeperPort, overrideBrokerProperties);
   }


   LocalKafkaBroker(int kafkaPort, int zookeeperPort, Properties overrideBrokerProperties) {
      this.kafkaPort = kafkaPort;
      this.zookeeperPort = zookeeperPort;
      this.overrideBrokerProperties = overrideBrokerProperties;
   }

   /**
    * Start the Kafka broker
    */
   public CompletableFuture<Void> start() throws Exception {
      if (!brokerStarted) {
         synchronized (this) {
            if (!brokerStarted) {
               return startBroker();
            }
         }
      }

      return CompletableFuture.completedFuture(null);
   }

   private CompletableFuture<Void> startBroker() throws Exception {
      if (zookeeperPort == ALLOCATE_RANDOM_PORT) {
         zookeeper = new TestingServer(true);
         zookeeperPort = zookeeper.getPort();
      } else {
         zookeeper = new TestingServer(zookeeperPort, true);
      }

      kafkaPort = kafkaPort == ALLOCATE_RANDOM_PORT ? InstanceSpec.getRandomPort() : kafkaPort;
      String zookeeperConnectionString = zookeeper.getConnectString();
      KafkaConfig kafkaConfig = buildKafkaConfig(zookeeperConnectionString);

      LOGGER.info("Starting Kafka server with config:" + kafkaConfig.props());
      kafkaServer = new KafkaServerStartable(kafkaConfig);
      brokerStarted = true;
      return CompletableFuture.runAsync(() -> kafkaServer.startup());
   }

   /**
    * Stop the Kafka broker
    */
   public void stop() {
      if (brokerStarted) {
         synchronized (this) {
            if (brokerStarted) {
               stopBroker();
               brokerStarted = false;
            }
         }
      }
   }

   private void stopBroker() {
      try {
         if (kafkaServer != null) {
            LOGGER.info("Shutting down Kafka Server");
            kafkaServer.shutdown();
         }

         if (zookeeper != null) {
            LOGGER.info("Shutting down Zookeeper");
            zookeeper.close();
         }

         if (Files.exists(kafkaLogDir)) {
            LOGGER.info("Deleting the log dir: " +  kafkaLogDir);
            Files.walkFileTree(kafkaLogDir, new SimpleFileVisitor<Path>() {
               @Override
               public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                  Files.deleteIfExists(file);
                  return FileVisitResult.CONTINUE;
               }

               @Override
               public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                  Files.deleteIfExists(dir);
                  return FileVisitResult.CONTINUE;
               }
            });
         }
      } catch (Exception e) {
         LOGGER.error("Failed to clean-up Kafka", e);
      }
   }

   private KafkaConfig buildKafkaConfig(String zookeeperQuorum) throws IOException {
      kafkaLogDir = Files.createTempDirectory("kafka_junit");

      Properties props = new Properties();
      props.put("advertised.listeners", "PLAINTEXT://" + LOCALHOST + ":" + kafkaPort);
      props.put("listeners", "PLAINTEXT://0.0.0.0:" + kafkaPort);
      props.put("port", kafkaPort + "");
      props.put("broker.id", "1");
      props.put("log.dirs", kafkaLogDir.toAbsolutePath().toString());
      props.put("zookeeper.connect", zookeeperQuorum);
      props.put("leader.imbalance.check.interval.seconds", "1");
      props.put("offsets.topic.num.partitions", "1");
      props.put("offsets.topic.replication.factor", "1");
      props.put("default.replication.factor", "1");
      props.put("num.partitions", "1");
      props.put("group.min.session.timeout.ms", "100");

      if (overrideBrokerProperties != null) {
         props.putAll(overrideBrokerProperties);
      }

      return new KafkaConfig(props);
   }

   /**
    * Create a minimal producer configuration that can be used to produce to this broker
    *
    * @return Properties
    */
   public Map<String, Object> producerConfig() {
      Map<String, Object> props = new HashMap<>();
      props.put("bootstrap.servers", LOCALHOST + ":" + kafkaPort);
      props.put("acks", "1");
      props.put("batch.size", "10");
      props.put("client.id", "kafka-junit");
      props.put("request.timeout.ms", "500");

      return props;
   }

   /**
    * Create a minimal consumer configuration with auto commit enabled. Offset is set to "earliest".
    *
    * @return Properies
    */
   public Map<String, Object> consumerConfig() {
      return consumerConfig(true);
   }

   /**
    * Create a minimal consumer configuration. Offset is set to "earliest".
    *
    * @return Properties
    */
   public Map<String, Object> consumerConfig(boolean enableAutoCommit) {
      Map<String, Object> props = new HashMap<>();
      props.put("bootstrap.servers", LOCALHOST + ":" + kafkaPort);
      props.put("group.id", "kafka-junit-consumer");
      props.put("enable.auto.commit", String.valueOf(enableAutoCommit));
      props.put("auto.commit.interval.ms", "10");
      props.put("auto.offset.reset", "earliest");
      props.put("heartbeat.interval.ms", "100");
      props.put("session.timeout.ms", "200");
      props.put("fetch.max.wait.ms", "200");
      props.put("metadata.max.age.ms", "100");

      return props;
   }

   /**
    * Create a producer that can write to this broker
    *
    * @param keySerializer   Key serializer class
    * @param valueSerializer Valuer serializer class
    * @param overrideConfig  Producer config to override. Pass null if there aren't any.
    * @param <K>          Type of Key
    * @param <V>          Type of Value
    * @return KafkaProducer
    */
   public <K, V> KafkaProducer<K, V> createProducer(Serializer<K> keySerializer, Serializer<V> valueSerializer,
                                        Map<String, Object> overrideConfig) {
      Map<String, Object> conf = producerConfig();
      if (overrideConfig != null) {
         conf.putAll(overrideConfig);
      }
      keySerializer.configure(conf, true);
      valueSerializer.configure(conf, false);
      return new KafkaProducer<>(conf, keySerializer, valueSerializer);
   }

   /**
    * Create a consumer that can read from this broker
    *
    * @param keyDeserializer   Key deserializer
    * @param valueDeserializer Value deserializer
    * @param overrideConfig   Consumer config to override. Pass null if there aren't any
    * @param <K>            Type of Key
    * @param <V>            Type of Value
    * @return KafkaConsumer
    */
   public <K, V> KafkaConsumer<K, V> createConsumer(Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer,
                                        Map<String, Object> overrideConfig) {
      Map<String, Object> conf = consumerConfig();
      if (overrideConfig != null) {
         conf.putAll(overrideConfig);
      }
      keyDeserializer.configure(conf, true);
      valueDeserializer.configure(conf, false);
      return new KafkaConsumer<>(conf, keyDeserializer, valueDeserializer);
   }

   public KafkaServerStartable getKafkaServer() {
      return kafkaServer;
   }
}
