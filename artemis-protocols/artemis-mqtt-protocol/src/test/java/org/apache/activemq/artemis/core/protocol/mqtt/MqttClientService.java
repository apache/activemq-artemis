/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.core.protocol.mqtt;

import static java.util.Objects.nonNull;
import static org.eclipse.paho.client.mqttv3.MqttConnectOptions.MQTT_VERSION_3_1_1;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Consumer;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.jboss.logging.Logger;

import io.netty.util.concurrent.DefaultThreadFactory;

public class MqttClientService implements MqttCallback {

   private final String clientId;

   private Consumer<MqttMessage> messageConsumer;

   private MqttClient mqttClient;

   private MemoryPersistence persistence = new MemoryPersistence();
   private ScheduledExecutorService executorService;
   private int corePoolSize = 5;

   private Logger log = Logger.getLogger(MqttClientService.class);

   public MqttClientService() {
      this("producer", null);
   }

   public MqttClientService(final String clientId, Consumer<MqttMessage> messageConsumer) {
      this.clientId = clientId;
      this.messageConsumer = messageConsumer;
   }

   @PostConstruct
   public void init() throws MqttException {
      final String serverURI = "tcp://localhost:1883";
      final MqttConnectOptions options = new MqttConnectOptions();
      options.setAutomaticReconnect(true);
      options.setCleanSession(false);
      options.setMaxInflight(1000);
      options.setServerURIs(new String[] {serverURI});
      options.setMqttVersion(MQTT_VERSION_3_1_1);

      final ThreadFactory threadFactory = new DefaultThreadFactory("mqtt-client-exec");
      executorService = new ScheduledThreadPoolExecutor(corePoolSize, threadFactory, new ThreadPoolExecutor.CallerRunsPolicy());
      mqttClient = new MqttClient(serverURI, clientId, persistence, executorService);
      mqttClient.setTimeToWait(-1);
      mqttClient.connect(options);
      mqttClient.setCallback(this);
      log.debugf("[MQTT][Connected][client: %s]", clientId);
   }

   @PreDestroy
   public void destroy() throws MqttException {
      mqttClient.disconnect();
      executorService.shutdownNow();
      log.debugf("[MQTT][Disconnected][client: %s]", clientId);
   }

   @Override
   public void connectionLost(Throwable cause) {
      log.errorf("[MQTT][connectionLost][%s]", cause.getMessage());
   }

   @Override
   public void messageArrived(String topic, MqttMessage message) {
      log.debugf("[MQTT][messageArrived][client: %s][topic: %s][message: %s]", clientId, topic, message);
      if (nonNull(messageConsumer)) {
         messageConsumer.accept(message);
      }
   }

   @Override
   public void deliveryComplete(IMqttDeliveryToken token) {
      log.tracef("[MQTT][deliveryComplete][token: %s]", token);
   }

   public void publish(final String topic, final MqttMessage message) {
      try {
         mqttClient.publish(topic, message);
      } catch (final MqttException e) {
         log.error(e.getMessage(), e);
      }
   }

   public void subscribe(final String topicFilter, int qos) throws MqttException {
      mqttClient.subscribe(topicFilter, qos);
   }

   public void unsubsribe(final String topicFilter) throws MqttException {
      mqttClient.unsubscribe(topicFilter);
   }

   public void setMessageConsumer(Consumer<MqttMessage> messageConsumer) {
      this.messageConsumer = messageConsumer;
   }
}
