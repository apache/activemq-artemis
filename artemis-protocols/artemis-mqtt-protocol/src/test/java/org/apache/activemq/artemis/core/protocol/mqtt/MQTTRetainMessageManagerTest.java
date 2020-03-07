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
import static org.awaitility.Awaitility.await;
import static org.awaitility.Durations.FIVE_HUNDRED_MILLISECONDS;
import static org.awaitility.Durations.TEN_SECONDS;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.config.WildcardConfiguration;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.junit.EmbeddedJMSResource;
import org.apache.activemq.artemis.utils.collections.LinkedListIterator;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.jboss.logging.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import com.github.javafaker.ChuckNorris;
import com.github.javafaker.Faker;

@SuppressWarnings("deprecation")
public class MQTTRetainMessageManagerTest {

   private Logger log = Logger.getLogger(MQTTRetainMessageManagerTest.class);

   public EmbeddedJMSResource jmsServer = new EmbeddedJMSResource("embedded-artemis-server-mqtt.xml");

   private MqttClientService mqttPublisher;

   private MqttClientService mqttConsumerCount;
   private MqttClientService mqttConsumerBeforePublish;
   private MqttClientService mqttConsumerAfterPublish;
   private MqttClientService mqttConsumerAfterPublish2;

   private final AtomicInteger publishCount = new AtomicInteger(0);

   private final AtomicInteger arrivedCountBeforePublish = new AtomicInteger();
   private final AtomicInteger arrivedCountAferPublish = new AtomicInteger();
   private final AtomicInteger arrivedCountAferPublish2 = new AtomicInteger();

   private final AtomicReference<MqttMessage> lastMessagePublished = new AtomicReference<>();
   private final AtomicReference<MqttMessage> lastMessageArrivedOnConsumerBeforePublish = new AtomicReference<>();
   private final AtomicReference<MqttMessage> lastMessageArrivedOnConsumerAfterPublish = new AtomicReference<>();
   private final AtomicReference<MqttMessage> lastMessageArrivedOnConsumerAfterPublish2 = new AtomicReference<>();

   private final String topic = "fact";

   private final ChuckNorris chuckNorris = (new Faker()).chuckNorris();

   private final int numberOfMessages = 1000;
   private final int numberOfTests = 10;

   @Rule
   public RuleChain rulechain = RuleChain.outerRule(jmsServer);

   @Before
   public void beforeEach() throws MqttException {
      publishCount.set(0);
      mqttPublisher = new MqttClientService("publisher", null);
      mqttPublisher.init();

      final MqttMessage clearRetainedMessage = new MqttMessage(new byte[] {});
      clearRetainedMessage.setRetained(true);
      clearRetainedMessage.setQos(1);
      mqttPublisher.publish(topic, clearRetainedMessage);

      mqttConsumerCount = new MqttClientService("consumer-count", null);
      mqttConsumerCount.init();
      mqttConsumerCount.setMessageConsumer(message -> publishCount.incrementAndGet());

      arrivedCountBeforePublish.set(0);
      mqttConsumerBeforePublish = new MqttClientService("consumer-before",
         message -> {
            final String payload = new String(message.getPayload());
            lastMessageArrivedOnConsumerBeforePublish.set(message);
            arrivedCountBeforePublish.incrementAndGet();
            log.debugf("[MQTT][before ][retained: %s][duplicate: %s][qos: %s] %s",
                  message.isRetained(), message.isDuplicate(), message.getQos(), payload);
         });
      mqttConsumerBeforePublish.init();

      arrivedCountAferPublish.set(0);
      arrivedCountAferPublish2.set(0);
      mqttConsumerAfterPublish = new MqttClientService("consumer-after",
         message -> {
            final String payload = new String(message.getPayload());
            lastMessageArrivedOnConsumerAfterPublish.set(message);
            arrivedCountAferPublish.incrementAndGet();
            log.infof("[MQTT][after  ][retained: %s][duplicate: %s][qos: %s] %s",
                  message.isRetained(), message.isDuplicate(), message.getQos(), payload);
         });
      mqttConsumerAfterPublish2 = new MqttClientService("consumer-after2",
         message -> {
            final String payload = new String(message.getPayload());
            lastMessageArrivedOnConsumerAfterPublish2.set(message);
            arrivedCountAferPublish2.incrementAndGet();
            log.infof("[MQTT][after2 ][retained: %s][duplicate: %s][qos: %s] %s",
                  message.isRetained(), message.isDuplicate(), message.getQos(), payload);
         });
      mqttConsumerAfterPublish.init();
      mqttConsumerAfterPublish2.init();
   }

   @After
   public void afterEach() throws MqttException {
      mqttPublisher.destroy();

      mqttConsumerCount.unsubsribe(topic);
      mqttConsumerCount.destroy();

      mqttConsumerBeforePublish.unsubsribe(topic);
      mqttConsumerBeforePublish.destroy();

      mqttConsumerAfterPublish.unsubsribe(topic);
      mqttConsumerAfterPublish.destroy();

      mqttConsumerAfterPublish2.unsubsribe(topic);
      mqttConsumerAfterPublish2.destroy();
   }

   @Test
   public void testAtMostOnce() throws MqttException {
      IntStream.of(numberOfTests).forEach(i -> actAndAssert(i, 0));
   }

   @Test
   public void testAtLeastOnce() throws MqttException {
      IntStream.of(numberOfTests).forEach(i -> actAndAssert(i, 1));
   }

   @Test
   public void testExactlyOnce() throws MqttException {
      IntStream.of(numberOfTests).forEach(i -> actAndAssert(i, 2));
   }

   private void actAndAssert(int i, int qos) {
      try {
         // Act
         mqttConsumerBeforePublish.subscribe(topic, qos);
         publish(qos);
         logAftePublish(i, qos);
         logRetainedMessagesQueue();
         mqttConsumerAfterPublish.subscribe(topic, qos);
         mqttConsumerAfterPublish2.subscribe(topic, qos);
         awaitUntilLastMessageArrivedOnConsumerAfterPublish();
         awaitUntilLastMessageArrivedOnConsumerAfterPublish2();

         // Assert
         assertEquals(1, arrivedCountAferPublish.get());
         assertLastMessageOnConsumerBeforePublishArrivedEqualsLastMessagePublished();
         assertLastMessageOnConsumerAfterPublishArrivedEqualsLastMessagePublished();
      } catch (MqttException e) {
         fail(e.getMessage());
      }
   }

   protected void publish(final int qos) throws MqttException {
      mqttConsumerCount.subscribe(topic, qos);
      IntStream.range(0, numberOfMessages).forEach(i -> {
         final String fact = String.format("[%s] %s", i, chuckNorris.fact());
         final MqttMessage message = message(fact, qos, true);
         mqttPublisher.publish(topic, message);
         lastMessagePublished.set(message);
      });
      awaitUntilPiblishCount();
   }

   protected MqttMessage message(final String payload, final int qos, final boolean retained) {
      final MqttMessage message = new MqttMessage();
      message.setQos(qos);
      message.setRetained(retained);
      message.setPayload(payload.getBytes());
      return message;
   }

   private void awaitUntilPiblishCount() {
      await()
            .with()
            .pollDelay(FIVE_HUNDRED_MILLISECONDS)
            .atMost(TEN_SECONDS)
            .until(() -> publishCount.get() >= numberOfMessages);
   }

   private void awaitUntilLastMessageArrivedOnConsumerAfterPublish() {
      await()
            .pollDelay(FIVE_HUNDRED_MILLISECONDS)
            .atMost(TEN_SECONDS)
            .until(() -> nonNull(lastMessageArrivedOnConsumerAfterPublish.get()));
   }

   private void awaitUntilLastMessageArrivedOnConsumerAfterPublish2() {
      await()
            .pollDelay(FIVE_HUNDRED_MILLISECONDS)
            .atMost(TEN_SECONDS)
            .until(() -> nonNull(lastMessageArrivedOnConsumerAfterPublish2.get()));
   }

   private void assertLastMessageOnConsumerBeforePublishArrivedEqualsLastMessagePublished() {
      assertArrayEquals(String.format(
            "\nMessage arrived on consumer subscribed before the publish is different from the last published message!\nPublished: %s\nArrived  : %s\n",
            new String(lastMessagePublished.get().getPayload()), new String(lastMessageArrivedOnConsumerAfterPublish.get().getPayload())),
            lastMessagePublished.get().getPayload(), lastMessageArrivedOnConsumerBeforePublish.get().getPayload());
   }

   private void assertLastMessageOnConsumerAfterPublishArrivedEqualsLastMessagePublished() {
      assertArrayEquals(String.format(
            "\nMessage arrived on consumer subscribed after the publish is different from the last published message!\nPublished: %s\nArrived  : %s\n",
            new String(lastMessagePublished.get().getPayload()), new String(lastMessageArrivedOnConsumerAfterPublish.get().getPayload())),
            lastMessagePublished.get().getPayload(), lastMessageArrivedOnConsumerAfterPublish.get().getPayload());
      assertArrayEquals(String.format(
            "\nMessage arrived on consumer subscribed after the publish (2) is different from the last published message!\nPublished: %s\nArrived  : %s\n",
            new String(lastMessagePublished.get().getPayload()), new String(lastMessageArrivedOnConsumerAfterPublish.get().getPayload())),
            lastMessagePublished.get().getPayload(), lastMessageArrivedOnConsumerAfterPublish2.get().getPayload());
   }

   private void logAftePublish(int i, int qos) {
      log.infof("--- QoS: %s --- %s/%s---", qos, i, numberOfTests);
      log.infof("[MQTT][publish][retained: %s][duplicate: %s][qos: %s] %s",
            lastMessagePublished.get().isRetained(), lastMessagePublished.get().isDuplicate(), lastMessagePublished.get().getQos(), lastMessagePublished.get());
      log.infof("[MQTT][before ][retained: %s][duplicate: %s][qos: %s] %s",
            lastMessageArrivedOnConsumerBeforePublish.get().isRetained(),
            lastMessageArrivedOnConsumerBeforePublish.get().isDuplicate(),
            lastMessageArrivedOnConsumerBeforePublish.get().getQos(),
            new String(lastMessageArrivedOnConsumerBeforePublish.get().getPayload()));
   }

   private void logRetainedMessagesQueue() {
      final WildcardConfiguration wildcardConfiguration = new WildcardConfiguration();
      final String retainAddress = MQTTUtil.convertMQTTAddressFilterToCoreRetain(topic, wildcardConfiguration);
      final Queue queue = jmsServer.getDestinationQueue(retainAddress);
      final LinkedListIterator<MessageReference> browserIterator = queue.browserIterator();
      browserIterator.forEachRemaining(messageReference -> {
         final Message message = messageReference.getMessage();
         final String body = message.getBuffer().toString(StandardCharsets.UTF_8);
         log.infof("[MQTT][%s][%s][%s]", retainAddress, message, body);
      });
   }
}
