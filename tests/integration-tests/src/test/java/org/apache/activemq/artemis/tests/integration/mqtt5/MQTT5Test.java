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
package org.apache.activemq.artemis.tests.integration.mqtt5;

import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.Message;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.paging.impl.PagingManagerImpl;
import org.apache.activemq.artemis.core.paging.impl.PagingManagerImplAccessor;
import org.apache.activemq.artemis.core.postoffice.impl.PostOfficeImpl;
import org.apache.activemq.artemis.core.postoffice.impl.PostOfficeTestAccessor;
import org.apache.activemq.artemis.core.protocol.mqtt.MQTTReasonCodes;
import org.apache.activemq.artemis.core.protocol.mqtt.MQTTUtil;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.logs.AssertionLoggerHandler;
import org.apache.activemq.artemis.tests.util.RandomUtil;
import org.apache.activemq.artemis.utils.ReusableLatch;
import org.apache.activemq.artemis.utils.Wait;
import org.eclipse.paho.mqttv5.client.MqttAsyncClient;
import org.eclipse.paho.mqttv5.client.MqttCallback;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptionsBuilder;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;
import org.eclipse.paho.mqttv5.common.packet.UserProperty;
import org.junit.Assume;
import org.junit.Test;

/*
 * General tests for things not covered directly in the specification.
 */
public class MQTT5Test extends MQTT5TestSupport {

   public MQTT5Test(String protocol) {
      super(protocol);
   }

   /*
    * Ensure that the broker adds a timestamp on the message when sending via MQTT
    */
   @Test(timeout = DEFAULT_TIMEOUT)
   public void testTimestamp() throws Exception {
      final String DESTINATION = RandomUtil.randomString();

      createJMSConnection();
      JMSContext context = cf.createContext();
      JMSConsumer consumer = context.createConsumer(context.createQueue(DESTINATION));

      long time = System.currentTimeMillis();
      MqttClient producer = createPahoClient(RandomUtil.randomString());
      producer.connect();
      producer.publish(DESTINATION, new byte[0], 1, false);
      producer.disconnect();
      producer.close();

      Message m = consumer.receive(200);
      assertNotNull(m);
      assertTrue(m.getJMSTimestamp() > time);
      context.close();
   }

   @Test(timeout = DEFAULT_TIMEOUT)
   public void testAddressAutoCreation() throws Exception {
      final String DESTINATION = RandomUtil.randomString();
      server.getAddressSettingsRepository().addMatch(DESTINATION, new AddressSettings().setAutoCreateAddresses(true));

      MqttClient producer = createPahoClient(RandomUtil.randomString());
      producer.connect();
      producer.publish(DESTINATION, new byte[0], 0, false);
      producer.disconnect();
      producer.close();

      Wait.assertTrue(() -> server.getAddressInfo(SimpleString.toSimpleString(DESTINATION)) != null, 2000, 100);
   }

   @Test(timeout = DEFAULT_TIMEOUT)
   public void testAddressAutoCreationNegative() throws Exception {
      final String DESTINATION = RandomUtil.randomString();
      server.getAddressSettingsRepository().addMatch(DESTINATION, new AddressSettings().setAutoCreateAddresses(false));

      MqttClient producer = createPahoClient(RandomUtil.randomString());
      producer.connect();
      producer.publish(DESTINATION, new byte[0], 0, false);
      producer.disconnect();
      producer.close();

      assertTrue(server.getAddressInfo(SimpleString.toSimpleString(DESTINATION)) == null);
   }

   /*
    * Trying to reproduce error from https://issues.apache.org/jira/browse/ARTEMIS-1184
    */
   @Test(timeout = DEFAULT_TIMEOUT)
   public void testMaxMessageSize() throws Exception {
      // this doesn't work with websockets because the websocket frame size is too low
      Assume.assumeTrue(protocol.equals(TCP));

      final String TOPIC = RandomUtil.randomString();
      // subtract a little to leave room for the header
      final int SIZE = MQTTUtil.MAX_PACKET_SIZE - 48;
      StringBuilder builder = new StringBuilder(SIZE);

      for (int i = 0; i < SIZE; i++) {
         builder.append("=");
      }
      byte[] bytes = builder.toString().getBytes(StandardCharsets.UTF_8);

      final CountDownLatch latch = new CountDownLatch(1);
      MqttClient consumer = createPahoClient("consumer");
      consumer.setCallback(new DefaultMqttCallback() {
         @Override
         public void messageArrived(String topic, MqttMessage message) throws Exception {
            assertEqualsByteArrays(bytes.length, bytes, message.getPayload());
            latch.countDown();
         }
      });
      consumer.connect();
      consumer.subscribe(TOPIC, 1);

      MqttClient producer = createPahoClient(RandomUtil.randomString());
      producer.connect();
      producer.publish(TOPIC, bytes, 1, false);
      producer.disconnect();
      producer.close();
      Wait.assertEquals(1L, () -> getSubscriptionQueue(TOPIC).getMessagesAdded(), 2000, 100);

      assertTrue(latch.await(30, TimeUnit.SECONDS));
      consumer.disconnect();
      consumer.close();
   }

   /*
    * There is no normative statement in the spec about supporting user properties on will messages, but it is implied
    * in various places.
    */
   @Test(timeout = DEFAULT_TIMEOUT)
   public void testWillMessageProperties() throws Exception {
      final byte[] WILL = RandomUtil.randomBytes();
      final String[][] properties = new String[10][2];
      for (String[] property : properties) {
         property[0] = RandomUtil.randomString();
         property[1] = RandomUtil.randomString();
      }

      // consumer of the will message
      MqttClient client1 = createPahoClient("willConsumer");
      CountDownLatch latch = new CountDownLatch(1);
      client1.setCallback(new DefaultMqttCallback() {
         @Override
         public void messageArrived(String topic, MqttMessage message) {
            int i = 0;
            for (UserProperty property : message.getProperties().getUserProperties()) {
               assertEquals(properties[i][0], property.getKey());
               assertEquals(properties[i][1], property.getValue());
               i++;
            }
            latch.countDown();
         }
      });
      client1.connect();
      client1.subscribe("/topic/foo", 1);

      // consumer to generate the will
      MqttClient client2 = createPahoClient("willGenerator");
      MqttProperties willMessageProperties = new MqttProperties();
      List<UserProperty> userProperties = new ArrayList<>();
      for (String[] property : properties) {
         userProperties.add(new UserProperty(property[0], property[1]));
      }
      willMessageProperties.setUserProperties(userProperties);
      MqttConnectionOptions options = new MqttConnectionOptionsBuilder()
         .will("/topic/foo", new MqttMessage(WILL))
         .build();
      options.setWillMessageProperties(willMessageProperties);
      client2.connect(options);
      client2.disconnectForcibly(0, 0, false);
      assertTrue(latch.await(2, TimeUnit.SECONDS));
   }

   /*
    * It's possible for a client to change their session expiry interval via the DISCONNECT packet. Ensure we respect
    * a new session expiry interval when disconnecting.
    */
   @Test(timeout = DEFAULT_TIMEOUT)
   public void testExpiryDelayOnDisconnect() throws Exception {
      final String CONSUMER_ID = RandomUtil.randomString();

      MqttAsyncClient consumer = createAsyncPahoClient(CONSUMER_ID);
      MqttConnectionOptions options = new MqttConnectionOptionsBuilder()
         .sessionExpiryInterval(300L)
         .build();
      consumer.connect(options).waitForCompletion();
      MqttProperties disconnectProperties = new MqttProperties();
      disconnectProperties.setSessionExpiryInterval(0L);
      consumer.disconnect(0, null, null, MQTTReasonCodes.SUCCESS, disconnectProperties).waitForCompletion();

      Wait.assertEquals(0, () -> getSessionStates().size(), 5000, 10);
   }

   /*
    * If the Will flag is false then don't send a will message even if the session expiry is > 0
    */
   @Test(timeout = DEFAULT_TIMEOUT)
   public void testWillFlagFalseWithSessionExpiryDelay() throws Exception {
      // enable send-to-dla-on-no-route so that we can detect an errant will message on disconnect
      server.createQueue(new QueueConfiguration("activemq.notifications"));
      server.createQueue(new QueueConfiguration("DLA"));
      server.getAddressSettingsRepository().addMatch("#", new AddressSettings().setSendToDLAOnNoRoute(true).setDeadLetterAddress(SimpleString.toSimpleString("DLA")));

      MqttClient client = createPahoClient("willGenerator");
      MqttConnectionOptions options = new MqttConnectionOptionsBuilder()
         .sessionExpiryInterval(1L)
         .build();
      client.connect(options);
      client.disconnectForcibly(0, 0, false);
      scanSessions();
      assertEquals(0, server.locateQueue("DLA").getMessageCount());
   }

   @Test(timeout = DEFAULT_TIMEOUT)
   public void testQueueCleanOnRestart() throws Exception {
      String topic = RandomUtil.randomString();
      String clientId = RandomUtil.randomString();

      MqttClient client = createPahoClient(clientId);
      MqttConnectionOptions options = new MqttConnectionOptionsBuilder()
         .sessionExpiryInterval(999L)
         .cleanStart(true)
         .build();
      client.connect(options);
      client.subscribe(topic, AT_LEAST_ONCE);
      server.stop();
      server.start();
      org.apache.activemq.artemis.tests.util.Wait.assertTrue(() -> getSubscriptionQueue(topic, clientId) != null, 3000, 10);
   }

   @Test(timeout = DEFAULT_TIMEOUT)
   public void testRecursiveWill() throws Exception {
      AssertionLoggerHandler.startCapture(true);
      try {
         final String WILL_QUEUE = "will";
         server.createQueue(new QueueConfiguration(WILL_QUEUE).setRoutingType(RoutingType.ANYCAST));
         PagingManagerImplAccessor.setDiskFull((PagingManagerImpl) server.getPagingManager(), true);
         MqttClient client = createPahoClient("willGenerator");
         MqttConnectionOptions options = new MqttConnectionOptionsBuilder().will(WILL_QUEUE, new MqttMessage(RandomUtil.randomBytes())).build();
         client.connect(options);
         client.disconnectForcibly(0, 0, false);
         Wait.assertTrue(() -> AssertionLoggerHandler.findText("AMQ229119"), 2000, 100);
      } finally {
         AssertionLoggerHandler.stopCapture();
      }
   }

   @Test(timeout = DEFAULT_TIMEOUT)
   public void testSharedSubscriptionsWithSameName() throws Exception {
      final String TOPIC1 = "myTopic1";
      final String TOPIC2 = "myTopic2";
      final String SUB_NAME = "mySub";
      final String SHARED_SUB1 = MQTTUtil.SHARED_SUBSCRIPTION_PREFIX + SUB_NAME + "/" + TOPIC1;
      final String SHARED_SUB2 = MQTTUtil.SHARED_SUBSCRIPTION_PREFIX + SUB_NAME + "/" + TOPIC2;
      CountDownLatch ackLatch1 = new CountDownLatch(1);
      CountDownLatch ackLatch2 = new CountDownLatch(1);

      MqttClient consumer1 = createPahoClient("consumer1");
      consumer1.connect();
      consumer1.setCallback(new LatchedMqttCallback(ackLatch1));
      consumer1.subscribe(SHARED_SUB1, 1);

      assertNotNull(server.getAddressInfo(SimpleString.toSimpleString(TOPIC1)));
      Queue q1 = getSubscriptionQueue(TOPIC1, "consumer1", SUB_NAME);
      assertNotNull(q1);
      assertEquals(TOPIC1, q1.getAddress().toString());
      assertEquals(1, q1.getConsumerCount());

      MqttClient consumer2 = createPahoClient("consumer2");
      consumer2.connect();
      consumer2.setCallback(new LatchedMqttCallback(ackLatch2));
      consumer2.subscribe(SHARED_SUB2, 1);

      assertNotNull(server.getAddressInfo(SimpleString.toSimpleString(TOPIC2)));
      Queue q2 = getSubscriptionQueue(TOPIC2, "consumer2", SUB_NAME);
      assertNotNull(q2);
      assertEquals(TOPIC2, q2.getAddress().toString());
      assertEquals(1, q2.getConsumerCount());

      MqttClient producer = createPahoClient("producer");
      producer.connect();
      producer.publish(TOPIC1, new byte[0], 1, false);
      producer.publish(TOPIC2, new byte[0], 1, false);
      producer.disconnect();
      producer.close();

      assertTrue(ackLatch1.await(2, TimeUnit.SECONDS));
      assertTrue(ackLatch2.await(2, TimeUnit.SECONDS));

      consumer1.unsubscribe(SHARED_SUB1);
      assertNull(getSubscriptionQueue(TOPIC1, "consumer1", SUB_NAME));

      consumer2.unsubscribe(SHARED_SUB2);
      assertNull(getSubscriptionQueue(TOPIC2, "consumer2", SUB_NAME));

      consumer1.disconnect();
      consumer1.close();
      consumer2.disconnect();
      consumer2.close();
   }

   @Test(timeout = DEFAULT_TIMEOUT)
   public void testSharedSubscriptionsWithSameName2() throws Exception {
      final String TOPIC1 = "myTopic1";
      final String TOPIC2 = "myTopic2";
      final String SUB_NAME = "mySub";
      final String[] SHARED_SUBS = new String[]{
         MQTTUtil.SHARED_SUBSCRIPTION_PREFIX + SUB_NAME + "/" + TOPIC1,
         MQTTUtil.SHARED_SUBSCRIPTION_PREFIX + SUB_NAME + "/" + TOPIC2
      };
      CountDownLatch ackLatch = new CountDownLatch(2);

      MqttClient consumer = createPahoClient("consumer1");
      consumer.connect();
      consumer.setCallback(new LatchedMqttCallback(ackLatch));
      consumer.subscribe(SHARED_SUBS, new int[]{1, 1});

      assertNotNull(server.getAddressInfo(SimpleString.toSimpleString(TOPIC1)));
      Queue q1 = getSubscriptionQueue(TOPIC1, "consumer1", SUB_NAME);
      assertNotNull(q1);
      assertEquals(TOPIC1, q1.getAddress().toString());
      assertEquals(1, q1.getConsumerCount());

      assertNotNull(server.getAddressInfo(SimpleString.toSimpleString(TOPIC2)));
      Queue q2 = getSubscriptionQueue(TOPIC2, "consumer1", SUB_NAME);
      assertNotNull(q2);
      assertEquals(TOPIC2, q2.getAddress().toString());
      assertEquals(1, q2.getConsumerCount());

      MqttClient producer = createPahoClient("producer");
      producer.connect();
      producer.publish(TOPIC1, new byte[0], 1, false);
      producer.publish(TOPIC2, new byte[0], 1, false);
      producer.disconnect();
      producer.close();

      assertTrue(ackLatch.await(2, TimeUnit.SECONDS));

      consumer.unsubscribe(SHARED_SUBS);
      assertNull(getSubscriptionQueue(TOPIC1, "consumer1", SUB_NAME));
      assertNull(getSubscriptionQueue(TOPIC2, "consumer1", SUB_NAME));

      consumer.disconnect();
      consumer.close();
   }

   @Test(timeout = DEFAULT_TIMEOUT)
   public void testSharedSubscriptionQueueRemoval() throws Exception {
      final String TOPIC = "myTopic";
      final String SUB_NAME = "myShare";
      final String SHARED_SUB = MQTTUtil.SHARED_SUBSCRIPTION_PREFIX + SUB_NAME + "/" + TOPIC;
      ReusableLatch ackLatch = new ReusableLatch(1);

      MqttCallback mqttCallback = new DefaultMqttCallback() {
         @Override
         public void messageArrived(String topic, org.eclipse.paho.mqttv5.common.MqttMessage message) throws Exception {
            ackLatch.countDown();
         }
      };

      // create consumer 1
      MqttClient consumer1 = createPahoClient("consumer1");
      consumer1.connect();
      consumer1.setCallback(mqttCallback);
      consumer1.subscribe(SHARED_SUB, 1);

      // create consumer 2
      MqttClient consumer2 = createPahoClient("consumer2");
      consumer2.connect();
      consumer2.setCallback(mqttCallback);
      consumer2.subscribe(SHARED_SUB, 1);

      // verify there are 2 consumers on the subscription queue
      Queue sharedSubQueue = server.locateQueue(SUB_NAME.concat(".").concat(TOPIC));
      assertNotNull(sharedSubQueue);
      assertEquals(TOPIC, sharedSubQueue.getAddress().toString());
      assertEquals(2, sharedSubQueue.getConsumerCount());

      // send a message
      MqttClient producer = createPahoClient("producer");
      producer.connect();
      producer.publish(TOPIC, new byte[0], 1, false);

      // ensure one of the consumers receives the message
      assertTrue(ackLatch.await(2, TimeUnit.SECONDS));

      // disconnect the first consumer
      consumer1.disconnect();

      // verify there is 1 consumer on the subscription queue
      sharedSubQueue = server.locateQueue(SUB_NAME.concat(".").concat(TOPIC));
      assertNotNull(sharedSubQueue);
      assertEquals(TOPIC, sharedSubQueue.getAddress().toString());
      assertEquals(1, sharedSubQueue.getConsumerCount());

      // send a message and ensure the remaining consumer receives it
      ackLatch.countUp();
      producer.publish(TOPIC, new byte[0], 1, false);
      assertTrue(ackLatch.await(2, TimeUnit.SECONDS));

      // reconnect previous consumer
      consumer1.connect();
      consumer1.setCallback(mqttCallback);
      consumer1.subscribe(SHARED_SUB, 1);

      // send a message and ensure one of the consumers receives it
      ackLatch.countUp();
      producer.publish(TOPIC, new byte[0], 1, false);
      assertTrue(ackLatch.await(2, TimeUnit.SECONDS));

      producer.disconnect();
      producer.close();
      consumer1.disconnect();
      consumer1.close();
      consumer2.disconnect();
      consumer2.close();

      // verify the shared subscription queue is removed after all the subscribers disconnect
      Wait.assertTrue(() -> server.locateQueue(SUB_NAME.concat(".").concat(TOPIC)) == null, 2000, 100);
   }

   @Test(timeout = DEFAULT_TIMEOUT)
   public void testAutoDeleteAddressWithWildcardSubscription() throws Exception {
      String prefix = "topic";
      server.getAddressSettingsRepository().addMatch(prefix + ".#", new AddressSettings().setAutoDeleteAddresses(true).setAutoDeleteAddressesSkipUsageCheck(true));
      String topic = prefix + "/#";
      final int MESSAGE_COUNT = 100;
      final CountDownLatch latch = new CountDownLatch(MESSAGE_COUNT);

      MqttClient consumer = createPahoClient("consumer");
      consumer.connect();
      consumer.subscribe(topic, AT_LEAST_ONCE);
      consumer.setCallback(new LatchedMqttCallback(latch));

      MqttClient producer = createPahoClient("producer");
      producer.connect();

      List<String> addresses = new ArrayList<>();
      for (int i = 0; i < MESSAGE_COUNT; i++) {
         String address = prefix + "/" + RandomUtil.randomString();
         addresses.add(address.replace('/', '.'));
         producer.publish(address, new MqttMessage());
      }
      producer.disconnect();
      producer.close();

      assertTrue(latch.await(2, TimeUnit.SECONDS));

      for (String address : addresses) {
         assertNotNull(server.getAddressInfo(SimpleString.toSimpleString(address)));
      }

      PostOfficeTestAccessor.sweepAndReapAddresses((PostOfficeImpl) server.getPostOffice());

      for (String address : addresses) {
         assertNull(server.getAddressInfo(SimpleString.toSimpleString(address)));
      }

      consumer.disconnect();
      consumer.close();
   }
}
