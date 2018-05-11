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

package org.apache.activemq.artemis.tests.smoke.crossprotocol;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.IllegalStateException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.tests.smoke.common.SmokeTestBase;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiThreadConvertTest extends SmokeTestBase {

   private static final String SERVER_NAME_0 = "standard";

   private static final Logger LOG = LoggerFactory.getLogger(MultiThreadConvertTest.class);

   @Before
   public void before() throws Exception {
      cleanupData(SERVER_NAME_0);
      disableCheckThread();
      startServer(SERVER_NAME_0, 0, 30000);
   }

   protected TransportConfiguration addAcceptorConfiguration(ActiveMQServer server, int port) {
      HashMap<String, Object> params = new HashMap<>();
      params.put(TransportConstants.PORT_PROP_NAME, String.valueOf(port));
      params.put(TransportConstants.PROTOCOLS_PROP_NAME, "AMQP");
      HashMap<String, Object> amqpParams = new HashMap<>();

      return new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, params, "netty-amqp-acceptor", amqpParams);
   }

   public String getTopicName() {
      return "test-topic-1";
   }

   @Override
   @After
   public void tearDown() throws Exception {
      super.tearDown();
   }

   @Test(timeout = 60000)
   public void testSendLotsOfDurableMessagesOnTopicWithManySubscribersPersistent() throws Exception {
      doTestSendLotsOfDurableMessagesOnTopicWithManySubscribers(DeliveryMode.PERSISTENT);
   }

   @Test(timeout = 60000)
   public void testSendLotsOfDurableMessagesOnTopicWithManySubscribersNonPersistent() throws Exception {
      doTestSendLotsOfDurableMessagesOnTopicWithManySubscribers(DeliveryMode.NON_PERSISTENT);
   }

   private void doTestSendLotsOfDurableMessagesOnTopicWithManySubscribers(int durability) throws Exception {

      final int MSG_COUNT = 400;
      final int SUBSCRIBER_COUNT = 4;
      final int DELIVERY_MODE = durability;

      JmsConnectionFactory amqpFactory = new JmsConnectionFactory("amqp://127.0.0.1:5672");
      ActiveMQConnectionFactory coreFactory = new ActiveMQConnectionFactory("tcp://127.0.0.1:61616");

      Connection amqpConnection = amqpFactory.createConnection();
      final ExecutorService executor = Executors.newFixedThreadPool(SUBSCRIBER_COUNT);

      try {
         final CountDownLatch subscribed = new CountDownLatch(SUBSCRIBER_COUNT);
         final CountDownLatch done = new CountDownLatch(MSG_COUNT * SUBSCRIBER_COUNT);
         final AtomicBoolean error = new AtomicBoolean(false);

         for (int i = 0; i < SUBSCRIBER_COUNT; ++i) {
            executor.execute(() -> {
               Session coreSession = null;
               Connection coreConnection = null;
               try {
                  coreConnection = coreFactory.createConnection();
                  coreSession = coreConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                  Topic topic = coreSession.createTopic(getTopicName());
                  MessageConsumer coreConsumer = coreSession.createConsumer(topic);

                  subscribed.countDown(); // Signal ready

                  coreConnection.start();

                  for (int j = 0; j < MSG_COUNT; j++) {
                     Message received = coreConsumer.receive(TimeUnit.SECONDS.toMillis(5));
                     done.countDown();

                     if (received.getJMSDeliveryMode() != DELIVERY_MODE) {
                        throw new IllegalStateException("Message durability state is not corret.");
                     }
                  }

               } catch (Throwable t) {
                  LOG.error("Error during message consumption: ", t);
                  error.set(true);
               } finally {
                  try {
                     coreConnection.close();
                  } catch (Throwable e) {
                  }
               }
            });
         }

         assertTrue("Receivers didn't signal ready", subscribed.await(10, TimeUnit.SECONDS));

         // Send using AMQP and receive using Core JMS client.
         Session amqpSession = amqpConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Topic topic = amqpSession.createTopic(getTopicName());
         MessageProducer producer = amqpSession.createProducer(topic);
         producer.setDeliveryMode(DELIVERY_MODE);

         for (int i = 0; i < MSG_COUNT; i++) {
            TextMessage message = amqpSession.createTextMessage("test");
            message.setJMSCorrelationID(UUID.randomUUID().toString());
            producer.send(message);
         }

         assertTrue("did not read all messages, waiting on: " + done.getCount(), done.await(30, TimeUnit.SECONDS));
         assertFalse("should not be any errors on receive", error.get());
      } finally {
         try {
            amqpConnection.close();
         } catch (Exception e) {
         }

         executor.shutdown();
         coreFactory.close();
      }
   }
}
