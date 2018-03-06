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
package org.apache.activemq.artemis.jms.example;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.naming.InitialContext;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.management.CoreNotificationType;
import org.apache.activemq.artemis.api.core.management.ManagementHelper;

/**
 * A simple JMS example that demonstrates the NOTIFY slow consumer policy
 *
 * - send messages to a queue "slow.consumer.notify".
 * - create a consumer on the topic "notify.topic" that has been configured as the broker's <b>management-notification-address</b>.
 * - start a consumer on "slow.consumer.notify" BUT does not consume any messages.
 * - consumer on "notify.topic" will receive a CONSUMER_SLOW management notification and signal to main thread.
 */
public class NotifySlowConsumerExample {

   public static final int WAIT_TIME = 10;

   public static void main(final String[] args) throws Exception {

      // Step 1. Create an initial context to perform the JNDI lookup
      InitialContext initialContext = new InitialContext();

      // Step 2. Perform a lookup on the queue and topic
      Queue slowConsumerNotifyQueue = (Queue) initialContext.lookup("queue/slow.consumer.notify");
      Topic notificationTopic = (Topic) initialContext.lookup("topic/notify.topic");

      // Step 3. Perform a lookup on the Connection Factory
      ConnectionFactory connectionFactory = (ConnectionFactory) initialContext.lookup("ConnectionFactory");

      // Step 4.Create a JMS Connection
      try (Connection connection = connectionFactory.createConnection()) {

         // Step 5. Create a JMS Session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 6. Create a JMS Message Producer
         MessageProducer producer = session.createProducer(slowConsumerNotifyQueue);

         CountDownLatch waitForSlowConsumerNotif = new CountDownLatch(1);

         // Step 7. Start the Connection
         connection.start();

         // Step 8. create a consumer on the broker's management-notification-address
         Session notifSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageConsumer notificationConsumer = notifSession.createConsumer(notificationTopic, ManagementHelper.HDR_NOTIFICATION_TYPE + " = '" + CoreNotificationType.CONSUMER_SLOW + "'");

         // Step 9. add a message listener to consumer listening to broker's management-notification-address,
         // when it receives notification it signals main thread.
         notificationConsumer.setMessageListener(message -> {
            System.out.println("SUCCESS! Received CONSUMER_SLOW notification as expected: " + message);
            //signal CONSUMER_SLOW notification received.
            waitForSlowConsumerNotif.countDown();
         });

         // Step 10. Create a Text Message
         TextMessage message = session.createTextMessage("This is a text message");

         // Step 11. Send the Message
         System.out.println("Sending messages to queue ... ");
         for (int i = 0; i < 50; i++) {
            producer.send(message);
         }

         // Step 12. Create a JMS Message Consumer
         MessageConsumer messageConsumer = session.createConsumer(slowConsumerNotifyQueue);

         System.out.println("About to wait for CONSUMER_SLOW notification, will timeout after " + WAIT_TIME + " seconds ...");

         // Step 13. wait for CONSUMER_SLOW notification
         boolean isNotified = waitForSlowConsumerNotif.await(WAIT_TIME, TimeUnit.SECONDS);

         // Step 14. ensure CONSUMER_SLOW notification was received and "waitForSlowConsumerNotif" did not timeout
         if (!isNotified) {
            throw new RuntimeException("SlowConsumerExample.demoSlowConsumerNotify() FAILED; timeout occurred before" + " - slow consumer notification was received. ");
         }
      }
   }
}