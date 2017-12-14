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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.SendAcknowledgementHandler;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.util.ServerUtil;

/**
 * Asynchronous Send Acknowledgements are an advanced feature of ActiveMQ Artemis which allow you to
 * receive acknowledgements that messages were successfully received at the server in a separate stream
 * to the stream of messages being sent to the server.
 * For more information please see the readme.html file
 */
public class SendAcknowledgementsExample {

   private static Process server0;
   private static final int numMessages = 30_000;
   private static final SimpleString queueName = SimpleString.toSimpleString("testQueue");

   public static void main(final String[] args) throws Exception {

      for (int i = 0; i < 500; i++) {
         System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ Running test " + i);
         server0 = ServerUtil.startServer(args[0], SendAcknowledgementsExample.class.getSimpleName() + "0", 0, 10000);
         sendMessages();

         server0 = ServerUtil.startServer(args[0], SendAcknowledgementsExample.class.getSimpleName() + "0", 0, 10000);
         consumeMessages();
      }
   }

   private static void sendMessages() throws Exception {
      try {

         ServerLocator locator = ActiveMQClient.createServerLocator("tcp://localhost:61616").setBlockOnDurableSend(false).setConfirmationWindowSize(1024 * 1024);

         ClientSessionFactory factory = locator.createSessionFactory();

         ClientSession session = factory.createSession(null, null, false, true, true, false, ActiveMQClient.DEFAULT_ACK_BATCH_SIZE);

         try {
            // Tried with and without the createAddress
            session.createAddress(queueName, RoutingType.MULTICAST, false);
            session.createQueue(queueName.toString(), RoutingType.MULTICAST, queueName.toString(), true);
         } catch (Exception e) {
         }

         ClientProducer producer = session.createProducer(queueName);

         CountDownLatch latch = new CountDownLatch(numMessages);

         for (int i = 0; i < numMessages; i++) {

            if (i % 10000 == 0) {
               System.out.println("Send " + i);
            }
            ClientMessage message = session.createMessage(true);
            message.getBodyBuffer().writeBytes("hello world".getBytes());

            // tried with producer.send(queueName, message, ...);; // didn't make a difference

            producer.send(message, new SendAcknowledgementHandler() {
               @Override
               public void sendAcknowledged(Message message) {
                  latch.countDown();
                  if (latch.getCount() % 10_000 == 0) {
                     System.out.println(latch.getCount() + " to go");
                  }
               }
            });
         }
         latch.await(10, TimeUnit.MINUTES);
      } finally {
         server0.destroy();
         server0.waitFor();
      }
   }

   private static void consumeMessages() throws Exception {
      try {

         ServerLocator locator = ActiveMQClient.createServerLocator("tcp://localhost:61616").setBlockOnDurableSend(false).setConfirmationWindowSize(-1);

         ClientSessionFactory factory = locator.createSessionFactory();

         ClientSession session = factory.createSession(null, null, false, false, false, false, ActiveMQClient.DEFAULT_ACK_BATCH_SIZE);

         ClientConsumer consumer = session.createConsumer(queueName);

         session.start();

         for (int i = 0; i < numMessages; i++) {

            if (i % 10000 == 0) {
               System.out.println("Received " + i);
            }

            ClientMessage message = consumer.receive(5000);
            message.acknowledge();

            if (message == null) {
               System.err.println("Expected message at " + i);
               System.exit(-1);
            }
         }

         session.commit();

         ClientMessage message = consumer.receiveImmediate();
         if (message != null) {
            System.err.println("Received too many messages");
            System.exit(-1);
         }

         session.close();
         locator.close();

      } finally {
         server0.destroy();
         server0.waitFor();
      }
   }

}
