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
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.ActiveMQConnectionFactory;

public class MessageListenerExample {

   public static void main(final String[] args) throws Exception {
      Connection connection = null;
      try {

         ConnectionFactory cf = new ActiveMQConnectionFactory();

         connection = cf.createConnection();

         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         Queue queue = session.createQueue("exampleQueue");

         int nMessages = 1000;

         CountDownLatch latch = new CountDownLatch(nMessages);
         MessageConsumer messageConsumer = session.createConsumer(queue);
         messageConsumer.setMessageListener(new LocalListener(latch));

         connection.start();

         MessageProducer producer = session.createProducer(queue);

         for (int i = 0; i < 1000; i++) {
            TextMessage message = session.createTextMessage("This is a text message " + i);

            System.out.println("Sent message: " + message.getText());

            producer.send(message);
         }

         if (!latch.await(5, TimeUnit.SECONDS)) {
            throw new RuntimeException("listener didn't receive all the messages");
         }

         System.out.println("Finished ok!");

      } finally {
         if (connection != null) {
            connection.close();
         }
      }
   }

   private static class LocalListener implements MessageListener {

      CountDownLatch latch;

      LocalListener(CountDownLatch latch) {
         this.latch = latch;
      }

      @Override
      public void onMessage(Message message) {
         latch.countDown();
         try {
            System.out.println("Received " + ((TextMessage) message).getText());
         } catch (Exception e) {
            e.printStackTrace();
         }
      }
   }
}
