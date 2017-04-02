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

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.naming.InitialContext;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.util.ServerUtil;
import org.apache.activemq.artemis.utils.ReusableLatch;
import org.apache.qpid.jms.JmsConnectionFactory;

/**
 * Example of live and replicating backup pair.
 * <p>
 * After both servers are started, the live server is killed and the backup becomes active ("fails-over").
 * <p>
 * Later the live server is restarted and takes back its position by asking the backup to stop ("fail-back").
 */
public class ReplicatedFailbackStaticSmoke {

   private static Process server0;

   private static Process server1;

   static final int NUM_MESSAGES = 300_000;
   static final int START_CONSUMERS = 100_000;
   static final int START_SERVER = 101_000;
   static final int KILL_SERVER = -1; // not killing the server right now.. just for future use
   static final int NUMBER_OF_CONSUMERS = 10;
   static final ReusableLatch latch = new ReusableLatch(NUM_MESSAGES);

   static AtomicBoolean running = new AtomicBoolean(true);
   static AtomicInteger totalConsumed = new AtomicInteger(0);

   public static void main(final String[] args) throws Exception {

      Connection connection = null;

      InitialContext initialContext = null;

      try {
         server0 = ServerUtil.startServer(args[0], ReplicatedFailbackStaticSmoke.class.getSimpleName() + "0", 0, 30000);

         initialContext = new InitialContext();

         ConnectionFactory connectionFactory = (ConnectionFactory) initialContext.lookup("ConnectionFactory");

         connection = connectionFactory.createConnection();

         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

         Queue queue = session.createQueue("exampleQueue");

         connection.start();

         MessageProducer producer = session.createProducer(queue);

         BytesMessage bytesMessage = session.createBytesMessage();
         bytesMessage.writeBytes(new byte[20 * 1024]);

         for (int i = 0; i < NUM_MESSAGES; i++) {

            producer.send(bytesMessage);
            if (i % 1000 == 0) {
               System.out.println("Sent " + i + " messages, consumed=" + totalConsumed.get());
               session.commit();
            }

            if (i == START_CONSUMERS) {
               System.out.println("Starting consumers");
               startConsumers();
            }

            if (KILL_SERVER >= 0 && i == KILL_SERVER) {
               System.out.println("Killing server");
               ServerUtil.killServer(server0);
            }

            if (i == START_SERVER) {
               System.out.println("Starting extra server");
               server1 = ServerUtil.startServer(args[1], ReplicatedFailbackStaticSmoke.class.getSimpleName() + "1", 1, 10000);
            }

         }

         session.commit();

         System.out.println("Awaiting all consumers to finish");
         while (!latch.await(5, TimeUnit.SECONDS)) {
            System.out.println("Missing " + latch.getCount() + ", totalConsumed = " + totalConsumed);
         }

      } finally {

         running.set(false);

         if (connection != null) {
            connection.close();
         }

         if (initialContext != null) {
            initialContext.close();
         }

         ServerUtil.killServer(server0);
         ServerUtil.killServer(server1);
      }
   }

   static void startConsumers() {
      for (int i = 0; i < NUMBER_OF_CONSUMERS; i++) {
         Consumer consumer = new Consumer(i % 2 == 0, i);
         consumer.start();

      }
   }

   static class Consumer extends Thread {

      ConnectionFactory factory;
      Connection connection;
      Session session;
      Queue queue;
      MessageConsumer consumer;
      int count = 0;
      int totalCount = 0;

      final int consumerID;

      final boolean amqp;

      Consumer(boolean amqp, int id) {
         super("amqp=" + amqp + ", id=" + id);
         this.amqp = amqp;
         this.consumerID = id;
      }

      @Override
      public String toString() {
         return "Consumer " + consumerID + ", amqp::" + amqp;
      }

      void connect() throws Exception {
         count = 0;
         if (amqp) {
            factory = new JmsConnectionFactory("amqp://localhost:61616");
         } else {
            factory = new ActiveMQConnectionFactory(); // using default is fine here
         }

         connection = factory.createConnection();
         session = connection.createSession(true, Session.SESSION_TRANSACTED);
         queue = session.createQueue("exampleQueue");
         consumer = session.createConsumer(queue);
         connection.start();
      }

      @Override
      public void run() {
         while (running.get()) {
            try {
               if (connection == null) {
                  connect();
               }

               totalCount++;
               if (totalCount % 1000 == 0) {
                  System.out.println(this + " received " + totalCount + " messages");
               }

               BytesMessage message = (BytesMessage) consumer.receive(5000);
               if (message == null) {
                  System.out.println("Consumer " + this + " couldn't get a message");
                  if (count > 0) {
                     session.commit();
                     latch.countDown(count);
                     totalConsumed.addAndGet(count);
                     count = 0;
                  }
               } else {
                  count++;

                  if (count == 100) {
                     session.commit();
                     latch.countDown(count);
                     totalConsumed.addAndGet(count);
                     count = 0;
                  }
               }

            } catch (Exception e) {
               e.printStackTrace();
            }
         }

         System.out.println("Giving up the loop " + this);

      }

   }
}
