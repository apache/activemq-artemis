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
package org.apache.activemq.artemis.tests.smoke.replicationflow;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.util.ServerUtil;
import org.apache.activemq.artemis.utils.ReusableLatch;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class ReplicatedFailbackSmokeTest extends ActiveMQTestBase {

   ArrayList<Consumer> consumers = new ArrayList<>();

   String server0Location = System.getProperty("basedir") + "/target/server0";
   String server1Location = System.getProperty("basedir") + "/target/server1";

   private static Process server0;

   private static Process server1;

   static final int NUM_MESSAGES = 300_000;
   static final int START_CONSUMERS = 100_000;
   static final int START_SERVER = 101_000;
   static final int NUMBER_OF_CONSUMERS = 10;
   static final ReusableLatch latch = new ReusableLatch(NUM_MESSAGES);

   static AtomicBoolean running = new AtomicBoolean(true);
   static AtomicInteger totalConsumed = new AtomicInteger(0);


   @Before
   public void cleanupTests() throws Exception {
      deleteDirectory(new File(server0Location, "data"));
      deleteDirectory(new File(server1Location, "data"));
      disableCheckThread();
   }

   @After
   public void after() throws Exception {
      ServerUtil.killServer(server0);
      ServerUtil.killServer(server1);
   }

   @Test
   public void testPageWhileSynchronizingReplica() throws Exception {
      internalTest(false);
   }

   @Ignore // need to fix this before I can let it running
   @Test
   public void testPageWhileSyncFailover() throws Exception {
      internalTest(true);
   }

   private void internalTest(boolean failover) throws Exception {

      int KILL_SERVER = failover ? 150_000 : -1;

      Connection connection = null;

      try {
         server0 = ServerUtil.startServer(server0Location, ReplicatedFailbackSmokeTest.class.getSimpleName() + "0", 0, 30000);

         ConnectionFactory connectionFactory = new ActiveMQConnectionFactory();

         connection = connectionFactory.createConnection();

         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

         Queue queue = session.createQueue("exampleQueue");

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
               startConsumers(!failover); // if failover, no AMQP
            }

            if (KILL_SERVER >= 0 && i == KILL_SERVER) {
               session.commit();
               System.out.println("Killing server");
               ServerUtil.killServer(server0);
               Thread.sleep(2000);
               connection.close();
               connection = connectionFactory.createConnection();

               session = connection.createSession(true, Session.SESSION_TRANSACTED);

               queue = session.createQueue("exampleQueue");

               producer = session.createProducer(queue);

            }

            if (i == START_SERVER) {
               System.out.println("Starting extra server");
               server1 = ServerUtil.startServer(server1Location, ReplicatedFailbackSmokeTest.class.getSimpleName() + "1", 1, 10000);
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

         for (Consumer consumer : consumers) {
            consumer.interrupt();
            consumer.join();
         }
      }
   }

   void startConsumers(boolean useAMQP) {
      for (int i = 0; i < NUMBER_OF_CONSUMERS; i++) {
         Consumer consumer = new Consumer(useAMQP && i % 2 == 0, i);
         consumer.start();
         consumers.add(consumer);
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
         if (connection != null) {
            connection.close();
         }
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
         try {
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
         } finally {
            try {
               connection.close();
            } catch (Throwable ignored) {
            }
         }

         System.out.println("Giving up the loop " + this);

      }

   }
}
