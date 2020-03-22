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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.tests.smoke.common.SmokeTestBase;
import org.apache.activemq.artemis.utils.SpawnedVMSupport;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SoakPagingTest extends SmokeTestBase {
   public static final String SERVER_NAME_0 = "replicated-static0";
   public static final String SERVER_NAME_1 = "replicated-static1";

   static AtomicInteger produced = new AtomicInteger(0);
   static AtomicInteger consumed = new AtomicInteger(0);
   private static Process server0;

   private static Process server1;
   @Before
   public void before() throws Exception {
      cleanupData(SERVER_NAME_0);
      cleanupData(SERVER_NAME_1);

      server0 = startServer(SERVER_NAME_0, 0, 30000);
      server1 = startServer(SERVER_NAME_1, 0, 30000);
   }

   final String destination = "exampleTopic";
   static final int consumer_threads = 20;
   static final int producer_threads = 20;
   static AtomicInteger j = new AtomicInteger(0);


   public static void main(String[] arg) {
      try {
         final String host = "localhost";
         final int port = 61616;

         final ConnectionFactory factory = new org.apache.qpid.jms.JmsConnectionFactory("failover:(amqp://" + host + ":" + port + ")");

         for (int i = 0; i < producer_threads; i++) {
            Thread t = new Thread(new Runnable() {
               @Override
               public void run() {
                  SoakPagingTest app = new SoakPagingTest();
                  app.produce(factory);
               }
            });
            t.start();
         }

         Thread.sleep(1000);

         for (int i = 0; i < consumer_threads; i++) {
            Thread t = new Thread(new Runnable() {
               @Override
               public void run() {
                  SoakPagingTest app = new SoakPagingTest();
                  app.consume(factory, j.getAndIncrement());
               }
            });
            t.start();
         }
         Thread.sleep(15000);

         System.exit(consumed.get());
      } catch (Exception e) {
         e.printStackTrace();
      }

   }

   @Test
   public void testPagingReplication() throws Throwable {
      for (int i = 0; i < 3; i++) {
         Process process = SpawnedVMSupport.spawnVM(SoakPagingTest.class.getName());
         Assert.assertTrue(process.waitFor() > 0);
      }

      server1.destroy();

      server1 = startServer(SERVER_NAME_1, 0, 30000);

      for (int i = 0; i < 2; i++) {
         Process process = SpawnedVMSupport.spawnVM(SoakPagingTest.class.getName());
         Assert.assertTrue(process.waitFor() > 0);
      }
   }

   public void produce(ConnectionFactory factory) {
      try {
         Connection connection = factory.createConnection("admin", "admin");

         connection.start();
         final Session session = connection.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);

         Destination address = session.createTopic(destination);
         MessageProducer messageProducer = session.createProducer(address);

         int i = 0;
         while (true) {
            Message message = session.createTextMessage("fkjdslkfjdskljf;lkdsjf;kdsajf;lkjdf;kdsajf;kjdsa;flkjdsa;lfkjdsa;flkj;dsakjf;dsajf;askjd;fkj;dsajflaskfja;fdlkajs;lfdkja;kfj;dsakfj;akdsjf;dsakjf;akfj;lakdsjf;lkasjdf;ksajf;kjdsa;fkj;adskjf;akdsjf;kja;sdkfj;akdsjf;akjdsf;adskjf;akdsjf;askfj;aksjfkdjafndmnfmdsnfjadshfjdsalkfjads;fkjdsa;kfja;skfj;akjfd;akjfd;ksaj;fkja;kfj;dsakjf;dsakjf;dksjf;akdsjf;kdsajf");

            messageProducer.send(message);
            produced.incrementAndGet();
            i++;
            if (i % 100 == 0)
               System.out.println("Published " + i + " messages");
         }
      } catch (Exception e) {
         e.printStackTrace();
      }
   }

   public void consume(ConnectionFactory factory, int j) {
      try {
         Connection connection = factory.createConnection("admin", "admin");

         final Session session = connection.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);

         Topic address = session.createTopic(destination);
         String consumerId = "ss" + (j % 5);
         MessageConsumer messageConsumer = session.createSharedConsumer(address, consumerId);

         Thread.sleep(5000);
         connection.start();

         int i = 0;
         while (true) {
            Message m = messageConsumer.receive(1000);
            consumed.incrementAndGet();
            if (m == null)
               System.out.println("receive() returned null");
            i++;
            if (i % 100 == 0)
               System.out.println("Consumed " + i + " messages");
         }
      } catch (Exception e) {
         e.printStackTrace();
      }
   }
}
