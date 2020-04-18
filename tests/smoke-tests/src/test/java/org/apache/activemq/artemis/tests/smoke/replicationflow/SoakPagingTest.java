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
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.tests.smoke.common.SmokeTestBase;
import org.apache.activemq.artemis.utils.RetryRule;
import org.apache.activemq.artemis.utils.SpawnedVMSupport;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class SoakPagingTest extends SmokeTestBase {

   @Rule
   public RetryRule retryRule = new RetryRule(1);

   public static final int LAG_CONSUMER_TIME = 1000;
   public static final int TIME_RUNNING = 4000;
   public static final int CLIENT_KILLS = 2;

   String protocol;
   String consumerType;
   boolean transaction;
   final String destination;

   public SoakPagingTest(String protocol, String consumerType, boolean transaction) {
      this.protocol = protocol;
      this.consumerType = consumerType;
      this.transaction = transaction;

      if (consumerType.equals("queue")) {
         destination = "exampleQueue";
      } else {
         destination = "exampleTopic";
      }
   }

   @Parameterized.Parameters(name = "protocol={0}, type={1}, tx={2}")
   public static Collection<Object[]> getParams() {
      return Arrays.asList(new Object[][]{{"AMQP", "shared", false}, {"AMQP", "queue", false}, {"OPENWIRE", "topic", false}, {"OPENWIRE", "queue", false}, {"CORE", "shared", false}, {"CORE", "queue", false},
         {"AMQP", "shared", true}, {"AMQP", "queue", true}, {"OPENWIRE", "topic", true}, {"OPENWIRE", "queue", true}, {"CORE", "shared", true}, {"CORE", "queue", true}});
   }

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
   }

   static final int consumer_threads = 20;
   static final int producer_threads = 20;
   static AtomicInteger j = new AtomicInteger(0);

   private static ConnectionFactory createConnectionFactory(String protocol, String uri) {
      if (protocol.toUpperCase().equals("OPENWIRE")) {
         return new org.apache.activemq.ActiveMQConnectionFactory("failover:(" + uri + ")");
      } else if (protocol.toUpperCase().equals("AMQP")) {

         if (uri.startsWith("tcp://")) {
            // replacing tcp:// by amqp://
            uri = "amqp" + uri.substring(3);

         }
         return new JmsConnectionFactory(uri);
      } else if (protocol.toUpperCase().equals("CORE") || protocol.toUpperCase().equals("ARTEMIS")) {
         return new org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory(uri);
      } else {
         throw new IllegalStateException("Unkown:" + protocol);
      }
   }

   public static void main(String[] arg) {
      try {

         if (arg.length != 4) {
            System.err.println("You need to pass in protocol, consumerType, Time, transaction");
            System.exit(0);
         }

         String protocol = arg[0];
         String consumerType = arg[1];
         int time = Integer.parseInt(arg[2]);
         boolean tx = Boolean.parseBoolean(arg[3]);
         if (time == 0) {
            time = 15000;
         }

         final String host = "localhost";
         final int port = 61616;

         final ConnectionFactory factory = createConnectionFactory(protocol, "tcp://" + host + ":" + port);

         for (int i = 0; i < producer_threads; i++) {
            Thread t = new Thread(new Runnable() {
               @Override
               public void run() {
                  SoakPagingTest app = new SoakPagingTest(protocol, consumerType, tx);
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
                  SoakPagingTest app = new SoakPagingTest(protocol, consumerType, tx);
                  app.consume(factory, j.getAndIncrement());
               }
            });
            t.start();
         }
         Thread.sleep(time);

         System.exit(consumed.get() > 0 ? 1 : 0);
      } catch (Throwable e) {
         e.printStackTrace();
         System.exit(0);
      }

   }

   @Test
   public void testPagingReplication() throws Throwable {

      server1 = startServer(SERVER_NAME_1, 0, 30000);

      for (int i = 0; i < CLIENT_KILLS; i++) {
         Process process = SpawnedVMSupport.spawnVM(SoakPagingTest.class.getName(), protocol, consumerType, "" + TIME_RUNNING, "" + transaction);

         int result = process.waitFor();
         Assert.assertTrue(result > 0);
      }
   }

   public void produce(ConnectionFactory factory) {
      try {

         StringBuffer bufferlarge = new StringBuffer();
         while (bufferlarge.length() < 110000) {
            bufferlarge.append("asdflkajdhsf akljsdfh akljsdfh alksjdfh alkdjsf ");
         }
         Connection connection = factory.createConnection("admin", "admin");

         connection.start();
         final Session session;

         if (transaction) {
            session = connection.createSession(true, Session.SESSION_TRANSACTED);
         } else {
            session = connection.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
         }

         Destination address;

         if (consumerType.equals("queue")) {
            address = session.createQueue(destination);
         } else {
            address = session.createTopic(destination);
         }

         MessageProducer messageProducer = session.createProducer(address);

         int i = 0;
         while (true) {

            Message message;
            if (i % 100 == 0) {
               message = session.createTextMessage(bufferlarge.toString());
            } else {
               message = session.createTextMessage("fkjdslkfjdskljf;lkdsjf;kdsajf;lkjdf;kdsajf;kjdsa;flkjdsa;lfkjdsa;flkj;dsakjf;dsajf;askjd;fkj;dsajflaskfja;fdlkajs;lfdkja;kfj;dsakfj;akdsjf;dsakjf;akfj;lakdsjf;lkasjdf;ksajf;kjdsa;fkj;adskjf;akdsjf;kja;sdkfj;akdsjf;akjdsf;adskjf;akdsjf;askfj;aksjfkdjafndmnfmdsnfjadshfjdsalkfjads;fkjdsa;kfja;skfj;akjfd;akjfd;ksaj;fkja;kfj;dsakjf;dsakjf;dksjf;akdsjf;kdsajf");
            }

            messageProducer.send(message);
            produced.incrementAndGet();
            i++;
            if (i % 100 == 0) {
               System.out.println("Published " + i + " messages");
               if (transaction) {
                  session.commit();
               }
            }
         }
      } catch (Exception e) {
         e.printStackTrace();
      }
   }

   public void consume(ConnectionFactory factory, int j) {
      try {
         Connection connection = factory.createConnection("admin", "admin");

         final Session session;

         if (transaction) {
            session = connection.createSession(true, Session.SESSION_TRANSACTED);
         } else {
            session = connection.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
         }

         Destination address;

         if (consumerType.equals("queue")) {
            address = session.createQueue(destination);
         } else {
            address = session.createTopic(destination);
         }

         String consumerId = "ss" + (j % 5);
         MessageConsumer messageConsumer;

         if (protocol.equals("shared")) {
            messageConsumer = session.createSharedConsumer((Topic)address, consumerId);
         } else {
            messageConsumer = session.createConsumer(address);
         }

         if (LAG_CONSUMER_TIME > 0) Thread.sleep(LAG_CONSUMER_TIME);

         connection.start();

         int i = 0;
         while (true) {
            Message m = messageConsumer.receive(1000);
            consumed.incrementAndGet();
            if (m == null)
               System.out.println("receive() returned null");
            i++;
            if (i % 100 == 0) {
               System.out.println("Consumed " + i + " messages");
               if (transaction) {
                  session.commit();
               }
            }
         }
      } catch (Exception e) {
         e.printStackTrace();
      }
   }
}
