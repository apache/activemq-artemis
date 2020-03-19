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

package org.apache.activemq.artemis.tests.integration.jms.consumer;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.Topic;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.logs.AssertionLoggerHandler;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.tests.util.JMSTestBase;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(value = Parameterized.class)
public class CreateSubscriptionTest extends JMSTestBase {


   private final String protocol;

   @Parameterized.Parameters(name = "persistenceEnabled = {0}")
   public static Iterable<? extends Object> persistenceEnabled() {
      return Arrays.asList(new Object[][]{{"AMQP"}, {"CORE"}});
   }

   public CreateSubscriptionTest(String protocol) {
      this.protocol = protocol;
   }

   @Test
   public void testSharedConsumer() throws Exception {

      server.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("myTopic")).addRoutingType(RoutingType.MULTICAST));
      ConnectionFactory cf = CFUtil.createConnectionFactory(protocol, "tcp://localhost:61616");
      Connection connection = cf.createConnection();
      Session session = connection.createSession();
      Connection connecton2 = cf.createConnection();
      Session session2 = connecton2.createSession();

      try {

         Topic topic = session.createTopic("myTopic");

         MessageConsumer messageConsumer = session.createSharedConsumer(topic, "consumer1");
         MessageConsumer messageConsumer2 = session2.createSharedConsumer(topic, "consumer1");



         connection.close();
      } finally {
         connection.close();
         connecton2.close();
      }
   }

   @Test
   public void testSharedDurableConsumer() throws Exception {

      server.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("myTopic")).addRoutingType(RoutingType.MULTICAST));
      ConnectionFactory cf = CFUtil.createConnectionFactory(protocol, "tcp://localhost:61616");
      Connection connection = cf.createConnection();
      Session session = connection.createSession();
      Connection connecton2 = cf.createConnection();
      Session session2 = connecton2.createSession();

      try {

         Topic topic = session.createTopic("myTopic");

         MessageConsumer messageConsumer = session.createSharedDurableConsumer(topic, "consumer1");
         MessageConsumer messageConsumer2 = session2.createSharedDurableConsumer(topic, "consumer1");



         connection.close();
      } finally {
         connection.close();
         connecton2.close();
      }
   }


   @Test
   public void testCreateManyConsumersDurable() throws Exception {
      testCreateManyConsumers("createSharedDurableConsumer");
   }

   @Test
   public void testCreateManyConsumersNonDurable() throws Exception {
      testCreateManyConsumers("createSharedConsumer");
   }

   @Test
   public void testDurableSubscriber() throws Exception {
      testCreateManyConsumers("createDurableSubscriber");
   }

   @Test
   public void testNonDurableSubscriber() throws Exception {
      testCreateManyConsumers("createConsumer");
   }

   public void testCreateManyConsumers(String queueType) throws Exception {

      AssertionLoggerHandler.startCapture();

      try {

         server.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("myTopic")).addRoutingType(RoutingType.MULTICAST));
         ConnectionFactory cf = CFUtil.createConnectionFactory(protocol, "tcp://localhost:61616");

         AtomicInteger errors = new AtomicInteger(0);

         Thread[] threads = new Thread[10];
         CyclicBarrier startBarrier = new CyclicBarrier(threads.length);
         CyclicBarrier closeBarrier = new CyclicBarrier(threads.length);

         Runnable runnable = new Runnable() {
            @Override
            public void run() {
               Connection connection = null;
               try {
                  connection = cf.createConnection();
                  if (queueType.equals("createDurableSubscriber")) {
                     connection.setClientID(UUID.randomUUID().toString());
                  }
                  Session session = connection.createSession();
                  Topic topic = session.createTopic("myTopic");
                  startBarrier.await(10, TimeUnit.SECONDS);

                  if (queueType.equals("createSharedDurableConsumer")) {
                     MessageConsumer messageConsumer = session.createSharedDurableConsumer(topic, "consumer1");
                  } else if (queueType.equals("createSharedConsumer")) {
                     MessageConsumer messageConsumer = session.createSharedConsumer(topic, "consumer1");
                  } else if (queueType.equals("createDurableSubscriber")) {
                     session.createDurableSubscriber(topic, "name", null, false);
                  } else if (queueType.equals("createDurableSubscriber")) {
                     session.createConsumer(topic);
                  }

               } catch (Exception e) {
                  e.printStackTrace();
                  errors.incrementAndGet();
               } finally {
                  try {
                     closeBarrier.await(10, TimeUnit.SECONDS);
                     if (connection != null) {
                        connection.close();
                     }
                  } catch (Exception ignored) {
                  }
               }

            }
         };

         for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(null, runnable, "test " + i);
            threads[i].start();
         }

         for (int i = 0; i < threads.length; i++) {
            threads[i].join();
         }

         Assert.assertEquals(0, errors.get());
         Assert.assertFalse(AssertionLoggerHandler.findText("AMQ229018"));
      } finally {
         AssertionLoggerHandler.stopCapture();
      }

   }

}
