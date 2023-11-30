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
package org.apache.activemq.artemis.tests.integration.openwire;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.RedeliveryPolicy;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.transport.tcp.TcpTransport;
import org.junit.Test;

public class FastReconnectOpenWireTest extends OpenWireTestBase {

   @Override
   protected void configureAddressSettings(Map<String, AddressSettings> addressSettingsMap) {
      super.configureAddressSettings(addressSettingsMap);
      // force send to dlq early
      addressSettingsMap.put("exampleQueue", new AddressSettings().setAutoCreateQueues(false).setAutoCreateAddresses(false).setDeadLetterAddress(new SimpleString("ActiveMQ.DLQ")).setAutoCreateAddresses(true).setMaxDeliveryAttempts(2));
      // force send to dlq late
      addressSettingsMap.put("exampleQueueTwo", new AddressSettings().setAutoCreateQueues(false).setAutoCreateAddresses(false).setDeadLetterAddress(new SimpleString("ActiveMQ.DLQ")).setAutoCreateAddresses(true).setMaxDeliveryAttempts(-1));
   }


   @Test(timeout = 80_000)
   public void testFastReconnectCreateConsumerNoErrors() throws Exception {

      final ArrayList<Throwable> errors = new ArrayList<>();
      SimpleString durableQueue = new SimpleString("exampleQueueTwo");
      this.server.createQueue(new QueueConfiguration(durableQueue).setRoutingType(RoutingType.ANYCAST));

      Queue queue = new ActiveMQQueue(durableQueue.toString());

      final ActiveMQConnectionFactory exFact = new ActiveMQConnectionFactory("failover:(tcp://localhost:61616?closeAsync=false)?startupMaxReconnectAttempts=-1&maxReconnectAttempts=-1&timeout=5000");
      exFact.setWatchTopicAdvisories(false);
      exFact.setConnectResponseTimeout(10000);
      exFact.setClientID("myID");

      RedeliveryPolicy redeliveryPolicy = new RedeliveryPolicy();
      redeliveryPolicy.setRedeliveryDelay(0);
      redeliveryPolicy.setMaximumRedeliveries(-1);
      exFact.setRedeliveryPolicy(redeliveryPolicy);

      publish(1000, durableQueue.toString());

      final AtomicInteger numIterations = new AtomicInteger(500);
      ExecutorService executor = Executors.newCachedThreadPool();

      final int concurrent = 2;
      for (int i = 0; i < concurrent; i++) {
         executor.execute(() -> {
            while (numIterations.decrementAndGet() > 0) {
               try (
                  Connection conn = exFact.createConnection();
                  Session consumerConnectionSession = conn.createSession(Session.SESSION_TRANSACTED);
                  MessageConsumer messageConsumer = consumerConnectionSession.createConsumer(queue)) {

                  messageConsumer.receiveNoWait();

                  if (numIterations.get() % 2 == 0) {
                     TimeUnit.MILLISECONDS.sleep(30);
                  } else {
                     TimeUnit.MILLISECONDS.sleep(50);
                  }

                  try {
                     // force a local socket close such that the broker sees an exception on the connection and fails the consumer via serverConsumer close
                     ((ActiveMQConnection) conn).getTransport().narrow(TcpTransport.class).stop();
                  } catch (Throwable expected) {
                  }

               } catch (javax.jms.InvalidClientIDException expected) {
                  // deliberate clash across concurrent connections
               } catch (Throwable unexpected) {
                  unexpected.printStackTrace();
                  errors.add(unexpected);
                  numIterations.set(0);
               }
            }
         });
      }

      executor.shutdown();
      assertTrue(executor.awaitTermination(60, TimeUnit.SECONDS));

      Wait.assertEquals(0, () -> server.locateQueue(durableQueue).getConsumerCount());
      assertTrue(errors.isEmpty());

   }

   @Test(timeout = 60_000)
   public void testFastReconnectCreateConsumerNoErrorsNoClientIdA() throws Exception {

      final ArrayList<Throwable> errors = new ArrayList<>();
      SimpleString durableQueue = new SimpleString("exampleQueueTwo");
      this.server.createQueue(new QueueConfiguration(durableQueue).setRoutingType(RoutingType.ANYCAST));

      Queue queue = new ActiveMQQueue(durableQueue.toString());

      final ActiveMQConnectionFactory exFact = new ActiveMQConnectionFactory("failover:(tcp://localhost:61616?closeAsync=false)?startupMaxReconnectAttempts=-1&maxReconnectAttempts=-1&timeout=5000");
      exFact.setWatchTopicAdvisories(false);
      exFact.setConnectResponseTimeout(10000);
      //exFact.setClientID("myID");

      RedeliveryPolicy redeliveryPolicy = new RedeliveryPolicy();
      redeliveryPolicy.setRedeliveryDelay(0);
      redeliveryPolicy.setMaximumRedeliveries(-1);
      exFact.setRedeliveryPolicy(redeliveryPolicy);

      publish(1000, durableQueue.toString());

      final AtomicInteger numIterations = new AtomicInteger(1000);
      ExecutorService executor = Executors.newCachedThreadPool();

      final int concurrent = 2;
      for (int i = 0; i < concurrent; i++) {
         executor.execute(() -> {
            while (numIterations.decrementAndGet() > 0) {
               try (Connection conn = exFact.createConnection(); Session consumerConnectionSession = conn.createSession(Session.SESSION_TRANSACTED); MessageConsumer messageConsumer = consumerConnectionSession.createConsumer(queue)) {

                  messageConsumer.receiveNoWait();

                  if (numIterations.get() % 2 == 0) {
                     TimeUnit.MILLISECONDS.sleep(30);
                  } else {
                     TimeUnit.MILLISECONDS.sleep(50);
                  }

                  try {
                     // force a local socket close such that the broker sees an exception on the connection and fails the consumer via serverConsumer close
                     ((ActiveMQConnection) conn).getTransport().narrow(TcpTransport.class).stop();
                  } catch (Throwable expected) {
                  }

               } catch (Throwable unexpected) {
                  unexpected.printStackTrace();
                  errors.add(unexpected);
                  numIterations.set(0);
               }
            }
         });
      }

      executor.shutdown();
      assertTrue(executor.awaitTermination(60, TimeUnit.SECONDS));

      Wait.assertEquals(0, () -> server.locateQueue(durableQueue).getConsumerCount());
      assertTrue(errors.isEmpty());

   }

   @Test(timeout = 60_000)
   public void testFastReconnectCreateConsumerNoErrorsNoClientId() throws Exception {

      final ArrayList<Throwable> errors = new ArrayList<>();
      SimpleString durableQueue = new SimpleString("exampleQueueTwo");
      this.server.createQueue(new QueueConfiguration(durableQueue).setRoutingType(RoutingType.ANYCAST));

      Queue queue = new ActiveMQQueue(durableQueue.toString());

      final ActiveMQConnectionFactory exFact = new ActiveMQConnectionFactory("failover:(tcp://localhost:61616?closeAsync=false)?startupMaxReconnectAttempts=-1&maxReconnectAttempts=-1&timeout=5000");
      exFact.setWatchTopicAdvisories(false);
      exFact.setConnectResponseTimeout(10000);

      RedeliveryPolicy redeliveryPolicy = new RedeliveryPolicy();
      redeliveryPolicy.setRedeliveryDelay(0);
      redeliveryPolicy.setMaximumRedeliveries(-1);
      exFact.setRedeliveryPolicy(redeliveryPolicy);

      publish(1000, durableQueue.toString());

      final AtomicInteger numIterations = new AtomicInteger(1000);
      ExecutorService executor = Executors.newCachedThreadPool();

      final int concurrent = 2;
      for (int i = 0; i < concurrent; i++) {
         executor.execute(() -> {
            try (Connection conn = exFact.createConnection();
                 Session consumerConnectionSession = conn.createSession(Session.SESSION_TRANSACTED);
                 MessageConsumer messageConsumer = consumerConnectionSession.createConsumer(queue)
            ) {
               conn.start();
               while (numIterations.decrementAndGet() > 0) {
                  try {
                     messageConsumer.receiveNoWait();

                     if (numIterations.get() % 2 == 0) {
                        TimeUnit.MILLISECONDS.sleep(30);
                     } else {
                        TimeUnit.MILLISECONDS.sleep(50);
                     }

                     try {
                        // force a local socket close such that the broker sees an exception on the connection and fails the consumer via serverConsumer close
                        ((ActiveMQConnection) conn).getTransport().narrow(TcpTransport.class).stop();
                     } catch (Throwable expected) {
                     }
                  } catch (Throwable unexpected) {
                     errors.add(unexpected);
                  }
               }
            } catch (javax.jms.InvalidClientIDException expected) {
               // deliberate clash across concurrent connections
            } catch (Throwable unexpected) {
               unexpected.printStackTrace();
               errors.add(unexpected);
               //   numIterations.set(0);
            }

         });
      }

      executor.shutdown();
      assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));

      Wait.assertEquals(0, () -> server.locateQueue(durableQueue).getConsumerCount());

      assertTrue(errors.isEmpty());

   }

   private void publish(int numMessages, String name) throws Exception {
      final ActiveMQConnectionFactory exFact = new ActiveMQConnectionFactory();
      Connection exConn = exFact.createConnection();
      exConn.start();

      Queue queue = new ActiveMQQueue(name);

      // put a few messages on the queue to have the broker do some dispatch work
      Session session = exConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer producer = session.createProducer(queue);

      TextMessage message = session.createTextMessage("This is a text message");
      for (int i = 0; i < numMessages; i++) {
         message.setIntProperty("SEQ", i);
         producer.send(message);
      }
      session.close();
      exConn.close();

   }
}
