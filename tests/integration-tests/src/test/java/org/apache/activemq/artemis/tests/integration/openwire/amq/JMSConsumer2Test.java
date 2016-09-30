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
package org.apache.activemq.artemis.tests.integration.openwire.amq;

import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.ActiveMQMessageConsumer;
import org.apache.activemq.artemis.tests.integration.openwire.BasicOpenWireTest;
import org.apache.activemq.artemis.utils.ActiveMQThreadFactory;
import org.apache.activemq.command.ActiveMQDestination;
import org.junit.Test;

/**
 * adapted from: org.apache.activemq.JMSConsumerTest
 */
public class JMSConsumer2Test extends BasicOpenWireTest {

   @Test
   public void testMessageListenerWithConsumerCanBeStoppedConcurently() throws Exception {

      final AtomicInteger counter = new AtomicInteger(0);
      final CountDownLatch closeDone = new CountDownLatch(1);

      connection.start();
      Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      ActiveMQDestination destination = createDestination(session, ActiveMQDestination.QUEUE_TYPE);

      // preload the queue
      sendMessages(session, destination, 2000);

      final ActiveMQMessageConsumer consumer = (ActiveMQMessageConsumer) session.createConsumer(destination);

      final Map<Thread, Throwable> exceptions = Collections.synchronizedMap(new HashMap<Thread, Throwable>());
      Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler() {
         @Override
         public void uncaughtException(Thread t, Throwable e) {
            exceptions.put(t, e);
         }
      });

      final class AckAndClose implements Runnable {

         private final Message message;

         AckAndClose(Message m) {
            this.message = m;
         }

         @Override
         public void run() {
            try {
               int count = counter.incrementAndGet();
               if (count == 590) {
                  // close in a separate thread is ok by jms
                  consumer.close();
                  closeDone.countDown();
               }
               if (count % 200 == 0) {
                  // ensure there are some outstanding messages
                  // ack every 200
                  message.acknowledge();
               }
            } catch (Exception e) {
               exceptions.put(Thread.currentThread(), e);
            }
         }
      }

      final ExecutorService executor = Executors.newCachedThreadPool(ActiveMQThreadFactory.defaultThreadFactory());
      consumer.setMessageListener(new MessageListener() {
         @Override
         public void onMessage(Message m) {
            // ack and close eventually in separate thread
            executor.execute(new AckAndClose(m));
         }
      });

      assertTrue(closeDone.await(20, TimeUnit.SECONDS));
      // await possible exceptions
      Thread.sleep(1000);
      assertTrue("no exceptions: " + exceptions, exceptions.isEmpty());
   }

   @Test
   public void testDupsOkConsumer() throws Exception {

      // Receive a message with the JMS API
      connection.start();
      Session session = connection.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
      ActiveMQDestination destination = createDestination(session, ActiveMQDestination.QUEUE_TYPE);
      MessageConsumer consumer = session.createConsumer(destination);

      // Send the messages
      sendMessages(session, destination, 4);

      // Make sure only 4 message are delivered.
      for (int i = 0; i < 4; i++) {
         Message m = consumer.receive(1000);
         assertNotNull(m);
      }
      assertNull(consumer.receive(1000));

      // Close out the consumer.. no other messages should be left on the queue.
      consumer.close();

      consumer = session.createConsumer(destination);
      assertNull(consumer.receive(1000));
   }

   @Test
   public void testRedispatchOfUncommittedTx() throws Exception {
      connection.start();
      Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
      ActiveMQDestination destination = createDestination(session, ActiveMQDestination.QUEUE_TYPE);

      sendMessages(connection, destination, 2);

      MessageConsumer consumer = session.createConsumer(destination);
      Message m = consumer.receive(1000);
      System.out.println("m1 received: " + m);
      assertNotNull(m);
      m = consumer.receive(5000);
      System.out.println("m2 received: " + m);
      assertNotNull(m);

      // install another consumer while message dispatch is unacked/uncommitted
      Session redispatchSession = connection.createSession(true, Session.SESSION_TRANSACTED);
      MessageConsumer redispatchConsumer = redispatchSession.createConsumer(destination);
      System.out.println("redispatch consumer: " + redispatchConsumer);

      // no commit so will auto rollback and get re-dispatched to
      // redisptachConsumer
      System.out.println("closing session: " + session);
      session.close();

      Message msg = redispatchConsumer.receive(3000);
      assertNotNull(msg);

      assertTrue("redelivered flag set", msg.getJMSRedelivered());
      assertEquals(2, msg.getLongProperty("JMSXDeliveryCount"));

      msg = redispatchConsumer.receive(1000);
      assertNotNull(msg);
      assertTrue(msg.getJMSRedelivered());
      assertEquals(2, msg.getLongProperty("JMSXDeliveryCount"));
      redispatchSession.commit();

      assertNull(redispatchConsumer.receive(500));
      System.out.println("closing dispatch session: " + redispatchSession);
      redispatchSession.close();
   }

   @Test
   public void testRedispatchOfRolledbackTx() throws Exception {

      connection.start();
      Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
      ActiveMQDestination destination = createDestination(session, ActiveMQDestination.QUEUE_TYPE);

      sendMessages(connection, destination, 2);

      MessageConsumer consumer = session.createConsumer(destination);
      assertNotNull(consumer.receive(1000));
      assertNotNull(consumer.receive(1000));

      // install another consumer while message dispatch is unacked/uncommitted
      Session redispatchSession = connection.createSession(true, Session.SESSION_TRANSACTED);
      MessageConsumer redispatchConsumer = redispatchSession.createConsumer(destination);

      session.rollback();
      session.close();

      Message msg = redispatchConsumer.receive(1000);
      assertNotNull(msg);
      assertTrue(msg.getJMSRedelivered());
      assertEquals(2, msg.getLongProperty("JMSXDeliveryCount"));
      msg = redispatchConsumer.receive(1000);
      assertNotNull(msg);
      assertTrue(msg.getJMSRedelivered());
      assertEquals(2, msg.getLongProperty("JMSXDeliveryCount"));
      redispatchSession.commit();

      assertNull(redispatchConsumer.receive(500));
      redispatchSession.close();
   }

}
