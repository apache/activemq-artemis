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

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.tests.integration.openwire.BasicOpenWireTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * adapted from: org.apache.activemq.JmsConnectionStartStopTest
 */
public class JmsConnectionStartStopTest extends BasicOpenWireTest {

   private Connection startedConnection;
   private Connection stoppedConnection;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      startedConnection = factory.createConnection();
      startedConnection.start();
      stoppedConnection = factory.createConnection();
   }

   /**
    * @see junit.framework.TestCase#tearDown()
    */
   @Override
   @After
   public void tearDown() throws Exception {
      stoppedConnection.close();
      startedConnection.close();
      super.tearDown();
   }

   /**
    * Tests if the consumer receives the messages that were sent before the
    * connection was started.
    *
    * @throws JMSException
    */
   @Test
   public void testStoppedConsumerHoldsMessagesTillStarted() throws JMSException {
      Session startedSession = startedConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session stoppedSession = stoppedConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      // Setup the consumers.
      Topic topic = startedSession.createTopic("test");
      MessageConsumer startedConsumer = startedSession.createConsumer(topic);
      MessageConsumer stoppedConsumer = stoppedSession.createConsumer(topic);

      // Send the message.
      MessageProducer producer = startedSession.createProducer(topic);
      TextMessage message = startedSession.createTextMessage("Hello");
      producer.send(message);

      // Test the assertions.
      Message m = startedConsumer.receive(1000);
      assertNotNull(m);

      m = stoppedConsumer.receive(1000);
      assertNull(m);

      stoppedConnection.start();
      m = stoppedConsumer.receive(5000);
      assertNotNull(m);

      startedSession.close();
      stoppedSession.close();
   }

   /**
    * Tests if the consumer is able to receive messages eveb when the
    * connecction restarts multiple times.
    *
    * @throws Exception
    */
   @Test
   public void testMultipleConnectionStops() throws Exception {
      testStoppedConsumerHoldsMessagesTillStarted();
      stoppedConnection.stop();
      testStoppedConsumerHoldsMessagesTillStarted();
      stoppedConnection.stop();
      testStoppedConsumerHoldsMessagesTillStarted();
   }

   @Test
   public void testConcurrentSessionCreateWithStart() throws Exception {
      ThreadPoolExecutor executor = new ThreadPoolExecutor(50, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>());
      final Vector<Throwable> exceptions = new Vector<>();
      final AtomicInteger counter = new AtomicInteger(0);
      final Random rand = new Random();
      Runnable createSessionTask = new Runnable() {
         @Override
         public void run() {
            try {
               TimeUnit.MILLISECONDS.sleep(rand.nextInt(10));
               stoppedConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
               counter.incrementAndGet();
            } catch (Exception e) {
               exceptions.add(e);
            } catch (Throwable t) {
            }
         }
      };

      Runnable startStopTask = new Runnable() {
         @Override
         public void run() {
            try {
               TimeUnit.MILLISECONDS.sleep(rand.nextInt(10));
               stoppedConnection.start();
               stoppedConnection.stop();
            } catch (Exception e) {
               exceptions.add(e);
            } catch (Throwable t) {
            }
         }
      };

      for (int i = 0; i < 1000; i++) {
         executor.execute(createSessionTask);
         executor.execute(startStopTask);
      }

      executor.shutdown();

      assertTrue("executor terminated", executor.awaitTermination(30, TimeUnit.SECONDS));
      assertTrue("no exceptions: " + exceptions, exceptions.isEmpty());
   }

}
