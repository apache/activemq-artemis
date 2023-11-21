/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.client;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.client.impl.ClientConsumerImpl;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test for slow consumer handling getting influenced by thread pool exhaustion.
 * See ARTEMIS-4522.
 * <p/>
 * Test is easily reproducing the issue described in ARTEMIS-4522 with settings of 2000 {@link  #NUM_OF_MESSAGES}, 100 {@link #CONSUMER_COUNT} and 50 {@link #THREAD_POOL_SIZE_CLIENT} on an 8 core machine.
 * Fewer messages and fewer consumers make the probability lower for the issue to appear.
 * More client-threads make the issue less likely to appear.
 * Theory: {@link #THREAD_POOL_SIZE_CLIENT} being larger than {@link #CONSUMER_COUNT} will make the issue impossible to appear.
 * <p/>
 * With 2000 messages, the test usually runs into the issue of ClientConsumerImpl#startSlowConsumer reaching its 10-second timeout (or taking significant long between 1 and 10 seconds)
 * on pendingFlowControl#await after about 1200-1500 messages.
 * Visible in log as 10 second pause before next bulk of messages get processed.
 */
public class ConsumerSlowConsumerTest extends ActiveMQTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
   private final SimpleString QUEUE = new SimpleString("SlowConsumerTestQueue");

   private ServerLocator locator;

   private static final int WINDOW_SIZE = 0;

   private static final int NUM_OF_MESSAGES = 2_000;
   private static final int CONSUMER_COUNT = 500;
   private static final int THREAD_POOL_SIZE_CLIENT = 5;

   private volatile boolean stopped = false;

   private final Set<ClientConsumerImpl> consumers = ConcurrentHashMap.newKeySet();
   private final Set<ClientSession> sessions = ConcurrentHashMap.newKeySet();


   protected boolean isNetty() {
      return false;
   }


   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      locator = createFactory(isNetty());
      locator.setConsumerWindowSize(WINDOW_SIZE);
      locator.setUseGlobalPools(false);
      locator.setThreadPoolMaxSize(THREAD_POOL_SIZE_CLIENT);
   }


   @Test
   public void testSlowConsumer() throws Exception {
      ActiveMQServer messagingService = createServer(false, isNetty());

      messagingService.start();
      messagingService.createQueue(new QueueConfiguration(QUEUE).setRoutingType(RoutingType.ANYCAST));

      ClientSessionFactory cf = createSessionFactory(locator);

      AtomicInteger sentMessages = new AtomicInteger(0);
      sendMessages(cf, sentMessages);

      AtomicInteger receivedMessages = new AtomicInteger(0);
      createConsumers(cf, receivedMessages);

      final long startTime = System.currentTimeMillis();
      // allow for duration of 5ms per message (neglecting concurrency, which makes it even a lot faster)
      // typical runtime for 1000 messages 100 consumers and 50 threads without issues is 160ms, so deadline is very generous
      final long deadLine = System.currentTimeMillis() +  sentMessages.get() *  5L;
      int counter = 0;
      while (receivedMessages.get() < sentMessages.get() && System.currentTimeMillis() < deadLine) {
         Thread.sleep(5);
         counter++;
         if (counter % 1000 == 0) {
            logger.info("Waiting for " + (sentMessages.get() - receivedMessages.get()) + " more messages...");
         }
      }
      final long endTime = System.currentTimeMillis();
      stopped = true; // signal stop to potentially still running consumer-creation thread

      // check amount of sent and received messages
      if (receivedMessages.get() < sentMessages.get()) {
         logger.error("Received only " + receivedMessages.get() + " messages out of " + sentMessages.get());
      } else {
         logger.info("Received all " + receivedMessages.get() + " messages");
      }
      assertEquals(sentMessages.get(), receivedMessages.get());

      final long duration = endTime - startTime;
      logger.info("Test took " + duration + " ms");

      long expectedDuration = NUM_OF_MESSAGES * 10;

      assertTrue("Test took " + duration + " ms, expected " + expectedDuration + " ms", duration < expectedDuration);

      cleanup(cf, messagingService);
   }

   private void cleanup(ClientSessionFactory cf, ActiveMQServer messagingService) throws Exception {
      consumers.parallelStream().forEach(c -> {
         try {
            c.close();
         } catch (ActiveMQException e) {
            //ignore
         }
      });
      logger.info("Closed " + consumers.size() + " consumers");
      sessions.parallelStream().forEach(s -> {
         try {
            s.close();
         } catch (ActiveMQException e) {
            //ignore
         }
      });
      logger.info("Closed " + sessions.size() + " sessions");

      cf.close();
      messagingService.stop();

      logger.info("Cleaned up.");
   }

   private void sendMessages(ClientSessionFactory cf, AtomicInteger sentMessages) throws ActiveMQException {
      logger.info("Creating " + NUM_OF_MESSAGES + " messages...");

      try (ClientSession sendingSession = cf.createSession(false, true, true);
          ClientProducer producer = sendingSession.createProducer(QUEUE)) {
         for (int i = 0; i < NUM_OF_MESSAGES; i++) {
            ClientMessage message = createTextMessage(sendingSession, "m" + i);
            producer.send(message);
            sentMessages.incrementAndGet();
         }
         logger.info("Created " + NUM_OF_MESSAGES + " messages");
      }
   }

   private void createConsumers(ClientSessionFactory cf, AtomicInteger receivedMessages) throws ActiveMQException {
      Thread consumerCreator = new Thread() {
         @Override
         public void run() {
            logger.info("Creating " + CONSUMER_COUNT + " consumers...");
            try {
               for (int i = 0; i < CONSUMER_COUNT; i++) {
                  if (stopped) {
                     logger.info("Stopping consumer creation, since test has ended already. Created " + i + " out of " + CONSUMER_COUNT + " consumers so far.");
                     return;
                  }

                  ClientSession session = cf.createSession(false, true, true);
                  ClientConsumerImpl consumer = (ClientConsumerImpl) session.createConsumer(QUEUE);
                  sessions.add(session);
                  consumers.add(consumer);

                  assertEquals(WINDOW_SIZE == 0 ? 0 : WINDOW_SIZE / 2, consumer.getClientWindowSize());
                  String consumerId = "[" + i + "]";
                  consumer.setMessageHandler(message -> {
                     Thread.yield(); // simulate processing and yield to other threads
                     receivedMessages.incrementAndGet();
                     //instanceLog.info(consumerId + "\t- Received message: " + message.getMessageID());
                  });

                  session.start();
               }

               logger.info("Created all " + CONSUMER_COUNT + " consumers.");
            } catch (Exception ex) {
               logger.error("Error creating consumers!", ex);
               //fail("Error creating consumers!");
            }
         }
      };

      consumerCreator.start();
   }

}
