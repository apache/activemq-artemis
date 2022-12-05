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

package org.apache.activemq.artemis.tests.soak.client;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LargeMessageSoakTest extends ActiveMQTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   ActiveMQServer server;

   @Override
   public void setUp() throws Exception {
      super.setUp();

      this.server = this.createServer(true, true);
      server.start();
   }

   @Test
   public void testAMQP() throws Exception {
      testSendReceive("AMQP");
   }

   @Test
   public void testCORE() throws Exception {
      testSendReceive("CORE");
   }

   @Test
   public void testOpenWire() throws Exception {
      testSendReceive("OPENWIRE");
   }

   public void testSendReceive(String protocol) throws Exception {
      AtomicInteger errors = new AtomicInteger(0);

      final int THREADS = 5;
      final int MESSAGE_COUNT = 5;
      final int MESSAGE_SIZE = 10000000;

      ExecutorService executorService = Executors.newFixedThreadPool(THREADS * 2);
      runAfter(executorService::shutdownNow);
      ConnectionFactory factory = CFUtil.createConnectionFactory(protocol, "tcp://localhost:61616");

      final Connection connectionConsumer = factory.createConnection();
      connectionConsumer.start();
      final Connection connectionProducer = factory.createConnection();

      runAfter(connectionProducer::close);
      runAfter(connectionConsumer::close);

      final String largetext;

      {
         StringBuffer buffer = new StringBuffer();
         while (buffer.length() < MESSAGE_SIZE) {
            buffer.append("Lorem Ypsum blablabla blabalbala I don't care whatever it is in that thing...");
         }
         largetext = buffer.toString();
      }

      CountDownLatch done = new CountDownLatch(THREADS * 2);


      for (int t = 0; t < THREADS; t++) {
         final int localT = t;
         executorService.execute(() -> {
            try {
               try (Session session = connectionConsumer.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                  MessageConsumer consumer = session.createConsumer(session.createQueue("TEST"));
                  for (int i = 0; i < MESSAGE_COUNT && errors.get() == 0; i++) {
                     TextMessage textMessage;
                     do {
                        textMessage = (TextMessage) consumer.receive(300);
                        if (textMessage == null) {
                           if (logger.isTraceEnabled()) {
                              logger.trace("Retrying on thread consumer {}", localT);
                           }
                        }
                     }
                     while (textMessage == null);


                     Assert.assertNotNull(textMessage);
                     if (logger.isDebugEnabled()) {
                        logger.debug("Consumer Thread {} received {} messages, protocol={}", localT, i, protocol);
                     }
                     // Since all messages come from the same queue on all consumers, this is the only assertion possible for the message
                     Assert.assertEquals(largetext, textMessage.getText());
                  }
               }
            } catch (Throwable e) {
               logger.warn(e.getMessage(), e);
               errors.incrementAndGet();
            } finally {
               done.countDown();
            }
         });
      }

      for (int t = 0; t < THREADS; t++) {
         final int localT = t;
         executorService.execute(() -> {
            try {
               try (Session session = connectionProducer.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                  MessageProducer producer = session.createProducer(session.createQueue("TEST"));
                  for (int i = 0; i < MESSAGE_COUNT && errors.get() == 0; i++) {
                     TextMessage textMessage = session.createTextMessage(largetext);
                     producer.send(textMessage);
                     if (logger.isDebugEnabled() && i % 10 == 0) {
                        logger.debug("Producing thread {} sent {} messages, protocol={}", localT, i, protocol);
                     }
                  }
               }
            } catch (Throwable e) {
               logger.warn(e.getMessage(), e);
               errors.incrementAndGet();
            } finally {
               done.countDown();
            }
         });
      }

      Assert.assertTrue(done.await(5, TimeUnit.MINUTES));
      Assert.assertEquals(0, errors.get());
   }


}
