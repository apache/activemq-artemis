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
package org.apache.activemq.artemis.tests.stress.paging;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.impl.QueueImpl;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.unit.UnitTestLogger;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MultipleConsumersPageStressTest extends ActiveMQTestBase {

   private final UnitTestLogger log = UnitTestLogger.LOGGER;

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private static final int TIME_TO_RUN = 60 * 1000;

   private static final SimpleString ADDRESS = new SimpleString("page-adr");

   private int numberOfProducers;

   private int numberOfConsumers;

   private QueueImpl pagedServerQueue;

   private boolean shareConnectionFactory = true;

   private boolean openConsumerOnEveryLoop = true;

   private ActiveMQServer server;

   private ServerLocator sharedLocator;

   private ClientSessionFactory sharedSf;

   final AtomicInteger messagesAvailable = new AtomicInteger(0);

   private volatile boolean runningProducer = true;

   private volatile boolean runningConsumer = true;

   ArrayList<TestProducer> producers = new ArrayList<>();

   ArrayList<TestConsumer> consumers = new ArrayList<>();

   ArrayList<Throwable> exceptions = new ArrayList<>();

   @Test
   public void testOpenConsumerEveryTimeDefaultFlowControl0() throws Throwable {
      shareConnectionFactory = true;
      openConsumerOnEveryLoop = true;
      numberOfProducers = 1;
      numberOfConsumers = 1;

      sharedLocator = createInVMNonHALocator().setConsumerWindowSize(0);

      sharedSf = createSessionFactory(sharedLocator);

      internalMultipleConsumers();
   }

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      HashMap<String, AddressSettings> settings = new HashMap<>();

      server = createServer(true, createDefaultInVMConfig(), 10024, 200024, settings);
      server.start();

      pagedServerQueue = (QueueImpl) server.createQueue(ADDRESS, ADDRESS, null, true, false);

   }

   @Test
   public void testOpenConsumerEveryTimeDefaultFlowControl() throws Throwable {
      shareConnectionFactory = true;
      openConsumerOnEveryLoop = true;
      numberOfProducers = 1;
      numberOfConsumers = 1;

      sharedLocator = createInVMNonHALocator();

      sharedSf = createSessionFactory(sharedLocator);

      System.out.println(pagedServerQueue.debug());

      internalMultipleConsumers();

   }

   @Test
   public void testReuseConsumersFlowControl0() throws Throwable {
      shareConnectionFactory = true;
      openConsumerOnEveryLoop = false;
      numberOfProducers = 1;
      numberOfConsumers = 1;

      sharedLocator = createInVMNonHALocator().setConsumerWindowSize(0);

      sharedSf = createSessionFactory(sharedLocator);

      try {
         internalMultipleConsumers();
      } catch (Throwable e) {
         TestConsumer tstConsumer = consumers.get(0);
         System.out.println("first retry: " + tstConsumer.consumer.receive(1000));

         System.out.println(pagedServerQueue.debug());

         pagedServerQueue.forceDelivery();
         System.out.println("Second retry: " + tstConsumer.consumer.receive(1000));

         System.out.println(pagedServerQueue.debug());

         tstConsumer.session.commit();
         System.out.println("Third retry:" + tstConsumer.consumer.receive(1000));

         tstConsumer.close();

         ClientSession session = sharedSf.createSession();
         session.start();
         ClientConsumer consumer = session.createConsumer(ADDRESS);

         pagedServerQueue.forceDelivery();

         System.out.println("Fourth retry: " + consumer.receive(1000));

         System.out.println(pagedServerQueue.debug());

         throw e;
      }

   }

   public void internalMultipleConsumers() throws Throwable {
      for (int i = 0; i < numberOfProducers; i++) {
         producers.add(new TestProducer());
      }

      for (int i = 0; i < numberOfConsumers; i++) {
         consumers.add(new TestConsumer());
      }

      for (Tester test : producers) {
         test.start();
      }

      Thread.sleep(2000);

      for (Tester test : consumers) {
         test.start();
      }

      for (Tester test : consumers) {
         test.join();
      }

      runningProducer = false;

      for (Tester test : producers) {
         test.join();
      }

      for (Throwable e : exceptions) {
         throw e;
      }

   }

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   abstract class Tester extends Thread {

      Random random = new Random();

      public abstract void close();

      protected abstract boolean enabled();

      protected void exceptionHappened(final Throwable e) {
         runningConsumer = false;
         runningProducer = false;
         e.printStackTrace();
         exceptions.add(e);
      }

      public int getNumberOfMessages() throws Exception {
         int numberOfMessages = random.nextInt(20);
         if (numberOfMessages <= 0) {
            return 1;
         } else {
            return numberOfMessages;
         }
      }
   }

   class TestConsumer extends Tester {

      public ClientConsumer consumer = null;

      public ClientSession session = null;

      public ServerLocator locator = null;

      public ClientSessionFactory sf = null;

      @Override
      public void close() {
         try {

            if (!openConsumerOnEveryLoop) {
               consumer.close();
            }
            session.rollback();
            session.close();

            if (!shareConnectionFactory) {
               sf.close();
               locator.close();
            }
         } catch (Exception ignored) {
         }

      }

      @Override
      protected boolean enabled() {
         return runningConsumer;
      }

      @Override
      public int getNumberOfMessages() throws Exception {
         while (enabled()) {
            int numberOfMessages = super.getNumberOfMessages();

            int resultMessages = messagesAvailable.addAndGet(-numberOfMessages);

            if (resultMessages < 0) {
               messagesAvailable.addAndGet(-numberOfMessages);
               numberOfMessages = 0;
               System.out.println("Negative, giving a little wait");
               Thread.sleep(1000);
            }

            if (numberOfMessages > 0) {
               return numberOfMessages;
            }
         }

         return 0;
      }

      @Override
      public void run() {
         try {
            if (shareConnectionFactory) {
               session = sharedSf.createSession(false, false);
            } else {
               locator = createInVMNonHALocator();
               sf = createSessionFactory(locator);
               session = sf.createSession(false, false);
            }

            long timeOut = System.currentTimeMillis() + MultipleConsumersPageStressTest.TIME_TO_RUN;

            session.start();

            if (!openConsumerOnEveryLoop) {
               consumer = session.createConsumer(MultipleConsumersPageStressTest.ADDRESS);
            }

            int count = 0;

            while (enabled() && timeOut > System.currentTimeMillis()) {

               if (openConsumerOnEveryLoop) {
                  consumer = session.createConsumer(MultipleConsumersPageStressTest.ADDRESS);
               }

               int numberOfMessages = getNumberOfMessages();

               for (int i = 0; i < numberOfMessages; i++) {
                  ClientMessage msg = consumer.receive(10000);
                  if (msg == null) {
                     log.warn("msg " + count +
                                 " was null, currentBatchSize=" +
                                 numberOfMessages +
                                 ", current msg being read=" +
                                 i);
                  }
                  Assert.assertNotNull("msg " + count +
                                          " was null, currentBatchSize=" +
                                          numberOfMessages +
                                          ", current msg being read=" +
                                          i, msg);

                  if (numberOfConsumers == 1 && numberOfProducers == 1) {
                     Assert.assertEquals(count, msg.getIntProperty("count").intValue());
                  }

                  count++;

                  msg.acknowledge();
               }

               session.commit();

               if (openConsumerOnEveryLoop) {
                  consumer.close();
               }

            }
         } catch (Throwable e) {
            exceptionHappened(e);
         }

      }
   }

   class TestProducer extends Tester {

      ClientSession session = null;

      ClientSessionFactory sf = null;

      ServerLocator locator = null;

      @Override
      public void close() {
         try {
            session.rollback();
            session.close();
         } catch (Exception ignored) {
         }

      }

      @Override
      protected boolean enabled() {
         return runningProducer;
      }

      @Override
      public void run() {
         try {
            if (shareConnectionFactory) {
               session = sharedSf.createSession(false, false);
            } else {
               locator = createInVMNonHALocator();
               sf = createSessionFactory(locator);
               session = sf.createSession(false, false);
            }

            ClientProducer prod = session.createProducer(MultipleConsumersPageStressTest.ADDRESS);

            int count = 0;

            while (enabled()) {
               int numberOfMessages = getNumberOfMessages();

               for (int i = 0; i < numberOfMessages; i++) {
                  ClientMessage msg = session.createMessage(true);
                  msg.putStringProperty("Test", "This is a simple test");
                  msg.putIntProperty("count", count++);
                  prod.send(msg);
               }

               messagesAvailable.addAndGet(numberOfMessages);
               session.commit();
            }
         } catch (Throwable e) {
            exceptionHappened(e);
         }
      }
   }

}
