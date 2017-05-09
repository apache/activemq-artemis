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
package org.apache.activemq.artemis.tests.integration.client;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.RoutingType;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.core.settings.impl.SlowConsumerPolicy;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.ConcurrentHashSet;
import org.apache.activemq.artemis.utils.TimeUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

public class MultipleSlowConsumerTest extends ActiveMQTestBase {

   private int checkPeriod = 3;
   private int threshold = 1;

   private ActiveMQServer server;

   private final SimpleString QUEUE = new SimpleString("SlowConsumerTestQueue");

   private ServerLocator locator;

   @Before
   @Override
   public void setUp() throws Exception {
      super.setUp();

      server = createServer(true, true);

      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setSlowConsumerCheckPeriod(checkPeriod);
      addressSettings.setSlowConsumerThreshold(threshold);
      addressSettings.setSlowConsumerPolicy(SlowConsumerPolicy.KILL);

      server.start();

      server.getAddressSettingsRepository().addMatch(QUEUE.toString(), addressSettings);

      server.createQueue(QUEUE, RoutingType.ANYCAST, QUEUE, null, true, false);

      locator = createFactory(true);
   }

   /**
    * This test creates 3 consumers on one queue. A producer sends
    * messages at a rate of 2 mesages per second. Each consumer
    * consumes messages at rate of 1 message per second. The slow
    * consumer threshold is 1 message per second.
    * Based on the above settings, at least one of the consumers
    * will be removed during the test, but at least one of the
    * consumers will remain and all messages will be received.
    */
   @Test
   public void testMultipleConsumersOneQueue() throws Exception {
      locator.setAckBatchSize(0);

      ClientSessionFactory sf1 = createSessionFactory(locator);
      ClientSessionFactory sf2 = createSessionFactory(locator);
      ClientSessionFactory sf3 = createSessionFactory(locator);
      ClientSessionFactory sf4 = createSessionFactory(locator);

      final int messages = 10;

      FixedRateProducer producer = new FixedRateProducer(sf1, QUEUE, messages);

      final Set<FixedRateConsumer> consumers = new ConcurrentHashSet<>();
      final Set<ClientMessage> receivedMessages = new ConcurrentHashSet<>();

      consumers.add(new FixedRateConsumer(sf2, QUEUE, consumers, receivedMessages, 1));
      consumers.add(new FixedRateConsumer(sf3, QUEUE, consumers, receivedMessages, 2));
      consumers.add(new FixedRateConsumer(sf4, QUEUE, consumers, receivedMessages, 3));

      try {
         producer.start(threshold * 1000 / 2);

         for (FixedRateConsumer consumer : consumers) {
            consumer.start(threshold * 1000);
         }

         //check at least one consumer is killed
         //but at least one survived
         //and all messages are received.
         assertTrue(TimeUtils.waitOnBoolean(true, 10000, () -> consumers.size() < 3));
         assertTrue(TimeUtils.waitOnBoolean(true, 10000, () -> consumers.size() > 0));
         assertTrue(TimeUtils.waitOnBoolean(true, 10000, () -> receivedMessages.size() == messages));
         assertTrue(TimeUtils.waitOnBoolean(true, 10000, () -> consumers.size() > 0));
      } finally {
         producer.stopRunning();
         for (FixedRateConsumer consumer : consumers) {
            consumer.stopRunning();
         }
         System.out.println("***report messages received: " + receivedMessages.size());
         System.out.println("***consumers left: " + consumers.size());
      }
   }

   private class FixedRateProducer extends FixedRateClient {

      int messages;
      ClientProducer producer;

      FixedRateProducer(ClientSessionFactory sf, SimpleString queue, int messages) throws ActiveMQException {
         super(sf, queue);
         this.messages = messages;
      }

      @Override
      protected void prepareWork() throws ActiveMQException {
         super.prepareWork();
         this.producer = session.createProducer(queue);
      }

      @Override
      protected void doWork(int count) throws Exception {

         if (count < messages) {
            ClientMessage m = createTextMessage(session, "msg" + count);
            producer.send(m);
            System.out.println("producer sent a message " + count);
         } else {
            stopRunning();
         }
      }
   }

   private class FixedRateConsumer extends FixedRateClient {

      Set<FixedRateConsumer> consumers;
      ClientConsumer consumer;
      Set<ClientMessage> receivedMessages;
      int id;

      FixedRateConsumer(ClientSessionFactory sf, SimpleString queue,
                               Set<FixedRateConsumer> consumers, Set<ClientMessage> receivedMessages,
                               int id) throws ActiveMQException {
         super(sf, queue);
         this.consumers = consumers;
         this.receivedMessages = receivedMessages;
         this.id = id;
      }

      @Override
      protected void prepareWork() throws ActiveMQException {
         super.prepareWork();
         this.consumer = session.createConsumer(queue);
         this.session.start();
      }

      @Override
      protected void doWork(int count) throws Exception {
         ClientMessage m = this.consumer.receive(rate);
         System.out.println("consumer " + id + " got m: " + m);
         if (m != null) {
            receivedMessages.add(m);
            m.acknowledge();
            System.out.println("acked " + m.getClass().getName() + "now total received: " + receivedMessages.size());
         }
      }

      @Override
      protected void handleError(int count, Exception e) {
         System.err.println("Got error receiving message " + count + " remove self " + this.id);
         consumers.remove(this);
         e.printStackTrace();
      }

   }

   private abstract class FixedRateClient extends Thread {

      protected ClientSessionFactory sf;
      protected SimpleString queue;
      protected ClientSession session;
      protected int rate;
      protected volatile boolean working;

      FixedRateClient(ClientSessionFactory sf, SimpleString queue) throws ActiveMQException {
         this.sf = sf;
         this.queue = queue;
      }

      public void start(int rate) {
         this.rate = rate;
         working = true;
         start();
      }

      protected void prepareWork() throws ActiveMQException {
         this.session = addClientSession(sf.createSession(true, true));
      }

      @Override
      public void run() {
         try {
            prepareWork();
         } catch (ActiveMQException e) {
            System.out.println("got error in prepareWork(), aborting...");
            e.printStackTrace();
            return;
         }
         int count = 0;
         while (working) {
            try {
               doWork(count);
               Thread.sleep(rate);
            } catch (InterruptedException e) {
               e.printStackTrace();
            } catch (Exception e) {
               System.err.println(this + " got exception ");
               e.printStackTrace();
               handleError(count, e);
               working = false;
            } finally {
               count++;
            }
         }
      }

      protected abstract void doWork(int count) throws Exception;

      protected void handleError(int count, Exception e) {
      }

      public void stopRunning() {
         working = false;
         interrupt();
         try {
            join();
         } catch (InterruptedException e) {
            e.printStackTrace();
         }
      }
   }
}
