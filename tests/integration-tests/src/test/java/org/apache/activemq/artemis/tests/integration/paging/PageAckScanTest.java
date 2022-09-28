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
package org.apache.activemq.artemis.tests.integration.paging;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.ToIntFunction;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.paging.cursor.PageSubscription;
import org.apache.activemq.artemis.core.paging.cursor.PagedReference;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.utils.ReusableLatch;
import org.apache.activemq.artemis.utils.Wait;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PageAckScanTest extends ActiveMQTestBase {

   private static final String ADDRESS = "MessagesExpiredPagingTest";

   ActiveMQServer server;

   protected static final int PAGE_MAX = 10 * 1024;

   protected static final int PAGE_SIZE = 1 * 1024;



   @Before
   @Override
   public void setUp() throws Exception {
      super.setUp();

      Configuration config = createDefaultConfig(0, true).setJournalSyncNonTransactional(false);

      config.setMessageExpiryScanPeriod(-1);
      server = createServer(true, config, PAGE_SIZE, PAGE_MAX);

      server.getAddressSettingsRepository().clear();

      AddressSettings defaultSetting = new AddressSettings().setPageSizeBytes(PAGE_SIZE).setMaxSizeBytes(PAGE_MAX).setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE).setAutoCreateAddresses(false).setAutoCreateQueues(false);

      server.getAddressSettingsRepository().addMatch("#", defaultSetting);


      server.start();


      server.addAddressInfo(new AddressInfo(ADDRESS).addRoutingType(RoutingType.ANYCAST));
      server.createQueue(new QueueConfiguration(ADDRESS).setRoutingType(RoutingType.ANYCAST));

   }

   @Test
   public void testScanCore() throws Exception {
      testScan("CORE", 5000, 1000, 100, 1024);
   }

   @Test
   public void testScanAMQP() throws Exception {
      testScan("AMQP", 5000, 1000, 100, 1024);
   }


   public void testScan(String protocol, int numberOfMessages, int numberOfMessageSecondWave, int pagingInterval, int bodySize) throws Exception {
      ConnectionFactory factory = CFUtil.createConnectionFactory(protocol, "tcp://localhost:61616");

      String extraBody;
      {
         StringBuffer buffer = new StringBuffer();
         for (int i = 0; i < bodySize; i++) {
            buffer.append("*");
         }
         extraBody = buffer.toString();
      }

      Queue queue = server.locateQueue(ADDRESS);
      queue.getPagingStore().startPaging();

      try (Connection connection = factory.createConnection()) {
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         javax.jms.Queue jmsQueue = session.createQueue(ADDRESS);
         MessageProducer producer = session.createProducer(jmsQueue);
         for (int i = 0; i < 20; i++) {
            TextMessage message = session.createTextMessage(extraBody);
            message.setIntProperty("i", i);
            producer.send(message);
         }
         session.commit();
      }

      AtomicInteger errors = new AtomicInteger(0);

      ReusableLatch latch = new ReusableLatch(4);

      Runnable done = latch::countDown;

      Runnable notFound = () -> {
         errors.incrementAndGet();
         done.run();
      };

      AtomicInteger retried = new AtomicInteger(0);
      PageSubscription subscription = queue.getPageSubscription();
      subscription.scanAck(() -> false, new CompareI(15), done, notFound);
      subscription.scanAck(() -> false, new CompareI(11), done, notFound);
      subscription.scanAck(() -> false, new CompareI(99), done, notFound);
      subscription.scanAck(() -> false, new CompareI(-30), done, notFound);
      subscription.scanAck(() -> {
         retried.incrementAndGet();
         return true;}, new CompareI(333), done, notFound);
      Assert.assertTrue(latch.await(5, TimeUnit.MINUTES));
      Assert.assertEquals(2, errors.get());
      Wait.assertEquals(1, retried::get);

      try (Connection connection = factory.createConnection()) {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         javax.jms.Queue jmsQueue = session.createQueue(ADDRESS);
         connection.start();
         MessageConsumer consumer = session.createConsumer(jmsQueue);
         for (int i = 0; i < 18; i++) {
            TextMessage message = (TextMessage) consumer.receive(5000);
            Assert.assertNotNull(message);
            Assert.assertTrue(message.getIntProperty("i") != 11 && message.getIntProperty("i") != 15);
         }
         Assert.assertNull(consumer.receiveNoWait());
      }
   }

   class CompareI implements ToIntFunction<PagedReference> {
      final int i;
      CompareI(int i) {
         this.i = i;
      }

      @Override
      public int applyAsInt(PagedReference ref) {
         return ref.getMessage().getIntProperty("i").intValue() - i;
      }
   }

}