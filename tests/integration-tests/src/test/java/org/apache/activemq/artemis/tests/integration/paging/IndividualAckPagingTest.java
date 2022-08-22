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
import java.util.Arrays;
import java.util.Collection;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.jboss.logging.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class IndividualAckPagingTest extends ActiveMQTestBase {
   private static final Logger logger = Logger.getLogger(IndividualAckPagingTest.class);

   // Even though the focus of the test is paging, I'm adding non paging here to verify the test semantics itself
   @Parameterized.Parameters(name = "paging={0}, restartServerBeforeConsume={1}")
   public static Collection getParams() {
      return Arrays.asList(new Object[][]{{true, false}, {true, true}, {false, false}});
   }

   protected final boolean paging;
   protected final boolean restartServerBeforeConsume;

   private static final String ADDRESS = "IndividualAckPagingTest";

   ActiveMQServer server;

   protected static final int PAGE_MAX = 10 * 1024;

   protected static final int PAGE_SIZE = 5 * 1024;

   public IndividualAckPagingTest(boolean paging, boolean restartServerBeforeConsume) {
      this.paging = paging;
      this.restartServerBeforeConsume = restartServerBeforeConsume;
   }

   @Before
   @Override
   public void setUp() throws Exception {
      super.setUp();

      Configuration config = createDefaultConfig(0, true).setJournalSyncNonTransactional(false);

      config.setMessageExpiryScanPeriod(-1);
      if (paging) {
         server = createServer(true, config, PAGE_SIZE, PAGE_MAX);
         server.getAddressSettingsRepository().clear();
         AddressSettings defaultSetting = new AddressSettings().setPageSizeBytes(PAGE_SIZE).setMaxSizeBytes(PAGE_MAX).setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE).setAutoCreateAddresses(false).setAutoCreateQueues(false).setMaxReadPageBytes(-1).setMaxReadPageMessages(-1);
         server.getAddressSettingsRepository().addMatch("#", defaultSetting);
      } else {
         server = createServer(true, config, 10 * 1024 * 1024, -1);
         server.getAddressSettingsRepository().clear();
         AddressSettings defaultSetting = new AddressSettings().setPageSizeBytes(10 * 1024 * 1024).setMaxSizeBytes(-1).setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE).setAutoCreateAddresses(false).setAutoCreateQueues(false).setMaxReadPageBytes(-1).setMaxReadPageMessages(-1);
         server.getAddressSettingsRepository().addMatch("#", defaultSetting);
      }


      server.start();


      server.addAddressInfo(new AddressInfo(ADDRESS).addRoutingType(RoutingType.ANYCAST));
      server.createQueue(new QueueConfiguration(ADDRESS).setRoutingType(RoutingType.ANYCAST));

   }

   @Test
   public void testIndividualAckCore() throws Exception {
      testIndividualAck("CORE", 1024);
   }

   @Test
   public void testIndividualAckAMQP() throws Exception {
      testIndividualAck("AMQP", 1024);
   }


   public void testIndividualAck(String protocol, int bodySize) throws Exception {
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

      try (Connection connection = factory.createConnection()) {
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         javax.jms.Queue jmsQueue = session.createQueue(ADDRESS);
         MessageProducer producer = session.createProducer(jmsQueue);
         for (int i = 0; i < 100; i++) {
            TextMessage message = session.createTextMessage(extraBody);
            message.setIntProperty("i", i);
            producer.send(message);
         }
         session.commit();
      }

      try (Connection connection = factory.createConnection()) {
         Session session = connection.createSession(false, 101); // INDIVIDUAL-ACK.. same constant for AMQP and CORE
         javax.jms.Queue jmsQueue = session.createQueue(ADDRESS);
         connection.start();
         MessageConsumer consumer = session.createConsumer(jmsQueue);
         for (int i = 0; i < 100; i++) {
            TextMessage message = (TextMessage) consumer.receive(5000);
            Assert.assertNotNull(message);
            if (message.getIntProperty("i") == 77) {
               message.acknowledge();
            }
         }
         Assert.assertNull(consumer.receiveNoWait());
      }

      if (restartServerBeforeConsume) {
         server.stop();
         server.start();
      }

      try (Connection connection = factory.createConnection()) {
         Session session;
         session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         javax.jms.Queue jmsQueue = session.createQueue(ADDRESS);
         connection.start();
         MessageConsumer consumer = session.createConsumer(jmsQueue);
         for (int i = 0; i < 99; i++) {
            TextMessage message = (TextMessage) consumer.receive(5000);
            Assert.assertNotNull(message);
            Assert.assertNotEquals(77, message.getIntProperty("i"));
         }
         Assert.assertNull(consumer.receiveNoWait());
      }
   }

}