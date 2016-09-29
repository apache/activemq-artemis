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
package org.apache.activemq.artemis.tests.integration.jms;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.api.jms.JMSFactoryType;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.registry.JndiBindingRegistry;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnectorFactory;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.jms.server.impl.JMSServerManagerImpl;
import org.apache.activemq.artemis.tests.integration.IntegrationTestLogger;
import org.apache.activemq.artemis.tests.unit.util.InVMNamingContext;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Before;
import org.junit.Test;

/**
 * A FloodServerTest
 */
public class FloodServerTest extends ActiveMQTestBase {
   // Constants -----------------------------------------------------

   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   private ActiveMQServer server;

   private JMSServerManagerImpl serverManager;

   private InVMNamingContext initialContext;

   private final String topicName = "my-topic";

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   // TestCase overrides -------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      Configuration config = createDefaultNettyConfig();
      server = addServer(ActiveMQServers.newActiveMQServer(config, false));
      server.start();

      serverManager = new JMSServerManagerImpl(server);
      initialContext = new InVMNamingContext();
      serverManager.setRegistry(new JndiBindingRegistry(initialContext));
      serverManager.start();
      serverManager.activated();

      serverManager.createTopic(false, topicName, topicName);
      registerConnectionFactory();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   private void registerConnectionFactory() throws Exception {
      int retryInterval = 1000;
      double retryIntervalMultiplier = 1.0;
      int reconnectAttempts = -1;
      long callTimeout = 30000;

      List<TransportConfiguration> connectorConfigs = new ArrayList<>();
      connectorConfigs.add(new TransportConfiguration(NettyConnectorFactory.class.getName()));

      serverManager.createConnectionFactory("ManualReconnectionToSingleServerTest", false, JMSFactoryType.CF, registerConnectors(server, connectorConfigs), null, 1000, ActiveMQClient.DEFAULT_CONNECTION_TTL, callTimeout, ActiveMQClient.DEFAULT_CALL_FAILOVER_TIMEOUT, ActiveMQClient.DEFAULT_CACHE_LARGE_MESSAGE_CLIENT, ActiveMQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE, ActiveMQClient.DEFAULT_COMPRESS_LARGE_MESSAGES, ActiveMQClient.DEFAULT_CONSUMER_WINDOW_SIZE, ActiveMQClient.DEFAULT_CONSUMER_MAX_RATE, ActiveMQClient.DEFAULT_CONFIRMATION_WINDOW_SIZE, ActiveMQClient.DEFAULT_PRODUCER_WINDOW_SIZE, ActiveMQClient.DEFAULT_PRODUCER_MAX_RATE, false, false, false, ActiveMQClient.DEFAULT_AUTO_GROUP, false, ActiveMQClient.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME, ActiveMQClient.DEFAULT_ACK_BATCH_SIZE, ActiveMQClient.DEFAULT_ACK_BATCH_SIZE, ActiveMQClient.DEFAULT_USE_GLOBAL_POOLS, ActiveMQClient.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE, ActiveMQClient.DEFAULT_THREAD_POOL_MAX_SIZE, retryInterval, retryIntervalMultiplier, 1000, reconnectAttempts, ActiveMQClient.DEFAULT_FAILOVER_ON_INITIAL_CONNECTION, null, "/cf");
   }

   @Test
   public void testFoo() {
   }

   public void _testFlood() throws Exception {
      ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("/cf");

      final int numProducers = 20;

      final int numConsumers = 20;

      final int numMessages = 10000;

      ProducerThread[] producers = new ProducerThread[numProducers];

      for (int i = 0; i < numProducers; i++) {
         producers[i] = new ProducerThread(cf, numMessages);
      }

      ConsumerThread[] consumers = new ConsumerThread[numConsumers];

      for (int i = 0; i < numConsumers; i++) {
         consumers[i] = new ConsumerThread(cf, numMessages);
      }

      for (int i = 0; i < numConsumers; i++) {
         consumers[i].start();
      }

      for (int i = 0; i < numProducers; i++) {
         producers[i].start();
      }

      for (int i = 0; i < numConsumers; i++) {
         consumers[i].join();
      }

      for (int i = 0; i < numProducers; i++) {
         producers[i].join();
      }

   }

   class ProducerThread extends Thread {

      private final Connection connection;

      private final Session session;

      private final MessageProducer producer;

      private final int numMessages;

      ProducerThread(final ConnectionFactory cf, final int numMessages) throws Exception {
         connection = cf.createConnection();

         session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         producer = session.createProducer(ActiveMQJMSClient.createTopic("my-topic"));

         producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

         this.numMessages = numMessages;
      }

      @Override
      public void run() {
         try {
            byte[] bytes = new byte[1000];

            BytesMessage message = session.createBytesMessage();

            message.writeBytes(bytes);

            for (int i = 0; i < numMessages; i++) {
               producer.send(message);

               // if (i % 1000 == 0)
               // {
               // log.info("Producer " + this + " sent " + i);
               // }
            }

            connection.close();
         } catch (Exception e) {
            e.printStackTrace();
         }
      }
   }

   class ConsumerThread extends Thread {

      private final Connection connection;

      private final Session session;

      private final MessageConsumer consumer;

      private final int numMessages;

      ConsumerThread(final ConnectionFactory cf, final int numMessages) throws Exception {
         connection = cf.createConnection();

         connection.start();

         session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         consumer = session.createConsumer(ActiveMQJMSClient.createTopic("my-topic"));

         this.numMessages = numMessages;
      }

      @Override
      public void run() {
         try {
            for (int i = 0; i < numMessages; i++) {
               Message msg = consumer.receive();

               if (msg == null) {
                  FloodServerTest.log.error("message is null");
                  break;
               }

               // if (i % 1000 == 0)
               // {
               // log.info("Consumer " + this + " received " + i);
               // }
            }

            connection.close();
         } catch (Exception e) {
            e.printStackTrace();
         }
      }
   }

}
