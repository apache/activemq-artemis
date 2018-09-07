/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.integration.cluster.distribution;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.server.cluster.impl.MessageLoadBalancingType;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.protocol.amqp.broker.ProtonProtocolManagerFactory;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(value = Parameterized.class)
public class AMQPLargeMessageClusterTest extends ClusterTestBase {

   private static final int RECEIVE_TIMEOUT_MILLIS = 20_000;
   private static final int MESSAGE_SIZE = 1024 * 1024;
   private static final int MESSAGES = 1;

   private final boolean persistenceEnabled;
   private final boolean compressLargeMessages;

   @Parameterized.Parameters(name = "persistenceEnabled = {0}, compressLargeMessages = {1}")
   public static Iterable<? extends Object> persistenceEnabled() {
      return Arrays.asList(new Object[][]{{true, false}, {false, false}, {true, true}, {false, true}});
   }

   public AMQPLargeMessageClusterTest(boolean persistenceEnabled, boolean compressLargeMessages) {
      this.persistenceEnabled = persistenceEnabled;
      this.compressLargeMessages = compressLargeMessages;
   }

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      start();
   }

   private void start() throws Exception {
      setupServers();

      setRedistributionDelay(0);
   }

   protected boolean isNetty() {
      return true;
   }

   @Test(timeout = RECEIVE_TIMEOUT_MILLIS * (MESSAGES + 1))
   public void testSendReceiveLargeMessage() throws Exception {
      setupCluster(MessageLoadBalancingType.ON_DEMAND);

      startServers(0, 1);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      final String queueName = "queues.0";

      createQueue(0, queueName, queueName, null, false, null, null, RoutingType.ANYCAST);
      createQueue(1, queueName, queueName, null, false, null, null, RoutingType.ANYCAST);

      waitForBindings(0, queueName, 1, 0, true);
      waitForBindings(1, queueName, 1, 0, true);

      waitForBindings(0, queueName, /**/1, 0, false);
      waitForBindings(1, queueName, 1, 0, false);
      String producerUri = "amqp://localhost:61616";
      if (compressLargeMessages) {
         producerUri = producerUri + "?compressLargeMessages=true";
      }
      final JmsConnectionFactory producerFactory = new JmsConnectionFactory(producerUri);
      try (Connection producerConnection = producerFactory.createConnection(); Session producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
         producerConnection.start();
         final Destination queue = producerSession.createQueue(queueName);
         String consumerUri = "amqp://localhost:61617";
         if (compressLargeMessages) {
            consumerUri = consumerUri + "?compressLargeMessages=true";
         }
         final JmsConnectionFactory consumerConnectionFactory = new JmsConnectionFactory(consumerUri);
         try (Connection consumerConnection = consumerConnectionFactory.createConnection(); Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE); MessageConsumer consumer = consumerSession.createConsumer(queue); MessageProducer producer = producerSession.createProducer(queue)) {
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            consumerConnection.start();
            final byte[] largeMessageContent = new byte[MESSAGE_SIZE];
            final byte[] receivedContent = new byte[largeMessageContent.length];
            ThreadLocalRandom.current().nextBytes(largeMessageContent);
            for (int i = 0; i < MESSAGES; i++) {
               final BytesMessage sentMessage = producerSession.createBytesMessage();
               sentMessage.writeBytes(largeMessageContent);
               producer.send(sentMessage);
               final Message receivedMessage = consumer.receive(RECEIVE_TIMEOUT_MILLIS);
               Assert.assertNotNull("A message should be received in " + RECEIVE_TIMEOUT_MILLIS + " ms", receivedMessage);
               Assert.assertThat(receivedMessage, IsInstanceOf.instanceOf(sentMessage.getClass()));
               Assert.assertEquals(largeMessageContent.length, ((BytesMessage) receivedMessage).readBytes(receivedContent));
               Assert.assertArrayEquals(largeMessageContent, receivedContent);
            }
         }
      }
   }

   protected void setupCluster(final MessageLoadBalancingType messageLoadBalancingType) throws Exception {
      setupClusterConnection("cluster0", "queues", messageLoadBalancingType, 1, isNetty(), 0, 1);
      setupClusterConnection("cluster1", "queues", messageLoadBalancingType, 1, isNetty(), 1, 0);
   }

   protected void setRedistributionDelay(final long delay) {
      AddressSettings as = new AddressSettings().setRedistributionDelay(delay);

      getServer(0).getAddressSettingsRepository().addMatch("queues.*", as);
      getServer(1).getAddressSettingsRepository().addMatch("queues.*", as);
   }

   protected void setupServers() throws Exception {
      setupServer(0, persistenceEnabled, isNetty());
      setupServer(1, persistenceEnabled, isNetty());

      servers[0].addProtocolManagerFactory(new ProtonProtocolManagerFactory());
      servers[1].addProtocolManagerFactory(new ProtonProtocolManagerFactory());
   }

   protected void stopServers() throws Exception {
      closeAllConsumers();

      closeAllSessionFactories();

      closeAllServerLocatorsFactories();

      stopServers(0, 1);

      clearServer(0, 1);
   }

}

