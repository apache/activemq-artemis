/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.amqp.connect;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectConfiguration;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPMirrorBrokerConnectionElement;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.tests.util.Wait;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class DisconnectConsumerMirrorTest extends ActiveMQTestBase {

   private static final int NUMBER_OF_MESSAGES = 5;

   ActiveMQServer server1;
   ActiveMQServer server2;

   @Test
   public void testDisconnectConsumers() throws Exception {

      try {
         String queueName = getName();

         {
            Configuration configuration = createDefaultConfig(0, false);
            configuration.setMirrorDisconnectConsumers(true);
            configuration.getAddressConfigurations().clear();
            configuration.setResolveProtocols(true);
            configuration.setMirrorAckManagerRetryDelay(100).setMirrorAckManagerPageAttempts(5).setMirrorAckManagerQueueAttempts(5);
            configuration.addQueueConfiguration(QueueConfiguration.of(queueName).setRoutingType(RoutingType.ANYCAST));
            configuration.addAcceptorConfiguration("clients", "tcp://localhost:61616");
            AMQPBrokerConnectConfiguration brokerConnectConfiguration = new AMQPBrokerConnectConfiguration("toDC2", "tcp://localhost:61617").setRetryInterval(100).setReconnectAttempts(-1);
            AMQPMirrorBrokerConnectionElement mirror = new AMQPMirrorBrokerConnectionElement().setDurable(true);
            brokerConnectConfiguration.addMirror(mirror);
            configuration.addAMQPConnection(brokerConnectConfiguration);
            server1 = createServer(true, configuration);
            server1.setIdentity("server1");
            server1.start();
         }

         {
            Configuration configuration = createDefaultConfig(1, false);
            configuration.setMirrorDisconnectConsumers(true);
            configuration.setResolveProtocols(true);
            configuration.setMirrorAckManagerRetryDelay(100).setMirrorAckManagerPageAttempts(5).setMirrorAckManagerQueueAttempts(5);
            configuration.addQueueConfiguration(QueueConfiguration.of(queueName).setRoutingType(RoutingType.ANYCAST));
            configuration.addAcceptorConfiguration("clients", "tcp://localhost:61617");
            AMQPBrokerConnectConfiguration brokerConnectConfiguration = new AMQPBrokerConnectConfiguration("toDC1", "tcp://localhost:61616").setRetryInterval(100).setReconnectAttempts(-1);
            AMQPMirrorBrokerConnectionElement mirror = new AMQPMirrorBrokerConnectionElement();
            brokerConnectConfiguration.addMirror(mirror);
            configuration.addAMQPConnection(brokerConnectConfiguration);
            server2 = createServer(true, configuration);
            server2.setIdentity("server2");
            server2.start();
         }

         validateProtocol("AMQP", queueName);
         validateProtocol("CORE", queueName);
         validateProtocol("OPENWIRE", queueName);
      } finally {
         server1.stop();
         server2.stop();

         server1 = null;
         server2 = null;
      }
   }

   private void validateProtocol(String protocol, String queueName) throws Exception {
      Queue mirrorQueue2 = server2.locateQueue("$ACTIVEMQ_ARTEMIS_MIRROR_toDC1");
      assertNotNull(mirrorQueue2);

      Queue mirrorQueue1 = server1.locateQueue("$ACTIVEMQ_ARTEMIS_MIRROR_toDC2");
      assertNotNull(mirrorQueue1);

      ConnectionFactory factory1 = CFUtil.createConnectionFactory(protocol, "tcp://localhost:61616");
      ConnectionFactory factory2 = CFUtil.createConnectionFactory(protocol, "tcp://localhost:61617");

      try (Connection connection = factory2.createConnection()) {
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         MessageProducer producer = session.createProducer(session.createQueue(queueName));
         for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
            producer.send(session.createTextMessage("hello " + i));
         }
         session.commit();
      }

      try (Connection connection1 = factory1.createConnection(); Connection connection2 = factory2.createConnection()) {

         connection1.start();
         connection2.start();

         Session session1 = connection1.createSession(true, Session.SESSION_TRANSACTED);
         MessageConsumer consumer_server1 = session1.createConsumer(session1.createQueue(queueName));
         assertNotNull(consumer_server1.receive(5000));

         Session session2 = connection2.createSession(true, Session.SESSION_TRANSACTED);
         MessageConsumer consumer_server2 = session2.createConsumer(session1.createQueue(queueName));

         for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
            assertNotNull(consumer_server2.receive(5000));
         }
         session2.commit();

         Wait.assertEquals(0, mirrorQueue1::getMessageCount);
         Wait.assertEquals(0, mirrorQueue2::getMessageCount);

         verifyNoMessages(server1, server2, queueName);

         // Consumers on server1 were supposed to be disconnected
         // as instructed on MirrorDisconnectConsumers
         Assertions.assertThrows(JMSException.class, () -> {
            consumer_server1.receive(5000);
         });
      }
   }

   private void verifyNoMessages(ActiveMQServer server1,
                                 ActiveMQServer server2,
                                 String queueName) throws Exception {
      Queue queueServer1 = server1.locateQueue(queueName);
      Queue queueServer2 = server2.locateQueue(queueName);

      Queue mirrorQueue1 = server1.locateQueue("$ACTIVEMQ_ARTEMIS_MIRROR_toDC2");
      Queue mirrorQueue2 = server2.locateQueue("$ACTIVEMQ_ARTEMIS_MIRROR_toDC1");

      Wait.assertEquals(0L, mirrorQueue1::getMessageCount, 5000, 100);
      Wait.assertEquals(0L, mirrorQueue2::getMessageCount, 5000, 100);

      Wait.assertEquals(0L, queueServer1::getMessageCount, 5000, 100);
      Wait.assertEquals(0L, queueServer2::getMessageCount, 5000, 100);
   }
}