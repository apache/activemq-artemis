/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.crossprotocol;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQDestination;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpSender;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.messaging.Header;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AMQPToJMSCoreTest extends ActiveMQTestBase {

   private ActiveMQServer server;
   protected String queueName = "amqTestQueue1";
   private SimpleString coreQueue;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      server = createServer(true, true);
      server.start();
      server.waitForActivation(10, TimeUnit.SECONDS);

      Configuration serverConfig = server.getConfiguration();
      serverConfig.getAddressesSettings().put("#", new AddressSettings().setAutoCreateQueues(false)
                                                                        .setAutoCreateAddresses(false)
                                                                        .setDeadLetterAddress(SimpleString.toSimpleString("ActiveMQ.DLQ")));
      serverConfig.setSecurityEnabled(false);
      coreQueue = SimpleString.toSimpleString(queueName);
      server.createQueue(coreQueue, RoutingType.ANYCAST, coreQueue, null, false, false);
   }

   @Override
   @After
   public void tearDown() throws Exception {
      server.stop();
      super.tearDown();
   }

   @Test
   public void testMessageDestination() throws Exception {
      System.out.println("foo");
      AmqpClient client = new AmqpClient(new URI("tcp://127.0.0.1:61616"), null, null);
      AmqpConnection amqpconnection = client.connect();
      try {
         AmqpSession session = amqpconnection.createSession();
         AmqpSender sender = session.createSender(queueName);
         AmqpMessage message = new AmqpMessage();
         message.setMessageId("MessageID:" + 0);
         //         message.setApplicationProperty("_AMQ_ROUTING_TYPE", (byte) 1);
         message.getWrappedMessage().setHeader(new Header());
         message.getWrappedMessage().getHeader().setDeliveryCount(new UnsignedInteger(2));
         sender.send(message);
      } finally {
         amqpconnection.close();
      }
      ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://127.0.0.1:61616");
      Connection connection = null;
      try {
         connection = factory.createConnection();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer consumer = session.createConsumer(ActiveMQJMSClient.createQueue(queueName));
         connection.start();
         Message message = consumer.receive(2000);
         Assert.assertNotNull(message);
         ActiveMQDestination jmsDestination = (ActiveMQDestination) message.getJMSDestination();
         Assert.assertEquals(queueName, jmsDestination.getAddress());
      } finally {
         if (connection != null) {
            connection.close();
         }
      }
   }
}
