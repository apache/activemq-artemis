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
package org.apache.activemq.artemis.tests.integration.security;


import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

public class JMSXUserIDPluginTest extends ActiveMQTestBase {

   private ActiveMQServer server;
   private SimpleString ADDRESS = new SimpleString("TestQueue");

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      server = addServer(ActiveMQServers.newActiveMQServer(createDefaultNettyConfig(), false));

      JMSXUserIDPlugin plugin = new JMSXUserIDPlugin();
      plugin.setPopulateValidatedUser("testuser");

      server.registerBrokerPlugin(plugin);
      server.start();
      server.createQueue(ADDRESS, RoutingType.ANYCAST, ADDRESS, null, true, false);
   }

   @Test
   public void testAddValidatedUserCore() throws Exception {
      ServerLocator locator = createNettyNonHALocator();
      ClientSessionFactory sf = createSessionFactory(locator);
      ClientSession session = sf.createSession(false, true, true);
      ClientProducer producer = session.createProducer(ADDRESS.toString());
      producer.send(session.createMessage(true));
      ClientConsumer consumer = session.createConsumer(ADDRESS.toString());
      session.start();
      ClientMessage clientMessage = consumer.receiveImmediate();
      Assert.assertNotNull(clientMessage);
      Assert.assertEquals(clientMessage.getValidatedUserID(), "testuser");
   }

   @Test
   public void testAddValidatedUserAMQP() throws Exception {
      JmsConnectionFactory factory = new JmsConnectionFactory("amqp://127.0.0.1:61616");
      Connection connection = factory.createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      javax.jms.Queue queue = session.createQueue(ADDRESS.toString());
      MessageProducer producer = session.createProducer(queue);
      producer.send(session.createMessage());
      connection.start();
      MessageConsumer consumer = session.createConsumer(queue);
      Message message = consumer.receive(5000);
      Assert.assertNotNull(message);
      Assert.assertEquals(message.getStringProperty("_AMQ_VALIDATED_USER"), "testuser");
      connection.close();
   }


}
