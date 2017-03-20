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

package org.apache.activemq.artemis.tests.integration.amqp;

import java.util.Set;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.CoreAddressConfiguration;
import org.apache.activemq.artemis.core.config.CoreQueueConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.jms.server.JMSServerManager;
import org.apache.activemq.artemis.jms.server.impl.JMSServerManagerImpl;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpSender;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.apache.qpid.proton.amqp.Symbol;
import org.junit.After;
import org.junit.Before;

/**
 * Test support class for tests that will be using the AMQP Proton wrapper client.
 * This is to make it easier to migrate tests from ActiveMQ5
 */
public class AmqpClientTestSupport extends AmqpTestSupport {

   protected static Symbol SHARED = Symbol.getSymbol("shared");
   protected static Symbol GLOBAL = Symbol.getSymbol("global");


   protected JMSServerManager serverManager;
   protected ActiveMQServer server;
   @Before
   @Override
   public void setUp() throws Exception {
      super.setUp();
      server = createServer();
   }

   @After
   @Override
   public void tearDown() throws Exception {
      for (AmqpConnection conn : connections) {
         try {
            conn.close();
         } catch (Throwable ignored) {
            ignored.printStackTrace();
         }
      }
      connections.clear();

      if (serverManager != null) {
         try {
            serverManager.stop();
         } catch (Throwable ignored) {
            ignored.printStackTrace();
         }
         serverManager = null;
      }

      server.stop();

      super.tearDown();
   }

   protected ActiveMQServer createServer() throws Exception {
      ActiveMQServer server = createServer(true, true);
      serverManager = new JMSServerManagerImpl(server);
      Configuration serverConfig = server.getConfiguration();

      // Address 1
      CoreAddressConfiguration address = new CoreAddressConfiguration();
      address.setName(getTestName()).getRoutingTypes().add(RoutingType.ANYCAST);
      CoreQueueConfiguration queueConfig = new CoreQueueConfiguration();
      queueConfig.setName(getTestName()).setAddress(getTestName()).setRoutingType(RoutingType.ANYCAST);
      address.getQueueConfigurations().add(queueConfig);
      serverConfig.addAddressConfiguration(address);

      // Address 2
      CoreAddressConfiguration address2 = new CoreAddressConfiguration();
      address2.setName(getTestName2()).getRoutingTypes().add(RoutingType.ANYCAST);
      CoreQueueConfiguration queueConfig2 = new CoreQueueConfiguration();
      queueConfig2.setName(getTestName2()).setAddress(getTestName2()).setRoutingType(RoutingType.ANYCAST);
      address2.getQueueConfigurations().add(queueConfig2);
      serverConfig.addAddressConfiguration(address2);

      serverConfig.getAddressesSettings().put("#", new AddressSettings().setAutoCreateQueues(true).setAutoCreateAddresses(true).setDeadLetterAddress(new SimpleString("ActiveMQ.DLQ")));
      serverConfig.setSecurityEnabled(false);
      Set<TransportConfiguration> acceptors = serverConfig.getAcceptorConfigurations();
      for (TransportConfiguration tc : acceptors) {
         if (tc.getName().equals("netty")) {
            tc.getExtraParams().put("anycastPrefix", "anycast://");
            tc.getExtraParams().put("multicastPrefix", "multicast://");
         }
      }
      serverManager.start();
      server.start();
      return server;
   }

   public Queue getProxyToQueue(String queueName) {
      return server.locateQueue(SimpleString.toSimpleString(queueName));
   }

   public String getTestName() {
      return getName();
   }

   public String getTestName2() {
      return getName() + "2";
   }

   public AmqpClientTestSupport() {
   }

   public AmqpClientTestSupport(String connectorScheme, boolean useSSL) {
      this.useSSL = useSSL;
   }

   protected void sendMessages(int numMessages, String address) throws Exception {
      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();
      AmqpSender sender = session.createSender(address);
      for (int i = 0; i < numMessages; i++) {
         AmqpMessage message = new AmqpMessage();
         message.setText("message-" +  i);
         sender.send(message);
      }
      sender.close();
      connection.connect();
   }
}
