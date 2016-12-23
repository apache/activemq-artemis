/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.client;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.WildcardConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class WildcardConfigurationTest extends ActiveMQTestBase {

   private ActiveMQServer server;
   private ServerLocator locator;
   private ClientSession clientSession;
   private ClientSessionFactory sf;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      WildcardConfiguration wildcardConfiguration = new WildcardConfiguration();
      wildcardConfiguration.setDelimiter('/');
      Configuration configuration = createDefaultInVMConfig().setWildcardRoutingEnabled(true).setTransactionTimeoutScanPeriod(500).setWildCardConfiguration(wildcardConfiguration);
      server = addServer(ActiveMQServers.newActiveMQServer(configuration, false));
      server.start();
      server.getManagementService().enableNotifications(false);
      locator = createInVMNonHALocator();
      sf = createSessionFactory(locator);
      clientSession = addClientSession(sf.createSession(false, true, true));
   }

   @Test
   public void testBasicWildcardRouting() throws Exception {
      SimpleString addressAB = new SimpleString("a/b");
      SimpleString addressAC = new SimpleString("a/c");
      SimpleString address = new SimpleString("a/*");
      SimpleString queueName1 = new SimpleString("Q1");
      SimpleString queueName2 = new SimpleString("Q2");
      SimpleString queueName = new SimpleString("Q");
      clientSession.createQueue(addressAB, queueName1, null, false);
      clientSession.createQueue(addressAC, queueName2, null, false);
      clientSession.createQueue(address, queueName, null, false);
      ClientProducer producer = clientSession.createProducer(addressAB);
      ClientProducer producer2 = clientSession.createProducer(addressAC);
      ClientConsumer clientConsumer = clientSession.createConsumer(queueName);
      clientSession.start();
      producer.send(createTextMessage(clientSession, "m1"));
      producer2.send(createTextMessage(clientSession, "m2"));
      ClientMessage m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals("m1", m.getBodyBuffer().readString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      Assert.assertNotNull(m);
      Assert.assertEquals("m2", m.getBodyBuffer().readString());
      m.acknowledge();
      m = clientConsumer.receiveImmediate();
      Assert.assertNull(m);
   }

}
