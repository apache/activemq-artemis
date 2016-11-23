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
package org.apache.activemq.artemis.tests.integration.server;

import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQQueueExistsException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.CoreQueueConfiguration;
import org.apache.activemq.artemis.core.postoffice.Bindings;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.tests.integration.IntegrationTestLogger;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PredefinedQueueTest extends ActiveMQTestBase {

   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   private Configuration configuration = null;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      configuration = createDefaultInVMConfig();
   }

   @Test
   public void testFailOnCreatePredefinedQueues() throws Exception {
      final String testAddress = "testAddress";

      final String queueName1 = "queue1";

      final String queueName2 = "queue2";

      final String queueName3 = "queue3";

      CoreQueueConfiguration queue1 = new CoreQueueConfiguration().setAddress(testAddress).setName(queueName1);

      CoreQueueConfiguration queue2 = new CoreQueueConfiguration().setAddress(testAddress).setName(queueName2);

      CoreQueueConfiguration queue3 = new CoreQueueConfiguration().setAddress(testAddress).setName(queueName3);

      List<CoreQueueConfiguration> queueConfs = new ArrayList<>();

      queueConfs.add(queue1);
      queueConfs.add(queue2);
      queueConfs.add(queue3);

      configuration.setQueueConfigurations(queueConfs);

      ActiveMQServer server = addServer(ActiveMQServers.newActiveMQServer(configuration, false));

      server.start();

      ServerLocator locator = createInVMNonHALocator();

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = addClientSession(sf.createSession(false, true, true));

      try {
         session.createQueue(testAddress, queueName1, "", false);

         Assert.fail("Should throw exception");
      } catch (ActiveMQQueueExistsException se) {
         //ok
      } catch (ActiveMQException e) {
         fail("Invalid Exception type:" + e.getType());
      }
      try {
         session.createQueue(testAddress, queueName2, null, false);

         Assert.fail("Should throw exception");
      } catch (ActiveMQQueueExistsException se) {
         //ok
      } catch (ActiveMQException e) {
         fail("Invalid Exception type:" + e.getType());
      }
      try {
         session.createQueue(testAddress, queueName3, null, false);

         Assert.fail("Should throw exception");
      } catch (ActiveMQQueueExistsException se) {
         //ok
      } catch (ActiveMQException e) {
         fail("Invalid Exception type:" + e.getType());
      }
   }

   @Test
   public void testDeploySameNames() throws Exception {
      final String testAddress = "testAddress";

      final String queueName1 = "queue1";

      final String queueName2 = "queue2";

      CoreQueueConfiguration queue1 = new CoreQueueConfiguration().setAddress(testAddress).setName(queueName1);

      CoreQueueConfiguration queue2 = new CoreQueueConfiguration().setAddress(testAddress).setName(queueName2);

      configuration.addQueueConfiguration(queue1).addQueueConfiguration(queue2);

      ActiveMQServer server = addServer(ActiveMQServers.newActiveMQServer(configuration, false));

      server.start();

      Bindings bindings = server.getPostOffice().getBindingsForAddress(new SimpleString(testAddress));

      Assert.assertEquals(2, bindings.getBindings().size());

      ServerLocator locator = createInVMNonHALocator();

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = addClientSession(sf.createSession(false, true, true));

      session.start();

      ClientProducer producer = addClientProducer(session.createProducer(new SimpleString(testAddress)));

      ClientConsumer consumer1 = addClientConsumer(session.createConsumer(queueName1));

      ClientConsumer consumer2 = addClientConsumer(session.createConsumer(queueName2));

      final int numMessages = 10;

      final SimpleString propKey = new SimpleString("testkey");

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = session.createMessage(false);

         message.putIntProperty(propKey, i);

         producer.send(message);
      }

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = consumer1.receive(200);
         Assert.assertNotNull(message);
         Assert.assertEquals(i, message.getObjectProperty(propKey));
         message.acknowledge();

         message = consumer2.receive(200);
         Assert.assertNotNull(message);
         Assert.assertEquals(i, message.getObjectProperty(propKey));
         message.acknowledge();
      }

      Assert.assertNull(consumer1.receiveImmediate());
      Assert.assertNull(consumer2.receiveImmediate());
   }

   @Test
   public void testDeployPreexistingQueues() throws Exception {
      final String testAddress = "testAddress";

      final String queueName1 = "queue1";

      final String queueName2 = "queue2";

      final String queueName3 = "queue3";

      ActiveMQServer server = addServer(ActiveMQServers.newActiveMQServer(configuration));

      server.start();

      ServerLocator locator = createInVMNonHALocator();

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = addClientSession(sf.createSession(false, true, true));

      session.createQueue(testAddress, queueName1, null, true);

      session.createQueue(testAddress, queueName2, null, true);

      session.createQueue(testAddress, queueName3, null, true);

      session.close();

      sf.close();

      server.stop();

      CoreQueueConfiguration queue1 = new CoreQueueConfiguration().setAddress(testAddress).setName(queueName1);

      CoreQueueConfiguration queue2 = new CoreQueueConfiguration().setAddress(testAddress).setName(queueName2);

      CoreQueueConfiguration queue3 = new CoreQueueConfiguration().setAddress(testAddress).setName(queueName3);

      configuration.addQueueConfiguration(queue1).addQueueConfiguration(queue2).addQueueConfiguration(queue3);

      server.start();

      sf = createSessionFactory(locator);

      session = addClientSession(sf.createSession(false, true, true));

      session.start();

      ClientProducer producer = session.createProducer(new SimpleString(testAddress));

      ClientConsumer consumer1 = session.createConsumer(queueName1);

      ClientConsumer consumer2 = session.createConsumer(queueName2);

      ClientConsumer consumer3 = session.createConsumer(queueName3);

      final int numMessages = 10;

      final SimpleString propKey = new SimpleString("testkey");

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = session.createMessage(false);

         message.putIntProperty(propKey, i);

         producer.send(message);
      }

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = consumer1.receive(200);
         Assert.assertNotNull(message);
         Assert.assertEquals(i, message.getObjectProperty(propKey));
         message.acknowledge();

         message = consumer2.receive(200);
         Assert.assertNotNull(message);
         Assert.assertEquals(i, message.getObjectProperty(propKey));
         message.acknowledge();

         message = consumer3.receive(200);
         Assert.assertNotNull(message);
         Assert.assertEquals(i, message.getObjectProperty(propKey));
         message.acknowledge();
      }

      Assert.assertNull(consumer1.receiveImmediate());
      Assert.assertNull(consumer2.receiveImmediate());
      Assert.assertNull(consumer3.receiveImmediate());
   }

   @Test
   public void testDurableNonDurable() throws Exception {
      final String testAddress = "testAddress";

      final String queueName1 = "queue1";

      final String queueName2 = "queue2";

      CoreQueueConfiguration queue1 = new CoreQueueConfiguration().setAddress(testAddress).setName(queueName1).setDurable(false);

      CoreQueueConfiguration queue2 = new CoreQueueConfiguration().setAddress(testAddress).setName(queueName2);

      List<CoreQueueConfiguration> queueConfs = new ArrayList<>();

      queueConfs.add(queue1);
      queueConfs.add(queue2);

      configuration.addQueueConfiguration(queue1).addQueueConfiguration(queue2);

      ActiveMQServer server = addServer(ActiveMQServers.newActiveMQServer(configuration));

      server.start();

      ServerLocator locator = createInVMNonHALocator();

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = addClientSession(sf.createSession(false, true, true));

      ClientProducer producer = session.createProducer(new SimpleString(testAddress));

      final SimpleString propKey = new SimpleString("testkey");

      final int numMessages = 1;

      PredefinedQueueTest.log.info("sending messages");

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = session.createMessage(true);

         message.putIntProperty(propKey, i);

         producer.send(message);
      }

      session.close();

      PredefinedQueueTest.log.info("stopping");

      sf.close();

      server.stop();

      server.start();

      sf = createSessionFactory(locator);

      session = addClientSession(sf.createSession(false, true, true));

      session.start();

      ClientConsumer consumer1 = session.createConsumer(queueName1);

      ClientConsumer consumer2 = session.createConsumer(queueName2);

      ClientMessage message = consumer1.receiveImmediate();

      Assert.assertNull(message);

      for (int i = 0; i < numMessages; i++) {
         message = consumer2.receive(200);
         Assert.assertNotNull(message);
         Assert.assertEquals(i, message.getObjectProperty(propKey));
         message.acknowledge();
      }

      Assert.assertNull(consumer1.receiveImmediate());
      Assert.assertNull(consumer2.receiveImmediate());
   }

   @Test
   public void testDeployWithFilter() throws Exception {
      final String testAddress = "testAddress";

      final String queueName1 = "queue1";

      final String filter = "cheese='camembert'";

      CoreQueueConfiguration queue1 = new CoreQueueConfiguration().setAddress(testAddress).setName(queueName1).setFilterString(filter).setDurable(false);

      configuration.addQueueConfiguration(queue1);

      ActiveMQServer server = addServer(ActiveMQServers.newActiveMQServer(configuration, false));

      server.start();

      ServerLocator locator = createInVMNonHALocator();

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = addClientSession(sf.createSession(false, true, true));

      ClientProducer producer = session.createProducer(new SimpleString(testAddress));

      final SimpleString propKey = new SimpleString("testkey");

      final int numMessages = 1;

      PredefinedQueueTest.log.info("sending messages");

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = session.createMessage(true);

         message.putStringProperty(new SimpleString("cheese"), new SimpleString("camembert"));

         message.putIntProperty(propKey, i);

         producer.send(message);
      }

      session.start();

      ClientConsumer consumer1 = session.createConsumer(queueName1);

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = consumer1.receive(200);
         Assert.assertNotNull(message);
         Assert.assertEquals(i, message.getObjectProperty(propKey));
         message.acknowledge();
      }

      Assert.assertNull(consumer1.receiveImmediate());

      for (int i = 0; i < numMessages; i++) {
         ClientMessage message = session.createMessage(true);

         message.putStringProperty(new SimpleString("cheese"), new SimpleString("roquefort"));

         message.putIntProperty(propKey, i);

         producer.send(message);
      }

      Assert.assertNull(consumer1.receiveImmediate());
   }

}
