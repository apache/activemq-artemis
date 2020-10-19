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

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.spi.core.security.ActiveMQBasicSecurityManager;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BasicSecurityManagerTest extends ActiveMQTestBase {

   private ServerLocator locator;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      locator = createInVMNonHALocator();
   }

   public ActiveMQServer initializeServer() throws Exception {
      Map<String, String> initProperties = new HashMap<>();
      initProperties.put(ActiveMQBasicSecurityManager.BOOTSTRAP_USER, "first");
      initProperties.put(ActiveMQBasicSecurityManager.BOOTSTRAP_PASSWORD, "secret");
      initProperties.put(ActiveMQBasicSecurityManager.BOOTSTRAP_ROLE, "programmers");
      ActiveMQBasicSecurityManager securityManager = new ActiveMQBasicSecurityManager().init(initProperties);
      ActiveMQServer server = addServer(ActiveMQServers.newActiveMQServer(createDefaultInVMConfig().setSecurityEnabled(true), ManagementFactory.getPlatformMBeanServer(), securityManager, true));
      return server;
   }

   @Test
   public void testAuthenticationForBootstrapUser() throws Exception {
      ActiveMQServer server = initializeServer();
      server.start();
      ClientSessionFactory cf = createSessionFactory(locator);

      try {
         ClientSession session = cf.createSession("first", "secret", false, true, true, false, 0);
         session.close();
      } catch (ActiveMQException e) {
         e.printStackTrace();
         Assert.fail("should not throw exception");
      }
   }

   @Test
   public void testAuthenticationForAddedUserHashed() throws Exception {
      internalTestAuthenticationForAddedUser(false);
   }

   @Test
   public void testAuthenticationForAddedUserPlainText() throws Exception {
      internalTestAuthenticationForAddedUser(true);
   }

   private void internalTestAuthenticationForAddedUser(boolean plaintext) throws Exception {
      ActiveMQServer server = initializeServer();
      server.start();
      ClientSessionFactory cf = createSessionFactory(locator);

      server.getActiveMQServerControl().addUser("foo", "bar", "baz", plaintext);

      try {
         ClientSession session = cf.createSession("foo", "bar", false, true, true, false, 0);
         session.close();
      } catch (ActiveMQException e) {
         e.printStackTrace();
         Assert.fail("should not throw exception");
      }
   }

   @Test
   public void testWithValidatedUser() throws Exception {
      ActiveMQServer server = initializeServer();
      server.getConfiguration().setPopulateValidatedUser(true);
      server.start();
      Role role = new Role("programmers", true, true, true, true, true, true, true, true, true, true);
      Set<Role> roles = new HashSet<>();
      roles.add(role);
      server.getSecurityRepository().addMatch("#", roles);
      ClientSessionFactory cf = createSessionFactory(locator);

      try {
         ClientSession session = cf.createSession("first", "secret", false, true, true, false, 0);
         server.createQueue(new QueueConfiguration("queue").setAddress("address").setRoutingType(RoutingType.ANYCAST));
         ClientProducer producer = session.createProducer("address");
         producer.send(session.createMessage(true));
         session.commit();
         producer.close();
         ClientConsumer consumer = session.createConsumer("queue");
         session.start();
         ClientMessage message = consumer.receive(1000);
         assertNotNull(message);
         assertEquals("first", message.getValidatedUserID());
         session.close();
      } catch (ActiveMQException e) {
         e.printStackTrace();
         Assert.fail("should not throw exception");
      }
   }

   @Test
   public void testAuthenticationBadPassword() throws Exception {
      ActiveMQServer server = initializeServer();
      server.start();
      ClientSessionFactory cf = createSessionFactory(locator);

      try {
         cf.createSession("first", "badpassword", false, true, true, false, 0);
         Assert.fail("should throw exception here");
      } catch (Exception e) {
         // ignore
      }
   }

   @Test
   public void testAuthorizationNegative() throws Exception {
      final SimpleString ADDRESS = new SimpleString("address");
      final SimpleString DURABLE_QUEUE = new SimpleString("durableQueue");
      final SimpleString NON_DURABLE_QUEUE = new SimpleString("nonDurableQueue");

      ActiveMQServer server = initializeServer();
      Set<Role> roles = new HashSet<>();
      roles.add(new Role("programmers", false, false, false, false, false, false, false, false, false, false));
      server.getConfiguration().putSecurityRoles("#", roles);
      server.start();
      server.addAddressInfo(new AddressInfo(ADDRESS, RoutingType.ANYCAST));
      server.createQueue(new QueueConfiguration(DURABLE_QUEUE).setAddress(ADDRESS).setRoutingType(RoutingType.ANYCAST));
      server.createQueue(new QueueConfiguration(NON_DURABLE_QUEUE).setAddress(ADDRESS).setRoutingType(RoutingType.ANYCAST).setDurable(false));

      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession session = addClientSession(cf.createSession("first", "secret", false, true, true, false, 0));

      // CREATE_DURABLE_QUEUE
      try {
         session.createQueue(new QueueConfiguration(DURABLE_QUEUE).setAddress(ADDRESS));
         Assert.fail("should throw exception here");
      } catch (ActiveMQException e) {
         assertTrue(e.getMessage().contains("User: first does not have permission='CREATE_DURABLE_QUEUE' for queue durableQueue on address address"));
      }

      // DELETE_DURABLE_QUEUE
      try {
         session.deleteQueue(DURABLE_QUEUE);
         Assert.fail("should throw exception here");
      } catch (ActiveMQException e) {
         assertTrue(e.getMessage().contains("User: first does not have permission='DELETE_DURABLE_QUEUE' for queue durableQueue on address address"));
      }

      // CREATE_NON_DURABLE_QUEUE
      try {
         session.createQueue(new QueueConfiguration(NON_DURABLE_QUEUE).setAddress(ADDRESS).setDurable(false));
         Assert.fail("should throw exception here");
      } catch (ActiveMQException e) {
         assertTrue(e.getMessage().contains("User: first does not have permission='CREATE_NON_DURABLE_QUEUE' for queue nonDurableQueue on address address"));
      }

      // DELETE_NON_DURABLE_QUEUE
      try {
         session.deleteQueue(NON_DURABLE_QUEUE);
         Assert.fail("should throw exception here");
      } catch (ActiveMQException e) {
         assertTrue(e.getMessage().contains("User: first does not have permission='DELETE_NON_DURABLE_QUEUE' for queue nonDurableQueue on address address"));
      }

      // PRODUCE
      try {
         ClientProducer producer = session.createProducer(ADDRESS);
         producer.send(session.createMessage(true));
         Assert.fail("should throw exception here");
      } catch (ActiveMQException e) {
         assertTrue(e.getMessage().contains("User: first does not have permission='SEND' on address address"));
      }

      // CONSUME
      try {
         ClientConsumer consumer = session.createConsumer(DURABLE_QUEUE);
         Assert.fail("should throw exception here");
      } catch (ActiveMQException e) {
         assertTrue(e.getMessage().contains("User: first does not have permission='CONSUME' for queue durableQueue on address address"));
      }

      // MANAGE
      try {
         ClientProducer producer = session.createProducer(server.getConfiguration().getManagementAddress());
         producer.send(session.createMessage(true));
         Assert.fail("should throw exception here");
      } catch (ActiveMQException e) {
         assertTrue(e.getMessage().contains("User: first does not have permission='MANAGE' on address activemq.management"));
      }

      // BROWSE
      try {
         ClientConsumer browser = session.createConsumer(DURABLE_QUEUE, true);
         Assert.fail("should throw exception here");
      } catch (ActiveMQException e) {
         assertTrue(e.getMessage().contains("User: first does not have permission='BROWSE' for queue durableQueue on address address"));
      }
   }

   @Test
   public void testAuthorizationPositive() throws Exception {
      final SimpleString ADDRESS = new SimpleString("address");
      final SimpleString DURABLE_QUEUE = new SimpleString("durableQueue");
      final SimpleString NON_DURABLE_QUEUE = new SimpleString("nonDurableQueue");

      ActiveMQServer server = initializeServer();
      Set<Role> roles = new HashSet<>();
      roles.add(new Role("programmers", true, true, true, true, true, true, true, true, true, true));
      server.getConfiguration().putSecurityRoles("#", roles);
      server.start();

      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession session = addClientSession(cf.createSession("first", "secret", false, true, true, false, 0));

      // CREATE_DURABLE_QUEUE
      try {
         session.createQueue(new QueueConfiguration(DURABLE_QUEUE).setAddress(ADDRESS));
      } catch (ActiveMQException e) {
         Assert.fail("should not throw exception here");
      }

      // DELETE_DURABLE_QUEUE
      try {
         session.deleteQueue(DURABLE_QUEUE);
      } catch (ActiveMQException e) {
         Assert.fail("should not throw exception here");
      }

      // CREATE_NON_DURABLE_QUEUE
      try {
         session.createQueue(new QueueConfiguration(NON_DURABLE_QUEUE).setAddress(ADDRESS).setDurable(false));
      } catch (ActiveMQException e) {
         Assert.fail("should not throw exception here");
      }

      // DELETE_NON_DURABLE_QUEUE
      try {
         session.deleteQueue(NON_DURABLE_QUEUE);
      } catch (ActiveMQException e) {
         Assert.fail("should not throw exception here");
      }

      session.createQueue(new QueueConfiguration(DURABLE_QUEUE).setAddress(ADDRESS));

      // PRODUCE
      try {
         ClientProducer producer = session.createProducer(ADDRESS);
         producer.send(session.createMessage(true));
      } catch (ActiveMQException e) {
         Assert.fail("should not throw exception here");
      }

      // CONSUME
      try {
         session.createConsumer(DURABLE_QUEUE);
      } catch (ActiveMQException e) {
         Assert.fail("should not throw exception here");
      }

      // MANAGE
      try {
         ClientProducer producer = session.createProducer(server.getConfiguration().getManagementAddress());
         producer.send(session.createMessage(true));
      } catch (ActiveMQException e) {
         Assert.fail("should not throw exception here");
      }

      // BROWSE
      try {
         session.createConsumer(DURABLE_QUEUE, true);
      } catch (ActiveMQException e) {
         Assert.fail("should not throw exception here");
      }
   }
}
