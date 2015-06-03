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

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.HashSet;
import java.util.Set;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQSecurityException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.tests.util.CreateMessage;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.settings.HierarchicalRepository;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManagerImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SecurityTest extends ActiveMQTestBase
{
   /*
    * create session tests
    */
   private static final String addressA = "addressA";

   private static final String queueA = "queueA";

   private ServerLocator locator;

   private Configuration configuration;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      locator = createInVMNonHALocator();
   }

   @Test
   public void testCreateSessionWithNullUserPass() throws Exception
   {
      ActiveMQServer server = createServer();
      ActiveMQSecurityManagerImpl securityManager = (ActiveMQSecurityManagerImpl) server.getSecurityManager();
      securityManager.getConfiguration().addUser("guest", "guest");
      securityManager.getConfiguration().setDefaultUser("guest");
      server.start();
      ClientSessionFactory cf = createSessionFactory(locator);

      try
      {
         ClientSession session = cf.createSession(false, true, true);

         session.close();
      }
      catch (ActiveMQException e)
      {
         Assert.fail("should not throw exception");
      }
   }

   /**
    * @return
    * @throws Exception
    */
   private ActiveMQServer createServer() throws Exception
   {
      configuration = createDefaultInVMConfig()
         .setSecurityEnabled(true);
      ActiveMQServer server = createServer(false, configuration);
      return server;
   }

   @Test
   public void testCreateSessionWithNullUserPassNoGuest() throws Exception
   {
      ActiveMQServer server = createServer();
      server.start();
      ClientSessionFactory cf = createSessionFactory(locator);
      try
      {
         cf.createSession(false, true, true);
         Assert.fail("should throw exception");
      }
      catch (ActiveMQSecurityException se)
      {
         //ok
      }
      catch (ActiveMQException e)
      {
         fail("Invalid Exception type:" + e.getType());
      }
   }

   @Test
   public void testCreateSessionWithCorrectUserWrongPass() throws Exception
   {
      ActiveMQServer server = createServer();
      ActiveMQSecurityManagerImpl securityManager = (ActiveMQSecurityManagerImpl) server.getSecurityManager();
      securityManager.getConfiguration().addUser("newuser", "apass");
      server.start();
      ClientSessionFactory cf = createSessionFactory(locator);

      try
      {
         cf.createSession("newuser", "awrongpass", false, true, true, false, -1);
         Assert.fail("should not throw exception");
      }
      catch (ActiveMQSecurityException se)
      {
         //ok
      }
      catch (ActiveMQException e)
      {
         fail("Invalid Exception type:" + e.getType());
      }
   }

   @Test
   public void testCreateSessionWithCorrectUserCorrectPass() throws Exception
   {
      ActiveMQServer server = createServer();
      ActiveMQSecurityManagerImpl securityManager = (ActiveMQSecurityManagerImpl) server.getSecurityManager();
      securityManager.getConfiguration().addUser("newuser", "apass");
      server.start();
      ClientSessionFactory cf = createSessionFactory(locator);

      try
      {
         ClientSession session = cf.createSession("newuser", "apass", false, true, true, false, -1);

         session.close();
      }
      catch (ActiveMQException e)
      {
         Assert.fail("should not throw exception");
      }
   }

   @Test
   public void testCreateDurableQueueWithRole() throws Exception
   {
      ActiveMQServer server = createServer();
      server.start();
      HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
      ActiveMQSecurityManagerImpl securityManager = (ActiveMQSecurityManagerImpl) server.getSecurityManager();
      securityManager.getConfiguration().addUser("auser", "pass");
      Role role = new Role("arole", false, false, true, false, false, false, false);
      Set<Role> roles = new HashSet<Role>();
      roles.add(role);
      securityRepository.addMatch(SecurityTest.addressA, roles);
      securityManager.getConfiguration().addRole("auser", "arole");
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
      session.createQueue(SecurityTest.addressA, SecurityTest.queueA, true);
      session.close();
   }

   @Test
   public void testCreateDurableQueueWithoutRole() throws Exception
   {
      ActiveMQServer server = createServer();

      server.start();
      HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
      ActiveMQSecurityManagerImpl securityManager = (ActiveMQSecurityManagerImpl) server.getSecurityManager();
      securityManager.getConfiguration().addUser("auser", "pass");
      Role role = new Role("arole", false, false, false, false, false, false, false);
      Set<Role> roles = new HashSet<Role>();
      roles.add(role);
      securityRepository.addMatch(SecurityTest.addressA, roles);
      securityManager.getConfiguration().addRole("auser", "arole");
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
      try
      {
         session.createQueue(SecurityTest.addressA, SecurityTest.queueA, true);
         Assert.fail("should throw exception");
      }
      catch (ActiveMQSecurityException se)
      {
         //ok
      }
      catch (ActiveMQException e)
      {
         fail("Invalid Exception type:" + e.getType());
      }
      session.close();
   }

   @Test
   public void testDeleteDurableQueueWithRole() throws Exception
   {
      ActiveMQServer server = createServer();
      server.start();
      HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
      ActiveMQSecurityManagerImpl securityManager = (ActiveMQSecurityManagerImpl) server.getSecurityManager();
      securityManager.getConfiguration().addUser("auser", "pass");
      Role role = new Role("arole", false, false, true, true, false, false, false);
      Set<Role> roles = new HashSet<Role>();
      roles.add(role);
      securityRepository.addMatch(SecurityTest.addressA, roles);
      securityManager.getConfiguration().addRole("auser", "arole");
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
      session.createQueue(SecurityTest.addressA, SecurityTest.queueA, true);
      session.deleteQueue(SecurityTest.queueA);
      session.close();
   }

   @Test
   public void testDeleteDurableQueueWithoutRole() throws Exception
   {
      ActiveMQServer server = createServer();
      server.start();
      HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
      ActiveMQSecurityManagerImpl securityManager = (ActiveMQSecurityManagerImpl) server.getSecurityManager();
      securityManager.getConfiguration().addUser("auser", "pass");
      Role role = new Role("arole", false, false, true, false, false, false, false);
      Set<Role> roles = new HashSet<Role>();
      roles.add(role);
      securityRepository.addMatch(SecurityTest.addressA, roles);
      securityManager.getConfiguration().addRole("auser", "arole");
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
      session.createQueue(SecurityTest.addressA, SecurityTest.queueA, true);
      try
      {
         session.deleteQueue(SecurityTest.queueA);
         Assert.fail("should throw exception");
      }
      catch (ActiveMQSecurityException se)
      {
         //ok
      }
      catch (ActiveMQException e)
      {
         fail("Invalid Exception type:" + e.getType());
      }
      session.close();
   }

   @Test
   public void testCreateTempQueueWithRole() throws Exception
   {
      ActiveMQServer server = createServer();

      server.start();
      HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
      ActiveMQSecurityManagerImpl securityManager = (ActiveMQSecurityManagerImpl) server.getSecurityManager();
      securityManager.getConfiguration().addUser("auser", "pass");
      Role role = new Role("arole", false, false, false, false, true, false, false);
      Set<Role> roles = new HashSet<Role>();
      roles.add(role);
      securityRepository.addMatch(SecurityTest.addressA, roles);
      securityManager.getConfiguration().addRole("auser", "arole");
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
      session.createQueue(SecurityTest.addressA, SecurityTest.queueA, false);
      session.close();
   }

   @Test
   public void testCreateTempQueueWithoutRole() throws Exception
   {
      ActiveMQServer server = createServer();

      server.start();
      HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
      ActiveMQSecurityManagerImpl securityManager = (ActiveMQSecurityManagerImpl) server.getSecurityManager();
      securityManager.getConfiguration().addUser("auser", "pass");
      Role role = new Role("arole", false, false, false, false, false, false, false);
      Set<Role> roles = new HashSet<Role>();
      roles.add(role);
      securityRepository.addMatch(SecurityTest.addressA, roles);
      securityManager.getConfiguration().addRole("auser", "arole");
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
      try
      {
         session.createQueue(SecurityTest.addressA, SecurityTest.queueA, false);
         Assert.fail("should throw exception");
      }
      catch (ActiveMQSecurityException se)
      {
         //ok
      }
      catch (ActiveMQException e)
      {
         fail("Invalid Exception type:" + e.getType());
      }
      session.close();
   }

   @Test
   public void testDeleteTempQueueWithRole() throws Exception
   {
      ActiveMQServer server = createServer();
      server.start();
      HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
      ActiveMQSecurityManagerImpl securityManager = (ActiveMQSecurityManagerImpl) server.getSecurityManager();
      securityManager.getConfiguration().addUser("auser", "pass");
      Role role = new Role("arole", false, false, false, false, true, true, false);
      Set<Role> roles = new HashSet<Role>();
      roles.add(role);
      securityRepository.addMatch(SecurityTest.addressA, roles);
      securityManager.getConfiguration().addRole("auser", "arole");
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
      session.createQueue(SecurityTest.addressA, SecurityTest.queueA, false);
      session.deleteQueue(SecurityTest.queueA);
      session.close();
   }

   @Test
   public void testDeleteTempQueueWithoutRole() throws Exception
   {
      ActiveMQServer server = createServer();
      server.start();
      HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
      ActiveMQSecurityManagerImpl securityManager = (ActiveMQSecurityManagerImpl) server.getSecurityManager();
      securityManager.getConfiguration().addUser("auser", "pass");
      Role role = new Role("arole", false, false, false, false, true, false, false);
      Set<Role> roles = new HashSet<Role>();
      roles.add(role);
      securityRepository.addMatch(SecurityTest.addressA, roles);
      securityManager.getConfiguration().addRole("auser", "arole");
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
      session.createQueue(SecurityTest.addressA, SecurityTest.queueA, false);
      try
      {
         session.deleteQueue(SecurityTest.queueA);
         Assert.fail("should throw exception");
      }
      catch (ActiveMQSecurityException se)
      {
         //ok
      }
      catch (ActiveMQException e)
      {
         fail("Invalid Exception type:" + e.getType());
      }
      session.close();
   }

   @Test
   public void testSendWithRole() throws Exception
   {
      ActiveMQServer server = createServer();

      server.start();

      HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();

      ActiveMQSecurityManagerImpl securityManager = (ActiveMQSecurityManagerImpl) server.getSecurityManager();

      securityManager.getConfiguration().addUser("auser", "pass");

      Role role = new Role("arole", true, true, true, false, false, false, false);

      Set<Role> roles = new HashSet<Role>();

      roles.add(role);

      securityRepository.addMatch(SecurityTest.addressA, roles);

      securityManager.getConfiguration().addRole("auser", "arole");

      locator.setBlockOnNonDurableSend(true);

      ClientSessionFactory cf = createSessionFactory(locator);

      ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);

      session.createQueue(SecurityTest.addressA, SecurityTest.queueA, true);

      ClientProducer cp = session.createProducer(SecurityTest.addressA);

      cp.send(session.createMessage(false));

      session.start();

      ClientConsumer cons = session.createConsumer(queueA);

      ClientMessage receivedMessage = cons.receive(5000);

      assertNotNull(receivedMessage);

      receivedMessage.acknowledge();

      role = new Role("arole", false, false, true, false, false, false, false);

      roles = new HashSet<Role>();

      roles.add(role);

      // This was added to validate https://issues.jboss.org/browse/SOA-3363
      securityRepository.addMatch(SecurityTest.addressA, roles);
      boolean failed = false;
      try
      {
         cp.send(session.createMessage(true));
      }
      catch (ActiveMQException e)
      {
         failed = true;
      }
      // This was added to validate https://issues.jboss.org/browse/SOA-3363 ^^^^^

      assertTrue("Failure expected on send after removing the match", failed);
   }

   @Test
   public void testSendWithoutRole() throws Exception
   {
      ActiveMQServer server = createServer();

      server.start();
      HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
      ActiveMQSecurityManagerImpl securityManager = (ActiveMQSecurityManagerImpl) server.getSecurityManager();
      securityManager.getConfiguration().addUser("auser", "pass");
      Role role = new Role("arole", false, false, true, false, false, false, false);
      Set<Role> roles = new HashSet<Role>();
      roles.add(role);
      securityRepository.addMatch(SecurityTest.addressA, roles);
      securityManager.getConfiguration().addRole("auser", "arole");
      locator.setBlockOnNonDurableSend(true);
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
      session.createQueue(SecurityTest.addressA, SecurityTest.queueA, true);
      ClientProducer cp = session.createProducer(SecurityTest.addressA);
      try
      {
         cp.send(session.createMessage(false));
      }
      catch (ActiveMQSecurityException se)
      {
         //ok
      }
      catch (ActiveMQException e)
      {
         fail("Invalid Exception type:" + e.getType());
      }
      session.close();
   }

   @Test
   public void testNonBlockSendWithoutRole() throws Exception
   {
      ActiveMQServer server = createServer();

      server.start();
      HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
      ActiveMQSecurityManagerImpl securityManager = (ActiveMQSecurityManagerImpl) server.getSecurityManager();
      securityManager.getConfiguration().addUser("auser", "pass");
      Role role = new Role("arole", false, false, true, false, false, false, false);
      Set<Role> roles = new HashSet<Role>();
      roles.add(role);
      securityRepository.addMatch(SecurityTest.addressA, roles);
      securityManager.getConfiguration().addRole("auser", "arole");
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
      session.createQueue(SecurityTest.addressA, SecurityTest.queueA, true);
      ClientProducer cp = session.createProducer(SecurityTest.addressA);
      cp.send(session.createMessage(false));
      session.close();

      Queue binding = (Queue) server.getPostOffice().getBinding(new SimpleString(SecurityTest.queueA)).getBindable();
      Assert.assertEquals(0, getMessageCount(binding));
   }

   @Test
   public void testCreateConsumerWithRole() throws Exception
   {
      ActiveMQServer server = createServer();
      server.start();
      HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
      ActiveMQSecurityManagerImpl securityManager = (ActiveMQSecurityManagerImpl) server.getSecurityManager();
      securityManager.getConfiguration().addUser("auser", "pass");
      securityManager.getConfiguration().addUser("guest", "guest");
      securityManager.getConfiguration().addRole("guest", "guest");
      securityManager.getConfiguration().setDefaultUser("guest");
      Role role = new Role("arole", false, true, false, false, false, false, false);
      Role sendRole = new Role("guest", true, false, true, false, false, false, false);
      Set<Role> roles = new HashSet<Role>();
      roles.add(sendRole);
      roles.add(role);
      securityRepository.addMatch(SecurityTest.addressA, roles);
      securityManager.getConfiguration().addRole("auser", "arole");
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession senSession = cf.createSession(false, true, true);
      ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
      senSession.createQueue(SecurityTest.addressA, SecurityTest.queueA, true);
      ClientProducer cp = senSession.createProducer(SecurityTest.addressA);
      cp.send(session.createMessage(false));
      session.createConsumer(SecurityTest.queueA);
      session.close();
      senSession.close();
   }

   @Test
   public void testCreateConsumerWithoutRole() throws Exception
   {
      ActiveMQServer server = createServer();
      server.start();
      HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
      ActiveMQSecurityManagerImpl securityManager = (ActiveMQSecurityManagerImpl) server.getSecurityManager();
      securityManager.getConfiguration().addUser("auser", "pass");
      securityManager.getConfiguration().addUser("guest", "guest");
      securityManager.getConfiguration().addRole("guest", "guest");
      securityManager.getConfiguration().setDefaultUser("guest");
      Role role = new Role("arole", false, false, false, false, false, false, false);
      Role sendRole = new Role("guest", true, false, true, false, false, false, false);
      Set<Role> roles = new HashSet<Role>();
      roles.add(sendRole);
      roles.add(role);
      securityRepository.addMatch(SecurityTest.addressA, roles);
      securityManager.getConfiguration().addRole("auser", "arole");
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession senSession = cf.createSession(false, true, true);
      ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
      senSession.createQueue(SecurityTest.addressA, SecurityTest.queueA, true);
      ClientProducer cp = senSession.createProducer(SecurityTest.addressA);
      cp.send(session.createMessage(false));
      try
      {
         session.createConsumer(SecurityTest.queueA);
      }
      catch (ActiveMQSecurityException se)
      {
         //ok
      }
      catch (ActiveMQException e)
      {
         fail("Invalid Exception type:" + e.getType());
      }
      session.close();
      senSession.close();
   }

   @Test
   public void testSendMessageUpdateRoleCached() throws Exception
   {
      Configuration configuration = createDefaultInVMConfig()
         .setSecurityEnabled(true)
         .setSecurityInvalidationInterval(10000);
      ActiveMQServer server = createServer(false, configuration);
      server.start();
      HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
      ActiveMQSecurityManagerImpl securityManager = (ActiveMQSecurityManagerImpl) server.getSecurityManager();
      securityManager.getConfiguration().addUser("auser", "pass");
      securityManager.getConfiguration().addUser("guest", "guest");
      securityManager.getConfiguration().addRole("guest", "guest");
      securityManager.getConfiguration().setDefaultUser("guest");
      Role role = new Role("arole", false, false, false, false, false, false, false);
      Role sendRole = new Role("guest", true, false, true, false, false, false, false);
      Role receiveRole = new Role("receiver", false, true, false, false, false, false, false);
      Set<Role> roles = new HashSet<Role>();
      roles.add(sendRole);
      roles.add(role);
      roles.add(receiveRole);
      securityRepository.addMatch(SecurityTest.addressA, roles);
      securityManager.getConfiguration().addRole("auser", "arole");
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession senSession = cf.createSession(false, true, true);
      ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
      senSession.createQueue(SecurityTest.addressA, SecurityTest.queueA, true);
      ClientProducer cp = senSession.createProducer(SecurityTest.addressA);
      cp.send(session.createMessage(false));
      try
      {
         session.createConsumer(SecurityTest.queueA);
      }
      catch (ActiveMQSecurityException se)
      {
         //ok
      }
      catch (ActiveMQException e)
      {
         fail("Invalid Exception type:" + e.getType());
      }

      securityManager.getConfiguration().addRole("auser", "receiver");

      session.createConsumer(SecurityTest.queueA);

      // Removing the Role... the check should be cached, so the next createConsumer shouldn't fail
      securityManager.getConfiguration().removeRole("auser", "receiver");

      session.createConsumer(SecurityTest.queueA);

      session.close();

      senSession.close();
   }

   @Test
   public void testSendMessageUpdateRoleCached2() throws Exception
   {
      Configuration configuration = createDefaultInVMConfig()
         .setSecurityEnabled(true)
         .setSecurityInvalidationInterval(0);
      ActiveMQServer server = createServer(false, configuration);

      server.start();
      HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
      ActiveMQSecurityManagerImpl securityManager = (ActiveMQSecurityManagerImpl) server.getSecurityManager();
      securityManager.getConfiguration().addUser("auser", "pass");
      securityManager.getConfiguration().addUser("guest", "guest");
      securityManager.getConfiguration().addRole("guest", "guest");
      securityManager.getConfiguration().setDefaultUser("guest");
      Role role = new Role("arole", false, false, false, false, false, false, false);
      Role sendRole = new Role("guest", true, false, true, false, false, false, false);
      Role receiveRole = new Role("receiver", false, true, false, false, false, false, false);
      Set<Role> roles = new HashSet<Role>();
      roles.add(sendRole);
      roles.add(role);
      roles.add(receiveRole);
      securityRepository.addMatch(SecurityTest.addressA, roles);
      securityManager.getConfiguration().addRole("auser", "arole");
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession senSession = cf.createSession(false, true, true);
      ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
      senSession.createQueue(SecurityTest.addressA, SecurityTest.queueA, true);
      ClientProducer cp = senSession.createProducer(SecurityTest.addressA);
      cp.send(session.createMessage(false));
      try
      {
         session.createConsumer(SecurityTest.queueA);
      }
      catch (ActiveMQSecurityException se)
      {
         //ok
      }
      catch (ActiveMQException e)
      {
         fail("Invalid Exception type:" + e.getType());
      }

      securityManager.getConfiguration().addRole("auser", "receiver");

      session.createConsumer(SecurityTest.queueA);

      // Removing the Role... the check should be cached... but we used
      // setSecurityInvalidationInterval(0), so the
      // next createConsumer should fail
      securityManager.getConfiguration().removeRole("auser", "receiver");

      try
      {
         session.createConsumer(SecurityTest.queueA);
      }
      catch (ActiveMQSecurityException se)
      {
         //ok
      }
      catch (ActiveMQException e)
      {
         fail("Invalid Exception type:" + e.getType());
      }

      session.close();

      senSession.close();
   }

   @Test
   public void testSendMessageUpdateSender() throws Exception
   {
      Configuration configuration = createDefaultInVMConfig()
         .setSecurityEnabled(true)
         .setSecurityInvalidationInterval(-1);
      ActiveMQServer server = createServer(false, configuration);
      server.start();
      HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
      ActiveMQSecurityManagerImpl securityManager = (ActiveMQSecurityManagerImpl) server.getSecurityManager();
      securityManager.getConfiguration().addUser("auser", "pass");
      securityManager.getConfiguration().addUser("guest", "guest");
      securityManager.getConfiguration().addRole("guest", "guest");
      securityManager.getConfiguration().setDefaultUser("guest");
      Role role = new Role("arole", false, false, false, false, false, false, false);
      System.out.println("guest:" + role);
      Role sendRole = new Role("guest", true, false, true, false, false, false, false);
      System.out.println("guest:" + sendRole);
      Role receiveRole = new Role("receiver", false, true, false, false, false, false, false);
      System.out.println("guest:" + receiveRole);
      Set<Role> roles = new HashSet<Role>();
      roles.add(sendRole);
      roles.add(role);
      roles.add(receiveRole);
      securityRepository.addMatch(SecurityTest.addressA, roles);
      securityManager.getConfiguration().addRole("auser", "arole");
      ClientSessionFactory cf = createSessionFactory(locator);

      ClientSession senSession = cf.createSession(false, true, true);
      ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
      senSession.createQueue(SecurityTest.addressA, SecurityTest.queueA, true);
      ClientProducer cp = senSession.createProducer(SecurityTest.addressA);
      cp.send(session.createMessage(false));
      try
      {
         session.createConsumer(SecurityTest.queueA);
      }
      catch (ActiveMQSecurityException se)
      {
         //ok
      }
      catch (ActiveMQException e)
      {
         fail("Invalid Exception type:" + e.getType());
      }

      securityManager.getConfiguration().addRole("auser", "receiver");

      session.createConsumer(SecurityTest.queueA);

      // Removing the Role... the check should be cached... but we used
      // setSecurityInvalidationInterval(0), so the
      // next createConsumer should fail
      securityManager.getConfiguration().removeRole("auser", "guest");

      ClientSession sendingSession = cf.createSession("auser", "pass", false, false, false, false, 0);
      ClientProducer prod = sendingSession.createProducer(SecurityTest.addressA);
      prod.send(CreateMessage.createTextMessage(sendingSession, "Test", true));
      prod.send(CreateMessage.createTextMessage(sendingSession, "Test", true));
      try
      {
         sendingSession.commit();
         Assert.fail("Expected exception");
      }
      catch (ActiveMQException e)
      {
         // I would expect the commit to fail, since there were failures registered
      }

      sendingSession.close();

      Xid xid = newXID();

      sendingSession = cf.createSession("auser", "pass", true, false, false, false, 0);
      sendingSession.start(xid, XAResource.TMNOFLAGS);

      prod = sendingSession.createProducer(SecurityTest.addressA);
      prod.send(CreateMessage.createTextMessage(sendingSession, "Test", true));
      prod.send(CreateMessage.createTextMessage(sendingSession, "Test", true));
      sendingSession.end(xid, XAResource.TMSUCCESS);

      try
      {
         sendingSession.prepare(xid);
         Assert.fail("Exception was expected");
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }

      // A prepare shouldn't mark any recoverable resources
      Xid[] xids = sendingSession.recover(XAResource.TMSTARTRSCAN);
      Assert.assertEquals(0, xids.length);

      session.close();

      senSession.close();

      sendingSession.close();
   }

   @Test
   public void testSendManagementWithRole() throws Exception
   {
      ActiveMQServer server = createServer();

      server.start();
      HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
      ActiveMQSecurityManagerImpl securityManager = (ActiveMQSecurityManagerImpl) server.getSecurityManager();
      securityManager.getConfiguration().addUser("auser", "pass");
      Role role = new Role("arole", false, false, false, false, false, false, true);
      Set<Role> roles = new HashSet<Role>();
      roles.add(role);
      securityRepository.addMatch(configuration.getManagementAddress().toString(), roles);
      securityManager.getConfiguration().addRole("auser", "arole");
      locator.setBlockOnNonDurableSend(true);
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
      ClientProducer cp = session.createProducer(configuration.getManagementAddress());
      cp.send(session.createMessage(false));
      session.close();
   }

   @Test
   public void testSendManagementWithoutRole() throws Exception
   {
      ActiveMQServer server = createServer();

      server.start();
      HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
      ActiveMQSecurityManagerImpl securityManager = (ActiveMQSecurityManagerImpl) server.getSecurityManager();
      securityManager.getConfiguration().addUser("auser", "pass");
      Role role = new Role("arole", false, false, true, false, false, false, false);
      Set<Role> roles = new HashSet<Role>();
      roles.add(role);
      securityRepository.addMatch(configuration.getManagementAddress().toString(), roles);
      securityManager.getConfiguration().addRole("auser", "arole");
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
      session.createQueue(configuration.getManagementAddress().toString(), SecurityTest.queueA, true);
      ClientProducer cp = session.createProducer(configuration.getManagementAddress());
      cp.send(session.createMessage(false));
      try
      {
         cp.send(session.createMessage(false));
      }
      catch (ActiveMQSecurityException se)
      {
         //ok
      }
      catch (ActiveMQException e)
      {
         fail("Invalid Exception type:" + e.getType());
      }
      session.close();

   }

   @Test
   public void testNonBlockSendManagementWithoutRole() throws Exception
   {
      ActiveMQServer server = createServer();

      server.start();
      HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
      ActiveMQSecurityManagerImpl securityManager = (ActiveMQSecurityManagerImpl) server.getSecurityManager();
      securityManager.getConfiguration().addUser("auser", "pass");
      Role role = new Role("arole", false, false, true, false, false, false, false);
      Set<Role> roles = new HashSet<Role>();
      roles.add(role);
      securityRepository.addMatch(configuration.getManagementAddress().toString(), roles);
      securityManager.getConfiguration().addRole("auser", "arole");
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
      session.createQueue(configuration.getManagementAddress().toString(), SecurityTest.queueA, true);
      ClientProducer cp = session.createProducer(configuration.getManagementAddress());
      cp.send(session.createMessage(false));
      session.close();

      Queue binding = (Queue) server.getPostOffice().getBinding(new SimpleString(SecurityTest.queueA)).getBindable();
      Assert.assertEquals(0, getMessageCount(binding));

   }

   @Test
   public void testComplexRoles() throws Exception
   {
      ActiveMQServer server = createServer();
      server.start();
      ActiveMQSecurityManagerImpl securityManager = (ActiveMQSecurityManagerImpl) server.getSecurityManager();
      securityManager.getConfiguration().addUser("all", "all");
      securityManager.getConfiguration().addUser("bill", "activemq");
      securityManager.getConfiguration().addUser("andrew", "activemq1");
      securityManager.getConfiguration().addUser("frank", "activemq2");
      securityManager.getConfiguration().addUser("sam", "activemq3");
      securityManager.getConfiguration().addRole("all", "all");
      securityManager.getConfiguration().addRole("bill", "user");
      securityManager.getConfiguration().addRole("andrew", "europe-user");
      securityManager.getConfiguration().addRole("andrew", "user");
      securityManager.getConfiguration().addRole("frank", "us-user");
      securityManager.getConfiguration().addRole("frank", "news-user");
      securityManager.getConfiguration().addRole("frank", "user");
      securityManager.getConfiguration().addRole("sam", "news-user");
      securityManager.getConfiguration().addRole("sam", "user");
      Role all = new Role("all", true, true, true, true, true, true, true);
      HierarchicalRepository<Set<Role>> repository = server.getSecurityRepository();
      Set<Role> add = new HashSet<Role>();
      add.add(new Role("user", true, true, true, true, true, true, false));
      add.add(all);
      repository.addMatch("#", add);
      Set<Role> add1 = new HashSet<Role>();
      add1.add(all);
      add1.add(new Role("user", false, false, true, true, true, true, false));
      add1.add(new Role("europe-user", true, false, false, false, false, false, false));
      add1.add(new Role("news-user", false, true, false, false, false, false, false));
      repository.addMatch("news.europe.#", add1);
      Set<Role> add2 = new HashSet<Role>();
      add2.add(all);
      add2.add(new Role("user", false, false, true, true, true, true, false));
      add2.add(new Role("us-user", true, false, false, false, false, false, false));
      add2.add(new Role("news-user", false, true, false, false, false, false, false));
      repository.addMatch("news.us.#", add2);
      ClientSession billConnection = null;
      ClientSession andrewConnection = null;
      ClientSession frankConnection = null;
      ClientSession samConnection = null;
      locator.setBlockOnNonDurableSend(true)
              .setBlockOnDurableSend(true);
      ClientSessionFactory factory = createSessionFactory(locator);

      ClientSession adminSession = factory.createSession("all", "all", false, true, true, false, -1);
      String genericQueueName = "genericQueue";
      adminSession.createQueue(genericQueueName, genericQueueName, false);
      String eurQueueName = "news.europe.europeQueue";
      adminSession.createQueue(eurQueueName, eurQueueName, false);
      String usQueueName = "news.us.usQueue";
      adminSession.createQueue(usQueueName, usQueueName, false);
      // Step 4. Try to create a JMS Connection without user/password. It will fail.
      try
      {
         factory.createSession(false, true, true);
         Assert.fail("should throw exception");
      }
      catch (ActiveMQSecurityException se)
      {
         //ok
      }
      catch (ActiveMQException e)
      {
         fail("Invalid Exception type:" + e.getType());
      }

      // Step 5. bill tries to make a connection using wrong password
      try
      {
         billConnection = factory.createSession("bill", "activemq1", false, true, true, false, -1);
         Assert.fail("should throw exception");
      }
      catch (ActiveMQSecurityException se)
      {
         //ok
      }
      catch (ActiveMQException e)
      {
         fail("Invalid Exception type:" + e.getType());
      }

      // Step 6. bill makes a good connection.
      billConnection = factory.createSession("bill", "activemq", false, true, true, false, -1);

      // Step 7. andrew makes a good connection.
      andrewConnection = factory.createSession("andrew", "activemq1", false, true, true, false, -1);

      // Step 8. frank makes a good connection.
      frankConnection = factory.createSession("frank", "activemq2", false, true, true, false, -1);

      // Step 9. sam makes a good connection.
      samConnection = factory.createSession("sam", "activemq3", false, true, true, false, -1);

      checkUserSendAndReceive(genericQueueName, billConnection);
      checkUserSendAndReceive(genericQueueName, andrewConnection);
      checkUserSendAndReceive(genericQueueName, frankConnection);
      checkUserSendAndReceive(genericQueueName, samConnection);

      // Step 11. Check permissions on news.europe.europeTopic for bill: can't send and can't
      // receive
      checkUserNoSendNoReceive(eurQueueName, billConnection, adminSession);

      // Step 12. Check permissions on news.europe.europeTopic for andrew: can send but can't
      // receive
      checkUserSendNoReceive(eurQueueName, andrewConnection);

      // Step 13. Check permissions on news.europe.europeTopic for frank: can't send but can
      // receive
      checkUserReceiveNoSend(eurQueueName, frankConnection, adminSession);

      // Step 14. Check permissions on news.europe.europeTopic for sam: can't send but can
      // receive
      checkUserReceiveNoSend(eurQueueName, samConnection, adminSession);

      // Step 15. Check permissions on news.us.usTopic for bill: can't send and can't receive
      checkUserNoSendNoReceive(usQueueName, billConnection, adminSession);

      // Step 16. Check permissions on news.us.usTopic for andrew: can't send and can't receive
      checkUserNoSendNoReceive(usQueueName, andrewConnection, adminSession);

      // Step 17. Check permissions on news.us.usTopic for frank: can both send and receive
      checkUserSendAndReceive(usQueueName, frankConnection);

      // Step 18. Check permissions on news.us.usTopic for same: can't send but can receive
      checkUserReceiveNoSend(usQueueName, samConnection, adminSession);

      billConnection.close();

      andrewConnection.close();

      frankConnection.close();

      samConnection.close();

      adminSession.close();

   }

   public void _testComplexRoles2() throws Exception
   {
      ActiveMQServer server = createServer();
      server.start();
      ActiveMQSecurityManagerImpl securityManager = (ActiveMQSecurityManagerImpl) server.getSecurityManager();
      securityManager.getConfiguration().addUser("all", "all");
      securityManager.getConfiguration().addUser("bill", "activemq");
      securityManager.getConfiguration().addUser("andrew", "activemq1");
      securityManager.getConfiguration().addUser("frank", "activemq2");
      securityManager.getConfiguration().addUser("sam", "activemq3");
      securityManager.getConfiguration().addRole("all", "all");
      securityManager.getConfiguration().addRole("bill", "user");
      securityManager.getConfiguration().addRole("andrew", "europe-user");
      securityManager.getConfiguration().addRole("andrew", "user");
      securityManager.getConfiguration().addRole("frank", "us-user");
      securityManager.getConfiguration().addRole("frank", "news-user");
      securityManager.getConfiguration().addRole("frank", "user");
      securityManager.getConfiguration().addRole("sam", "news-user");
      securityManager.getConfiguration().addRole("sam", "user");
      Role all = new Role("all", true, true, true, true, true, true, true);
      HierarchicalRepository<Set<Role>> repository = server.getSecurityRepository();
      Set<Role> add = new HashSet<Role>();
      add.add(new Role("user", true, true, true, true, true, true, false));
      add.add(all);
      repository.addMatch("#", add);
      Set<Role> add1 = new HashSet<Role>();
      add1.add(all);
      add1.add(new Role("user", false, false, true, true, true, true, false));
      add1.add(new Role("europe-user", true, false, false, false, false, false, false));
      add1.add(new Role("news-user", false, true, false, false, false, false, false));
      repository.addMatch("news.europe.#", add1);
      Set<Role> add2 = new HashSet<Role>();
      add2.add(all);
      add2.add(new Role("user", false, false, true, true, true, true, false));
      add2.add(new Role("us-user", true, false, false, false, false, false, false));
      add2.add(new Role("news-user", false, true, false, false, false, false, false));
      repository.addMatch("news.us.#", add2);
      ClientSession billConnection = null;
      ClientSession andrewConnection = null;
      ClientSession frankConnection = null;
      ClientSession samConnection = null;
      ClientSessionFactory factory = createSessionFactory(locator);
      factory.getServerLocator().setBlockOnNonDurableSend(true)
              .setBlockOnDurableSend(true);

      ClientSession adminSession = factory.createSession("all", "all", false, true, true, false, -1);
      String genericQueueName = "genericQueue";
      adminSession.createQueue(genericQueueName, genericQueueName, false);
      String eurQueueName = "news.europe.europeQueue";
      adminSession.createQueue(eurQueueName, eurQueueName, false);
      String usQueueName = "news.us.usQueue";
      adminSession.createQueue(usQueueName, usQueueName, false);
      // Step 4. Try to create a JMS Connection without user/password. It will fail.
      try
      {
         factory.createSession(false, true, true);
         Assert.fail("should throw exception");
      }
      catch (ActiveMQSecurityException se)
      {
         //ok
      }
      catch (ActiveMQException e)
      {
         fail("Invalid Exception type:" + e.getType());
      }

      // Step 5. bill tries to make a connection using wrong password
      try
      {
         billConnection = factory.createSession("bill", "activemq1", false, true, true, false, -1);
         Assert.fail("should throw exception");
      }
      catch (ActiveMQSecurityException se)
      {
         //ok
      }
      catch (ActiveMQException e)
      {
         fail("Invalid Exception type:" + e.getType());
      }

      // Step 6. bill makes a good connection.
      billConnection = factory.createSession("bill", "activemq", false, true, true, false, -1);

      // Step 7. andrew makes a good connection.
      andrewConnection = factory.createSession("andrew", "activemq1", false, true, true, false, -1);

      // Step 8. frank makes a good connection.
      frankConnection = factory.createSession("frank", "activemq2", false, true, true, false, -1);

      // Step 9. sam makes a good connection.
      samConnection = factory.createSession("sam", "activemq3", false, true, true, false, -1);

      checkUserSendAndReceive(genericQueueName, billConnection);
      checkUserSendAndReceive(genericQueueName, andrewConnection);
      checkUserSendAndReceive(genericQueueName, frankConnection);
      checkUserSendAndReceive(genericQueueName, samConnection);

      // Step 11. Check permissions on news.europe.europeTopic for bill: can't send and can't
      // receive
      checkUserNoSendNoReceive(eurQueueName, billConnection, adminSession);

      // Step 12. Check permissions on news.europe.europeTopic for andrew: can send but can't
      // receive
      checkUserSendNoReceive(eurQueueName, andrewConnection);

      // Step 13. Check permissions on news.europe.europeTopic for frank: can't send but can
      // receive
      checkUserReceiveNoSend(eurQueueName, frankConnection, adminSession);

      // Step 14. Check permissions on news.europe.europeTopic for sam: can't send but can
      // receive
      checkUserReceiveNoSend(eurQueueName, samConnection, adminSession);

      // Step 15. Check permissions on news.us.usTopic for bill: can't send and can't receive
      checkUserNoSendNoReceive(usQueueName, billConnection, adminSession);

      // Step 16. Check permissions on news.us.usTopic for andrew: can't send and can't receive
      checkUserNoSendNoReceive(usQueueName, andrewConnection, adminSession);

      // Step 17. Check permissions on news.us.usTopic for frank: can both send and receive
      checkUserSendAndReceive(usQueueName, frankConnection);

      // Step 18. Check permissions on news.us.usTopic for same: can't send but can receive
      checkUserReceiveNoSend(usQueueName, samConnection, adminSession);

   }

   // Check the user connection has both send and receive permissions on the queue
   private void checkUserSendAndReceive(final String genericQueueName, final ClientSession connection) throws Exception
   {
      connection.start();
      try
      {
         ClientProducer prod = connection.createProducer(genericQueueName);
         ClientConsumer con = connection.createConsumer(genericQueueName);
         ClientMessage m = connection.createMessage(false);
         prod.send(m);
         ClientMessage rec = con.receive(1000);
         Assert.assertNotNull(rec);
         rec.acknowledge();
      }
      finally
      {
         connection.stop();
      }
   }

   // Check the user can receive message but cannot send message.
   private void checkUserReceiveNoSend(final String queue, final ClientSession connection,
                                       final ClientSession sendingConn) throws Exception
   {
      connection.start();
      try
      {
         ClientProducer prod = connection.createProducer(queue);
         ClientMessage m = connection.createMessage(false);
         try
         {
            prod.send(m);
            Assert.fail("should throw exception");
         }
         catch (ActiveMQException e)
         {
            // pass
         }

         prod = sendingConn.createProducer(queue);
         prod.send(m);
         ClientConsumer con = connection.createConsumer(queue);
         ClientMessage rec = con.receive(1000);
         Assert.assertNotNull(rec);
         rec.acknowledge();
      }
      finally
      {
         connection.stop();
      }
   }

   private void checkUserNoSendNoReceive(final String queue, final ClientSession connection,
                                         final ClientSession sendingConn) throws Exception
   {
      connection.start();
      try
      {
         ClientProducer prod = connection.createProducer(queue);
         ClientMessage m = connection.createMessage(false);
         try
         {
            prod.send(m);
            Assert.fail("should throw exception");
         }
         catch (ActiveMQException e)
         {
            // pass
         }

         prod = sendingConn.createProducer(queue);
         prod.send(m);

         try
         {
            connection.createConsumer(queue);
            Assert.fail("should throw exception");
         }
         catch (ActiveMQException e)
         {
            // pass
         }
      }
      finally
      {
         connection.stop();
      }
   }

   // Check the user can send message but cannot receive message
   private void checkUserSendNoReceive(final String queue, final ClientSession connection) throws Exception
   {
      ClientProducer prod = connection.createProducer(queue);
      ClientMessage m = connection.createMessage(false);
      prod.send(m);

      try
      {
         connection.createConsumer(queue);
         Assert.fail("should throw exception");
      }
      catch (ActiveMQSecurityException se)
      {
         //ok
      }
      catch (ActiveMQException e)
      {
         fail("Invalid Exception type:" + e.getType());
      }
   }
}
