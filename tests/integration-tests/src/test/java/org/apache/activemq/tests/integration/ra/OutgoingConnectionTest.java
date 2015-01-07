/**
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
package org.apache.activemq.tests.integration.ra;

import javax.jms.Connection;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSProducer;
import javax.jms.JMSSecurityException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.TopicConnection;
import javax.jms.XAConnection;
import javax.jms.XAQueueConnection;
import javax.jms.XASession;
import javax.resource.spi.ManagedConnection;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.HashSet;
import java.util.Set;

import org.apache.activemq.api.core.client.ClientConsumer;
import org.apache.activemq.api.core.client.ClientMessage;
import org.apache.activemq.api.core.client.ClientSession;
import org.apache.activemq.api.core.client.ClientSessionFactory;
import org.apache.activemq.api.jms.ActiveMQJMSClient;
import org.apache.activemq.core.remoting.impl.invm.InVMConnectorFactory;
import org.apache.activemq.core.security.Role;
import org.apache.activemq.core.transaction.impl.XidImpl;
import org.apache.activemq.ra.ActiveMQRAConnectionFactory;
import org.apache.activemq.ra.ActiveMQRAConnectionFactoryImpl;
import org.apache.activemq.ra.ActiveMQRAConnectionManager;
import org.apache.activemq.ra.ActiveMQRAManagedConnectionFactory;
import org.apache.activemq.ra.ActiveMQRASession;
import org.apache.activemq.ra.ActiveMQResourceAdapter;
import org.apache.activemq.service.extensions.xa.ActiveMQXAResourceWrapper;
import org.apache.activemq.service.extensions.xa.ActiveMQXAResourceWrapperImpl;
import org.apache.activemq.utils.UUIDGenerator;
import org.apache.activemq.utils.VersionLoader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 * @author <a href="mailto:mtaylor@redhat.com">Martyn Taylor</a>
 *         Created Jul 7, 2010
 */
public class OutgoingConnectionTest extends ActiveMQRATestBase
{
   private ActiveMQResourceAdapter resourceAdapter;
   private ActiveMQRAConnectionFactory qraConnectionFactory;
   private ActiveMQRAManagedConnectionFactory mcf;

   @Override
   public boolean useSecurity()
   {
      return true;
   }

   ActiveMQRAConnectionManager qraConnectionManager = new ActiveMQRAConnectionManager();

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      server.getSecurityManager().addUser("testuser", "testpassword");
      server.getSecurityManager().addUser("guest", "guest");
      server.getSecurityManager().setDefaultUser("guest");
      server.getSecurityManager().addRole("testuser", "arole");
      server.getSecurityManager().addRole("guest", "arole");
      Role role = new Role("arole", true, true, true, true, true, true, true);
      Set<Role> roles = new HashSet<Role>();
      roles.add(role);
      server.getSecurityRepository().addMatch(MDBQUEUEPREFIXED, roles);

      resourceAdapter = new ActiveMQResourceAdapter();
      resourceAdapter.setEntries("[\"java://jmsXA\"]");

      resourceAdapter.setConnectorClassName(InVMConnectorFactory.class.getName());
      MyBootstrapContext ctx = new MyBootstrapContext();
      resourceAdapter.start(ctx);
      mcf = new ActiveMQRAManagedConnectionFactory();
      mcf.setResourceAdapter(resourceAdapter);
      qraConnectionFactory = new ActiveMQRAConnectionFactoryImpl(mcf, qraConnectionManager);
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      DummyTransactionManager.tm.tx = null;
      if (resourceAdapter != null)
      {
         resourceAdapter.stop();
      }

      qraConnectionManager.stop();
      super.tearDown();
   }

   @Test
   public void testSimpleMessageSendAndReceive() throws Exception
   {
      QueueConnection queueConnection = qraConnectionFactory.createQueueConnection();
      Session s = queueConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue q = ActiveMQJMSClient.createQueue(MDBQUEUE);
      MessageProducer mp = s.createProducer(q);
      MessageConsumer consumer = s.createConsumer(q);
      Message message = s.createTextMessage("test");
      mp.send(message);
      queueConnection.start();
      TextMessage textMessage = (TextMessage) consumer.receive(1000);
      assertNotNull(textMessage);
      assertEquals(textMessage.getText(), "test");
   }

   @Test
   public void testSimpleSendNoXAJMSContext() throws Exception
   {
      Queue q = ActiveMQJMSClient.createQueue(MDBQUEUE);

      try (ClientSessionFactory sf = locator.createSessionFactory();
           ClientSession session = sf.createSession();
           ClientConsumer consVerify = session.createConsumer("jms.queue." + MDBQUEUE);
           JMSContext jmsctx = qraConnectionFactory.createContext();
      )
      {
         session.start();
         // These next 4 lines could be written in a single line however it makes difficult for debugging
         JMSProducer producer = jmsctx.createProducer();
         producer.setProperty("strvalue", "hello");
         TextMessage msgsend = jmsctx.createTextMessage("hello");
         producer.send(q, msgsend);

         ClientMessage msg = consVerify.receive(1000);
         assertNotNull(msg);
         assertEquals("hello", msg.getStringProperty("strvalue"));
      }
   }

   @Test
   public void testSimpleSendNoXAJMS1() throws Exception
   {
      Queue q = ActiveMQJMSClient.createQueue(MDBQUEUE);
      try (ClientSessionFactory sf = locator.createSessionFactory();
           ClientSession session = sf.createSession();
           ClientConsumer consVerify = session.createConsumer("jms.queue." + MDBQUEUE);
           Connection conn = qraConnectionFactory.createConnection();
      )
      {
         Session jmsSess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         session.start();
         MessageProducer producer = jmsSess.createProducer(q);
         // These next 4 lines could be written in a single line however it makes difficult for debugging
         TextMessage msgsend = jmsSess.createTextMessage("hello");
         msgsend.setStringProperty("strvalue", "hello");
         producer.send(msgsend);

         ClientMessage msg = consVerify.receive(1000);
         assertNotNull(msg);
         assertEquals("hello", msg.getStringProperty("strvalue"));
      }
   }

   @Test
   public void testSimpleMessageSendAndReceiveXA() throws Exception
   {
      Xid xid = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());
      XAQueueConnection queueConnection = qraConnectionFactory.createXAQueueConnection();
      XASession s = queueConnection.createXASession();

      XAResource resource = s.getXAResource();
      resource.start(xid, XAResource.TMNOFLAGS);
      Queue q = ActiveMQJMSClient.createQueue(MDBQUEUE);
      MessageProducer mp = s.createProducer(q);
      MessageConsumer consumer = s.createConsumer(q);
      Message message = s.createTextMessage("test");
      mp.send(message);
      queueConnection.start();
      TextMessage textMessage = (TextMessage) consumer.receiveNoWait();
      assertNull(textMessage);
      resource.end(xid, XAResource.TMSUCCESS);
      resource.commit(xid, true);
      resource.start(xid, XAResource.TMNOFLAGS);
      textMessage = (TextMessage) consumer.receiveNoWait();
      resource.end(xid, XAResource.TMSUCCESS);
      resource.commit(xid, true);
      assertNotNull(textMessage);
      assertEquals(textMessage.getText(), "test");

      // When I wrote this call, this method was doing an infinite loop.
      // this is just to avoid such thing again
      textMessage.getJMSDeliveryTime();

   }

   @Test
   public void testInexistentUserOnCreateConnection() throws Exception
   {
      resourceAdapter = newResourceAdapter();
      MyBootstrapContext ctx = new MyBootstrapContext();
      resourceAdapter.start(ctx);
      ActiveMQRAManagedConnectionFactory mcf = new ActiveMQRAManagedConnectionFactory();
      mcf.setResourceAdapter(resourceAdapter);
      ActiveMQRAConnectionFactory qraConnectionFactory = new ActiveMQRAConnectionFactoryImpl(mcf, qraConnectionManager);

      Connection conn = null;
      try
      {
         conn = qraConnectionFactory.createConnection("IDont", "Exist");
         fail("Exception was expected");
      }
      catch (JMSSecurityException expected)
      {
      }

      conn = qraConnectionFactory.createConnection("testuser", "testpassword");
      conn.close();


      try
      {
         XAConnection xaconn = qraConnectionFactory.createXAConnection("IDont", "Exist");
         fail("Exception was expected");
      }
      catch (JMSSecurityException expected)
      {
      }

      XAConnection xaconn = qraConnectionFactory.createXAConnection("testuser", "testpassword");
      xaconn.close();

      try
      {
         TopicConnection topicconn = qraConnectionFactory.createTopicConnection("IDont", "Exist");
         fail("Exception was expected");
      }
      catch (JMSSecurityException expected)
      {
      }

      TopicConnection topicconn = qraConnectionFactory.createTopicConnection("testuser", "testpassword");
      topicconn.close();

      try
      {
         QueueConnection queueconn = qraConnectionFactory.createQueueConnection("IDont", "Exist");
         fail("Exception was expected");
      }
      catch (JMSSecurityException expected)
      {
      }

      QueueConnection queueconn = qraConnectionFactory.createQueueConnection("testuser", "testpassword");
      queueconn.close();

      mcf.stop();


   }

   @Test
   public void testMultipleSessionsThrowsException() throws Exception
   {
      resourceAdapter = newResourceAdapter();
      MyBootstrapContext ctx = new MyBootstrapContext();
      resourceAdapter.start(ctx);
      ActiveMQRAManagedConnectionFactory mcf = new ActiveMQRAManagedConnectionFactory();
      mcf.setResourceAdapter(resourceAdapter);
      ActiveMQRAConnectionFactory qraConnectionFactory = new ActiveMQRAConnectionFactoryImpl(mcf, qraConnectionManager);
      QueueConnection queueConnection = qraConnectionFactory.createQueueConnection();
      Session s = queueConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      try
      {
         Session s2 = queueConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         fail("should throw javax,jms.IllegalStateException: Only allowed one session per connection. See the J2EE spec, e.g. J2EE1.4 Section 6.6");
      }
      catch (JMSException e)
      {
      }
   }

   @Test
   public void testConnectionCredentials() throws Exception
   {
      resourceAdapter = newResourceAdapter();
      MyBootstrapContext ctx = new MyBootstrapContext();
      resourceAdapter.start(ctx);
      ActiveMQRAManagedConnectionFactory mcf = new ActiveMQRAManagedConnectionFactory();
      mcf.setResourceAdapter(resourceAdapter);
      ActiveMQRAConnectionFactory qraConnectionFactory = new ActiveMQRAConnectionFactoryImpl(mcf, qraConnectionManager);
      QueueConnection queueConnection = qraConnectionFactory.createQueueConnection();
      QueueSession session = queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);

      queueConnection = qraConnectionFactory.createQueueConnection("testuser", "testpassword");
      session = queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);

   }

   @Test
   public void testConnectionCredentialsFail() throws Exception
   {
      resourceAdapter = newResourceAdapter();
      MyBootstrapContext ctx = new MyBootstrapContext();
      resourceAdapter.start(ctx);
      ActiveMQRAManagedConnectionFactory mcf = new ActiveMQRAManagedConnectionFactory();
      mcf.setResourceAdapter(resourceAdapter);
      ActiveMQRAConnectionFactory qraConnectionFactory = new ActiveMQRAConnectionFactoryImpl(mcf, qraConnectionManager);
      QueueConnection queueConnection = qraConnectionFactory.createQueueConnection();
      QueueSession session = queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);

      ManagedConnection mc = ((ActiveMQRASession) session).getManagedConnection();
      queueConnection.close();
      mc.destroy();

      try
      {
         queueConnection = qraConnectionFactory.createQueueConnection("testuser", "testwrongpassword");
         queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE).close();
         fail("should throw esxception");
      }
      catch (JMSException e)
      {
         //pass
      }
   }

   @Test
   public void testConnectionCredentialsFailRecovery() throws Exception
   {
      resourceAdapter = newResourceAdapter();
      MyBootstrapContext ctx = new MyBootstrapContext();
      resourceAdapter.start(ctx);
      ActiveMQRAManagedConnectionFactory mcf = new ActiveMQRAManagedConnectionFactory();
      mcf.setResourceAdapter(resourceAdapter);
      ActiveMQRAConnectionFactory qraConnectionFactory = new ActiveMQRAConnectionFactoryImpl(mcf, qraConnectionManager);
      try
      {
         QueueConnection queueConnection = qraConnectionFactory.createQueueConnection("testuser", "testwrongpassword");
         queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE).close();
         fail("should throw esxception");
      }
      catch (JMSException e)
      {
         //make sure the recovery is null
         assertNull(mcf.getResourceRecovery());
      }
   }

   @Test
   public void testConnectionCredentialsOKRecovery() throws Exception
   {
      resourceAdapter = newResourceAdapter();
      MyBootstrapContext ctx = new MyBootstrapContext();
      resourceAdapter.start(ctx);
      ActiveMQRAManagedConnectionFactory mcf = new ActiveMQRAManagedConnectionFactory();
      mcf.setResourceAdapter(resourceAdapter);
      ActiveMQRAConnectionFactory qraConnectionFactory = new ActiveMQRAConnectionFactoryImpl(mcf, qraConnectionManager);
      QueueConnection queueConnection = qraConnectionFactory.createQueueConnection();
      QueueSession session = queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);

      assertNotNull(mcf.getResourceRecovery());
   }

   @Test
   public void testJMSContext() throws Exception
   {
      resourceAdapter = newResourceAdapter();
      MyBootstrapContext ctx = new MyBootstrapContext();
      resourceAdapter.start(ctx);
      ActiveMQRAManagedConnectionFactory mcf = new ActiveMQRAManagedConnectionFactory();
      mcf.setResourceAdapter(resourceAdapter);
      ActiveMQRAConnectionFactory qraConnectionFactory = new ActiveMQRAConnectionFactoryImpl(mcf, qraConnectionManager);

      JMSContext jmsctx = qraConnectionFactory.createContext(JMSContext.DUPS_OK_ACKNOWLEDGE);
      assertEquals(JMSContext.DUPS_OK_ACKNOWLEDGE, jmsctx.getSessionMode());

   }

   @Test
   public void testOutgoingXAResourceWrapper() throws Exception
   {
      XAQueueConnection queueConnection = qraConnectionFactory.createXAQueueConnection();
      XASession s = queueConnection.createXASession();

      XAResource resource = s.getXAResource();
      assertTrue(resource instanceof ActiveMQXAResourceWrapper);

      ActiveMQXAResourceWrapperImpl xaResourceWrapper  = (ActiveMQXAResourceWrapperImpl) resource;
      assertTrue(xaResourceWrapper.getJndiName().equals("java://jmsXA NodeId:" + server.getNodeID()));
      assertTrue(xaResourceWrapper.getProductVersion().equals(VersionLoader.getVersion().getFullVersion()));
      assertTrue(xaResourceWrapper.getProductName().equals(ActiveMQResourceAdapter.PRODUCT_NAME));
   }
}
