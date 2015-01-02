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

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.Session;
import javax.jms.TextMessage;

import java.util.HashSet;
import java.util.Set;

import org.apache.activemq.api.jms.ActiveMQJMSClient;
import org.apache.activemq.core.remoting.impl.invm.InVMConnectorFactory;
import org.apache.activemq.core.security.Role;
import org.apache.activemq.ra.ActiveMQRAConnectionFactory;
import org.apache.activemq.ra.ActiveMQRAConnectionFactoryImpl;
import org.apache.activemq.ra.ActiveMQRAConnectionManager;
import org.apache.activemq.ra.ActiveMQRAManagedConnectionFactory;
import org.apache.activemq.ra.ActiveMQResourceAdapter;
import org.apache.activemq.spi.core.security.ActiveMQSecurityManagerImpl;
import org.apache.activemq.tests.integration.jms.bridge.TransactionManagerLocatorImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author mtaylor
 */

public class OutgoingConnectionTestJTA extends ActiveMQRATestBase
{
   protected ActiveMQResourceAdapter resourceAdapter;
   protected ActiveMQRAConnectionFactory qraConnectionFactory;
   protected ActiveMQRAManagedConnectionFactory mcf;
   ActiveMQRAConnectionManager qraConnectionManager = new ActiveMQRAConnectionManager();

   static
   {
      DummyTransactionManager dummyTransactionManager = new DummyTransactionManager();
      TransactionManagerLocatorImpl.tm = dummyTransactionManager;
   }

   @Override
   public boolean useSecurity()
   {
      return true;
   }

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      ((ActiveMQSecurityManagerImpl)server.getSecurityManager()).getConfiguration().addUser("testuser", "testpassword");
      ((ActiveMQSecurityManagerImpl)server.getSecurityManager()).getConfiguration().addUser("guest", "guest");
      ((ActiveMQSecurityManagerImpl)server.getSecurityManager()).getConfiguration().setDefaultUser("guest");
      ((ActiveMQSecurityManagerImpl)server.getSecurityManager()).getConfiguration().addRole("testuser", "arole");
      ((ActiveMQSecurityManagerImpl)server.getSecurityManager()).getConfiguration().addRole("guest", "arole");
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
      ((DummyTransactionManager) TransactionManagerLocatorImpl.tm).tx = null;
      if (resourceAdapter != null)
      {
         resourceAdapter.stop();
      }

      qraConnectionManager.stop();
      super.tearDown();
   }

   @Test
   public void testSimpleMessageSendAndReceiveTransacted() throws Exception
   {
      setDummyTX();
      setupDLQ(10);
      resourceAdapter = newResourceAdapter();
      MyBootstrapContext ctx = new MyBootstrapContext();
      resourceAdapter.start(ctx);
      ActiveMQRAManagedConnectionFactory mcf = new ActiveMQRAManagedConnectionFactory();
      mcf.setResourceAdapter(resourceAdapter);
      ActiveMQRAConnectionFactory qraConnectionFactory = new ActiveMQRAConnectionFactoryImpl(mcf, qraConnectionManager);
      QueueConnection queueConnection = qraConnectionFactory.createQueueConnection();
      Session s = queueConnection.createSession(true, Session.AUTO_ACKNOWLEDGE);
      Queue q = ActiveMQJMSClient.createQueue(MDBQUEUE);
      MessageProducer mp = s.createProducer(q);
      MessageConsumer consumer = s.createConsumer(q);
      Message message = s.createTextMessage("test");
      mp.send(message);
      s.commit();
      queueConnection.start();
      TextMessage textMessage = (TextMessage) consumer.receive(1000);
      assertNotNull(textMessage);
      assertEquals(textMessage.getText(), "test");
      s.rollback();
      textMessage = (TextMessage) consumer.receive(1000);
      assertNotNull(textMessage);
      assertEquals(textMessage.getText(), "test");
      s.commit();
   }

   public void testQueuSessionAckMode(boolean inTx) throws Exception
   {
      if (inTx)
      {
         setDummyTX();
      }
      QueueConnection queueConnection = qraConnectionFactory.createQueueConnection();

      Session s = queueConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      if (inTx)
      {
         assertEquals(Session.SESSION_TRANSACTED, s.getAcknowledgeMode());
      }
      else
      {
         assertEquals(Session.AUTO_ACKNOWLEDGE, s.getAcknowledgeMode());
      }
      s.close();

      s = queueConnection.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
      if (inTx)
      {
         assertEquals(Session.SESSION_TRANSACTED, s.getAcknowledgeMode());
      }
      else
      {
         assertEquals(Session.DUPS_OK_ACKNOWLEDGE, s.getAcknowledgeMode());
      }
      s.close();

      //exception should be thrown if ack mode is SESSION_TRANSACTED or
      //CLIENT_ACKNOWLEDGE when in a JTA else ackmode should bee ignored
      try
      {
         s = queueConnection.createSession(false, Session.SESSION_TRANSACTED);
         if (inTx)
         {
            assertEquals(s.getAcknowledgeMode(), Session.SESSION_TRANSACTED);
         }
         else
         {
            fail("didn't get expected exception creating session with SESSION_TRANSACTED mode ");
         }
         s.close();
      }
      catch (JMSException e)
      {
         if (inTx)
         {
            fail("shouldn't throw exception " + e);
         }
      }

      try
      {
         s = queueConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
         if (inTx)
         {
            assertEquals(s.getAcknowledgeMode(), Session.SESSION_TRANSACTED);
         }
         else
         {
            fail("didn't get expected exception creating session with CLIENT_ACKNOWLEDGE mode");
         }
      }
      catch (JMSException e)
      {
         if (inTx)
         {
            fail("shouldn't throw exception " + e);
         }
      }

   }

   @Test
   public void testQueueSessionAckModeJTA() throws Exception
   {
      testQueuSessionAckMode(true);
   }

   @Test
   public void testSessionAckModeNoJTA() throws Exception
   {
      testQueuSessionAckMode(false);
   }

   private void setDummyTX()
   {
      ((DummyTransactionManager) TransactionManagerLocatorImpl.tm).tx = new DummyTransaction();
   }
}
