/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.activemq.tests.integration.ra;

import javax.jms.JMSContext;
import javax.jms.JMSRuntimeException;
import javax.transaction.TransactionManager;
import java.util.HashSet;
import java.util.Set;

import org.apache.activemq.core.remoting.impl.invm.InVMConnectorFactory;
import org.apache.activemq.core.security.Role;
import org.apache.activemq.ra.HornetQRAConnectionFactory;
import org.apache.activemq.ra.HornetQRAConnectionFactoryImpl;
import org.apache.activemq.ra.HornetQRAConnectionManager;
import org.apache.activemq.ra.HornetQRAManagedConnectionFactory;
import org.apache.activemq.ra.HornetQResourceAdapter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class JMSContextTest extends HornetQRATestBase
{
   private HornetQResourceAdapter resourceAdapter;

   HornetQRAConnectionManager qraConnectionManager = new HornetQRAConnectionManager();
   private HornetQRAConnectionFactory qraConnectionFactory;

   public TransactionManager getTm()
   {
      return DummyTransactionManager.tm;
   }

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
      resourceAdapter = new HornetQResourceAdapter();
      resourceAdapter.setTransactionManagerLocatorClass(JMSContextTest.class.getName());
      resourceAdapter.setTransactionManagerLocatorMethod("getTm");

      resourceAdapter.setConnectorClassName(InVMConnectorFactory.class.getName());
      MyBootstrapContext ctx = new MyBootstrapContext();
      resourceAdapter.start(ctx);
      HornetQRAManagedConnectionFactory mcf = new HornetQRAManagedConnectionFactory();
      mcf.setResourceAdapter(resourceAdapter);
      qraConnectionFactory = new HornetQRAConnectionFactoryImpl(mcf, qraConnectionManager);
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
   public void testCreateContextThrowsException() throws Exception
   {
      JMSContext jmsctx = qraConnectionFactory.createContext();
      try
      {
         jmsctx.createContext(JMSContext.AUTO_ACKNOWLEDGE);
         fail("expected JMSRuntimeException");
      }
      catch (JMSRuntimeException e)
      {
         //pass
      }
      catch (Exception e)
      {
         fail("wrong exception thrown: " + e);
      }
   }

   @Test
   public void testCreateXAContextThrowsException() throws Exception
   {
      JMSContext jmsctx = qraConnectionFactory.createXAContext();
      try
      {
         jmsctx.createContext(JMSContext.AUTO_ACKNOWLEDGE);
         fail("expected JMSRuntimeException");
      }
      catch (JMSRuntimeException e)
      {
         //pass
      }
      catch (Exception e)
      {
         fail("wrong exception thrown: " + e);
      }
   }

   @Test
   public void sessionTransactedTestActiveJTATx() throws Exception
   {
      try
      {
         qraConnectionFactory.createContext(JMSContext.SESSION_TRANSACTED);
         fail();
      }
      catch (JMSRuntimeException e)
      {
         //pass
      }
   }

   @Test
   public void sessionTransactedTestNoActiveJTATx() throws Exception
   {
      DummyTransactionManager.tm.tx = new DummyTransaction();
      JMSContext context = qraConnectionFactory.createContext(JMSContext.SESSION_TRANSACTED);
      assertEquals(context.getSessionMode(), JMSContext.AUTO_ACKNOWLEDGE);
   }

   @Test
   public void clientAckTestActiveJTATx() throws Exception
   {
      try
      {
         qraConnectionFactory.createContext(JMSContext.CLIENT_ACKNOWLEDGE);
         fail();
      }
      catch (JMSRuntimeException e)
      {
         //pass
      }
   }

   @Test
   public void clientAckTestNoActiveJTATx() throws Exception
   {
      DummyTransactionManager.tm.tx = new DummyTransaction();
      JMSContext context = qraConnectionFactory.createContext(JMSContext.CLIENT_ACKNOWLEDGE);
      assertEquals(context.getSessionMode(), JMSContext.AUTO_ACKNOWLEDGE);
   }

}
