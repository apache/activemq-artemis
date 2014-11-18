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
package org.apache.activemq.tests.integration.client;
import org.junit.Before;
import org.junit.After;

import org.junit.Test;

import java.util.HashMap;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.junit.Assert;

import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.api.core.client.ClientConsumer;
import org.apache.activemq.api.core.client.ClientMessage;
import org.apache.activemq.api.core.client.ClientProducer;
import org.apache.activemq.api.core.client.ClientSession;
import org.apache.activemq.api.core.client.ClientSessionFactory;
import org.apache.activemq.api.core.client.ServerLocator;
import org.apache.activemq.api.core.management.ActiveMQServerControl;
import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.core.server.ActiveMQServer;
import org.apache.activemq.core.server.Queue;
import org.apache.activemq.core.settings.impl.AddressSettings;
import org.apache.activemq.core.transaction.impl.XidImpl;
import org.apache.activemq.tests.integration.management.ManagementControlHelper;
import org.apache.activemq.tests.util.ServiceTestBase;

/**
 * A HeuristicXATest
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class HeuristicXATest extends ServiceTestBase
{
   // Constants -----------------------------------------------------
   final SimpleString ADDRESS = new SimpleString("ADDRESS");

   final String body = "this is the body";

   // Attributes ----------------------------------------------------

   private MBeanServer mbeanServer;

   private ServerLocator locator;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testInvalidCall() throws Exception
   {
      Configuration configuration = createDefaultConfig()
         .setJMXManagementEnabled(true);

      ActiveMQServer server = createServer(false, configuration, mbeanServer, new HashMap<String, AddressSettings>());
      server.start();

      ActiveMQServerControl jmxServer = ManagementControlHelper.createActiveMQServerControl(mbeanServer);

      Assert.assertFalse(jmxServer.commitPreparedTransaction("Nananananana"));
   }

   @Test
   public void testHeuristicCommit() throws Exception
   {
      internalTest(true);
   }

   @Test
   public void testHeuristicRollback() throws Exception
   {
      internalTest(false);
   }

   private void internalTest(final boolean isCommit) throws Exception
   {
      Configuration configuration = createDefaultConfig()
         .setJMXManagementEnabled(true);

      ActiveMQServer server = createServer(false, configuration, mbeanServer, new HashMap<String, AddressSettings>());
      server.start();
      Xid xid = newXID();

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(true, false, false);

      session.createQueue(ADDRESS, ADDRESS, true);

      session.start(xid, XAResource.TMNOFLAGS);

      ClientProducer producer = session.createProducer(ADDRESS);

      ClientMessage msg = session.createMessage(true);

      msg.getBodyBuffer().writeString(body);

      producer.send(msg);

      session.end(xid, XAResource.TMSUCCESS);

      session.prepare(xid);

      session.close();

      ActiveMQServerControl jmxServer = ManagementControlHelper.createActiveMQServerControl(mbeanServer);

      String[] preparedTransactions = jmxServer.listPreparedTransactions();

      Assert.assertEquals(1, preparedTransactions.length);

      System.out.println(preparedTransactions[0]);

      Assert.assertEquals(0, jmxServer.listHeuristicCommittedTransactions().length);
      Assert.assertEquals(0, jmxServer.listHeuristicRolledBackTransactions().length);

      if (isCommit)
      {
         jmxServer.commitPreparedTransaction(XidImpl.toBase64String(xid));
      }
      else
      {
         jmxServer.rollbackPreparedTransaction(XidImpl.toBase64String(xid));
      }

      Assert.assertEquals(0, jmxServer.listPreparedTransactions().length);
      if (isCommit)
      {
         Assert.assertEquals(1, jmxServer.listHeuristicCommittedTransactions().length);
         Assert.assertEquals(0, jmxServer.listHeuristicRolledBackTransactions().length);
      }
      else
      {
         Assert.assertEquals(0, jmxServer.listHeuristicCommittedTransactions().length);
         Assert.assertEquals(1, jmxServer.listHeuristicRolledBackTransactions().length);
      }

      if (isCommit)
      {
         Assert.assertEquals(1, getMessageCount(((Queue)server.getPostOffice().getBinding(ADDRESS).getBindable())));

         session = sf.createSession(false, false, false);

         session.start();
         ClientConsumer consumer = session.createConsumer(ADDRESS);
         msg = consumer.receive(1000);
         Assert.assertNotNull(msg);
         msg.acknowledge();
         Assert.assertEquals(body, msg.getBodyBuffer().readString());

         session.commit();
         session.close();
      }

      Assert.assertEquals(0, getMessageCount(((Queue)server.getPostOffice().getBinding(ADDRESS).getBindable())));
   }

   @Test
   public void testHeuristicCommitWithRestart() throws Exception
   {
      doHeuristicCompletionWithRestart(true);
   }

   @Test
   public void testHeuristicRollbackWithRestart() throws Exception
   {
      doHeuristicCompletionWithRestart(false);
   }

   private void doHeuristicCompletionWithRestart(final boolean isCommit) throws Exception
   {
      Configuration configuration = createDefaultConfig()
         .setJMXManagementEnabled(true);

      ActiveMQServer server = createServer(true, configuration, mbeanServer, new HashMap<String, AddressSettings>());
      server.start();
      Xid xid = newXID();

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(true, false, false);

      session.createQueue(ADDRESS, ADDRESS, true);

      session.start(xid, XAResource.TMNOFLAGS);

      ClientProducer producer = session.createProducer(ADDRESS);

      ClientMessage msg = session.createMessage(true);

      msg.getBodyBuffer().writeString(body);

      producer.send(msg);

      session.end(xid, XAResource.TMSUCCESS);

      session.prepare(xid);

      session.close();

      ActiveMQServerControl jmxServer = ManagementControlHelper.createActiveMQServerControl(mbeanServer);

      String[] preparedTransactions = jmxServer.listPreparedTransactions();

      Assert.assertEquals(1, preparedTransactions.length);
      System.out.println(preparedTransactions[0]);

      if (isCommit)
      {
         jmxServer.commitPreparedTransaction(XidImpl.toBase64String(xid));
      }
      else
      {
         jmxServer.rollbackPreparedTransaction(XidImpl.toBase64String(xid));
      }

      preparedTransactions = jmxServer.listPreparedTransactions();
      Assert.assertEquals(0, preparedTransactions.length);

      if (isCommit)
      {
         Assert.assertEquals(1, getMessageCount(((Queue)server.getPostOffice().getBinding(ADDRESS).getBindable())));

         session = sf.createSession(false, false, false);

         session.start();
         ClientConsumer consumer = session.createConsumer(ADDRESS);
         msg = consumer.receive(1000);
         Assert.assertNotNull(msg);
         msg.acknowledge();
         Assert.assertEquals(body, msg.getBodyBuffer().readString());

         session.commit();
         session.close();
      }

      Assert.assertEquals(0, getMessageCount(((Queue)server.getPostOffice().getBinding(ADDRESS).getBindable())));

      server.stop();

      server.start();

      jmxServer = ManagementControlHelper.createActiveMQServerControl(mbeanServer);
      if (isCommit)
      {
         String[] listHeuristicCommittedTransactions = jmxServer.listHeuristicCommittedTransactions();
         Assert.assertEquals(1, listHeuristicCommittedTransactions.length);
         System.out.println(listHeuristicCommittedTransactions[0]);
      }
      else
      {
         String[] listHeuristicRolledBackTransactions = jmxServer.listHeuristicRolledBackTransactions();
         Assert.assertEquals(1, listHeuristicRolledBackTransactions.length);
         System.out.println(listHeuristicRolledBackTransactions[0]);
      }
   }

   @Test
   public void testRecoverHeuristicCommitWithRestart() throws Exception
   {
      doRecoverHeuristicCompletedTxWithRestart(true);
   }

   @Test
   public void testRecoverHeuristicRollbackWithRestart() throws Exception
   {
      doRecoverHeuristicCompletedTxWithRestart(false);
   }

   private void doRecoverHeuristicCompletedTxWithRestart(final boolean heuristicCommit) throws Exception
   {
      Configuration configuration = createDefaultConfig()
         .setJMXManagementEnabled(true);

      ActiveMQServer server = createServer(true, configuration, mbeanServer, new HashMap<String, AddressSettings>());
      server.start();
      Xid xid = newXID();

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(true, false, false);

      session.createQueue(ADDRESS, ADDRESS, true);

      session.start(xid, XAResource.TMNOFLAGS);

      ClientProducer producer = session.createProducer(ADDRESS);

      ClientMessage msg = session.createMessage(true);

      msg.getBodyBuffer().writeString(body);

      producer.send(msg);

      session.end(xid, XAResource.TMSUCCESS);

      session.prepare(xid);

      session.close();

      ActiveMQServerControl jmxServer = ManagementControlHelper.createActiveMQServerControl(mbeanServer);

      String[] preparedTransactions = jmxServer.listPreparedTransactions();

      Assert.assertEquals(1, preparedTransactions.length);
      System.out.println(preparedTransactions[0]);

      if (heuristicCommit)
      {
         jmxServer.commitPreparedTransaction(XidImpl.toBase64String(xid));
      }
      else
      {
         jmxServer.rollbackPreparedTransaction(XidImpl.toBase64String(xid));
      }

      preparedTransactions = jmxServer.listPreparedTransactions();
      Assert.assertEquals(0, preparedTransactions.length);

      if (heuristicCommit)
      {
         Assert.assertEquals(1, getMessageCount(((Queue)server.getPostOffice().getBinding(ADDRESS).getBindable())));

         session = sf.createSession(false, false, false);

         session.start();
         ClientConsumer consumer = session.createConsumer(ADDRESS);
         msg = consumer.receive(1000);
         Assert.assertNotNull(msg);
         msg.acknowledge();
         Assert.assertEquals(body, msg.getBodyBuffer().readString());

         session.commit();
         session.close();
      }

      Assert.assertEquals(0, getMessageCount(((Queue)server.getPostOffice().getBinding(ADDRESS).getBindable())));

      server.stop();

      server.start();
      // we need to recreate the locator and session factory
      sf = createSessionFactory(locator);
      jmxServer = ManagementControlHelper.createActiveMQServerControl(mbeanServer);
      if (heuristicCommit)
      {
         String[] listHeuristicCommittedTransactions = jmxServer.listHeuristicCommittedTransactions();
         Assert.assertEquals(1, listHeuristicCommittedTransactions.length);
         System.out.println(listHeuristicCommittedTransactions[0]);
      }
      else
      {
         String[] listHeuristicRolledBackTransactions = jmxServer.listHeuristicRolledBackTransactions();
         Assert.assertEquals(1, listHeuristicRolledBackTransactions.length);
         System.out.println(listHeuristicRolledBackTransactions[0]);
      }

      session = sf.createSession(true, false, false);
      Xid[] recoveredXids = session.recover(XAResource.TMSTARTRSCAN);
      Assert.assertEquals(1, recoveredXids.length);
      Assert.assertEquals(xid, recoveredXids[0]);
      Assert.assertEquals(0, session.recover(XAResource.TMENDRSCAN).length);

      session.close();
   }

   @Test
   public void testForgetHeuristicCommitAndRestart() throws Exception
   {
      doForgetHeuristicCompletedTxAndRestart(true);
   }

   @Test
   public void testForgetHeuristicRollbackAndRestart() throws Exception
   {
      doForgetHeuristicCompletedTxAndRestart(false);
   }

   private void doForgetHeuristicCompletedTxAndRestart(final boolean heuristicCommit) throws Exception
   {
      Configuration configuration = createDefaultConfig()
         .setJMXManagementEnabled(true);

      ActiveMQServer server = createServer(true, configuration, mbeanServer, new HashMap<String, AddressSettings>());
      server.start();
      Xid xid = newXID();

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(true, false, false);

      session.createQueue(ADDRESS, ADDRESS, true);

      session.start(xid, XAResource.TMNOFLAGS);

      ClientProducer producer = session.createProducer(ADDRESS);

      ClientMessage msg = session.createMessage(true);

      msg.getBodyBuffer().writeBytes(new byte[123]);

      producer.send(msg);

      session.end(xid, XAResource.TMSUCCESS);

      session.prepare(xid);

      ActiveMQServerControl jmxServer = ManagementControlHelper.createActiveMQServerControl(mbeanServer);

      String[] preparedTransactions = jmxServer.listPreparedTransactions();

      Assert.assertEquals(1, preparedTransactions.length);
      System.out.println(preparedTransactions[0]);

      if (heuristicCommit)
      {
         jmxServer.commitPreparedTransaction(XidImpl.toBase64String(xid));
      }
      else
      {
         jmxServer.rollbackPreparedTransaction(XidImpl.toBase64String(xid));
      }

      preparedTransactions = jmxServer.listPreparedTransactions();
      Assert.assertEquals(0, preparedTransactions.length);

      session.forget(xid);

      session.close();

      if (heuristicCommit)
      {
         Assert.assertEquals(0, jmxServer.listHeuristicCommittedTransactions().length);
      }
      else
      {
         Assert.assertEquals(0, jmxServer.listHeuristicRolledBackTransactions().length);
      }

      server.stop();

      server.start();
      // we need to recreate the sf
      sf = createSessionFactory(locator);
      session = sf.createSession(true, false, false);
      Xid[] recoveredXids = session.recover(XAResource.TMSTARTRSCAN);
      Assert.assertEquals(0, recoveredXids.length);
      jmxServer = ManagementControlHelper.createActiveMQServerControl(mbeanServer);
      if (heuristicCommit)
      {
         Assert.assertEquals(0, jmxServer.listHeuristicCommittedTransactions().length);
      }
      else
      {
         Assert.assertEquals(0, jmxServer.listHeuristicRolledBackTransactions().length);
      }

      session.close();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   @After
   public void tearDown() throws Exception
   {
      locator.close();
      MBeanServerFactory.releaseMBeanServer(mbeanServer);
      super.tearDown();
   }

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      mbeanServer = MBeanServerFactory.createMBeanServer();
      locator = createInVMNonHALocator();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
