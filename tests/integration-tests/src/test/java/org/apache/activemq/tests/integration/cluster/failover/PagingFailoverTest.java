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
package org.apache.activemq.tests.integration.cluster.failover;

import java.util.HashMap;

import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.api.core.client.ClientConsumer;
import org.apache.activemq.api.core.client.ClientMessage;
import org.apache.activemq.api.core.client.ClientProducer;
import org.apache.activemq.api.core.client.ClientSession;
import org.apache.activemq.api.core.client.ServerLocator;
import org.apache.activemq.core.client.impl.ClientSessionFactoryInternal;
import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.core.server.HornetQServer;
import org.apache.activemq.core.server.Queue;
import org.apache.activemq.core.settings.impl.AddressSettings;
import org.apache.activemq.tests.integration.cluster.util.SameProcessHornetQServer;
import org.apache.activemq.tests.integration.cluster.util.TestableServer;
import org.apache.activemq.tests.util.TransportConfigurationUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * A PagingFailoverTest
 * <p/>
 * TODO: validate replication failover also
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class PagingFailoverTest extends FailoverTestBase
{
   // Constants -----------------------------------------------------

   private static final SimpleString ADDRESS = new SimpleString("SimpleAddress");

   private ServerLocator locator;

   private ClientSession session;

   private ClientSessionFactoryInternal sf;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      locator = getServerLocator();
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      addClientSession(session);
      super.tearDown();
   }

   @Test
   public void testPageFailBeforeConsume() throws Exception
   {
      internalTestPage(false, true);
   }


   @Test
   public void testPage() throws Exception
   {
      internalTestPage(false, false);
   }

   @Test
   public void testPageTransactioned() throws Exception
   {
      internalTestPage(true, false);
   }

   @Test
   public void testPageTransactionedFailBeforeConsume() throws Exception
   {
      internalTestPage(true, true);
   }

   public void internalTestPage(final boolean transacted, final boolean failBeforeConsume) throws Exception
   {
      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);
      locator.setReconnectAttempts(-1);

      sf = createSessionFactoryAndWaitForTopology(locator, 2);
      session = sf.createSession(!transacted, !transacted, 0);

      session.createQueue(PagingFailoverTest.ADDRESS, PagingFailoverTest.ADDRESS, true);

      ClientProducer prod = session.createProducer(PagingFailoverTest.ADDRESS);

      final int TOTAL_MESSAGES = 2000;

      for (int i = 0; i < TOTAL_MESSAGES; i++)
      {
         if (transacted && i % 10 == 0)
         {
            session.commit();
         }
         ClientMessage msg = session.createMessage(true);
         msg.putIntProperty(new SimpleString("key"), i);
         prod.send(msg);
      }

      session.commit();

      if (failBeforeConsume)
      {
         crash(session);
         waitForBackup(null, 5);
      }

      session.close();

      session = sf.createSession(!transacted, !transacted, 0);

      session.start();

      ClientConsumer cons = session.createConsumer(PagingFailoverTest.ADDRESS);

      final int MIDDLE = TOTAL_MESSAGES / 2;

      for (int i = 0; i < MIDDLE; i++)
      {
         ClientMessage msg = cons.receive(20000);
         Assert.assertNotNull(msg);
         msg.acknowledge();
         if (transacted && i % 10 == 0)
         {
            session.commit();
         }
         Assert.assertEquals(i, msg.getObjectProperty(new SimpleString("key")));
      }

      session.commit();

      cons.close();

      Thread.sleep(1000);

      if (!failBeforeConsume)
      {
         crash(session);
         // failSession(session, latch);
      }

      session.close();

      session = sf.createSession(true, true, 0);

      cons = session.createConsumer(PagingFailoverTest.ADDRESS);

      session.start();

      for (int i = MIDDLE; i < TOTAL_MESSAGES; i++)
      {
         ClientMessage msg = cons.receive(5000);
         Assert.assertNotNull(msg);

         msg.acknowledge();
         int result = (Integer) msg.getObjectProperty(new SimpleString("key"));
         Assert.assertEquals(i, result);
      }
   }

   @Test
   public void testExpireMessage() throws Exception
   {
      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);
      locator.setReconnectAttempts(-1);

      ClientSessionFactoryInternal sf = createSessionFactoryAndWaitForTopology(locator, 2);
      session = sf.createSession(true, true, 0);

      session.createQueue(PagingFailoverTest.ADDRESS, PagingFailoverTest.ADDRESS, true);

      ClientProducer prod = session.createProducer(PagingFailoverTest.ADDRESS);

      final int TOTAL_MESSAGES = 1000;

      for (int i = 0; i < TOTAL_MESSAGES; i++)
      {
         ClientMessage msg = session.createMessage(true);
         msg.putIntProperty(new SimpleString("key"), i);
         msg.setExpiration(System.currentTimeMillis() + 1000);
         prod.send(msg);
      }

      crash(session);

      session.close();

      Queue queue = backupServer.getServer().locateQueue(ADDRESS);

      long timeout = System.currentTimeMillis() + 60000;

      while (timeout > System.currentTimeMillis() && queue.getPageSubscription().isPaging())
      {
         Thread.sleep(100);
         // Simulating what would happen on expire
         queue.expireReferences();
      }

      assertFalse(queue.getPageSubscription().isPaging());

   }


   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected TransportConfiguration getAcceptorTransportConfiguration(final boolean live)
   {
      return TransportConfigurationUtils.getInVMAcceptor(live);
   }

   @Override
   protected TransportConfiguration getConnectorTransportConfiguration(final boolean live)
   {
      return TransportConfigurationUtils.getInVMConnector(live);
   }

   @Override
   protected HornetQServer createServer(final boolean realFiles, final Configuration configuration)
   {
      return addServer(createInVMFailoverServer(true, configuration, PAGE_SIZE, PAGE_MAX,
                                                new HashMap<String, AddressSettings>(), nodeManager, 2));
   }

   @Override
   protected TestableServer createTestableServer(Configuration config)
   {
      return new SameProcessHornetQServer(createServer(true, config));
   }
}
