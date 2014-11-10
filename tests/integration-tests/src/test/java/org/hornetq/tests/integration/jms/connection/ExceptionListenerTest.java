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
package org.hornetq.tests.integration.jms.connection;
import org.junit.Before;
import org.junit.After;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Session;

import org.junit.Assert;

import org.hornetq.api.core.HornetQInternalErrorException;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.api.jms.JMSFactoryType;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.jms.client.HornetQConnection;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.client.HornetQSession;
import org.hornetq.jms.server.impl.JMSServerManagerImpl;
import org.hornetq.tests.integration.jms.server.management.NullInitialContext;
import org.hornetq.tests.util.UnitTestCase;

/**
 *
 * A ExceptionListenerTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class ExceptionListenerTest extends UnitTestCase
{
   private HornetQServer server;

   private JMSServerManagerImpl jmsServer;

   private HornetQConnectionFactory cf;

   private static final String Q_NAME = "ConnectionTestQueue";

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      Configuration conf = createBasicConfig()
         .addAcceptorConfiguration(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory"));
      server = addServer(HornetQServers.newHornetQServer(conf, false));
      jmsServer = new JMSServerManagerImpl(server);
      jmsServer.setContext(new NullInitialContext());
      jmsServer.start();
      jmsServer.createQueue(false, ExceptionListenerTest.Q_NAME, null, true, ExceptionListenerTest.Q_NAME);
      cf = HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"));
      cf.setBlockOnDurableSend(true);
      cf.setPreAcknowledge(true);
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      jmsServer.stop();
      cf = null;

      server = null;
      jmsServer = null;
      cf = null;

      super.tearDown();
   }

   private class MyExceptionListener implements ExceptionListener
   {
      volatile int numCalls;

      private final CountDownLatch latch;

      public MyExceptionListener(final CountDownLatch latch)
      {
         this.latch = latch;
      }

      public synchronized void onException(final JMSException arg0)
      {
         numCalls++;
         latch.countDown();
      }
   }

   @Test
   public void testListenerCalledForOneConnection() throws Exception
   {
      Connection conn = cf.createConnection();
      CountDownLatch latch = new CountDownLatch(1);
      MyExceptionListener listener = new MyExceptionListener(latch);

      conn.setExceptionListener(listener);

      ClientSessionInternal coreSession = (ClientSessionInternal)((HornetQConnection)conn).getInitialSession();

      coreSession.getConnection().fail(new HornetQInternalErrorException("blah"));

      latch.await(5, TimeUnit.SECONDS);

      Assert.assertEquals(1, listener.numCalls);

      conn.close();
   }

   @Test
   public void testListenerCalledForOneConnectionAndSessions() throws Exception
   {
      Connection conn = cf.createConnection();

      CountDownLatch latch = new CountDownLatch(1);
      MyExceptionListener listener = new MyExceptionListener(latch);

      conn.setExceptionListener(listener);

      Session sess1 = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      Session sess2 = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      Session sess3 = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      ClientSessionInternal coreSession0 = (ClientSessionInternal)((HornetQConnection)conn).getInitialSession();

      ClientSessionInternal coreSession1 = (ClientSessionInternal)((HornetQSession)sess1).getCoreSession();

      ClientSessionInternal coreSession2 = (ClientSessionInternal)((HornetQSession)sess2).getCoreSession();

      ClientSessionInternal coreSession3 = (ClientSessionInternal)((HornetQSession)sess3).getCoreSession();

      coreSession0.getConnection().fail(new HornetQInternalErrorException("blah"));

      coreSession1.getConnection().fail(new HornetQInternalErrorException("blah"));

      coreSession2.getConnection().fail(new HornetQInternalErrorException("blah"));

      coreSession3.getConnection().fail(new HornetQInternalErrorException("blah"));

      latch.await(5, TimeUnit.SECONDS);
      // Listener should only be called once even if all sessions connections die
      Assert.assertEquals(1, listener.numCalls);

      conn.close();
   }

}
