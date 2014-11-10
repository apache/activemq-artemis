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
package org.hornetq.tests.integration.client;
import org.junit.Before;
import org.junit.After;

import org.junit.Test;

import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Session;

import org.hornetq.api.core.HornetQNotConnectedException;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.api.jms.JMSFactoryType;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.client.HornetQSession;
import org.hornetq.jms.server.impl.JMSServerManagerImpl;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.tests.integration.IntegrationTestLogger;
import org.hornetq.tests.integration.jms.server.management.NullInitialContext;
import org.hornetq.tests.util.ServiceTestBase;

/**
 *
 * A FailureDeadlockTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class FailureDeadlockTest extends ServiceTestBase
{
   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   private HornetQServer server;

   private JMSServerManagerImpl jmsServer;

   private HornetQConnectionFactory cf1;

   private HornetQConnectionFactory cf2;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      Configuration conf = createDefaultConfig()
         .setSecurityEnabled(false)
         .addAcceptorConfiguration(new TransportConfiguration(INVM_ACCEPTOR_FACTORY));
      server = createServer(false, conf);
      jmsServer = new JMSServerManagerImpl(server);
      jmsServer.setContext(new NullInitialContext());
      jmsServer.start();

      cf1 =
               HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF,
                                                                 new TransportConfiguration(INVM_CONNECTOR_FACTORY));
      cf2 =
               HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF,
                                                                 new TransportConfiguration(INVM_CONNECTOR_FACTORY));
   }

   @Override
   @After
   public void tearDown() throws Exception
   {

      cf1.close();

      cf2.close();

      if (jmsServer != null && jmsServer.isStarted())
      {
         jmsServer.stop();
      }

      super.tearDown();
   }

   // https://jira.jboss.org/jira/browse/JBMESSAGING-1702
   // Test that two failures concurrently executing and calling the same exception listener
   // don't deadlock
   @Test
   public void testDeadlock() throws Exception
   {
      for (int i = 0; i < 100; i++)
      {
         final Connection conn1 = cf1.createConnection();

         Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         RemotingConnection rc1 = ((ClientSessionInternal)((HornetQSession)sess1).getCoreSession()).getConnection();

         final Connection conn2 = cf2.createConnection();

         Session sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         RemotingConnection rc2 = ((ClientSessionInternal)((HornetQSession)sess2).getCoreSession()).getConnection();

         ExceptionListener listener1 = new ExceptionListener()
         {
            public void onException(final JMSException exception)
            {
               try
               {
                  conn2.close();
               }
               catch (Exception e)
               {
                  FailureDeadlockTest.log.error("Failed to close connection2", e);
               }
            }
         };

         conn1.setExceptionListener(listener1);

         conn2.setExceptionListener(listener1);

         Failer f1 = new Failer(rc1);

         Failer f2 = new Failer(rc2);

         f1.start();

         f2.start();

         f1.join();

         f2.join();

         conn1.close();

         conn2.close();
      }
   }

   private class Failer extends Thread
   {
      RemotingConnection conn;

      Failer(final RemotingConnection conn)
      {
         this.conn = conn;
      }

      @Override
      public void run()
      {
         conn.fail(new HornetQNotConnectedException("blah"));
      }
   }

   // https://jira.jboss.org/jira/browse/JBMESSAGING-1703
   // Make sure that failing a connection removes it from the connection manager and can't be returned in a subsequent
   // call
   @Test
   public void testUsingDeadConnection() throws Exception
   {
      for (int i = 0; i < 100; i++)
      {
         final Connection conn1 = cf1.createConnection();

         Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         RemotingConnection rc1 = ((ClientSessionInternal)((HornetQSession)sess1).getCoreSession()).getConnection();

         rc1.fail(new HornetQNotConnectedException( "blah"));

         try
         {
            conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
            fail("should throw exception");
         }
         catch (JMSException e)
         {
            //pass
         }

         conn1.close();
      }
   }

}
