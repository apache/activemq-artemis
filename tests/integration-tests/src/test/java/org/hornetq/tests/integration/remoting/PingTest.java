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
package org.hornetq.tests.integration.remoting;

import org.hornetq.core.protocol.core.impl.wireformat.Ping;
import org.junit.Before;

import org.junit.Test;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Interceptor;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.api.core.client.SessionFailureListener;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.protocol.core.CoreRemotingConnection;
import org.hornetq.core.protocol.core.Packet;
import org.hornetq.core.protocol.core.impl.PacketImpl;
import org.hornetq.core.remoting.CloseListener;
import org.hornetq.core.remoting.server.impl.RemotingServiceImpl;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.tests.integration.IntegrationTestLogger;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class PingTest extends ServiceTestBase
{
   // Constants -----------------------------------------------------

   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   private static final long CLIENT_FAILURE_CHECK_PERIOD = 500;

   // Attributes ----------------------------------------------------

   private HornetQServer server;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      Configuration config = createDefaultConfig(true);
      server = createServer(false, config);
      server.start();
   }

   class Listener implements SessionFailureListener
   {
      volatile HornetQException me;

      @Override
      public void connectionFailed(final HornetQException me, boolean failedOver)
      {
         this.me = me;
      }

      @Override
      public void connectionFailed(final HornetQException me, boolean failedOver, String scaleDownTargetNodeID)
      {
         connectionFailed(me, failedOver);
      }

      public HornetQException getException()
      {
         return me;
      }

      public void beforeReconnect(final HornetQException exception)
      {
      }
   }

   /*
    * Test that no failure listeners are triggered in a non failure case with pinging going on
    */
   @Test
   public void testNoFailureWithPinging() throws Exception
   {
      ServerLocator locator = createNettyNonHALocator();

      locator.setClientFailureCheckPeriod(PingTest.CLIENT_FAILURE_CHECK_PERIOD);
      locator.setConnectionTTL(PingTest.CLIENT_FAILURE_CHECK_PERIOD * 2);

      ClientSessionFactory csf = createSessionFactory(locator);

      ClientSession session = csf.createSession(false, true, true);

      PingTest.log.info("Created session");

      Assert.assertEquals(1, ((ClientSessionFactoryInternal)csf).numConnections());

      Listener clientListener = new Listener();

      session.addFailureListener(clientListener);

      RemotingConnection serverConn = null;
      while (serverConn == null)
      {
         Set<RemotingConnection> conns = server.getRemotingService().getConnections();

         if (!conns.isEmpty())
         {
            serverConn = server.getRemotingService().getConnections().iterator().next();
         }
         else
         {
            // It's async so need to wait a while
            Thread.sleep(10);
         }
      }

      Listener serverListener = new Listener();

      serverConn.addFailureListener(serverListener);

      Thread.sleep(PingTest.CLIENT_FAILURE_CHECK_PERIOD * 10);

      Assert.assertNull(clientListener.getException());

      Assert.assertNull(serverListener.getException());

      RemotingConnection serverConn2 = server.getRemotingService().getConnections().iterator().next();

      PingTest.log.info("Server conn2 is " + serverConn2);

      Assert.assertTrue(serverConn == serverConn2);

      session.close();

      csf.close();

      locator.close();
   }

   /*
    * Test that no failure listeners are triggered in a non failure case with no pinging going on
    */
   @Test
   public void testNoFailureNoPinging() throws Exception
   {
      TransportConfiguration transportConfig = new TransportConfiguration("org.hornetq.core.remoting.impl.netty.NettyConnectorFactory");
      ServerLocator locator = addServerLocator(HornetQClient.createServerLocatorWithoutHA(transportConfig));
      locator.setClientFailureCheckPeriod(-1);
      locator.setConnectionTTL(-1);
      ClientSessionFactory csf = createSessionFactory(locator);

      ClientSession session = csf.createSession(false, true, true);

      Assert.assertEquals(1, ((ClientSessionFactoryInternal)csf).numConnections());

      Listener clientListener = new Listener();

      session.addFailureListener(clientListener);

      RemotingConnection serverConn = null;
      while (serverConn == null)
      {
         Set<RemotingConnection> conns = server.getRemotingService().getConnections();

         if (!conns.isEmpty())
         {
            serverConn = server.getRemotingService().getConnections().iterator().next();
         }
         else
         {
            // It's async so need to wait a while
            Thread.sleep(10);
         }
      }

      Listener serverListener = new Listener();

      serverConn.addFailureListener(serverListener);

      Thread.sleep(HornetQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD);

      Assert.assertNull(clientListener.getException());

      Assert.assertNull(serverListener.getException());

      RemotingConnection serverConn2 = server.getRemotingService().getConnections().iterator().next();

      PingTest.log.info("Serverconn2 is " + serverConn2);

      Assert.assertTrue(serverConn == serverConn2);

      session.close();

      csf.close();

      locator.close();
   }

   /*
    * Test that pinging is disabled for in-vm connection when using the default settings
    */
   @Test
   public void testNoPingingOnInVMConnection() throws Exception
   {
      // server should receive one and only one ping from the client so that
      // the server connection TTL is configured with the client value
      final CountDownLatch pingOnServerLatch = new CountDownLatch(2);
      server.getRemotingService().addIncomingInterceptor(new Interceptor()
      {
         public boolean intercept(final Packet packet, final RemotingConnection connection) throws HornetQException
         {
            if (packet.getType() == PacketImpl.PING)
            {
               Assert.assertEquals(HornetQClient.DEFAULT_CONNECTION_TTL_INVM, ((Ping) packet).getConnectionTTL());
               pingOnServerLatch.countDown();
            }
            return true;
         }
      });

      TransportConfiguration transportConfig = new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory");
      ServerLocator locator = addServerLocator(HornetQClient.createServerLocatorWithoutHA(transportConfig));
      ClientSessionFactory csf = createSessionFactory(locator);

      ClientSession session = csf.createSession(false, true, true);

      Assert.assertEquals(1, ((ClientSessionFactoryInternal)csf).numConnections());

      Assert.assertFalse("server received an unexpected ping from the client", pingOnServerLatch.await(HornetQClient.DEFAULT_CONNECTION_TTL, TimeUnit.MILLISECONDS));

      session.close();

      csf.close();

      locator.close();
   }

   /*
    * Test the server timing out a connection since it doesn't receive a ping in time
    */
   @Test
   public void testServerFailureNoPing() throws Exception
   {
      TransportConfiguration transportConfig = new TransportConfiguration("org.hornetq.core.remoting.impl.netty.NettyConnectorFactory");
      ServerLocator locator = addServerLocator(HornetQClient.createServerLocatorWithoutHA(transportConfig));
      locator.setClientFailureCheckPeriod(PingTest.CLIENT_FAILURE_CHECK_PERIOD);
      locator.setConnectionTTL(PingTest.CLIENT_FAILURE_CHECK_PERIOD * 2);
      ClientSessionFactoryImpl csf = (ClientSessionFactoryImpl) createSessionFactory(locator);


      Listener clientListener = new Listener();

      ClientSession session = csf.createSession(false, true, true);

      Assert.assertEquals(1, csf.numConnections());

      session.addFailureListener(clientListener);

      // We need to get it to stop pinging after one

      csf.stopPingingAfterOne();

      RemotingConnection serverConn = null;

      while (serverConn == null)
      {
         Set<RemotingConnection> conns = server.getRemotingService().getConnections();

         if (!conns.isEmpty())
         {
            serverConn = server.getRemotingService().getConnections().iterator().next();
         }
         else
         {
            // It's async so need to wait a while
            Thread.sleep(10);
         }
      }

      Listener serverListener = new Listener();

      serverConn.addFailureListener(serverListener);

      for (int i = 0; i < 1000; i++)
      {
         // a few tries to avoid a possible race caused by GCs or similar issues
         if (server.getRemotingService().getConnections().isEmpty() && clientListener.getException() != null)
         {
            break;
         }

         Thread.sleep(10);
      }

      if (!server.getRemotingService().getConnections().isEmpty())
      {
         RemotingConnection serverConn2 = server.getRemotingService().getConnections().iterator().next();

         PingTest.log.info("Serverconn2 is " + serverConn2);
      }

      Assert.assertTrue(server.getRemotingService().getConnections().isEmpty());

      // The client listener should be called too since the server will close it from the server side which will result
      // in the
      // netty detecting closure on the client side and then calling failure listener
      Assert.assertNotNull(clientListener.getException());

      Assert.assertNotNull(serverListener.getException());

      session.close();

      csf.close();

      locator.close();
   }

   /*
   * Test the client triggering failure due to no ping from server received in time
   */
   @Test
   public void testClientFailureNoServerPing() throws Exception
   {
      // server must received at least one ping from the client to pass
      // so that the server connection TTL is configured with the client value
      final CountDownLatch pingOnServerLatch = new CountDownLatch(2);
      server.getRemotingService().addIncomingInterceptor(new Interceptor()
      {

         public boolean intercept(final Packet packet, final RemotingConnection connection) throws HornetQException
         {
            if (packet.getType() == PacketImpl.PING)
            {
               pingOnServerLatch.countDown();
            }
            return true;
         }
      });

      TransportConfiguration transportConfig = new TransportConfiguration("org.hornetq.core.remoting.impl.netty.NettyConnectorFactory");
      ServerLocator locator = addServerLocator(HornetQClient.createServerLocatorWithoutHA(transportConfig));
      locator.setClientFailureCheckPeriod(PingTest.CLIENT_FAILURE_CHECK_PERIOD);
      locator.setConnectionTTL(PingTest.CLIENT_FAILURE_CHECK_PERIOD * 2);
      ClientSessionFactory csf = createSessionFactory(locator);


      ClientSession session = csf.createSession(false, true, true);

      Assert.assertEquals(1, ((ClientSessionFactoryInternal)csf).numConnections());

      final CountDownLatch clientLatch = new CountDownLatch(1);
      SessionFailureListener clientListener = new SessionFailureListener()
      {
         @Override
         public void connectionFailed(final HornetQException me, boolean failedOver)
         {
            clientLatch.countDown();
         }

         @Override
         public void connectionFailed(final HornetQException me, boolean failedOver, String scaleDownTargetNodeID)
         {
            connectionFailed(me, failedOver);
         }

         public void beforeReconnect(final HornetQException exception)
         {
         }
      };

      final CountDownLatch serverLatch = new CountDownLatch(1);
      CloseListener serverListener = new CloseListener()
      {
         public void connectionClosed()
         {
            serverLatch.countDown();
         }
      };

      session.addFailureListener(clientListener);

      CoreRemotingConnection serverConn = null;
      while (serverConn == null)
      {
         Set<RemotingConnection> conns = server.getRemotingService().getConnections();

         if (!conns.isEmpty())
         {
            serverConn = (CoreRemotingConnection)server.getRemotingService().getConnections().iterator().next();
         }
         else
         {
            // It's async so need to wait a while
            Thread.sleep(10);
         }
      }

      serverConn.addCloseListener(serverListener);
      Assert.assertTrue("server has not received any ping from the client",
                        pingOnServerLatch.await(4000, TimeUnit.MILLISECONDS));

      // we let the server receives at least 1 ping (so that it uses the client ConnectionTTL value)

      // Setting the handler to null will prevent server sending pings back to client
      serverConn.getChannel(0, -1).setHandler(null);

      Assert.assertTrue(clientLatch.await(8 * PingTest.CLIENT_FAILURE_CHECK_PERIOD, TimeUnit.MILLISECONDS));

      // Server connection will be closed too, when client closes client side connection after failure is detected
      Assert.assertTrue(serverLatch.await(2 * RemotingServiceImpl.CONNECTION_TTL_CHECK_INTERVAL, TimeUnit.MILLISECONDS));

      long start = System.currentTimeMillis();
      while (true)
      {
         if (!server.getRemotingService().getConnections().isEmpty() && System.currentTimeMillis() - start < 10000)
         {
            Thread.sleep(500);
         }
         else
         {
            break;
         }
      }
      Assert.assertTrue(server.getRemotingService().getConnections().isEmpty());

      session.close();

      csf.close();

      locator.close();
   }
}