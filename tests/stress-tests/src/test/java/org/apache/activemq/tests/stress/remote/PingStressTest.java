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
package org.apache.activemq.tests.stress.remote;
import org.apache.activemq.api.core.ActiveMQException;
import org.junit.Before;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.api.core.Interceptor;
import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.api.core.client.ClientSession;
import org.apache.activemq.api.core.client.ClientSessionFactory;
import org.apache.activemq.api.core.client.ActiveMQClient;
import org.apache.activemq.api.core.client.ServerLocator;
import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.core.protocol.core.Packet;
import org.apache.activemq.core.protocol.core.impl.PacketImpl;
import org.apache.activemq.core.server.ActiveMQServer;
import org.apache.activemq.spi.core.protocol.RemotingConnection;
import org.apache.activemq.tests.unit.UnitTestLogger;
import org.apache.activemq.tests.util.RandomUtil;
import org.apache.activemq.tests.util.ServiceTestBase;

/**
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class PingStressTest extends ServiceTestBase
{
   private static final UnitTestLogger log = UnitTestLogger.LOGGER;

   private static final long PING_INTERVAL = 500;

   private ActiveMQServer server;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      Configuration config = createDefaultConfig(true);
      server = createServer(false, config);
      server.start();
   }

   protected int getNumberOfIterations()
   {
      return 20;
   }

   @Test
   public void testMultiThreadOpenAndCloses() throws Exception
   {
      for (int i = 0; i < getNumberOfIterations(); i++)
      {
         if (i > 0)
         {
            tearDown();
            setUp();
         }
         internalTest();
      }

   }

   /*
    * Test the client triggering failure due to no pong received in time
    */
   private void internalTest() throws Exception
   {
      final TransportConfiguration transportConfig = new TransportConfiguration("org.apache.activemq.core.remoting.impl.netty.NettyConnectorFactory");

      Interceptor noPongInterceptor = new Interceptor()
      {
         public boolean intercept(final Packet packet, final RemotingConnection conn) throws ActiveMQException
         {
            PingStressTest.log.info("In interceptor, packet is " + packet.getType());
            if (packet.getType() == PacketImpl.PING)
            {
               PingStressTest.log.info("Ignoring Ping packet.. it will be dropped");
               return false;
            }
            else
            {
               return true;
            }
         }
      };

      server.getRemotingService().addIncomingInterceptor(noPongInterceptor);
      ServerLocator locator = addServerLocator(ActiveMQClient.createServerLocatorWithoutHA(transportConfig));
      locator.setClientFailureCheckPeriod(PingStressTest.PING_INTERVAL);
      locator.setConnectionTTL((long)(PingStressTest.PING_INTERVAL * 1.5));
      locator.setCallTimeout(PingStressTest.PING_INTERVAL * 10);
      final ClientSessionFactory csf1 = createSessionFactory(locator);


      final int numberOfSessions = 1;
      final int numberOfThreads = 30;

      final CountDownLatch flagStart = new CountDownLatch(1);
      final CountDownLatch flagAligned = new CountDownLatch(numberOfThreads);

      class LocalThread extends Thread
      {
         Throwable failure;

         int threadNumber;

         public LocalThread(final int i)
         {
            super("LocalThread i = " + i);
            threadNumber = i;
         }

         @Override
         public void run()
         {
            try
            {

               ServerLocator locator = addServerLocator(ActiveMQClient.createServerLocatorWithoutHA(transportConfig));
               locator.setClientFailureCheckPeriod(PingStressTest.PING_INTERVAL);
               locator.setConnectionTTL((long)(PingStressTest.PING_INTERVAL * 1.5));
               locator.setCallTimeout(PingStressTest.PING_INTERVAL * 10);

               final ClientSessionFactory csf2 = createSessionFactory(locator);

               // Start all at once to make concurrency worst
               flagAligned.countDown();
               flagStart.await();
               for (int i = 0; i < numberOfSessions; i++)
               {
                  System.out.println(getName() + " Session = " + i);

                  ClientSession session;

                  // Sometimes we use the SessionFactory declared on this thread, sometimes the SessionFactory declared
                  // on the test, sharing it with other threads
                  // (playing a possible user behaviour where you share the Factories among threads, versus not sharing
                  // them)
                  if (RandomUtil.randomBoolean())
                  {
                     session = csf1.createSession(false, false, false);
                  }
                  else
                  {
                     session = csf2.createSession(false, false, false);
                  }

                  // We will wait to anything between 0 to PING_INTERVAL * 2
                  Thread.sleep(PingStressTest.PING_INTERVAL * (threadNumber % 3));

                  session.close();

                  csf2.close();

                  locator.close();
               }
            }
            catch (Throwable e)
            {
               e.printStackTrace();
               failure = e;
            }
         }
      }

      LocalThread[] threads = new LocalThread[numberOfThreads];

      for (int i = 0; i < numberOfThreads; i++)
      {
         threads[i] = new LocalThread(i);
         threads[i].start();
      }

      assertTrue(flagAligned.await(10, TimeUnit.SECONDS));
      flagStart.countDown();

      Throwable e = null;
      for (LocalThread t : threads)
      {
         t.join();
         if (t.failure != null)
         {
            e = t.failure;
         }
      }

      if (e != null)
      {
         throw new Exception("Test Failed", e);
      }

      csf1.close();

      locator.close();

   }
}