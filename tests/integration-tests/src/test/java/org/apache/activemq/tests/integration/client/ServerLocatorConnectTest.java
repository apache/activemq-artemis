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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.api.core.ActiveMQException;
import org.apache.activemq.api.core.ActiveMQNotConnectedException;
import org.apache.activemq.api.core.client.ClientSessionFactory;
import org.apache.activemq.api.core.client.ActiveMQClient;
import org.apache.activemq.api.core.client.ServerLocator;
import org.apache.activemq.core.client.impl.ClientSessionFactoryInternal;
import org.apache.activemq.core.client.impl.ServerLocatorInternal;
import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.core.server.ActiveMQServer;
import org.apache.activemq.tests.util.ServiceTestBase;
import org.junit.Before;
import org.junit.Test;

/**
 * User: andy
 * Date: Sep 15, 2010
 * Time: 2:27:07 PM
 * * @author <a href="mailto:andy.taylor@jboss.com">Andy Taylor</a>
 */
public class ServerLocatorConnectTest extends ServiceTestBase
{
   private ActiveMQServer server;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      Configuration configuration = createDefaultConfig(isNetty());
      server = createServer(false, configuration);
      server.start();
   }

   @Test
   public void testSingleConnectorSingleServer() throws Exception
   {
      ServerLocator locator = ActiveMQClient.createServerLocatorWithoutHA(createTransportConfiguration(isNetty(), false, generateParams(0, isNetty())));
      ClientSessionFactory csf = createSessionFactory(locator);
      csf.close();
      locator.close();
   }

   @Test
   public void testSingleConnectorSingleServerConnect() throws Exception
   {
      ServerLocatorInternal locator = (ServerLocatorInternal) ActiveMQClient.createServerLocatorWithoutHA(createTransportConfiguration(isNetty(), false, generateParams(0, isNetty())));
      ClientSessionFactoryInternal csf = locator.connect();
      assertNotNull(csf);
      assertEquals(csf.numConnections(), 1);
      locator.close();
   }

   @Test
   public void testMultipleConnectorSingleServerConnect() throws Exception
   {
      ServerLocatorInternal locator = (ServerLocatorInternal) ActiveMQClient.createServerLocatorWithoutHA(
         createTransportConfiguration(isNetty(), false, generateParams(0, isNetty())),
         createTransportConfiguration(isNetty(), false, generateParams(1, isNetty())),
         createTransportConfiguration(isNetty(), false, generateParams(2, isNetty())),
         createTransportConfiguration(isNetty(), false, generateParams(3, isNetty())),
         createTransportConfiguration(isNetty(), false, generateParams(4, isNetty()))
      );
      ClientSessionFactoryInternal csf = locator.connect();
      assertNotNull(csf);
      assertEquals(csf.numConnections(), 1);
      locator.close();
   }

   @Test
   public void testMultipleConnectorSingleServerConnectReconnect() throws Exception
   {
      ServerLocatorInternal locator = (ServerLocatorInternal) ActiveMQClient.createServerLocatorWithoutHA(
         createTransportConfiguration(isNetty(), false, generateParams(0, isNetty())),
         createTransportConfiguration(isNetty(), false, generateParams(1, isNetty())),
         createTransportConfiguration(isNetty(), false, generateParams(2, isNetty())),
         createTransportConfiguration(isNetty(), false, generateParams(3, isNetty())),
         createTransportConfiguration(isNetty(), false, generateParams(4, isNetty()))
      );
      locator.setReconnectAttempts(-1);
      ClientSessionFactoryInternal csf = locator.connect();
      assertNotNull(csf);
      assertEquals(csf.numConnections(), 1);
      locator.close();
   }

   @Test
   public void testMultipleConnectorSingleServerNoConnect() throws Exception
   {
      ServerLocatorInternal locator = (ServerLocatorInternal) ActiveMQClient.createServerLocatorWithoutHA(
         createTransportConfiguration(isNetty(), false, generateParams(1, isNetty())),
         createTransportConfiguration(isNetty(), false, generateParams(2, isNetty())),
         createTransportConfiguration(isNetty(), false, generateParams(3, isNetty())),
         createTransportConfiguration(isNetty(), false, generateParams(4, isNetty())),
         createTransportConfiguration(isNetty(), false, generateParams(5, isNetty()))
      );
      ClientSessionFactoryInternal csf = null;
      try
      {
         csf = locator.connect();
      }
      catch (ActiveMQNotConnectedException nce)
      {
         //ok
      }
      catch (Exception e)
      {
         assertTrue(e instanceof ActiveMQException);
         fail("Invalid Exception type:" + ((ActiveMQException) e).getType());
      }
      assertNull(csf);
      locator.close();
   }

   @Test
   public void testMultipleConnectorSingleServerNoConnectAttemptReconnect() throws Exception
   {
      ServerLocatorInternal locator = (ServerLocatorInternal) ActiveMQClient.createServerLocatorWithoutHA(
         createTransportConfiguration(isNetty(), false, generateParams(1, isNetty())),
         createTransportConfiguration(isNetty(), false, generateParams(2, isNetty())),
         createTransportConfiguration(isNetty(), false, generateParams(3, isNetty())),
         createTransportConfiguration(isNetty(), false, generateParams(4, isNetty())),
         createTransportConfiguration(isNetty(), false, generateParams(5, isNetty()))
      );
      locator.setReconnectAttempts(-1);
      CountDownLatch countDownLatch = new CountDownLatch(1);
      Connector target = new Connector(locator, countDownLatch);
      Thread t = new Thread(target);
      t.start();
      //let them get started
      Thread.sleep(500);
      locator.close();
      assertTrue(countDownLatch.await(5, TimeUnit.SECONDS));
      assertNull(target.csf);
   }

   public boolean isNetty()
   {
      return false;
   }

   static class Connector implements Runnable
   {
      private final ServerLocatorInternal locator;
      ClientSessionFactory csf = null;
      CountDownLatch latch;
      Exception e;

      public Connector(ServerLocatorInternal locator, CountDownLatch latch)
      {
         this.locator = locator;
         this.latch = latch;
      }

      public void run()
      {
         try
         {
            csf = locator.connect();
         }
         catch (Exception e)
         {
            this.e = e;
         }
         latch.countDown();
      }
   }
}
