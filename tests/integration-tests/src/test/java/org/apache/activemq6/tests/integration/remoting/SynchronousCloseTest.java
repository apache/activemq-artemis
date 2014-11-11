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
package org.apache.activemq6.tests.integration.remoting;
import org.junit.Before;

import org.junit.Test;

import org.junit.Assert;

import org.apache.activemq6.api.core.TransportConfiguration;
import org.apache.activemq6.api.core.client.ClientSession;
import org.apache.activemq6.api.core.client.ClientSessionFactory;
import org.apache.activemq6.api.core.client.HornetQClient;
import org.apache.activemq6.api.core.client.ServerLocator;
import org.apache.activemq6.core.config.Configuration;
import org.apache.activemq6.core.server.HornetQServer;
import org.apache.activemq6.tests.util.ServiceTestBase;

/**
 *
 * A SynchronousCloseTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class SynchronousCloseTest extends ServiceTestBase
{

   private HornetQServer server;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      Configuration config = createDefaultConfig(isNetty())
         .setSecurityEnabled(false);
      server = createServer(false, config);
      server.start();
   }

   protected boolean isNetty()
   {
      return false;
   }

   protected ClientSessionFactory createSessionFactory() throws Exception
   {
      ServerLocator locator;
      if (isNetty())
      {
         locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(ServiceTestBase.NETTY_CONNECTOR_FACTORY));
      }
      else
      {
         locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(ServiceTestBase.INVM_CONNECTOR_FACTORY));
      }

      return createSessionFactory(locator);
   }

   /*
    * Server side resources should be cleaned up befor call to close has returned or client could launch
    * DoS attack
    */
   @Test
   public void testSynchronousClose() throws Exception
   {
      Assert.assertEquals(0, server.getHornetQServerControl().listRemoteAddresses().length);

      ClientSessionFactory sf = createSessionFactory();

      for (int i = 0; i < 2000; i++)
      {
         ClientSession session = sf.createSession(false, true, true);

         session.close();
      }

      sf.close();

      sf.getServerLocator().close();
   }
}
