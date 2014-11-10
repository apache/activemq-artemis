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
package org.hornetq.tests.util;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.server.HornetQServer;
import org.junit.Before;

/**
 * Any test based on a single server can extend this class.
 * This is useful for quick writing tests with starting a server, locator, factory... etc
 * @author Clebert Suconic
 */

public abstract class SingleServerTestBase extends ServiceTestBase
{

   protected HornetQServer server;

   protected ClientSession session;

   protected ClientSessionFactory sf;

   protected ServerLocator locator;


   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      Configuration configuration = createDefaultConfig()
         .setSecurityEnabled(false);
      server = createServer(false, configuration);
      server.start();

      locator = createLocator();
      sf = createSessionFactory(locator);
      session = addClientSession(sf.createSession(false, true, true));
   }

   protected ServerLocator createLocator()
   {
      ServerLocator retlocator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));
      addServerLocator(retlocator);
      return retlocator;
   }

}
