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
package org.apache.activemq6.tests.integration.client;

import org.apache.activemq6.api.core.HornetQException;
import org.apache.activemq6.api.core.HornetQNotConnectedException;
import org.apache.activemq6.api.core.HornetQObjectClosedException;
import org.apache.activemq6.api.core.TransportConfiguration;
import org.apache.activemq6.api.core.client.ClientConsumer;
import org.apache.activemq6.api.core.client.ClientProducer;
import org.apache.activemq6.api.core.client.ClientSession;
import org.apache.activemq6.api.core.client.ClientSessionFactory;
import org.apache.activemq6.api.core.client.ServerLocator;
import org.apache.activemq6.core.client.impl.ClientSessionInternal;
import org.apache.activemq6.core.config.Configuration;
import org.apache.activemq6.core.server.HornetQServer;
import org.apache.activemq6.spi.core.protocol.RemotingConnection;
import org.apache.activemq6.tests.util.ServiceTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * A SessionClosedOnRemotingConnectionFailureTest
 *
 * @author Tim Fox
 */
public class SessionClosedOnRemotingConnectionFailureTest extends ServiceTestBase
{
   private HornetQServer server;

   private ClientSessionFactory sf;

   @Test
   public void testSessionClosedOnRemotingConnectionFailure() throws Exception
   {
      ClientSession session = addClientSession(sf.createSession());

      session.createQueue("fooaddress", "fooqueue");

      ClientProducer prod = session.createProducer("fooaddress");

      ClientConsumer cons = session.createConsumer("fooqueue");

      session.start();

      prod.send(session.createMessage(false));

      Assert.assertNotNull(cons.receive());

      // Now fail the underlying connection

      RemotingConnection connection = ((ClientSessionInternal) session).getConnection();

      connection.fail(new HornetQNotConnectedException());

      Assert.assertTrue(session.isClosed());

      Assert.assertTrue(prod.isClosed());

      Assert.assertTrue(cons.isClosed());

      // Now try and use the producer

      try
      {
         prod.send(session.createMessage(false));

         Assert.fail("Should throw exception");
      }
      catch (HornetQObjectClosedException oce)
      {
         //ok
      }
      catch (HornetQException e)
      {
         fail("Invalid Exception type:" + e.getType());
      }

      try
      {
         cons.receive();

         Assert.fail("Should throw exception");
      }
      catch (HornetQObjectClosedException oce)
      {
         //ok
      }
      catch (HornetQException e)
      {
         fail("Invalid Exception type:" + e.getType());
      }

      session.close();
   }

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      Configuration config = createDefaultConfig()
         .addAcceptorConfiguration(new TransportConfiguration(INVM_ACCEPTOR_FACTORY))
         .setSecurityEnabled(false);
      server = createServer(false, config);

      server.start();
      ServerLocator locator = createInVMNonHALocator();
      sf = createSessionFactory(locator);
   }
}
