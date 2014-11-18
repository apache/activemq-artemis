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
package org.apache.activemq.tests.integration.management;
import org.apache.activemq.api.config.ActiveMQDefaultConfiguration;
import org.junit.Before;
import org.junit.After;

import org.junit.Assert;

import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.api.core.client.ClientMessage;
import org.apache.activemq.api.core.client.ClientRequestor;
import org.apache.activemq.api.core.client.ClientSession;
import org.apache.activemq.api.core.client.ClientSessionFactory;
import org.apache.activemq.api.core.client.HornetQClient;
import org.apache.activemq.api.core.client.ServerLocator;
import org.apache.activemq.api.core.management.ManagementHelper;
import org.apache.activemq.api.core.management.ResourceNames;
import org.apache.activemq.core.server.HornetQServer;
import org.apache.activemq.tests.util.UnitTestCase;

/**
 * A SecurityManagementTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 *
 */
public abstract class SecurityManagementTestBase extends UnitTestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private HornetQServer service;

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

      service = setupAndStartHornetQServer();
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      service.stop();

      service = null;

      super.tearDown();
   }

   protected abstract HornetQServer setupAndStartHornetQServer() throws Exception;

   protected void doSendManagementMessage(final String user, final String password, final boolean expectSuccess) throws Exception
   {
      ServerLocator locator =
               addServerLocator(HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(
                                                                                                      UnitTestCase.INVM_CONNECTOR_FACTORY)));
      ClientSessionFactory sf = locator.createSessionFactory();
      try
      {
         ClientSession session = null;
         if (user == null)
         {
            session = sf.createSession(false, true, true);
         }
         else
         {
            session = sf.createSession(user, password, false, true, true, false, 1);
         }

         session.start();

         ClientRequestor requestor = new ClientRequestor(session, ActiveMQDefaultConfiguration.getDefaultManagementAddress());

         ClientMessage mngmntMessage = session.createMessage(false);
         ManagementHelper.putAttribute(mngmntMessage, ResourceNames.CORE_SERVER, "started");
         ClientMessage reply = requestor.request(mngmntMessage, 500);
         if (expectSuccess)
         {
            Assert.assertNotNull(reply);
            Assert.assertTrue((Boolean)ManagementHelper.getResult(reply));
         }
         else
         {
            Assert.assertNull(reply);
         }

         requestor.close();
      }
      catch (Exception e)
      {
         if (expectSuccess)
         {
            Assert.fail("got unexpected exception " + e.getClass() + ": " + e.getMessage());
            e.printStackTrace();
         }
      }
      finally
      {
         sf.close();
      }
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
