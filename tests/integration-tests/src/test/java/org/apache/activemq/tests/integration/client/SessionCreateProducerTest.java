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

import org.apache.activemq.api.core.ActiveMQException;
import org.apache.activemq.api.core.ActiveMQObjectClosedException;
import org.apache.activemq.api.core.client.ClientProducer;
import org.apache.activemq.api.core.client.ClientSessionFactory;
import org.apache.activemq.api.core.client.ServerLocator;
import org.apache.activemq.core.client.impl.ClientSessionInternal;
import org.apache.activemq.core.server.HornetQServer;
import org.apache.activemq.tests.util.ServiceTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class SessionCreateProducerTest extends ServiceTestBase
{
   private ServerLocator locator;
   private ClientSessionInternal clientSession;
   private ClientSessionFactory cf;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      locator = createInVMNonHALocator();
      HornetQServer service = createServer(false);
      service.start();
      locator.setProducerMaxRate(99);
      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnNonDurableSend(true);
      cf = createSessionFactory(locator);
      clientSession = (ClientSessionInternal) addClientSession(cf.createSession(false, true, true));
   }

   @Test
   public void testCreateAnonProducer() throws Exception
   {
      ClientProducer producer = clientSession.createProducer();
      Assert.assertNull(producer.getAddress());
      Assert.assertEquals(cf.getServerLocator().getProducerMaxRate(), producer.getMaxRate());
      Assert.assertEquals(cf.getServerLocator().isBlockOnNonDurableSend(), producer.isBlockOnNonDurableSend());
      Assert.assertEquals(cf.getServerLocator().isBlockOnDurableSend(), producer.isBlockOnDurableSend());
      Assert.assertFalse(producer.isClosed());
   }

   @Test
   public void testCreateProducer1() throws Exception
   {
      ClientProducer producer = clientSession.createProducer("testAddress");
      Assert.assertNotNull(producer.getAddress());
      Assert.assertEquals(cf.getServerLocator().getProducerMaxRate(), producer.getMaxRate());
      Assert.assertEquals(cf.getServerLocator().isBlockOnNonDurableSend(), producer.isBlockOnNonDurableSend());
      Assert.assertEquals(cf.getServerLocator().isBlockOnDurableSend(), producer.isBlockOnDurableSend());
      Assert.assertFalse(producer.isClosed());
   }

   @Test
   public void testProducerOnClosedSession() throws Exception
   {
      clientSession.close();
      try
      {
         clientSession.createProducer();
         Assert.fail("should throw exception");
      }
      catch (ActiveMQObjectClosedException oce)
      {
         //ok
      }
      catch (ActiveMQException e)
      {
         fail("Invalid Exception type:" + e.getType());
      }
   }

}
