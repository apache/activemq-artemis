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
import org.apache.activemq.api.core.ActiveMQIllegalStateException;
import org.apache.activemq.api.core.ActiveMQObjectClosedException;
import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.api.core.client.ClientConsumer;
import org.apache.activemq.api.core.client.ClientMessage;
import org.apache.activemq.api.core.client.ClientProducer;
import org.apache.activemq.api.core.client.ClientSession;
import org.apache.activemq.api.core.client.ClientSessionFactory;
import org.apache.activemq.api.core.client.MessageHandler;
import org.apache.activemq.api.core.client.ServerLocator;
import org.apache.activemq.core.server.HornetQServer;
import org.apache.activemq.tests.util.ServiceTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class ReceiveTest extends ServiceTestBase
{
   SimpleString addressA = new SimpleString("addressA");

   SimpleString queueA = new SimpleString("queueA");

   private ServerLocator locator;

   private HornetQServer server;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      locator = createInVMNonHALocator();
      server = createServer(false);
      server.start();
   }

   @Test
   public void testBasicReceive() throws Exception
   {
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession sendSession = cf.createSession(false, true, true);
      ClientProducer cp = sendSession.createProducer(addressA);
      ClientSession session = cf.createSession(false, true, true);
      session.createQueue(addressA, queueA, false);
      ClientConsumer cc = session.createConsumer(queueA);
      session.start();
      cp.send(sendSession.createMessage(false));
      Assert.assertNotNull(cc.receive());
      session.close();
      sendSession.close();
   }

   @Test
   public void testReceiveTimesoutCorrectly() throws Exception
   {

      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession session = cf.createSession(false, true, true);
      session.createQueue(addressA, queueA, false);
      ClientConsumer cc = session.createConsumer(queueA);
      session.start();
      long time = System.currentTimeMillis();
      cc.receive(1000);
      Assert.assertTrue(System.currentTimeMillis() - time >= 1000);
      session.close();
   }

   @Test
   public void testReceiveOnClosedException() throws Exception
   {

      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession session = cf.createSession(false, true, true);
      session.createQueue(addressA, queueA, false);
      ClientConsumer cc = session.createConsumer(queueA);
      session.start();
      session.close();
      try
      {
         cc.receive();
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
      session.close();
   }

   @Test
   public void testReceiveThrowsExceptionWhenHandlerSet() throws Exception
   {

      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession session = cf.createSession(false, true, true);
      session.createQueue(addressA, queueA, false);
      ClientConsumer cc = session.createConsumer(queueA);
      session.start();
      cc.setMessageHandler(new MessageHandler()
      {
         public void onMessage(final ClientMessage message)
         {
         }
      });
      try
      {
         cc.receive();
         Assert.fail("should throw exception");
      }
      catch (ActiveMQIllegalStateException ise)
      {
         //ok
      }
      catch (ActiveMQException e)
      {
         fail("Invalid Exception type:" + e.getType());
      }
      session.close();
   }

   @Test
   public void testReceiveImmediate() throws Exception
   {

      // forces perfect round robin
      locator.setConsumerWindowSize(1);
      ClientSessionFactory cf = createSessionFactory(locator);
      ClientSession sendSession = cf.createSession(false, true, true);
      ClientProducer cp = sendSession.createProducer(addressA);
      ClientSession session = cf.createSession(false, true, true);
      session.createQueue(addressA, queueA, false);
      ClientConsumer cc = session.createConsumer(queueA);
      ClientConsumer cc2 = session.createConsumer(queueA);
      session.start();
      cp.send(sendSession.createMessage(false));
      cp.send(sendSession.createMessage(false));
      cp.send(sendSession.createMessage(false));

      Assert.assertNotNull(cc2.receive(5000));
      Assert.assertNotNull(cc.receive(5000));
      if (cc.receiveImmediate() == null)
      {
         assertNotNull(cc2.receiveImmediate());
      }
      session.close();
      sendSession.close();
   }
}
