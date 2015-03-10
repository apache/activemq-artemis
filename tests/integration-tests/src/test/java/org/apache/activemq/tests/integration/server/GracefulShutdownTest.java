/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.tests.integration.server;

import org.apache.activemq.api.core.ActiveMQExceptionType;
import org.apache.activemq.api.core.ActiveMQSessionCreationException;
import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.api.core.client.ActiveMQClient;
import org.apache.activemq.api.core.client.ClientProducer;
import org.apache.activemq.api.core.client.ClientSession;
import org.apache.activemq.api.core.client.ClientSessionFactory;
import org.apache.activemq.api.core.client.ServerLocator;
import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.core.server.ActiveMQServer;
import org.apache.activemq.core.server.ActiveMQServers;
import org.apache.activemq.tests.util.ServiceTestBase;
import org.junit.Test;

public class GracefulShutdownTest extends ServiceTestBase
{
   @Test
   public void testGracefulShutdown() throws Exception
   {
      Configuration conf = createDefaultConfig();

      conf.setGracefulShutdownEnabled(true);

      final ActiveMQServer server = ActiveMQServers.newActiveMQServer(conf, false);

      server.start();

      ServerLocator locator = ActiveMQClient.createServerLocatorWithoutHA(new TransportConfiguration(ServiceTestBase.INVM_CONNECTOR_FACTORY));

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(true, true);

      Thread t = new Thread(new Runnable()
      {
         public void run()
         {
            try
            {
               server.stop();
            }
            catch (Exception e)
            {
               e.printStackTrace();
            }
         }
      });

      t.setName("shutdown thread");
      t.start();

      // wait for the thread to actually call stop() on the server
      while (server.isStarted())
      {
         Thread.sleep(100);
      }

      // confirm we can still do work on the original connection even though the server is stopping
      session.createQueue("testAddress", "testQueue");
      ClientProducer producer = session.createProducer("testAddress");
      producer.send(session.createMessage(true));
      session.start();
      assertNotNull(session.createConsumer("testQueue").receive(500));

      try
      {
         sf.createSession();
         fail("Creating a session here should fail because the acceptors should be paused");
      }
      catch (Exception e)
      {
         assertTrue(e instanceof ActiveMQSessionCreationException);
         ActiveMQSessionCreationException activeMQSessionCreationException = (ActiveMQSessionCreationException) e;
         assertEquals(activeMQSessionCreationException.getType(), ActiveMQExceptionType.SESSION_CREATION_REJECTED);
      }

      // close the connection to allow broker shutdown to complete
      locator.close();

      long start = System.currentTimeMillis();

      // wait for the shutdown thread to complete, interrupt it if it takes too long
      while (t.isAlive())
      {
         if (System.currentTimeMillis() - start > 3000)
         {
            t.interrupt();
            break;
         }
         Thread.sleep(100);
      }

      // make sure the shutdown thread is dead
      assertFalse(t.isAlive());
   }

   @Test
   public void testGracefulShutdownWithTimeout() throws Exception
   {
      long timeout = 10000;

      Configuration conf = createDefaultConfig();

      conf.setGracefulShutdownEnabled(true);
      conf.setGracefulShutdownTimeout(timeout);

      final ActiveMQServer server = ActiveMQServers.newActiveMQServer(conf, false);

      server.start();

      ServerLocator locator = ActiveMQClient.createServerLocatorWithoutHA(new TransportConfiguration(ServiceTestBase.INVM_CONNECTOR_FACTORY));

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession();

      Thread t = new Thread(new Runnable()
      {
         public void run()
         {
            try
            {
               server.stop();
            }
            catch (Exception e)
            {
               e.printStackTrace();
            }
         }
      });

      t.setName("shutdown thread");
      long start = System.currentTimeMillis();
      t.start();

      // wait for the thread to actually call stop() on the server
      while (server.isStarted())
      {
         Thread.sleep(100);
      }

      try
      {
         sf.createSession();
         fail("Creating a session here should fail because the acceptors should be paused");
      }
      catch (Exception e)
      {
         assertTrue(e instanceof ActiveMQSessionCreationException);
         ActiveMQSessionCreationException activeMQSessionCreationException = (ActiveMQSessionCreationException) e;
         assertEquals(activeMQSessionCreationException.getType(), ActiveMQExceptionType.SESSION_CREATION_REJECTED);
      }

      Thread.sleep(timeout / 2);

      assertTrue("thread should still be alive here waiting for the timeout to elapse", t.isAlive());

      while (t.isAlive())
      {
         Thread.sleep(100);
      }

      assertTrue("thread terminated too soon, the graceful shutdown timeout wasn't enforced properly", System.currentTimeMillis() - start >= timeout);

      locator.close();
   }
}
