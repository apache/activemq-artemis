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
package org.apache.activemq6.tests.integration.paging;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq6.api.core.SimpleString;
import org.apache.activemq6.api.core.client.ClientConsumer;
import org.apache.activemq6.api.core.client.ClientMessage;
import org.apache.activemq6.api.core.client.ClientProducer;
import org.apache.activemq6.api.core.client.ClientSession;
import org.apache.activemq6.api.core.client.ClientSessionFactory;
import org.apache.activemq6.api.core.client.ServerLocator;
import org.apache.activemq6.core.server.HornetQServer;
import org.apache.activemq6.core.settings.impl.AddressSettings;
import org.apache.activemq6.tests.util.ServiceTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * A SendTest
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class PagingSendTest extends ServiceTestBase
{
   public static final SimpleString ADDRESS = new SimpleString("SimpleAddress");

   private ServerLocator locator;

   private HornetQServer server;

   protected boolean isNetty()
   {
      return false;
   }

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      server = newHornetQServer();
      server.start();
      waitForServer(server);
      locator = createFactory(isNetty());
   }

   private HornetQServer newHornetQServer() throws Exception
   {
      HornetQServer server = createServer(true, isNetty());

      AddressSettings defaultSetting = new AddressSettings();
      defaultSetting.setPageSizeBytes(10 * 1024);
      defaultSetting.setMaxSizeBytes(20 * 1024);

      server.getAddressSettingsRepository().addMatch("#", defaultSetting);

      return server;
   }

   @Test
   public void testSameMessageOverAndOverBlocking() throws Exception
   {
      dotestSameMessageOverAndOver(true);
   }

   @Test
   public void testSameMessageOverAndOverNonBlocking() throws Exception
   {
      dotestSameMessageOverAndOver(false);
   }

   public void dotestSameMessageOverAndOver(final boolean blocking) throws Exception
   {
      // Making it synchronous, just because we want to stop sending messages as soon as the
      // page-store becomes in
      // page mode
      // and we could only guarantee that by setting it to synchronous
      locator.setBlockOnNonDurableSend(blocking);
      locator.setBlockOnDurableSend(blocking);
      locator.setBlockOnAcknowledge(blocking);

      ClientSessionFactory sf = createSessionFactory(locator);
      ClientSession session = sf.createSession(null, null, false, true, true, false, 0);

      session.createQueue(PagingSendTest.ADDRESS, PagingSendTest.ADDRESS, null, true);

      ClientProducer producer = session.createProducer(PagingSendTest.ADDRESS);

      ClientMessage message = null;

      message = session.createMessage(true);
      message.getBodyBuffer().writeBytes(new byte[1024]);

      for (int i = 0; i < 200; i++)
      {
         producer.send(message);
      }

      session.close();

      session = sf.createSession(null, null, false, true, true, false, 0);

      ClientConsumer consumer = session.createConsumer(PagingSendTest.ADDRESS);

      session.start();

      for (int i = 0; i < 200; i++)
      {
         ClientMessage message2 = consumer.receive(10000);

         Assert.assertNotNull(message2);

         if (i == 100)
         {
            session.commit();
         }

         message2.acknowledge();
      }

      consumer.close();

      session.close();
   }

   @Test
   public void testOrderOverTX() throws Exception
   {
      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession sessionConsumer = sf.createSession(true, true, 0);

      sessionConsumer.createQueue(PagingSendTest.ADDRESS, PagingSendTest.ADDRESS, null, true);

      final ClientSession sessionProducer = sf.createSession(false, false);
      final ClientProducer producer = sessionProducer.createProducer(PagingSendTest.ADDRESS);

      final AtomicInteger errors = new AtomicInteger(0);

      final int TOTAL_MESSAGES = 1000;

      // Consumer will be ready after we have commits
      final CountDownLatch ready = new CountDownLatch(1);

      Thread tProducer = new Thread()
      {
         @Override
         public void run()
         {
            try
            {
               int commit = 0;
               for (int i = 0; i < TOTAL_MESSAGES; i++)
               {
                  ClientMessage msg = sessionProducer.createMessage(true);
                  msg.getBodyBuffer().writeBytes(new byte[1024]);
                  msg.putIntProperty("count", i);
                  producer.send(msg);

                  if (i % 100 == 0 && i > 0)
                  {
                     sessionProducer.commit();
                     if (commit++ > 2)
                     {
                        ready.countDown();
                     }
                  }
               }

               sessionProducer.commit();

            }
            catch (Exception e)
            {
               e.printStackTrace();
               errors.incrementAndGet();
            }
         }
      };

      ClientConsumer consumer = sessionConsumer.createConsumer(PagingSendTest.ADDRESS);

      sessionConsumer.start();

      tProducer.start();

      assertTrue(ready.await(10, TimeUnit.SECONDS));

      for (int i = 0; i < TOTAL_MESSAGES; i++)
      {
         ClientMessage msg = consumer.receive(10000);

         Assert.assertNotNull(msg);

         assertEquals(i, msg.getIntProperty("count").intValue());

         msg.acknowledge();
      }

      tProducer.join();

      sessionConsumer.close();

      sessionProducer.close();

      assertEquals(0, errors.get());
   }
}