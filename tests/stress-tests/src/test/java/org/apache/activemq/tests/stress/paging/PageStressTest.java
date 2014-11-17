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
package org.apache.activemq.tests.stress.paging;

import java.util.HashMap;

import org.apache.activemq.api.core.HornetQException;
import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.api.core.client.ClientConsumer;
import org.apache.activemq.api.core.client.ClientMessage;
import org.apache.activemq.api.core.client.ClientProducer;
import org.apache.activemq.api.core.client.ClientSession;
import org.apache.activemq.api.core.client.ClientSessionFactory;
import org.apache.activemq.api.core.client.ServerLocator;
import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.core.server.HornetQServer;
import org.apache.activemq.core.settings.impl.AddressSettings;
import org.apache.activemq.jms.client.HornetQBytesMessage;
import org.apache.activemq.tests.util.ServiceTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * This is an integration-tests that will take some time to run.
 *
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 */
public class PageStressTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private HornetQServer messagingService;

   private ServerLocator locator;

   @Test
   public void testStopDuringDepage() throws Exception
   {
      Configuration config = createDefaultConfig()
         .setJournalSyncNonTransactional(false)
         .setJournalSyncTransactional(false);

      HashMap<String, AddressSettings> settings = new HashMap<String, AddressSettings>();

      AddressSettings setting = new AddressSettings();
      setting.setMaxSizeBytes(20 * 1024 * 1024);
      settings.put("page-adr", setting);

      messagingService = createServer(true, config, 10 * 1024 * 1024, 20 * 1024 * 1024, settings);
      messagingService.start();

      ClientSessionFactory factory = createSessionFactory(locator);
      ClientSession session = null;

      try
      {

         final int NUMBER_OF_MESSAGES = 60000;

         session = factory.createSession(null, null, false, false, true, false, 1024 * NUMBER_OF_MESSAGES);

         SimpleString address = new SimpleString("page-adr");

         session.createQueue(address, address, null, true);

         ClientProducer prod = session.createProducer(address);

         ClientMessage message = createBytesMessage(session, HornetQBytesMessage.TYPE, new byte[700], true);

         for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
         {
            if (i % 10000 == 0)
            {
               System.out.println("Sent " + i);
            }
            prod.send(message);
         }

         session.commit();

         session.start();

         ClientConsumer consumer = session.createConsumer(address);

         int msgs = 0;
         ClientMessage msg = null;
         do
         {
            msg = consumer.receive(10000);
            if (msg != null)
            {
               msg.acknowledge();
               if (++msgs % 1000 == 0)
               {
                  System.out.println("Received " + msgs);
               }
            }
         }
         while (msg != null);

         session.commit();

         session.close();

         messagingService.stop();

         System.out.println("server stopped, nr msgs: " + msgs);

         messagingService = createServer(true, config, 10 * 1024 * 1024, 20 * 1024 * 1024, settings);

         messagingService.start();

         factory = createSessionFactory(locator);

         session = factory.createSession(false, false, false);

         consumer = session.createConsumer(address);

         session.start();

         msg = null;
         do
         {
            msg = consumer.receive(10000);
            if (msg != null)
            {
               msg.acknowledge();
               session.commit();
               if (++msgs % 1000 == 0)
               {
                  System.out.println("Received " + msgs);
               }
            }
         }
         while (msg != null);

         System.out.println("msgs second time: " + msgs);

         Assert.assertEquals(NUMBER_OF_MESSAGES, msgs);
      }
      finally
      {
         session.close();
      }

   }

   @Test
   public void testPageOnMultipleDestinations() throws Exception
   {
      Configuration config = createDefaultConfig();

      HashMap<String, AddressSettings> settings = new HashMap<String, AddressSettings>();

      AddressSettings setting = new AddressSettings();
      setting.setMaxSizeBytes(20 * 1024 * 1024);
      settings.put("page-adr", setting);

      messagingService = createServer(true, config, 10 * 1024 * 1024, 20 * 1024 * 1024, settings);
      messagingService.start();

      ClientSessionFactory factory = createSessionFactory(locator);
      ClientSession session = null;

      try
      {
         session = factory.createSession(false, false, false);

         SimpleString address = new SimpleString("page-adr");
         SimpleString[] queue = new SimpleString[]{new SimpleString("queue1"), new SimpleString("queue2")};

         session.createQueue(address, queue[0], null, true);
         session.createQueue(address, queue[1], null, true);

         ClientProducer prod = session.createProducer(address);

         ClientMessage message = createBytesMessage(session, HornetQBytesMessage.TYPE, new byte[700], false);

         int NUMBER_OF_MESSAGES = 60000;

         for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
         {
            if (i % 10000 == 0)
            {
               System.out.println(i);
            }
            prod.send(message);
         }

         session.commit();

         session.start();

         int[] counters = new int[2];

         ClientConsumer[] consumers = new ClientConsumer[]{session.createConsumer(queue[0]),
            session.createConsumer(queue[1])};

         while (true)
         {
            int msgs1 = readMessages(session, consumers[0], queue[0]);
            int msgs2 = readMessages(session, consumers[1], queue[1]);
            counters[0] += msgs1;
            counters[1] += msgs2;

            System.out.println("msgs1 = " + msgs1 + " msgs2 = " + msgs2);

            if (msgs1 + msgs2 == 0)
            {
               break;
            }
         }

         consumers[0].close();
         consumers[1].close();

         Assert.assertEquals(NUMBER_OF_MESSAGES, counters[0]);
         Assert.assertEquals(NUMBER_OF_MESSAGES, counters[1]);
      }
      finally
      {
         session.close();
         messagingService.stop();
      }

   }

   private int readMessages(final ClientSession session, final ClientConsumer consumer, final SimpleString queue) throws HornetQException
   {
      session.start();
      int msgs = 0;

      ClientMessage msg = null;
      do
      {
         msg = consumer.receive(1000);
         if (msg != null)
         {
            msg.acknowledge();
            if (++msgs % 10000 == 0)
            {
               System.out.println("received " + msgs);
               session.commit();

            }
         }
      }
      while (msg != null);

      session.commit();

      return msgs;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------
   @Override
   protected Configuration createDefaultConfig() throws Exception
   {
      Configuration config = super.createDefaultConfig()
         .setJournalFileSize(10 * 1024 * 1024)
         .setJournalMinFiles(5);

      return config;
   }

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      locator = createInVMNonHALocator();

      locator.setBlockOnAcknowledge(true);
      locator.setBlockOnDurableSend(false);
      locator.setBlockOnNonDurableSend(false);

   }
}
