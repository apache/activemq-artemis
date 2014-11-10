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
package org.hornetq.tests.soak.client;
import org.junit.Before;

import org.junit.Test;

import java.util.HashMap;

import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * A ClientSoakTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class SimpleSendReceiveSoakTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private static final SimpleString ADDRESS = new SimpleString("ADD");

   private static final boolean IS_NETTY = false;

   private static final boolean IS_JOURNAL = false;

   public static final int MIN_MESSAGES_ON_QUEUE = 1000;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   private HornetQServer server;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      clearDataRecreateServerDirs();

      Configuration config = createDefaultConfig(SimpleSendReceiveSoakTest.IS_NETTY)
         .setJournalFileSize(10 * 1024 * 1024);

      server = createServer(IS_JOURNAL, config, -1, -1, new HashMap<String, AddressSettings>());

      server.start();

      ServerLocator locator = createFactory(IS_NETTY);

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession();

      session.createQueue(SimpleSendReceiveSoakTest.ADDRESS, SimpleSendReceiveSoakTest.ADDRESS, true);

      session.close();
   }

   @Test
   public void testSoakClientTransactions() throws Exception
   {
      final ServerLocator locator = createFactory(IS_NETTY);

      final ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(true, true);

      ClientProducer producer = session.createProducer(ADDRESS);

      long msgId = 0;

      long msgReceivedID = 0;

      for (int i = 0; i < MIN_MESSAGES_ON_QUEUE; i++)
      {
         ClientMessage msg = session.createMessage(IS_JOURNAL);
         msg.putLongProperty("count", msgId++);
         msg.getBodyBuffer().writeBytes(new byte[10 * 1024]);
         producer.send(msg);
      }

      ClientSession sessionConsumer = sf.createSession(true, true, 0);
      ClientConsumer consumer = sessionConsumer.createConsumer(ADDRESS);
      sessionConsumer.start();

      for (int loopNumber = 0; loopNumber < 1000; loopNumber++)
      {
         System.out.println("Loop " + loopNumber);
         for (int i = 0; i < MIN_MESSAGES_ON_QUEUE; i++)
         {
            ClientMessage msg = session.createMessage(IS_JOURNAL);
            msg.putLongProperty("count", msgId++);
            msg.getBodyBuffer().writeBytes(new byte[10 * 1024]);
            producer.send(msg);
         }

         for (int i = 0; i < MIN_MESSAGES_ON_QUEUE; i++)
         {
            ClientMessage msg = consumer.receive(5000);
            assertNotNull(msg);
            msg.acknowledge();
            assertEquals(msgReceivedID++, msg.getLongProperty("count").longValue());
         }
      }

      sessionConsumer.close();
      session.close();
   }
}
