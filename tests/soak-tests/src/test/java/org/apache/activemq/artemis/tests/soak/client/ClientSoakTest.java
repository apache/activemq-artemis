/*
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
package org.apache.activemq.artemis.tests.soak.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.DivertConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Before;
import org.junit.Test;

public class ClientSoakTest extends ActiveMQTestBase {

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private static final SimpleString ADDRESS = new SimpleString("ADD");

   private static final SimpleString DIVERTED_AD1 = ClientSoakTest.ADDRESS.concat("-1");

   private static final SimpleString DIVERTED_AD2 = ClientSoakTest.ADDRESS.concat("-2");

   private static final boolean IS_JOURNAL = true;

   public static final int MIN_MESSAGES_ON_QUEUE = 5000;

   protected boolean isNetty() {
      return true;
   }

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   private ActiveMQServer server;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      clearDataRecreateServerDirs();

      Configuration config = createDefaultConfig(isNetty()).setJournalFileSize(10 * 1024 * 1024);

      server = createServer(IS_JOURNAL, config, -1, -1, new HashMap<String, AddressSettings>());

      DivertConfiguration divert1 = new DivertConfiguration().setName("dv1").setRoutingName("nm1").setAddress(ClientSoakTest.ADDRESS.toString()).setForwardingAddress(ClientSoakTest.DIVERTED_AD1.toString()).setExclusive(true);

      DivertConfiguration divert2 = new DivertConfiguration().setName("dv2").setRoutingName("nm2").setAddress(ClientSoakTest.ADDRESS.toString()).setForwardingAddress(ClientSoakTest.DIVERTED_AD2.toString()).setExclusive(true);

      ArrayList<DivertConfiguration> divertList = new ArrayList<>();
      divertList.add(divert1);
      divertList.add(divert2);

      config.setDivertConfigurations(divertList);

      server.start();

      ServerLocator locator = createFactory(isNetty());

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession();

      session.createQueue(ClientSoakTest.ADDRESS, ClientSoakTest.ADDRESS, true);

      session.createQueue(ClientSoakTest.DIVERTED_AD1, ClientSoakTest.DIVERTED_AD1, true);

      session.createQueue(ClientSoakTest.DIVERTED_AD2, ClientSoakTest.DIVERTED_AD2, true);

      session.close();

      sf.close();

      locator.close();

   }

   @Test
   public void testSoakClient() throws Exception {
      final ServerLocator locator = createFactory(isNetty());
      final ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, false);

      ClientProducer producer = session.createProducer(ADDRESS);

      for (int i = 0; i < MIN_MESSAGES_ON_QUEUE; i++) {
         ClientMessage msg = session.createMessage(true);
         msg.putLongProperty("count", i);
         msg.getBodyBuffer().writeBytes(new byte[10 * 1024]);
         producer.send(msg);

         if (i % 1000 == 0) {
            System.out.println("Sent " + i + " messages");
            session.commit();
         }
      }

      session.commit();

      session.close();
      sf.close();

      Receiver rec1 = new Receiver(createSessionFactory(locator), DIVERTED_AD1.toString());
      Receiver rec2 = new Receiver(createSessionFactory(locator), DIVERTED_AD2.toString());

      Sender send = new Sender(createSessionFactory(locator), ADDRESS.toString(), new Receiver[]{rec1, rec2});

      send.start();
      rec1.start();
      rec2.start();

      long timeEnd = System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1);
      while (timeEnd > System.currentTimeMillis()) {
         if (send.getErrorsCount() != 0 || rec1.getErrorsCount() != 0 || rec2.getErrorsCount() != 0) {
            System.out.println("There are sequence errors in some of the clients, please look at the logs");
            break;
         }
         Thread.sleep(10000);
      }

      send.setRunning(false);
      rec1.setRunning(false);
      rec2.setRunning(false);

      send.join();
      rec1.join();
      rec2.join();

      assertEquals(0, send.getErrorsCount());
      assertEquals(0, rec1.getErrorsCount());
      assertEquals(0, rec2.getErrorsCount());

      locator.close();

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
