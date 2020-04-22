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
package org.apache.activemq.artemis.tests.integration.clientcrash;

import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.jms.client.ActiveMQTextMessage;
import org.apache.activemq.artemis.tests.util.SpawnedVMCheck;
import org.apache.activemq.artemis.utils.SpawnedVMSupport;
import org.jboss.logging.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * A test that makes sure that an ActiveMQ Artemis server cleans up the associated
 * resources when one of its client crashes.
 */
public class ClientCrashTest extends ClientTestBase {

   private static final Logger log = Logger.getLogger(ClientCrashTest.class);

   @Rule
   public SpawnedVMCheck spawnedVMCheck = new SpawnedVMCheck();

   // using short values so this test can run fast
   static final int PING_PERIOD = 100;

   // using short values so this test can run fast
   static final int CONNECTION_TTL = 1000;

   // Constants -----------------------------------------------------

   public static final SimpleString QUEUE = new SimpleString("ClientCrashTestQueue");
   public static final SimpleString QUEUE2 = new SimpleString("ClientCrashTestQueue2");

   public static final String MESSAGE_TEXT_FROM_SERVER = "ClientCrashTest from server";

   public static final String MESSAGE_TEXT_FROM_CLIENT = "ClientCrashTest from client";

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private ClientSessionFactory sf;

   private ServerLocator locator;

   Process p;

   @After
   @Override
   public void tearDown() throws Exception {
      super.tearDown();
      if (p != null) {
         long timeout = System.currentTimeMillis() + 5000;
         while (timeout > System.currentTimeMillis() && p.isAlive()) {
            p.destroy();
         }
      }
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testCrashClient() throws Exception {
      assertActiveConnections(1);

      ClientSession session = sf.createSession(false, true, true);
      session.createQueue(new QueueConfiguration(ClientCrashTest.QUEUE).setDurable(false));

      // spawn a JVM that creates a Core client, which sends a message
      // It has to be spawned after the queue was created.
      // if the client is too fast you race the send before the queue was created, missing a message
      p = SpawnedVMSupport.spawnVM(CrashClient.class.getName());

      Assert.assertTrue(p.waitFor(1, TimeUnit.MINUTES));

      Assert.assertEquals(CrashClient.OK, p.exitValue());

      ClientConsumer consumer = session.createConsumer(ClientCrashTest.QUEUE);
      ClientProducer producer = session.createProducer(ClientCrashTest.QUEUE);

      session.start();

      // receive a message from the queue
      Message messageFromClient = consumer.receive(5000);
      assertNotNull("no message received", messageFromClient);
      assertEquals(ClientCrashTest.MESSAGE_TEXT_FROM_CLIENT, messageFromClient.getBodyBuffer().readString());

      assertActiveConnections( 1); // One local and one from the other vm
      assertActiveSession(1);

      ClientMessage message = session.createMessage(ActiveMQTextMessage.TYPE, false, 0, System.currentTimeMillis(), (byte) 1);
      message.getBodyBuffer().writeString(ClientCrashTest.MESSAGE_TEXT_FROM_SERVER);
      producer.send(message);

      assertEquals(9, p.exitValue());

      long timeout = ClientCrashTest.CONNECTION_TTL + ClientCrashTest.PING_PERIOD + 10000;

      assertActiveConnections(1, timeout);
      assertActiveSession(1, timeout);

      session.close();

      // the crash must have been detected and the resources cleaned up
      assertActiveConnections(1);
      assertActiveSession(0);
   }

   @Test
   public void testCrashClient2() throws Exception {
      // set the redelivery delay to avoid an attempt to redeliver the message to the dead client
      AddressSettings addressSettings = new AddressSettings().setRedeliveryDelay(ClientCrashTest.CONNECTION_TTL + ClientCrashTest.PING_PERIOD);
      server.getAddressSettingsRepository().addMatch(ClientCrashTest.QUEUE2.toString(), addressSettings);

      assertActiveConnections(1);

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(new QueueConfiguration(ClientCrashTest.QUEUE2).setDurable(false));

      // spawn a JVM that creates a Core client, which sends a message
      p = SpawnedVMSupport.spawnVM(CrashClient2.class.getName());

      ClientCrashTest.log.debug("waiting for the client VM to crash ...");
      Assert.assertTrue(p.waitFor(1, TimeUnit.MINUTES));

      assertEquals(CrashClient2.OK, p.exitValue());

      long timeout = ClientCrashTest.CONNECTION_TTL + ClientCrashTest.PING_PERIOD + 10000L;

      assertActiveConnections(1, timeout);
      assertActiveSession(1, timeout);

      ClientConsumer consumer = session.createConsumer(ClientCrashTest.QUEUE2);

      session.start();

      // receive a message from the queue
      ClientMessage messageFromClient = consumer.receive(timeout);
      assertNotNull("no message received", messageFromClient);
      assertEquals(ClientCrashTest.MESSAGE_TEXT_FROM_CLIENT, messageFromClient.getBodyBuffer().readString());

      assertEquals("delivery count", 2, messageFromClient.getDeliveryCount());

      consumer.close();
      session.close();
   }

   // Package protected ---------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      locator = createNettyNonHALocator();
      addServerLocator(locator);
      locator.setClientFailureCheckPeriod(ClientCrashTest.PING_PERIOD);
      locator.setConnectionTTL(ClientCrashTest.CONNECTION_TTL);
      sf = createSessionFactory(locator);
   }
}
