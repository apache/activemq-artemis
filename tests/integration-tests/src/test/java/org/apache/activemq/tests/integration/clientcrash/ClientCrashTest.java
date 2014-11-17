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
package org.apache.activemq.tests.integration.clientcrash;
import org.junit.Before;

import org.junit.Test;

import org.junit.Assert;

import org.apache.activemq.api.core.Message;
import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.api.core.client.ClientConsumer;
import org.apache.activemq.api.core.client.ClientMessage;
import org.apache.activemq.api.core.client.ClientProducer;
import org.apache.activemq.api.core.client.ClientSession;
import org.apache.activemq.api.core.client.ClientSessionFactory;
import org.apache.activemq.api.core.client.ServerLocator;
import org.apache.activemq.core.settings.impl.AddressSettings;
import org.apache.activemq.jms.client.HornetQTextMessage;
import org.apache.activemq.tests.integration.IntegrationTestLogger;
import org.apache.activemq.tests.util.SpawnedVMSupport;

/**
 * A test that makes sure that a HornetQ server cleans up the associated
 * resources when one of its client crashes.
 *
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @version <tt>$Revision: 4032 $</tt>
 */
public class ClientCrashTest extends ClientTestBase
{
   static final int PING_PERIOD = 2000;

   static final int CONNECTION_TTL = 6000;

   // Constants -----------------------------------------------------

   public static final SimpleString QUEUE = new SimpleString("ClientCrashTestQueue");
   public static final SimpleString QUEUE2 = new SimpleString("ClientCrashTestQueue2");

   public static final String MESSAGE_TEXT_FROM_SERVER = "ClientCrashTest from server";

   public static final String MESSAGE_TEXT_FROM_CLIENT = "ClientCrashTest from client";

   // Static --------------------------------------------------------

   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   // Attributes ----------------------------------------------------

   private ClientSessionFactory sf;

   private ServerLocator locator;

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testCrashClient() throws Exception
   {
      assertActiveConnections(1);

      // spawn a JVM that creates a Core client, which sends a message
      Process p = SpawnedVMSupport.spawnVM(CrashClient.class.getName());

      ClientSession session = sf.createSession(false, true, true);
      session.createQueue(ClientCrashTest.QUEUE, ClientCrashTest.QUEUE, null, false);
      ClientConsumer consumer = session.createConsumer(ClientCrashTest.QUEUE);
      ClientProducer producer = session.createProducer(ClientCrashTest.QUEUE);

      session.start();

      // receive a message from the queue
      Message messageFromClient = consumer.receive(500000);
      Assert.assertNotNull("no message received", messageFromClient);
      Assert.assertEquals(ClientCrashTest.MESSAGE_TEXT_FROM_CLIENT, messageFromClient.getBodyBuffer().readString());

      assertActiveConnections(1 + 1); // One local and one from the other vm
      assertActiveSession(1 + 1);

      ClientMessage message = session.createMessage(HornetQTextMessage.TYPE,
                                                          false,
                                                          0,
                                                          System.currentTimeMillis(),
                                                          (byte)1);
      message.getBodyBuffer().writeString(ClientCrashTest.MESSAGE_TEXT_FROM_SERVER);
      producer.send(message);

      ClientCrashTest.log.debug("waiting for the client VM to crash ...");
      p.waitFor();

      Assert.assertEquals(9, p.exitValue());

      System.out.println("VM Exited");

      long timeout = ClientCrashTest.CONNECTION_TTL + ClientCrashTest.PING_PERIOD + 2000;

      assertActiveConnections(1, timeout);
      assertActiveSession(1, timeout);

      session.close();

      // the crash must have been detected and the resources cleaned up
      assertActiveConnections(1);
      assertActiveSession(0);
   }

   @Test
   public void testCrashClient2() throws Exception
   {
      // set the redelivery delay to avoid an attempt to redeliver the message to the dead client
      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setRedeliveryDelay(ClientCrashTest.CONNECTION_TTL + ClientCrashTest.PING_PERIOD);
      server.getAddressSettingsRepository().addMatch(ClientCrashTest.QUEUE2.toString(), addressSettings);

      assertActiveConnections(1);

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(ClientCrashTest.QUEUE2, ClientCrashTest.QUEUE2, null, false);

      // spawn a JVM that creates a Core client, which sends a message
      Process p = SpawnedVMSupport.spawnVM(CrashClient2.class.getName());

      ClientCrashTest.log.debug("waiting for the client VM to crash ...");
      p.waitFor();

      Assert.assertEquals(9, p.exitValue());

      System.out.println("VM Exited");

      long timeout = ClientCrashTest.CONNECTION_TTL + ClientCrashTest.PING_PERIOD + 2000;

      assertActiveConnections(1, timeout);
      assertActiveSession(1, timeout);

      ClientConsumer consumer = session.createConsumer(ClientCrashTest.QUEUE2);

      session.start();

      // receive a message from the queue
      ClientMessage messageFromClient = consumer.receive(10000);
      Assert.assertNotNull("no message received", messageFromClient);
      Assert.assertEquals(ClientCrashTest.MESSAGE_TEXT_FROM_CLIENT, messageFromClient.getBodyBuffer().readString());

      assertEquals("delivery count", 2, messageFromClient.getDeliveryCount());

      consumer.close();
      session.close();
   }

   // Package protected ---------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      locator = createNettyNonHALocator();
      addServerLocator(locator);
      locator.setClientFailureCheckPeriod(ClientCrashTest.PING_PERIOD);
      locator.setConnectionTTL(ClientCrashTest.CONNECTION_TTL);
      sf = createSessionFactory(locator);
   }
}
