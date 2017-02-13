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
package org.apache.activemq.artemis.tests.integration.client;

import java.io.File;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.integration.IntegrationTestLogger;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Test;

/**
 * This test will send large messages in page-mode, DLQ then, expiry then, and they should be received fine
 */
public class ExpiryLargeMessageTest extends ActiveMQTestBase {

   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   // Constants -----------------------------------------------------
   final SimpleString EXPIRY = new SimpleString("my-expiry");

   final SimpleString DLQ = new SimpleString("my-DLQ");

   final SimpleString MY_QUEUE = new SimpleString("MY-QUEUE");

   final int messageSize = 10 * 1024;

   // it has to be an even number
   final int numberOfMessages = 50;

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testExpiryMessagesThenDLQ() throws Exception {
      ActiveMQServer server = createServer(true);

      server.getConfiguration().setMessageExpiryScanPeriod(600000);

      AddressSettings setting = new AddressSettings().setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE).setMaxDeliveryAttempts(5).setMaxSizeBytes(50 * 1024).setPageSizeBytes(10 * 1024).setExpiryAddress(EXPIRY).setDeadLetterAddress(DLQ);
      server.getAddressSettingsRepository().addMatch(MY_QUEUE.toString(), setting);
      server.getAddressSettingsRepository().addMatch(EXPIRY.toString(), setting);

      server.start();

      server.createQueue(EXPIRY, RoutingType.ANYCAST, EXPIRY, null, true, false);

      server.createQueue(DLQ, RoutingType.ANYCAST, DLQ, null, true, false);

      server.createQueue(MY_QUEUE, RoutingType.ANYCAST, MY_QUEUE, null, true, false);

      ServerLocator locator = createInVMNonHALocator();

      ClientSessionFactory sf = addSessionFactory(createSessionFactory(locator));

      ClientSession session = sf.createSession(true, true, 0);

      byte[] bufferSample = new byte[messageSize];

      for (int i = 0; i < bufferSample.length; i++) {
         bufferSample[i] = getSamplebyte(i);
      }

      ClientProducer producer = session.createProducer(MY_QUEUE);

      long timeToExpiry = System.currentTimeMillis() + 1000;
      for (int i = 0; i < numberOfMessages; i++) {
         ClientMessage message = session.createMessage(true);

         message.putIntProperty("count", i);

         // Send a few regular messages first, then all is just large messages
         if (i % 2 == 0) {
            message.putBooleanProperty("tst-large", false);
            message.getBodyBuffer().writeBytes(bufferSample);
         } else {
            message.putBooleanProperty("tst-large", true);
            message.setBodyInputStream(createFakeLargeStream(messageSize));
         }

         message.setExpiration(timeToExpiry);

         producer.send(message);
      }

      session.close();

      server.stop();
      server.start();

      Queue queueExpiry = server.locateQueue(EXPIRY);
      Queue myQueue = server.locateQueue(MY_QUEUE);

      sf = createSessionFactory(locator);

      Thread.sleep(1500);

      long timeout = System.currentTimeMillis() + 5000;
      while (timeout > System.currentTimeMillis() && getMessageCount(queueExpiry) != numberOfMessages) {
         // What the Expiry Scan would be doing
         myQueue.expireReferences();
         Thread.sleep(50);
      }

      assertEquals(50, getMessageCount(queueExpiry));

      session = sf.createSession(false, false);

      ClientConsumer cons = session.createConsumer(EXPIRY);
      session.start();

      // Consume half of the messages to make sure all the messages are paging (on the second try)
      for (int i = 0; i < numberOfMessages / 2; i++) {
         ClientMessage msg = cons.receive(5000);
         assertNotNull(msg);
         msg.acknowledge();
      }

      session.commit();

      cons.close();

      for (int rep = 0; rep < 6; rep++) {
         cons = session.createConsumer(EXPIRY);
         session.start();

         log.info("Trying " + rep);
         for (int i = 0; i < numberOfMessages / 2; i++) {
            ClientMessage message = cons.receive(5000);
            assertNotNull(message);

            if (i % 10 == 0) {
               System.out.println("Received " + i);
            }

            for (int location = 0; location < messageSize; location++) {
               assertEquals(getSamplebyte(location), message.getBodyBuffer().readByte());
            }
            message.acknowledge();
         }

         session.rollback();

         cons.close();

         session.close();
         sf.close();

         if (rep == 0) {
            // restart the server at the first try
            server.stop();
            server.start();
         }

         sf = createSessionFactory(locator);
         session = sf.createSession(false, false);
         session.start();
      }

      cons = session.createConsumer(EXPIRY);
      session.start();
      assertNull(cons.receiveImmediate());

      cons.close();

      session.close();
      sf.close();

      for (int rep = 0; rep < 2; rep++) {
         sf = createSessionFactory(locator);

         session = sf.createSession(false, false);

         cons = session.createConsumer(DLQ);

         session.start();

         for (int i = 0; i < numberOfMessages / 2; i++) {
            ClientMessage message = cons.receive(5000);
            assertNotNull(message);

            if (i % 10 == 0) {
               System.out.println("Received " + i);
            }

            for (int location = 0; location < messageSize; location++) {
               assertEquals(getSamplebyte(location), message.getBodyBuffer().readByte());
            }
            message.acknowledge();
         }
         if (rep == 0) {
            session.rollback();
            session.close();
            sf.close();
            server.stop();
            server.start();
         }
      }

      session.commit();

      assertNull(cons.receiveImmediate());

      session.close();
      sf.close();
      locator.close();

      validateNoFilesOnLargeDir();
   }

   /**
    * Tests if the system would still couple with old data where the LargeMessage was linked to its previous copy
    *
    * @throws Exception
    */
   @Test
   public void testCompatilityWithLinks() throws Exception {
      ActiveMQServer server = createServer(true);

      server.getConfiguration().setMessageExpiryScanPeriod(600000);

      AddressSettings setting = new AddressSettings().setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE).setMaxDeliveryAttempts(5).setMaxSizeBytes(-1).setPageSizeBytes(10 * 1024).setExpiryAddress(EXPIRY).setDeadLetterAddress(DLQ);
      server.getAddressSettingsRepository().addMatch(MY_QUEUE.toString(), setting);
      server.getAddressSettingsRepository().addMatch(EXPIRY.toString(), setting);

      server.start();

      server.createQueue(EXPIRY, RoutingType.ANYCAST, EXPIRY, null, true, false);

      server.createQueue(DLQ, RoutingType.ANYCAST, DLQ, null, true, false);

      server.createQueue(MY_QUEUE, RoutingType.ANYCAST, MY_QUEUE, null, true, false);

      ServerLocator locator = createInVMNonHALocator();

      ClientSessionFactory sf = addSessionFactory(createSessionFactory(locator));

      ClientSession session = sf.createSession(true, true, 0);

      byte[] bufferSample = new byte[messageSize];

      for (int i = 0; i < bufferSample.length; i++) {
         bufferSample[i] = getSamplebyte(i);
      }

      ClientProducer producer = session.createProducer(MY_QUEUE);

      long timeToExpiry = System.currentTimeMillis() + 1000;
      for (int i = 0; i < numberOfMessages; i++) {
         ClientMessage message = session.createMessage(true);

         message.putIntProperty("count", i);

         // Everything is going to be a large message
         message.putBooleanProperty("tst-large", true);
         message.setBodyInputStream(createFakeLargeStream(messageSize));

         message.setExpiration(timeToExpiry);

         producer.send(message);
      }

      server.stop();
      server.start();

      sf = createSessionFactory(locator);

      session = sf.createSession(true, true, 0);
      session.start();

      Thread.sleep(1500);

      ClientConsumer cons = session.createConsumer(MY_QUEUE);
      assertNull(cons.receive(1000));

      session.close();

      session = sf.createSession(false, false);

      cons = session.createConsumer(EXPIRY);
      session.start();

      ClientMessage msg = cons.receive(5000);
      assertNotNull(msg);
      msg.acknowledge();
      session.rollback();

      server.stop();

      // rename the file, simulating old behaviour
      long messageID = msg.getMessageID();
      long oldID = msg.getLongProperty(Message.HDR_ORIG_MESSAGE_ID);

      File largeMessagesFileDir = new File(getLargeMessagesDir());
      File oldFile = new File(largeMessagesFileDir, oldID + ".msg");
      File currentFile = new File(largeMessagesFileDir, messageID + ".msg");
      currentFile.renameTo(oldFile);

      server.start();

      sf = createSessionFactory(locator);

      session = sf.createSession(true, true, 0);
      session.start();

      cons = session.createConsumer(EXPIRY);

      for (int i = 0; i < numberOfMessages; i++) {
         ClientMessage message = cons.receive(5000);
         assertNotNull(message);

         if (i % 10 == 0) {
            System.out.println("Received " + i);
         }

         for (int location = 0; location < messageSize; location++) {
            assertEquals(getSamplebyte(location), message.getBodyBuffer().readByte());
         }
         message.acknowledge();
      }

      session.commit();

      session.close();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
