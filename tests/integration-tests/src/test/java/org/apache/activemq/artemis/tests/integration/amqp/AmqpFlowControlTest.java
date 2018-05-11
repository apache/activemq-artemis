/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.amqp;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.MessageProducer;
import javax.jms.ResourceAllocationException;
import javax.jms.Session;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSender;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.junit.Assert;
import org.junit.Test;

public class AmqpFlowControlTest extends JMSClientTestSupport {

   private static final long MAX_SIZE_BYTES = 1 * 1024 * 1024;
   private static final long MAX_SIZE_BYTES_REJECT_THRESHOLD = 2 * 1024 * 1024;

   private String singleCreditAcceptorURI = new String("tcp://localhost:" + (AMQP_PORT + 8));
   private int messagesSent;

   @Override
   public void setUp() throws Exception {
      super.setUp();

      messagesSent = 0;
   }

   @Override
   protected void addAdditionalAcceptors(ActiveMQServer server) throws Exception {
      server.getConfiguration().addAcceptorConfiguration("flow", singleCreditAcceptorURI + "?protocols=AMQP;useEpoll=false;amqpCredits=1;amqpLowCredits=1");
   }

   @Override
   protected void configureAddressPolicy(ActiveMQServer server) {
      // For BLOCK tests
      AddressSettings addressSettings = server.getAddressSettingsRepository().getMatch("#");
      addressSettings.setAddressFullMessagePolicy(AddressFullMessagePolicy.BLOCK);
      addressSettings.setMaxSizeBytes(MAX_SIZE_BYTES);
      addressSettings.setMaxSizeBytesRejectThreshold(MAX_SIZE_BYTES_REJECT_THRESHOLD);
      server.getAddressSettingsRepository().addMatch("#", addressSettings);
   }

   @Test(timeout = 60000)
   public void testCreditsAreAllocatedOnceOnLinkCreated() throws Exception {
      AmqpClient client = createAmqpClient(new URI(singleCreditAcceptorURI));
      AmqpConnection connection = addConnection(client.connect());

      try {
         AmqpSession session = connection.createSession();
         AmqpSender sender = session.createSender(getQueueName());
         assertEquals("Should only be issued one credit", 1, sender.getSender().getCredit());
      } finally {
         connection.close();
      }
   }

   @Test(timeout = 60000)
   public void testCreditsAreNotAllocatedWhenAddressIsFull() throws Exception {
      AmqpClient client = createAmqpClient(new URI(singleCreditAcceptorURI));
      AmqpConnection connection = addConnection(client.connect());

      try {
         AmqpSession session = connection.createSession();
         AmqpSender sender = session.createSender(getQueueName());

         // Use blocking send to ensure buffered messages do not interfere with credit.
         sender.setSendTimeout(-1);
         sendUntilFull(sender);

         // This should be -1. A single message is buffered in the client, and 0 credit has been allocated.
         assertTrue(sender.getSender().getCredit() == -1);

         long addressSize = server.getPagingManager().getPageStore(new SimpleString(getQueueName())).getAddressSize();
         assertTrue(addressSize >= MAX_SIZE_BYTES && addressSize <= MAX_SIZE_BYTES_REJECT_THRESHOLD);
      } finally {
         connection.close();
      }
   }

   @Test(timeout = 60000)
   public void testAddressIsBlockedForOtherProdudersWhenFull() throws Exception {
      Connection connection = createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Destination d = session.createQueue(getQueueName());
      MessageProducer p = session.createProducer(d);

      fillAddress(getQueueName());

      Exception e = null;
      try {
         p.send(session.createBytesMessage());
      } catch (ResourceAllocationException rae) {
         e = rae;
      }
      assertTrue(e instanceof ResourceAllocationException);
      assertTrue(e.getMessage().contains("resource-limit-exceeded"));

      long addressSize = server.getPagingManager().getPageStore(new SimpleString(getQueueName())).getAddressSize();
      assertTrue(addressSize >= MAX_SIZE_BYTES_REJECT_THRESHOLD);
   }

   @Test(timeout = 60000)
   public void testCreditsAreRefreshedWhenAddressIsUnblocked() throws Exception {
      fillAddress(getQueueName());

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());

      try {
         AmqpSession session = connection.createSession();
         AmqpSender sender = session.createSender(getQueueName());

         // Wait for a potential flow frame.
         Thread.sleep(500);
         assertEquals(0, sender.getSender().getCredit());

         // Empty Address except for 1 message used later.
         AmqpReceiver receiver = session.createReceiver(getQueueName());
         receiver.flow(100);

         AmqpMessage m;
         for (int i = 0; i < messagesSent - 1; i++) {
            m = receiver.receive(5000, TimeUnit.MILLISECONDS);
            m.accept();
         }

         // Wait for address to unblock and flow frame to arrive
         Thread.sleep(500);

         assertTrue(sender.getSender().getCredit() >= 0);
      } finally {
         connection.close();
      }
   }

   @Test(timeout = 60000)
   public void testNewLinkAttachAreNotAllocatedCreditsWhenAddressIsBlocked() throws Exception {
      fillAddress(getQueueName());

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());

      try {
         AmqpSession session = connection.createSession();
         AmqpSender sender = session.createSender(getQueueName());

         // Wait for a potential flow frame.
         Thread.sleep(1000);
         assertEquals(0, sender.getSender().getCredit());
      } finally {
         connection.close();
      }
   }

   @Test(timeout = 60000)
   public void testTxIsRolledBackOnRejectedPreSettledMessage() throws Throwable {

      // Create the link attach before filling the address to ensure the link is allocated credit.
      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());

      AmqpSession session = connection.createSession();
      AmqpSender sender = session.createSender(getQueueName());
      sender.setPresettle(true);

      fillAddress(getQueueName());

      final AmqpMessage message = new AmqpMessage();
      byte[] payload = new byte[50 * 1024];
      message.setBytes(payload);

      Exception expectedException = null;
      try {
         session.begin();
         sender.send(message);
         session.commit();
      } catch (Exception e) {
         expectedException = e;
      } finally {
         connection.close();
      }

      assertNotNull(expectedException);
      assertTrue(expectedException.getMessage().contains("resource-limit-exceeded"));
      assertTrue(expectedException.getMessage().contains("Address is full: " + getQueueName()));
   }

   /*
    * Fills an address.  Careful when using this method.  Only use when rejected messages are switched on.
    */
   private void fillAddress(String address) throws Exception {
      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      Exception exception = null;
      try {
         AmqpSession session = connection.createSession();
         AmqpSender sender = session.createSender(address);
         sendUntilFull(sender);
      } catch (Exception e) {
         exception = e;
      } finally {
         connection.close();
      }

      // Should receive a rejected error
      assertNotNull(exception);
      assertTrue(exception.getMessage().contains("amqp:resource-limit-exceeded"));
   }

   private void sendUntilFull(final AmqpSender sender) throws Exception {
      final AmqpMessage message = new AmqpMessage();
      byte[] payload = new byte[50 * 1024];
      message.setBytes(payload);

      final int maxMessages = 50;
      final AtomicInteger sentMessages = new AtomicInteger(0);
      final Exception[] errors = new Exception[1];
      final CountDownLatch timeout = new CountDownLatch(1);

      Runnable sendMessages = new Runnable() {
         @Override
         public void run() {
            try {
               for (int i = 0; i < maxMessages; i++) {
                  sender.send(message);
                  System.out.println("Sent " + i);
                  sentMessages.getAndIncrement();
               }
               timeout.countDown();
            } catch (IOException e) {
               errors[0] = e;
            }
         }
      };

      Thread t = new Thread(sendMessages);

      try {
         t.start();

         timeout.await(1, TimeUnit.SECONDS);

         messagesSent = sentMessages.get();
         if (errors[0] != null) {
            throw errors[0];
         }
      } finally {
         t.interrupt();
         t.join(1000);
         Assert.assertFalse(t.isAlive());
      }
   }
}
