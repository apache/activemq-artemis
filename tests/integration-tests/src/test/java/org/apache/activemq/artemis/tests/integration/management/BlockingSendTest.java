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
package org.apache.activemq.artemis.tests.integration.management;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.api.core.management.ActiveMQServerControl;
import org.apache.activemq.artemis.api.core.management.AddressControl;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.junit.Wait;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.util.concurrent.atomic.AtomicBoolean;

public class BlockingSendTest extends ManagementTestBase {

   private static final String BLOCK_ADDRESS1 = "BLOCK.ADDRESS.1";
   private static final String BLOCK_ADDRESS2 = "BLOCK.ADDRESS.2";
   private static final String NORMAL_ADDRESS1 = "NORMAL.ADDRESS.1";
   private static final String NORMAL_ADDRESS2 = "NORMAL.ADDRESS.2";
   private static final String BLOCK_ADDRESS_ALL = "BLOCK.ADDRESS.#";
   private static final String NORMAL_ADDRESS_ALL = "NORMAL.ADDRESS.#";

   private static final String KEY_PRODUCER_ID = "producer_id";

   private ActiveMQServer server;

   Queue blockQueue1;
   Queue blockQueue2;
   Queue normalQueue1;
   Queue normalQueue2;

   TestProducer[] blockProducers1 = null;
   TestProducer[] blockProducers2 = null;
   TestProducer[] normalProducers1 = null;
   TestProducer[] normalProducers2 = null;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      leakCheckRule.disable();//tmp disable

      Configuration config = createDefaultConfig(true).setJMXManagementEnabled(true);
      server = createServer(true, config);
      server.setMBeanServer(mbeanServer);
      server.start();

      blockQueue1 = server.createQueue(new SimpleString(BLOCK_ADDRESS1), RoutingType.ANYCAST, new SimpleString(BLOCK_ADDRESS1), null, true, false);
      blockProducers1 = createTestProducers(BLOCK_ADDRESS1);
      blockQueue2 = server.createQueue(new SimpleString(BLOCK_ADDRESS2), RoutingType.ANYCAST, new SimpleString(BLOCK_ADDRESS2), null, true, false);
      blockProducers2 = createTestProducers(BLOCK_ADDRESS2);
      normalQueue1 = server.createQueue(new SimpleString(NORMAL_ADDRESS1), RoutingType.ANYCAST, new SimpleString(NORMAL_ADDRESS1), null, true, false);
      normalProducers1 = createTestProducers(NORMAL_ADDRESS1);
      normalQueue2 = server.createQueue(new SimpleString(NORMAL_ADDRESS2), RoutingType.ANYCAST, new SimpleString(NORMAL_ADDRESS2), null, true, false);
      normalProducers2 = createTestProducers(NORMAL_ADDRESS2);

      for (TestProducer producer : blockProducers1) {
         producer.start();
      }
      for (TestProducer producer : blockProducers2) {
         producer.start();
      }
      for (TestProducer producer : normalProducers1) {
         producer.start();
      }
      for (TestProducer producer : normalProducers2) {
         producer.start();
      }
   }

   @Override
   @After
   public void tearDown() throws Exception {
      stopAllProducers();
      server.destroyQueue(blockQueue1.getName());
      server.destroyQueue(blockQueue2.getName());
      server.destroyQueue(normalQueue1.getName());
      server.destroyQueue(normalQueue2.getName());
      server.stop();
   }

   private void checkProducersNotBlocked(TestProducer[]... producers) throws Exception {
      checkProducerBlockStatus(false, producers);
   }

   private void checkProducersBlocked(TestProducer[]... producers) throws Exception {
      checkProducerBlockStatus(true, producers);
   }

   private void checkProducerBlockStatus(boolean expected, TestProducer[]... producers) throws Exception {
      for (TestProducer[] producerGroup : producers) {
         for (TestProducer producer : producerGroup) {
            assertTrue("producer " + (expected ? "should" : "shouldn't") + " be blocked: " + producer, producer.checkSendingBlocked(expected, 1000));
         }
      }
   }

   @Test
   public void testBlockSendingOnOneAddress() throws Exception {

      checkProducersNotBlocked(blockProducers1);

      server.blockSendingOnAddresses(true, BLOCK_ADDRESS1);

      checkProducersBlocked(blockProducers1);

      checkQueueEmpty(blockQueue1);

      checkProducersNotBlocked(blockProducers2, normalProducers1, normalProducers2);

      server.blockSendingOnAddresses(true, BLOCK_ADDRESS2);

      checkProducersBlocked(blockProducers1, blockProducers2);
      checkQueueEmpty(blockQueue2);

      checkProducersNotBlocked(normalProducers1, normalProducers2);

      server.blockSendingOnAddresses(true, NORMAL_ADDRESS1);

      checkProducersBlocked(blockProducers1, blockProducers2, normalProducers1);

      checkQueueEmpty(normalQueue1);

      checkProducersNotBlocked(normalProducers2);

      server.blockSendingOnAddresses(true, NORMAL_ADDRESS2);

      checkProducersBlocked(blockProducers1, blockProducers2, normalProducers1, normalProducers2);
      checkQueueEmpty(normalQueue2);

      //unblocking
      server.blockSendingOnAddresses(false, BLOCK_ADDRESS1);

      checkProducersNotBlocked(blockProducers1);
      checkProducersBlocked(blockProducers2, normalProducers1, normalProducers2);

      server.blockSendingOnAddresses(false, BLOCK_ADDRESS2);

      checkProducersNotBlocked(blockProducers1, blockProducers2);
      checkProducersBlocked(normalProducers1, normalProducers2);

      server.blockSendingOnAddresses(false, NORMAL_ADDRESS1);

      checkProducersNotBlocked(blockProducers1, blockProducers2, normalProducers1);
      checkProducersBlocked(normalProducers2);

      server.blockSendingOnAddresses(false, NORMAL_ADDRESS2);

      checkProducersNotBlocked(blockProducers1, blockProducers2, normalProducers1, normalProducers2);
      assertNull(server.getAddressBlockRules());
   }

   @Test
   public void testBlockSendingOnWildcardAddress() throws Exception {

      server.blockSendingOnAddresses(true, BLOCK_ADDRESS_ALL);

      checkProducersBlocked(blockProducers1, blockProducers2);
      checkQueueEmpty(blockQueue1);
      checkQueueEmpty(blockQueue2);

      checkProducersNotBlocked(normalProducers1, normalProducers2);

      server.blockSendingOnAddresses(true, NORMAL_ADDRESS_ALL);

      checkProducersBlocked(blockProducers1, blockProducers2, normalProducers1, normalProducers2);
      checkQueueEmpty(normalQueue1);
      checkQueueEmpty(normalQueue2);

      server.blockSendingOnAddresses(false, BLOCK_ADDRESS_ALL);

      checkProducersNotBlocked(blockProducers1, blockProducers2);
      checkProducersBlocked(normalProducers1, normalProducers2);

      server.blockSendingOnAddresses(false, NORMAL_ADDRESS_ALL);

      checkProducersNotBlocked(blockProducers1, blockProducers2, normalProducers1, normalProducers2);

      server.blockSendingOnAddresses(true, "#");

      checkProducersBlocked(blockProducers1, blockProducers2, normalProducers1, normalProducers2);
      checkQueueEmpty(blockQueue1);
      checkQueueEmpty(blockQueue2);
      checkQueueEmpty(normalQueue1);
      checkQueueEmpty(normalQueue2);

      server.blockSendingOnAddresses(false, "#");

      checkProducersNotBlocked(blockProducers1, blockProducers2, normalProducers1, normalProducers2);
      assertNull(server.getAddressBlockRules());
   }

   @Test
   public void testBlockSendingOnMultiAddresses() throws Exception {

      server.blockSendingOnAddresses(true, BLOCK_ADDRESS1, BLOCK_ADDRESS2, NORMAL_ADDRESS1);

      checkProducersBlocked(blockProducers1, blockProducers2, normalProducers1);
      checkQueueEmpty(normalQueue1);
      checkProducersNotBlocked(normalProducers2);

      server.blockSendingOnAddresses(false, BLOCK_ADDRESS_ALL, NORMAL_ADDRESS1);

      checkProducersNotBlocked(blockProducers1, blockProducers2, normalProducers1, normalProducers2);

      server.blockSendingOnAddresses(true, NORMAL_ADDRESS_ALL, BLOCK_ADDRESS_ALL);

      checkProducersBlocked(blockProducers1, blockProducers2, normalProducers1, normalProducers2);
      checkQueueEmpty(blockQueue1);
      checkQueueEmpty(blockQueue2);
      checkQueueEmpty(normalQueue1);
      checkQueueEmpty(normalQueue2);

      server.blockSendingOnAddresses(false, NORMAL_ADDRESS_ALL, BLOCK_ADDRESS_ALL);

      checkProducersNotBlocked(blockProducers1, blockProducers2, normalProducers1, normalProducers2);
      assertNull(server.getAddressBlockRules());
   }

   @Test
   public void testManagementAPI() throws Exception {
      //we just need to verify that management api can reach the actual code.
      stopAllProducers();

      PostOffice po = server.getPostOffice();

      AddressControl addressControl1 = ManagementControlHelper.createAddressControl(new SimpleString(BLOCK_ADDRESS1), mbeanServer);

      addressControl1.blockSending(true);
      assertTrue(po.isAddressBlocked(new SimpleString(BLOCK_ADDRESS1)));

      addressControl1.blockSending(false);
      assertFalse(po.isAddressBlocked(new SimpleString(BLOCK_ADDRESS1)));

      ActiveMQServerControl serverControl = ManagementControlHelper.createActiveMQServerControl(mbeanServer);

      serverControl.blockSendingOnAddresses(true, BLOCK_ADDRESS1, BLOCK_ADDRESS2, NORMAL_ADDRESS1, NORMAL_ADDRESS2);
      assertTrue(po.isAddressBlocked(new SimpleString(BLOCK_ADDRESS1)));
      assertTrue(po.isAddressBlocked(new SimpleString(BLOCK_ADDRESS2)));
      assertTrue(po.isAddressBlocked(new SimpleString(NORMAL_ADDRESS1)));
      assertTrue(po.isAddressBlocked(new SimpleString(NORMAL_ADDRESS2)));

      serverControl.blockSendingOnAddresses(false, BLOCK_ADDRESS_ALL);
      assertFalse(po.isAddressBlocked(new SimpleString(BLOCK_ADDRESS1)));
      assertFalse(po.isAddressBlocked(new SimpleString(BLOCK_ADDRESS2)));
      assertTrue(po.isAddressBlocked(new SimpleString(NORMAL_ADDRESS1)));
      assertTrue(po.isAddressBlocked(new SimpleString(NORMAL_ADDRESS2)));

      serverControl.blockSendingOnAddresses(false, NORMAL_ADDRESS1);
      assertFalse(po.isAddressBlocked(new SimpleString(BLOCK_ADDRESS1)));
      assertFalse(po.isAddressBlocked(new SimpleString(BLOCK_ADDRESS2)));
      assertFalse(po.isAddressBlocked(new SimpleString(NORMAL_ADDRESS1)));
      assertTrue(po.isAddressBlocked(new SimpleString(NORMAL_ADDRESS2)));

      serverControl.blockSendingOnAddresses(false, NORMAL_ADDRESS2);
      assertFalse(po.isAddressBlocked(new SimpleString(BLOCK_ADDRESS1)));
      assertFalse(po.isAddressBlocked(new SimpleString(BLOCK_ADDRESS2)));
      assertFalse(po.isAddressBlocked(new SimpleString(NORMAL_ADDRESS1)));
      assertFalse(po.isAddressBlocked(new SimpleString(NORMAL_ADDRESS2)));

      assertNull(po.getAddressBlockRules());
   }

   private TestProducer[] createTestProducers(String address) throws Exception {
      //core, jms, openwire, amqp
      TestProducer[] producers = new TestProducer[4];
      producers[0] = new CoreTestProducer(address);
      producers[1] = new JmsTestProducer(address);
      producers[2] = new OpenwireTestProducer(address);
      producers[3] = new AmqpTestProducer(address);
      return producers;
   }

   private void stopAllProducers() {
      for (TestProducer producer : blockProducers1) {
         producer.discard();
      }
      for (TestProducer producer : blockProducers2) {
         producer.discard();
      }
      for (TestProducer producer : normalProducers1) {
         producer.discard();
      }
      for (TestProducer producer : normalProducers2) {
         producer.discard();
      }
   }

   private void checkQueueEmpty(Queue queue) throws Exception {
      queue.deleteAllReferences();
      boolean result = Wait.waitFor(new Wait.Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return queue.getMessageCount() == 0;
         }
      }, 2000, 200);
      assertTrue("queue not empty: " + queue.getMessageCount(), result);
   }

   private abstract class TestProducer extends Thread {

      protected static final long SEND_INTERVAL = 100;

      protected String address;
      protected volatile boolean running = true;
      protected AtomicBoolean blocked = new AtomicBoolean(false);

      TestProducer(String address) throws Exception {
         this.address = address;
      }

      public void discard() {
         running = false;
         try {
            this.join();
         } catch (InterruptedException e) {
         }
         cleanupProducer();
      }

      public abstract void createProducer() throws Exception;
      public abstract void cleanupProducer();
      public abstract void sendMessage() throws Exception;

      public boolean checkSendingBlocked(boolean expected, long timeout) throws Exception {

         if (!running) {
            throw new Exception(this + " producer already stopped");
         }

         boolean result = Wait.waitFor(() -> blocked.get() == expected, timeout);

         return result;
      }

      @Override
      public void run() {
         try {
            createProducer();
         } catch (Throwable e) {
            System.err.println(this + " error creating producer: " + e.getMessage());
            e.printStackTrace();
            return;
         }

         while (running) {
            try {
               sendMessage();
               blocked.set(false);
               try {
                  Thread.sleep(SEND_INTERVAL);
               } catch (InterruptedException e) {
               }
            } catch (Exception e) {
               System.err.println(this + " error sending message: " + e.getMessage());

               if (e.getMessage().contains("Sending is blocked on address")) {
                  blocked.set(true);
                  try {
                     cleanupProducer();
                     createProducer();
                  } catch (Exception e1) {
                     running = false;
                     discard();
                  }
               } else {
                  running = false;
                  discard();
               }
            }
         }
      }
   }

   private class CoreTestProducer extends TestProducer {

      private ServerLocator locator;
      private ClientSessionFactory sf;
      private ClientSession session;
      private ClientProducer producer;

      CoreTestProducer(String address) throws Exception {
         super(address);
      }

      @Override
      public void createProducer() throws Exception {
         locator = createNettyNonHALocator();
         sf = locator.createSessionFactory();
         session = sf.createSession(true, true);
         producer = session.createProducer(address);
      }

      @Override
      public void cleanupProducer() {
         if (producer == null) {
            return;
         }
         try {
            producer.close();
            session.close();
         } catch (ActiveMQException e) {
            System.err.println("failed to close resource: " + e.getMessage());
            e.printStackTrace();
         }
         sf.close();
         locator.close();
      }

      @Override
      public void sendMessage() throws Exception {
         ClientMessage message = session.createMessage(true);
         message.putStringProperty(KEY_PRODUCER_ID, "CoreProducer@" + address);
         producer.send(message);
      }

      @Override
      public String toString() {
         return "CoreProducer@" + address;
      }
   }

   private class JmsTestProducer extends TestProducer {

      private Connection connection;
      private Session session;
      private javax.jms.Queue dest;
      private MessageProducer producer;

      JmsTestProducer(String address) throws Exception {
         super(address);
      }

      @Override
      public void createProducer() throws Exception {
         ConnectionFactory cf = createCF();
         connection = cf.createConnection();
         session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         dest = session.createQueue(address);
         producer = session.createProducer(dest);
      }

      protected ConnectionFactory createCF() {
         return new ActiveMQConnectionFactory("tcp://localhost:61616");
      }

      @Override
      public void cleanupProducer() {
         try {
            producer.close();
            session.close();
            connection.close();
         } catch (JMSException e) {
            System.err.println("failed to close resources: " + e.getMessage());
            e.printStackTrace();
         }
      }

      @Override
      public void sendMessage() throws Exception {
         Message message = session.createMessage();
         message.setStringProperty(KEY_PRODUCER_ID, "JMSProducer@" + address);
         producer.send(message);
      }

      @Override
      public String toString() {
         return "JmsProducer@" + address;
      }
   }

   private class OpenwireTestProducer extends JmsTestProducer {
      private org.apache.activemq.ActiveMQConnectionFactory cf;
      private Connection connection;
      private Session session;
      private javax.jms.Queue dest;
      private MessageProducer producer;

      OpenwireTestProducer(String address) throws Exception {
         super(address);
      }

      @Override
      protected ConnectionFactory createCF() {
         return new org.apache.activemq.ActiveMQConnectionFactory("tcp://localhost:61616");
      }

      @Override
      public String toString() {
         return "OpenwireProducer@" + address;
      }
   }

   private class AmqpTestProducer extends JmsTestProducer {

      AmqpTestProducer(String address) throws Exception {
         super(address);
      }

      @Override
      public String toString() {
         return "AmqpProducer@" + address;
      }

      @Override
      public ConnectionFactory createCF() {
         return new JmsConnectionFactory("amqp://127.0.0.1:61616");
      }
   }
}
