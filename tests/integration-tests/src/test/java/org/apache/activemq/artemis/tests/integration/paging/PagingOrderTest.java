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
package org.apache.activemq.artemis.tests.integration.paging;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.api.jms.JMSFactoryType;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.Bindings;
import org.apache.activemq.artemis.core.postoffice.impl.LocalQueueBinding;
import org.apache.activemq.artemis.core.registry.JndiBindingRegistry;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.RoutingType;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.server.impl.QueueImpl;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.jms.client.ActiveMQJMSConnectionFactory;
import org.apache.activemq.artemis.jms.server.impl.JMSServerManagerImpl;
import org.apache.activemq.artemis.tests.unit.util.InVMNamingContext;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Test;

/**
 * A PagingOrderTest. PagingTest has a lot of tests already. I decided to create a newer one more
 * specialized on Ordering and counters
 */
public class PagingOrderTest extends ActiveMQTestBase {

   private static final int PAGE_MAX = 100 * 1024;

   private static final int PAGE_SIZE = 10 * 1024;

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   static final SimpleString ADDRESS = new SimpleString("SimpleAddress");

   private Connection conn;

   @Test
   public void testOrder1() throws Throwable {
      boolean persistentMessages = true;

      Configuration config = createDefaultInVMConfig().setJournalSyncNonTransactional(false);

      ActiveMQServer server = createServer(true, config, PAGE_SIZE, PAGE_MAX, new HashMap<String, AddressSettings>());

      server.start();

      final int messageSize = 1024;

      final int numberOfMessages = 500;

      ServerLocator locator = createInVMNonHALocator().setClientFailureCheckPeriod(1000).setConnectionTTL(2000).setReconnectAttempts(0).setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true).setConsumerWindowSize(1024 * 1024);

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, false, false);

      server.createAddressInfo(new AddressInfo(ADDRESS, RoutingType.ANYCAST));
      server.createQueue(ADDRESS, RoutingType.ANYCAST, ADDRESS, null, true, false);

      ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

      byte[] body = new byte[messageSize];

      ByteBuffer bb = ByteBuffer.wrap(body);

      for (int j = 1; j <= messageSize; j++) {
         bb.put(getSamplebyte(j));
      }

      for (int i = 0; i < numberOfMessages; i++) {
         ClientMessage message = session.createMessage(persistentMessages);

         ActiveMQBuffer bodyLocal = message.getBodyBuffer();

         bodyLocal.writeBytes(body);

         message.putIntProperty(new SimpleString("id"), i);

         producer.send(message);
         if (i % 1000 == 0) {
            session.commit();
         }
      }

      session.commit();

      session.close();

      session = sf.createSession(true, true, 0);

      session.start();

      ClientConsumer consumer = session.createConsumer(ADDRESS);

      for (int i = 0; i < numberOfMessages / 2; i++) {
         ClientMessage message = consumer.receive(5000);
         assertNotNull(message);
         assertEquals(i, message.getIntProperty("id").intValue());

         if (i < 100) {
            // Do not consume the last one so we could restart
            message.acknowledge();
         }
      }

      session.close();

      sf.close();
      sf = createSessionFactory(locator);

      session = sf.createSession(true, true, 0);

      session.start();

      consumer = session.createConsumer(ADDRESS);

      for (int i = 100; i < numberOfMessages; i++) {
         ClientMessage message = consumer.receive(5000);
         assertNotNull(message);
         assertEquals(i, message.getIntProperty("id").intValue());
         message.acknowledge();
      }

      session.close();
   }

   @Test
   public void testPageCounter() throws Throwable {
      boolean persistentMessages = true;

      Configuration config = createDefaultInVMConfig().setJournalSyncNonTransactional(false);

      ActiveMQServer server = createServer(true, config, PAGE_SIZE, PAGE_MAX, new HashMap<String, AddressSettings>());

      server.start();

      final int messageSize = 1024;

      final int numberOfMessages = 500;

      ServerLocator locator = createInVMNonHALocator().setClientFailureCheckPeriod(1000).setConnectionTTL(2000).setReconnectAttempts(0).setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true).setConsumerWindowSize(1024 * 1024);

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, false, false);

      server.createAddressInfo(new AddressInfo(ADDRESS, RoutingType.ANYCAST));
      Queue q1 = server.createQueue(ADDRESS, RoutingType.ANYCAST, ADDRESS, null, true, false);

      Queue q2 = server.createQueue(ADDRESS, RoutingType.ANYCAST, new SimpleString("inactive"), null, true, false);

      ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

      byte[] body = new byte[messageSize];

      ByteBuffer bb = ByteBuffer.wrap(body);

      for (int j = 1; j <= messageSize; j++) {
         bb.put(getSamplebyte(j));
      }

      final AtomicInteger errors = new AtomicInteger(0);

      Thread t1 = new Thread() {
         @Override
         public void run() {
            try {
               ServerLocator sl = createInVMNonHALocator();
               ClientSessionFactory sf = sl.createSessionFactory();
               ClientSession sess = sf.createSession(true, true, 0);
               sess.start();
               ClientConsumer cons = sess.createConsumer(ADDRESS);
               for (int i = 0; i < numberOfMessages; i++) {
                  ClientMessage msg = cons.receive(5000);
                  assertNotNull(msg);
                  assertEquals(i, msg.getIntProperty("id").intValue());
                  msg.acknowledge();
               }

               assertNull(cons.receiveImmediate());
               sess.close();
               sl.close();
            } catch (Throwable e) {
               e.printStackTrace();
               errors.incrementAndGet();
            }

         }
      };

      t1.start();

      for (int i = 0; i < numberOfMessages; i++) {
         ClientMessage message = session.createMessage(persistentMessages);

         ActiveMQBuffer bodyLocal = message.getBodyBuffer();

         bodyLocal.writeBytes(body);

         message.putIntProperty(new SimpleString("id"), i);

         producer.send(message);
         if (i % 20 == 0) {
            session.commit();
         }
      }

      session.commit();

      t1.join();

      assertEquals(0, errors.get());

      assertEquals(numberOfMessages, getMessageCount(q2));
      assertEquals(numberOfMessages, getMessagesAdded(q2));
      assertEquals(0, getMessageCount(q1));
      assertEquals(numberOfMessages, getMessagesAdded(q1));

      session.close();
      sf.close();
      locator.close();

      server.stop();

      server.start();

      Bindings bindings = server.getPostOffice().getBindingsForAddress(ADDRESS);

      q1 = null;
      q2 = null;

      for (Binding bind : bindings.getBindings()) {
         if (bind instanceof LocalQueueBinding) {
            LocalQueueBinding qb = (LocalQueueBinding) bind;
            if (qb.getQueue().getName().equals(ADDRESS)) {
               q1 = qb.getQueue();
            }

            if (qb.getQueue().getName().equals(new SimpleString("inactive"))) {
               q2 = qb.getQueue();
            }
         }
      }

      assertNotNull(q1);

      assertNotNull(q2);

      assertEquals("q2 msg count", numberOfMessages, getMessageCount(q2));
      assertEquals("q2 msgs added", numberOfMessages, getMessagesAdded(q2));
      assertEquals("q1 msg count", 0, getMessageCount(q1));
      // 0, since nothing was sent to the queue after the server was restarted
      assertEquals("q1 msgs added", 0, getMessagesAdded(q1));

   }

   @Test
   public void testPageCounter2() throws Throwable {
      boolean persistentMessages = true;

      Configuration config = createDefaultInVMConfig().setJournalSyncNonTransactional(false);

      ActiveMQServer server = createServer(true, config, PAGE_SIZE, PAGE_MAX, new HashMap<String, AddressSettings>());

      server.start();

      final int messageSize = 1024;

      final int numberOfMessages = 500;

      ServerLocator locator = createInVMNonHALocator().setClientFailureCheckPeriod(1000).setConnectionTTL(2000).setReconnectAttempts(0).setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true).setConsumerWindowSize(1024 * 1024);

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, false, false);

      server.createAddressInfo(new AddressInfo(ADDRESS, RoutingType.ANYCAST));
      Queue q1 = server.createQueue(ADDRESS, RoutingType.ANYCAST, ADDRESS, null, true, false);

      Queue q2 = server.createQueue(ADDRESS, RoutingType.ANYCAST, new SimpleString("inactive"), null, true, false);

      ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

      byte[] body = new byte[messageSize];

      ByteBuffer bb = ByteBuffer.wrap(body);

      for (int j = 1; j <= messageSize; j++) {
         bb.put(getSamplebyte(j));
      }

      final AtomicInteger errors = new AtomicInteger(0);

      Thread t1 = new Thread() {
         @Override
         public void run() {
            try {
               ServerLocator sl = createInVMNonHALocator();
               ClientSessionFactory sf = sl.createSessionFactory();
               ClientSession sess = sf.createSession(true, true, 0);
               sess.start();
               ClientConsumer cons = sess.createConsumer(ADDRESS);
               for (int i = 0; i < 100; i++) {
                  ClientMessage msg = cons.receive(5000);
                  assertNotNull(msg);
                  assertEquals(i, msg.getIntProperty("id").intValue());
                  msg.acknowledge();
               }
               sess.close();
               sl.close();
            } catch (Throwable e) {
               e.printStackTrace();
               errors.incrementAndGet();
            }

         }
      };

      for (int i = 0; i < numberOfMessages; i++) {
         ClientMessage message = session.createMessage(persistentMessages);

         ActiveMQBuffer bodyLocal = message.getBodyBuffer();

         bodyLocal.writeBytes(body);

         message.putIntProperty(new SimpleString("id"), i);

         producer.send(message);
         if (i % 20 == 0) {
            session.commit();
         }
      }

      session.commit();

      t1.start();
      t1.join();

      assertEquals(0, errors.get());
      long timeout = System.currentTimeMillis() + 10000;
      while (numberOfMessages - 100 != getMessageCount(q1) && System.currentTimeMillis() < timeout) {
         Thread.sleep(500);

      }

      assertEquals(numberOfMessages, getMessageCount(q2));
      assertEquals(numberOfMessages, getMessagesAdded(q2));
      assertEquals(numberOfMessages - 100, getMessageCount(q1));
   }

   @Test
   public void testOrderOverRollback() throws Throwable {
      boolean persistentMessages = true;

      Configuration config = createDefaultInVMConfig().setJournalSyncNonTransactional(false);

      ActiveMQServer server = createServer(true, config, PAGE_SIZE, PAGE_MAX, new HashMap<String, AddressSettings>());

      server.start();

      final int messageSize = 1024;

      final int numberOfMessages = 3000;

      ServerLocator locator = createInVMNonHALocator().setClientFailureCheckPeriod(1000).setConnectionTTL(2000).setReconnectAttempts(0).setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true).setConsumerWindowSize(1024 * 1024);

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, false, false);

      server.createAddressInfo(new AddressInfo(ADDRESS, RoutingType.ANYCAST));
      server.createQueue(ADDRESS, RoutingType.ANYCAST, ADDRESS, null, true, false);

      ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

      byte[] body = new byte[messageSize];

      ByteBuffer bb = ByteBuffer.wrap(body);

      for (int j = 1; j <= messageSize; j++) {
         bb.put(getSamplebyte(j));
      }

      for (int i = 0; i < numberOfMessages; i++) {
         ClientMessage message = session.createMessage(persistentMessages);

         ActiveMQBuffer bodyLocal = message.getBodyBuffer();

         bodyLocal.writeBytes(body);

         message.putIntProperty(new SimpleString("id"), i);

         producer.send(message);
         if (i % 1000 == 0) {
            session.commit();
         }
      }

      session.commit();

      session.close();

      session = sf.createSession(false, false, 0);

      session.start();

      ClientConsumer consumer = session.createConsumer(ADDRESS);

      for (int i = 0; i < numberOfMessages / 2; i++) {
         ClientMessage message = consumer.receive(5000);
         assertNotNull(message);
         assertEquals(i, message.getIntProperty("id").intValue());
         message.acknowledge();
      }

      session.rollback();

      session.close();

      session = sf.createSession(false, false, 0);

      session.start();

      consumer = session.createConsumer(ADDRESS);

      for (int i = 0; i < numberOfMessages; i++) {
         ClientMessage message = consumer.receive(5000);
         assertNotNull(message);
         assertEquals(i, message.getIntProperty("id").intValue());
         message.acknowledge();
      }

      session.commit();
   }

   @Test
   public void testOrderOverRollback2() throws Throwable {
      boolean persistentMessages = true;

      Configuration config = createDefaultInVMConfig().setJournalSyncNonTransactional(false);

      ActiveMQServer server = createServer(true, config, PAGE_SIZE, PAGE_MAX, new HashMap<String, AddressSettings>());

      server.start();

      final int messageSize = 1024;

      final int numberOfMessages = 200;

      ServerLocator locator = createInVMNonHALocator().setClientFailureCheckPeriod(1000).setConnectionTTL(2000).setReconnectAttempts(0).setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true).setConsumerWindowSize(0);

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, false, false);

      server.createAddressInfo(new AddressInfo(ADDRESS, RoutingType.ANYCAST));
      QueueImpl queue = (QueueImpl) server.createQueue(ADDRESS, RoutingType.ANYCAST, ADDRESS, null, true, false);

      ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

      byte[] body = new byte[messageSize];

      ByteBuffer bb = ByteBuffer.wrap(body);

      for (int j = 1; j <= messageSize; j++) {
         bb.put(getSamplebyte(j));
      }

      for (int i = 0; i < numberOfMessages; i++) {
         ClientMessage message = session.createMessage(persistentMessages);

         ActiveMQBuffer bodyLocal = message.getBodyBuffer();

         bodyLocal.writeBytes(body);

         message.putIntProperty(new SimpleString("id"), i);

         producer.send(message);
         if (i % 1000 == 0) {
            session.commit();
         }
      }

      session.commit();

      session.close();

      session = sf.createSession(false, false, 0);

      session.start();

      ClientConsumer consumer = session.createConsumer(ADDRESS);

      // number of references without paging
      int numberOfRefs = queue.getNumberOfReferences();

      // consume all non-paged references
      for (int ref = 0; ref < numberOfRefs; ref++) {
         ClientMessage msg = consumer.receive(5000);
         assertNotNull(msg);
         msg.acknowledge();
      }

      session.commit();

      session.close();

      session = sf.createSession(false, false, 0);

      session.start();

      consumer = session.createConsumer(ADDRESS);

      ClientMessage msg = consumer.receive(5000);
      assertNotNull(msg);
      int msgIDRolledBack = msg.getIntProperty("id").intValue();
      msg.acknowledge();

      session.rollback();

      msg = consumer.receive(5000);

      assertNotNull(msg);

      assertEquals(msgIDRolledBack, msg.getIntProperty("id").intValue());

      session.rollback();

      session.close();

      sf.close();
      locator.close();

      server.stop();

      server.start();

      locator = createInVMNonHALocator().setClientFailureCheckPeriod(1000).setConnectionTTL(2000).setReconnectAttempts(0).setBlockOnNonDurableSend(true).setBlockOnDurableSend(true).setBlockOnAcknowledge(true).setConsumerWindowSize(0);

      sf = createSessionFactory(locator);

      session = sf.createSession(false, false, 0);

      session.start();

      consumer = session.createConsumer(ADDRESS);

      for (int i = msgIDRolledBack; i < numberOfMessages; i++) {
         ClientMessage message = consumer.receive(5000);
         assertNotNull(message);
         assertEquals(i, message.getIntProperty("id").intValue());
         message.acknowledge();
      }

      session.commit();

      session.close();
   }

   @Test
   public void testPagingOverCreatedDestinationTopics() throws Exception {

      Configuration config = createDefaultInVMConfig().setJournalSyncNonTransactional(false);

      ActiveMQServer server = createServer(true, config, PAGE_SIZE, -1, new HashMap<String, AddressSettings>());

      JMSServerManagerImpl jmsServer = new JMSServerManagerImpl(server);
      InVMNamingContext context = new InVMNamingContext();
      jmsServer.setRegistry(new JndiBindingRegistry(context));
      jmsServer.start();

      jmsServer.createTopic(true, "tt", "/topic/TT");

      server.getActiveMQServerControl().addAddressSettings("TT", "DLQ", "DLQ", -1, false, 5, 1024 * 1024, 1024 * 10, 5, 5, 1, 1000, 0, false, "PAGE", -1, 10, "KILL", true, true, true, true);

      ActiveMQJMSConnectionFactory cf = (ActiveMQJMSConnectionFactory) ActiveMQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, new TransportConfiguration(INVM_CONNECTOR_FACTORY));

      Connection conn = cf.createConnection();
      conn.setClientID("tst");
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Topic topic = (Topic) context.lookup("/topic/TT");
      sess.createDurableSubscriber(topic, "t1");

      MessageProducer prod = sess.createProducer(topic);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      TextMessage txt = sess.createTextMessage("TST");
      prod.send(txt);

      PagingStore store = server.getPagingManager().getPageStore(new SimpleString("TT"));

      assertEquals(1024 * 1024, store.getMaxSize());
      assertEquals(10 * 1024, store.getPageSizeBytes());

      jmsServer.stop();

      server = createServer(true, config, PAGE_SIZE, -1, new HashMap<String, AddressSettings>());

      jmsServer = new JMSServerManagerImpl(server);
      context = new InVMNamingContext();
      jmsServer.setRegistry(new JndiBindingRegistry(context));
      jmsServer.start();

      AddressSettings settings = server.getAddressSettingsRepository().getMatch("TT");

      assertEquals(1024 * 1024, settings.getMaxSizeBytes());
      assertEquals(10 * 1024, settings.getPageSizeBytes());
      assertEquals(AddressFullMessagePolicy.PAGE, settings.getAddressFullMessagePolicy());

      store = server.getPagingManager().getPageStore(new SimpleString("TT"));

      conn.close();

      server.stop();

   }

   @Test
   public void testPagingOverCreatedDestinationQueues() throws Exception {

      Configuration config = createDefaultInVMConfig().setJournalSyncNonTransactional(false);

      ActiveMQServer server = createServer(true, config, -1, -1, new HashMap<String, AddressSettings>());
      server.getAddressSettingsRepository().getMatch("#").setAddressFullMessagePolicy(AddressFullMessagePolicy.BLOCK);

      JMSServerManagerImpl jmsServer = new JMSServerManagerImpl(server);
      InVMNamingContext context = new InVMNamingContext();
      jmsServer.setRegistry(new JndiBindingRegistry(context));
      jmsServer.start();

      server.getActiveMQServerControl().addAddressSettings("Q1", "DLQ", "DLQ", -1, false, 5, 100 * 1024, 10 * 1024, 5, 5, 1, 1000, 0, false, "PAGE", -1, 10, "KILL", true, true, true, true);

      jmsServer.createQueue(true, "Q1", null, true, "/queue/Q1");

      ActiveMQJMSConnectionFactory cf = (ActiveMQJMSConnectionFactory) ActiveMQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, new TransportConfiguration(INVM_CONNECTOR_FACTORY));

      conn = cf.createConnection();
      conn.setClientID("tst");
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      javax.jms.Queue queue = (javax.jms.Queue) context.lookup("/queue/Q1");

      MessageProducer prod = sess.createProducer(queue);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      BytesMessage bmt = sess.createBytesMessage();

      bmt.writeBytes(new byte[1024]);

      for (int i = 0; i < 500; i++) {
         prod.send(bmt);
      }

      PagingStore store = server.getPagingManager().getPageStore(new SimpleString("Q1"));

      assertEquals(100 * 1024, store.getMaxSize());
      assertEquals(10 * 1024, store.getPageSizeBytes());
      assertEquals(AddressFullMessagePolicy.PAGE, store.getAddressFullMessagePolicy());

      jmsServer.stop();

      server = createServer(true, config, -1, -1, new HashMap<String, AddressSettings>());
      server.getAddressSettingsRepository().getMatch("#").setAddressFullMessagePolicy(AddressFullMessagePolicy.BLOCK);

      jmsServer = new JMSServerManagerImpl(server);
      context = new InVMNamingContext();
      jmsServer.setRegistry(new JndiBindingRegistry(context));
      jmsServer.start();

      AddressSettings settings = server.getAddressSettingsRepository().getMatch("Q1");

      assertEquals(100 * 1024, settings.getMaxSizeBytes());
      assertEquals(10 * 1024, settings.getPageSizeBytes());
      assertEquals(AddressFullMessagePolicy.PAGE, settings.getAddressFullMessagePolicy());

      store = server.getPagingManager().getPageStore(new SimpleString("Q1"));
      assertEquals(100 * 1024, store.getMaxSize());
      assertEquals(10 * 1024, store.getPageSizeBytes());
      assertEquals(AddressFullMessagePolicy.PAGE, store.getAddressFullMessagePolicy());
   }
}
