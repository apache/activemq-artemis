/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.transport.failover;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.TransactionRolledBackException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.server.impl.QueueImpl;
import org.apache.activemq.artemis.jms.server.config.impl.JMSConfigurationImpl;
import org.apache.activemq.artemis.jms.server.embedded.EmbeddedJMS;
import org.apache.activemq.broker.artemiswrapper.OpenwireArtemisBaseTest;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * TestCase showing the message-destroying described in AMQ-1925
 */
public class AMQ1925Test extends OpenwireArtemisBaseTest implements ExceptionListener {

   private static final Logger log = Logger.getLogger(AMQ1925Test.class);

   private static final String QUEUE_NAME = "test.amq1925";
   private static final String PROPERTY_MSG_NUMBER = "NUMBER";
   private static final int MESSAGE_COUNT = 10000;

   private EmbeddedJMS bs;
   private URI tcpUri;
   private ActiveMQConnectionFactory cf;

   private JMSException exception;

   public void XtestAMQ1925_TXInProgress() throws Exception {
      Connection connection = cf.createConnection();
      connection.start();
      Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
      MessageConsumer consumer = session.createConsumer(session.createQueue(QUEUE_NAME));

      // The runnable is likely to interrupt during the session#commit, since
      // this takes the longest
      final CountDownLatch starter = new CountDownLatch(1);
      final AtomicBoolean restarted = new AtomicBoolean();
      new Thread(new Runnable() {
         @Override
         public void run() {
            try {
               starter.await();

               // Simulate broker failure & restart
               bs.stop();
               bs = createNewServer();
               bs.start();

               restarted.set(true);
            } catch (Exception e) {
               e.printStackTrace();
            }
         }
      }).start();

      for (int i = 0; i < MESSAGE_COUNT; i++) {
         Message message = consumer.receive(500);
         Assert.assertNotNull("No Message " + i + " found", message);

         if (i < 10)
            Assert.assertFalse("Timing problem, restarted too soon", restarted.get());
         if (i == 10) {
            starter.countDown();
         }
         if (i > MESSAGE_COUNT - 100) {
            Assert.assertTrue("Timing problem, restarted too late", restarted.get());
         }

         Assert.assertEquals(i, message.getIntProperty(PROPERTY_MSG_NUMBER));
         session.commit();
      }
      Assert.assertNull(consumer.receive(500));

      consumer.close();
      session.close();
      connection.close();

      assertQueueEmpty();
   }

   public void XtestAMQ1925_TXInProgress_TwoConsumers() throws Exception {
      Connection connection = cf.createConnection();
      connection.start();
      Session session1 = connection.createSession(true, Session.SESSION_TRANSACTED);
      MessageConsumer consumer1 = session1.createConsumer(session1.createQueue(QUEUE_NAME));
      Session session2 = connection.createSession(true, Session.SESSION_TRANSACTED);
      MessageConsumer consumer2 = session2.createConsumer(session2.createQueue(QUEUE_NAME));

      // The runnable is likely to interrupt during the session#commit, since
      // this takes the longest
      final CountDownLatch starter = new CountDownLatch(1);
      final AtomicBoolean restarted = new AtomicBoolean();
      new Thread(new Runnable() {
         @Override
         public void run() {
            try {
               starter.await();

               // Simulate broker failure & restart
               bs.stop();
               bs = createNewServer();
               bs.start();

               restarted.set(true);
            } catch (Exception e) {
               e.printStackTrace();
            }
         }
      }).start();

      Collection<Integer> results = new ArrayList<>(MESSAGE_COUNT);
      for (int i = 0; i < MESSAGE_COUNT; i++) {
         Message message1 = consumer1.receive(20);
         Message message2 = consumer2.receive(20);
         if (message1 == null && message2 == null) {
            if (results.size() < MESSAGE_COUNT) {
               message1 = consumer1.receive(500);
               message2 = consumer2.receive(500);

               if (message1 == null && message2 == null) {
                  // Missing messages
                  break;
               }
            }
            break;
         }

         if (i < 10)
            Assert.assertFalse("Timing problem, restarted too soon", restarted.get());
         if (i == 10) {
            starter.countDown();
         }
         if (i > MESSAGE_COUNT - 50) {
            Assert.assertTrue("Timing problem, restarted too late", restarted.get());
         }

         if (message1 != null) {
            results.add(message1.getIntProperty(PROPERTY_MSG_NUMBER));
            session1.commit();
         }
         if (message2 != null) {
            results.add(message2.getIntProperty(PROPERTY_MSG_NUMBER));
            session2.commit();
         }
      }
      Assert.assertNull(consumer1.receive(500));
      Assert.assertNull(consumer2.receive(500));

      consumer1.close();
      session1.close();
      consumer2.close();
      session2.close();
      connection.close();

      int foundMissingMessages = 0;
      if (results.size() < MESSAGE_COUNT) {
         foundMissingMessages = tryToFetchMissingMessages();
      }
      for (int i = 0; i < MESSAGE_COUNT; i++) {
         Assert.assertTrue("Message-Nr " + i + " not found (" + results.size() + " total, " + foundMissingMessages + " have been found 'lingering' in the queue)", results.contains(i));
      }
      assertQueueEmpty();
   }

   private int tryToFetchMissingMessages() throws JMSException {
      Connection connection = cf.createConnection();
      connection.start();
      Session session = connection.createSession(true, 0);
      MessageConsumer consumer = session.createConsumer(session.createQueue(QUEUE_NAME));

      int count = 0;
      while (true) {
         Message message = consumer.receive(500);
         if (message == null)
            break;

         log.info("Found \"missing\" message: " + message);
         count++;
      }

      consumer.close();
      session.close();
      connection.close();

      return count;
   }

   @Test
   public void testAMQ1925_TXBegin() throws Exception {
      Connection connection = cf.createConnection();
      connection.start();
      connection.setExceptionListener(this);
      Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
      MessageConsumer consumer = session.createConsumer(session.createQueue(QUEUE_NAME));

      boolean restartDone = false;
      try {
         for (int i = 0; i < MESSAGE_COUNT; i++) {
            Message message = consumer.receive(5000);
            Assert.assertNotNull(message);

            if (i == 222 && !restartDone) {
               // Simulate broker failure & restart
               bs.stop();
               bs = createNewServer();
               bs.start();
               restartDone = true;
            }

            Assert.assertEquals(i, message.getIntProperty(PROPERTY_MSG_NUMBER));
            try {
               session.commit();
            } catch (TransactionRolledBackException expectedOnOccasion) {
               log.info("got rollback: " + expectedOnOccasion);
               i--;
            }
         }
         Assert.assertNull(consumer.receive(500));
      } catch (Exception eee) {
         log.error("got exception", eee);
         throw eee;
      } finally {
         consumer.close();
         session.close();
         connection.close();
      }

      assertQueueEmpty();
      Assert.assertNull("no exception on connection listener: " + exception, exception);
   }

   @Test
   public void testAMQ1925_TXCommited() throws Exception {
      Connection connection = cf.createConnection();
      connection.start();
      Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
      MessageConsumer consumer = session.createConsumer(session.createQueue(QUEUE_NAME));

      for (int i = 0; i < MESSAGE_COUNT; i++) {
         Message message = consumer.receive(5000);
         Assert.assertNotNull(message);

         Assert.assertEquals(i, message.getIntProperty(PROPERTY_MSG_NUMBER));
         session.commit();

         if (i == 222) {
            // Simulate broker failure & restart
            bs.stop();
            bs = createNewServer();
            bs.start();
         }
      }
      Assert.assertNull(consumer.receive(500));

      consumer.close();
      session.close();
      connection.close();

      assertQueueEmpty();
   }

   private void assertQueueEmpty() throws Exception {
      Connection connection = cf.createConnection();
      connection.start();
      Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
      MessageConsumer consumer = session.createConsumer(session.createQueue(QUEUE_NAME));

      Message msg = consumer.receive(500);
      if (msg != null) {
         Assert.fail(msg.toString());
      }

      consumer.close();
      session.close();
      connection.close();

      assertQueueLength(0);
   }

   private void assertQueueLength(int len) throws Exception, IOException {
      QueueImpl queue = (QueueImpl) bs.getActiveMQServer().getPostOffice().getBinding(new SimpleString(QUEUE_NAME)).getBindable();
      if (len > queue.getMessageCount()) {
         //we wait for a moment as the tx might still in afterCommit stage (async op)
         Thread.sleep(5000);
      }
      Assert.assertEquals(len, queue.getMessageCount());
   }

   private void sendMessagesToQueue() throws Exception {
      Connection connection = cf.createConnection();
      Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
      MessageProducer producer = session.createProducer(session.createQueue(QUEUE_NAME));

      producer.setDeliveryMode(DeliveryMode.PERSISTENT);
      for (int i = 0; i < MESSAGE_COUNT; i++) {
         TextMessage message = session.createTextMessage("Test message " + i);
         message.setIntProperty(PROPERTY_MSG_NUMBER, i);
         producer.send(message);
      }
      session.commit();

      producer.close();
      session.close();
      connection.close();

      assertQueueLength(MESSAGE_COUNT);
   }

   @Before
   public void setUp() throws Exception {
      exception = null;
      bs = createNewServer();
      bs.start();
      //auto created queue can't survive a restart, so we need this
      bs.getJMSServerManager().createQueue(false, QUEUE_NAME, null, true, QUEUE_NAME);

      tcpUri = new URI(newURI(0));

      cf = new ActiveMQConnectionFactory("failover://(" + tcpUri + ")");

      sendMessagesToQueue();
   }

   @After
   public void tearDown() throws Exception {
      try {
         if (bs != null) {
            bs.stop();
            bs = null;
         }
      } catch (Exception e) {
         log.error(e);
      }
   }

   @Override
   public void onException(JMSException exception) {
      this.exception = exception;
   }

   private EmbeddedJMS createNewServer() throws Exception {
      Configuration config = createConfig("localhost", 0);
      EmbeddedJMS server = new EmbeddedJMS().setConfiguration(config).setJmsConfiguration(new JMSConfigurationImpl());
      return server;
   }
}
