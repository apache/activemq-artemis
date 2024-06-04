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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TestCase showing the message-destroying described in AMQ-1925
 */
public class AMQ1925Test extends OpenwireArtemisBaseTest implements ExceptionListener {

   private static final Logger log = LoggerFactory.getLogger(AMQ1925Test.class);

   private static final String QUEUE_NAME = "test.amq1925";
   private static final String PROPERTY_MSG_NUMBER = "NUMBER";
   private static final int MESSAGE_COUNT = 10000;

   private EmbeddedJMS bs;
   private URI tcpUri;
   private ActiveMQConnectionFactory cf;

   private JMSException exception;

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
      QueueImpl queue = (QueueImpl) bs.getActiveMQServer().getPostOffice().getBinding(SimpleString.of(QUEUE_NAME)).getBindable();
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
         log.error(e.getMessage(), e);
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
