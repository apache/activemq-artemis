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
package org.apache.activemq.artemis.tests.integration.jms;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.naming.Context;
import javax.naming.InitialContext;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.concurrent.CountDownLatch;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.CoreQueueConfiguration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.tests.integration.IntegrationTestLogger;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;

public class ManualReconnectionToSingleServerTest extends ActiveMQTestBase {
   // Constants -----------------------------------------------------

   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   private Connection connection;

   private MessageConsumer consumer;

   private CountDownLatch exceptionLatch;
   private CountDownLatch reconnectionLatch;
   private CountDownLatch allMessagesReceived;

   private Context context;

   private static final String QUEUE_NAME = ManualReconnectionToSingleServerTest.class.getSimpleName() + ".queue";

   private static final int NUM = 20;

   private final ExceptionListener exceptionListener = new ExceptionListener() {
      @Override
      public void onException(final JMSException e) {
         exceptionLatch.countDown();
         disconnect();
         connect();
         reconnectionLatch.countDown();
      }
   };

   private Listener listener;

   private ActiveMQServer server;

   @Test
   public void testExceptionListener() throws Exception {
      connect();

      ConnectionFactory cf = (ConnectionFactory) context.lookup("cf");
      Destination dest = (Destination) context.lookup(QUEUE_NAME);
      Connection conn = cf.createConnection();
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer prod = sess.createProducer(dest);

      for (int i = 0; i < NUM; i++) {
         Message message = sess.createTextMessage(new Date().toString());
         message.setIntProperty("counter", i + 1);
         prod.send(message);

         if (i == NUM / 2) {
            conn.close();
            server.stop();
            Thread.sleep(5000);
            server.start();
            cf = (ConnectionFactory) context.lookup("cf");
            dest = (Destination) context.lookup(QUEUE_NAME);
            conn = cf.createConnection();
            sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            prod = sess.createProducer(dest);
         }
      }

      conn.close();

      boolean gotException = exceptionLatch.await(10, SECONDS);
      Assert.assertTrue(gotException);

      boolean clientReconnected = reconnectionLatch.await(10, SECONDS);

      Assert.assertTrue("client did not reconnect after server was restarted", clientReconnected);

      boolean gotAllMessages = allMessagesReceived.await(10, SECONDS);
      Assert.assertTrue(gotAllMessages);

      connection.close();

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      Hashtable<String, String> props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory.class.getCanonicalName());
      props.put("queue." + QUEUE_NAME, QUEUE_NAME);
      props.put("connectionFactory.cf", "tcp://127.0.0.1:61616?retryInterval=1000&reconnectAttempts=-1");

      context = new InitialContext(props);

      server = createServer(false, createDefaultNettyConfig());

      Configuration configuration = new ConfigurationImpl();

      configuration.getQueueConfigurations().add(new CoreQueueConfiguration().setName(QUEUE_NAME));

      ArrayList<TransportConfiguration> configs = new ArrayList<>();
      configs.add(new TransportConfiguration(NETTY_CONNECTOR_FACTORY));
      server.start();

      listener = new Listener();

      exceptionLatch = new CountDownLatch(1);
      reconnectionLatch = new CountDownLatch(1);
      allMessagesReceived = new CountDownLatch(1);
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   protected void disconnect() {
      ManualReconnectionToSingleServerTest.log.info("calling disconnect");
      if (connection == null) {
         ManualReconnectionToSingleServerTest.log.info("connection is null");
         return;
      }

      try {
         connection.setExceptionListener(null);
         ManualReconnectionToSingleServerTest.log.info("closing the connection");
         connection.close();
         connection = null;
         ManualReconnectionToSingleServerTest.log.info("connection closed");
      } catch (Exception e) {
         ManualReconnectionToSingleServerTest.log.info("** got exception");
         e.printStackTrace();
      }
   }

   protected void connect() {
      int retries = 0;
      final int retryLimit = 1000;
      try {
         if (context == null) {
            return;
         }
         Context initialContext = context;
         Queue queue;
         ConnectionFactory cf;
         while (true) {
            try {
               queue = (Queue) initialContext.lookup(QUEUE_NAME);
               cf = (ConnectionFactory) initialContext.lookup("cf");
               break;
            } catch (Exception e) {
               if (retries++ > retryLimit)
                  throw e;
               // retry until server is up
               Thread.sleep(100);
            }
         }
         connection = cf.createConnection();
         connection.setExceptionListener(exceptionListener);
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         consumer = session.createConsumer(queue);
         consumer.setMessageListener(listener);
         connection.start();
      } catch (Exception e) {
         if (connection != null) {
            try {
               connection.close();
            } catch (JMSException e1) {
               e1.printStackTrace();
            }
         }
      }
   }

   private class Listener implements MessageListener {

      private int count = 0;

      @Override
      public void onMessage(final Message msg) {
         count++;

         try {
            msg.getIntProperty("counter");
         } catch (JMSException e) {
            e.printStackTrace();
         }
         if (count == NUM) {
            allMessagesReceived.countDown();
         }
      }
   }

}
