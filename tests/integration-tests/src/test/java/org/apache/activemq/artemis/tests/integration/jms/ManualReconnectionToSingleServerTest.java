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
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.CountDownLatch;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.registry.JndiBindingRegistry;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.jms.server.JMSServerManager;
import org.apache.activemq.artemis.jms.server.config.ConnectionFactoryConfiguration;
import org.apache.activemq.artemis.jms.server.config.JMSConfiguration;
import org.apache.activemq.artemis.jms.server.config.impl.ConnectionFactoryConfigurationImpl;
import org.apache.activemq.artemis.jms.server.config.impl.JMSConfigurationImpl;
import org.apache.activemq.artemis.jms.server.config.impl.JMSQueueConfigurationImpl;
import org.apache.activemq.artemis.jms.server.impl.JMSServerManagerImpl;
import org.apache.activemq.artemis.tests.integration.IntegrationTestLogger;
import org.apache.activemq.artemis.tests.unit.util.InVMNamingContext;
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

   private JMSServerManager serverManager;

   private InVMNamingContext context;

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

      ConnectionFactory cf = (ConnectionFactory) context.lookup("/cf");
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
            serverManager.stop();
            Thread.sleep(5000);
            serverManager.start();
            cf = (ConnectionFactory) context.lookup("/cf");
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

      context = new InVMNamingContext();

      server = createServer(false, createDefaultNettyConfig());

      JMSConfiguration configuration = new JMSConfigurationImpl();
      serverManager = new JMSServerManagerImpl(server, configuration);
      serverManager.setRegistry(new JndiBindingRegistry(context));

      configuration.getQueueConfigurations().add(new JMSQueueConfigurationImpl().setName(QUEUE_NAME).setBindings(QUEUE_NAME));

      ArrayList<TransportConfiguration> configs = new ArrayList<>();
      configs.add(new TransportConfiguration(NETTY_CONNECTOR_FACTORY));
      ConnectionFactoryConfiguration cfConfig = new ConnectionFactoryConfigurationImpl().setName("cf").setConnectorNames(registerConnectors(server, configs)).setBindings("/cf").setRetryInterval(1000).setReconnectAttempts(-1);
      configuration.getConnectionFactoryConfigurations().add(cfConfig);
      serverManager.start();

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
               cf = (ConnectionFactory) initialContext.lookup("/cf");
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
