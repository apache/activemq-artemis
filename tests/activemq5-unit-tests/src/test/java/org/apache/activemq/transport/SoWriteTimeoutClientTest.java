/**
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
package org.apache.activemq.transport;

import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.jms.server.config.impl.JMSConfigurationImpl;
import org.apache.activemq.artemis.jms.server.embedded.EmbeddedJMS;
import org.apache.activemq.artemis.utils.ThreadLeakCheckRule;
import org.apache.activemq.broker.artemiswrapper.OpenwireArtemisBaseTest;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.util.SocketProxy;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SoWriteTimeoutClientTest extends OpenwireArtemisBaseTest {

   private static final Logger LOG = LoggerFactory.getLogger(SoWriteTimeoutClientTest.class);
   private String messageTextPrefix = "";
   private EmbeddedJMS server;

   @BeforeClass
   public static void beforeTest() throws Exception {
      //this thread keeps alive in original test too. Exclude it.
      ThreadLeakCheckRule.addKownThread("WriteTimeoutFilter-Timeout");
   }

   @AfterClass
   public static void afterTest() throws Exception {
      ThreadLeakCheckRule.removeKownThread("WriteTimeoutFilter-Timeout");
   }

   @Before
   public void setUp() throws Exception {
      Configuration config = this.createConfig(0);
      server = new EmbeddedJMS().setConfiguration(config).setJmsConfiguration(new JMSConfigurationImpl());
      server.start();
   }

   @After
   public void tearDown() throws Exception {
      server.stop();
   }

   @Test
   public void testSendWithClientWriteTimeout() throws Exception {
      final ActiveMQQueue dest = new ActiveMQQueue("testClientWriteTimeout");
      messageTextPrefix = initMessagePrefix(80 * 1024);

      URI tcpBrokerUri = new URI(newURI(0));
      LOG.info("consuming using uri: " + tcpBrokerUri);

      ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(tcpBrokerUri);
      Connection c = factory.createConnection();
      c.start();
      Session session = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageConsumer consumer = session.createConsumer(dest);

      SocketProxy proxy = new SocketProxy();
      try {
         proxy.setTarget(tcpBrokerUri);
         proxy.open();

         ActiveMQConnectionFactory pFactory = new ActiveMQConnectionFactory("failover:(" + proxy.getUrl() + "?soWriteTimeout=4000&sleep=500)?jms.useAsyncSend=true&trackMessages=true&maxCacheSize=6638400");
         final Connection pc = pFactory.createConnection();
         try {
            pc.start();
            proxy.pause();

            final int messageCount = 20;
            ExecutorService executorService = Executors.newCachedThreadPool();
            executorService.execute(new Runnable() {
               @Override
               public void run() {
                  try {
                     sendMessages(pc, dest, messageCount);
                  }
                  catch (Exception ignored) {
                     ignored.printStackTrace();
                  }
               }
            });

            // wait for timeout and reconnect
            TimeUnit.SECONDS.sleep(8);
            proxy.goOn();
            for (int i = 0; i < messageCount; i++) {
               TextMessage m = (TextMessage) consumer.receive(10000);
               Assert.assertNotNull("Got message " + i + " after reconnect", m);
            }

            Assert.assertNull(consumer.receive(5000));
         }
         finally {
            pc.close();
         }
      }
      finally {
         proxy.close();
         c.close();
      }

   }

   protected void sendMessages(Connection connection, Destination destination, int count) throws JMSException {
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      sendMessages(session, destination, count);
      session.close();
   }

   protected void sendMessages(Session session, Destination destination, int count) throws JMSException {
      MessageProducer producer = session.createProducer(destination);
      sendMessages(session, producer, count);
      producer.close();
   }

   protected void sendMessages(Session session, MessageProducer producer, int count) throws JMSException {
      for (int i = 0; i < count; i++) {
         producer.send(session.createTextMessage(messageTextPrefix + i));
      }
   }

   private String initMessagePrefix(int i) {
      byte[] content = new byte[i];
      return new String(content);
   }
}

