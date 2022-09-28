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

package org.apache.activemq.artemis.tests.smoke.bridgeTransfer;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.Arrays;
import java.util.Collection;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.tests.smoke.common.SmokeTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.utils.Wait;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
public class BridgeTransferingTest extends SmokeTestBase {

   public static final String SERVER_NAME_0 = "bridgeTransfer/serverA";
   public static final String SERVER_NAME_1 = "bridgeTransfer/serverB";
   private static final Logger logger = LoggerFactory.getLogger(BridgeTransferingTest.class);
   private static final String JMX_SERVER_HOSTNAME = "localhost";
   private static final int JMX_SERVER_PORT = 11099;

   final String theprotocol;
   // As the produces sends messages, a client will be killed every X messages. This is it!
   final int killServerInterval;
   final int numberOfMessages;
   final int commitInterval;
   final int messageSize;
   final boolean killBothServers;
   final int minlargeMessageSize;
   Process serverProcess;
   Process serverProcess2;

   public BridgeTransferingTest(String protocol, int commitInterval, int killServerInterval, int numberOfMessages, int messageSize, int minlargeMessageSize, boolean killBothServers) {
      this.theprotocol = protocol;
      this.killServerInterval = killServerInterval;
      this.messageSize = messageSize;
      this.commitInterval = commitInterval;
      this.numberOfMessages = numberOfMessages;
      this.killBothServers = killBothServers;
      this.minlargeMessageSize = minlargeMessageSize;
   }

   @Parameterized.Parameters(name = "protocol={0}, commitInterval={1}, killInterval={2}, numberOfMessages={3}, messageSize={4}, minLargeMessageSize={5}, KillBothServers={6}")
   public static Collection<Object[]> parameters() {
      return Arrays.asList(new Object[][]{{"CORE", 200, 1000, 10000, 15_000, 5000, true}, {"CORE", 200, 1000, 10000, 15_000, 5000, false}});
   }

   @Before
   public void before() throws Exception {
      cleanupData(SERVER_NAME_0);
      cleanupData(SERVER_NAME_1);
      disableCheckThread();
      serverProcess = startServer(SERVER_NAME_0, 0, 30000);
      serverProcess2 = startServer(SERVER_NAME_1, 1, 30000);
   }

   @After
   public void stopServers() throws Exception {
      serverProcess2.destroyForcibly();
      serverProcess.destroyForcibly();
   }

   @Test
   public void testTransfer() throws Exception {
      ConnectionFactory cf = CFUtil.createConnectionFactory(theprotocol, "tcp://localhost:61616");
      ((ActiveMQConnectionFactory) cf).setMinLargeMessageSize(minlargeMessageSize);

      String body;

      {
         StringBuffer buffer = new StringBuffer();
         while (buffer.length() < messageSize) {
            buffer.append(" ");
         }
         body = buffer.toString();
      }

      {
         Connection connection = cf.createConnection();
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         Queue queue = session.createQueue("bridgeQueue");
         MessageProducer producer = session.createProducer(queue);

         int txElement = 0;
         int killElement = 0;

         for (int i = 0; i < numberOfMessages; i++) {
            producer.send(session.createTextMessage(body + " " + i));

            if (++txElement == commitInterval) {
               logger.debug("Sent {} messages", (i + 1));
               txElement = 0;
               session.commit();
            }

            if (++killElement == killServerInterval) {
               logger.debug("Killing server at {}", (i + 1));
               killElement = 0;
               if (killBothServers) {
                  serverProcess.destroyForcibly();
                  Wait.assertFalse(serverProcess::isAlive);
               }
               serverProcess2.destroyForcibly();
               Wait.assertFalse(serverProcess2::isAlive);
               serverProcess2 = startServer(SERVER_NAME_1, 1, 30000);
               if (killBothServers) {
                  serverProcess = startServer(SERVER_NAME_0, 0, 30000);
               }
               if (killBothServers) {
                  connection.close();
                  connection = cf.createConnection();
                  session = connection.createSession(true, Session.SESSION_TRANSACTED);
                  queue = session.createQueue("bridgeQueue");
                  producer = session.createProducer(queue);
               }
            }
         }

         if (txElement > 0) {
            session.commit();
         }
      }
      ConnectionFactory cf2 = CFUtil.createConnectionFactory(theprotocol, "tcp://localhost:61617");
      try (Connection connection = cf2.createConnection()) {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue queue = session.createQueue("bridgeQueue");
         MessageConsumer consumer = session.createConsumer(queue);
         connection.start();

         for (int i = 0; i < numberOfMessages; i++) {
            if (i % 100 == 0) {
               logger.debug("consuming {}", i);
            }
            TextMessage message = (TextMessage) consumer.receive(5000);
            Assert.assertNotNull(message);
            Assert.assertEquals(body + " " + i, message.getText());
         }

         Assert.assertNull(consumer.receiveNoWait());
      }

   }

}
