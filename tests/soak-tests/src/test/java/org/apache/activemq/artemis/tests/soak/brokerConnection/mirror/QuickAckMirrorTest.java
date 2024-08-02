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

package org.apache.activemq.artemis.tests.soak.brokerConnection.mirror;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.File;
import java.lang.invoke.MethodHandles;
import java.util.Properties;

import org.apache.activemq.artemis.api.core.management.SimpleManagement;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectionAddressType;
import org.apache.activemq.artemis.tests.soak.SoakTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.tests.util.RandomUtil;
import org.apache.activemq.artemis.util.ServerUtil;
import org.apache.activemq.artemis.utils.FileUtil;
import org.apache.activemq.artemis.utils.Wait;
import org.apache.activemq.artemis.utils.cli.helper.HelperCreate;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QuickAckMirrorTest extends SoakTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private static final String QUEUE_NAME = "myQueue";

   public static final String DC1_NODE_A = "ImmediateAckIdempotentTest/DC1";
   public static final String DC2_NODE_A = "ImmediateAckIdempotentTest/DC2";

   Process processDC1_node_A;
   Process processDC2_node_A;

   private static String DC1_NODEA_URI = "tcp://localhost:61616";
   private static String DC2_NODEA_URI = "tcp://localhost:61618";

   private static void createServer(String serverName,
                                    String connectionName,
                                    String mirrorURI,
                                    int porOffset) throws Exception {
      File serverLocation = getFileServerLocation(serverName);
      deleteDirectory(serverLocation);

      HelperCreate cliCreateServer = new HelperCreate();
      cliCreateServer.setAllowAnonymous(true).setNoWeb(true).setArtemisInstance(serverLocation);
      cliCreateServer.setMessageLoadBalancing("ON_DEMAND");
      cliCreateServer.setClustered(false);
      cliCreateServer.setNoWeb(true);
      cliCreateServer.setArgs("--no-stomp-acceptor", "--no-hornetq-acceptor", "--no-mqtt-acceptor", "--no-amqp-acceptor", "--max-hops", "1", "--name", DC1_NODE_A);
      cliCreateServer.addArgs("--queues", QUEUE_NAME);
      cliCreateServer.setPortOffset(porOffset);
      cliCreateServer.createServer();

      Properties brokerProperties = new Properties();
      brokerProperties.put("AMQPConnections." + connectionName + ".uri", mirrorURI);
      brokerProperties.put("AMQPConnections." + connectionName + ".retryInterval", "1000");
      brokerProperties.put("AMQPConnections." + connectionName + ".type", AMQPBrokerConnectionAddressType.MIRROR.toString());
      brokerProperties.put("AMQPConnections." + connectionName + ".connectionElements.mirror.sync", "false");


      /* one way to show missed ACKs would be by setting:
       *
       * mirrorAckManagerMinQueueAttempts=1
       * mirrorAckManagerMaxPageAttempts=1
       * mirrorAckManagerRetryDelay=1
       *
       *
       * the retry will be faster than the message would arrive at the queue
       * */


      brokerProperties.put("mirrorAckManagerMinQueueAttempts", "10");
      brokerProperties.put("mirrorAckManagerMaxPageAttempts", "10");
      brokerProperties.put("mirrorAckManagerRetryDelay", "1000");

      // introducing more delay in storage

      Assertions.assertTrue(FileUtil.findReplace(new File(serverLocation, "/etc/broker.xml"), "</journal-file-size>", "</journal-file-size>\n      <journal-buffer-timeout>20000000</journal-buffer-timeout>"));

      brokerProperties.put("largeMessageSync", "false");
      File brokerPropertiesFile = new File(serverLocation, "broker.properties");
      saveProperties(brokerProperties, brokerPropertiesFile);
   }

   @BeforeAll
   public static void createServers() throws Exception {
      createServer(DC1_NODE_A, "mirror", DC2_NODEA_URI, 0);
      createServer(DC2_NODE_A, "mirror", DC1_NODEA_URI, 2);
   }

   @BeforeEach
   public void cleanupServers() {
      cleanupData(DC1_NODE_A);
      cleanupData(DC2_NODE_A);
   }

   @Test
   public void testQuickACKAMQP() throws Exception {
      testQuickACK("AMQP");
   }

   @Test
   public void testQuickACKCORE() throws Exception {
      testQuickACK("CORE");
   }

   @Test
   public void testQuickACKOpenWire() throws Exception {
      testQuickACK("OPENWIRE");
   }


   private void testQuickACK(final String protocol) throws Exception {
      processDC1_node_A = startServer(DC1_NODE_A, -1, -1, new File(getServerLocation(DC1_NODE_A), "broker.properties"));
      ServerUtil.waitForServerToStart(0, 10_000);

      ConnectionFactory connectionFactoryDC1A = CFUtil.createConnectionFactory(protocol, DC1_NODEA_URI);

      final int numberOfMessages = 1_000;
      String snfQueue = "$ACTIVEMQ_ARTEMIS_MIRROR_mirror";

      try (Connection connection = connectionFactoryDC1A.createConnection()) {
         connection.start();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue queue = session.createQueue(QUEUE_NAME);
         MessageProducer producer = session.createProducer(queue);
         MessageConsumer consumer = session.createConsumer(queue);

         for (int i = 0; i < numberOfMessages; i++) {
            if (i % 100 == 0) {
               logger.info("Sent and received {}", i);
            }
            String text = "hello hello hello " + RandomUtil.randomString();
            producer.send(session.createTextMessage(text));
            TextMessage textMessage = (TextMessage) consumer.receive(5000);
            Assertions.assertNotNull(textMessage);
            Assertions.assertEquals(text, textMessage.getText());
         }
      } catch (Throwable e) {
         logger.warn(e.getMessage(), e);
         try {
            Thread.sleep(100);
         } catch (Throwable ignored) {
         }
      }

      processDC2_node_A = startServer(DC2_NODE_A, -1, -1, new File(getServerLocation(DC2_NODE_A), "broker.properties"));
      ServerUtil.waitForServerToStart(0, 10_000);

      SimpleManagement simpleManagementDC1A = new SimpleManagement(DC1_NODEA_URI, null, null);
      SimpleManagement simpleManagementDC2A = new SimpleManagement(DC2_NODEA_URI, null, null);

      Wait.assertEquals(0, () -> getMessageCount(simpleManagementDC1A, snfQueue));
      Wait.assertEquals(0, () -> getMessageCount(simpleManagementDC1A, QUEUE_NAME));
      Wait.assertEquals(0, () -> getMessageCount(simpleManagementDC2A, QUEUE_NAME));
   }
}
