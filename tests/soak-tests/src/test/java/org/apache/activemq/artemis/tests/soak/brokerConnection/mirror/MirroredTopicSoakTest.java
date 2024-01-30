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
import javax.jms.Topic;
import java.io.File;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.util.Properties;

import org.apache.activemq.artemis.api.core.management.SimpleManagement;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectionAddressType;
import org.apache.activemq.artemis.tests.soak.SoakTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.util.ServerUtil;
import org.apache.activemq.artemis.utils.Wait;
import org.apache.activemq.artemis.utils.cli.helper.HelperCreate;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MirroredTopicSoakTest extends SoakTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private static String largeBody;
   private static String smallBody = "This is a small body";

   static {
      StringWriter writer = new StringWriter();
      while (writer.getBuffer().length() < 1024 * 1024) {
         writer.append("This is a large string ..... ");
      }
      largeBody = writer.toString();
   }

   public static final String DC1_NODE_A = "mirror/DC1/A";
   public static final String DC2_NODE_A = "mirror/DC2/A";
   public static final String DC1_NODE_B = "mirror/DC1/B";
   public static final String DC2_NODE_B = "mirror/DC2/B";

   Process processDC1_node_A;
   Process processDC1_node_B;
   Process processDC2_node_A;
   Process processDC2_node_B;


   private static String DC1_NODEA_URI = "tcp://localhost:61616";
   private static String DC1_NODEB_URI = "tcp://localhost:61617";
   private static String DC2_NODEA_URI = "tcp://localhost:61618";
   private static String DC2_NODEB_URI = "tcp://localhost:61619";

   private static void createServer(String serverName, String connectionName, String clusterURI, String mirrorURI, int porOffset) throws Exception {
      File serverLocation = getFileServerLocation(serverName);
      deleteDirectory(serverLocation);

      HelperCreate cliCreateServer = new HelperCreate();
      cliCreateServer.setAllowAnonymous(true).setNoWeb(true).setArtemisInstance(serverLocation);
      cliCreateServer.setMessageLoadBalancing("ON_DEMAND");
      cliCreateServer.setClustered(true);
      cliCreateServer.setNoWeb(true);
      cliCreateServer.setStaticCluster(clusterURI);
      cliCreateServer.setArgs("--no-stomp-acceptor", "--no-hornetq-acceptor", "--no-mqtt-acceptor", "--no-amqp-acceptor", "--max-hops", "1", "--name", DC1_NODE_A);
      cliCreateServer.addArgs("--addresses", "order");
      cliCreateServer.addArgs("--queues", "myQueue");
      cliCreateServer.setPortOffset(porOffset);
      cliCreateServer.createServer();

      Properties brokerProperties = new Properties();
      brokerProperties.put("AMQPConnections." + connectionName + ".uri", mirrorURI);
      brokerProperties.put("AMQPConnections." + connectionName + ".retryInterval", "1000");
      brokerProperties.put("AMQPConnections." + connectionName + ".type", AMQPBrokerConnectionAddressType.MIRROR.toString());
      brokerProperties.put("AMQPConnections." + connectionName + ".connectionElements.mirror.sync", "false");
      brokerProperties.put("largeMessageSync", "false");
      File brokerPropertiesFile = new File(serverLocation, "broker.properties");
      saveProperties(brokerProperties, brokerPropertiesFile);
   }

   @BeforeClass
   public static void createServers() throws Exception {
      createServer(DC1_NODE_A, "mirror", DC1_NODEB_URI, DC2_NODEA_URI, 0);
      createServer(DC1_NODE_B, "mirror", DC1_NODEA_URI, DC2_NODEB_URI, 1);
      createServer(DC2_NODE_A, "mirror", DC2_NODEB_URI, DC1_NODEA_URI, 2);
      createServer(DC2_NODE_B, "mirror", DC2_NODEA_URI, DC1_NODEB_URI, 3);
   }

   private void startServers() throws Exception {
      processDC1_node_A = startServer(DC1_NODE_A, -1, -1, new File(getServerLocation(DC1_NODE_A), "broker.properties"));
      processDC1_node_B = startServer(DC1_NODE_B, -1, -1, new File(getServerLocation(DC1_NODE_B), "broker.properties"));
      processDC2_node_A = startServer(DC2_NODE_A, -1, -1, new File(getServerLocation(DC2_NODE_A), "broker.properties"));
      processDC2_node_B = startServer(DC2_NODE_B, -1, -1, new File(getServerLocation(DC2_NODE_B), "broker.properties"));

      ServerUtil.waitForServerToStart(0, 10_000);
      ServerUtil.waitForServerToStart(1, 10_000);
      ServerUtil.waitForServerToStart(2, 10_000);
      ServerUtil.waitForServerToStart(3, 10_000);
   }

   @Test
   public void testQueue() throws Exception {
      startServers();

      final int numberOfMessages = 200;

      Assert.assertTrue("numberOfMessages must be even", numberOfMessages % 2 == 0);

      ConnectionFactory connectionFactoryDC1A = CFUtil.createConnectionFactory("amqp", DC1_NODEA_URI);
      ConnectionFactory connectionFactoryDC2A = CFUtil.createConnectionFactory("amqp", DC2_NODEA_URI);
      String snfQueue = "$ACTIVEMQ_ARTEMIS_MIRROR_mirror";

      SimpleManagement simpleManagementDC1A = new SimpleManagement(DC1_NODEA_URI, null, null);

      try (Connection connection = connectionFactoryDC1A.createConnection()) {
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         Queue queue = session.createQueue("myQueue");
         MessageProducer producer = session.createProducer(queue);

         for (int i = 0; i < numberOfMessages; i++) {
            TextMessage message;
            boolean large;
            if (i % 1 == 2) {
               message = session.createTextMessage(largeBody);
               large = true;
            } else {
               message = session.createTextMessage(smallBody);
               large = false;
            }
            message.setIntProperty("i", i);
            message.setBooleanProperty("large", large);
            producer.send(message);
            if (i % 100 == 0) {
               logger.debug("commit {}", i);
               session.commit();
            }
         }
         session.commit();
      }

      logger.debug("All messages were sent");

      try (Connection connection = connectionFactoryDC1A.createConnection()) {
         connection.start();
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         Queue queue = session.createQueue("myQueue");
         MessageConsumer consumer = session.createConsumer(queue);

         for (int i = 0; i < numberOfMessages / 2; i++) {
            TextMessage message = (TextMessage) consumer.receive(5000);
            Assert.assertNotNull(message);
            logger.debug("Received message {}, large={}", message.getIntProperty("i"), message.getBooleanProperty("large"));
         }
         session.commit();
      }

      processDC2_node_A.destroyForcibly();
      processDC2_node_A.waitFor();
      processDC2_node_A = startServer(DC2_NODE_A, 2, 5000, new File(getServerLocation(DC2_NODE_A), "broker.properties"));

      Wait.assertEquals(0L, () -> getCount(simpleManagementDC1A, snfQueue), 250_000, 1000);

      try (Connection connection = connectionFactoryDC2A.createConnection()) {
         connection.start();
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         Queue queue = session.createQueue("myQueue");
         MessageConsumer consumer = session.createConsumer(queue);

         for (int i = 0; i < numberOfMessages / 2; i++) {
            TextMessage message = (TextMessage) consumer.receive(5000);
            Assert.assertNotNull(message);
            logger.debug("Received message {}, large={}", message.getIntProperty("i"), message.getBooleanProperty("large"));
         }
         session.commit();
      }
   }

   @Test
   public void testMirroredTopics() throws Exception {
      startServers();

      final int numberOfMessages = 200;

      Assert.assertTrue("numberOfMessages must be even", numberOfMessages % 2 == 0);

      String clientIDA = "nodeA";
      String clientIDB = "nodeB";
      String subscriptionID = "my-order";
      String snfQueue = "$ACTIVEMQ_ARTEMIS_MIRROR_mirror";

      ConnectionFactory connectionFactoryDC1A = CFUtil.createConnectionFactory("amqp", "tcp://localhost:61616");
      ConnectionFactory connectionFactoryDC1B = CFUtil.createConnectionFactory("amqp", "tcp://localhost:61617");
      ConnectionFactory connectionFactoryDC2A = CFUtil.createConnectionFactory("amqp", "tcp://localhost:61618");
      ConnectionFactory connectionFactoryDC2B = CFUtil.createConnectionFactory("amqp", "tcp://localhost:61619");

      SimpleManagement simpleManagementDC1B = new SimpleManagement(DC1_NODEB_URI, null, null);
      SimpleManagement simpleManagementDC2B = new SimpleManagement(DC2_NODEB_URI, null, null);

      consume(connectionFactoryDC1A, clientIDA, subscriptionID,  0, 0, false);
      consume(connectionFactoryDC1B, clientIDB, subscriptionID,  0, 0, false);

      try (Connection connection = connectionFactoryDC1B.createConnection()) {
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         Topic topic = session.createTopic("order");
         MessageProducer producer = session.createProducer(topic);

         for (int i = 0; i < numberOfMessages; i++) {
            TextMessage message;
            boolean large;
            message = session.createTextMessage(largeBody);
            large = true;
            message.setIntProperty("i", i);
            message.setBooleanProperty("large", large);
            producer.send(message);
            if (i % 100 == 0) {
               logger.debug("commit {}", i);
               session.commit();
            }
         }
         session.commit();
      }

      logger.debug("Consuming from DC1B");
      consume(connectionFactoryDC1B, clientIDB, subscriptionID,  0, numberOfMessages / 2, false);

      processDC2_node_B.destroyForcibly();
      processDC2_node_B.waitFor();
      processDC2_node_B = startServer(DC2_NODE_B, 3, 5000, new File(getServerLocation(DC2_NODE_B), "broker.properties"));

      Wait.assertEquals(0L, () -> getCount(simpleManagementDC1B, snfQueue), 250_000, 1000);
      Wait.assertEquals(numberOfMessages / 2, () -> simpleManagementDC2B.getMessageCountOnQueue("nodeB.my-order"), 10000);

      logger.debug("Consuming from DC2B with {}", simpleManagementDC2B.getMessageCountOnQueue("nodeB.my-order"));

      consume(connectionFactoryDC2B, clientIDB, subscriptionID,  numberOfMessages / 2, numberOfMessages / 2, true);

      Wait.assertEquals(0, () -> simpleManagementDC2B.getMessageCountOnQueue("nodeB.my-order"), 10000);

      Wait.assertEquals(0, () -> simpleManagementDC1B.getMessageCountOnQueue("nodeB.my-order"), 10000);
      consume(connectionFactoryDC1B, clientIDB, subscriptionID,  numberOfMessages, 0, true);
      logger.debug("DC1B nodeB.my-order=0");
   }

   public long getCount(SimpleManagement simpleManagement, String queue) throws Exception {
      long value = simpleManagement.getMessageCountOnQueue(queue);
      logger.debug("count on queue {} is {}", queue, value);
      return value;
   }

   private static void consume(ConnectionFactory factory, String clientID, String subscriptionID, int start, int numberOfMessages, boolean expectEmpty) throws Exception {
      try (Connection connection = factory.createConnection()) {
         connection.setClientID(clientID);
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         Topic topic = session.createTopic("order");
         connection.start();
         MessageConsumer consumer = session.createDurableConsumer(topic, subscriptionID);
         boolean failed = false;

         for (int i = start; i < start + numberOfMessages; i++) {
            TextMessage message = (TextMessage) consumer.receive(10_000);
            Assert.assertNotNull(message);
            logger.debug("Received message {}, large={}", message.getIntProperty("i"), message.getBooleanProperty("large"));
            if (message.getIntProperty("i") != i) {
               failed = true;
               logger.warn("Expected message {} but got {}", i, message.getIntProperty("i"));
            }
            if (message.getBooleanProperty("large")) {
               Assert.assertEquals(largeBody, message.getText());
            } else {
               Assert.assertEquals(smallBody, message.getText());
            }
            logger.debug("Consumed {}, large={}", i, message.getBooleanProperty("large"));
         }
         session.commit();

         Assert.assertFalse(failed);

         if (expectEmpty) {
            Assert.assertNull(consumer.receiveNoWait());
         }
      }

   }

}
