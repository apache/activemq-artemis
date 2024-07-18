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
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.util.HashSet;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.apache.activemq.artemis.api.core.management.SimpleManagement;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectionAddressType;
import org.apache.activemq.artemis.tests.soak.SoakTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.util.ServerUtil;
import org.apache.activemq.artemis.utils.FileUtil;
import org.apache.activemq.artemis.utils.TestParameters;
import org.apache.activemq.artemis.utils.Wait;
import org.apache.activemq.artemis.utils.cli.helper.HelperCreate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class ReplicatedBothNodesMirrorTest extends SoakTestBase {

   private static final String TEST_NAME = "REPLICATED_BOTH_NODES_MIRROR_SOAK";

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   // Set this to true and log4j will be configured with some relevant log.trace for the AckManager at the server's
   private static final boolean TRACE_LOGS = Boolean.parseBoolean(TestParameters.testProperty(TEST_NAME, "TRACE_LOGS", "false"));
   private static final int NUMBER_MESSAGES = TestParameters.testProperty(TEST_NAME, "NUMBER_MESSAGES", 200);

   private static final boolean REUSE_SERVERS = Boolean.parseBoolean(TestParameters.testProperty(TEST_NAME, "REUSE_SERVERS", "false"));

   private static final int SEND_COMMIT = TestParameters.testProperty(TEST_NAME, "SEND_COMMIT", 50);

   /*
    * Time each consumer takes to process a message received to allow some messages accumulating.
    * This sleep happens right before the commit.
    */
   private static final String QUEUE_NAME = "queueTest";

   private static String body;

   static {
      StringWriter writer = new StringWriter();
      while (writer.getBuffer().length() < 30 * 1024) {
         writer.append("The sky is blue, ..... watch out for poop from the birds though!...");
      }
      body = writer.toString();
   }

   public static final String DC1_NODE = "ReplicatedBothNodesMirrorTest/DC1";
   public static final String DC2_NODE = "ReplicatedBothNodesMirrorTest/DC2";
   public static final String DC2_REPLICA_NODE = "ReplicatedBothNodesMirrorTest/DC2_REPLICA";
   public static final String DC1_REPLICA_NODE = "ReplicatedBothNodesMirrorTest/DC1_REPLICA";

   volatile Process processDC1;
   volatile Process processDC2;
   volatile Process processDC1_REPLICA;
   volatile Process processDC2_REPLICA;

   @AfterEach
   public void destroyServers() throws Exception {
      if (processDC2_REPLICA != null) {
         processDC2_REPLICA.destroyForcibly();
         processDC2_REPLICA.waitFor(1, TimeUnit.MINUTES);
         processDC2_REPLICA = null;
      }
      if (processDC1_REPLICA != null) {
         processDC1_REPLICA.destroyForcibly();
         processDC1_REPLICA.waitFor(1, TimeUnit.MINUTES);
         processDC1_REPLICA = null;
      }
      if (processDC1 != null) {
         processDC1.destroyForcibly();
         processDC1.waitFor(1, TimeUnit.MINUTES);
         processDC1 = null;
      }
      if (processDC2 != null) {
         processDC2.destroyForcibly();
         processDC2.waitFor(1, TimeUnit.MINUTES);
         processDC2 = null;
      }
   }

   private static final String DC1_IP = "localhost:61616";
   private static final String DC1_BACKUP_IP = "localhost:61617";
   private static final String DC2_IP = "localhost:61618";
   private static final String DC2_BACKUP_IP = "localhost:61619";

   private static String uri(String ip) {
      return "tcp://" + ip;
   }
   private static String uriWithAlternate(String ip, String alternate) {
      return "tcp://" + ip + "#tcp://" + alternate;
   }

   private void startDC2(SimpleManagement managementDC2) throws Exception {
      processDC2 = startServer(DC2_NODE, -1, -1, new File(getServerLocation(DC2_NODE), "broker.properties"));
      ServerUtil.waitForServerToStart(2, 10_000);
      processDC2_REPLICA = startServer(DC2_REPLICA_NODE, -1, -1, new File(getServerLocation(DC2_REPLICA_NODE), "broker.properties"));
      Wait.assertTrue(managementDC2::isReplicaSync);
   }

   private void startDC1(SimpleManagement managementDC1) throws Exception {
      processDC1 = startServer(DC1_NODE, -1, -1, new File(getServerLocation(DC1_NODE), "broker.properties"));
      ServerUtil.waitForServerToStart(0, 10_000);
      processDC1_REPLICA = startServer(DC1_REPLICA_NODE, -1, -1, new File(getServerLocation(DC1_REPLICA_NODE), "broker.properties"));
      Wait.assertTrue(managementDC1::isReplicaSync);
   }

   private static void createMirroredServer(String serverName,
                                    String connectionName,
                                    String mirrorURI,
                                    int porOffset,
                                    boolean replicated,
                                    String clusterStatic) throws Exception {
      File serverLocation = getFileServerLocation(serverName);
      if (REUSE_SERVERS && serverLocation.exists()) {
         deleteDirectory(new File(serverLocation, "data"));
         return;
      }
      deleteDirectory(serverLocation);

      HelperCreate cliCreateServer = new HelperCreate();
      cliCreateServer.setAllowAnonymous(true).setArtemisInstance(serverLocation);
      cliCreateServer.setNoWeb(true);
      cliCreateServer.setArgs("--no-stomp-acceptor", "--no-hornetq-acceptor", "--no-mqtt-acceptor", "--no-amqp-acceptor", "--max-hops", "1", "--name", DC1_NODE);
      cliCreateServer.addArgs("--queues", QUEUE_NAME);
      cliCreateServer.setPortOffset(porOffset);
      if (replicated) {
         cliCreateServer.setReplicated(true);
         cliCreateServer.setStaticCluster(clusterStatic);
         cliCreateServer.setClustered(true);
      } else {
         cliCreateServer.setClustered(false);
      }

      cliCreateServer.createServer();

      Properties brokerProperties = new Properties();
      brokerProperties.put("messageExpiryScanPeriod", "1000");
      brokerProperties.put("AMQPConnections." + connectionName + ".uri", mirrorURI);
      brokerProperties.put("AMQPConnections." + connectionName + ".retryInterval", "1000");
      brokerProperties.put("AMQPConnections." + connectionName + ".type", AMQPBrokerConnectionAddressType.MIRROR.toString());
      brokerProperties.put("AMQPConnections." + connectionName + ".connectionElements.mirror.sync", "false");
      brokerProperties.put("largeMessageSync", "false");

      brokerProperties.put("addressSettings.#.maxSizeMessages", "50");
      brokerProperties.put("addressSettings.#.maxReadPageMessages", "2000");
      brokerProperties.put("addressSettings.#.maxReadPageBytes", "-1");
      brokerProperties.put("addressSettings.#.prefetchPageMessages", "500");
      // if we don't use pageTransactions we may eventually get a few duplicates
      brokerProperties.put("mirrorPageTransaction", "true");
      File brokerPropertiesFile = new File(serverLocation, "broker.properties");
      saveProperties(brokerProperties, brokerPropertiesFile);

      File brokerXml = new File(serverLocation, "/etc/broker.xml");
      assertTrue(brokerXml.exists());
      // Adding redistribution delay to broker configuration
      assertTrue(FileUtil.findReplace(brokerXml, "<address-setting match=\"#\">", "<address-setting match=\"#\">\n\n" + "            <redistribution-delay>0</redistribution-delay>\n"));
      assertTrue(FileUtil.findReplace(brokerXml, "<page-size-bytes>10M</page-size-bytes>", "<page-size-bytes>100K</page-size-bytes>"));

      if (TRACE_LOGS) {
         replaceLogs(serverLocation);
      }

   }

   private static void replaceLogs(File serverLocation) throws Exception {
      File log4j = new File(serverLocation, "/etc/log4j2.properties");
      assertTrue(FileUtil.findReplace(log4j, "logger.artemis_utils.level=INFO", "logger.artemis_utils.level=INFO\n" + "\n" + "logger.endpoint.name=org.apache.activemq.artemis.core.replication.ReplicationEndpoint\n" + "logger.endpoint.level=DEBUG\n" + "appender.console.filter.threshold.type = ThresholdFilter\n" + "appender.console.filter.threshold.level = info"));
   }

   private static void createMirroredBackupServer(String serverName, int porOffset, String clusterStatic, String mirrorURI) throws Exception {
      File serverLocation = getFileServerLocation(serverName);
      if (REUSE_SERVERS && serverLocation.exists()) {
         deleteDirectory(new File(serverLocation, "data"));
         return;
      }
      deleteDirectory(serverLocation);

      HelperCreate cliCreateServer = new HelperCreate();
      cliCreateServer.setAllowAnonymous(true).setArtemisInstance(serverLocation);
      cliCreateServer.setMessageLoadBalancing("ON_DEMAND");
      cliCreateServer.setNoWeb(true);
      cliCreateServer.setArgs("--no-stomp-acceptor", "--no-hornetq-acceptor", "--no-mqtt-acceptor", "--no-amqp-acceptor", "--max-hops", "1", "--name", DC1_NODE);
      cliCreateServer.setPortOffset(porOffset);
      cliCreateServer.setClustered(true);
      cliCreateServer.setReplicated(true);
      cliCreateServer.setBackup(true);
      cliCreateServer.setStaticCluster(clusterStatic);
      cliCreateServer.createServer();

      Properties brokerProperties = new Properties();
      brokerProperties.put("messageExpiryScanPeriod", "1000");
      brokerProperties.put("AMQPConnections.mirror.uri", mirrorURI);
      brokerProperties.put("AMQPConnections.mirror.retryInterval", "1000");
      brokerProperties.put("AMQPConnections.mirror.type", AMQPBrokerConnectionAddressType.MIRROR.toString());
      brokerProperties.put("AMQPConnections.mirror.connectionElements.mirror.sync", "false");
      brokerProperties.put("largeMessageSync", "false");

      brokerProperties.put("addressSettings.#.maxSizeMessages", "1");
      brokerProperties.put("addressSettings.#.maxReadPageMessages", "2000");
      brokerProperties.put("addressSettings.#.maxReadPageBytes", "-1");
      brokerProperties.put("addressSettings.#.prefetchPageMessages", "500");
      // if we don't use pageTransactions we may eventually get a few duplicates
      brokerProperties.put("mirrorPageTransaction", "true");
      File brokerPropertiesFile = new File(serverLocation, "broker.properties");
      saveProperties(brokerProperties, brokerPropertiesFile);


      File brokerXml = new File(serverLocation, "/etc/broker.xml");
      assertTrue(brokerXml.exists());
      // Adding redistribution delay to broker configuration
      assertTrue(FileUtil.findReplace(brokerXml, "<address-setting match=\"#\">", "<address-setting match=\"#\">\n\n" + "            <redistribution-delay>0</redistribution-delay> <!-- added by SimpleMirrorSoakTest.java --> \n"));
      assertTrue(FileUtil.findReplace(brokerXml, "<page-size-bytes>10M</page-size-bytes>", "<page-size-bytes>100K</page-size-bytes>"));

      if (TRACE_LOGS) {
         replaceLogs(serverLocation);
      }
   }

   public static void createRealServers() throws Exception {
      createMirroredServer(DC1_NODE, "mirror", uriWithAlternate(DC2_IP, DC2_BACKUP_IP), 0, true, uri(DC1_BACKUP_IP));
      createMirroredBackupServer(DC1_REPLICA_NODE, 1, uri(DC1_IP), uriWithAlternate(DC2_IP, DC2_BACKUP_IP));
      createMirroredServer(DC2_NODE, "mirror", uriWithAlternate(DC1_IP, DC1_BACKUP_IP), 2, true, uri(DC2_BACKUP_IP));
      createMirroredBackupServer(DC2_REPLICA_NODE, 3, uri(DC2_IP), uriWithAlternate(DC1_IP, DC1_BACKUP_IP));
   }

   @Test
   public void testFailoverLaterStart() throws Exception {
      testMirror(true);
   }

   @Test
   public void testFailoverWhileMirroring() throws Exception {
      testMirror(false);
   }

   private void testMirror(boolean laterStart) throws Exception {
      createRealServers();

      SimpleManagement managementDC1 = new SimpleManagement(uri(DC1_IP), null, null);
      SimpleManagement managementDC2 = new SimpleManagement(uri(DC2_IP), null, null);

      startDC1(managementDC1);

      if (!laterStart) {
         startDC2(managementDC2);
      }

      runAfter(() -> managementDC1.close());
      runAfter(() -> managementDC2.close());

      sendMessages(QUEUE_NAME);

      processDC1.destroyForcibly();
      processDC1.waitFor(10, TimeUnit.SECONDS);

      if (laterStart) {
         startDC2(managementDC2);
      }

      // Mirror failover could challenge the order
      HashSet<Integer> receivedIDs = new HashSet<>();

      ConnectionFactory connectionFactoryDC2 = CFUtil.createConnectionFactory("amqp", uri(DC2_IP));
      try (Connection connection = connectionFactoryDC2.createConnection()) {
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         connection.start();
         MessageConsumer consumer = session.createConsumer(session.createQueue(QUEUE_NAME));
         for (int i = 0; i < NUMBER_MESSAGES; i++) {
            TextMessage message = (TextMessage) consumer.receive(30_000);
            Assertions.assertNotNull(message);
            receivedIDs.add(message.getIntProperty("i"));
            if (i > 0 && i % SEND_COMMIT == 0) {
               logger.info("Received {} messages", i);
               session.commit();
            }
         }

         session.commit();
      }

      Assertions.assertEquals(NUMBER_MESSAGES, receivedIDs.size());
      for (int i = 0; i < NUMBER_MESSAGES; i++) {
         Assertions.assertTrue(receivedIDs.contains(i));
      }
   }


   @Test
   public void testMultipleSenders() throws Exception {
      try {
         lsof();
      } catch (IOException e) {
         logger.warn("lsof is not available in this platform, we will ignore this test - {}", e.getMessage(), e);
         Assumptions.abort("lsof is not available");
      }
      createRealServers();

      SimpleManagement managementDC1 = new SimpleManagement(uri(DC1_IP), null, null);
      SimpleManagement managementDC2 = new SimpleManagement(uri(DC2_IP), null, null);

      startDC1(managementDC1);
      startDC2(managementDC2);

      runAfter(managementDC1::close);
      runAfter(managementDC2::close);

      int destinations = 5;
      ExecutorService executorService = Executors.newFixedThreadPool(destinations);
      runAfter(executorService::shutdownNow);

      CountDownLatch latch = new CountDownLatch(destinations);
      AtomicInteger errors = new AtomicInteger(0);

      for (int i = 0; i < destinations; i++) {
         String destination = "queue" + i;
         executorService.execute(() -> {
            try {
               sendMessages(destination);
            } catch (Throwable e) {
               logger.warn(e.getMessage(), e);
               errors.incrementAndGet();
            } finally {
               latch.countDown();
            }
         });
      }

      Assertions.assertTrue(latch.await(5, TimeUnit.MINUTES));

      int openFiles = lsof();

      logger.info("There are {} open files", openFiles);

      // lsof is showing a file descriptor associated with multiple threads. So it is expected to have quite a few repetitions
      // when the issue is happening we would have around 40k, 50k entries or a lot more if you add more messages.
      Assertions.assertTrue(openFiles < 4000, () -> "There was " + openFiles + " open files");
      Assertions.assertEquals(0, errors.get(), "There are errors on the senders");

   }

   private int lsof() throws IOException, InterruptedException {
      ProcessBuilder lsofBuilder = new ProcessBuilder();
      lsofBuilder.command("lsof", "-n", "-P");

      Process process = lsofBuilder.start();
      runAfter(process::destroyForcibly);

      InputStream inputStream = process.getInputStream();
      BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
      AtomicInteger filesCounter = new AtomicInteger();
      try (Stream<String> lines = reader.lines()) {
         lines.filter(line -> line.contains(basedir)).forEach(l -> {
            logger.info("file {}", l);
            filesCounter.incrementAndGet();
         });
      }
      Assertions.assertTrue(process.waitFor(10, TimeUnit.SECONDS));
      return filesCounter.get();
   }

   private static void sendMessages(String queueName) throws JMSException {
      ConnectionFactory connectionFactoryDC1A = CFUtil.createConnectionFactory("amqp", uri(DC1_IP));
      try (Connection connection = connectionFactoryDC1A.createConnection()) {
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         MessageProducer producer = session.createProducer(session.createQueue(queueName));
         for (int i = 0; i < NUMBER_MESSAGES; i++) {
            TextMessage message = session.createTextMessage(body);
            message.setIntProperty("i", i);
            message.setBooleanProperty("large", false);
            producer.send(message);
            if (i > 0 && i % SEND_COMMIT == 0) {
               logger.info("Sent {} messages on {}", i, queueName);
               session.commit();
            }
         }

         session.commit();
      }
   }
}