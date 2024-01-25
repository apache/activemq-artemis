/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.soak.clusterNotificationsContinuity;

import javax.jms.Connection;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.Session;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQDestination;
import org.apache.activemq.artemis.tests.soak.SoakTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.artemis.utils.cli.helper.HelperCreate;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.jms.connection.CachingConnectionFactory;
import org.springframework.jms.listener.DefaultMessageListenerContainer;
import org.springframework.util.backoff.FixedBackOff;

import static org.apache.activemq.artemis.utils.TestParameters.testProperty;

/**
 * Refer to ./scripts/parameters.sh for suggested parameters
 *
 * Tests for an issue that's dependent on high overall system load.
 * The following parameters are used to tune the resource demands of the test:
 * NUMBER_OF_SERVERS, NUMBER_OF_QUEUES, NUMBER_OF_CONSUMERS
 *
 */

public class ClusterNotificationsContinuityTest extends SoakTestBase {

   public static final String SERVER_NAME_BASE = "cncBroker-";
   public static final int SERVER_PORT_BASE = 61616;
   private static final String TEST_NAME = "CLUSTER_NOTIFICATIONS_CONTINUITY";
   private static final boolean TEST_ENABLED = Boolean.parseBoolean(testProperty(TEST_NAME, "TEST_ENABLED", "true"));

   private static final int NUMBER_OF_SERVERS = testProperty(TEST_NAME, "NUMBER_OF_SERVERS", 3);
   private final int NUMBER_OF_QUEUES = testProperty(TEST_NAME, "NUMBER_OF_QUEUES", 500);
   private final int NUMBER_OF_CONSUMERS = testProperty(TEST_NAME, "NUMBER_OF_CONSUMERS", 20);;
   private final Process[] serverProcesses = new Process[NUMBER_OF_SERVERS];

   @BeforeClass
   public static void createServers() throws Exception {
      String configTemplate = Files.readString(
         Path.of("./src/main/resources/servers/clusterNotificationsContinuity/template.broker.xml"));

      for (int i = 0; i < NUMBER_OF_SERVERS; i++) {
         String serverName = SERVER_NAME_BASE + i;
         String serverPort = String.valueOf(SERVER_PORT_BASE + i);

         String brokerConfig = configTemplate
            .replaceAll("BROKER_NAME", serverName)
            .replaceAll("BROKER_PORT", serverPort);

         File serverLocation = getFileServerLocation(serverName);
         deleteDirectory(serverLocation);

         String serverConfDir = serverLocation.getPath() + "/config";
         Files.createDirectories(Path.of(serverConfDir));

         Files.writeString(Path.of(serverConfDir + "/broker.xml"), brokerConfig);

         HelperCreate cliCreateServer = new HelperCreate();
         cliCreateServer
            .setRole("amq")
            .setUser("admin")
            .setPassword("admin")
            .setAllowAnonymous(true)
            .setNoWeb(true)
            .setArtemisInstance(serverLocation)
            .setConfiguration(serverConfDir);

         cliCreateServer.createServer();
      }
   }

   @Before
   public void before() throws Exception {
      Assume.assumeTrue(TEST_ENABLED);
      for (int i = 0; i < NUMBER_OF_SERVERS; i++) {
         String serverName = SERVER_NAME_BASE + i;

         cleanupData(serverName);
         serverProcesses[i] = startServer(serverName, i, 30_000);
      }
   }

   @Test
   public void testClusterNotificationsContinuity() throws Throwable {
      final ActiveMQConnectionFactory factory = (ActiveMQConnectionFactory) CFUtil.createConnectionFactory("CORE", "tcp://localhost:61616");
      final String queueNamePrefix = "TEST.QUEUE.";
      ExecutorService executorService = Executors.newCachedThreadPool();
      CountDownLatch latch = new CountDownLatch(NUMBER_OF_QUEUES * 2);
      List<DefaultMessageListenerContainer> containers = new ArrayList<>();

      //wait for cluster formation
      try (Connection ignored = factory.createConnection()) {
         Wait.assertEquals(NUMBER_OF_SERVERS, () -> factory.getServerLocator().getTopology().getMembers().size(), 5000);
      }

      for (int i = 0; i < NUMBER_OF_QUEUES; i++) {
         Queue queue = ActiveMQDestination.createQueue(queueNamePrefix + i + "-");

         executorService.submit(() -> {
            try (Connection connection = factory.createConnection();
                 Session session = connection.createSession(Session.SESSION_TRANSACTED)) {

               session.createProducer(queue).send(session.createTextMessage("Message"));
               session.commit();
               latch.countDown();
            } catch (Exception e) { }
         });

         DefaultMessageListenerContainer container = new DefaultMessageListenerContainer();
         container.setCacheLevelName("CACHE_NONE");
         container.setSessionTransacted(true);
         container.setSessionAcknowledgeModeName("SESSION_TRANSACTED");
         container.setConcurrentConsumers(NUMBER_OF_CONSUMERS);
         container.setConnectionFactory(new CachingConnectionFactory(factory));
         container.setDestinationName(queue.getQueueName());
         container.setBackOff(new FixedBackOff(1000, 2));
         container.setReceiveTimeout(100);
         container.setMessageListener((MessageListener) msg -> {
            latch.countDown();
         });

         container.initialize();
         container.start();
         containers.add(container);
      }

      assertTrue(latch.await(30_000, TimeUnit.MILLISECONDS));

      executorService.shutdown();
      containers.parallelStream().forEach(dmlc -> {
         try {
            dmlc.stop();
            Wait.waitFor(() -> dmlc.getActiveConsumerCount() == 0);
            dmlc.shutdown();
            ((CachingConnectionFactory) dmlc.getConnectionFactory()).destroy();
         } catch (Exception ignore) { }
      });

      for (int i = 0; i < NUMBER_OF_SERVERS; i++) {
         String serverName = SERVER_NAME_BASE + i;

         File artemisLog = new File("target/" + serverName + "/log/artemis.log");
         checkLogRecord(artemisLog, false, "AMQ224037");
      }

   }

   @After
   public void cleanup() {
      for (int i = 0; i < NUMBER_OF_SERVERS; i++) {
         serverProcesses[i].destroy();
         String serverName = SERVER_NAME_BASE + i;
         cleanupData(serverName);
      }
   }

}
