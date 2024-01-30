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

package org.apache.activemq.artemis.tests.soak.clientFailure;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.File;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.RedeliveryPolicy;
import org.apache.activemq.artemis.api.core.management.SimpleManagement;
import org.apache.activemq.artemis.tests.soak.SoakTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.utils.SpawnedVMSupport;
import org.apache.activemq.artemis.utils.Wait;
import org.apache.activemq.artemis.utils.cli.helper.HelperCreate;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.activemq.artemis.utils.TestParameters.testProperty;

@RunWith(Parameterized.class)
public class ClientFailureSoakTest extends SoakTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   Process serverProcess;

   public static final String SERVER_NAME_0 = "clientFailure";

   private static File brokerPropertiesFile;

   @BeforeClass
   public static void createServers() throws Exception {
      File serverLocation = getFileServerLocation(SERVER_NAME_0);
      deleteDirectory(serverLocation);
      HelperCreate cliCreateServer = new HelperCreate();
      cliCreateServer.setAllowAnonymous(true).setArtemisInstance(serverLocation);
      cliCreateServer.setArgs("--global-max-messages", "500000", "--java-options", "-ea", "--java-options", "-Xmx512M", "--queues", "CLIENT_TEST,OUT_QUEUE");
      cliCreateServer.createServer();

      // Creating a broker properties file instead of providing a new broker.xml just for these options
      Properties brokerProperties = new Properties();
      brokerProperties.put("addressesSettings.#.redeliveryDelay", "0");
      brokerProperties.put("addressesSettings.#.maxDeliveryAttempts", "-1");
      brokerPropertiesFile = new File(serverLocation, "broker.properties");
      saveProperties(brokerProperties, brokerPropertiesFile);
   }

   private static final String QUEUE_NAME = "CLIENT_TEST";

   private static final String TEST_NAME = "CLIENT_FAILURE";

   private final String protocol;
   private static final boolean TEST_ENABLED = Boolean.parseBoolean(testProperty(TEST_NAME, "TEST_ENABLED", "true"));
   private static final String PROTOCOL_LIST = testProperty(TEST_NAME, "PROTOCOL_LIST", "OPENWIRE,AMQP,CORE");

   private boolean USE_LARGE_MESSAGE;

   private final int THREADS_PER_VM;
   private final int CLIENT_CONSUMERS_PER_THREAD;
   private final int TEST_REPEATS;
   private final int TOTAL_ITERATIONS;
   private final int NUMBER_OF_VMS;
   private final int NUMBER_OF_MESSAGES;
   private final String MEMORY_CLIENT;

   @Parameterized.Parameters(name = "protocol={0}")
   public static Collection<Object[]> parameters() {
      String[] protocols = PROTOCOL_LIST.split(",");

      ArrayList<Object[]> parameters = new ArrayList<>();
      for (String str : protocols) {
         logger.info("Adding {} to the list for the test", str);
         parameters.add(new Object[]{str});
      }

      return parameters;
   }

   @Before
   public void before() throws Exception {
      Assume.assumeTrue(TEST_ENABLED);
      cleanupData(SERVER_NAME_0);

      serverProcess = startServer(SERVER_NAME_0, 0, 30_000, brokerPropertiesFile);
   }

   public ClientFailureSoakTest(String protocol) {
      this.protocol = protocol;

      THREADS_PER_VM = testProperty(TEST_NAME, protocol + "_THREADS_PER_VM", 6);
      USE_LARGE_MESSAGE = Boolean.valueOf(testProperty(TEST_NAME, protocol + "_USE_LARGE_MESSAGE", "false"));
      CLIENT_CONSUMERS_PER_THREAD = testProperty(TEST_NAME, protocol + "_CLIENT_CONSUMERS_PER_THREAD", 10);
      TEST_REPEATS = testProperty(TEST_NAME, protocol + "_TEST_REPEATS", 1);
      TOTAL_ITERATIONS = testProperty(TEST_NAME, protocol + "_TOTAL_ITERATION", 2);
      NUMBER_OF_VMS = testProperty(TEST_NAME, protocol + "_NUMBER_OF_VMS", 5);
      NUMBER_OF_MESSAGES = testProperty(TEST_NAME, protocol + "_NUMBER_OF_MESSAGES", 1_000);
      MEMORY_CLIENT = testProperty(TEST_NAME, protocol + "_MEMORY_CLIENT", "-Xmx128m");
   }

   @Test
   public void testSoakClientFailures() throws Exception {
      SimpleManagement simpleManagement = new SimpleManagement("tcp://localhost:61616", null, null);

      ConnectionFactory factory = CFUtil.createConnectionFactory(protocol, "tcp://localhost:61616");
      if (protocol.equals("OPENWIRE")) {
         RedeliveryPolicy inifinitePolicy = new RedeliveryPolicy();
         inifinitePolicy.setMaximumRedeliveries(-1);
         ((ActiveMQConnectionFactory)factory).setRedeliveryPolicy(inifinitePolicy);
      }
      try (Connection connection = factory.createConnection()) {
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         Queue queue = session.createQueue(QUEUE_NAME);
         MessageProducer producer = session.createProducer(queue);

         String largeBody;

         {
            StringBuilder builder = new StringBuilder();
            while (builder.length() < 150 * 1024) {
               builder.append("This is a large string... LOREM IPSUM WHATEVER IT SAYS IN THAT THING... ");
            }
            largeBody = builder.toString();
         }

         for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
            Message message;
            if (i % 100 == 0 && USE_LARGE_MESSAGE) {
               message = session.createTextMessage(largeBody);
            } else {
               message = session.createTextMessage("text " + i);
            }
            message.setIntProperty("i", i);
            producer.send(message);
            if (i > 0 && i % 1000 == 0) {
               logger.info("Sent {} messages", i);
               session.commit();
            }
         }
         session.commit();
      }

      ExecutorService service = Executors.newFixedThreadPool(NUMBER_OF_VMS);
      runAfter(service::shutdownNow);

      AtomicInteger errors = new AtomicInteger(0);

      for (int testRepeat = 0; testRepeat < TEST_REPEATS; testRepeat++) {
         logger.info("\n*******************************************************************************************************************************" + "\nTest repeat {}" + "\n*******************************************************************************************************************************", testRepeat);

         CountDownLatch done = new CountDownLatch(NUMBER_OF_VMS);

         for (int i = 0; i < NUMBER_OF_VMS; i++) {
            int threadID = i;
            service.execute(() -> {
               try {
                  for (int it = 0; it < TOTAL_ITERATIONS; it++) {
                     logger.info("\n*******************************************************************************************************************************" + "\nThread {} iteration {}" + "\n*******************************************************************************************************************************", threadID, it);
                     Process process = SpawnedVMSupport.spawnVM(null, null, ClientFailureSoakTestClient.class.getName(), "-Xms128m", MEMORY_CLIENT, new String[]{}, true, true, protocol, String.valueOf(THREADS_PER_VM), String.valueOf(CLIENT_CONSUMERS_PER_THREAD), QUEUE_NAME);
                     logger.info("Started process");
                     Assert.assertTrue(process.waitFor(10, TimeUnit.HOURS));
                     Assert.assertEquals(ClientFailureSoakTestClient.RETURN_OK, process.exitValue());
                  }
               } catch (Throwable throwable) {
                  logger.warn(throwable.getMessage(), throwable);
                  errors.incrementAndGet();
               } finally {
                  done.countDown();
               }
            });
         }

         Assert.assertTrue(done.await(10, TimeUnit.HOURS));

         if (errors.get() != 0) {
            logger.warn("There were errors in previous executions:: {}. We will look into the receiving part now, but beware of previous errors", errors.get());
         } else {
            logger.info("No errors on any previous execution, checking consumer now");
         }

         int outOfOrder = 0;

         try {
            Wait.assertEquals(0, () -> simpleManagement.getDeliveringCountOnQueue(QUEUE_NAME), 60_000, 100);
            Wait.assertEquals(0, () -> simpleManagement.getNumberOfConsumersOnQueue(QUEUE_NAME), 60_000, 100);
            Wait.assertEquals((long) NUMBER_OF_MESSAGES, () -> simpleManagement.getMessageCountOnQueue(QUEUE_NAME), 60_000, 500);
         } catch (Throwable e) {
            logger.warn(e.getMessage(), e);
            errors.incrementAndGet();
         }

         try (Connection connection = factory.createConnection()) {
            connection.start();
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queue = session.createQueue(QUEUE_NAME);
            MessageConsumer consumer = session.createConsumer(queue);

            Wait.assertEquals(1, () -> simpleManagement.getNumberOfConsumersOnQueue(QUEUE_NAME), 60_000, 100);

            HashSet<Integer> receivedIDs = new HashSet<>();

            for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
               Message message = consumer.receive(60_000);
               Assert.assertNotNull(message);

               if (!receivedIDs.add(message.getIntProperty("i"))) {
                  logger.warn("Message {} received in duplicate", message.getIntProperty("i"));
                  Assert.fail("Message " + message.getIntProperty("i") + " received in duplicate");
               }

               if (i != message.getIntProperty("i")) {
                  Assert.fail("Message " + message.getIntProperty("i") + " received out of order, when it was supposed to be " + i + " with body size = " + ((TextMessage) message).getText().length());
                  logger.info("message {} received out of order. Expected {}", message.getIntProperty("i"), i);
                  outOfOrder++;
               }

               if (i % 1000 == 0) {
                  logger.info("Received {} messages with {} outOfOrder", i, outOfOrder);
               }
            }
            logger.info("Received {} messages outOfOrder", outOfOrder);
            Assert.assertNull(consumer.receiveNoWait());
            session.rollback();
            Assert.assertEquals(0, outOfOrder);
         }

         Wait.assertEquals(0, () -> simpleManagement.getDeliveringCountOnQueue(QUEUE_NAME), 10_000, 100);
         Wait.assertEquals(0, () -> simpleManagement.getNumberOfConsumersOnQueue(QUEUE_NAME), 10_000, 100);
         Wait.assertEquals((long) NUMBER_OF_MESSAGES, () -> simpleManagement.getMessageCountOnQueue(QUEUE_NAME), 10_000, 500);
         Assert.assertEquals("There were errors in the consumers", 0, errors.get());
      }
   }

}
