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

package org.apache.activemq.artemis.tests.soak.interruptlm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.File;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.management.ObjectNameBuilder;
import org.apache.activemq.artemis.api.core.management.QueueControl;
import org.apache.activemq.artemis.tests.soak.SoakTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.utils.Wait;
import org.apache.activemq.artemis.cli.commands.helper.HelperCreate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusteredLargeMessageInterruptTest extends SoakTestBase {
   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   public static final String SERVER_NAME_0 = "lmbroker1";
   public static final String SERVER_NAME_1 = "lmbroker2";

   @BeforeAll
   public static void createServers() throws Exception {
      {
         File serverLocation = getFileServerLocation(SERVER_NAME_0);
         deleteDirectory(serverLocation);

         HelperCreate cliCreateServer = helperCreate();
         cliCreateServer.setRole("amq").setUser("artemis").setPassword("artemis").setAllowAnonymous(true).setNoWeb(false).setArtemisInstance(serverLocation).
            setConfiguration("./src/main/resources/servers/lmbroker1");
         cliCreateServer.setArgs("--java-options", "-Djava.rmi.server.hostname=localhost", "--clustered", "--static-cluster", "tcp://localhost:61716", "--queues", "ClusteredLargeMessageInterruptTest", "--name", "lmbroker1");
         cliCreateServer.createServer();
      }

      {
         File serverLocation = getFileServerLocation(SERVER_NAME_1);
         deleteDirectory(serverLocation);

         HelperCreate cliCreateServer = helperCreate();
         cliCreateServer.setRole("amq").setUser("artemis").setPassword("artemis").setAllowAnonymous(true).setNoWeb(false).setArtemisInstance(serverLocation).
            setConfiguration("./src/main/resources/servers/lmbroker2").setPortOffset(100);
         cliCreateServer.setArgs("--java-options", "-Djava.rmi.server.hostname=localhost", "--clustered", "--static-cluster", "tcp://localhost:61616", "--queues", "ClusteredLargeMessageInterruptTest", "--name", "lmbroker2");
         cliCreateServer.createServer();
      }
   }

   private static final String JMX_SERVER_HOSTNAME = "localhost";
   private static final int JMX_SERVER_PORT_0 = 1099;
   private static final int JMX_SERVER_PORT_1 = 1199;

   static String server1URI = "service:jmx:rmi:///jndi/rmi://" + JMX_SERVER_HOSTNAME + ":" + JMX_SERVER_PORT_0 + "/jmxrmi";
   static ObjectNameBuilder builderServer1 = ObjectNameBuilder.create(ActiveMQDefaultConfiguration.getDefaultJmxDomain(), "lmbroker1", true);

   static String server2URI = "service:jmx:rmi:///jndi/rmi://" + JMX_SERVER_HOSTNAME + ":" + JMX_SERVER_PORT_1 + "/jmxrmi";
   static ObjectNameBuilder builderServer2 = ObjectNameBuilder.create(ActiveMQDefaultConfiguration.getDefaultJmxDomain(), "lmbroker2", true);

   private volatile boolean runningSend = true;
   private volatile boolean runningConsumer = true;
   private final AtomicInteger errors = new AtomicInteger(0);

   static final String largebody = createBody();
   static final int BODY_SIZE = 500 * 1024;

   private static String createBody() {
      StringBuffer buffer = new StringBuffer();
      while (buffer.length() < BODY_SIZE) {
         buffer.append("LOREM IPSUM WHATEVER THEY SAY IN THERE I DON'T REALLY CARE. I'M NOT SURE IF IT'S LOREM, LAUREM, LAUREN, IPSUM OR YPSUM AND I DON'T REALLY CARE ");
      }
      return buffer.toString();
   }

   Process serverProcess;
   Process serverProcess2;

   public ConnectionFactory createConnectionFactory(int broker, String protocol) {

      int portUsed = 61616 + broker * 100;

      if (protocol.equals("CORE")) {
         return new org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory("tcp://localhost:" + portUsed + "?ha=false&useTopologyForLoadBalancing=false&callTimeout=1000");
      } else {
         return CFUtil.createConnectionFactory(protocol, "tcp://localhost:" + portUsed);
      }
   }

   @BeforeEach
   public void before() throws Exception {
      cleanupData(SERVER_NAME_0);
      cleanupData(SERVER_NAME_1);
      serverProcess = startServer0();
      serverProcess2 = startServer1();
      disableCheckThread();
   }

   private Process startServer0() throws Exception {
      return startServer(SERVER_NAME_0, 0, 30000);
   }

   private Process startServer1() throws Exception {
      return startServer(SERVER_NAME_1, 100, 30000);
   }

   @Test
   public void testLargeMessageAMQPTX() throws Throwable {
      testInterrupt("AMQP", true);
   }

   @Test
   public void testInterruptAMQPNonTX() throws Throwable {
      testInterrupt("AMQP", false);
   }

   @Test
   public void testInterruptCORETX() throws Throwable {
      testInterrupt("CORE", true);
   }

   @Test
   public void testInterruptOPENWIRETX() throws Throwable {
      testInterrupt("OPENWIRE", true);
   }

   @Test
   public void testInterruptCORENonTX() throws Throwable {
      testInterrupt("CORE", false);
   }

   private CountDownLatch startSendingThreads(Executor executor, String protocol, int broker, int threads, boolean tx, String queueName) {
      runningSend = true;
      CountDownLatch done = new CountDownLatch(threads);

      ConnectionFactory factory = createConnectionFactory(broker, protocol);
      final CyclicBarrier startFlag = new CyclicBarrier(threads);

      for (int i = 0; i < threads; i++) {
         int threadID = i;
         executor.execute(() -> {
            int numberOfMessages = 0;
            try {
               Connection connection = factory.createConnection();
               Session session = connection.createSession(tx, tx ? Session.SESSION_TRANSACTED : Session.AUTO_ACKNOWLEDGE);
               MessageProducer producer = session.createProducer(session.createQueue(queueName));

               startFlag.await(10, TimeUnit.SECONDS);
               while (runningSend) {
                  producer.send(session.createTextMessage(largebody));
                  if (tx) {
                     session.commit();
                  }
                  if (numberOfMessages++ % 10 == 0) {
                     logger.info("Sent {}", numberOfMessages);
                  }
               }
            } catch (Exception e) {
               logger.info("Thread {} got an error {}", threadID, e.getMessage());
            } finally {
               done.countDown();
               logger.info("CountDown:: current Count {}", done.getCount());
            }
         });
      }

      return done;
   }


   private CountDownLatch startConsumingThreads(Executor executor, String protocol, int broker, int threads, boolean tx, String queueName) {
      runningConsumer = true;
      CountDownLatch done = new CountDownLatch(threads);

      ConnectionFactory factory = createConnectionFactory(broker, protocol);
      final CyclicBarrier startFlag = new CyclicBarrier(threads);

      for (int i = 0; i < threads; i++) {
         executor.execute(() -> {
            int numberOfMessages = 0;
            try (Connection connection = factory.createConnection()) {
               connection.start();
               Session session = connection.createSession(tx, tx ? Session.SESSION_TRANSACTED : Session.AUTO_ACKNOWLEDGE);
               MessageConsumer consumer = session.createConsumer(session.createQueue(queueName));

               startFlag.await(10, TimeUnit.SECONDS);
               while (runningConsumer) {
                  TextMessage message = (TextMessage)consumer.receive(100);
                  if (message != null) {
                     if (!message.getText().startsWith(largebody)) {
                        logger.warn("Body does not match!");
                        errors.incrementAndGet();
                     }
                     if (tx) {
                        session.commit();
                     }
                     if (numberOfMessages++ % 10 == 0) {
                        logger.info("Received {}", numberOfMessages);
                     }
                  }
               }
            } catch (Exception e) {
               logger.warn(e.getMessage(), e);
            } finally {
               logger.info("Done sending");
               done.countDown();
            }
         });
      }

      return done;
   }



   // this test has sleeps as the test will send while still active
   // we keep sending all the time.. so the testInterruptLM acts like a controller telling the threads when to stop
   private void testInterrupt(String protocol, boolean tx) throws Throwable {
      final int SENDING_THREADS = 10;
      final int CONSUMING_THREADS = 10;
      final AtomicInteger errors = new AtomicInteger(0); // I don't expect many errors since this test is disconnecting and reconnecting the server

      String queueName = "ClusteredLargeMessageInterruptTest";

      ExecutorService executorService = Executors.newFixedThreadPool(SENDING_THREADS + CONSUMING_THREADS);
      runAfter(executorService::shutdownNow);

      File lmFolder = new File(getServerLocation(SERVER_NAME_0) + "/data/large-messages");
      File lmFolder2 = new File(getServerLocation(SERVER_NAME_1) + "/data/large-messages");

      {
         CountDownLatch sendDone = startSendingThreads(executorService, protocol, 0, SENDING_THREADS, tx, queueName);
         CountDownLatch receiverDone = startConsumingThreads(executorService, protocol, 1, CONSUMING_THREADS, tx, queueName);

         // let it producing for a while
         Thread.sleep(2000);

         runningSend = false;
         assertTrue(sendDone.await(1, TimeUnit.MINUTES));

         killProcess(serverProcess);
         assertTrue(serverProcess.waitFor(1, TimeUnit.MINUTES));
         serverProcess = startServer0();
         runningConsumer = false;
         assertTrue(receiverDone.await(1, TimeUnit.MINUTES));

         long timeout = System.currentTimeMillis() + 60_000;

         ConnectionFactory factory = createConnectionFactory(1, protocol);

         // This will flush all messages, making sure everything is consumed.
         try (Connection connection = factory.createConnection()) {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(session.createQueue(queueName));
            connection.start();
            while (System.currentTimeMillis() < timeout) {
               TextMessage message = (TextMessage)consumer.receive(100);
               if (message == null) {
                  if (lmFolder.listFiles().length == 0 && lmFolder2.listFiles().length == 0) {
                     break;
                  }
               } else {
                  assertTrue(message.getText().startsWith(largebody));
               }
            }
         }

      }

      logger.info("All receivers and senders are done!!!");

      assertEquals(0, lmFolder.listFiles().length);
      assertEquals(0, lmFolder2.listFiles().length);
      assertEquals(0, errors.get());
   }

   @Test
   public void testBridgeFailureAMQP() throws Throwable {
      testInterruptFailOnBridge("AMQP", false);
   }

   @Test
   public void testBridgeFailureCORE() throws Throwable {
      testInterruptFailOnBridge("CORE", false);
   }

   private void killProcess(Process process) throws Exception {
      process.destroyForcibly();
   }


   // this is a slight variation of testInterruptLM where I switch over consumers before killing the previous node
   // this is to force messages being redistributed and try to get the bridge to failure.
   // I could played with a parameter but ellected to copy instead for simplicity
   private void testInterruptFailOnBridge(String protocol, boolean tx) throws Throwable {
      final int SENDING_THREADS = 10;
      final int CONSUMING_THREADS = 10;
      final AtomicInteger errors = new AtomicInteger(0); // I don't expect many errors since this test is disconnecting and reconnecting the server

      String queueName = "ClusteredLargeMessageInterruptTest";

      ExecutorService executorService = Executors.newFixedThreadPool(SENDING_THREADS + CONSUMING_THREADS);
      runAfter(executorService::shutdownNow);

      CountDownLatch sendDone = startSendingThreads(executorService, protocol, 0, SENDING_THREADS, tx, queueName);

      Thread.sleep(2000);

      runningSend = runningConsumer = false;

      killProcess(serverProcess);
      assertTrue(serverProcess.waitFor(1, TimeUnit.MINUTES));
      assertTrue(sendDone.await(1, TimeUnit.MINUTES));

      sendDone = startSendingThreads(executorService, protocol, 1, SENDING_THREADS, tx, queueName);
      CountDownLatch receiverDone = startConsumingThreads(executorService, protocol, 1, CONSUMING_THREADS, tx, queueName);
      killProcess(serverProcess);
      assertTrue(serverProcess.waitFor(1, TimeUnit.MINUTES));
      serverProcess = startServer0();

      Thread.sleep(5000);
      runningSend = false;
      assertTrue(sendDone.await(1, TimeUnit.MINUTES));

      QueueControl queueControl1 = getQueueControl(server1URI, builderServer1, queueName, queueName, RoutingType.ANYCAST, 5000);
      QueueControl queueControl2 = getQueueControl(server2URI, builderServer2, queueName, queueName, RoutingType.ANYCAST, 5000);

      File lmFolder = new File(getServerLocation(SERVER_NAME_0) + "/data/large-messages");
      File lmFolder2 = new File(getServerLocation(SERVER_NAME_1) + "/data/large-messages");
      Wait.waitFor(() -> lmFolder.listFiles().length == 0 && lmFolder2.listFiles().length == 0);
      Wait.assertTrue(() -> queueControl1.getMessageCount() == 0 && queueControl2.getMessageCount() == 0);

      runningConsumer = false;
      assertTrue(receiverDone.await(1, TimeUnit.MINUTES));


      Wait.assertEquals(0, () -> lmFolder.listFiles().length);
      Wait.assertEquals(0, () -> {
         logger.info("queueControl2.count={}", queueControl2.getMessageCount());
         return lmFolder2.listFiles().length;
      });
      assertEquals(0, errors.get());

   }


}