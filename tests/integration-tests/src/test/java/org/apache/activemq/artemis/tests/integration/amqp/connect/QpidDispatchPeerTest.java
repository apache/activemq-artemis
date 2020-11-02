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
package org.apache.activemq.artemis.tests.integration.amqp.connect;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.net.URL;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectConfiguration;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectionAddressType;
import org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectionElement;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.tests.integration.amqp.AmqpClientTestSupport;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.artemis.utils.ExecuteUtil;
import org.apache.activemq.artemis.utils.UUIDGenerator;
import org.jboss.logging.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** This test will only be executed if you have qdrouterd available on your system, otherwise is ignored by an assume exception. */
public class QpidDispatchPeerTest extends AmqpClientTestSupport {
   private static final Logger logger = Logger.getLogger(QpidDispatchPeerTest.class);

   ExecuteUtil.ProcessHolder qpidProcess;

   /**
    * This will validate if the environemnt has qdrouterd installed and if this test can be used or not.
    */
   @BeforeClass
   public static void validateqdrotuer() {
      try {
         int result = ExecuteUtil.runCommand(true, "qdrouterd", "--version");
         Assume.assumeTrue("qdrouterd does not exist", result == 0);
      } catch (Exception e) {
         logger.debug(e.getMessage(), e);
         Assume.assumeNoException("qdrouterd does not exist", e);
      }
   }

   @Override
   protected ActiveMQServer createServer() throws Exception {
      ActiveMQServer server = createServer(AMQP_PORT, false);
      server.getConfiguration().setNetworkCheckPeriod(100);
      return server;
   }

   @Before
   public void startQpidRouter() throws Exception {
      URL qpidConfig = this.getClass().getClassLoader().getResource("QpidRouterPeerTest-qpidr.conf");
      qpidProcess = ExecuteUtil.run(true, "qdrouterd", "-c", qpidConfig.getFile());
   }

   @After
   public void stopQpidRouter() throws Exception {
      qpidProcess.kill();
   }

   public void pauseThenKill(int timeToWait) throws Exception {
      int pid = qpidProcess.pid();
      int result = ExecuteUtil.runCommand(true, "kill", "-STOP", Long.toString(pid));
      Assert.assertEquals(0, result);
      logger.info("\n*******************************************************************************************************************************\n" +
                   "Paused" +
                   "\n*******************************************************************************************************************************");
      Thread.sleep(timeToWait);
      result = ExecuteUtil.runCommand(true, "kill", "-9", Long.toString(pid));
      Assert.assertEquals(0, result);
   }

   @Test(timeout = 60_000)
   public void testWithMatchingDifferentNamesOnQueueKill() throws Exception {
      internalMultipleQueues(true, true, true, false, false);
   }


   /** On this test the max reconnect attemps is reached. after a reconnect I will force a stop on the broker connection and retry it.
    *  The reconnection should succeed. */
   @Test(timeout = 60_000)
   public void testWithMatchingDifferentNamesOnQueueKillMaxAttempts() throws Exception {
      internalMultipleQueues(true, true, true, false, true);
   }

   @Test(timeout = 60_000)
   public void testWithMatchingDifferentNamesOnQueuePauseMaxAttempts() throws Exception {
      internalMultipleQueues(true, true, false, true, false);
   }

   @Test(timeout = 60_000)
   public void testWithMatchingDifferentNamesOnQueuePause() throws Exception {
      internalMultipleQueues(true, true, false, true, false);
   }

   @Test(timeout = 60_000)
   public void testWithMatchingDifferentNamesOnQueue() throws Exception {
      internalMultipleQueues(true, true, false, false, false);
   }

   @Test(timeout = 60_000)
   public void testWithMatching() throws Exception {
      internalMultipleQueues(true, false, false, false, false);
   }

   @Test(timeout = 60_000)
   public void testwithQueueName() throws Exception {
      internalMultipleQueues(false, false, false, false, false);
   }

   @Test(timeout = 60_000)
   public void testwithQueueNameDistinctName() throws Exception {
      internalMultipleQueues(false, true, false, false, false);
   }

   private void internalMultipleQueues(boolean useMatching, boolean distinctNaming, boolean kill, boolean pause, boolean maxReconnectAttemps) throws Exception {
      final int numberOfMessages = 100;
      final int numberOfQueues = 10;
      String brokerConnectionName = "brokerConnection." + UUIDGenerator.getInstance().generateStringUUID();
      AMQPBrokerConnectConfiguration amqpConnection = new AMQPBrokerConnectConfiguration(brokerConnectionName, "tcp://localhost:24622?amqpIdleTimeout=1000").setRetryInterval(10).setReconnectAttempts(maxReconnectAttemps ? 10 : -1);
      if (useMatching) {
         amqpConnection.addElement(new AMQPBrokerConnectionElement().setMatchAddress("queue.#").setType(AMQPBrokerConnectionAddressType.PEER));
      } else {
         for (int i = 0; i < numberOfQueues; i++) {
            amqpConnection.addElement(new AMQPBrokerConnectionElement().setQueueName(createQueueName(i, distinctNaming)).setType(AMQPBrokerConnectionAddressType.PEER));
         }
      }
      server.getConfiguration().addAMQPConnection(amqpConnection);
      server.start();
      for (int i = 0; i < numberOfQueues; i++) {
         server.addAddressInfo(new AddressInfo("queue.test" + i).addRoutingType(RoutingType.ANYCAST).setAutoCreated(false).setTemporary(false));
         server.createQueue(new QueueConfiguration(createQueueName(i, distinctNaming)).setAddress("queue.test" + i).setRoutingType(RoutingType.ANYCAST));
      }

      for (int dest = 0; dest < numberOfQueues; dest++) {
         ConnectionFactory factoryProducer = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:24622");
         Connection connection = null;

         connection = createConnectionDumbRetry(factoryProducer);

         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue queue = session.createQueue("queue.test" + dest);
         MessageProducer producer = session.createProducer(queue);
         producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

         org.apache.activemq.artemis.core.server.Queue testQueueOnServer = server.locateQueue(createQueueName(dest, distinctNaming));

         for (int i = 0; i < numberOfMessages; i++) {
            producer.send(session.createTextMessage("hello " + i));
         }

         Wait.assertEquals(numberOfMessages, testQueueOnServer::getMessageCount);
         connection.close();
      }

      if (kill) {
         qpidProcess.kill();
         if (maxReconnectAttemps) {
            Thread.sleep(1000); // wait some time so the connection is sure to have stopped retrying
         }
         startQpidRouter();
      } else if (pause) {
         pauseThenKill(3_000);
         startQpidRouter();
      }

      if (maxReconnectAttemps) {
         ConnectionFactory factoryConsumer = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:24622");
         Connection connection = createConnectionDumbRetry(factoryConsumer);
         connection.close();
         server.stopBrokerConnection(brokerConnectionName);
         server.startBrokerConnection(brokerConnectionName);
      }


      for (int dest = 0; dest < numberOfQueues; dest++) {
         ConnectionFactory factoryConsumer = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:24622");
         Connection connectionConsumer = createConnectionDumbRetry(factoryConsumer);
         Session sessionConsumer = connectionConsumer.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue queueConsumer = sessionConsumer.createQueue("queue.test" + dest);
         MessageConsumer consumer = sessionConsumer.createConsumer(queueConsumer);
         connectionConsumer.start();

         try {
            for (int i = 0; i < numberOfMessages; i++) {
               TextMessage received = (TextMessage) consumer.receive(5000);
               if (received == null) {
                  System.out.println("*******************************************************************************************************************************");
                  System.out.println("qdstat after message timed out:");
                  ExecuteUtil.runCommand(true, "qdstat", "-b", "127.0.0.1:24622", "-l");
                  System.out.println("*******************************************************************************************************************************");
               }
               Assert.assertNotNull(received);
               Assert.assertEquals("hello " + i, received.getText());
            }
            Assert.assertNull(consumer.receiveNoWait());
         } finally {
            try {
               connectionConsumer.close();
            } catch (Throwable ignored) {

            }
         }
         org.apache.activemq.artemis.core.server.Queue testQueueOnServer = server.locateQueue(createQueueName(dest, distinctNaming));
         Wait.assertEquals(0, testQueueOnServer::getMessageCount);
      }
   }

   private String createQueueName(int i, boolean useDistinctName) {
      if (useDistinctName) {
         return "distinct.test" + i;
      } else {
         return "queue.test" + i;
      }
   }

   private Connection createConnectionDumbRetry(ConnectionFactory factoryProducer) throws InterruptedException {
      for (int i = 0; i < 100; i++) {
         try {
            // Some retry
            return factoryProducer.createConnection();
         } catch (Exception e) {
            Thread.sleep(10);
         }
      }
      return null;
   }

}
