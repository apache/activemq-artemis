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
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** This test will only be executed if you have qdrouterd available on your system, otherwise is ignored by an assume exception. */
public class QpidDispatchPeerTest extends AmqpClientTestSupport {

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
         e.printStackTrace();
         Assume.assumeNoException(e);
      }
   }

   @Override
   protected ActiveMQServer createServer() throws Exception {
      return createServer(AMQP_PORT, false);
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

   @Test(timeout = 60_000)
   public void testWithMatching() throws Exception {
      internalMultipleQueues(true);
   }

   @Test(timeout = 60_000)
   public void testwithQueueName() throws Exception {
      internalMultipleQueues(false);
   }

   private void internalMultipleQueues(boolean useMatching) throws Exception {
      final int numberOfMessages = 100;
      final int numberOfQueues = 10;
      AMQPBrokerConnectConfiguration amqpConnection = new AMQPBrokerConnectConfiguration("test", "tcp://localhost:24621").setRetryInterval(10).setReconnectAttempts(-1);
      if (useMatching) {
         amqpConnection.addElement(new AMQPBrokerConnectionElement().setMatchAddress("queue.#").setType(AMQPBrokerConnectionAddressType.PEER));
      } else {
         for (int i = 0; i < numberOfQueues; i++) {
            amqpConnection.addElement(new AMQPBrokerConnectionElement().setQueueName("queue.test" + i).setType(AMQPBrokerConnectionAddressType.PEER));
         }
      }
      server.getConfiguration().addAMQPConnection(amqpConnection);
      server.start();
      for (int i = 0; i < numberOfQueues; i++) {
         server.addAddressInfo(new AddressInfo("queue.test" + i).addRoutingType(RoutingType.ANYCAST).setAutoCreated(false).setTemporary(false));
         server.createQueue(new QueueConfiguration("queue.test" + i).setAddress("queue.test" + i).setRoutingType(RoutingType.ANYCAST));
      }

      for (int dest = 0; dest < numberOfQueues; dest++) {
         ConnectionFactory factoryProducer = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:24622");
         Connection connection = null;

         connection = createConnectionDumbRetry(factoryProducer, connection);

         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue queue = session.createQueue("queue.test" + dest);
         MessageProducer producer = session.createProducer(queue);
         producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

         org.apache.activemq.artemis.core.server.Queue testQueueOnServer = server.locateQueue("queue.test" + dest);

         for (int i = 0; i < numberOfMessages; i++) {
            producer.send(session.createTextMessage("hello " + i));
         }

         Wait.assertEquals(numberOfMessages, testQueueOnServer::getMessageCount);
         connection.close();
      }

      System.out.println("*******************************************************************************************************************************");
      System.out.println("Creating consumer");

      for (int dest = 0; dest < numberOfQueues; dest++) {
         ConnectionFactory factoryConsumer = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:24622");
         Connection connectionConsumer = factoryConsumer.createConnection();
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
               System.out.println("message " + received.getText());
               Assert.assertEquals("hello " + i, received.getText());
            }
            Assert.assertNull(consumer.receiveNoWait());
         } finally {
            try {
               connectionConsumer.close();
            } catch (Throwable ignored) {

            }
         }
         org.apache.activemq.artemis.core.server.Queue testQueueOnServer = server.locateQueue("queue.test" + dest);
         Wait.assertEquals(0, testQueueOnServer::getMessageCount);
      }

   }

   private Connection createConnectionDumbRetry(ConnectionFactory factoryProducer,
                                                Connection connection) throws InterruptedException {
      for (int i = 0; i < 100; i++) {
         try {
            // Some retry
            connection = factoryProducer.createConnection();
            break;
         } catch (Exception e) {
            Thread.sleep(10);
         }
      }
      return connection;
   }

}
