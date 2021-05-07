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

package org.apache.activemq.artemis.tests.smoke.mmfactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.management.ObjectNameBuilder;
import org.apache.activemq.artemis.api.core.management.QueueControl;
import org.apache.activemq.artemis.tests.smoke.common.SmokeTestBase;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.apache.activemq.artemis.utils.SpawnedVMSupport;
import org.apache.activemq.artemis.utils.Wait;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.jboss.logging.Logger;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class MMSFactoryTest extends SmokeTestBase {

   private static final Logger logger = Logger.getLogger(MMSFactoryTest.class);

   public static final String SERVER_NAME_0 = "mmfactory";
   private static final String JMX_SERVER_HOSTNAME = "localhost";
   private static final int JMX_SERVER_PORT = 11099;

   final String theprotocol;
   final int BATCH_SIZE;
   // how many times the server will be restarted
   final int restarts;
   // how many times the clients will run per restart
   final int clientRuns;
   // As the produces sends messages, a client will be killed every X messages. This is it!
   final int killClientEveryX;


   @Parameterized.Parameters(name = "protocol={0}, batchSize={1}, restarts={2}, clientRuns={3}, killEveryX={4}")
   public static Collection<Object[]> parameters() {
      return Arrays.asList(new Object[][]{{"CORE", 2000, 2, 2, 500}, {"AMQP", 2000, 2, 2, 500}});
   }

   public MMSFactoryTest(String protocol, int batchSize, int restarts, int clientRuns, int killClientEveryX) {
      this.theprotocol = protocol;
      this.BATCH_SIZE = batchSize;
      this.restarts = restarts;
      this.clientRuns = clientRuns;
      this.killClientEveryX = killClientEveryX;
   }


   public static void main(String[] arg) {

      try {
         Consumer consumer = new Consumer(arg[0], Integer.parseInt(arg[1]), arg[2], Integer.parseInt(arg[3]), arg[4], Integer.parseInt(arg[5]));
         //consumer.run();
         consumer.runListener();

         while (true) {
            Thread.sleep(10_000);
         }
      } catch (Throwable e) {
         System.exit(1);
      }
   }

   public static String getConsumerLog(int id) {
      return getServerLocation(SERVER_NAME_0) + "/data/" + "Consumer" + id + ".log";
   }

   public static ConnectionFactory createConnectionFactory(String protocol, String uri) {
      if (protocol.toUpperCase().equals("OPENWIRE")) {
         return new org.apache.activemq.ActiveMQConnectionFactory(uri);
      } else if (protocol.toUpperCase().equals("AMQP")) {

         if (uri.startsWith("tcp://")) {
            // replacing tcp:// by amqp://
            uri = "amqp" + uri.substring(3);
         }
         return new JmsConnectionFactory(uri);
      } else if (protocol.toUpperCase().equals("CORE") || protocol.toUpperCase().equals("ARTEMIS")) {
         return new org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory(uri);
      } else {
         throw new IllegalStateException("Unkown:" + protocol);
      }
   }

   Process startConsumerProcess(String protocol,
                                int slowTime,
                                String queueName,
                                int credits,
                                int consumerID) throws Exception {

      return SpawnedVMSupport.spawnVM(MMSFactoryTest.class.getName(), protocol, "" + slowTime, queueName, "" + credits, getConsumerLog(consumerID), "" + consumerID);
   }

   Process serverProcess;

   @Before
   public void before() throws Exception {
      cleanupData(SERVER_NAME_0);
      disableCheckThread();
      serverProcess = startServer(SERVER_NAME_0, 0, 30000);
   }

   @Test
   public void testMMSorting() throws Exception {
      for (int i = 0; i < restarts; i++) {
         logger.debug("*******************************************************************************************************************************");
         logger.debug("Starting " + clientRuns);
         logger.debug("*******************************************************************************************************************************");
         testMMSorting(clientRuns * i, clientRuns * (i + 1));

         stopServerWithFile(getServerLocation(SERVER_NAME_0));
         Thread.sleep(1000);

         try {
            serverProcess.destroyForcibly();
         } catch (Throwable ignored) {
         }
         serverProcess = startServer(SERVER_NAME_0, 0, 30000);
      }
   }

   public void testMMSorting(int countStart, int countEnd) throws Exception {

      JMXConnector jmxConnector = getJmxConnector(JMX_SERVER_HOSTNAME, JMX_SERVER_PORT);

      MBeanServerConnection mBeanServerConnection = jmxConnector.getMBeanServerConnection();
      String brokerName = "0.0.0.0";  // configured e.g. in broker.xml <broker-name> element
      ObjectNameBuilder objectNameBuilder = ObjectNameBuilder.create(ActiveMQDefaultConfiguration.getDefaultJmxDomain(), brokerName, true);
      //ActiveMQServerControl activeMQServerControl = MBeanServerInvocationHandler.newProxyInstance(mBeanServerConnection, objectNameBuilder.getActiveMQServerObjectName(), ActiveMQServerControl.class, false);

      ObjectName queueObjectName = objectNameBuilder.getQueueObjectName(SimpleString.toSimpleString("MMFactory"), SimpleString.toSimpleString("MMConsumer"), RoutingType.MULTICAST);
      QueueControl queueControl = MBeanServerInvocationHandler.newProxyInstance(mBeanServerConnection, queueObjectName, QueueControl.class, false);

      final int NUMBER_OF_CONSUMERS = 6;
      final int NUMBER_OF_FASTCONSUMERS = 0; // not using at the moment

      Process[] consumers = new Process[NUMBER_OF_CONSUMERS + NUMBER_OF_FASTCONSUMERS];
      int[] timeForConsumers = new int[NUMBER_OF_CONSUMERS + NUMBER_OF_FASTCONSUMERS];

      for (int i = 0; i < NUMBER_OF_CONSUMERS; i++) {
         timeForConsumers[i] = (i % 2 == 0 ? 200 : 300);
      }

      timeForConsumers[1] = 100;
      timeForConsumers[5] = 500;

      for (int i = NUMBER_OF_CONSUMERS; i < NUMBER_OF_CONSUMERS + NUMBER_OF_FASTCONSUMERS; i++) {
         timeForConsumers[i] = 0;
      }

      for (int i = 0; i < consumers.length; i++) {
         consumers[i] = startConsumerProcess(theprotocol, timeForConsumers[i], "MMFactory::MMConsumer", 100, i);
      }

      Process dlqProcess = startConsumerProcess(theprotocol, 0, "DLQ", 100, 1000);

      AtomicInteger retryNumber = new AtomicInteger(0);
      int expectedTotalSize = 0;

      ConnectionFactory factory = createConnectionFactory(theprotocol, "tcp://localhost:61616");
      try (Connection connection = factory.createConnection()) {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Topic queue = session.createTopic("MMFactory");
         MessageProducer mmsFactory = session.createProducer(queue);

         Topic controlTopic = session.createTopic("MMControl");


         String largeString;
         {
            StringBuffer largeStringBuffer = new StringBuffer();

            while (largeStringBuffer.length() < 10) {
               largeStringBuffer.append(RandomUtil.randomString());
            }
            largeString = largeStringBuffer.toString();
         }

         try {
            for (int run = countStart; run <= countEnd; run++) {
               AtomicInteger lastTime = new AtomicInteger((int)queueControl.getMessagesAcknowledged());
               for (int i = 0; i < BATCH_SIZE; i++) {
                  if (i > 0 && i % killClientEveryX == 0) {
                     System.out.println("REconnecting...");
                     logger.debug("Reconnecting...");
                     consumers[0].destroyForcibly();
                     consumers[0] = startConsumerProcess(theprotocol, timeForConsumers[0], "MMFactory::MMConsumer", 100, 0);
                     consumers[1].destroyForcibly();
                     consumers[1] = startConsumerProcess(theprotocol, timeForConsumers[1], "MMFactory::MMConsumer", 100, 1);
                     logger.debug("...Reconnected");
                     logger.debug("retry=" + retryNumber + ",sent=" + i + ", acked on this batch = " + (queueControl.getMessagesAcknowledged() - (retryNumber.get() * BATCH_SIZE * 2)) + ", total acked = " + queueControl.getMessagesAcknowledged());
                  }
                  TextMessage message = session.createTextMessage("This is blue " + largeString);
                  message.setStringProperty("color", "blue");
                  message.setIntProperty("i", i);
                  mmsFactory.send(message);

                  message = session.createTextMessage("This is red " + largeString);
                  message.setStringProperty("color", "red");
                  message.setIntProperty("i", i);
                  mmsFactory.send(message);

                  if (i % 10 == 0) {
                     logger.debug("Sending " + i + " run = " + run);
                  }

                  if (i == 0) {
                     waitForAckChange(queueControl, lastTime);
                  }
               }

               Session sessionControl = connection.createSession(true, Session.SESSION_TRANSACTED);
               MessageProducer producerControl = sessionControl.createProducer(controlTopic);
               Message controlmessage = sessionControl.createMessage();
               controlmessage.setStringProperty("control", "flush");
               producerControl.send(controlmessage);
               sessionControl.commit();
               sessionControl.close();

               Wait.assertTrue(() -> {
                  // We will wait a bit here until it's a least a bit closer to the whole Batch
                  if ((queueControl.getMessagesAcknowledged() + queueControl.getMessagesKilled() + queueControl.getMessagesExpired()) - (retryNumber.get() * BATCH_SIZE * 2) > (BATCH_SIZE * 2 - 500)) {
                     return true;
                  } else {
                     logger.debug("Received " + queueControl.getMessagesAcknowledged());
                     return false;
                  }
               }, 45_000, 1_000);

               expectedTotalSize += BATCH_SIZE * 2;
               Wait.assertEquals(expectedTotalSize, queueControl::getMessagesAdded);

               Wait.assertEquals(expectedTotalSize, () -> queueControl.getMessagesAcknowledged() + queueControl.getMessagesKilled());
               retryNumber.incrementAndGet();

               for (Process c : consumers) {
                  c.destroyForcibly();
               }
               for (int i = 0; i < consumers.length; i++) {
                  File file = new File(getConsumerLog(i));
                  if (!file.delete()) {
                     logger.debug("not possible to remove " + file);
                  }
               }
               for (int r = 0; r < consumers.length; r++) {
                  consumers[r] = startConsumerProcess(theprotocol, timeForConsumers[r], "MMFactory::MMConsumer", 100, r);
               }
            }

            Thread.sleep(1000);
         } finally {
            for (Process c : consumers) {
               c.destroyForcibly();
            }

            dlqProcess.destroyForcibly();

            for (int i = 0; i < consumers.length; i++) {
               File file = new File(getConsumerLog(i));
               if (!file.delete()) {
                  logger.warn("not possible to remove " + file);
               }
            }

            File file = new File(getConsumerLog(1000)); //the DLQ processing ID used
            if (!file.delete()) {
               logger.warn("not possible to remove " + file);
            }
         }
      }

   }

   private void waitForAckChange(QueueControl queueControl, AtomicInteger lastTime) throws Exception {
      Wait.waitFor(() -> {

         if (lastTime.get() == queueControl.getMessagesAcknowledged()) {
            logger.debug("Waiting some change on " + queueControl.getMessagesAcknowledged() + " with messages Added = " + queueControl.getMessagesAdded() + " and killed = " + queueControl.getMessagesKilled());
            return false;
         } else {
            logger.debug("Condition met! with " + queueControl.getMessagesAcknowledged() + " with messages Added = " + queueControl.getMessagesAdded() + " and killed = " + queueControl.getMessagesKilled());
            lastTime.set((int)queueControl.getMessagesAcknowledged());
            return true;
         }
      }, 5_000);
   }

   static class Consumer implements MessageListener {

      boolean clientAck = false;
      volatile int slowTime;
      final String queuename;
      final int credits;
      final String protocol;
      final int id;
      ConnectionFactory factory;
      Connection connection;
      Session session;
      MessageConsumer consumer;
      MessageConsumer controlConsumer;
      Queue queue;

      Session sessionControl;

      PrintStream fileStream;

      Consumer(String protocol, int slowTime, String queueName, int credits, String logOutput, int id) throws Exception {
         this.slowTime = slowTime;
         this.queuename = queueName;
         this.credits = credits;
         this.protocol = protocol;
         fileStream = new PrintStream(new FileOutputStream(logOutput, true), true);
         this.id = id;
      }

      @Override
      public void onMessage(Message message) {
         try {
            String color = message.getStringProperty("color");
            int messageSequence = message.getIntProperty("i");
            if (queuename.equals("DLQ")) {
               logger.debug("Processing DLQ on color=" + color + ", sequence=" + messageSequence);
            } else if (slowTime > 0) {
               Thread.sleep(slowTime);
            }

            if (clientAck) {
               message.acknowledge();
            }

            fileStream.println(color + ";" + messageSequence);
         } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
         }

      }

      class ControlListener implements MessageListener {

         @Override
         public void onMessage(Message message) {
            try {

               // This is received at the client, on a remote machine, System.out is the best option to log here
               System.out.println("Received control message");
               if (message.getStringProperty("control").equals("flush")) {
                  Consumer.this.slowTime = 0;
                  System.out.println("Setting slow time to 0");
               }
               sessionControl.commit();
            } catch (Throwable e) {
               e.printStackTrace();
            }
         }
      }

      public void runListener() {

         //factory = createConnectionFactory(protocol, "tcp://localhost:61616?jms.prefetchPolicy.queuePrefetch=" + credits);
         factory = createConnectionFactory(protocol, "tcp://localhost:61616");

         System.out.println("Starting");
         connect();
         try {
            consumer.setMessageListener(Consumer.this);
         } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
         }
      }

      private void connect() {
         try {
            if (connection != null) {
               connection.close();
            }

            connection = factory.createConnection();
            connection.start();
            session = connection.createSession(false, clientAck ? Session.CLIENT_ACKNOWLEDGE : Session.AUTO_ACKNOWLEDGE);
            queue = session.createQueue(queuename);
            consumer = session.createConsumer(queue);

            sessionControl = connection.createSession(true, Session.SESSION_TRANSACTED);
            Topic topic = sessionControl.createTopic("MMControl");
            controlConsumer = sessionControl.createSharedDurableConsumer(topic, "consumer" + id);
            controlConsumer.setMessageListener(new ControlListener());


         } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
         }
      }
   }

}
