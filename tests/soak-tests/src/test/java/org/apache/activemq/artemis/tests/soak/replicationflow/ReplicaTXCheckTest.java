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

package org.apache.activemq.artemis.tests.soak.replicationflow;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.tests.soak.SoakTestBase;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicaTXCheckTest  extends SoakTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   public static final String SERVER_NAME_0 = "replica-tx-check/replicated-static0";
   public static final String SERVER_NAME_1 = "replica-tx-check/replicated-static1";
   public static final String SERVER_NAME_2 = "replica-tx-check/standalone";

   ArrayList<ReplicationFlowControlTest.Consumer> consumers = new ArrayList<>();
   private static Process server0;
   private static Process server1;
   private static Process server2;

   int NUMBER_OF_MESSAGES = 300;
   int KILL_AT = 100;

   @Before
   public void before() throws Exception {
      cleanupData(SERVER_NAME_0);
      cleanupData(SERVER_NAME_1);
      cleanupData(SERVER_NAME_2);
      disableCheckThread();

      server0 = startServer(SERVER_NAME_0, 0, 30000);
      server1 = startServer(SERVER_NAME_1, 1, 0);
      server2 = startServer(SERVER_NAME_2, -1, 30000);
   }

   @After
   @Override
   public void after() throws Exception {
      super.after();
   }

   @Test
   public void testTXCheckAMQP() throws Exception {
      testTXCheck("AMQP");
   }

   @Test
   public void testTXCheckCORE() throws Exception {
      testTXCheck("CORE");
   }

   void testTXCheck(String protocol) throws Exception {

      ConnectionFactory replicaPairCF;
      ConnectionFactory standaloneSource;

      switch(protocol) {
         case "AMQP":
            replicaPairCF = new JmsConnectionFactory("failover:(amqp://localhost:61616,amqp://localhost:61617)");
            standaloneSource = new JmsConnectionFactory("amqp://localhost:61615");
            break;
         case "CORE":
         default:
            replicaPairCF = new ActiveMQConnectionFactory("tcp://localhost:61616?ha=true&reconnectAttempts=-1");
            standaloneSource = new ActiveMQConnectionFactory("tcp://localhost:61615");
      }

      try (Connection sourceConnetion = standaloneSource.createConnection()) {
         Session session = sourceConnetion.createSession(true, Session.SESSION_TRANSACTED);
         Queue queue = session.createQueue("exampleQueue");
         MessageProducer producer = session.createProducer(queue);
         for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
            producer.send(session.createTextMessage("hello " + i));
         }
         session.commit();
      }

      try (Connection sourceConnection = standaloneSource.createConnection();
           Session sourceSession = sourceConnection.createSession(true, Session.SESSION_TRANSACTED);
           Connection targetConnection = replicaPairCF.createConnection();
           Session targetSession = targetConnection.createSession(true, Session.SESSION_TRANSACTED)) {

         sourceConnection.start();
         targetConnection.start();
         MessageConsumer consumer = sourceSession.createConsumer(targetSession.createQueue("exampleQueue"));

         Topic topic = targetSession.createTopic("exampleTopic");
         MessageConsumer subscription = targetSession.createSharedDurableConsumer(topic, "durable-consumer");
         MessageProducer producer = targetSession.createProducer(topic);

         int i = 0;
         while (true) {
            try {
               TextMessage message = (TextMessage)consumer.receive(5000);
               i++;
               if (message == null) {
                  logger.info("repeating receive i={}", i);
                  i--;
                  continue;
               }
               producer.send(message);
               if (i % 10 == 0) {
                  logger.info("Commit {}", i);
                  targetSession.commit();
                  sourceSession.commit();
               }
               if (i == KILL_AT) {
                  server0.destroyForcibly();
                  server0.waitFor(10, TimeUnit.SECONDS);
               }
               if (message.getText().equals("hello " + (NUMBER_OF_MESSAGES - 1))) {
                  logger.info("got to the end");
                  break;
               }
            } catch (Exception e) {
               logger.warn(e.getMessage(), e);
               sourceSession.rollback();
               targetSession.rollback();
            }
         }

         int rec = 0;
         while (true) {
            TextMessage message = (TextMessage) subscription.receive(100);
            if (message == null) {
               logger.info("Received {} messages", rec);
               break;
            }
            rec++;
         }
         targetSession.commit();
         // we could receive duplicates, but not lose messages
         Assert.assertTrue(rec >= NUMBER_OF_MESSAGES);
      }
   }
}