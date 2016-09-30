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
package org.apache.activemq.artemis.tests.extras.byteman;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.artemis.api.core.ActiveMQNotConnectedException;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.tests.util.JMSTestBase;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * GroupingTest
 */
@RunWith(BMUnitRunner.class)
public class GroupingTest extends JMSTestBase {

   private Queue queue;
   static boolean pause = false;

   @Before
   @Override
   public void setUp() throws Exception {
      super.setUp();

      queue = createQueue("TestQueue");
   }

   protected ConnectionFactory getCF() throws Exception {
      return cf;
   }

   @Test
   @BMRules(
      rules = {@BMRule(
         name = "trace clientsessionimpl commit",
         targetClass = "org.apache.activemq.artemis.core.server.impl.ServerSessionImpl",
         targetMethod = "rollback",
         targetLocation = "EXIT",
         action = "org.apache.activemq.artemis.tests.extras.byteman.GroupingTest.pause();")})
   public void testGroupingRollbackOnClose() throws Exception {
      Connection sendConnection = null;
      Connection connection = null;
      Connection connection2 = null;
      try {
         ActiveMQConnectionFactory fact = (ActiveMQConnectionFactory) getCF();
         fact.setReconnectAttempts(0);
         //fact.setConsumerWindowSize(1000);
         //fact.setTransactionBatchSize(0);
         connection = fact.createConnection();
         RemotingConnection rc = server.getRemotingService().getConnections().iterator().next();
         connection2 = fact.createConnection();
         sendConnection = fact.createConnection();

         final Session sendSession = sendConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         Session session2 = connection2.createSession(true, Session.SESSION_TRANSACTED);

         final MessageProducer producer = sendSession.createProducer(queue);

         MessageConsumer consumer1 = session.createConsumer(queue);
         MessageConsumer consumer2 = session2.createConsumer(queue);

         connection.start();
         connection2.start();

         Thread t = new Thread(new Runnable() {
            @Override
            public void run() {

               try {
                  for (int j = 0; j < 10000; j++) {
                     TextMessage message = sendSession.createTextMessage();

                     message.setText("Message" + j);

                     message.setStringProperty("JMSXGroupID", "foo");

                     producer.send(message);
                  }
               } catch (JMSException e) {
                  e.printStackTrace();
               }
            }
         });
         t.start();

         //consume 5 msgs from 1st first consumer
         for (int j = 0; j < 5; j++) {
            TextMessage tm = (TextMessage) consumer1.receive(10000);

            assertNotNull(tm);

            assertEquals("Message" + j, tm.getText());

            assertEquals(tm.getStringProperty("JMSXGroupID"), "foo");
         }

         pause = true;
         rc.fail(new ActiveMQNotConnectedException());
         pause = false;

         for (int j = 0; j < 10000; j++) {
            TextMessage tm = (TextMessage) consumer2.receive(5000);

            assertNotNull(tm);

            assertEquals("Message" + j, tm.getText());

            assertEquals(tm.getStringProperty("JMSXGroupID"), "foo");
         }
      } finally {
         if (sendConnection != null) {
            sendConnection.close();
         }
         if (connection2 != null) {
            connection2.close();
         }
      }
   }

   public static void pause() {
      if (pause) {
         try {
            System.out.println("pausing after rollback");
            Thread.sleep(500);
         } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
         }
         System.out.println("finished pausing after rollback");
      }
   }
}
