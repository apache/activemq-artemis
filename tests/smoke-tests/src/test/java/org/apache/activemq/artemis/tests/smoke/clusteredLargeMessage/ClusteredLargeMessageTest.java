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

package org.apache.activemq.artemis.tests.smoke.clusteredLargeMessage;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.artemis.tests.smoke.common.SmokeTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ClusteredLargeMessageTest extends SmokeTestBase {

   public static final String SERVER_NAME_0 = "clusteredLargeMessage/cluster1";
   public static final String SERVER_NAME_1 = "clusteredLargeMessage/cluster2";

   Process server1Process;

   @Before
   public void before() throws Exception {
      cleanupData(SERVER_NAME_0);
      cleanupData(SERVER_NAME_1);
      server1Process = startServer(SERVER_NAME_0, 0, 30000);
      startServer(SERVER_NAME_1, 100, 30000);
   }

   @Test
   public void testLargeMessage() throws Exception {

      // I'm calling all 3 here as I want to run all of these with a single server start
      // without having to deal with beforeClass and afterClass on this test
      internalTestLargeMessge("CORE");
      internalTestLargeMessge("AMQP");
      internalTestLargeMessge("OPENWIRE");
   }

   private void internalTestLargeMessge(String protocol) throws Exception {

      ConnectionFactory server2CF = CFUtil.createConnectionFactory(protocol, "tcp://localhost:61716");
      Connection connection2 = server2CF.createConnection();
      Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue queue2 = session2.createQueue("testQueue");
      MessageConsumer consumer2 = session2.createConsumer(queue2);
      connection2.start();

      ConnectionFactory server1CF = CFUtil.createConnectionFactory(protocol, "tcp://localhost:61616");
      Connection connection1 = server1CF.createConnection();
      Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue queue1 = session1.createQueue("testQueue");
      MessageProducer producer1 = session1.createProducer(queue1);

      String largeBody;

      {
         StringBuffer largeBodyBuffer = new StringBuffer();
         while (largeBodyBuffer.length() < 2_000_000) {
            largeBodyBuffer.append("This is large ");
         }
         largeBody = largeBodyBuffer.toString();
      }

      for (int i = 0; i < 10; i++) {
         TextMessage message = session1.createTextMessage(largeBody);
         message.setStringProperty("i", Integer.toString(i));
         producer1.send(message);
      }

      for (int i = 0; i < 10; i++) {
         TextMessage message = (TextMessage) consumer2.receive(5000);
         Assert.assertNotNull(message);
         Assert.assertEquals(largeBody, message.getText());
      }

      connection1.close();
      connection2.close();
   }
}

