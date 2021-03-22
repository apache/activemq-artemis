/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <br>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <br>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.smoke.brokerConnection;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.artemis.tests.smoke.common.SmokeTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.util.ServerUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BrokerConnectionMirrorSecurityTest extends SmokeTestBase {

   public static final String SERVER_NAME_A = "brokerConnectMirrorSecurityA";
   public static final String SERVER_NAME_B = "brokerConnectMirrorSecurityB";

   @Before
   public void before() throws Exception {
      // no need to cleanup, these servers don't have persistence
      // start serverB first, after all ServerA needs it alive to create connections
      startServer(SERVER_NAME_B, 0, 0);
      startServer(SERVER_NAME_A, 0, 0);

      ServerUtil.waitForServerToStart(1, "B", "B", 30000);
      ServerUtil.waitForServerToStart(0, "A", "A", 30000);
   }

   @Test
   public void testMirrorOverBokerConnection() throws Throwable {
      ConnectionFactory cfA = CFUtil.createConnectionFactory("amqp", "tcp://localhost:61616");
      ConnectionFactory cfB = CFUtil.createConnectionFactory("amqp", "tcp://localhost:61617");


      try (Connection connectionA = cfA.createConnection("A", "A");
           Connection connectionB = cfB.createConnection("B", "B")) {
         Session sessionA = connectionA.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Queue queue = sessionA.createQueue("someQueue");
         MessageProducer producerA = sessionA.createProducer(queue);
         for (int i = 0; i < 10; i++) {
            producerA.send(sessionA.createTextMessage("message"));
         }

         Session sessionB = connectionB.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer consumerB = sessionB.createConsumer(queue);
         connectionB.start();

         for (int i = 0; i < 10; i++) {
            TextMessage message = (TextMessage) consumerB.receive(1000);
            Assert.assertNotNull(message);
            Assert.assertEquals("message", message.getText());
         }
      }
   }

}
