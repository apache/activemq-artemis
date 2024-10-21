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

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.File;
import java.lang.invoke.MethodHandles;

import org.apache.activemq.artemis.tests.smoke.common.SmokeTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.utils.Wait;
import org.apache.activemq.artemis.util.ServerUtil;
import org.apache.activemq.artemis.cli.commands.helper.HelperCreate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PagedMirrorSmokeTest extends SmokeTestBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   // Change this to true to generate a print-data in certain cases on this test
   private static final boolean PRINT_DATA = false;

   public static final String SERVER_NAME_A = "brokerConnect/pagedA";
   public static final String SERVER_NAME_B = "brokerConnect/pagedB";

   @BeforeAll
   public static void createServers() throws Exception {

      File server0Location = getFileServerLocation(SERVER_NAME_A);
      File server1Location = getFileServerLocation(SERVER_NAME_B);
      deleteDirectory(server1Location);
      deleteDirectory(server0Location);

      if (!server0Location.exists()) {
         HelperCreate cliCreateServer = helperCreate();
         cliCreateServer.setAllowAnonymous(true).setRole("amq").setUser("artemis").setPassword("artemis").setNoWeb(true).setConfiguration("./src/main/resources/servers/brokerConnect/pagedA").setArtemisInstance(server0Location);
         cliCreateServer.createServer();
      }

      if (!server1Location.exists()) {
         HelperCreate cliCreateServer = helperCreate();
         cliCreateServer.setAllowAnonymous(true).setRole("amq").setUser("artemis").setPassword("artemis").setNoWeb(true).setConfiguration("./src/main/resources/servers/brokerConnect/pagedB").setArtemisInstance(server1Location);
         cliCreateServer.createServer();
      }
   }


   Process processB;
   Process processA;

   @BeforeEach
   public  void beforeClass() throws Exception {
      cleanupData(SERVER_NAME_A);
      cleanupData(SERVER_NAME_B);
      processB = startServer(SERVER_NAME_B, 1, 0);
      processA = startServer(SERVER_NAME_A, 0, 0);

      ServerUtil.waitForServerToStart(1, "B", "B", 30000);
      ServerUtil.waitForServerToStart(0, "A", "A", 30000);
   }

   @Test
   public void testPaged() throws Throwable {
      String sendURI = "tcp://localhost:61616";
      String consumeURI = "tcp://localhost:61616";
      String secondConsumeURI = "tcp://localhost:61617";

      String protocol = "amqp";

      ConnectionFactory sendCF = CFUtil.createConnectionFactory(protocol, sendURI);
      ConnectionFactory consumeCF = CFUtil.createConnectionFactory(protocol, consumeURI);
      ConnectionFactory secondConsumeCF = CFUtil.createConnectionFactory(protocol, secondConsumeURI);

      String bodyBuffer;
      {
         StringBuffer buffer = new StringBuffer();
         for (int i = 0; i < 1024; i++) {
            buffer.append("*");
         }
         bodyBuffer = buffer.toString();
      }

      int NUMBER_OF_MESSAGES = 200;
      int ACK_I = 77;

      try (Connection sendConnecton = sendCF.createConnection()) {
         Session sendSession = sendConnecton.createSession(true, Session.SESSION_TRANSACTED);
         Queue jmsQueue = sendSession.createQueue("someQueue");
         MessageProducer producer = sendSession.createProducer(jmsQueue);

         for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
            TextMessage message = sendSession.createTextMessage(bodyBuffer);
            message.setIntProperty("i", i);
            producer.send(message);
         }
         sendSession.commit();
      }


      try (Connection consumeConnection = consumeCF.createConnection()) {
         Session consumeSession = consumeConnection.createSession(false, 101); // individual ack
         Queue jmsQueue = consumeSession.createQueue("someQueue");
         MessageConsumer consumer = consumeSession.createConsumer(jmsQueue);
         consumeConnection.start();
         for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
            TextMessage message = (TextMessage) consumer.receive(6000);
            if (message.getIntProperty("i") == ACK_I) {
               message.acknowledge();
            }
         }
         assertNull(consumer.receiveNoWait());
      }

      Wait.assertEquals(0, () -> getMessageCount(consumeURI, "$ACTIVEMQ_ARTEMIS_MIRROR_outgoing"));
      Wait.assertEquals(NUMBER_OF_MESSAGES - 1, () -> getMessageCount(secondConsumeURI, "someQueue"));

      try (Connection consumeConnection = secondConsumeCF.createConnection()) {
         Session consumeSession = consumeConnection.createSession(true, Session.SESSION_TRANSACTED);
         Queue jmsQueue = consumeSession.createQueue("someQueue");
         MessageConsumer consumer = consumeSession.createConsumer(jmsQueue);
         consumeConnection.start();

         for (int i = 0; i < NUMBER_OF_MESSAGES - 1; i++) {
            TextMessage message = (TextMessage) consumer.receive(6000);
            assertNotNull(message);
            assertNotEquals(ACK_I, message.getIntProperty("i"));
         }
         assertNull(consumer.receiveNoWait());
      }
   }
}
