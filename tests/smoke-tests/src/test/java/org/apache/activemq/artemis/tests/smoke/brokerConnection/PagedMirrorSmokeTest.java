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
import java.io.File;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.core.persistence.impl.journal.JournalRecordIds;
import org.apache.activemq.artemis.tests.smoke.common.SmokeTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.utils.Wait;
import org.apache.activemq.artemis.util.ServerUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PagedMirrorSmokeTest extends SmokeTestBase {

   // Change this to true to generate a print-data in certain cases on this test
   private static final boolean PRINT_DATA = false;

   public static final String SERVER_NAME_A = "brokerConnect/pagedA";
   public static final String SERVER_NAME_B = "brokerConnect/pagedB";

   Process processB;
   Process processA;

   @Before
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

      File countJournalLocation = new File(getServerLocation(SERVER_NAME_A), "data/journal");
      File countJournalLocationB = new File(getServerLocation(SERVER_NAME_B), "data/journal");
      Assert.assertTrue(countJournalLocation.exists() && countJournalLocation.isDirectory());
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

      Thread.sleep(500);
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
         Assert.assertNull(consumer.receiveNoWait());
      }
      Wait.assertEquals(1, () -> acksCount(countJournalLocation), 5000, 1000);
      Wait.assertEquals(1, () -> acksCount(countJournalLocationB), 5000, 1000);

      try (Connection consumeConnection = secondConsumeCF.createConnection()) {
         Session consumeSession = consumeConnection.createSession(true, Session.SESSION_TRANSACTED);
         Queue jmsQueue = consumeSession.createQueue("someQueue");
         MessageConsumer consumer = consumeSession.createConsumer(jmsQueue);
         consumeConnection.start();

         for (int i = 0; i < NUMBER_OF_MESSAGES - 1; i++) {
            TextMessage message = (TextMessage) consumer.receive(6000);
            Assert.assertNotNull(message);
            Assert.assertNotEquals(ACK_I, message.getIntProperty("i"));
         }
         Assert.assertNull(consumer.receiveNoWait());
      }
   }

   private int acksCount(File countJournalLocation) throws Exception {
      HashMap<Integer, AtomicInteger> countJournal = countJournal(countJournalLocation, 10485760, 2, 2);
      AtomicInteger acksCount = countJournal.get((int)JournalRecordIds.ACKNOWLEDGE_CURSOR);
      return acksCount != null ? acksCount.get() : 0;
   }

}
