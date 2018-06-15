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
package org.apache.activemq.artemis.tests.integration.openwire;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.apache.activemq.artemis.logs.AssertionLoggerHandler;
import org.apache.activemq.command.ActiveMQDestination;
import org.junit.AfterClass;
import static org.junit.Assert.assertNotNull;
import org.junit.BeforeClass;
import org.junit.Test;

public class SessionHandlingOpenWireTest extends BasicOpenWireTest {

   @BeforeClass
   public static void prepareLogger() {
      AssertionLoggerHandler.startCapture();
   }

   @AfterClass
   public static void clearLogger() {
      AssertionLoggerHandler.stopCapture();
   }

   @Test
   public void testInternalSessionHandling() throws Exception {
      try (Connection conn = factory.createConnection()) {
         conn.start();
         try (Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            Destination dest = createDestination(session,ActiveMQDestination.QUEUE_TYPE);
            sendMessages(session, dest, 1);
            MessageConsumer consumer = session.createConsumer(dest);
            Message m = consumer.receive(2000);
            assertNotNull(m);
         }
      }
      assertFalse(AssertionLoggerHandler.findText("Client connection failed, clearing up resources for session"));
      assertFalse(AssertionLoggerHandler.findText("Cleared up resources for session"));
   }

   @Test
   public void testInternalSessionHandlingNoSessionClose() throws Exception {
      try (Connection conn = factory.createConnection()) {
         conn.start();
         for (int i = 0; i < 100; i++) {
            Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination dest = createDestination(session,ActiveMQDestination.QUEUE_TYPE);
            sendMessages(session, dest, 1);
            MessageConsumer consumer = session.createConsumer(dest);
            Message m = consumer.receive(2000);
            consumer.close();
            assertNotNull(m);

            if (i % 2 == 1) {
               // it will close only half of the sessions
               // just to introduce error conditions
               session.close();
            }

         }
      }
      assertFalse(AssertionLoggerHandler.findText("Client connection failed, clearing up resources for session"));
      assertFalse(AssertionLoggerHandler.findText("Cleared up resources for session"));
   }
}
