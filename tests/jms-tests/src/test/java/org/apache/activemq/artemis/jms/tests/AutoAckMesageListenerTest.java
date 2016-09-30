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
package org.apache.activemq.artemis.jms.tests;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

public class AutoAckMesageListenerTest extends JMSTestCase {

   // Constants -----------------------------------------------------

   private static final JmsTestLogger log = JmsTestLogger.LOGGER;

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testAutoAckMsgListenerQueue() throws Exception {
      Connection conn = null;

      try {
         CountDownLatch latch = new CountDownLatch(1);

         conn = createConnection();
         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = session.createProducer(queue1);
         MessageConsumer consumer = session.createConsumer(queue1);
         AutoAckMsgListener listener = new AutoAckMsgListener(latch, session);
         consumer.setMessageListener(listener);

         // create and send messages
         log.info("Send and receive two message");
         Message messageSent = session.createMessage();
         messageSent.setBooleanProperty("last", false);
         producer.send(messageSent);
         messageSent.setBooleanProperty("last", true);
         producer.send(messageSent);

         conn.start();

         // wait until message is received
         log.info("waiting until message has been received by message listener...");
         latch.await(10, TimeUnit.SECONDS);

         // check message listener status
         if (listener.getPassed() == false) {
            throw new Exception("failed");
         }
      } finally {
         if (conn != null) {
            conn.close();
         }
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   private static class AutoAckMsgListener implements MessageListener {

      private boolean passed;

      private final Session session;

      private final CountDownLatch monitor;

      private AutoAckMsgListener(CountDownLatch latch, Session session) {
         this.monitor = latch;
         this.session = session;
      }

      // get state of test
      public boolean getPassed() {
         return passed;
      }

      // will receive two messages
      @Override
      public void onMessage(Message message) {
         try {
            if (message.getBooleanProperty("last") == false) {
               log.info("Received first message.");
               if (message.getJMSRedelivered() == true) {
                  // should not re-receive this one
                  log.info("Error: received first message twice");
                  passed = false;
               }
            } else {
               if (message.getJMSRedelivered() == false) {
                  // received second message for first time
                  log.info("Received second message. Calling recover()");
                  session.recover();
               } else {
                  // should be redelivered after recover
                  log.info("Received second message again as expected");
                  passed = true;
                  monitor.countDown();
               }
            }
         } catch (JMSException e) {
            log.warn("Exception caught in message listener:\n" + e);
            passed = false;
            monitor.countDown();
         }

      }
   }
}
