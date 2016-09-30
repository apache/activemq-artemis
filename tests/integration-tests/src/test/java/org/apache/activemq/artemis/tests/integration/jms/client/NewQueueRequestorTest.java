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
package org.apache.activemq.artemis.tests.integration.jms.client;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueReceiver;
import javax.jms.QueueRequestor;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.artemis.tests.integration.IntegrationTestLogger;
import org.apache.activemq.artemis.tests.util.JMSTestBase;
import org.junit.Test;

public class NewQueueRequestorTest extends JMSTestBase {

   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   @Test
   public void testQueueRequestor() throws Exception {
      QueueConnection conn1 = null, conn2 = null;

      try {
         Queue queue1 = createQueue(true, "myQueue");
         conn1 = (QueueConnection) cf.createConnection();
         QueueSession sess1 = conn1.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
         QueueRequestor requestor = new QueueRequestor(sess1, queue1);
         conn1.start();

         // And the responder
         conn2 = (QueueConnection) cf.createConnection();
         QueueSession sess2 = conn2.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
         TestMessageListener listener = new TestMessageListener(sess2);
         QueueReceiver receiver = sess2.createReceiver(queue1);
         receiver.setMessageListener(listener);
         conn2.start();

         Message m1 = sess1.createMessage();
         log.trace("Sending request message");
         TextMessage m2 = (TextMessage) requestor.request(m1);
         assertNotNull(m2);

         assertEquals("This is the response", m2.getText());

         requestor.close();
      } finally {
         conn1.close();
         conn2.close();
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   class TestMessageListener implements MessageListener {

      private final QueueSession sess;

      private final QueueSender sender;

      TestMessageListener(final QueueSession sess) throws JMSException {
         this.sess = sess;
         sender = sess.createSender(null);
      }

      @Override
      public void onMessage(final Message m) {
         try {
            Destination queue = m.getJMSReplyTo();
            Message m2 = sess.createTextMessage("This is the response");
            sender.send(queue, m2);
         } catch (JMSException e) {
            log.error(e);
         }
      }
   }
}
