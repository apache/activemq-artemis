/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.amqp;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.JMSSecurityException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.artemis.tests.integration.IntegrationTestLogger;
import org.junit.Test;

public class JMSConnectionWithSecurityTest extends JMSClientTestSupport {

   @Override
   protected boolean isSecurityEnabled() {
      return true;
   }

   @Override
   protected String getJmsConnectionURIOptions() {
      return "amqp.saslMechanisms=PLAIN";
   }

   @Test(timeout = 10000)
   public void testNoUserOrPassword() throws Exception {
      Connection connection = null;
      try {
         connection = createConnection("", "", null, false);
         connection.start();
         fail("Expected JMSException");
      } catch (JMSSecurityException ex) {
         IntegrationTestLogger.LOGGER.debug("Failed to authenticate connection with no user / password.");
      } finally {
         if (connection != null) {
            connection.close();
         }
      }
   }

   @Test(timeout = 10000)
   public void testUnknownUser() throws Exception {
      Connection connection = null;
      try {
         connection = createConnection("nosuchuser", "blah", null, false);
         connection.start();
         fail("Expected JMSException");
      } catch (JMSSecurityException ex) {
         IntegrationTestLogger.LOGGER.debug("Failed to authenticate connection with unknown user ID");
      } finally {
         if (connection != null) {
            connection.close();
         }
      }
   }

   @Test(timeout = 10000)
   public void testKnownUserWrongPassword() throws Exception {
      Connection connection = null;
      try {
         connection = createConnection(fullUser, "wrongPassword", null, false);
         connection.start();
         fail("Expected JMSException");
      } catch (JMSSecurityException ex) {
         IntegrationTestLogger.LOGGER.debug("Failed to authenticate connection with incorrect password.");
      } finally {
         if (connection != null) {
            connection.close();
         }
      }
   }

   @Test(timeout = 30000)
   public void testRepeatedWrongPasswordAttempts() throws Exception {
      for (int i = 0; i < 25; ++i) {
         Connection connection = null;
         try {
            connection = createConnection(fullUser, "wrongPassword", null, false);
            connection.start();
            fail("Expected JMSException");
         } catch (JMSSecurityException ex) {
            IntegrationTestLogger.LOGGER.debug("Failed to authenticate connection with incorrect password.");
         } finally {
            if (connection != null) {
               connection.close();
            }
         }
      }
   }

   @Test(timeout = 30000)
   public void testSendReceive() throws Exception {
      Connection connection = createConnection(fullUser, fullPass);

      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         javax.jms.Queue queue = session.createQueue(getQueueName());
         MessageProducer p = session.createProducer(queue);
         TextMessage message = null;
         message = session.createTextMessage();
         String messageText = "hello  sent at " + new java.util.Date().toString();
         message.setText(messageText);
         p.send(message);

         // Get the message we just sent
         MessageConsumer consumer = session.createConsumer(queue);
         connection.start();
         Message msg = consumer.receive(5000);
         assertNotNull(msg);
         assertTrue(msg instanceof TextMessage);
         TextMessage textMessage = (TextMessage) msg;
         assertEquals(messageText, textMessage.getText());
      } finally {
         connection.close();
      }
   }

   @Test(timeout = 30000)
   public void testConsumerNotAuthorized() throws Exception {
      Connection connection = createConnection(noprivUser, noprivPass);

      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         javax.jms.Queue queue = session.createQueue(getQueueName());
         try {
            session.createConsumer(queue);
            fail("Should not be able to consume here.");
         } catch (JMSSecurityException jmsSE) {
            IntegrationTestLogger.LOGGER.info("Caught expected exception");
         }
      } finally {
         connection.close();
      }
   }

   @Test(timeout = 30000)
   public void testBrowserNotAuthorized() throws Exception {
      Connection connection = createConnection(noprivUser, noprivPass);

      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         javax.jms.Queue queue = session.createQueue(getQueueName());
         try {
            QueueBrowser browser = session.createBrowser(queue);
            // Browser is not created until an enumeration is requesteda
            browser.getEnumeration();
            fail("Should not be able to consume here.");
         } catch (JMSSecurityException jmsSE) {
            IntegrationTestLogger.LOGGER.info("Caught expected exception");
         }
      } finally {
         connection.close();
      }
   }

   @Test(timeout = 30000)
   public void testConsumerNotAuthorizedToCreateQueues() throws Exception {
      Connection connection = createConnection(noprivUser, noprivPass);

      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         javax.jms.Queue queue = session.createQueue(getQueueName(getPrecreatedQueueSize() + 1));
         try {
            session.createConsumer(queue);
            fail("Should not be able to consume here.");
         } catch (JMSSecurityException jmsSE) {
            IntegrationTestLogger.LOGGER.info("Caught expected exception");
         }
      } finally {
         connection.close();
      }
   }

   @Test(timeout = 30000)
   public void testProducerNotAuthorized() throws Exception {
      Connection connection = createConnection(guestUser, guestPass);

      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         javax.jms.Queue queue = session.createQueue(getQueueName());
         try {
            session.createProducer(queue);
            fail("Should not be able to produce here.");
         } catch (JMSSecurityException jmsSE) {
            IntegrationTestLogger.LOGGER.info("Caught expected exception");
         }
      } finally {
         connection.close();
      }
   }

   @Test(timeout = 30000)
   public void testAnonymousProducerNotAuthorized() throws Exception {
      Connection connection = createConnection(guestUser, guestPass);

      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         javax.jms.Queue queue = session.createQueue(getQueueName());
         MessageProducer producer = session.createProducer(null);

         try {
            producer.send(queue, session.createTextMessage());
            fail("Should not be able to produce here.");
         } catch (JMSSecurityException jmsSE) {
            IntegrationTestLogger.LOGGER.info("Caught expected exception");
         }
      } finally {
         connection.close();
      }
   }

   @Test(timeout = 30000)
   public void testCreateTemporaryQueueNotAuthorized() throws JMSException {
      Connection connection = createConnection(guestUser, guestPass);

      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         try {
            session.createTemporaryQueue();
         } catch (JMSSecurityException jmsse) {
            IntegrationTestLogger.LOGGER.info("Client should have thrown a JMSSecurityException but only threw JMSException");
         }

         // Should not be fatal
         assertNotNull(connection.createSession(false, Session.AUTO_ACKNOWLEDGE));
      } finally {
         connection.close();
      }
   }

   @Test(timeout = 30000)
   public void testCreateTemporaryTopicNotAuthorized() throws JMSException {
      Connection connection = createConnection(guestUser, guestPass);

      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         try {
            session.createTemporaryTopic();
         } catch (JMSSecurityException jmsse) {
            IntegrationTestLogger.LOGGER.info("Client should have thrown a JMSSecurityException but only threw JMSException");
         }

         // Should not be fatal
         assertNotNull(connection.createSession(false, Session.AUTO_ACKNOWLEDGE));
      } finally {
         connection.close();
      }
   }
}
