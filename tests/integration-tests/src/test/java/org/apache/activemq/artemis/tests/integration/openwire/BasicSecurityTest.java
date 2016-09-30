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
import javax.jms.JMSSecurityException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TextMessage;

import org.apache.activemq.command.ActiveMQQueue;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BasicSecurityTest extends BasicOpenWireTest {

   @Override
   @Before
   public void setUp() throws Exception {
      this.enableSecurity = true;
      super.setUp();
   }

   @Test
   public void testConnectionWithCredentials() throws Exception {
      Connection newConn = null;

      //correct
      try {
         newConn = factory.createConnection("openwireSender", "SeNdEr");
         newConn.start();
         newConn.close();

         newConn = factory.createConnection("openwireReceiver", "ReCeIvEr");
         newConn.start();
         newConn.close();

         newConn = null;
      } finally {
         if (newConn != null) {
            newConn.close();
         }
      }

      //wrong password
      try {
         newConn = factory.createConnection("openwireSender", "WrongPasswD");
         newConn.start();
      } catch (JMSSecurityException e) {
         //expected
      } finally {
         if (newConn != null) {
            newConn.close();
         }
      }

      //wrong user
      try {
         newConn = factory.createConnection("wronguser", "SeNdEr");
         newConn.start();
      } catch (JMSSecurityException e) {
         //expected
      } finally {
         if (newConn != null) {
            newConn.close();
         }
      }

      //both wrong
      try {
         newConn = factory.createConnection("wronguser", "wrongpass");
         newConn.start();
      } catch (JMSSecurityException e) {
         //expected
      } finally {
         if (newConn != null) {
            newConn.close();
         }
      }

      //default user
      try {
         newConn = factory.createConnection();
         newConn.start();
      } catch (JMSSecurityException e) {
         //expected
      } finally {
         if (newConn != null) {
            newConn.close();
         }
      }
   }

   @Test
   public void testSendnReceiveAuthorization() throws Exception {
      Connection sendingConn = null;
      Connection receivingConn = null;

      //Sender
      try {
         Destination dest = new ActiveMQQueue(queueName);

         receivingConn = factory.createConnection("openwireReceiver", "ReCeIvEr");
         receivingConn.start();

         sendingConn = factory.createConnection("openwireSender", "SeNdEr");
         sendingConn.start();

         Session sendingSession = sendingConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session receivingSession = receivingConn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         TextMessage message = sendingSession.createTextMessage("Hello World");

         MessageProducer producer = null;

         producer = receivingSession.createProducer(dest);

         try {
            producer.send(message);
         } catch (JMSSecurityException e) {
            //expected
            producer.close();
         }

         producer = sendingSession.createProducer(dest);
         producer.send(message);

         MessageConsumer consumer;
         try {
            consumer = sendingSession.createConsumer(dest);
            Assert.fail("exception expected");
         } catch (JMSSecurityException e) {
            e.printStackTrace();
            //expected
         }

         consumer = receivingSession.createConsumer(dest);
         TextMessage received = (TextMessage) consumer.receive(5000);

         assertNotNull(received);
         assertEquals("Hello World", received.getText());
      } finally {
         if (sendingConn != null) {
            sendingConn.close();
         }

         if (receivingConn != null) {
            receivingConn.close();
         }
      }
   }

   @Test
   public void testCreateTempDestinationAuthorization() throws Exception {
      Connection conn1 = null;
      Connection conn2 = null;

      //Sender
      try {
         conn1 = factory.createConnection("openwireGuest", "GuEsT");
         conn1.start();

         conn2 = factory.createConnection("openwireDestinationManager", "DeStInAtIoN");
         conn2.start();

         Session session1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);

         try {
            session1.createTemporaryQueue();
            fail("user shouldn't be able to create temp queue");
         } catch (JMSSecurityException e) {
            //expected
         }

         Session session2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);

         TemporaryQueue q = session2.createTemporaryQueue();
         assertNotNull(q);
      } finally {
         if (conn1 != null) {
            conn1.close();
         }

         if (conn2 != null) {
            conn2.close();
         }
      }
   }

}
