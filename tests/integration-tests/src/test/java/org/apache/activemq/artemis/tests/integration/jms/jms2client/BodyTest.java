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
package org.apache.activemq.artemis.tests.integration.jms.jms2client;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSProducer;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageFormatException;
import javax.jms.MessageFormatRuntimeException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.jms.client.ActiveMQQueue;
import org.apache.activemq.artemis.jms.client.ActiveMQTopic;
import org.apache.activemq.artemis.tests.util.JMSTestBase;
import org.junit.Before;
import org.junit.Test;

public class BodyTest extends JMSTestBase {

   private static final String Q_NAME = "SomeQueue";
   private javax.jms.Queue queue;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      jmsServer.createQueue(false, Q_NAME, null, true, Q_NAME);
      queue = ActiveMQJMSClient.createQueue(Q_NAME);
   }

   @Test
   public void testBodyConversion() throws Throwable {
      try (Connection conn = cf.createConnection();) {

         Session sess = conn.createSession();
         MessageProducer producer = sess.createProducer(queue);

         MessageConsumer cons = sess.createConsumer(queue);
         conn.start();

         BytesMessage bytesMessage = sess.createBytesMessage();
         producer.send(bytesMessage);

         Message msg = cons.receiveNoWait();
         assertNotNull(msg);

         try {
            msg.getBody(String.class);
            fail("Exception expected");
         }
         catch (MessageFormatException e) {
         }
      }

   }

   @Test
   public void testJMSConsumerReceiveBodyHandlesMFECorrectly() throws Exception {
      final String CONTENT_EXPECTED = "Message-Content";
      ActiveMQTopic topic = new ActiveMQTopic("myTopic");

      JMSContext context = null;

      try {
         context = cf.createContext(Session.CLIENT_ACKNOWLEDGE);

         JMSProducer producer = context.createProducer();
         JMSConsumer consumer = context.createConsumer(topic);

         producer.send(topic, context.createTextMessage(CONTENT_EXPECTED));

         try {
            consumer.receiveBody(Boolean.class, 2000);
            fail("Should have thrown MessageFormatRuntimeException");
         }
         catch (MessageFormatRuntimeException mfre) {
         }

         String received = consumer.receiveBody(String.class, 2000);
         assertNotNull(received);
         assertEquals(CONTENT_EXPECTED, received);
      }
      finally {
         if (context != null) {
            context.close();
         }
      }
   }

   @Test
   public void testJMSConsumerReceiveBodyHandlesMFECorrectlyWithContextClosure() throws Exception {
      final String CONTENT_EXPECTED = "Message-Content";
      ActiveMQQueue queue = new ActiveMQQueue("myQueue");

      JMSContext context = null;

      try {
         context = cf.createContext(Session.CLIENT_ACKNOWLEDGE);

         JMSProducer producer = context.createProducer();
         JMSConsumer consumer = context.createConsumer(queue);

         TextMessage msg = context.createTextMessage(CONTENT_EXPECTED);

         producer.send(queue, msg);

         try {
            consumer.receiveBody(Boolean.class, 2000);
            fail("Should have thrown MessageFormatRuntimeException");
         }
         catch (MessageFormatRuntimeException mfre) {
         }

         context.close();

         context = cf.createContext();
         consumer = context.createConsumer(queue);

         String received = consumer.receiveBody(String.class, 2000);
         assertNotNull(received);
         assertEquals(CONTENT_EXPECTED, received);
      }
      finally {
         if (context != null) {
            context.close();
         }
      }
   }
}
