/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.activemq.tests.integration.jms.client;
import org.junit.Before;
import org.junit.After;

import org.junit.Test;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.tests.util.JMSTestBase;

/**
 *
 * A ReceiveNoWaitTest
 *
 * @author Tim Fox
 *
 *
 */
public class ReceiveNoWaitTest extends JMSTestBase
{
   private Queue queue;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      queue = createQueue("TestQueue");
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      jmsServer.destroyQueue("TestQueue");

      super.tearDown();
   }


   /*
    * Test that after sending persistent messages to a queue (these will be sent blocking)
    * that all messages are available for consumption by receiveNoWait()
    * https://jira.jboss.org/jira/browse/HORNETQ-284
    */
   @Test
   public void testReceiveNoWait() throws Exception
   {
      assertNotNull(queue);

      for (int i = 0; i < 10; i++)
      {
         Connection connection = cf.createConnection();

         Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

         MessageProducer producer = session.createProducer(queue);

         producer.setDeliveryMode(DeliveryMode.PERSISTENT);

         for (int j = 0; j < 100; j++)
         {
            String text = "Message" + j;

            TextMessage message = session.createTextMessage();

            message.setText(text);

            producer.send(message);
         }

         connection.start();

         MessageConsumer consumer = session.createConsumer(queue);

         for (int j = 0; j < 100; j++)
         {
            TextMessage m = (TextMessage)consumer.receiveNoWait();

            if (m == null)
            {
               throw new IllegalStateException("msg null");
            }

            assertEquals("Message" + j, m.getText());

            m.acknowledge();
         }

         connection.close();
      }
   }
}
