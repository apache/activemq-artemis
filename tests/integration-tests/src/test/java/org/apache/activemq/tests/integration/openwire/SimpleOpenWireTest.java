/**
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
package org.apache.activemq.tests.integration.openwire;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class SimpleOpenWireTest extends BasicOpenWireTest
{
   @Rule
   public ExpectedException thrown= ExpectedException.none();

   @Override
   @Before
   public void setUp() throws Exception
   {
      this.realStore = true;
      super.setUp();
   }

   @Test
   public void testSimpleQueue() throws Exception
   {
      connection.start();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      System.out.println("creating queue: " + queueName);
      Destination dest = new ActiveMQQueue(queueName);

      System.out.println("creating producer...");
      MessageProducer producer = session.createProducer(dest);

      final int num = 1;
      final String msgBase = "MfromAMQ-";
      for (int i = 0; i < num; i++)
      {
         TextMessage msg = session.createTextMessage("MfromAMQ-" + i);
         producer.send(msg);
         System.out.println("sent: ");
      }

      //receive
      MessageConsumer consumer = session.createConsumer(dest);

      System.out.println("receiving messages...");
      for (int i = 0; i < num; i++)
      {
         TextMessage msg = (TextMessage) consumer.receive(5000);
         System.out.println("received: " + msg);
         String content = msg.getText();
         System.out.println("content: " + content);
         assertEquals(msgBase + i, content);
      }

      assertNull(consumer.receive(1000));

      session.close();
   }

   @Test
   public void testSimpleTopic() throws Exception
   {
      connection.start();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      System.out.println("creating queue: " + topicName);
      Destination dest = new ActiveMQTopic(topicName);

      MessageConsumer consumer1 = session.createConsumer(dest);
      MessageConsumer consumer2 = session.createConsumer(dest);

      MessageProducer producer = session.createProducer(dest);

      final int num = 1;
      final String msgBase = "MfromAMQ-";
      for (int i = 0; i < num; i++)
      {
         TextMessage msg = session.createTextMessage("MfromAMQ-" + i);
         producer.send(msg);
         System.out.println("Sent a message");
      }

      //receive
      System.out.println("receiving messages...");
      for (int i = 0; i < num; i++)
      {
         TextMessage msg = (TextMessage) consumer1.receive(5000);
         System.out.println("received: " + msg);
         String content = msg.getText();
         assertEquals(msgBase + i, content);
      }

      assertNull(consumer1.receive(500));

      System.out.println("receiving messages...");
      for (int i = 0; i < num; i++)
      {
         TextMessage msg = (TextMessage) consumer2.receive(5000);
         System.out.println("received: " + msg);
         String content = msg.getText();
         assertEquals(msgBase + i, content);
      }

      assertNull(consumer2.receive(500));
      session.close();
   }

   @Test
   public void testSimpleTempTopic() throws Exception
   {
      connection.start();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      System.out.println("creating temp topic");
      TemporaryTopic tempTopic = session.createTemporaryTopic();

      System.out.println("create consumer 1");
      MessageConsumer consumer1 = session.createConsumer(tempTopic);
      System.out.println("create consumer 2");
      MessageConsumer consumer2 = session.createConsumer(tempTopic);

      System.out.println("create producer");
      MessageProducer producer = session.createProducer(tempTopic);

      System.out.println("sending messages");
      final int num = 1;
      final String msgBase = "MfromAMQ-";
      for (int i = 0; i < num; i++)
      {
         TextMessage msg = session.createTextMessage("MfromAMQ-" + i);
         producer.send(msg);
         System.out.println("Sent a message");
      }

      //receive
      System.out.println("receiving messages...");
      for (int i = 0; i < num; i++)
      {
         TextMessage msg = (TextMessage) consumer1.receive(5000);
         System.out.println("received: " + msg);
         String content = msg.getText();
         assertEquals(msgBase + i, content);
      }

      assertNull(consumer1.receive(500));

      System.out.println("receiving messages...");
      for (int i = 0; i < num; i++)
      {
         TextMessage msg = (TextMessage) consumer2.receive(5000);
         System.out.println("received: " + msg);
         String content = msg.getText();
         assertEquals(msgBase + i, content);
      }

      assertNull(consumer2.receive(500));
      session.close();
   }

   @Test
   public void testSimpleTempQueue() throws Exception
   {
      connection.start();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      System.out.println("creating temp queue");
      TemporaryQueue tempQueue = session.createTemporaryQueue();

      System.out.println("create consumer 1");
      MessageConsumer consumer1 = session.createConsumer(tempQueue);

      System.out.println("create producer");
      MessageProducer producer = session.createProducer(tempQueue);

      System.out.println("sending messages");
      final int num = 1;
      final String msgBase = "MfromAMQ-";
      for (int i = 0; i < num; i++)
      {
         TextMessage msg = session.createTextMessage("MfromAMQ-" + i);
         producer.send(msg);
         System.out.println("Sent a message");
      }

      //receive
      System.out.println("receiving messages...");
      for (int i = 0; i < num; i++)
      {
         TextMessage msg = (TextMessage) consumer1.receive(5000);
         System.out.println("received: " + msg);
         String content = msg.getText();
         assertEquals(msgBase + i, content);
      }

      assertNull(consumer1.receive(500));
      session.close();
   }

   @Test
   public void testInvalidDestinationExceptionWhenNoQueueExistsOnCreateProducer() throws Exception
   {
      connection.start();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue queue = session.createQueue("fake.queue");

      thrown.expect(InvalidDestinationException.class);
      thrown.expect(JMSException.class);
      session.createProducer(queue);
      session.close();
   }

   @Test
   public void testInvalidDestinationExceptionWhenNoTopicExistsOnCreateProducer() throws Exception
   {
      connection.start();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Destination destination = session.createTopic("fake.queue");

      thrown.expect(InvalidDestinationException.class);
      session.createProducer(destination);
      session.close();
   }

   /**
    * This is the example shipped with the distribution
    * @throws Exception
    */
   @Test
   public void testOpenWireExample() throws Exception
   {
      Connection exConn = null;

      try
      {
         String urlString = "tcp://" + OWHOST + ":" + OWPORT + "?wireFormat.cacheEnabled=true";
         ActiveMQConnectionFactory exFact = new ActiveMQConnectionFactory(urlString);

         // Step 2. Perfom a lookup on the queue
         Queue queue = new ActiveMQQueue(durableQueueName);

         // Step 4.Create a JMS Connection
         exConn = exFact.createConnection();

         // Step 10. Start the Connection
         exConn.start();

         // Step 5. Create a JMS Session
         Session session = exConn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 6. Create a JMS Message Producer
         MessageProducer producer = session.createProducer(queue);

         // Step 7. Create a Text Message
         TextMessage message = session.createTextMessage("This is a text message");

         //System.out.println("Sent message: " + message.getText());

         // Step 8. Send the Message
         producer.send(message);

         // Step 9. Create a JMS Message Consumer
         MessageConsumer messageConsumer = session.createConsumer(queue);

         // Step 11. Receive the message
         TextMessage messageReceived = (TextMessage) messageConsumer.receive(5000);

         System.out.println("Received message: " + messageReceived);

         assertEquals("This is a text message", messageReceived.getText());
      }
      finally
      {
         if (exConn != null)
         {
            exConn.close();
         }
      }

   }
}
