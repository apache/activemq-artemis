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
package org.apache.activemq.jms.example;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.common.example.ActiveMQExample;

/**
 * A simple JMS Queue example that creates a producer and consumer on a queue
 * and sends then receives a message.
 * 
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class OpenWireExample extends ActiveMQExample
{
   public static final String OWHOST = "localhost";
   public static final int OWPORT = 61616;

   public static void main(final String[] args)
   {
      new OpenWireExample().run(args);
   }

   @Override
   public boolean runExample() throws Exception
   {
      Connection connection = null;

      try
      {
         String urlString = "tcp://" + OWHOST + ":" + OWPORT;

         // Step 1. Create an ActiveMQ Connection Factory
         ConnectionFactory factory = new ActiveMQConnectionFactory(urlString);

         // Step 2. Create the target queue
         Queue queue = new ActiveMQQueue("exampleQueue");

         // Step 3. Create a JMS Connection
         connection = factory.createConnection();

         // Step 4. Start the Connection
         connection.start();

         // Step 5. Create a JMS Session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 6. Create a JMS Message Producer
         MessageProducer producer = session.createProducer(queue);

         // Step 7. Create a Text Message
         TextMessage message = session.createTextMessage("This is a text message");

         // Step 8. Send the Message
         producer.send(message);

         System.out.println("Sent message: " + message.getText());

         // Step 9. Create a JMS Message Consumer
         MessageConsumer messageConsumer = session.createConsumer(queue);

         // Step 10. Receive the message
         TextMessage messageReceived = (TextMessage) messageConsumer.receive(5000);

         System.out.println("Received message: " + messageReceived.getText());

         return true;
      }
      finally
      {
         if (connection != null)
         {
            connection.close();
         }
      }
   }
}
