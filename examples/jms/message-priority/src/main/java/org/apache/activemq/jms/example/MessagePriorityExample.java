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

import java.util.ArrayList;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import org.apache.activemq.common.example.ActiveMQExample;

/**
 * A simple JMS example that shows the delivery order of messages with priorities.
 *
 * @author <a href="hgao@redhat.com">Howard Gao</a>
 */
public class MessagePriorityExample extends ActiveMQExample
{
   private volatile boolean result = true;

   private final ArrayList<TextMessage> msgReceived = new ArrayList<TextMessage>();

   public static void main(final String[] args)
   {
      new MessagePriorityExample().run(args);
   }

   @Override
   public boolean runExample() throws Exception
   {
      Connection connection = null;
      InitialContext initialContext = null;
      try
      {
         // Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = getContext(0);

         // Step 2. look-up the JMS queue object from JNDI
         Queue queue = (Queue)initialContext.lookup("queue/exampleQueue");

         // Step 3. look-up the JMS connection factory object from JNDI
         ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("ConnectionFactory");

         // Step 4. Create a JMS Connection
         connection = cf.createConnection();

         // Step 5. Create a JMS Session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 6. Create a JMS Message Producer
         MessageProducer producer = session.createProducer(queue);

         // Step 7. Create a JMS Message Consumer
         MessageConsumer redConsumer = session.createConsumer(queue);
         redConsumer.setMessageListener(new SimpleMessageListener());

         // Step 8. Create three messages
         TextMessage[] sentMessages = new TextMessage[3];
         sentMessages[0] = session.createTextMessage("first message");
         sentMessages[1] = session.createTextMessage("second message");
         sentMessages[2] = session.createTextMessage("third message");

         // Step 9. Send the Messages, each has a different priority
         producer.send(sentMessages[0]);
         System.out.println("Message sent: " + sentMessages[0].getText() +
                            " with priority: " +
                            sentMessages[0].getJMSPriority());
         producer.send(sentMessages[1], DeliveryMode.NON_PERSISTENT, 5, 0);
         System.out.println("Message sent: " + sentMessages[1].getText() +
                            "with priority: " +
                            sentMessages[1].getJMSPriority());
         producer.send(sentMessages[2], DeliveryMode.NON_PERSISTENT, 9, 0);
         System.out.println("Message sent: " + sentMessages[2].getText() +
                            "with priority: " +
                            sentMessages[2].getJMSPriority());

         // Step 10. Start the connection now.
         connection.start();

         // Step 11. Wait for message delivery completion
         Thread.sleep(5000);

         // Step 12. Examine the order
         for (int i = 0; i < 3; i++)
         {
            TextMessage rm = msgReceived.get(i);
            if (!rm.getText().equals(sentMessages[2 - i].getText()))
            {
               System.err.println("Priority is broken!");
               result = false;
            }
         }

         return result;
      }
      finally
      {
         // Step 13. Be sure to close our JMS resources!
         if (initialContext != null)
         {
            initialContext.close();
         }
         if (connection != null)
         {
            connection.close();
         }
      }
   }

   public class SimpleMessageListener implements MessageListener
   {

      public SimpleMessageListener()
      {
      }

      public void onMessage(final Message msg)
      {
         TextMessage textMessage = (TextMessage)msg;
         try
         {
            System.out.println("Received message : [" + textMessage.getText() + "]");
         }
         catch (JMSException e)
         {
            result = false;
         }
         msgReceived.add(textMessage);
      }

   }

}
