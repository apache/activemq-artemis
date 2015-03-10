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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
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
 * A simple JMS Queue example that sends and receives message groups.
 */
public class MessageGroup2Example extends ActiveMQExample
{
   private final Map<String, String> messageReceiverMap = new ConcurrentHashMap<String, String>();
   private boolean result = true;

   public static void main(String[] args)
   {
      new MessageGroup2Example().run(args);
   }

   @Override
   public boolean runExample() throws Exception
   {
      Connection connection = null;
      InitialContext initialContext = null;
      try
      {
         //Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = new InitialContext();

         //Step 2. Perform a lookup on the queue
         Queue queue = (Queue) initialContext.lookup("queue/exampleQueue");

         //Step 3. Perform a lookup on the Connection Factory
         ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("ConnectionFactory");

         //Step 4. Create a JMS Connection
         connection = cf.createConnection();

         //Step 5. Create a JMS Session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         //Step 6. Create 2 JMS Message Producers
         MessageProducer producer1 = session.createProducer(queue);

         MessageProducer producer2 = session.createProducer(queue);

         //Step 7. Create two consumers
         MessageConsumer consumer1 = session.createConsumer(queue);
         consumer1.setMessageListener(new SimpleMessageListener("consumer-1"));
         MessageConsumer consumer2 = session.createConsumer(queue);
         consumer2.setMessageListener(new SimpleMessageListener("consumer-2"));

         //Step 8. Create and send 10 text messages with each producer
         int msgCount = 10;
         for (int i = 0; i < msgCount; i++)
         {
            TextMessage m = session.createTextMessage("producer1 message " + i);
            producer1.send(m);
            System.out.println("Sent message: " + m.getText());
            TextMessage m2 = session.createTextMessage("producer2 message " + i);
            producer2.send(m2);
            System.out.println("Sent message: " + m2.getText());
         }

         System.out.println("all messages are sent");

         //Step 9. Start the connection
         connection.start();

         Thread.sleep(2000);

         //Step 10. check the group messages are received by only one consumer

         String trueReceiver = messageReceiverMap.get("producer1 message " + 0);
         for (int i = 0; i < msgCount; i++)
         {
            String receiver = messageReceiverMap.get("producer1 message " + i);
            if (!trueReceiver.equals(receiver))
            {
               System.out.println("Group message [producer1 message " + i + "] went to wrong receiver: " + receiver);
               result = false;
            }
            receiver = messageReceiverMap.get("producer2 message " + i);
            if (!trueReceiver.equals(receiver))
            {
               System.out.println("Group message [producer2 message " + i + "] went to wrong receiver: " + receiver);
               result = false;
            }
         }

         return result;
      }
      finally
      {
         //Step 11. Be sure to close our JMS resources!
         if (initialContext != null)
         {
            initialContext.close();
         }
         if(connection != null)
         {
            connection.close();
         }
      }
   }

   private class SimpleMessageListener implements MessageListener
   {
      private final String name;

      public SimpleMessageListener(String listenerName)
      {
         name = listenerName;
      }

      public void onMessage(Message message)
      {
         try
         {
            TextMessage msg = (TextMessage)message;
            System.out.format("Message: [%s] received by %s%n",
                              msg.getText(),
                              name);
            messageReceiverMap.put(msg.getText(), name);
         }
         catch (JMSException e)
         {
            e.printStackTrace();
         }
      }
   }

}