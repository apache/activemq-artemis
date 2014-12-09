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
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.naming.InitialContext;

import org.apache.activemq.common.example.ActiveMQExample;

/**
 * A simple JMS example that consumes messages using selectors.
 *
 * @author <a href="hgao@redhat.com">Howard Gao</a>
 */
public class TopicSelectorExample2 extends ActiveMQExample
{
   private volatile boolean result = true;

   public static void main(final String[] args)
   {
      new TopicSelectorExample2().run(args);
   }

   @Override
   public boolean runExample() throws Exception
   {
      Connection connection = null;
      InitialContext initialContext = null;
      try
      {
         // /Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = new InitialContext();

         // Step 2. perform a lookup on the topic
         Topic topic = (Topic)initialContext.lookup("topic/exampleTopic");

         // Step 3. perform a lookup on the Connection Factory
         ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("ConnectionFactory");

         // Step 4. Create a JMS Connection
         connection = cf.createConnection();

         // Step 5. Start the Connection
         connection.start();

         // Step 6. Create a JMS Session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 7. Create a Message Producer
         MessageProducer producer = session.createProducer(topic);

         // Step 8. Prepare two selectors
         String redSelector = "color='red'";
         String greenSelector = "color='green'";

         // Step 9. Create a JMS Message Consumer that receives 'red' messages
         MessageConsumer redConsumer = session.createConsumer(topic, redSelector);
         redConsumer.setMessageListener(new SimpleMessageListener("red"));

         // Step 10. Create a second JMS message consumer that receives 'green' messages
         MessageConsumer greenConsumer = session.createConsumer(topic, greenSelector);
         greenConsumer.setMessageListener(new SimpleMessageListener("green"));

         // Step 11. Create another JMS message consumer that receives all messages.
         MessageConsumer allConsumer = session.createConsumer(topic);
         allConsumer.setMessageListener(new SimpleMessageListener("all"));

         // Step 12. Create three messages, each has a color property
         TextMessage redMessage = session.createTextMessage("Red");
         redMessage.setStringProperty("color", "red");
         TextMessage greenMessage = session.createTextMessage("Green");
         greenMessage.setStringProperty("color", "green");
         TextMessage blueMessage = session.createTextMessage("Blue");
         blueMessage.setStringProperty("color", "blue");

         // Step 13. Send the Messages
         producer.send(redMessage);
         System.out.println("Message sent: " + redMessage.getText());
         producer.send(greenMessage);
         System.out.println("Message sent: " + greenMessage.getText());
         producer.send(blueMessage);
         System.out.println("Message sent: " + blueMessage.getText());

         Thread.sleep(5000);

         return result;
      }
      finally
      {
         // Step 14. Be sure to close our JMS resources!
         if (connection != null)
         {
            connection.close();
         }

         // Also the initialContext
         if (initialContext != null)
         {
            initialContext.close();
         }
      }
   }

   public class SimpleMessageListener implements MessageListener
   {

      private final String name;

      public SimpleMessageListener(final String listener)
      {
         name = listener;
      }

      public void onMessage(final Message msg)
      {
         TextMessage textMessage = (TextMessage)msg;
         try
         {
            String colorProp = msg.getStringProperty("color");
            System.out.println("Receiver " + name +
                               " receives message [" +
                               textMessage.getText() +
                               "] with color property: " +
                               colorProp);
            if (!colorProp.equals(name) && !name.equals("all"))
            {
               result = false;
            }
         }
         catch (JMSException e)
         {
            e.printStackTrace();
            result = false;
         }
      }

   }
}
