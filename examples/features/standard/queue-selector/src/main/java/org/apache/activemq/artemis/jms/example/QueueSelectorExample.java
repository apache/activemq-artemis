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
package org.apache.activemq.artemis.jms.example;

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
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A simple JMS example that uses selectors with queue consumers.
 */
public class QueueSelectorExample {

   public static void main(final String[] args) throws Exception {
      AtomicBoolean result = new AtomicBoolean(true);
      Connection connection = null;
      InitialContext initialContext = null;
      try {
         // Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = new InitialContext();

         // Step 2. look-up the JMS queue object from JNDI
         Queue queue = (Queue) initialContext.lookup("queue/exampleQueue");

         // Step 3. look-up the JMS connection factory object from JNDI
         ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("ConnectionFactory");

         // Step 4. Create a JMS Connection
         connection = cf.createConnection();

         // Step 5. Start the connection
         connection.start();

         // Step 5. Create a JMS Session
         Session senderSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 6. Create a JMS Message Producer
         MessageProducer producer = senderSession.createProducer(queue);

         // Step 8. Prepare two selectors
         String redSelector = "color='red'";
         String greenSelector = "color='green'";

         // Step 9. Create a JMS Message Consumer that receives 'red' messages
         Session redSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer redConsumer = redSession.createConsumer(queue, redSelector);
         redConsumer.setMessageListener(new SimpleMessageListener("red", result));

         // Step 10. Create a second JMS message consumer that receives 'green' messages
         Session greenSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer greenConsumer = greenSession.createConsumer(queue, greenSelector);
         greenConsumer.setMessageListener(new SimpleMessageListener("green", result));

         // Step 11. Create another JMS message consumer that receives any messages.
         Session blankSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer anyConsumer = blankSession.createConsumer(queue);
         anyConsumer.setMessageListener(new SimpleMessageListener("any", result));

         // Step 12. Create three messages, each has a color property
         TextMessage redMessage = senderSession.createTextMessage("Red");
         redMessage.setStringProperty("color", "red");
         TextMessage greenMessage = senderSession.createTextMessage("Green");
         greenMessage.setStringProperty("color", "green");
         TextMessage blueMessage = senderSession.createTextMessage("Blue");
         blueMessage.setStringProperty("color", "blue");

         // Step 13. Send the Messages
         producer.send(redMessage);
         System.out.println("Message sent: " + redMessage.getText());
         producer.send(greenMessage);
         System.out.println("Message sent: " + greenMessage.getText());
         producer.send(blueMessage);
         System.out.println("Message sent: " + blueMessage.getText());

         Thread.sleep(5000);

         if (!result.get())
            throw new IllegalStateException();
      } finally {
         // Step 12. Be sure to close our JMS resources!
         if (initialContext != null) {
            initialContext.close();
         }
         if (connection != null) {
            connection.close();
         }
      }
   }
}

class SimpleMessageListener implements MessageListener {

   private final String name;
   private AtomicBoolean result;

   SimpleMessageListener(final String listener, AtomicBoolean result) {
      name = listener;
      this.result = result;
   }

   @Override
   public void onMessage(final Message msg) {
      TextMessage textMessage = (TextMessage) msg;
      try {
         String colorProp = msg.getStringProperty("color");
         System.out.println("Receiver " + name +
                               " receives message [" +
                               textMessage.getText() +
                               "] with color property: " +
                               colorProp);
         if (!colorProp.equals(name) && !name.equals("any")) {
            result.set(false);
         }
      } catch (JMSException e) {
         e.printStackTrace();
         result.set(false);
      }
   }
}
