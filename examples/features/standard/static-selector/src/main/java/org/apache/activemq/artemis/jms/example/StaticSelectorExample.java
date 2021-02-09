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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A simple JMS example that shows how static message selectors work.
 */
public class StaticSelectorExample {

   public static void main(final String[] args) throws Exception {
      AtomicInteger good = new AtomicInteger(0);
      AtomicInteger bad = new AtomicInteger(0);
      AtomicBoolean failed = new AtomicBoolean(false);
      Connection connection = null;
      InitialContext initialContext = null;
      try {
         // Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = new InitialContext();

         // Step 2. look-up the JMS queue object from JNDI, this is the queue that has filter configured with it.
         Queue queue = (Queue) initialContext.lookup("queue/exampleQueue");

         // Step 3. look-up the JMS connection factory object from JNDI
         ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("ConnectionFactory");

         // Step 4. Create a JMS Connection
         connection = cf.createConnection();

         // Step 5. Start the connection
         connection.start();

         // Step 6. Create a JMS Session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 7. Create a JMS Message Producer
         MessageProducer producer = producerSession.createProducer(queue);

         // Step 8. Create a JMS Message Consumer that receives 'red' messages
         MessageConsumer redConsumer = session.createConsumer(queue);
         redConsumer.setMessageListener(new SimpleMessageListener("red", failed, good, bad));

         // Step 9. Create five messages with different 'color' properties
         TextMessage redMessage1 = session.createTextMessage("Red-1");
         redMessage1.setStringProperty("color", "red");
         TextMessage redMessage2 = session.createTextMessage("Red-2");
         redMessage2.setStringProperty("color", "red");
         TextMessage greenMessage = session.createTextMessage("Green");
         greenMessage.setStringProperty("color", "green");
         TextMessage blueMessage = session.createTextMessage("Blue");
         blueMessage.setStringProperty("color", "blue");
         TextMessage normalMessage = session.createTextMessage("No color");

         // Step 10. Send the Messages
         producer.send(redMessage1);
         System.out.println("Message sent: " + redMessage1.getText());
         producer.send(greenMessage);
         System.out.println("Message sent: " + greenMessage.getText());
         producer.send(blueMessage);
         System.out.println("Message sent: " + blueMessage.getText());
         producer.send(redMessage2);
         System.out.println("Message sent: " + redMessage2.getText());
         producer.send(normalMessage);
         System.out.println("Message sent: " + normalMessage.getText());

         // Step 11. Waiting for the message listener to check the received messages.
         Thread.sleep(5000);

         if (good.get() != 2 || bad.get() != 0 || failed.get())
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
   private AtomicInteger good;
   private AtomicInteger bad;
   private AtomicBoolean failed;

   SimpleMessageListener(final String listener, AtomicBoolean failed, AtomicInteger good, AtomicInteger bad) {
      name = listener;
      this.failed = failed;
      this.good = good;
      this.bad = bad;
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
         if (colorProp != null && colorProp.equals(name)) {
            good.incrementAndGet();
         } else {
            bad.incrementAndGet();
         }
      } catch (JMSException e) {
         e.printStackTrace();
         failed.set(true);
      }
   }
}
