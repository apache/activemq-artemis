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
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

/**
 * A simple JMS example that shows the delivery order of messages with priorities.
 */
public class MessagePriorityExample {

   public static void main(final String[] args) throws Exception {
      AtomicBoolean result = new AtomicBoolean(true);
      final ArrayList<TextMessage> msgReceived = new ArrayList<>();
      Connection connection = null;
      try {

         // Step 2. look-up the JMS queue object from JNDI
         Queue queue = ActiveMQJMSClient.createQueue("exampleQueue");

         // Step 3. look-up the JMS connection factory object from JNDI
         ConnectionFactory cf = new ActiveMQConnectionFactory();

         // Step 4. Create a JMS Connection
         connection = cf.createConnection();

         // Step 5. Create a JMS Session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 6. Create a JMS Message Producer
         MessageProducer producer = session.createProducer(queue);
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

         MessageConsumer redConsumer = session.createConsumer(queue);
         redConsumer.setMessageListener(new SimpleMessageListener(msgReceived, result));

         // Step 10. Start the connection now.
         connection.start();

         // Step 11. Wait for message delivery completion
         Thread.sleep(5000);

         // Step 12. Examine the order
         for (int i = 0; i < 3; i++) {
            TextMessage rm = msgReceived.get(i);
            if (!rm.getText().equals(sentMessages[2 - i].getText())) {
               throw new IllegalStateException("Priority is broken!");
            }
         }

         if (!result.get())
            throw new IllegalStateException();
      } finally {
         // Step 13. Be sure to close our JMS resources!
         if (connection != null) {
            connection.close();
         }
      }
   }
}

class SimpleMessageListener implements MessageListener {

   ArrayList<TextMessage> msgReceived;
   AtomicBoolean result;

   SimpleMessageListener(ArrayList<TextMessage> msgReceived, AtomicBoolean result) {
      this.msgReceived = msgReceived;
      this.result = result;
   }

   @Override
   public void onMessage(final Message msg) {
      TextMessage textMessage = (TextMessage) msg;
      try {
         System.out.println("Received message : [" + textMessage.getText() + "]");
      } catch (JMSException e) {
         result.set(false);
      }
      msgReceived.add(textMessage);
   }

}
