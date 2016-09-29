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
import javax.jms.Topic;
import javax.naming.InitialContext;
import java.util.Enumeration;

/**
 * An example that shows how to receive management notifications using JMS messages.
 */
public class ManagementNotificationExample {

   public static void main(final String[] args) throws Exception {
      Connection connection = null;
      InitialContext initialContext = null;
      try {
         // Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = new InitialContext();

         // Step 2. Perform a lookup on the queue
         Queue queue = (Queue) initialContext.lookup("queue/exampleQueue");

         // Step 3. Perform a lookup on the Connection Factory
         ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("ConnectionFactory");

         // Step 4.Create a JMS connection, a session and a producer for the queue
         connection = cf.createConnection();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = session.createProducer(queue);

         // Step 5. Perform a lookup on the notifications topic
         Topic notificationsTopic = (Topic) initialContext.lookup("topic/notificationsTopic");

         // Step 6. Create a JMS message consumer for the notification queue and set its message listener
         // It will display all the properties of the JMS Message
         MessageConsumer notificationConsumer = session.createConsumer(notificationsTopic);
         notificationConsumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(final Message notif) {
               System.out.println("------------------------");
               System.out.println("Received notification:");
               try {
                  Enumeration propertyNames = notif.getPropertyNames();
                  while (propertyNames.hasMoreElements()) {
                     String propertyName = (String) propertyNames.nextElement();
                     System.out.format("  %s: %s%n", propertyName, notif.getObjectProperty(propertyName));
                  }
               } catch (JMSException e) {
               }
               System.out.println("------------------------");
            }
         });

         // Step 7. Start the Connection to allow the consumers to receive messages
         connection.start();

         // Step 8. Create a JMS Message Consumer on the queue
         MessageConsumer consumer = session.createConsumer(queue);

         // Step 9. Close the consumer
         consumer.close();

         // Step 10. Try to create a connection with unknown user
         try {
            cf.createConnection("not.a.valid.user", "not.a.valid.password");
         } catch (JMSException e) {
         }

         // sleep a little bit to be sure to receive the notification for the security
         // authentication violation before leaving the example
         Thread.sleep(2000);
      } finally {
         // Step 11. Be sure to close the resources!
         if (initialContext != null) {
            initialContext.close();
         }
         if (connection != null) {
            connection.close();
         }
      }
   }
}
