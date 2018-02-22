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

import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.naming.InitialContext;
import java.util.HashMap;

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.management.ObjectNameBuilder;
import org.apache.activemq.artemis.api.core.management.QueueControl;

/**
 * An example that shows how to manage ActiveMQ Artemis using JMX.
 */
public class JMXExample {

   private static final String JMX_URL = "service:jmx:rmi:///jndi/rmi://localhost:1099/jmxrmi";

   public static void main(final String[] args) throws Exception {
      QueueConnection connection = null;
      InitialContext initialContext = null;
      try {
         // Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = new InitialContext();

         // Step 2. Perfom a lookup on the queue
         Queue queue = (Queue) initialContext.lookup("queue/exampleQueue");

         // Step 3. Perform a lookup on the Connection Factory
         QueueConnectionFactory cf = (QueueConnectionFactory) initialContext.lookup("ConnectionFactory");

         // Step 4.Create a JMS Connection
         connection = cf.createQueueConnection();

         // Step 5. Create a JMS Session
         QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 6. Create a JMS Message Producer
         MessageProducer producer = session.createProducer(queue);

         // Step 7. Create a Text Message
         TextMessage message = session.createTextMessage("This is a text message");
         System.out.println("Sent message: " + message.getText());

         // Step 8. Send the Message
         producer.send(message);

         // Step 9. Retrieve the ObjectName of the queue. This is used to identify the server resources to manage
         ObjectName on = ObjectNameBuilder.DEFAULT.getQueueObjectName(SimpleString.toSimpleString(queue.getQueueName()), SimpleString.toSimpleString(queue.getQueueName()), RoutingType.ANYCAST);

         // Step 10. Create JMX Connector to connect to the server's MBeanServer
         //we dont actually need credentials as the guest login i sused but this is how its done
         HashMap env = new HashMap();
         String[] creds = {"guest", "guest"};
         env.put(JMXConnector.CREDENTIALS, creds);

         JMXConnector connector = JMXConnectorFactory.connect(new JMXServiceURL(JMXExample.JMX_URL), env);

         // Step 11. Retrieve the MBeanServerConnection
         MBeanServerConnection mbsc = connector.getMBeanServerConnection();

         // Step 12. Create a QueueControl proxy to manage the queue on the server
         QueueControl queueControl = MBeanServerInvocationHandler.newProxyInstance(mbsc, on, QueueControl.class, false);
         // Step 13. Display the number of messages in the queue
         System.out.println(queueControl.getName() + " contains " + queueControl.getMessageCount() + " messages");

         // Step 14. Remove the message sent at step #8
         System.out.println("message has been removed: " + queueControl.removeMessages(null));

         // Step 15. Display the number of messages in the queue
         System.out.println(queueControl.getName() + " contains " + queueControl.getMessageCount() + " messages");

         // Step 16. We close the JMX connector
         connector.close();

         // Step 17. Create a JMS Message Consumer on the queue
         MessageConsumer messageConsumer = session.createConsumer(queue);

         // Step 18. Start the Connection
         connection.start();

         // Step 19. Trying to receive a message. Since the only message in the queue was removed by a management
         // operation, there is none to consume.
         // The call will timeout after 5000ms and messageReceived will be null
         TextMessage messageReceived = (TextMessage) messageConsumer.receive(5000);

         if (messageReceived != null) {
            throw new IllegalStateException("message should be null!");
         }

         System.out.println("Received message: " + messageReceived);
      } finally {
         // Step 20. Be sure to close the resources!
         if (initialContext != null) {
            initialContext.close();
         }
         if (connection != null) {
            connection.close();
         }
      }
   }
}
