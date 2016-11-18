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

import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueRequestor;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.api.jms.management.JMSManagementHelper;
import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.api.core.FilterConstants;

/**
 * An example that shows how to manage ActiveMQ Artemis using JMS messages.
 */
public class ManagementExample {

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

         // Step 9. create the JMS management queue.
         // It is a "special" queue and it is not looked up from JNDI but constructed directly
         Queue managementQueue = ActiveMQJMSClient.createQueue("activemq.management");

         // Step 10. Create a QueueRequestor for the management queue (see queue-requestor example)
         QueueRequestor requestor = new QueueRequestor(session, managementQueue);

         // Step 11. Start the Connection to allow the queue requestor to receive replies
         connection.start();

         // Step 12. Create a JMS message which is used to send a management message
         Message m = session.createMessage();

         // Step 13. Use a helper class to fill the JMS message with management information:
         // * the name of the resource to manage
         // * in this case, we want to retrieve the value of the messageCount of the queue
         JMSManagementHelper.putAttribute(m, ResourceNames.QUEUE + "exampleQueue", "messageCount");

         // Step 14. Use the requestor to send the request and wait for the reply
         Message reply = requestor.request(m);

         // Step 15. Use a helper class to retrieve the operation result
         int messageCount = (Integer) JMSManagementHelper.getResult(reply, Integer.class);
         System.out.println(queue.getQueueName() + " contains " + messageCount + " messages");

         // Step 16. Create another JMS message to use as a management message
         m = session.createMessage();

         // Step 17. Use a helper class to fill the JMS message with management information:
         // * the object name of the resource to manage (i.e. the queue)
         // * in this case, we want to call the "removeMessage" operation with the JMS MessageID
         // of the message sent to the queue in step 8.
         JMSManagementHelper.putOperationInvocation(m, ResourceNames.QUEUE + "exampleQueue", "removeMessages", FilterConstants.ACTIVEMQ_USERID + " = '" + message.getJMSMessageID() + "'");

         // Step 18 Use the requestor to send the request and wait for the reply
         reply = requestor.request(m);

         // Step 19. Use a helper class to check that the operation has succeeded
         boolean success = JMSManagementHelper.hasOperationSucceeded(reply);
         System.out.println("operation invocation has succeeded: " + success);

         // Step 20. Use a helper class to retrieve the operation result
         // in that case, a long which is 1 if the message was removed, 0 else
         boolean messageRemoved = 1 == (long) JMSManagementHelper.getResult(reply);
         System.out.println("message has been removed: " + messageRemoved);

         // Step 21. Create a JMS Message Consumer on the queue
         MessageConsumer messageConsumer = session.createConsumer(queue);

         // Step 22. Trying to receive a message. Since the only message in the queue was removed by a management
         // operation,
         // there is none to consume. The call will timeout after 5000ms and messageReceived will be null
         TextMessage messageReceived = (TextMessage) messageConsumer.receive(5000);
         System.out.println("Received message: " + messageReceived);
      } finally {
         // Step 23. Be sure to close the resources!
         if (initialContext != null) {
            initialContext.close();
         }
         if (connection != null) {
            connection.close();
         }
      }
   }
}
