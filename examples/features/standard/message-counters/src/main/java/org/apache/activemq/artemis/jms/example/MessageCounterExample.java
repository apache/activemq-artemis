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
import org.apache.activemq.artemis.api.core.management.MessageCounterInfo;
import org.apache.activemq.artemis.api.core.management.ObjectNameBuilder;
import org.apache.activemq.artemis.api.core.management.QueueControl;

/**
 * An example showing how to use message counters to have information on a queue.
 */
public class MessageCounterExample {

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

         // Step 4.Create a JMS Connection, session and a producer for the queue
         connection = cf.createQueueConnection();
         QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = session.createProducer(queue);

         // Step 5. Create and send a Text Message
         TextMessage message = session.createTextMessage("This is a text message");
         producer.send(message);
         System.out.println("Sent message: " + message.getText());

         // Step 6. Sleep a little bit so that the queue is sampled
         System.out.println("Sleep a little bit to have the queue sampled...");
         Thread.sleep(3000);

         // Step 7. Use JMX to retrieve the message counters using the JMSQueueControl
         ObjectName on = ObjectNameBuilder.DEFAULT.getQueueObjectName(SimpleString.toSimpleString(queue.getQueueName()), SimpleString.toSimpleString(queue.getQueueName()), RoutingType.ANYCAST);
         //we dont actually need credentials as the guest login i sused but this is how its done
         HashMap env = new HashMap();
         String[] creds = {"guest", "guest"};
         env.put(JMXConnector.CREDENTIALS, creds);
         JMXConnector connector = JMXConnectorFactory.connect(new JMXServiceURL(JMX_URL), env);
         MBeanServerConnection mbsc = connector.getMBeanServerConnection();
         QueueControl queueControl = MBeanServerInvocationHandler.newProxyInstance(mbsc, on, QueueControl.class, false);

         // Step 8. List the message counters and convert them to MessageCounterInfo data structure.
         String counters = queueControl.listMessageCounter();
         MessageCounterInfo messageCounter = MessageCounterInfo.fromJSON(counters);

         // Step 9. Display the message counter
         displayMessageCounter(messageCounter);

         // Step 10. Sleep again to have the queue sampled again
         System.out.println("Sleep a little bit again...");
         Thread.sleep(3000);

         // Step 11. List the messages counters again
         counters = queueControl.listMessageCounter();
         messageCounter = MessageCounterInfo.fromJSON(counters);
         displayMessageCounter(messageCounter);

         // Step 12. Create a JMS consumer on the queue
         MessageConsumer consumer = session.createConsumer(queue);

         // Step 13. Start the connection to receive messages on the consumer
         connection.start();

         // Step 14. Receive a JMS message from the queue. It corresponds to the message sent at step #5
         TextMessage messageReceived = (TextMessage) consumer.receive(5000);
         System.out.format("Received message: %s%n%n", messageReceived.getText());

         // Step 15. Sleep on last time to have the queue sampled
         System.out.println("Sleep a little bit one last time...");
         Thread.sleep(3000);

         // Step 16. Display one last time the message counter
         counters = queueControl.listMessageCounter();
         messageCounter = MessageCounterInfo.fromJSON(counters);
         displayMessageCounter(messageCounter);
      } finally {
         // Step 17. Be sure to close our JMS resources!
         if (initialContext != null) {
            initialContext.close();
         }
         if (connection != null) {
            connection.close();
         }
      }
   }

   private static void displayMessageCounter(final MessageCounterInfo counter) {
      System.out.format("%s (sample updated at %s)%n", counter.getName(), counter.getUpdateTimestamp());
      System.out.format("   %s message(s) added to the queue (since last sample: %s)%n", counter.getCount(), counter.getCountDelta());
      System.out.format("   %s message(s) in the queue (since last sample: %s)%n", counter.getDepth(), counter.getDepthDelta());
      System.out.format("   last message added at %s%n%n", counter.getLastAddTimestamp());
   }

}
