/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.activemq.jms.example;

import java.util.HashMap;

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

import org.apache.activemq.api.core.management.MessageCounterInfo;
import org.apache.activemq.api.core.management.ObjectNameBuilder;
import org.apache.activemq.api.jms.management.JMSQueueControl;
import org.apache.activemq.common.example.ActiveMQExample;

/**
 * An example showing how to use message counters to have information on a queue.
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 */
public class MessageCounterExample extends ActiveMQExample
{
   private static final String JMX_URL = "service:jmx:rmi:///jndi/rmi://localhost:3001/jmxrmi";

   public static void main(final String[] args)
   {
      new MessageCounterExample().run(args);
   }

   @Override
   public boolean runExample() throws Exception
   {
      QueueConnection connection = null;
      InitialContext initialContext = null;
      try
      {
         // Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = getContext(0);

         // Step 2. Perfom a lookup on the queue
         Queue queue = (Queue)initialContext.lookup("/queue/exampleQueue");

         // Step 3. Perform a lookup on the Connection Factory
         QueueConnectionFactory cf = (QueueConnectionFactory)initialContext.lookup("/ConnectionFactory");

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
         ObjectName on = ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queue.getQueueName());
         JMXConnector connector =
                  JMXConnectorFactory.connect(new JMXServiceURL(JMX_URL), new HashMap<String, Object>());
         MBeanServerConnection mbsc = connector.getMBeanServerConnection();
         JMSQueueControl queueControl = MBeanServerInvocationHandler.newProxyInstance(mbsc,
                                                                                      on,
                                                                                      JMSQueueControl.class,
                                                                                      false);

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
         TextMessage messageReceived = (TextMessage)consumer.receive(5000);
         System.out.format("Received message: %s%n%n", messageReceived.getText());

         // Step 15. Sleep on last time to have the queue sampled
         System.out.println("Sleep a little bit one last time...");
         Thread.sleep(3000);

         // Step 16. Display one last time the message counter
         counters = queueControl.listMessageCounter();
         messageCounter = MessageCounterInfo.fromJSON(counters);
         displayMessageCounter(messageCounter);

         return true;
      }
      finally
      {
         // Step 17. Be sure to close our JMS resources!
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

   private void displayMessageCounter(final MessageCounterInfo counter)
   {
      System.out.format("%s (sample updated at %s)%n", counter.getName(), counter.getUdpateTimestamp());
      System.out.format("   %s message(s) added to the queue (since last sample: %s)%n",
                        counter.getCount(),
                        counter.getCountDelta());
      System.out.format("   %s message(s) in the queue (since last sample: %s)%n",
                        counter.getDepth(),
                        counter.getDepthDelta());
      System.out.format("   last message added at %s%n%n", counter.getLastAddTimestamp());
   }

}
