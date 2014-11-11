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
package org.apache.activemq6.jms.example;

import java.text.SimpleDateFormat;
import java.util.Date;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import org.apache.activemq6.api.core.Message;
import org.apache.activemq6.common.example.HornetQExample;

/**
 * A simple JMS scheduled delivery example that delivers a message in 5 seconds.
 *
 * @author <a href="hgao@redhat.com">Howard Gao</a>
 */
public class ScheduledMessageExample extends HornetQExample
{
   public static void main(final String[] args)
   {
      new ScheduledMessageExample().run(args);
   }

   @Override
   public boolean runExample() throws Exception
   {
      Connection connection = null;
      InitialContext initialContext = null;
      try
      {
         // Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = getContext(0);

         // Step 2. Perfom a lookup on the queue
         Queue queue = (Queue)initialContext.lookup("/queue/exampleQueue");

         // Step 3. Perform a lookup on the Connection Factory
         ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("/ConnectionFactory");

         // Step 4.Create a JMS Connection
         connection = cf.createConnection();

         // Step 5. Create a JMS Session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 6. Create a JMS Message Producer
         MessageProducer producer = session.createProducer(queue);

         // Step 7. Create a Text Message
         TextMessage message = session.createTextMessage("This is a scheduled message message which will be delivered in 5 sec.");

         // Step 8. Set the delivery time to be 5 sec later.
         long time = System.currentTimeMillis();
         time += 5000;
         message.setLongProperty(Message.HDR_SCHEDULED_DELIVERY_TIME.toString(), time);

         // Step 9. Send the Message
         producer.send(message);

         System.out.println("Sent message: " + message.getText());
         SimpleDateFormat formatter = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss");
         System.out.println("Time of send: " + formatter.format(new Date()));

         // Step 10. Create a JMS Message Consumer
         MessageConsumer messageConsumer = session.createConsumer(queue);

         // Step 11. Start the Connection
         connection.start();

         // Step 12. Receive the message
         TextMessage messageReceived = (TextMessage)messageConsumer.receive();

         System.out.println("Received message: " + messageReceived.getText());
         System.out.println("Time of receive: " + formatter.format(new Date()));

         return true;
      }
      finally
      {
         // Step 13. Be sure to close our JMS resources!
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

}
