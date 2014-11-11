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


import org.apache.qpid.amqp_1_0.client.Connection;
import org.apache.qpid.amqp_1_0.client.Message;
import org.apache.qpid.amqp_1_0.client.Receiver;
import org.apache.qpid.amqp_1_0.client.Sender;
import org.apache.qpid.amqp_1_0.client.Session;
import org.apache.qpid.amqp_1_0.type.UnsignedInteger;
import org.apache.activemq6.common.example.HornetQExample;

public class ProtonJExample extends HornetQExample
{
   public static void main(String[] args)
   {
      new ProtonJExample().run(args);
   }
   @Override
   public boolean runExample() throws Exception
   {
      Connection connection = null;

      try
      {
         // Step 1. Create an amqp qpid 1.0 connection
         connection= new Connection("localhost", 5672, null, null);

         // Step 2. Create a session
         Session session = connection.createSession();

         // Step 3. Create a sender
         Sender sender = session.createSender("jms.queue.exampleQueue");

         // Step 4. send a simple message
         sender.send(new Message("I am an amqp message"));

         // Step 5. create a moving receiver, this means the message will be removed from the queue
         Receiver rec = session.createMovingReceiver("jms.queue.exampleQueue");

         // Step 6. set some credit so we can receive
         rec.setCredit(UnsignedInteger.valueOf(1), false);

         // Step 7. receive the simple message
         Message m = rec.receive(5000);
         System.out.println("message = " + m.getPayload());

         // Step 8. acknowledge the message
         rec.acknowledge(m);
      }
      finally
      {
         if(connection != null)
         {
            // Step 9. close the connection
            connection.close();
         }
      }

      return true;
   }
}
