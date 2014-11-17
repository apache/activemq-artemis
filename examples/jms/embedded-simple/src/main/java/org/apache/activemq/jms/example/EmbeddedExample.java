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

import java.util.Date;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq6.common.example.HornetQExample;
import org.apache.activemq6.jms.server.embedded.EmbeddedJMS;

/**
 * This example demonstrates how to run a HornetQ embedded with JMS
 *
 * @author <a href="clebert.suconic@jboss.com">Clebert Suconic</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class EmbeddedExample extends HornetQExample
{

   public static void main(final String[] args) throws Exception
   {
      new EmbeddedExample().runExample();
   }

   @Override
   public boolean runExample() throws Exception
   {
      try
      {
         EmbeddedJMS jmsServer = new EmbeddedJMS();
         jmsServer.start();
         System.out.println("Started Embedded JMS Server");

         ConnectionFactory cf = (ConnectionFactory)jmsServer.lookup("ConnectionFactory");
         Queue queue = (Queue)jmsServer.lookup("/queue/exampleQueue");

         // Step 10. Send and receive a message using JMS API
         Connection connection = null;
         try
         {
            connection = cf.createConnection();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(queue);
            TextMessage message = session.createTextMessage("Hello sent at " + new Date());
            System.out.println("Sending message: " + message.getText());
            producer.send(message);
            MessageConsumer messageConsumer = session.createConsumer(queue);
            connection.start();
            TextMessage messageReceived = (TextMessage)messageConsumer.receive(1000);
            System.out.println("Received message:" + messageReceived.getText());
         }
         finally
         {
            if (connection != null)
            {
               connection.close();
            }

            // Step 11. Stop the JMS server
            jmsServer.stop();
            System.out.println("Stopped the JMS Server");
         }
      }
      catch (Exception e)
      {
         e.printStackTrace();
         return false;
      }
      return true;
   }
}
