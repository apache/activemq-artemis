/**
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
package org.apache.activemq.jms.example;

import java.util.Date;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.naming.InitialContext;

import org.apache.activemq.common.example.ActiveMQExample;

/**
 * An example where a client will send a JMS message to a Topic.
 * Browser clients connected using Web Sockets will be able to receive the message.
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class StompWebSocketExample extends ActiveMQExample
{
   public static void main(final String[] args)
   {
      new StompWebSocketExample().run(args);
   }

   @Override
   public boolean runExample() throws Exception
   {
      Connection connection = null;
      InitialContext initialContext = null;
      try
      {
         initialContext = getContext(0);
         Topic topic = (Topic)initialContext.lookup("topic/chat");
         ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("ConnectionFactory");
         connection = cf.createConnection();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer producer = session.createProducer(topic);
         MessageConsumer consumer = session.createConsumer(topic);

         // use JMS bytes message with UTF-8 String to send a text to Stomp clients
         String text = "message sent from a Java application at " + new Date();
         //BytesMessage message = session.createBytesMessage();
         //message.writeBytes(text.getBytes("UTF-8"));
         TextMessage message = session.createTextMessage(text);
         System.out.println("Sent message: " + text);
         System.out.println("Open up the chat/index.html file in a browser and press enter");
         System.in.read();
         producer.send(message);

         connection.start();

         message = (TextMessage)consumer.receive();
         System.out.println("Received message: " + message.getText());

         return true;
      }
      finally
      {
         if (connection != null)
         {
            connection.close();
         }

         if (initialContext != null)
         {
            initialContext.close();
         }
      }
   }
}