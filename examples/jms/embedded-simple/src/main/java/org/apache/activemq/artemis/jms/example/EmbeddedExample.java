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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.artemis.common.example.ActiveMQExample;
import org.apache.activemq.artemis.core.config.impl.SecurityConfiguration;
import org.apache.activemq.artemis.jms.server.embedded.EmbeddedJMS;
import org.apache.activemq.artemis.jms.server.JMSServerManager;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManagerImpl;
import org.apache.activemq.artemis.api.jms.JMSFactoryType;

/**
 * This example demonstrates how to run an ActiveMQ Artemis embedded with JMS
 */
public class EmbeddedExample extends ActiveMQExample
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

         SecurityConfiguration securityConfig = new SecurityConfiguration();
         securityConfig.addUser("guest", "guest");
         securityConfig.addRole("guest", "guest");
         securityConfig.setDefaultUser("guest");
         jmsServer.setSecurityManager(new ActiveMQSecurityManagerImpl(securityConfig));

         jmsServer.start();
         System.out.println("Started Embedded JMS Server");

         JMSServerManager jmsServerManager = jmsServer.getJMSServerManager();
         List<String> connectors = new ArrayList<String>();
         connectors.add("in-vm");
         jmsServerManager.createConnectionFactory("ConnectionFactory", false, JMSFactoryType.CF, connectors, "ConnectionFactory");
         jmsServerManager.createQueue(false, "exampleQueue", null, false, "queue/exampleQueue");

         ConnectionFactory cf = (ConnectionFactory)jmsServer.lookup("ConnectionFactory");
         Queue queue = (Queue)jmsServer.lookup("queue/exampleQueue");

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
