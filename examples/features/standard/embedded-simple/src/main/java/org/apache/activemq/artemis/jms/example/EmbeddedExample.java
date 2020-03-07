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
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;
import java.util.Date;

import org.apache.activemq.artemis.core.config.impl.SecurityConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.apache.activemq.artemis.spi.core.security.jaas.InVMLoginModule;

/**
 * This example demonstrates how to run an embedded ActiveMQ Artemis broker with external file configuration
 */
public class EmbeddedExample {

   public static void main(final String[] args) throws Exception {
      // Step 1. Configure security.
      SecurityConfiguration securityConfig = new SecurityConfiguration();
      securityConfig.addUser("guest", "guest");
      securityConfig.addRole("guest", "guest");
      securityConfig.setDefaultUser("guest");
      ActiveMQJAASSecurityManager securityManager = new ActiveMQJAASSecurityManager(InVMLoginModule.class.getName(), securityConfig);

      // Step 2. Create and start embedded broker.
      ActiveMQServer server = ActiveMQServers.newActiveMQServer("broker.xml", null, securityManager);
      server.start();
      System.out.println("Started Embedded Broker");

      InitialContext initialContext = null;
      // Step 3. Create an initial context to perform the JNDI lookup.
      initialContext = new InitialContext();

      // Step 4. Look-up the JMS queue
      Queue queue = (Queue) initialContext.lookup("queue/exampleQueue");

      // Step 5. Look-up the JMS connection factory
      ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("ConnectionFactory");

      // Step 6. Send and receive a message using JMS API
      try (Connection connection = cf.createConnection()) {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = session.createProducer(queue);
         TextMessage message = session.createTextMessage("Hello sent at " + new Date());
         System.out.println("Sending message: " + message.getText());
         producer.send(message);
         MessageConsumer messageConsumer = session.createConsumer(queue);
         connection.start();
         TextMessage messageReceived = (TextMessage) messageConsumer.receive(1000);
         System.out.println("Received message:" + messageReceived.getText());
      } finally {
         // Step 7. Stop the embedded broker.
         server.stop();
         System.out.println("Stopped the Embedded Broker");
      }
   }
}
