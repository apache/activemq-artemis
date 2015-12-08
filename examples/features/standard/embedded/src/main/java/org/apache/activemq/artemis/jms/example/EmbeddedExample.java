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
import java.util.ArrayList;
import java.util.Date;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyAcceptorFactory;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnectorFactory;
import org.apache.activemq.artemis.jms.server.config.ConnectionFactoryConfiguration;
import org.apache.activemq.artemis.jms.server.config.JMSConfiguration;
import org.apache.activemq.artemis.jms.server.config.JMSQueueConfiguration;
import org.apache.activemq.artemis.jms.server.config.impl.ConnectionFactoryConfigurationImpl;
import org.apache.activemq.artemis.jms.server.config.impl.JMSConfigurationImpl;
import org.apache.activemq.artemis.jms.server.config.impl.JMSQueueConfigurationImpl;
import org.apache.activemq.artemis.jms.server.embedded.EmbeddedJMS;

/**
 * This example demonstrates how to run an ActiveMQ Artemis embedded with JMS
 */
public final class EmbeddedExample {

   public static void main(final String[] args) throws Exception {
      // Step 1. Create ActiveMQ Artemis core configuration, and set the properties accordingly
      Configuration configuration = new ConfigurationImpl();
      configuration.setPersistenceEnabled(false);
      configuration.setJournalDirectory("target/data/journal");
      configuration.setSecurityEnabled(false);
      configuration.getAcceptorConfigurations().add(new TransportConfiguration(NettyAcceptorFactory.class.getName()));

      TransportConfiguration connectorConfig = new TransportConfiguration(NettyConnectorFactory.class.getName());

      configuration.getConnectorConfigurations().put("connector", connectorConfig);

      // Step 2. Create the JMS configuration
      JMSConfiguration jmsConfig = new JMSConfigurationImpl();

      // Step 3. Configure the JMS ConnectionFactory
      ArrayList<String> connectorNames = new ArrayList<>();
      connectorNames.add("connector");
      ConnectionFactoryConfiguration cfConfig = new ConnectionFactoryConfigurationImpl().setName("cf").setConnectorNames(connectorNames).setBindings("cf");
      jmsConfig.getConnectionFactoryConfigurations().add(cfConfig);

      // Step 4. Configure the JMS Queue
      JMSQueueConfiguration queueConfig = new JMSQueueConfigurationImpl().setName("queue1").setDurable(false).setBindings("queue/queue1");
      jmsConfig.getQueueConfigurations().add(queueConfig);

      // Step 5. Start the JMS Server using the ActiveMQ Artemis core server and the JMS configuration
      EmbeddedJMS jmsServer = new EmbeddedJMS();
      jmsServer.setConfiguration(configuration);
      jmsServer.setJmsConfiguration(jmsConfig);
      jmsServer.start();
      System.out.println("Started Embedded JMS Server");

      // Step 6. Lookup JMS resources defined in the configuration
      ConnectionFactory cf = (ConnectionFactory) jmsServer.lookup("cf");
      Queue queue = (Queue) jmsServer.lookup("queue/queue1");

      // Step 7. Send and receive a message using JMS API
      Connection connection = null;
      try {
         connection = cf.createConnection();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = session.createProducer(queue);
         TextMessage message = session.createTextMessage("Hello sent at " + new Date());
         System.out.println("Sending message: " + message.getText());
         producer.send(message);
         MessageConsumer messageConsumer = session.createConsumer(queue);
         connection.start();
         TextMessage messageReceived = (TextMessage) messageConsumer.receive(1000);
         System.out.println("Received message:" + messageReceived.getText());
      }
      finally {
         if (connection != null) {
            connection.close();
         }

         // Step 11. Stop the JMS server
         jmsServer.stop();
         System.out.println("Stopped the JMS Server");
      }
   }
}
