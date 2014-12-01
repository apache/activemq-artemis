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

import java.util.ArrayList;
import java.util.Date;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.common.example.ActiveMQExample;
import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.core.config.impl.ConfigurationImpl;
import org.apache.activemq.core.remoting.impl.netty.NettyAcceptorFactory;
import org.apache.activemq.core.remoting.impl.netty.NettyConnectorFactory;
import org.apache.activemq.jms.server.config.ConnectionFactoryConfiguration;
import org.apache.activemq.jms.server.config.JMSConfiguration;
import org.apache.activemq.jms.server.config.JMSQueueConfiguration;
import org.apache.activemq.jms.server.config.impl.ConnectionFactoryConfigurationImpl;
import org.apache.activemq.jms.server.config.impl.JMSConfigurationImpl;
import org.apache.activemq.jms.server.config.impl.JMSQueueConfigurationImpl;
import org.apache.activemq.jms.server.embedded.EmbeddedJMS;

/**
 * This example demonstrates how to run a ActiveMQ embedded with JMS
 *
 * @author <a href="clebert.suconic@jboss.com">Clebert Suconic</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:bburke@redhat.com">Bill Burke</a>
 */
public final class EmbeddedExample extends ActiveMQExample
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

         // Step 1. Create ActiveMQ core configuration, and set the properties accordingly
         Configuration configuration = new ConfigurationImpl();
         configuration.setPersistenceEnabled(false);
         configuration.setJournalDirectory("target/data/journal");
         configuration.setSecurityEnabled(false);
         configuration.getAcceptorConfigurations()
                      .add(new TransportConfiguration(NettyAcceptorFactory.class.getName()));

         TransportConfiguration connectorConfig = new TransportConfiguration(NettyConnectorFactory.class.getName());

         configuration.getConnectorConfigurations().put("connector", connectorConfig);


         // Step 2. Create the JMS configuration
         JMSConfiguration jmsConfig = new JMSConfigurationImpl();

         // Step 3. Configure the JMS ConnectionFactory
         ArrayList<String> connectorNames = new ArrayList<String>();
         connectorNames.add("connector");
         ConnectionFactoryConfiguration cfConfig = new ConnectionFactoryConfigurationImpl("cf", false,  connectorNames, "/cf");
         jmsConfig.getConnectionFactoryConfigurations().add(cfConfig);

         // Step 4. Configure the JMS Queue
         JMSQueueConfiguration queueConfig = new JMSQueueConfigurationImpl("queue1", null, false, "/queue/queue1");
         jmsConfig.getQueueConfigurations().add(queueConfig);

         // Step 5. Start the JMS Server using the ActiveMQ core server and the JMS configuration
         EmbeddedJMS jmsServer = new EmbeddedJMS();
         jmsServer.setConfiguration(configuration);
         jmsServer.setJmsConfiguration(jmsConfig);
         jmsServer.start();
         System.out.println("Started Embedded JMS Server");

         // Step 6. Lookup JMS resources defined in the configuration
         ConnectionFactory cf = (ConnectionFactory)jmsServer.lookup("/cf");
         Queue queue = (Queue)jmsServer.lookup("/queue/queue1");

         // Step 7. Send and receive a message using JMS API
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
