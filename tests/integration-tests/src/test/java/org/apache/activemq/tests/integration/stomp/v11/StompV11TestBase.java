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
package org.apache.activemq.tests.integration.stomp.v11;
import org.junit.Before;
import org.junit.After;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.core.protocol.stomp.StompProtocolManagerFactory;
import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.apache.activemq.core.remoting.impl.invm.InVMConnectorFactory;
import org.apache.activemq.core.remoting.impl.netty.NettyAcceptorFactory;
import org.apache.activemq.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.core.server.HornetQServer;
import org.apache.activemq.core.server.HornetQServers;
import org.apache.activemq.jms.client.HornetQJMSConnectionFactory;
import org.apache.activemq.jms.server.JMSServerManager;
import org.apache.activemq.jms.server.config.JMSConfiguration;
import org.apache.activemq.jms.server.config.impl.JMSConfigurationImpl;
import org.apache.activemq.jms.server.config.impl.JMSQueueConfigurationImpl;
import org.apache.activemq.jms.server.config.impl.TopicConfigurationImpl;
import org.apache.activemq.jms.server.impl.JMSServerManagerImpl;
import org.apache.activemq.tests.unit.util.InVMNamingContext;
import org.apache.activemq.tests.util.UnitTestCase;

public abstract class StompV11TestBase extends UnitTestCase
{
   protected String hostname = "127.0.0.1";

   protected int port = 61613;

   private ConnectionFactory connectionFactory;

   private Connection connection;

   protected Session session;

   protected Queue queue;

   protected Topic topic;

   protected JMSServerManager server;

   protected String defUser = "brianm";

   protected String defPass = "wombats";

   protected boolean persistenceEnabled = false;

   // Implementation methods
   // -------------------------------------------------------------------------
   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      server = createServer();
      server.start();
      connectionFactory = createConnectionFactory();

      connection = connectionFactory.createConnection();
      session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      queue = session.createQueue(getQueueName());
      topic = session.createTopic(getTopicName());
      connection.start();
   }

   /**
   * @return
   * @throws Exception
   */
   protected JMSServerManager createServer() throws Exception
   {
      Map<String, Object> params = new HashMap<String, Object>();
      params.put(TransportConstants.PROTOCOL_PROP_NAME, StompProtocolManagerFactory.STOMP_PROTOCOL_NAME);
      params.put(TransportConstants.PORT_PROP_NAME, TransportConstants.DEFAULT_STOMP_PORT);
      params.put(TransportConstants.STOMP_CONSUMERS_CREDIT, "-1");
      TransportConfiguration stompTransport = new TransportConfiguration(NettyAcceptorFactory.class.getName(), params);

      Configuration config = createBasicConfig()
         .setPersistenceEnabled(persistenceEnabled)
         .addAcceptorConfiguration(stompTransport)
         .addAcceptorConfiguration(new TransportConfiguration(InVMAcceptorFactory.class.getName()));

      HornetQServer hornetQServer = addServer(HornetQServers.newHornetQServer(config, defUser, defPass));

      JMSConfiguration jmsConfig = new JMSConfigurationImpl();
      jmsConfig.getQueueConfigurations().add(new JMSQueueConfigurationImpl()
                                                .setName(getQueueName())
                                                .setBindings(getQueueName()));
      jmsConfig.getTopicConfigurations().add(new TopicConfigurationImpl()
                                                .setName(getTopicName())
                                                .setBindings(getTopicName()));
      server = new JMSServerManagerImpl(hornetQServer, jmsConfig);
      server.setContext(new InVMNamingContext());
      return server;
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      try
      {
         if (connection != null)
            connection.close();
         if (server != null)
            server.stop();
      }
      finally
      {
         super.tearDown();
      }
   }

   protected ConnectionFactory createConnectionFactory()
   {
      return new HornetQJMSConnectionFactory(false, new TransportConfiguration(InVMConnectorFactory.class.getName()));
   }

   protected String getQueueName()
   {
      return "test";
   }

   protected String getQueuePrefix()
   {
      return "jms.queue.";
   }

   protected String getTopicName()
   {
      return "testtopic";
   }

   protected String getTopicPrefix()
   {
      return "jms.topic.";
   }

   public void sendMessage(String msg) throws Exception
   {
      sendMessage(msg, queue);
   }

   public void sendMessage(String msg, Destination destination) throws Exception
   {
      MessageProducer producer = session.createProducer(destination);
      TextMessage message = session.createTextMessage(msg);
      producer.send(message);
   }

   public void sendMessage(byte[] data, Destination destination) throws Exception
   {
      sendMessage(data, "foo", "xyz", destination);
   }

   public void sendMessage(String msg, String propertyName, String propertyValue) throws Exception
   {
      sendMessage(msg.getBytes(StandardCharsets.UTF_8), propertyName, propertyValue, queue);
   }

   public void sendMessage(byte[] data, String propertyName, String propertyValue, Destination destination) throws Exception
   {
      MessageProducer producer = session.createProducer(destination);
      BytesMessage message = session.createBytesMessage();
      message.setStringProperty(propertyName, propertyValue);
      message.writeBytes(data);
      producer.send(message);
   }

}
