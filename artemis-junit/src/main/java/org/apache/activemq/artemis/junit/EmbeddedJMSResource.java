/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.junit;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;
import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.FileDeploymentManager;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.config.impl.FileConfiguration;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMAcceptorFactory;
import org.apache.activemq.artemis.core.remoting.impl.invm.TransportConstants;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.BindingQueryResult;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQDestination;
import org.apache.activemq.artemis.jms.client.DefaultConnectionProperties;
import org.apache.activemq.artemis.jms.server.config.JMSConfiguration;
import org.apache.activemq.artemis.jms.server.config.impl.FileJMSConfiguration;
import org.apache.activemq.artemis.jms.server.config.impl.JMSConfigurationImpl;
import org.apache.activemq.artemis.jms.server.embedded.EmbeddedJMS;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A JUnit Rule that embeds an ActiveMQ Artemis JMS server into a test.
 *
 * This JUnit Rule is designed to simplify using embedded servers in unit tests.  Adding the rule to a test will startup
 * an embedded JMS server, which can then be used by client applications.
 *
 * <pre><code>
 * public class SimpleTest {
 *     @Rule
 *     public EmbeddedJMSResource server = new EmbeddedJMSResource();
 *
 *     @Test
 *     public void testSomething() throws Exception {
 *         // Use the embedded server here
 *     }
 * }
 * </code></pre>
 */
public class EmbeddedJMSResource extends ExternalResource {

   static final String SERVER_NAME = "embedded-jms-server";

   Logger log = LoggerFactory.getLogger(this.getClass());
   Integer serverId = null;

   Configuration configuration;
   JMSConfiguration jmsConfiguration;
   EmbeddedJMS jmsServer;

   InternalClient internalClient;

   /**
    * Create a default EmbeddedJMSResource
    */
   public EmbeddedJMSResource() {
      this(false);
   }

   /**
    * Create a default EmbeddedJMSResource
    */
   public EmbeddedJMSResource(boolean useNetty) {
      try {
         configuration = new ConfigurationImpl().setName(SERVER_NAME).setPersistenceEnabled(false).setSecurityEnabled(false).addAcceptorConfiguration("invm", "vm://0");

         if (useNetty) {
            configuration.addAcceptorConfiguration("netty", DefaultConnectionProperties.DEFAULT_BROKER_BIND_URL);
         }

         jmsConfiguration = new JMSConfigurationImpl();

         init();
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   /**
    * The acceptor used
    */
   public EmbeddedJMSResource addAcceptor(String name, String uri) throws Exception {
      configuration.addAcceptorConfiguration(name, uri);
      return this;
   }

   /**
    * Create a default EmbeddedJMSResource with the specified server id
    */
   public EmbeddedJMSResource(int serverId) {
      this.serverId = serverId;
      Map<String, Object> props = new HashMap<>();
      props.put(TransportConstants.SERVER_ID_PROP_NAME, serverId);

      configuration = new ConfigurationImpl().setName(SERVER_NAME + "-" + serverId).setPersistenceEnabled(false).setSecurityEnabled(false).addAcceptorConfiguration(new TransportConfiguration(InVMAcceptorFactory.class.getName(), props));

      jmsConfiguration = new JMSConfigurationImpl();

      init();
   }

   /**
    * Create an EmbeddedJMSResource with the specified configurations
    *
    * @param configuration    ActiveMQServer configuration
    * @param jmsConfiguration JMSServerManager configuration
    */
   public EmbeddedJMSResource(Configuration configuration, JMSConfiguration jmsConfiguration) {
      this.configuration = configuration;
      this.jmsConfiguration = jmsConfiguration;
      init();
   }

   /**
    * Create an EmbeddedJMSResource with the specified configuration file
    *
    * @param filename configuration file name
    */
   public EmbeddedJMSResource(String filename) {
      this(filename, filename);
   }

   /**
    * Create an EmbeddedJMSResource with the specified configuration file
    *
    * @param serverConfigurationFileName ActiveMQServer configuration file name
    * @param jmsConfigurationFileName    JMSServerManager configuration file name
    */
   public EmbeddedJMSResource(String serverConfigurationFileName, String jmsConfigurationFileName) {
      if (serverConfigurationFileName == null) {
         throw new IllegalArgumentException("ActiveMQServer configuration file name cannot be null");
      }
      if (jmsConfigurationFileName == null) {
         throw new IllegalArgumentException("JMSServerManager configuration file name cannot be null");
      }

      FileDeploymentManager coreDeploymentManager = new FileDeploymentManager(serverConfigurationFileName);
      FileConfiguration coreConfiguration = new FileConfiguration();
      coreDeploymentManager.addDeployable(coreConfiguration);
      try {
         coreDeploymentManager.readConfiguration();
      } catch (Exception readCoreConfigEx) {
         throw new EmbeddedJMSResourceException(String.format("Failed to read ActiveMQServer configuration from file %s", serverConfigurationFileName), readCoreConfigEx);
      }
      this.configuration = coreConfiguration;

      FileJMSConfiguration jmsConfiguration = new FileJMSConfiguration();
      FileDeploymentManager jmsDeploymentManager = new FileDeploymentManager(jmsConfigurationFileName);
      jmsDeploymentManager.addDeployable(jmsConfiguration);
      try {
         jmsDeploymentManager.readConfiguration();
      } catch (Exception readJmsConfigEx) {
         throw new EmbeddedJMSResourceException(String.format("Failed to read JMSServerManager configuration from file %s", jmsConfigurationFileName), readJmsConfigEx);
      }
      this.jmsConfiguration = jmsConfiguration;

      init();
   }

   public static void setMessageProperties(Message message, Map<String, Object> properties) {
      if (properties != null && properties.size() > 0) {
         for (Map.Entry<String, Object> property : properties.entrySet()) {
            try {
               message.setObjectProperty(property.getKey(), property.getValue());
            } catch (JMSException jmsEx) {
               throw new EmbeddedJMSResourceException(String.format("Failed to set property {%s = %s}", property.getKey(), property.getValue().toString()), jmsEx);
            }
         }
      }
   }

   private void init() {
      if (jmsServer == null) {
         jmsServer = new EmbeddedJMS().setConfiguration(configuration).setJmsConfiguration(jmsConfiguration);
      }
   }

   /**
    * Start the embedded EmbeddedJMSResource.
    * <p>
    * The server will normally be started by JUnit using the before() method.  This method allows the server to
    * be started manually to support advanced testing scenarios.
    */
   public void start() {
      log.info("Starting {}: {}", this.getClass().getSimpleName(), this.getServerName());
      try {
         jmsServer.start();
      } catch (Exception ex) {
         throw new RuntimeException(String.format("Exception encountered starting %s: %s", jmsServer.getClass().getSimpleName(), this.getServerName()), ex);
      }
   }

   /**
    * Stop the embedded ActiveMQ broker, blocking until the broker has stopped.
    * <p>
    * The broker will normally be stopped by JUnit using the after() method.  This method allows the broker to
    * be stopped manually to support advanced testing scenarios.
    */
   public void stop() {
      String name = "null";
      if (jmsServer != null) {
         name = this.getServerName();
      }
      log.info("Stopping {}: {}", this.getClass().getSimpleName(), name);
      if (internalClient != null) {
         internalClient.stop();
         internalClient = null;
      }

      if (jmsServer != null) {
         try {
            jmsServer.stop();
         } catch (Exception ex) {
            log.warn(String.format("Exception encountered stopping %s: %s - ignoring", jmsServer.getClass().getSimpleName(), this.getServerName()), ex);
         }
      }
   }

   /**
    * Start the embedded ActiveMQ Broker
    * <p>
    * Invoked by JUnit to setup the resource
    */
   @Override
   protected void before() throws Throwable {
      log.info("Starting {}: {}", this.getClass().getSimpleName(), getServerName());

      this.start();

      super.before();
   }

   /**
    * Stop the embedded ActiveMQ Broker
    * <p>
    * Invoked by JUnit to tear down the resource
    */
   @Override
   protected void after() {
      log.info("Stopping {}: {}", this.getClass().getSimpleName(), getServerName());

      super.after();

      this.stop();
   }

   /**
    * Get the EmbeddedJMS server.
    * <p>
    * This may be required for advanced configuration of the EmbeddedJMS server.
    *
    * @return
    */
   public EmbeddedJMS getJmsServer() {
      return jmsServer;
   }

   /**
    * Get the name of the EmbeddedJMS server
    *
    * @return name of the server
    */
   public String getServerName() {
      String name = "unknown";
      ActiveMQServer activeMQServer = jmsServer.getActiveMQServer();
      if (activeMQServer != null) {
         name = activeMQServer.getConfiguration().getName();
      } else if (configuration != null) {
         name = configuration.getName();
      }

      return name;
   }

   /**
    * Get the VM URL for the embedded EmbeddedActiveMQ server
    *
    * @return the VM URL for the embedded server
    */
   public String getVmURL() {
      String vmURL = "vm://0";
      for (TransportConfiguration transportConfiguration : configuration.getAcceptorConfigurations()) {
         Map<String, Object> params = transportConfiguration.getParams();
         if (params != null && params.containsKey(TransportConstants.SERVER_ID_PROP_NAME)) {
            vmURL = "vm://" + params.get(TransportConstants.SERVER_ID_PROP_NAME);
         }
      }

      return vmURL;
   }

   /**
    * Get the Queue corresponding to a JMS Destination.
    * <p>
    * The full name of the JMS destination including the prefix should be provided - i.e. queue://myQueue
    * or topic://myTopic.  If the destination type prefix is not included in the destination name, a prefix
    * of "queue://" is assumed.
    *
    * @param destinationName the full name of the JMS Destination
    * @return the number of messages in the JMS Destination
    */
   public Queue getDestinationQueue(String destinationName) {
      Queue queue = null;
      ActiveMQDestination destination = ActiveMQDestination.createDestination(destinationName, ActiveMQDestination.TYPE.QUEUE);
      String address = destination.getAddress();
      String name = destination.getName();
      if (destination.isQueue()) {
         queue = jmsServer.getActiveMQServer().locateQueue(destination.getSimpleAddress());
      } else {
         BindingQueryResult bindingQueryResult = null;
         try {
            bindingQueryResult = jmsServer.getActiveMQServer().bindingQuery(destination.getSimpleAddress());
         } catch (Exception ex) {
            log.error(String.format("getDestinationQueue( %s ) - bindingQuery for %s failed", destinationName, destination.getAddress()), ex);
            return null;
         }
         if (bindingQueryResult.isExists()) {
            List<SimpleString> queueNames = bindingQueryResult.getQueueNames();
            if (queueNames.size() > 0) {
               queue = jmsServer.getActiveMQServer().locateQueue(queueNames.get(0));
            }
         }
      }

      return queue;
   }

   /**
    * Get the Queues corresponding to a JMS Topic.
    * <p>
    * The full name of the JMS Topic including the prefix should be provided - i.e. topic://myTopic.  If the destination type prefix
    * is not included in the destination name, a prefix of "topic://" is assumed.
    *
    * @param topicName the full name of the JMS Destination
    * @return the number of messages in the JMS Destination
    */
   public List<Queue> getTopicQueues(String topicName) {
      List<Queue> queues = new LinkedList<>();
      ActiveMQDestination destination = ActiveMQDestination.createDestination(topicName, ActiveMQDestination.TYPE.TOPIC);
      if (!destination.isQueue()) {
         BindingQueryResult bindingQueryResult = null;
         try {
            bindingQueryResult = jmsServer.getActiveMQServer().bindingQuery(destination.getSimpleAddress());
         } catch (Exception ex) {
            log.error(String.format("getTopicQueues( %s ) - bindingQuery for %s failed", topicName, destination.getAddress()), ex);
            return queues;
         }

         if (bindingQueryResult.isExists()) {
            ActiveMQServer activeMQServer = jmsServer.getActiveMQServer();
            for (SimpleString queueName : bindingQueryResult.getQueueNames()) {
               queues.add(activeMQServer.locateQueue(queueName));
            }
         }
      }

      return queues;
   }

   /**
    * Get the number of messages in a specific JMS Destination.
    * <p>
    * The full name of the JMS destination including the prefix should be provided - i.e. queue://myQueue
    * or topic://myTopic.  If the destination type prefix is not included in the destination name, a prefix
    * of "queue://" is assumed.
    *
    * NOTE:  For JMS Topics, this returned count will be the total number of messages for all subscribers.  For
    * example, if there are two subscribers on the topic and a single message is published, the returned count will
    * be two (one message for each subscriber).
    *
    * @param destinationName the full name of the JMS Destination
    * @return the number of messages in the JMS Destination
    */
   public long getMessageCount(String destinationName) {
      long count = 0;
      ActiveMQDestination destination = ActiveMQDestination.createDestination(destinationName, ActiveMQDestination.TYPE.QUEUE);
      if (destination.isQueue()) {
         Queue queue = getDestinationQueue(destinationName);
         if (queue == null) {
            log.warn("getMessageCount(destinationName) - destination {} not found; returning -1", destinationName);
            count = -1;
         } else {
            count = queue.getMessageCount();
         }
      } else {
         for (Queue topicQueue : getTopicQueues(destinationName)) {
            count += topicQueue.getMessageCount();
         }
      }

      return count;
   }

   public BytesMessage createBytesMessage() {
      return getInternalClient().createBytesMessage();
   }

   public TextMessage createTextMessage() {
      return getInternalClient().createTextMessage();
   }

   public MapMessage createMapMessage() {
      return getInternalClient().createMapMessage();
   }

   public ObjectMessage createObjectMessage() {
      return getInternalClient().createObjectMessage();
   }

   public StreamMessage createStreamMessage() {
      return getInternalClient().createStreamMessage();
   }

   public BytesMessage createMessage(byte[] body) {
      return createMessage(body, null);
   }

   public TextMessage createMessage(String body) {
      return createMessage(body, null);
   }

   public MapMessage createMessage(Map<String, Object> body) {
      return createMessage(body, null);
   }

   public ObjectMessage createMessage(Serializable body) {
      return createMessage(body, null);
   }

   public BytesMessage createMessage(byte[] body, Map<String, Object> properties) {
      BytesMessage message = this.createBytesMessage();
      if (body != null) {
         try {
            message.writeBytes(body);
         } catch (JMSException jmsEx) {
            throw new EmbeddedJMSResourceException(String.format("Failed to set body {%s} on BytesMessage", new String(body)), jmsEx);
         }
      }

      setMessageProperties(message, properties);

      return message;
   }

   public TextMessage createMessage(String body, Map<String, Object> properties) {
      TextMessage message = this.createTextMessage();
      if (body != null) {
         try {
            message.setText(body);
         } catch (JMSException jmsEx) {
            throw new EmbeddedJMSResourceException(String.format("Failed to set body {%s} on TextMessage", body), jmsEx);
         }
      }

      setMessageProperties(message, properties);

      return message;
   }

   public MapMessage createMessage(Map<String, Object> body, Map<String, Object> properties) {
      MapMessage message = this.createMapMessage();

      if (body != null) {
         for (Map.Entry<String, Object> entry : body.entrySet()) {
            try {
               message.setObject(entry.getKey(), entry.getValue());
            } catch (JMSException jmsEx) {
               throw new EmbeddedJMSResourceException(String.format("Failed to set body entry {%s = %s} on MapMessage", entry.getKey(), entry.getValue().toString()), jmsEx);
            }
         }
      }

      setMessageProperties(message, properties);

      return message;
   }

   public ObjectMessage createMessage(Serializable body, Map<String, Object> properties) {
      ObjectMessage message = this.createObjectMessage();

      if (body != null) {
         try {
            message.setObject(body);
         } catch (JMSException jmsEx) {
            throw new EmbeddedJMSResourceException(String.format("Failed to set body {%s} on ObjectMessage", body.toString()), jmsEx);
         }
      }

      setMessageProperties(message, properties);

      return message;
   }

   public void pushMessage(String destinationName, Message message) {
      if (destinationName == null) {
         throw new IllegalArgumentException("sendMessage failure - destination name is required");
      } else if (message == null) {
         throw new IllegalArgumentException("sendMessage failure - a Message is required");
      }
      ActiveMQDestination destination = ActiveMQDestination.createDestination(destinationName, ActiveMQDestination.TYPE.QUEUE);

      getInternalClient().pushMessage(destination, message);
   }

   public BytesMessage pushMessage(String destinationName, byte[] body) {
      BytesMessage message = createMessage(body, null);
      pushMessage(destinationName, message);
      return message;
   }

   public TextMessage pushMessage(String destinationName, String body) {
      TextMessage message = createMessage(body, null);
      pushMessage(destinationName, message);
      return message;
   }

   public MapMessage pushMessage(String destinationName, Map<String, Object> body) {
      MapMessage message = createMessage(body, null);
      pushMessage(destinationName, message);
      return message;
   }

   public ObjectMessage pushMessage(String destinationName, Serializable body) {
      ObjectMessage message = createMessage(body, null);
      pushMessage(destinationName, message);
      return message;
   }

   public BytesMessage pushMessageWithProperties(String destinationName, byte[] body, Map<String, Object> properties) {
      BytesMessage message = createMessage(body, properties);
      pushMessage(destinationName, message);
      return message;
   }

   public TextMessage pushMessageWithProperties(String destinationName, String body, Map<String, Object> properties) {
      TextMessage message = createMessage(body, properties);
      pushMessage(destinationName, message);
      return message;
   }

   public MapMessage pushMessageWithProperties(String destinationName,
                                               Map<String, Object> body,
                                               Map<String, Object> properties) {
      MapMessage message = createMessage(body, properties);
      pushMessage(destinationName, message);
      return message;
   }

   public ObjectMessage pushMessageWithProperties(String destinationName,
                                                  Serializable body,
                                                  Map<String, Object> properties) {
      ObjectMessage message = createMessage(body, properties);
      pushMessage(destinationName, message);
      return message;
   }

   public Message peekMessage(String destinationName) {
      if (null == jmsServer) {
         throw new NullPointerException("peekMessage failure  - BrokerService is null");
      }

      if (destinationName == null) {
         throw new IllegalArgumentException("peekMessage failure - destination name is required");
      }

      throw new UnsupportedOperationException("Not yet implemented");
   }

   public BytesMessage peekBytesMessage(String destinationName) {
      return (BytesMessage) peekMessage(destinationName);
   }

   public TextMessage peekTextMessage(String destinationName) {
      return (TextMessage) peekMessage(destinationName);
   }

   public MapMessage peekMapMessage(String destinationName) {
      return (MapMessage) peekMessage(destinationName);
   }

   public ObjectMessage peekObjectMessage(String destinationName) {
      return (ObjectMessage) peekMessage(destinationName);
   }

   public StreamMessage peekStreamMessage(String destinationName) {
      return (StreamMessage) peekMessage(destinationName);
   }

   private InternalClient getInternalClient() {
      if (internalClient == null) {
         log.info("Creating InternalClient");
         internalClient = new InternalClient();
         internalClient.start();
      }

      return internalClient;
   }

   public static class EmbeddedJMSResourceException extends RuntimeException {

      public EmbeddedJMSResourceException(String message) {
         super(message);
      }

      public EmbeddedJMSResourceException(String message, Exception cause) {
         super(message, cause);
      }
   }

   private class InternalClient {

      ConnectionFactory connectionFactory;
      Connection connection;
      Session session;
      MessageProducer producer;

      InternalClient() {
      }

      void start() {
         connectionFactory = new ActiveMQConnectionFactory(getVmURL());
         try {
            connection = connectionFactory.createConnection();
            session = connection.createSession();
            producer = session.createProducer(null);
            connection.start();
         } catch (JMSException jmsEx) {
            throw new EmbeddedJMSResourceException("InternalClient creation failure", jmsEx);
         }
      }

      void stop() {
         try {
            producer.close();
         } catch (JMSException jmsEx) {
            log.warn("JMSException encounter closing InternalClient Session - MessageProducer", jmsEx);
         } finally {
            producer = null;
         }

         try {
            session.close();
         } catch (JMSException jmsEx) {
            log.warn("JMSException encounter closing InternalClient Session - ignoring", jmsEx);
         } finally {
            session = null;
         }

         if (null != connection) {
            try {
               connection.close();
            } catch (JMSException jmsEx) {
               log.warn("JMSException encounter closing InternalClient Connection - ignoring", jmsEx);
            } finally {
               connection = null;
            }
         }
      }

      public BytesMessage createBytesMessage() {
         checkSession();

         try {
            return session.createBytesMessage();
         } catch (JMSException jmsEx) {
            throw new EmbeddedJMSResourceException("Failed to create BytesMessage", jmsEx);
         }
      }

      public TextMessage createTextMessage() {
         checkSession();

         try {
            return session.createTextMessage();
         } catch (JMSException jmsEx) {
            throw new EmbeddedJMSResourceException("Failed to create TextMessage", jmsEx);
         }
      }

      public MapMessage createMapMessage() {
         checkSession();

         try {
            return session.createMapMessage();
         } catch (JMSException jmsEx) {
            throw new EmbeddedJMSResourceException("Failed to create MapMessage", jmsEx);
         }
      }

      public ObjectMessage createObjectMessage() {
         checkSession();

         try {
            return session.createObjectMessage();
         } catch (JMSException jmsEx) {
            throw new EmbeddedJMSResourceException("Failed to create ObjectMessage", jmsEx);
         }
      }

      public StreamMessage createStreamMessage() {
         checkSession();
         try {
            return session.createStreamMessage();
         } catch (JMSException jmsEx) {
            throw new EmbeddedJMSResourceException("Failed to create StreamMessage", jmsEx);
         }
      }

      public void pushMessage(ActiveMQDestination destination, Message message) {
         if (producer == null) {
            throw new IllegalStateException("JMS MessageProducer is null - has the InternalClient been started?");
         }

         try {
            producer.send(destination, message);
         } catch (JMSException jmsEx) {
            throw new EmbeddedJMSResourceException(String.format("Failed to push %s to %s", message.getClass().getSimpleName(), destination.toString()), jmsEx);
         }
      }

      void checkSession() {
         if (session == null) {
            throw new IllegalStateException("JMS Session is null - has the InternalClient been started?");
         }
      }
   }
}
