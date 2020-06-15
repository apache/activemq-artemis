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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.FileDeploymentManager;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.config.impl.FileConfiguration;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMAcceptorFactory;
import org.apache.activemq.artemis.core.remoting.impl.invm.TransportConstants;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.BindingQueryResult;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A JUnit Rule that embeds an ActiveMQ Artemis server into a test.
 *
 * This JUnit Rule is designed to simplify using embedded servers in unit tests.  Adding the rule to a test will startup
 * an embedded server, which can then be used by client applications.
 *
 * <pre><code>
 * public class SimpleTest {
 *     &#64;Rule
 *     public EmbeddedActiveMQResource server = new EmbeddedActiveMQResource();
 *
 *     &#64;Test
 *     public void testSomething() throws Exception {
 *         // Use the embedded server here
 *     }
 * }
 * </code></pre>
 */
public class EmbeddedActiveMQResource extends ExternalResource {

   static final String SERVER_NAME = "embedded-server";

   Logger log = LoggerFactory.getLogger(this.getClass());

   boolean useDurableMessage = true;
   boolean useDurableQueue = true;
   long defaultReceiveTimeout = 50;

   Configuration configuration;

   EmbeddedActiveMQ server;

   InternalClient internalClient;

   /**
    * Create a default EmbeddedActiveMQResource
    */
   public EmbeddedActiveMQResource() {
      configuration = new ConfigurationImpl().setName(SERVER_NAME).setPersistenceEnabled(false).setSecurityEnabled(false).addAcceptorConfiguration(new TransportConfiguration(InVMAcceptorFactory.class.getName())).addAddressesSetting("#", new AddressSettings().setDeadLetterAddress(SimpleString.toSimpleString("dla")).setExpiryAddress(SimpleString.toSimpleString("expiry")));
      init();
   }

   /**
    * Create a default EmbeddedActiveMQResource with the specified serverId
    *
    * @param serverId server id
    */
   public EmbeddedActiveMQResource(int serverId) {
      Map<String, Object> params = new HashMap<>();
      params.put(TransportConstants.SERVER_ID_PROP_NAME, serverId);
      TransportConfiguration transportConfiguration = new TransportConfiguration(InVMAcceptorFactory.class.getName(), params);
      configuration = new ConfigurationImpl().setName(SERVER_NAME + "-" + serverId).setPersistenceEnabled(false).setSecurityEnabled(false).addAcceptorConfiguration(transportConfiguration);
      init();
   }

   /**
    * Creates an EmbeddedActiveMQResource using the specified configuration
    *
    * @param configuration ActiveMQServer configuration
    */
   public EmbeddedActiveMQResource(Configuration configuration) {
      this.configuration = configuration;
      init();
   }

   /**
    * Creates an EmbeddedActiveMQResource using the specified configuration file
    *
    * @param filename ActiveMQServer configuration file name
    */
   public EmbeddedActiveMQResource(String filename) {
      if (filename == null) {
         throw new IllegalArgumentException("ActiveMQServer configuration file name cannot be null");
      }
      FileDeploymentManager deploymentManager = new FileDeploymentManager(filename);
      FileConfiguration config = new FileConfiguration();
      deploymentManager.addDeployable(config);
      try {
         deploymentManager.readConfiguration();
      } catch (Exception ex) {
         throw new EmbeddedActiveMQResourceException(String.format("Failed to read configuration file %s", filename), ex);
      }
      this.configuration = config;
      init();
   }

   /**
    * Adds properties to a ClientMessage
    *
    * @param message
    * @param properties
    */
   public static void addMessageProperties(ClientMessage message, Map<String, Object> properties) {
      if (properties != null && properties.size() > 0) {
         for (Map.Entry<String, Object> property : properties.entrySet()) {
            message.putObjectProperty(property.getKey(), property.getValue());
         }
      }
   }

   private void init() {
      if (server == null) {
         server = new EmbeddedActiveMQ().setConfiguration(configuration);
      }
   }

   /**
    * Start the embedded ActiveMQ Artemis server.
    *
    * The server will normally be started by JUnit using the before() method.  This method allows the server to
    * be started manually to support advanced testing scenarios.
    */
   public void start() {
      try {
         server.start();
      } catch (Exception ex) {
         throw new RuntimeException(String.format("Exception encountered starting %s: %s", server.getClass().getName(), this.getServerName()), ex);
      }

      configuration = server.getActiveMQServer().getConfiguration();
   }

   /**
    * Stop the embedded ActiveMQ Artemis server
    *
    * The server will normally be stopped by JUnit using the after() method.  This method allows the server to
    * be stopped manually to support advanced testing scenarios.
    */
   public void stop() {
      if (internalClient != null) {
         internalClient.stop();
         internalClient = null;
      }

      if (server != null) {
         try {
            server.stop();
         } catch (Exception ex) {
            log.warn(String.format("Exception encountered stopping %s: %s", server.getClass().getSimpleName(), this.getServerName()), ex);
         }
      }
   }

   /**
    * Invoked by JUnit to setup the resource - start the embedded ActiveMQ Artemis server
    */
   @Override
   protected void before() throws Throwable {
      log.info("Starting {}: {}", this.getClass().getSimpleName(), getServerName());

      this.start();

      super.before();
   }

   /**
    * Invoked by JUnit to tear down the resource - stops the embedded ActiveMQ Artemis server
    */
   @Override
   protected void after() {
      log.info("Stopping {}: {}", this.getClass().getSimpleName(), getServerName());

      this.stop();

      super.after();
   }

   public boolean isUseDurableMessage() {
      return useDurableMessage;
   }

   /**
    * Disables/Enables creating durable messages.  By default, durable messages are created
    *
    * @param useDurableMessage if true, durable messages will be created
    */
   public void setUseDurableMessage(boolean useDurableMessage) {
      this.useDurableMessage = useDurableMessage;
   }

   public boolean isUseDurableQueue() {
      return useDurableQueue;
   }

   /**
    * Disables/Enables creating durable queues.  By default, durable queues are created
    *
    * @param useDurableQueue if true, durable messages will be created
    */
   public void setUseDurableQueue(boolean useDurableQueue) {
      this.useDurableQueue = useDurableQueue;
   }

   public long getDefaultReceiveTimeout() {
      return defaultReceiveTimeout;
   }

   /**
    * Sets the default timeout in milliseconds used when receiving messages.  Defaults to 50 milliseconds
    *
    * @param defaultReceiveTimeout received timeout in milliseconds
    */
   public void setDefaultReceiveTimeout(long defaultReceiveTimeout) {
      this.defaultReceiveTimeout = defaultReceiveTimeout;
   }

   /**
    * Get the EmbeddedActiveMQ server.
    *
    * This may be required for advanced configuration of the EmbeddedActiveMQ server.
    *
    * @return the embedded ActiveMQ broker
    */
   public EmbeddedActiveMQ getServer() {
      return server;
   }

   /**
    * Get the name of the EmbeddedActiveMQ server
    *
    * @return name of the embedded server
    */
   public String getServerName() {
      String name = "unknown";
      ActiveMQServer activeMQServer = server.getActiveMQServer();
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

   public long getMessageCount(String queueName) {
      return getMessageCount(SimpleString.toSimpleString(queueName));
   }

   /**
    * Get the number of messages in a specific queue.
    *
    * @param queueName the name of the queue
    * @return the number of messages in the queue; -1 if queue is not found
    */
   public long getMessageCount(SimpleString queueName) {
      Queue queue = locateQueue(queueName);
      if (queue == null) {
         log.warn("getMessageCount(queueName) - queue {} not found; returning -1", queueName.toString());
         return -1;
      }

      return queue.getMessageCount();
   }

   public Queue locateQueue(String queueName) {
      return locateQueue(SimpleString.toSimpleString(queueName));
   }

   public Queue locateQueue(SimpleString queueName) {
      return server.getActiveMQServer().locateQueue(queueName);
   }

   public List<Queue> getBoundQueues(String address) {
      return getBoundQueues(SimpleString.toSimpleString(address));
   }

   public List<Queue> getBoundQueues(SimpleString address) {
      if (address == null) {
         throw new IllegalArgumentException("getBoundQueues( address ) - address cannot be null");
      }
      List<Queue> boundQueues = new java.util.LinkedList<>();

      BindingQueryResult bindingQueryResult = null;
      try {
         bindingQueryResult = server.getActiveMQServer().bindingQuery(address);
      } catch (Exception e) {
         throw new EmbeddedActiveMQResourceException(String.format("getBoundQueues( %s ) - bindingQuery( %s ) failed", address.toString(), address.toString()));
      }
      if (bindingQueryResult.isExists()) {
         for (SimpleString queueName : bindingQueryResult.getQueueNames()) {
            boundQueues.add(server.getActiveMQServer().locateQueue(queueName));
         }
      }
      return boundQueues;
   }

   public Queue createQueue(String name) {
      return createQueue(SimpleString.toSimpleString(name), SimpleString.toSimpleString(name));
   }

   public Queue createQueue(String address, String name) {
      return createQueue(SimpleString.toSimpleString(address), SimpleString.toSimpleString(name));
   }

   public Queue createQueue(SimpleString address, SimpleString name) {
      Queue queue = null;
      try {
         queue = server.getActiveMQServer().createQueue(new QueueConfiguration(name).setAddress(address).setDurable(isUseDurableQueue()));
      } catch (Exception ex) {
         throw new EmbeddedActiveMQResourceException(String.format("Failed to create queue: queueName = %s, name = %s", address.toString(), name.toString()), ex);
      }

      return queue;
   }

   public void createSharedQueue(String name, String user) {
      createSharedQueue(SimpleString.toSimpleString(name), SimpleString.toSimpleString(name), SimpleString.toSimpleString(user));
   }

   public void createSharedQueue(String address, String name, String user) {
      createSharedQueue(SimpleString.toSimpleString(address), SimpleString.toSimpleString(name), SimpleString.toSimpleString(user));
   }

   public void createSharedQueue(SimpleString address, SimpleString name, SimpleString user) {
      try {
         server.getActiveMQServer().createSharedQueue(new QueueConfiguration(name).setAddress(address).setRoutingType(RoutingType.MULTICAST).setDurable(isUseDurableQueue()).setUser(user));
      } catch (Exception ex) {
         throw new EmbeddedActiveMQResourceException(String.format("Failed to create shared queue: queueName = %s, name = %s, user = %s", address.toString(), name.toString(), user.toString()), ex);
      }
   }

   /**
    * Create a ClientMessage
    *
    * If useDurableMessage is false, a non-durable message is created.  Otherwise, a durable message is created
    *
    * @return a new ClientMessage
    */
   public ClientMessage createMessage() {
      getInternalClient();
      return internalClient.createMessage(isUseDurableMessage());
   }

   /**
    * Create a ClientMessage with the specified body
    *
    * If useDurableMessage is false, a non-durable message is created.  Otherwise, a durable message is created
    *
    * @param body the body for the new message
    * @return a new ClientMessage with the specified body
    */
   public ClientMessage createMessage(byte[] body) {
      getInternalClient();
      ClientMessage message = internalClient.createMessage(isUseDurableMessage());

      if (body != null) {
         message.writeBodyBufferBytes(body);
      }

      return message;
   }

   /**
    * Create a ClientMessage with the specified body
    *
    * If useDurableMessage is false, a non-durable message is created.  Otherwise, a durable message is created
    *
    * @param body the body for the new message
    * @return a new ClientMessage with the specified body
    */
   public ClientMessage createMessage(String body) {
      getInternalClient();
      ClientMessage message = internalClient.createMessage(isUseDurableMessage());

      if (body != null) {
         message.writeBodyBufferString(body);
      }

      return message;
   }

   /**
    * Create a ClientMessage with the specified message properties
    *
    * If useDurableMessage is false, a non-durable message is created.  Otherwise, a durable message is created
    *
    * @param properties message properties for the new message
    * @return a new ClientMessage with the specified message properties
    */
   public ClientMessage createMessageWithProperties(Map<String, Object> properties) {
      getInternalClient();
      ClientMessage message = internalClient.createMessage(isUseDurableMessage());

      addMessageProperties(message, properties);

      return message;
   }

   /**
    * Create a ClientMessage with the specified body and message properties
    *
    * If useDurableMessage is false, a non-durable message is created.  Otherwise, a durable message is created
    *
    * @param body       the body for the new message
    * @param properties message properties for the new message
    * @return a new ClientMessage with the specified body and message properties
    */
   public ClientMessage createMessageWithProperties(byte[] body, Map<String, Object> properties) {
      ClientMessage message = createMessage(body);

      addMessageProperties(message, properties);

      return message;
   }

   /**
    * Create a ClientMessage with the specified body and message properties
    *
    * If useDurableMessage is false, a non-durable message is created.  Otherwise, a durable message is created
    *
    * @param body       the body for the new message
    * @param properties message properties for the new message
    * @return a new ClientMessage with the specified body and message properties
    */
   public ClientMessage createMessageWithProperties(String body, Map<String, Object> properties) {
      ClientMessage message = createMessage(body);

      addMessageProperties(message, properties);

      return message;
   }

   /**
    * Send a message to an address
    *
    * @param address the target queueName for the message
    * @param message the message to send
    */
   public void sendMessage(String address, ClientMessage message) {
      sendMessage(SimpleString.toSimpleString(address), message);
   }

   /**
    * Create a new message with the specified body, and send the message to an address
    *
    * @param address the target queueName for the message
    * @param body    the body for the new message
    * @return the message that was sent
    */
   public ClientMessage sendMessage(String address, byte[] body) {
      return sendMessage(SimpleString.toSimpleString(address), body);
   }

   /**
    * Create a new message with the specified body, and send the message to an address
    *
    * @param address the target queueName for the message
    * @param body    the body for the new message
    * @return the message that was sent
    */
   public ClientMessage sendMessage(String address, String body) {
      return sendMessage(SimpleString.toSimpleString(address), body);
   }

   /**
    * Create a new message with the specified properties, and send the message to an address
    *
    * @param address    the target queueName for the message
    * @param properties message properties for the new message
    * @return the message that was sent
    */
   public ClientMessage sendMessageWithProperties(String address, Map<String, Object> properties) {
      return sendMessageWithProperties(SimpleString.toSimpleString(address), properties);
   }

   /**
    * Create a new message with the specified body and properties, and send the message to an address
    *
    * @param address    the target queueName for the message
    * @param body       the body for the new message
    * @param properties message properties for the new message
    * @return the message that was sent
    */
   public ClientMessage sendMessageWithProperties(String address, byte[] body, Map<String, Object> properties) {
      return sendMessageWithProperties(SimpleString.toSimpleString(address), body, properties);
   }

   /**
    * Create a new message with the specified body and properties, and send the message to an address
    *
    * @param address    the target queueName for the message
    * @param body       the body for the new message
    * @param properties message properties for the new message
    * @return the message that was sent
    */
   public ClientMessage sendMessageWithProperties(String address, String body, Map<String, Object> properties) {
      return sendMessageWithProperties(SimpleString.toSimpleString(address), body, properties);
   }

   /**
    * Send a message to an queueName
    *
    * @param address the target queueName for the message
    * @param message the message to send
    */
   public void sendMessage(SimpleString address, ClientMessage message) {
      if (address == null) {
         throw new IllegalArgumentException("sendMessage failure - queueName is required");
      } else if (message == null) {
         throw new IllegalArgumentException("sendMessage failure - a ClientMessage is required");
      }

      getInternalClient();
      internalClient.sendMessage(address, message);
   }

   /**
    * Create a new message with the specified body, and send the message to an queueName
    *
    * @param address the target queueName for the message
    * @param body    the body for the new message
    * @return the message that was sent
    */
   public ClientMessage sendMessage(SimpleString address, byte[] body) {
      ClientMessage message = createMessage(body);
      sendMessage(address, message);
      return message;
   }

   /**
    * Create a new message with the specified body, and send the message to an queueName
    *
    * @param address the target queueName for the message
    * @param body    the body for the new message
    * @return the message that was sent
    */
   public ClientMessage sendMessage(SimpleString address, String body) {
      ClientMessage message = createMessage(body);
      sendMessage(address, message);
      return message;
   }

   /**
    * Create a new message with the specified properties, and send the message to an queueName
    *
    * @param address    the target queueName for the message
    * @param properties message properties for the new message
    * @return the message that was sent
    */
   public ClientMessage sendMessageWithProperties(SimpleString address, Map<String, Object> properties) {
      ClientMessage message = createMessageWithProperties(properties);
      sendMessage(address, message);
      return message;
   }

   /**
    * Create a new message with the specified body and properties, and send the message to an queueName
    *
    * @param address    the target queueName for the message
    * @param body       the body for the new message
    * @param properties message properties for the new message
    * @return the message that was sent
    */
   public ClientMessage sendMessageWithProperties(SimpleString address, byte[] body, Map<String, Object> properties) {
      ClientMessage message = createMessageWithProperties(body, properties);
      sendMessage(address, message);
      return message;
   }

   /**
    * Create a new message with the specified body and properties, and send the message to an queueName
    *
    * @param address    the target queueName for the message
    * @param body       the body for the new message
    * @param properties message properties for the new message
    * @return the message that was sent
    */
   public ClientMessage sendMessageWithProperties(SimpleString address, String body, Map<String, Object> properties) {

      ClientMessage message = createMessageWithProperties(body, properties);
      sendMessage(address, message);
      return message;
   }

   /**
    * Receive a message from the specified queue using the default receive timeout
    *
    * @param queueName name of the source queue
    * @return the received ClientMessage, null if the receive timed-out
    */
   public ClientMessage receiveMessage(String queueName) {
      return receiveMessage(SimpleString.toSimpleString(queueName));
   }

   /**
    * Receive a message from the specified queue using the specified receive timeout
    *
    * @param queueName name of the source queue
    * @param timeout   receive timeout in milliseconds
    * @return the received ClientMessage, null if the receive timed-out
    */
   public ClientMessage receiveMessage(String queueName, long timeout) {
      return receiveMessage(SimpleString.toSimpleString(queueName), timeout);
   }

   /**
    * Receive a message from the specified queue using the default receive timeout
    *
    * @param queueName name of the source queue
    * @return the received ClientMessage, null if the receive timed-out
    */
   public ClientMessage receiveMessage(SimpleString queueName) {
      final boolean browseOnly = false;
      return getInternalClient().receiveMessage(queueName, defaultReceiveTimeout, browseOnly);
   }

   /**
    * Receive a message from the specified queue using the specified receive timeout
    *
    * @param queueName name of the source queue
    * @param timeout   receive timeout in milliseconds
    * @return the received ClientMessage, null if the receive timed-out
    */
   public ClientMessage receiveMessage(SimpleString queueName, long timeout) {
      final boolean browseOnly = false;
      return getInternalClient().receiveMessage(queueName, timeout, browseOnly);
   }

   /**
    * Browse a message (receive but do not consume) from the specified queue using the default receive timeout
    *
    * @param queueName name of the source queue
    * @return the received ClientMessage, null if the receive timed-out
    */
   public ClientMessage browseMessage(String queueName) {
      return browseMessage(SimpleString.toSimpleString(queueName), defaultReceiveTimeout);
   }

   /**
    * Browse a message (receive but do not consume) a message from the specified queue using the specified receive timeout
    *
    * @param queueName name of the source queue
    * @param timeout   receive timeout in milliseconds
    * @return the received ClientMessage, null if the receive timed-out
    */
   public ClientMessage browseMessage(String queueName, long timeout) {
      return browseMessage(SimpleString.toSimpleString(queueName), timeout);
   }

   /**
    * Browse a message (receive but do not consume) from the specified queue using the default receive timeout
    *
    * @param queueName name of the source queue
    * @return the received ClientMessage, null if the receive timed-out
    */
   public ClientMessage browseMessage(SimpleString queueName) {
      final boolean browseOnly = true;
      return getInternalClient().receiveMessage(queueName, defaultReceiveTimeout, browseOnly);
   }

   /**
    * Browse a message (receive but do not consume) a message from the specified queue using the specified receive timeout
    *
    * @param queueName name of the source queue
    * @param timeout   receive timeout in milliseconds
    * @return the received ClientMessage, null if the receive timed-out
    */
   public ClientMessage browseMessage(SimpleString queueName, long timeout) {
      final boolean browseOnly = true;
      return getInternalClient().receiveMessage(queueName, timeout, browseOnly);
   }

   private InternalClient getInternalClient() {
      if (internalClient == null) {
         log.info("Creating Internal Client");
         internalClient = new InternalClient();
         internalClient.start();
      }

      return internalClient;
   }

   public static class EmbeddedActiveMQResourceException extends RuntimeException {

      public EmbeddedActiveMQResourceException(String message) {
         super(message);
      }

      public EmbeddedActiveMQResourceException(String message, Exception cause) {
         super(message, cause);
      }
   }

   private class InternalClient {

      ServerLocator serverLocator;
      ClientSessionFactory sessionFactory;
      ClientSession session;
      ClientProducer producer;

      InternalClient() {
      }

      void start() {
         log.info("Starting {}", this.getClass().getSimpleName());
         try {
            serverLocator = ActiveMQClient.createServerLocator(getVmURL());
            sessionFactory = serverLocator.createSessionFactory();
         } catch (RuntimeException runtimeEx) {
            throw runtimeEx;
         } catch (Exception ex) {
            throw new EmbeddedActiveMQResourceException("Internal Client creation failure", ex);
         }

         try {
            session = sessionFactory.createSession();
            producer = session.createProducer((String) null);
            session.start();
         } catch (ActiveMQException amqEx) {
            throw new EmbeddedActiveMQResourceException("Internal Client creation failure", amqEx);
         }
      }

      void stop() {
         if (producer != null) {
            try {
               producer.close();
            } catch (ActiveMQException amqEx) {
               log.warn("ActiveMQException encountered closing InternalClient ClientProducer - ignoring", amqEx);
            } finally {
               producer = null;
            }
         }
         if (session != null) {
            try {
               session.close();
            } catch (ActiveMQException amqEx) {
               log.warn("ActiveMQException encountered closing InternalClient ClientSession - ignoring", amqEx);
            } finally {
               session = null;
            }
         }
         if (sessionFactory != null) {
            sessionFactory.close();
            sessionFactory = null;
         }
         if (serverLocator != null) {
            serverLocator.close();
            serverLocator = null;
         }

      }

      public ClientMessage createMessage(boolean durable) {
         checkSession();

         return session.createMessage(durable);
      }

      public void sendMessage(SimpleString address, ClientMessage message) {
         checkSession();
         if (producer == null) {
            throw new IllegalStateException("ClientProducer is null - has the InternalClient been started?");
         }

         try {
            producer.send(address, message);
         } catch (ActiveMQException amqEx) {
            throw new EmbeddedActiveMQResourceException(String.format("Failed to send message to %s", address.toString()), amqEx);
         }
      }

      public ClientMessage receiveMessage(SimpleString address, long timeout, boolean browseOnly) {
         checkSession();

         ClientConsumer consumer = null;
         try {
            consumer = session.createConsumer(address, browseOnly);
         } catch (ActiveMQException amqEx) {
            throw new EmbeddedActiveMQResourceException(String.format("Failed to create consumer for %s", address.toString()), amqEx);
         }

         ClientMessage message = null;
         if (timeout > 0) {
            try {
               message = consumer.receive(timeout);
            } catch (ActiveMQException amqEx) {
               throw new EmbeddedActiveMQResourceException(String.format("ClientConsumer.receive( timeout = %d ) for %s failed", timeout, address.toString()), amqEx);
            }
         } else if (timeout == 0) {
            try {
               message = consumer.receiveImmediate();
            } catch (ActiveMQException amqEx) {
               throw new EmbeddedActiveMQResourceException(String.format("ClientConsumer.receiveImmediate() for %s failed", address.toString()), amqEx);
            }
         } else {
            try {
               message = consumer.receive();
            } catch (ActiveMQException amqEx) {
               throw new EmbeddedActiveMQResourceException(String.format("ClientConsumer.receive() for %s failed", address.toString()), amqEx);
            }
         }

         return message;
      }

      void checkSession() {
         getInternalClient();
         if (session == null) {
            throw new IllegalStateException("ClientSession is null - has the InternalClient been started?");
         }
      }
   }
}
