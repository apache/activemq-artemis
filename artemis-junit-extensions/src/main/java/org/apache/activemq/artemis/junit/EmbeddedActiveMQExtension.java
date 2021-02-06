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


import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

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
public class EmbeddedActiveMQExtension implements BeforeAllCallback, AfterAllCallback {

   Logger log = LoggerFactory.getLogger(this.getClass());

   private EmbeddedActiveMQDelegate embeddedActiveMQDelegate;

   /**
    * Create a default EmbeddedActiveMQResource
    */
   public EmbeddedActiveMQExtension() {
      this.embeddedActiveMQDelegate = new EmbeddedActiveMQDelegate();
   }

   /**
    * Create a default EmbeddedActiveMQResource with the specified serverId
    *
    * @param serverId server id
    */
   public EmbeddedActiveMQExtension(int serverId) {
      this.embeddedActiveMQDelegate = new EmbeddedActiveMQDelegate(serverId);
   }

   /**
    * Creates an EmbeddedActiveMQResource using the specified configuration
    *
    * @param configuration ActiveMQServer configuration
    */
   public EmbeddedActiveMQExtension(Configuration configuration) {
      this.embeddedActiveMQDelegate = new EmbeddedActiveMQDelegate(configuration);
   }

   /**
    * Creates an EmbeddedActiveMQResource using the specified configuration file
    *
    * @param filename ActiveMQServer configuration file name
    */
   public EmbeddedActiveMQExtension(String filename) {
      this.embeddedActiveMQDelegate = new EmbeddedActiveMQDelegate(filename);
   }


   /**
    * Invoked by JUnit to setup the resource - start the embedded ActiveMQ Artemis server
    */
   @Override
   public void beforeAll(ExtensionContext context) throws Exception {
      log.info("Starting {}: {}", this.getClass().getSimpleName(), embeddedActiveMQDelegate.getServerName());

      embeddedActiveMQDelegate.start();

   }

   /**
    * Invoked by JUnit to tear down the resource - stops the embedded ActiveMQ Artemis server
    */
   @Override
   public void afterAll(ExtensionContext context) throws Exception {
      log.info("Stopping {}: {}", this.getClass().getSimpleName(), embeddedActiveMQDelegate.getServerName());

      embeddedActiveMQDelegate.stop();

   }

   public static void addMessageProperties(ClientMessage message, Map<String, Object> properties) {
      EmbeddedActiveMQDelegate.addMessageProperties(message, properties);
   }

   public boolean isUseDurableMessage() {
      return embeddedActiveMQDelegate.isUseDurableMessage();
   }

   public void setUseDurableMessage(boolean useDurableMessage) {
      embeddedActiveMQDelegate.setUseDurableMessage(useDurableMessage);
   }

   public boolean isUseDurableQueue() {
      return embeddedActiveMQDelegate.isUseDurableQueue();
   }

   public void setUseDurableQueue(boolean useDurableQueue) {
      embeddedActiveMQDelegate.setUseDurableQueue(useDurableQueue);
   }

   public long getDefaultReceiveTimeout() {
      return embeddedActiveMQDelegate.getDefaultReceiveTimeout();
   }

   public void setDefaultReceiveTimeout(long defaultReceiveTimeout) {
      embeddedActiveMQDelegate.setDefaultReceiveTimeout(defaultReceiveTimeout);
   }

   public EmbeddedActiveMQ getServer() {
      return embeddedActiveMQDelegate.getServer();
   }

   public String getServerName() {
      return embeddedActiveMQDelegate.getServerName();
   }

   public String getVmURL() {
      return embeddedActiveMQDelegate.getVmURL();
   }

   public long getMessageCount(String queueName) {
      return embeddedActiveMQDelegate.getMessageCount(queueName);
   }

   public long getMessageCount(SimpleString queueName) {
      return embeddedActiveMQDelegate.getMessageCount(queueName);
   }

   public Queue locateQueue(String queueName) {
      return embeddedActiveMQDelegate.locateQueue(queueName);
   }

   public Queue locateQueue(SimpleString queueName) {
      return embeddedActiveMQDelegate.locateQueue(queueName);
   }

   public List<Queue> getBoundQueues(String address) {
      return embeddedActiveMQDelegate.getBoundQueues(address);
   }

   public List<Queue> getBoundQueues(SimpleString address) {
      return embeddedActiveMQDelegate.getBoundQueues(address);
   }

   public Queue createQueue(String name) {
      return embeddedActiveMQDelegate.createQueue(name);
   }

   public Queue createQueue(String address, String name) {
      return embeddedActiveMQDelegate.createQueue(address, name);
   }

   public Queue createQueue(SimpleString address, SimpleString name) {
      return embeddedActiveMQDelegate.createQueue(address, name);
   }

   public void createSharedQueue(String name, String user) {
      embeddedActiveMQDelegate.createSharedQueue(name, user);
   }

   public void createSharedQueue(String address, String name, String user) {
      embeddedActiveMQDelegate.createSharedQueue(address, name, user);
   }

   public void createSharedQueue(SimpleString address, SimpleString name, SimpleString user) {
      embeddedActiveMQDelegate.createSharedQueue(address, name, user);
   }

   public ClientMessage createMessage() {
      return embeddedActiveMQDelegate.createMessage();
   }

   public ClientMessage createMessage(byte[] body) {
      return embeddedActiveMQDelegate.createMessage(body);
   }

   public ClientMessage createMessage(String body) {
      return embeddedActiveMQDelegate.createMessage(body);
   }

   public ClientMessage createMessageWithProperties(Map<String, Object> properties) {
      return embeddedActiveMQDelegate.createMessageWithProperties(properties);
   }

   public ClientMessage createMessageWithProperties(byte[] body, Map<String, Object> properties) {
      return embeddedActiveMQDelegate.createMessageWithProperties(body, properties);
   }

   public ClientMessage createMessageWithProperties(String body, Map<String, Object> properties) {
      return embeddedActiveMQDelegate.createMessageWithProperties(body, properties);
   }

   public void sendMessage(String address, ClientMessage message) {
      embeddedActiveMQDelegate.sendMessage(address, message);
   }

   public ClientMessage sendMessage(String address, byte[] body) {
      return embeddedActiveMQDelegate.sendMessage(address, body);
   }

   public ClientMessage sendMessage(String address, String body) {
      return embeddedActiveMQDelegate.sendMessage(address, body);
   }

   public ClientMessage sendMessageWithProperties(String address, Map<String, Object> properties) {
      return embeddedActiveMQDelegate.sendMessageWithProperties(address, properties);
   }

   public ClientMessage sendMessageWithProperties(String address, byte[] body, Map<String, Object> properties) {
      return embeddedActiveMQDelegate.sendMessageWithProperties(address, body, properties);
   }

   public ClientMessage sendMessageWithProperties(String address, String body, Map<String, Object> properties) {
      return embeddedActiveMQDelegate.sendMessageWithProperties(address, body, properties);
   }

   public void sendMessage(SimpleString address, ClientMessage message) {
      embeddedActiveMQDelegate.sendMessage(address, message);
   }

   public ClientMessage sendMessage(SimpleString address, byte[] body) {
      return embeddedActiveMQDelegate.sendMessage(address, body);
   }

   public ClientMessage sendMessage(SimpleString address, String body) {
      return embeddedActiveMQDelegate.sendMessage(address, body);
   }

   public ClientMessage sendMessageWithProperties(SimpleString address, Map<String, Object> properties) {
      return embeddedActiveMQDelegate.sendMessageWithProperties(address, properties);
   }

   public ClientMessage sendMessageWithProperties(SimpleString address, byte[] body, Map<String, Object> properties) {
      return embeddedActiveMQDelegate.sendMessageWithProperties(address, body, properties);
   }

   public ClientMessage sendMessageWithProperties(SimpleString address, String body, Map<String, Object> properties) {
      return embeddedActiveMQDelegate.sendMessageWithProperties(address, body, properties);
   }

   public ClientMessage receiveMessage(String queueName) {
      return embeddedActiveMQDelegate.receiveMessage(queueName);
   }

   public ClientMessage receiveMessage(String queueName, long timeout) {
      return embeddedActiveMQDelegate.receiveMessage(queueName, timeout);
   }

   public ClientMessage receiveMessage(SimpleString queueName) {
      return embeddedActiveMQDelegate.receiveMessage(queueName);
   }

   public ClientMessage receiveMessage(SimpleString queueName, long timeout) {
      return embeddedActiveMQDelegate.receiveMessage(queueName, timeout);
   }

   public ClientMessage browseMessage(String queueName) {
      return embeddedActiveMQDelegate.browseMessage(queueName);
   }

   public ClientMessage browseMessage(String queueName, long timeout) {
      return embeddedActiveMQDelegate.browseMessage(queueName, timeout);
   }

   public ClientMessage browseMessage(SimpleString queueName) {
      return embeddedActiveMQDelegate.browseMessage(queueName);
   }

   public ClientMessage browseMessage(SimpleString queueName, long timeout) {
      return embeddedActiveMQDelegate.browseMessage(queueName, timeout);
   }
}
