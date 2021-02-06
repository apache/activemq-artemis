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
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Map;

/**
 * A JUnit Rule that embeds an ActiveMQ Artemis ClientProducer bound to a specific address into a test.
 *
 * This JUnit Rule is designed to simplify using ActiveMQ Artemis clients in unit tests.  Adding the rule to a test will startup
 * a ClientProducer, which can then be used to feed messages to the bound address on an ActiveMQ Artemis server.
 *
 * <pre><code>
 * public class SimpleTest {
 *     &#64;Rule
 *     public ActiveMQProducerResource producer = new ActiveMQProducerResource( "vm://0", "test.queue");
 *
 *     &#64;Test
 *     public void testSomething() throws Exception {
 *         // Use the embedded ClientProducer here
 *         producer.sendMessage( "String Body" );
 *     }
 * }
 * </code></pre>
 */
public class ActiveMQProducerExtension implements BeforeAllCallback, AfterAllCallback {

   private ActiveMQProducerDelegate activeMQProducerDelegate;

   protected ActiveMQProducerExtension(String url, String username, String password) {
      this.activeMQProducerDelegate = new ActiveMQProducerDelegate(url, username, password);
   }

   protected ActiveMQProducerExtension(String url) {
      this.activeMQProducerDelegate = new ActiveMQProducerDelegate(url);
   }

   protected ActiveMQProducerExtension(ServerLocator serverLocator, String username, String password) {
      this.activeMQProducerDelegate = new ActiveMQProducerDelegate(serverLocator, username, password);
   }

   protected ActiveMQProducerExtension(ServerLocator serverLocator) {
      this.activeMQProducerDelegate = new ActiveMQProducerDelegate(serverLocator);
   }

   public ActiveMQProducerExtension(String url, String address, String username, String password) {
      this.activeMQProducerDelegate = new ActiveMQProducerDelegate(url, address, username, password);
   }

   public ActiveMQProducerExtension(String url, String address) {
      this.activeMQProducerDelegate = new ActiveMQProducerDelegate(url, address);
   }

   public ActiveMQProducerExtension(String url, SimpleString address, String username, String password) {
      this.activeMQProducerDelegate = new ActiveMQProducerDelegate(url, username, password);
   }

   public ActiveMQProducerExtension(String url, SimpleString address) {
      this.activeMQProducerDelegate = new ActiveMQProducerDelegate(url, address);
   }

   public ActiveMQProducerExtension(ServerLocator serverLocator, String address, String username, String password) {
      this.activeMQProducerDelegate = new ActiveMQProducerDelegate(serverLocator, address, username, password);
   }

   public ActiveMQProducerExtension(ServerLocator serverLocator, String address) {
      this.activeMQProducerDelegate = new ActiveMQProducerDelegate(serverLocator, address);
   }

   public ActiveMQProducerExtension(ServerLocator serverLocator, SimpleString address, String username, String password) {
      this.activeMQProducerDelegate = new ActiveMQProducerDelegate(serverLocator, address, username, password);
   }

   public ActiveMQProducerExtension(ServerLocator serverLocator, SimpleString address) {
      this.activeMQProducerDelegate = new ActiveMQProducerDelegate(serverLocator, address);
   }

   /**
    * Invoked by JUnit to setup the resource - start the embedded ActiveMQ Artemis server
    */
   @Override
   public void beforeAll(ExtensionContext context) throws Exception {
      activeMQProducerDelegate.start();
   }

   /**
    * Invoked by JUnit to tear down the resource - stops the embedded ActiveMQ Artemis server
    */
   @Override
   public void afterAll(ExtensionContext context) throws Exception {
      activeMQProducerDelegate.stop();
   }

   public boolean isUseDurableMessage() {
      return activeMQProducerDelegate.isUseDurableMessage();
   }

   public void setUseDurableMessage(boolean useDurableMessage) {
      activeMQProducerDelegate.setUseDurableMessage(useDurableMessage);
   }

   public void createClient() {
      activeMQProducerDelegate.createClient();
   }

   public void stopClient() {
      activeMQProducerDelegate.stopClient();
   }

   public ClientMessage createMessage() {
      return activeMQProducerDelegate.createMessage();
   }

   public ClientMessage createMessage(byte[] body) {
      return activeMQProducerDelegate.createMessage(body);
   }

   public ClientMessage createMessage(String body) {
      return activeMQProducerDelegate.createMessage(body);
   }

   public ClientMessage createMessage(Map<String, Object> properties) {
      return activeMQProducerDelegate.createMessage(properties);
   }

   public ClientMessage createMessage(byte[] body, Map<String, Object> properties) {
      return activeMQProducerDelegate.createMessage(body, properties);
   }

   public ClientMessage createMessage(String body, Map<String, Object> properties) {
      return activeMQProducerDelegate.createMessage(body, properties);
   }

   public void sendMessage(ClientMessage message) {
      activeMQProducerDelegate.sendMessage(message);
   }

   public ClientMessage sendMessage(byte[] body) {
      return activeMQProducerDelegate.sendMessage(body);
   }

   public ClientMessage sendMessage(String body) {
      return activeMQProducerDelegate.sendMessage(body);
   }

   public ClientMessage sendMessage(Map<String, Object> properties) {
      return activeMQProducerDelegate.sendMessage(properties);
   }

   public ClientMessage sendMessage(byte[] body, Map<String, Object> properties) {
      return activeMQProducerDelegate.sendMessage(body, properties);
   }

   public ClientMessage sendMessage(String body, Map<String, Object> properties) {
      return activeMQProducerDelegate.sendMessage(body, properties);
   }

   public static void addMessageProperties(ClientMessage message, Map<String, Object> properties) {
      AbstractActiveMQClientDelegate.addMessageProperties(message, properties);
   }

   public boolean isAutoCreateQueue() {
      return activeMQProducerDelegate.isAutoCreateQueue();
   }

   public void setAutoCreateQueue(boolean autoCreateQueue) {
      activeMQProducerDelegate.setAutoCreateQueue(autoCreateQueue);
   }
}
