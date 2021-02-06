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

import java.util.Map;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.junit.rules.ExternalResource;

/**
 * A JUnit Rule that embeds an dynamic (i.e. unbound) ActiveMQ Artemis ClientProducer into a test.
 * <p>
 * This JUnit Rule is designed to simplify using ActiveMQ Artemis clients in unit tests.  Adding the rule to a test will startup
 * an unbound ClientProducer, which can then be used to feed messages to any address on the ActiveMQ Artemis server.
 *
 * <pre><code>
 * public class SimpleTest {
 *     &#64;Rule
 *     public ActiveMQDynamicProducerResource producer = new ActiveMQDynamicProducerResource( "vm://0");
 *
 *     &#64;Test
 *     public void testSomething() throws Exception {
 *         // Use the embedded ClientProducer here
 *         producer.sendMessage( "test.address", "String Body" );
 *     }
 * }
 * </code></pre>
 */
public class ActiveMQDynamicProducerResource extends ExternalResource {

   private ActiveMQDynamicProducerDelegate activeMQDynamicProducer;

   public ActiveMQDynamicProducerResource(String url, String username, String password) {
      this.activeMQDynamicProducer = new ActiveMQDynamicProducerDelegate(url, username, password);
   }

   public ActiveMQDynamicProducerResource(String url) {
      this.activeMQDynamicProducer = new ActiveMQDynamicProducerDelegate(url);
   }

   public ActiveMQDynamicProducerResource(ServerLocator serverLocator, String username, String password) {
      this.activeMQDynamicProducer = new ActiveMQDynamicProducerDelegate(serverLocator, username, password);
   }

   public ActiveMQDynamicProducerResource(ServerLocator serverLocator) {
      this.activeMQDynamicProducer = new ActiveMQDynamicProducerDelegate(serverLocator);
   }

   public ActiveMQDynamicProducerResource(String url, SimpleString address, String username, String password) {
      this.activeMQDynamicProducer = new ActiveMQDynamicProducerDelegate(url, address, username, password);
   }

   public ActiveMQDynamicProducerResource(String url, SimpleString address) {
      this.activeMQDynamicProducer = new ActiveMQDynamicProducerDelegate(url, address);
   }

   public ActiveMQDynamicProducerResource(ServerLocator serverLocator,
                                          SimpleString address,
                                          String username,
                                          String password) {
      this.activeMQDynamicProducer = new ActiveMQDynamicProducerDelegate(serverLocator, address, username, password);
   }

   public ActiveMQDynamicProducerResource(ServerLocator serverLocator, SimpleString address) {
      this.activeMQDynamicProducer = new ActiveMQDynamicProducerDelegate(serverLocator, address);
   }

   @Override
   protected void before() throws Throwable {
      super.before();
      activeMQDynamicProducer.start();
   }

   @Override
   protected void after() {
      activeMQDynamicProducer.stop();
      super.after();
   }

   protected void createClient() {
      activeMQDynamicProducer.createClient();
   }

   protected void sendMessage(ClientMessage message) {
      activeMQDynamicProducer.sendMessage(message);
   }

   public void sendMessage(SimpleString targetAddress, ClientMessage message) {
      activeMQDynamicProducer.sendMessage(targetAddress, message);
   }

   public ClientMessage sendMessage(SimpleString targetAddress, byte[] body) {
      return activeMQDynamicProducer.sendMessage(targetAddress, body);
   }

   public ClientMessage sendMessage(SimpleString targetAddress, String body) {
      return activeMQDynamicProducer.sendMessage(targetAddress, body);
   }

   public ClientMessage sendMessage(SimpleString targetAddress, Map<String, Object> properties) {
      return activeMQDynamicProducer.sendMessage(targetAddress, properties);
   }

   public ClientMessage sendMessage(SimpleString targetAddress, byte[] body, Map<String, Object> properties) {
      return activeMQDynamicProducer.sendMessage(targetAddress, body, properties);
   }

   public ClientMessage sendMessage(SimpleString targetAddress, String body, Map<String, Object> properties) {
      return activeMQDynamicProducer.sendMessage(targetAddress, body, properties);
   }

   public boolean isUseDurableMessage() {
      return activeMQDynamicProducer.isUseDurableMessage();
   }

   public void setUseDurableMessage(boolean useDurableMessage) {
      activeMQDynamicProducer.setUseDurableMessage(useDurableMessage);
   }

   public void stopClient() {
      activeMQDynamicProducer.stopClient();
   }

   public ClientMessage createMessage() {
      return activeMQDynamicProducer.createMessage();
   }

   public ClientMessage createMessage(byte[] body) {
      return activeMQDynamicProducer.createMessage(body);
   }

   public ClientMessage createMessage(String body) {
      return activeMQDynamicProducer.createMessage(body);
   }

   public ClientMessage createMessage(Map<String, Object> properties) {
      return activeMQDynamicProducer.createMessage(properties);
   }

   public ClientMessage createMessage(byte[] body, Map<String, Object> properties) {
      return activeMQDynamicProducer.createMessage(body, properties);
   }

   public ClientMessage createMessage(String body, Map<String, Object> properties) {
      return activeMQDynamicProducer.createMessage(body, properties);
   }

   public ClientMessage sendMessage(byte[] body) {
      return activeMQDynamicProducer.sendMessage(body);
   }

   public ClientMessage sendMessage(String body) {
      return activeMQDynamicProducer.sendMessage(body);
   }

   public ClientMessage sendMessage(Map<String, Object> properties) {
      return activeMQDynamicProducer.sendMessage(properties);
   }

   public ClientMessage sendMessage(byte[] body, Map<String, Object> properties) {
      return activeMQDynamicProducer.sendMessage(body, properties);
   }

   public ClientMessage sendMessage(String body, Map<String, Object> properties) {
      return activeMQDynamicProducer.sendMessage(body, properties);
   }

   public static void addMessageProperties(ClientMessage message, Map<String, Object> properties) {
      AbstractActiveMQClientDelegate.addMessageProperties(message, properties);
   }

   public boolean isAutoCreateQueue() {
      return activeMQDynamicProducer.isAutoCreateQueue();
   }

   public void setAutoCreateQueue(boolean autoCreateQueue) {
      activeMQDynamicProducer.setAutoCreateQueue(autoCreateQueue);
   }
}
