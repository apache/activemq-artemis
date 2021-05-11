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
 * A JUnit Rule that embeds an ActiveMQ Artemis ClientConsumer into a test.
 * <p>
 * This JUnit Rule is designed to simplify using ActiveMQ Artemis clients in unit tests.  Adding the rule to a test will startup
 * a ClientConsumer, which can then be used to consume messages from an ActiveMQ Artemis server.
 *
 * <pre><code>
 * public class SimpleTest {
 *     &#64;Rule
 *     public ActiveMQConsumerResource client = new ActiveMQProducerResource( "vm://0", "test.queue" );
 *
 *     &#64;Test
 *     public void testSomething() throws Exception {
 *         // Use the embedded client here
 *         ClientMessage message = client.receiveMessage();
 *     }
 * }
 * </code></pre>
 */
public class ActiveMQConsumerResource extends ExternalResource {

   private ActiveMQConsumerDelegate activeMQConsumer;

   public ActiveMQConsumerResource(String url, String queueName) {
      this.activeMQConsumer = new ActiveMQConsumerDelegate(url, queueName);
   }

   public ActiveMQConsumerResource(String url, String queueName, String username, String password) {
      this.activeMQConsumer = new ActiveMQConsumerDelegate(url, queueName, username, password);
   }

   public ActiveMQConsumerResource(String url, SimpleString queueName, String username, String password) {
      this.activeMQConsumer = new ActiveMQConsumerDelegate(url, queueName, username, password);
   }

   public ActiveMQConsumerResource(String url, SimpleString queueName) {
      this.activeMQConsumer = new ActiveMQConsumerDelegate(url, queueName);
   }

   public ActiveMQConsumerResource(ServerLocator serverLocator, String queueName, String username, String password) {
      this.activeMQConsumer = new ActiveMQConsumerDelegate(serverLocator, queueName, username, password);
   }

   public ActiveMQConsumerResource(ServerLocator serverLocator, String queueName) {
      this.activeMQConsumer = new ActiveMQConsumerDelegate(serverLocator, queueName);
   }

   public ActiveMQConsumerResource(ServerLocator serverLocator,
                                   SimpleString queueName,
                                   String username,
                                   String password) {
      this.activeMQConsumer = new ActiveMQConsumerDelegate(serverLocator, queueName, username, password);
   }

   public ActiveMQConsumerResource(ServerLocator serverLocator, SimpleString queueName) {
      this.activeMQConsumer = new ActiveMQConsumerDelegate(serverLocator, queueName);
   }

   @Override
   protected void before() throws Throwable {
      super.before();
      activeMQConsumer.start();
   }

   @Override
   protected void after() {
      activeMQConsumer.stop();
      super.after();
   }

   protected void createClient() {
      activeMQConsumer.createClient();
   }

   protected void stopClient() {
      activeMQConsumer.stopClient();
   }

   public boolean isAutoCreateQueue() {
      return activeMQConsumer.isAutoCreateQueue();
   }

   public void setAutoCreateQueue(boolean autoCreateQueue) {
      activeMQConsumer.setAutoCreateQueue(autoCreateQueue);
   }

   public ClientMessage receiveMessage() {
      return activeMQConsumer.receiveMessage();
   }

   public ClientMessage receiveMessage(long timeout) {
      return activeMQConsumer.receiveMessage(timeout);
   }

   public static void addMessageProperties(ClientMessage message, Map<String, Object> properties) {
      AbstractActiveMQClientDelegate.addMessageProperties(message, properties);
   }

   public long getDefaultReceiveTimeout() {
      return activeMQConsumer.getDefaultReceiveTimeout();
   }

   public void setDefaultReceiveTimeout(long defaultReceiveTimeout) {
      activeMQConsumer.setDefaultReceiveTimeout(defaultReceiveTimeout);
   }
}
