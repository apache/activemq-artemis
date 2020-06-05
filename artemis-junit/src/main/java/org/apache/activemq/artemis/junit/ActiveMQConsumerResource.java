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

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.api.core.RoutingType;

/**
 * A JUnit Rule that embeds an ActiveMQ Artemis ClientConsumer into a test.
 *
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
public class ActiveMQConsumerResource extends AbstractActiveMQClientResource {

   long defaultReceiveTimeout = 50;

   SimpleString queueName;
   ClientConsumer consumer;

   public ActiveMQConsumerResource(String url, String queueName) {
      this(url, SimpleString.toSimpleString(queueName), null, null);
   }

   public ActiveMQConsumerResource(String url, String queueName, String username, String password) {
      this(url, SimpleString.toSimpleString(queueName), username, password);
   }

   public ActiveMQConsumerResource(String url, SimpleString queueName, String username, String password) {
      super(url, username, password);
      this.queueName = queueName;
   }

   public ActiveMQConsumerResource(String url, SimpleString queueName) {
      this(url, queueName, null, null);
   }

   public ActiveMQConsumerResource(ServerLocator serverLocator, String queueName, String username, String password) {
      this(serverLocator, SimpleString.toSimpleString(queueName), username, password);
   }

   public ActiveMQConsumerResource(ServerLocator serverLocator, String queueName) {
      this(serverLocator, SimpleString.toSimpleString(queueName), null, null);
   }

   public ActiveMQConsumerResource(ServerLocator serverLocator, SimpleString queueName, String username, String password) {
      super(serverLocator, username, password);
      this.queueName = queueName;
   }

   public ActiveMQConsumerResource(ServerLocator serverLocator, SimpleString queueName) {
      this(serverLocator, queueName, null, null);
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

   @Override
   protected void createClient() {
      boolean browseOnly = false;
      try {
         if (!session.queueQuery(queueName).isExists() && autoCreateQueue) {
            log.warn("{}: queue does not exist - creating queue: address = {}, name = {}", this.getClass().getSimpleName(), queueName.toString(), queueName.toString());
            session.createAddress(queueName, RoutingType.MULTICAST, true);
            session.createQueue(new QueueConfiguration(queueName));
         }
         consumer = session.createConsumer(queueName, browseOnly);
      } catch (ActiveMQException amqEx) {
         throw new ActiveMQClientResourceException(String.format("Error creating consumer for queueName %s", queueName.toString()), amqEx);
      }
   }

   @Override
   protected void stopClient() {
      if (consumer != null) {
         try {
            consumer.close();
         } catch (ActiveMQException amqEx) {
            log.warn("Exception encountered closing consumer - ignoring", amqEx);
         } finally {
            consumer = null;
         }
      }
   }

   @Override
   public boolean isAutoCreateQueue() {
      return autoCreateQueue;
   }

   /**
    * Enable/Disable the automatic creation of non-existent queues.  The default is to automatically create non-existent queues
    *
    * @param autoCreateQueue
    */
   @Override
   public void setAutoCreateQueue(boolean autoCreateQueue) {
      this.autoCreateQueue = autoCreateQueue;
   }

   public ClientMessage receiveMessage() {
      return receiveMessage(defaultReceiveTimeout);
   }

   public ClientMessage receiveMessage(long timeout) {
      ClientMessage message = null;
      if (timeout > 0) {
         try {
            message = consumer.receive(timeout);
         } catch (ActiveMQException amqEx) {
            throw new EmbeddedActiveMQResource.EmbeddedActiveMQResourceException(String.format("ClientConsumer.receive( timeout = %d ) for %s failed", timeout, queueName.toString()), amqEx);
         }
      } else if (timeout == 0) {
         try {
            message = consumer.receiveImmediate();
         } catch (ActiveMQException amqEx) {
            throw new EmbeddedActiveMQResource.EmbeddedActiveMQResourceException(String.format("ClientConsumer.receiveImmediate() for %s failed", queueName.toString()), amqEx);
         }
      } else {
         try {
            message = consumer.receive();
         } catch (ActiveMQException amqEx) {
            throw new EmbeddedActiveMQResource.EmbeddedActiveMQResourceException(String.format("ClientConsumer.receive() for %s failed", queueName.toString()), amqEx);
         }
      }

      return message;
   }

}
