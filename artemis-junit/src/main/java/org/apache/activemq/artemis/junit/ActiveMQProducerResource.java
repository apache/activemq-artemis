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

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ServerLocator;

/**
 * A JUnit Rule that embeds an ActiveMQ Artemis ClientProducer bound to a specific address into a test.
 *
 * This JUnit Rule is designed to simplify using ActiveMQ Artemis clients in unit tests.  Adding the rule to a test will startup
 * a ClientProducer, which can then be used to feed messages to the bound address on an ActiveMQ Artemis server.
 *
 * <pre><code>
 * public class SimpleTest {
 *     @Rule
 *     public ActiveMQProducerResource producer = new ActiveMQProducerResource( "vm://0", "test.queue");
 *
 *     @Test
 *     public void testSomething() throws Exception {
 *         // Use the embedded ClientProducer here
 *         producer.sendMessage( "String Body" );
 *     }
 * }
 * </code></pre>
 */
public class ActiveMQProducerResource extends AbstractActiveMQClientResource {

   boolean useDurableMessage = true;
   SimpleString address = null;
   ClientProducer producer;

   protected ActiveMQProducerResource(String url) {
      super(url);
   }

   protected ActiveMQProducerResource(ServerLocator serverLocator) {
      super(serverLocator);
   }

   public ActiveMQProducerResource(String url, String address) {
      this(url, SimpleString.toSimpleString(address));
   }

   public ActiveMQProducerResource(String url, SimpleString address) {
      super(url);
      if (address == null) {
         throw new IllegalArgumentException(String.format("%s construction error - address cannot be null", this.getClass().getSimpleName()));
      }
      this.address = address;
   }

   public ActiveMQProducerResource(ServerLocator serverLocator, String address) {
      this(serverLocator, SimpleString.toSimpleString(address));
   }

   public ActiveMQProducerResource(ServerLocator serverLocator, SimpleString address) {
      super(serverLocator);
      if (address == null) {
         throw new IllegalArgumentException(String.format("%s construction error - address cannot be null", this.getClass().getSimpleName()));
      }
      this.address = address;
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

   @Override
   protected void createClient() {
      try {
         if (!session.addressQuery(address).isExists() && autoCreateQueue) {
            log.warn("{}: queue does not exist - creating queue: address = {}, name = {}", this.getClass().getSimpleName(), address.toString(), address.toString());
            session.createQueue(address, address);
         }
         producer = session.createProducer(address);
      } catch (ActiveMQException amqEx) {
         throw new ActiveMQClientResourceException(String.format("Error creating producer for address %s", address.toString()), amqEx);
      }
   }

   @Override
   protected void stopClient() {
      if (producer != null) {
         try {
            producer.close();
         } catch (ActiveMQException amqEx) {
            log.warn("ActiveMQException encountered closing InternalClient ClientProducer - ignoring", amqEx);
         } finally {
            producer = null;
         }
      }
   }

   /**
    * Create a ClientMessage
    * <p>
    * If useDurableMessage is false, a non-durable message is created.  Otherwise, a durable message is created
    *
    * @return a new ClientMessage
    */
   public ClientMessage createMessage() {
      if (session == null) {
         throw new IllegalStateException("ClientSession is null");
      }
      return session.createMessage(isUseDurableMessage());
   }

   /**
    * Create a ClientMessage with the specified body
    * <p>
    * If useDurableMessage is false, a non-durable message is created.  Otherwise, a durable message is created
    *
    * @param body the body for the new message
    * @return a new ClientMessage with the specified body
    */
   public ClientMessage createMessage(byte[] body) {
      ClientMessage message = createMessage();

      if (body != null) {
         message.writeBodyBufferBytes(body);
      }

      return message;
   }

   /**
    * Create a ClientMessage with the specified body
    * <p>
    * If useDurableMessage is false, a non-durable message is created.  Otherwise, a durable message is created
    *
    * @param body the body for the new message
    * @return a new ClientMessage with the specified body
    */
   public ClientMessage createMessage(String body) {
      ClientMessage message = createMessage();

      if (body != null) {
         message.writeBodyBufferString(body);
      }

      return message;
   }

   /**
    * Create a ClientMessage with the specified message properties
    * <p>
    * If useDurableMessage is false, a non-durable message is created.  Otherwise, a durable message is created
    *
    * @param properties message properties for the new message
    * @return a new ClientMessage with the specified message properties
    */
   public ClientMessage createMessage(Map<String, Object> properties) {
      ClientMessage message = createMessage();

      addMessageProperties(message, properties);

      return message;
   }

   /**
    * Create a ClientMessage with the specified body and message properties
    * <p>
    * If useDurableMessage is false, a non-durable message is created.  Otherwise, a durable message is created
    *
    * @param body       the body for the new message
    * @param properties message properties for the new message
    * @return a new ClientMessage with the specified body and message properties
    */
   public ClientMessage createMessage(byte[] body, Map<String, Object> properties) {
      ClientMessage message = createMessage(body);

      addMessageProperties(message, properties);

      return message;
   }

   /**
    * Create a ClientMessage with the specified body and message properties
    * <p>
    * If useDurableMessage is false, a non-durable message is created.  Otherwise, a durable message is created
    *
    * @param body       the body for the new message
    * @param properties message properties for the new message
    * @return a new ClientMessage with the specified body and message properties
    */
   public ClientMessage createMessage(String body, Map<String, Object> properties) {
      ClientMessage message = createMessage(body);

      addMessageProperties(message, properties);

      return message;
   }

   /**
    * Send a ClientMessage to the server
    *
    * @param message the message to send
    */
   public void sendMessage(ClientMessage message) {
      try {
         producer.send(message);
      } catch (ActiveMQException amqEx) {
         throw new ActiveMQClientResourceException(String.format("Failed to send message to %s", producer.getAddress().toString()), amqEx);
      }
   }

   /**
    * Create a new ClientMessage with the specified body and send to the server
    *
    * @param body the body for the new message
    * @return the message that was sent
    */
   public ClientMessage sendMessage(byte[] body) {
      ClientMessage message = createMessage(body);
      sendMessage(message);
      return message;
   }

   /**
    * Create a new ClientMessage with the specified body and send to the server
    *
    * @param body the body for the new message
    * @return the message that was sent
    */
   public ClientMessage sendMessage(String body) {
      ClientMessage message = createMessage(body);
      sendMessage(message);
      return message;
   }

   /**
    * Create a new ClientMessage with the specified properties and send to the server
    *
    * @param properties the properties for the new message
    * @return the message that was sent
    */
   public ClientMessage sendMessage(Map<String, Object> properties) {
      ClientMessage message = createMessage(properties);
      sendMessage(message);
      return message;
   }

   /**
    * Create a new ClientMessage with the specified body and and properties and send to the server
    *
    * @param properties the properties for the new message
    * @return the message that was sent
    */
   public ClientMessage sendMessage(byte[] body, Map<String, Object> properties) {
      ClientMessage message = createMessage(body);
      sendMessage(message);
      return message;
   }

   /**
    * Create a new ClientMessage with the specified body and and properties and send to the server
    *
    * @param properties the properties for the new message
    * @return the message that was sent
    */
   public ClientMessage sendMessage(String body, Map<String, Object> properties) {
      ClientMessage message = createMessage(body);
      sendMessage(message);
      return message;
   }

}
