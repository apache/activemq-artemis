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
import org.apache.activemq.artemis.api.core.client.ServerLocator;

/**
 * A JUnit Rule that embeds an dynamic (i.e. unbound) ActiveMQ Artemis ClientProducer into a test.
 *
 * This JUnit Rule is designed to simplify using ActiveMQ Artemis clients in unit tests.  Adding the rule to a test will startup
 * an unbound ClientProducer, which can then be used to feed messages to any address on the ActiveMQ Artemis server.
 *
 * <pre><code>
 * public class SimpleTest {
 *     @Rule
 *     public ActiveMQDynamicProducerResource producer = new ActiveMQDynamicProducerResource( "vm://0");
 *
 *     @Test
 *     public void testSomething() throws Exception {
 *         // Use the embedded ClientProducer here
 *         producer.sendMessage( "test.address", "String Body" );
 *     }
 * }
 * </code></pre>
 */
public class ActiveMQDynamicProducerResource extends ActiveMQProducerResource {

   public ActiveMQDynamicProducerResource(String url) {
      super(url);
   }

   public ActiveMQDynamicProducerResource(ServerLocator serverLocator) {
      super(serverLocator);
   }

   public ActiveMQDynamicProducerResource(String url, SimpleString address) {
      super(url, address);
   }

   public ActiveMQDynamicProducerResource(ServerLocator serverLocator, SimpleString address) {
      super(serverLocator, address);
   }

   @Override
   protected void createClient() {
      try {
         if (address != null && !session.addressQuery(address).isExists() && autoCreateQueue) {
            log.warn("queue does not exist - creating queue: address = {}, name = {}", address.toString(), address.toString());
            session.createQueue(address, address);
         }
         producer = session.createProducer((SimpleString) null);
      } catch (ActiveMQException amqEx) {
         if (address == null) {
            throw new ActiveMQClientResourceException(String.format("Error creating producer for address %s", address.toString()), amqEx);
         } else {
            throw new ActiveMQClientResourceException("Error creating producer", amqEx);
         }
      }
   }

   /**
    * Send a ClientMessage to the default address on the server
    *
    * @param message the message to send
    */
   @Override
   public void sendMessage(ClientMessage message) {
      sendMessage(address, message);
   }

   /**
    * Send a ClientMessage to the specified address on the server
    *
    * @param targetAddress the target address
    * @param message       the message to send
    */
   public void sendMessage(SimpleString targetAddress, ClientMessage message) {
      if (targetAddress == null) {
         throw new IllegalArgumentException(String.format("%s error - address cannot be null", this.getClass().getSimpleName()));
      }
      try {
         if (autoCreateQueue && !session.addressQuery(targetAddress).isExists()) {
            log.warn("queue does not exist - creating queue: address = {}, name = {}", address.toString(), address.toString());
            session.createQueue(targetAddress, targetAddress);
         }
      } catch (ActiveMQException amqEx) {
         throw new ActiveMQClientResourceException(String.format("Queue creation failed for queue: address = %s, name = %s", address.toString(), address.toString()));
      }

      try {
         producer.send(targetAddress, message);
      } catch (ActiveMQException amqEx) {
         throw new ActiveMQClientResourceException(String.format("Failed to send message to %s", targetAddress.toString()), amqEx);
      }
   }

   /**
    * Create a new ClientMessage with the specified body and send to the specified address on the server
    *
    * @param targetAddress the target address
    * @param body          the body for the new message
    * @return the message that was sent
    */
   public ClientMessage sendMessage(SimpleString targetAddress, byte[] body) {
      ClientMessage message = createMessage(body);
      sendMessage(targetAddress, message);
      return message;
   }

   /**
    * Create a new ClientMessage with the specified body and send to the server
    *
    * @param targetAddress the target address
    * @param body          the body for the new message
    * @return the message that was sent
    */
   public ClientMessage sendMessage(SimpleString targetAddress, String body) {
      ClientMessage message = createMessage(body);
      sendMessage(targetAddress, message);
      return message;
   }

   /**
    * Create a new ClientMessage with the specified properties and send to the server
    *
    * @param targetAddress the target address
    * @param properties    the properties for the new message
    * @return the message that was sent
    */
   public ClientMessage sendMessage(SimpleString targetAddress, Map<String, Object> properties) {
      ClientMessage message = createMessage(properties);
      sendMessage(targetAddress, message);
      return message;
   }

   /**
    * Create a new ClientMessage with the specified body and and properties and send to the server
    *
    * @param targetAddress the target address
    * @param properties    the properties for the new message
    * @return the message that was sent
    */
   public ClientMessage sendMessage(SimpleString targetAddress, byte[] body, Map<String, Object> properties) {
      ClientMessage message = createMessage(body);
      sendMessage(targetAddress, message);
      return message;
   }

   /**
    * Create a new ClientMessage with the specified body and and properties and send to the server
    *
    * @param targetAddress the target address
    * @param properties    the properties for the new message
    * @return the message that was sent
    */
   public ClientMessage sendMessage(SimpleString targetAddress, String body, Map<String, Object> properties) {
      ClientMessage message = createMessage(body);
      sendMessage(targetAddress, message);
      return message;
   }

}
