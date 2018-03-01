/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.jms.example;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.naming.InitialContext;
import java.util.Hashtable;

/**
 * This example demonstrates a core bridge set-up between two nodes, consuming messages from a queue
 * on one node and forwarding them to an address on the second node.
 */
public class BridgeExample {

   public static void main(final String[] args) throws Exception {
      Connection connection0 = null;

      Connection connection1 = null;

      InitialContext ic0 = null;

      InitialContext ic1 = null;

      try {
         // Step 1 - we create an initial context for looking up JNDI on node 0

         Hashtable<String, Object> properties = new Hashtable<>();
         properties.put("java.naming.factory.initial", "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory");
         properties.put("connectionFactory.ConnectionFactory", "tcp://127.0.0.1:61616");
         properties.put("queue.queue/sausage-factory", "sausage-factory");
         ic0 = new InitialContext(properties);

         // Step 2 - we look up the sausage-factory queue from node 0

         Queue sausageFactory = (Queue) ic0.lookup("queue/sausage-factory");

         // Step 3 - we look up a JMS ConnectionFactory object from node 0

         ConnectionFactory cf0 = (ConnectionFactory) ic0.lookup("ConnectionFactory");

         // Step 4 - we create an initial context for looking up JNDI on node 1

         properties = new Hashtable<>();
         properties.put("java.naming.factory.initial", "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory");
         properties.put("connectionFactory.ConnectionFactory", "tcp://127.0.0.1:61617");
         properties.put("queue.queue/mincing-machine", "mincing-machine");
         ic1 = new InitialContext(properties);

         // Step 5 - we look up the mincing-machine queue on node 1

         Queue mincingMachine = (Queue) ic1.lookup("queue/mincing-machine");

         // Step 6 - we look up a JMS ConnectionFactory object from node 1

         ConnectionFactory cf1 = (ConnectionFactory) ic1.lookup("ConnectionFactory");

         // Step 7. We create a JMS Connection connection0 which is a connection to server 0

         connection0 = cf0.createConnection();

         // Step 8. We create a JMS Connection connection1 which is a connection to server 1
         connection1 = cf1.createConnection();

         // Step 9. We create a JMS Session on server 0

         Session session0 = connection0.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 10. We create a JMS Session on server 1

         Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 10. We start the connection to ensure delivery occurs on them

         connection1.start();

         // Step 11. We create JMS MessageConsumer object
         MessageConsumer consumer = session1.createConsumer(mincingMachine);

         // Step 12. We create a JMS MessageProducer object on server 0
         MessageProducer producer = session0.createProducer(sausageFactory);

         // Step 13. We create and send a message representing an aardvark with a green hat to the sausage-factory
         // on node 0
         Message message = session0.createMessage();

         message.setStringProperty("name", "aardvark");

         message.setStringProperty("hat", "green");

         producer.send(message);

         System.out.println("Sent " + message.getStringProperty("name") +
                               " message with " +
                               message.getStringProperty("hat") +
                               " hat to sausage-factory on node 0");

         // Step 14 - we successfully receive the aardvark message from the mincing-machine one node 1. The aardvark's
         // hat is now blue since it has been transformed!

         Message receivedMessage = consumer.receive(5000);

         System.out.println("Received " + receivedMessage.getStringProperty("name") +
                               " message with " +
                               receivedMessage.getStringProperty("hat") +
                               " hat from mincing-machine on node 1");

         // Step 13. We create and send another message, this time representing a sasquatch with a mauve hat to the
         // sausage-factory on node 0. This won't be bridged to the mincing-machine since we only want aardvarks, not
         // sasquatches

         message = session0.createMessage();

         message.setStringProperty("name", "sasquatch");

         message.setStringProperty("hat", "mauve");

         producer.send(message);

         System.out.println("Sent " + message.getStringProperty("name") +
                               " message with " +
                               message.getStringProperty("hat") +
                               " hat to sausage-factory on node 0");

         // Step 14. We don't receive the message since it has not been bridged.

         receivedMessage = consumer.receive(1000);

         if (receivedMessage == null) {
            System.out.println("Didn't receive that message from mincing-machine on node 1");
         } else {
            throw new IllegalStateException();
         }
      } finally {
         // Step 15. Be sure to close our resources!

         if (connection0 != null) {
            connection0.close();
         }

         if (connection1 != null) {
            connection1.close();
         }

         if (ic0 != null) {
            ic0.close();
         }

         if (ic1 != null) {
            ic1.close();
         }
      }
   }
}
