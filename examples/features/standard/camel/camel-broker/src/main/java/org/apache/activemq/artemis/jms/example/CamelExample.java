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

public class CamelExample {

   public static void main(final String[] args) throws Exception {
      Connection connection = null;

      InitialContext ic = null;

      try {
         // Step 1. - we create an initial context for looking up JNDI

         Hashtable<String, Object> properties = new Hashtable<>();
         properties.put("java.naming.factory.initial", "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory");
         properties.put("connectionFactory.ConnectionFactory", "tcp://127.0.0.1:61616");
         properties.put("queue.queue/sausage-factory", "sausage-factory");
         properties.put("queue.queue/mincing-machine", "mincing-machine");
         ic = new InitialContext(properties);

         // Step 2. - we look up the sausage-factory queue from node 0

         Queue sausageFactory = (Queue) ic.lookup("queue/sausage-factory");

         Queue mincingMachine = (Queue) ic.lookup("queue/mincing-machine");

         // Step 3. - we look up a JMS ConnectionFactory object from node 0

         ConnectionFactory cf = (ConnectionFactory) ic.lookup("ConnectionFactory");

         // Step 4. We create a JMS Connection connection which is a connection to server 0

         connection = cf.createConnection();

         // Step 5. We create a JMS Session on server 0

         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 6. We start the connection to ensure delivery occurs on them

         connection.start();

         // Step 7. We create JMS MessageConsumer object
         MessageConsumer consumer = session.createConsumer(mincingMachine);

         // Step 8. We create a JMS MessageProducer object
         MessageProducer producer = session.createProducer(sausageFactory);

         // Step 9. We create and send a message to the sausage-factory
         Message message = session.createMessage();

         producer.send(message);

         System.out.println("Sent message to sausage-factory");

         // Step 10. - we successfully receive the message from the mincing-machine.

         Message receivedMessage = consumer.receive(5000);

         if (receivedMessage == null) {
            throw new IllegalStateException();
         }

         System.out.println("Received message from mincing-machine");
      } finally {
         // Step 15. Be sure to close our resources!

         if (connection != null) {
            connection.close();
         }

         if (ic != null) {
            ic.close();
         }
      }
   }
}
