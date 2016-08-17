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
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import org.apache.activemq.artemis.core.client.impl.ClientSessionInternal;
import org.apache.activemq.artemis.util.ServerUtil;

/**
 * A simple example that demonstrates server side load-balancing of messages between the queue instances on different
 * nodes of the cluster. The cluster is created from a static list of nodes and each ConnectionFactory create a Connection.
 */
public class StaticClusteredQueueExamplewithMultiFactory {

   public static void main(final String[] args) {
      for (int j = 0; j < 10; j++) {
         new Thread() {
            public void run() {  

            Connection connection0 = null;

            try {
               InitialContext context = new InitialContext();
               // Step 1. Use JNDI lookup the queue
               Queue queue = (Queue) context.lookup("queue/exampleQueue");

               // Step 2. new JMS Connection Factory object from JNDI
               ConnectionFactory cf0 = (ConnectionFactory) context.lookup("ConnectionFactory");

               // Step 3. We create a JMS Connection connection0 use the initial transportations 
               connection0 = cf0.createConnection();

               // Step 4. We create a JMS Session
               Session session0 = connection0.createSession(false, Session.AUTO_ACKNOWLEDGE);

               // Step 5. We start the connections to ensure delivery occurs on them
               connection0.start();

               System.out.println("Session A - " + ((ClientSessionInternal) ((org.apache.activemq.artemis.jms.client.ActiveMQSession) session0).getCoreSession()).getConnection().getRemoteAddress());

               // Step 6. We create JMS MessageConsumer objects
               MessageConsumer consumer0 = session0.createConsumer(queue);


               // Step 7. We create a JMS MessageProducer object
               MessageProducer producer = session0.createProducer(queue);

               // Step 8. We send some messages

               final int numMessages = 10;

               for (int i = 0; i < numMessages; i++) {
                  TextMessage message = session0.createTextMessage("This is text message " + i);

                     producer.send(message);

                     System.out.println("Sent message: " + message.getText());
               }

               // Step 9. We now consume those messages on *both* server 0 server1 server2 server3
               // We note the messages have been distributed between servers in a round robin fashion
               // JMS Queues implement point-to-point message where each message is only ever consumed by a
               // maximum of one consumer
               int conNode = ServerUtil.getServer(connection0);

               for (int i = 0; i < numMessages; i += 4) {
                  TextMessage message0 = (TextMessage) consumer0.receive(5000);

                  System.out.println("Got message: " + message0.getText() + " from node " + conNode);
               }
            } catch (Exception e) {
               e.printStackTrace();
            }
            finally {
               // Step 0. Be sure to close our resources!
               try {

                  if (connection0 != null) {
                     connection0.close();
                  }
               }
               catch (Exception e) {
                  e.printStackTrace();
               }
           }
          }
         }.start();
      }
   }
}
