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
import javax.jms.Topic;

import org.apache.activemq.artemis.api.core.DiscoveryGroupConfiguration;
import org.apache.activemq.artemis.api.core.UDPBroadcastEndpointFactory;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.api.jms.JMSFactoryType;

/**
 * This example demonstrates a cluster of three nodes set up in a symmetric topology - i.e. each
 * node is connected to every other node in the cluster. Also each node, has it's own backup node.
 * <p>
 * This is probably the most obvious clustering topology and the one most people will be familiar
 * with from using clustering in an app server, where every node has pretty much identical
 * configuration to every other node.
 * <p>
 * By clustering nodes symmetrically, ActiveMQ Artemis can give the impression of clustered queues, topics
 * and durable subscriptions.
 * <p>
 * In this example we send some messages to a distributed queue and topic and kill all the live
 * servers at different times, and verify that they transparently fail over onto their backup
 * servers.
 * <p>
 * Please see the readme for more information.
 */
public class SymmetricClusterExample {

   public static void main(final String[] args) throws Exception {
      Connection connection0 = null;

      Connection connection1 = null;

      Connection connection2 = null;

      Connection connection3 = null;

      Connection connection4 = null;

      Connection connection5 = null;

      try {
         // Step 1 - We instantiate a connection factory directly, specifying the UDP address and port for discovering
         // the list of servers in the cluster.
         // We could use JNDI to look-up a connection factory, but we'd need to know the JNDI server host and port for
         // the
         // specific server to do that, and that server might not be available at the time. By creating the
         // connection factory directly we avoid having to worry about a JNDI look-up.
         // In an app server environment you could use HA-JNDI to lookup from the clustered JNDI servers without
         // having to know about a specific one.
         UDPBroadcastEndpointFactory udpCfg = new UDPBroadcastEndpointFactory();
         udpCfg.setGroupAddress("231.7.7.7").setGroupPort(9876);
         DiscoveryGroupConfiguration groupConfiguration = new DiscoveryGroupConfiguration();
         groupConfiguration.setBroadcastEndpointFactory(udpCfg);

         ConnectionFactory cf = ActiveMQJMSClient.createConnectionFactoryWithHA(groupConfiguration, JMSFactoryType.CF);

         // We give a little while for each server to broadcast its whereabouts to the client
         Thread.sleep(2000);

         // Step 2. Directly instantiate JMS Queue and Topic objects
         Queue queue = ActiveMQJMSClient.createQueue("exampleQueue");

         Topic topic = ActiveMQJMSClient.createTopic("exampleTopic");

         // Step 3. We create six connections, they should be to different nodes of the cluster in a round-robin fashion
         // and start them
         connection0 = cf.createConnection();

         connection1 = cf.createConnection();

         connection2 = cf.createConnection();

         connection3 = cf.createConnection();

         connection4 = cf.createConnection();

         connection5 = cf.createConnection();

         connection0.start();

         connection1.start();

         connection2.start();

         connection3.start();

         connection4.start();

         connection5.start();

         // Step 4. We create a session on each connection

         Session session0 = connection0.createSession(false, Session.AUTO_ACKNOWLEDGE);

         Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);

         Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);

         Session session3 = connection0.createSession(false, Session.AUTO_ACKNOWLEDGE);

         Session session4 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);

         Session session5 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 5. We create a topic subscriber on each server

         MessageConsumer subscriber0 = session0.createConsumer(topic);

         MessageConsumer subscriber1 = session1.createConsumer(topic);

         MessageConsumer subscriber2 = session2.createConsumer(topic);

         MessageConsumer subscriber3 = session3.createConsumer(topic);

         MessageConsumer subscriber4 = session4.createConsumer(topic);

         MessageConsumer subscriber5 = session5.createConsumer(topic);

         // Step 6. We create a queue consumer on server 0

         MessageConsumer consumer0 = session0.createConsumer(queue);

         // Step 7. We create an anonymous message producer on just one server 2

         MessageProducer producer2 = session2.createProducer(null);

         // Step 8. We send 500 messages each to the queue and topic

         final int numMessages = 500;

         for (int i = 0; i < numMessages; i++) {
            TextMessage message1 = session2.createTextMessage("Topic message " + i);

            producer2.send(topic, message1);

            TextMessage message2 = session2.createTextMessage("Queue message " + i);

            producer2.send(queue, message2);
         }

         // Step 9. Verify all subscribers and the consumer receive the messages

         for (int i = 0; i < numMessages; i++) {
            TextMessage received0 = (TextMessage) subscriber0.receive(5000);

            if (received0 == null) {
               throw new IllegalStateException("Message is null!");
            }

            TextMessage received1 = (TextMessage) subscriber1.receive(5000);

            if (received1 == null) {
               throw new IllegalStateException("Message is null!");
            }

            TextMessage received2 = (TextMessage) subscriber2.receive(5000);

            if (received2 == null) {
               throw new IllegalStateException("Message is null!");
            }

            TextMessage received3 = (TextMessage) subscriber3.receive(5000);

            if (received3 == null) {
               throw new IllegalStateException("Message is null!");
            }

            TextMessage received4 = (TextMessage) subscriber4.receive(5000);

            if (received4 == null) {
               throw new IllegalStateException("Message is null!");
            }

            TextMessage received5 = (TextMessage) subscriber5.receive(5000);

            if (received5 == null) {
               throw new IllegalStateException("Message is null!");
            }

            TextMessage received6 = (TextMessage) consumer0.receive(5000);

            if (received6 == null) {
               throw new IllegalStateException("Message is null!");
            }
         }
      } finally {
         // Step 15. Be sure to close our resources!

         connection0.close();
         connection1.close();
         connection2.close();
         connection3.close();
         connection4.close();
         connection5.close();
      }
   }
}
