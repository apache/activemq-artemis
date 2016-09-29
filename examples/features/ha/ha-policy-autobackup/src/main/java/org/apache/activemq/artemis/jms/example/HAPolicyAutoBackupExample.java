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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ClusterTopologyListener;
import org.apache.activemq.artemis.api.core.client.TopologyMember;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.util.ServerUtil;

/**
 * A simple example that demonstrates server side load-balancing of messages between the queue instances on different
 * nodes of the cluster.
 */
public class HAPolicyAutoBackupExample {

   private static Process server0;

   private static Process server1;

   public static void main(final String[] args) throws Exception {
      Connection connection0 = null;

      Connection connection1 = null;

      try {
         server0 = ServerUtil.startServer(args[0], HAPolicyAutoBackupExample.class.getSimpleName() + "0", 0, 5000);
         server1 = ServerUtil.startServer(args[1], HAPolicyAutoBackupExample.class.getSimpleName() + "1", 1, 5000);

         // Step 2. Look-up the JMS Queue object from JNDI
         Queue queue = ActiveMQJMSClient.createQueue("exampleQueue");

         // Step 3. new connection factories towards server 0 and 1
         ConnectionFactory cf0 = new ActiveMQConnectionFactory("tcp://localhost:61616?ha=true&retryInterval=1000&retryIntervalMultiplier=1.0&reconnectAttempts=-1");
         ConnectionFactory cf1 = new ActiveMQConnectionFactory("tcp://localhost:61617?ha=true&retryInterval=1000&retryIntervalMultiplier=1.0&reconnectAttempts=-1");

         // Step 6. We create JMS Connections to server 0 and 1
         connection0 = cf0.createConnection();
         connection1 = cf1.createConnection();

         // step 7. wait for the backups to start replication
         waitForBackups(cf0, 2);

         // Step 8. We create JMS Sessions on server 0 and 1
         Session session0 = connection0.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 9. We start the connections to ensure delivery occurs on them
         connection0.start();
         connection1.start();

         // Step 10. We create JMS MessageConsumer objects on server 0 and server 1
         MessageConsumer consumer0 = session0.createConsumer(queue);
         MessageConsumer consumer1 = session1.createConsumer(queue);

         // Step 11. We create a JMS MessageProducer object on server 0
         MessageProducer producer = session0.createProducer(queue);

         // Step 12. We send some messages to server 0

         final int numMessages = 10;

         for (int i = 0; i < numMessages; i++) {
            TextMessage message = session0.createTextMessage("This is text message " + i);

            producer.send(message);

            System.out.println("Sent message: " + message.getText());
         }

         // Step 13. We now consume half the messages on consumer0
         // note that the other half of the messages will have been sent to server1 for consumer1
         for (int i = 0; i < numMessages / 2; i++) {
            TextMessage message0 = (TextMessage) consumer0.receive(5000);

            System.out.println("Got message: " + message0.getText() + " from node 0");
         }

         // Step 14.close the consumer so it doesn't get any messages
         consumer1.close();

         // Step 15.now kill server1, messages will be scaled down to server0
         ServerUtil.killServer(server1);
         Thread.sleep(5000);

         // Step 16. we now receive the messages that were on server1 but were scaled down to server0
         for (int i = 0; i < numMessages / 2; i++) {
            TextMessage message0 = (TextMessage) consumer0.receive(5000);

            System.out.println("Got message: " + message0.getText() + " from node 1");
         }
      } finally {
         // Step 17. Be sure to close our resources!

         if (connection0 != null) {
            connection0.close();
         }

         if (connection1 != null) {
            connection1.close();
         }

         ServerUtil.killServer(server0);
         ServerUtil.killServer(server1);
      }
   }

   private static void waitForBackups(ConnectionFactory cf0, int backups) throws InterruptedException {
      final CountDownLatch latch = new CountDownLatch(backups);
      ((ActiveMQConnectionFactory) cf0).getServerLocator().addClusterTopologyListener(new ClusterTopologyListener() {
         List<TransportConfiguration> backups = new ArrayList<>();

         @Override
         public void nodeUP(TopologyMember member, boolean last) {
            if (member.getBackup() != null && !backups.contains(member.getBackup())) {
               backups.add(member.getBackup());
               latch.countDown();
            }
         }

         @Override
         public void nodeDown(long eventUID, String nodeID) {
         }
      });
      latch.await(30000, TimeUnit.MILLISECONDS);
   }
}
