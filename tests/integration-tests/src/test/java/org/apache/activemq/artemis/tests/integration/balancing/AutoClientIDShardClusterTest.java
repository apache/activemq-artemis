/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.integration.balancing;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TopicSubscriber;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.balancing.BrokerBalancerConfiguration;
import org.apache.activemq.artemis.core.config.balancing.NamedPropertyConfiguration;
import org.apache.activemq.artemis.core.protocol.openwire.OpenWireProtocolManagerFactory;
import org.apache.activemq.artemis.core.server.balancing.targets.TargetKey;
import org.apache.activemq.artemis.core.server.balancing.targets.TargetKeyResolver;
import org.apache.activemq.artemis.core.server.balancing.transformer.ConsistentHashModulo;
import org.apache.activemq.artemis.core.server.cluster.impl.MessageLoadBalancingType;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.protocol.amqp.broker.ProtonProtocolManagerFactory;
import org.apache.activemq.artemis.utils.Wait;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class AutoClientIDShardClusterTest extends BalancingTestBase {

   @Parameterized.Parameters(name = "protocol: {0}")
   public static Collection<Object[]> data() {
      final String[] protocols = new String[] {AMQP_PROTOCOL, CORE_PROTOCOL, OPENWIRE_PROTOCOL};
      Collection<Object[]> data = new ArrayList<>();
      for (String protocol : protocols) {
         data.add(new Object[] {protocol});
      }
      return data;
   }

   private final String protocol;
   final int numMessages = 50;
   AtomicInteger toSend = new AtomicInteger(numMessages);

   public AutoClientIDShardClusterTest(String protocol) {
      this.protocol = protocol;
   }

   protected void setupServers() throws Exception {
      for (int i = 0; i < 2; i++) {
         setupLiveServer(i, true, HAType.SharedNothingReplication, true, false);
         servers[i].addProtocolManagerFactory(new ProtonProtocolManagerFactory());
         servers[i].addProtocolManagerFactory(new OpenWireProtocolManagerFactory());
      }
      setupClusterConnection("cluster0", name.getMethodName(), MessageLoadBalancingType.ON_DEMAND, 1, true, 0, 1);
      setupClusterConnection("cluster1", name.getMethodName(), MessageLoadBalancingType.ON_DEMAND, 1, true, 1, 0);
      toSend.set(numMessages);
   }

   Runnable producer = new Runnable() {
      final AtomicInteger producerSeq = new AtomicInteger();

      @Override
      public void run() {
         while (toSend.get() > 0) {
            try {
               ConnectionFactory connectionFactory = createFactory(protocol, "producer", "admin", "admin");
               try (Connection connection = connectionFactory.createConnection()) {
                  connection.start();
                  try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                     javax.jms.Topic topic = session.createTopic(name.getMethodName());
                     try (MessageProducer producer = session.createProducer(topic)) {
                        for (int i = 0; i < 10 && toSend.get() > 0; i++) {
                           Message message = session.createTextMessage();
                           message.setIntProperty("SEQ", producerSeq.get() + 1);
                           producer.send(message);
                           producerSeq.incrementAndGet();
                           toSend.decrementAndGet();
                        }
                        TimeUnit.MILLISECONDS.sleep(100);
                     }
                  }
               }
            } catch (Exception ok) {
            }
         }
      }
   };

   class DurableSub implements Runnable {

      final String id;
      int receivedInOrder = -1;
      int lastReceived;
      int maxReceived;
      AtomicBoolean consumerDone = new AtomicBoolean();
      AtomicBoolean orderShot = new AtomicBoolean();
      CountDownLatch registered = new CountDownLatch(1);

      DurableSub(String id) {
         this.id = id;
      }

      @Override
      public void run() {
         while (!consumerDone.get()) {
            try {
               ConnectionFactory connectionFactory = createFactory(protocol, "ClientId-" + id, "admin", "admin");
               Connection connection = null;
               try {
                  connection = connectionFactory.createConnection();
                  connection.start();
                  try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                     javax.jms.Topic topic = session.createTopic(name.getMethodName());
                     try (TopicSubscriber durableSubscriber = session.createDurableSubscriber(topic, "Sub-" + id)) {
                        registered.countDown();
                        for (int i = 0; i < 5; i++) {
                           Message message = durableSubscriber.receive(500);
                           if (message != null) {
                              lastReceived = message.getIntProperty("SEQ");
                              if (lastReceived > maxReceived) {
                                 maxReceived = lastReceived;
                              }
                              if (receivedInOrder < 0) {
                                 receivedInOrder = lastReceived;
                              } else if (receivedInOrder == lastReceived - 1) {
                                 receivedInOrder++;
                              } else {
                                 if (!orderShot.get()) {
                                    System.err.println("Sub: " + id + ", received: out of order " + lastReceived + ", last in order: " + receivedInOrder);
                                 }
                                 orderShot.set(true);
                              }
                           } else {
                              // no point trying again if there is nothing for us now.
                              break;
                           }
                        }
                        TimeUnit.MILLISECONDS.sleep(500);
                     }
                  }
               } finally {
                  if (connection != null) {
                     connection.close(); // seems openwire not jms2.0 auto closable always
                  }
               }
            } catch (Exception ok) {
            }
         }
      }
   }

   @Ignore("not totally reliable, but does show the root cause of the problem being solved")
   public void testWithoutOutSharding() throws Exception {
      setupServers();
      startServers(0, 1);

      // two bouncy durable consumers
      DurableSub sub0 = new DurableSub("0");
      DurableSub sub1 = new DurableSub("1");

      ExecutorService executorService = Executors.newFixedThreadPool(3);
      try {
         executorService.submit(sub0);
         executorService.submit(sub1);

         // waiting for registration before production to give bridges a chance
         assertTrue(sub0.registered.await(20, TimeUnit.SECONDS));
         assertTrue(sub1.registered.await(20, TimeUnit.SECONDS));

         assertTrue(waitForBindings(servers[0], name.getMethodName(), true, 2, -1, 10000));
         assertTrue(waitForBindings(servers[1], name.getMethodName(), true, 2, -1, 10000));

         // wait for remote bindings!
         assertTrue(waitForBindings(servers[0], name.getMethodName(), false, 2, -1, 10000));
         assertTrue(waitForBindings(servers[1], name.getMethodName(), false, 2, -1, 10000));

         // produce a few every second with failover randomize=true so we produce on all nodes
         executorService.submit(producer);

         assertTrue("All sent", Wait.waitFor(() -> toSend.get() == 0));

         assertTrue("All received sub0", Wait.waitFor(() -> sub0.maxReceived == numMessages));

         assertTrue("All received sub1", Wait.waitFor(() -> sub1.maxReceived == numMessages));

         // with bouncing, one 'may' be out of order, hence ignored
         assertTrue(sub0.orderShot.get() || sub1.orderShot.get());

      } finally {
         sub0.consumerDone.set(true);
         sub1.consumerDone.set(true);
         executorService.shutdown();
         stopServers(0, 1);
      }
   }

   @Test
   public void testWithConsistentHashClientIDModTwo() throws Exception {
      setupServers();

      addBalancerWithClientIdConsistentHashMod();

      startServers(0, 1);

      // two bouncy durable consumers
      DurableSub sub0 = new DurableSub("0");
      DurableSub sub1 = new DurableSub("1");

      ExecutorService executorService = Executors.newFixedThreadPool(3);
      try {
         executorService.submit(sub0);
         executorService.submit(sub1);

         // waiting for registration before production to give bridges a chance
         assertTrue(sub0.registered.await(5, TimeUnit.SECONDS));
         assertTrue(sub1.registered.await(5, TimeUnit.SECONDS));

         assertTrue(waitForBindings(servers[0], name.getMethodName(), true, 1, 1, 2000));
         assertTrue(waitForBindings(servers[1], name.getMethodName(), true, 1, 1, 2000));

         // wait for remote bindings!
         assertTrue(waitForBindings(servers[0], name.getMethodName(), false, 1, 1, 10000));
         assertTrue(waitForBindings(servers[1], name.getMethodName(), false, 1, 1, 10000));

         // produce a few every second with failover randomize=true so we produce on all nodes
         executorService.submit(producer);

         assertTrue("All sent", Wait.waitFor(() -> toSend.get() == 0));

         assertTrue("All received sub0", Wait.waitFor(() -> sub0.maxReceived == numMessages));

         assertTrue("All received sub1", Wait.waitFor(() -> sub1.maxReceived == numMessages));

         // with partition, none will be out of order
         assertFalse(sub0.orderShot.get() && sub1.orderShot.get());

      } finally {
         sub0.consumerDone.set(true);
         sub1.consumerDone.set(true);
         executorService.shutdown();
         stopServers(0, 1);
      }
   }

   private void addBalancerWithClientIdConsistentHashMod() {
      final int numberOfNodes = 2;
      for (int node = 0; node < numberOfNodes; node++) {
         Configuration configuration = servers[node].getConfiguration();
         BrokerBalancerConfiguration brokerBalancerConfiguration = new BrokerBalancerConfiguration().setName(BROKER_BALANCER_NAME);
         brokerBalancerConfiguration.setTargetKey(TargetKey.CLIENT_ID).setLocalTargetFilter(TargetKeyResolver.DEFAULT_KEY_VALUE + "|" + node);
         NamedPropertyConfiguration transformerConfig = new NamedPropertyConfiguration();
         transformerConfig.setName(ConsistentHashModulo.NAME);
         HashMap<String, String> properties = new HashMap<>();
         properties.put(ConsistentHashModulo.MODULO, String.valueOf(numberOfNodes));
         transformerConfig.setProperties(properties);
         brokerBalancerConfiguration.setTransformerConfiguration(transformerConfig);

         configuration.setBalancerConfigurations(Collections.singletonList(brokerBalancerConfiguration));

         TransportConfiguration acceptor = getDefaultServerAcceptor(node);
         acceptor.getParams().put("redirect-to", BROKER_BALANCER_NAME);
      }
   }

   protected ConnectionFactory createFactory(String protocol, String clientID, String user, String password) throws Exception {
      StringBuilder urlBuilder = new StringBuilder();

      switch (protocol) {

         case CORE_PROTOCOL: {
            urlBuilder.append("(tcp://localhost:61616,tcp://localhost:61617)?connectionLoadBalancingPolicyClassName=org.apache.activemq.artemis.api.core.client.loadbalance.RandomConnectionLoadBalancingPolicy");
            urlBuilder.append("&clientID=");
            urlBuilder.append(clientID);

            return new ActiveMQConnectionFactory(urlBuilder.toString(), user, password);
         }
         case AMQP_PROTOCOL: {

            urlBuilder.append("failover:(amqp://localhost:61616,amqp://localhost:61617)?failover.randomize=true");
            urlBuilder.append("&jms.clientID=");
            urlBuilder.append(clientID);

            return new JmsConnectionFactory(user, password, urlBuilder.toString());
         }
         case OPENWIRE_PROTOCOL: {

            urlBuilder.append("failover:(tcp://localhost:61616,tcp://localhost:61617)?randomize=true&maxReconnectAttempts=0&startupMaxReconnectAttempts=0");
            urlBuilder.append("&jms.clientID=");
            urlBuilder.append(clientID);

            return new org.apache.activemq.ActiveMQConnectionFactory(user, password, urlBuilder.toString());
         }
         default:
            throw new IllegalStateException("Unexpected value: " + protocol);
      }
   }
}
