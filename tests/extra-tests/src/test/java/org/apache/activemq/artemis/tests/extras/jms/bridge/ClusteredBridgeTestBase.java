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
package org.apache.activemq.artemis.tests.extras.jms.bridge;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.arjuna.ats.arjuna.coordinator.TransactionReaper;
import com.arjuna.ats.arjuna.coordinator.TxControl;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.FailoverEventListener;
import org.apache.activemq.artemis.api.core.client.FailoverEventType;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.api.jms.JMSFactoryType;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.ha.ReplicaPolicyConfiguration;
import org.apache.activemq.artemis.core.config.ha.ReplicatedPolicyConfiguration;
import org.apache.activemq.artemis.core.remoting.impl.invm.TransportConstants;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.jms.bridge.ConnectionFactoryFactory;
import org.apache.activemq.artemis.jms.bridge.DestinationFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQDestination;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.After;
import org.junit.Before;

/**
 * A ClusteredBridgeTestBase
 * This class serves as a base class for jms bridge tests in
 * clustered scenarios.
 */
public abstract class ClusteredBridgeTestBase extends ActiveMQTestBase {

   private static int index = 0;
   private static final int QUORUM_VOTE_WAIT_TIME_SEC = 30;

   protected Map<String, ServerGroup> groups = new HashMap<>();

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
      Iterator<ServerGroup> iter = groups.values().iterator();
      while (iter.hasNext()) {
         iter.next().start();
      }
      TxControl.enable();
   }

   @Override
   @After
   public void tearDown() throws Exception {
      Iterator<ServerGroup> iter = groups.values().iterator();
      while (iter.hasNext()) {
         iter.next().stop();
      }

      TxControl.disable(true);

      TransactionReaper.terminate(false);

      super.tearDown();
   }

   //create a live/backup pair.
   protected ServerGroup createServerGroup(String name) throws Exception {
      ServerGroup server = groups.get(name);
      if (server == null) {
         server = new ServerGroup(name, groups.size());
         server.create();
         groups.put(name, server);
      }
      return server;
   }

   //each ServerGroup represents a live/backup pair
   protected class ServerGroup {

      private static final int ID_OFFSET = 100;
      private String name;
      private int id;

      private ActiveMQServer liveNode;
      private ActiveMQServer backupNode;

      private TransportConfiguration liveConnector;
      private TransportConfiguration backupConnector;

      private ServerLocator locator;
      private ClientSessionFactory sessionFactory;

      /**
       * @param name - name of the group
       * @param id   - id of the live (should be < 100)
       */
      public ServerGroup(String name, int id) {
         this.name = name;
         this.id = id;
      }

      public String getName() {
         return name;
      }

      public void create() throws Exception {
         Map<String, Object> params0 = new HashMap<>();
         params0.put(TransportConstants.SERVER_ID_PROP_NAME, id);
         liveConnector = new TransportConfiguration(INVM_CONNECTOR_FACTORY, params0, "in-vm-live");

         Map<String, Object> params = new HashMap<>();
         params.put(TransportConstants.SERVER_ID_PROP_NAME, id + ID_OFFSET);
         backupConnector = new TransportConfiguration(INVM_CONNECTOR_FACTORY, params, "in-vm-backup");

         //live
         Configuration conf0 = createBasicConfig().setJournalDirectory(getJournalDir(id, false)).setBindingsDirectory(getBindingsDir(id, false)).addAcceptorConfiguration(new TransportConfiguration(INVM_ACCEPTOR_FACTORY, params0)).addConnectorConfiguration(liveConnector.getName(), liveConnector).setHAPolicyConfiguration(new ReplicatedPolicyConfiguration()).addClusterConfiguration(basicClusterConnectionConfig(liveConnector.getName()));

         liveNode = addServer(ActiveMQServers.newActiveMQServer(conf0, true));

         //backup
         ReplicaPolicyConfiguration replicaPolicyConfiguration = new ReplicaPolicyConfiguration();
         replicaPolicyConfiguration.setQuorumVoteWait(QUORUM_VOTE_WAIT_TIME_SEC);
         Configuration config = createBasicConfig().setJournalDirectory(getJournalDir(id, true)).setBindingsDirectory(getBindingsDir(id, true)).addAcceptorConfiguration(new TransportConfiguration(INVM_ACCEPTOR_FACTORY, params)).addConnectorConfiguration(backupConnector.getName(), backupConnector).addConnectorConfiguration(liveConnector.getName(), liveConnector).setHAPolicyConfiguration(replicaPolicyConfiguration).addClusterConfiguration(basicClusterConnectionConfig(backupConnector.getName(), liveConnector.getName()));

         backupNode = addServer(ActiveMQServers.newActiveMQServer(config, true));
      }

      public void start() throws Exception {
         liveNode.start();
         waitForServerToStart(liveNode);
         backupNode.start();
         waitForRemoteBackupSynchronization(backupNode);

         locator = ActiveMQClient.createServerLocatorWithHA(liveConnector).setReconnectAttempts(15);
         sessionFactory = locator.createSessionFactory();
      }

      public void stop() throws Exception {
         sessionFactory.close();
         locator.close();
         liveNode.stop();
         backupNode.stop();
      }

      public void createQueue(String queueName) throws Exception {
         liveNode.createQueue(new QueueConfiguration(queueName).setRoutingType(RoutingType.ANYCAST));
      }

      public ConnectionFactoryFactory getConnectionFactoryFactory() {
         ConnectionFactoryFactory cff = new ConnectionFactoryFactory() {
            @Override
            public ConnectionFactory createConnectionFactory() throws Exception {
               ActiveMQConnectionFactory cf = ActiveMQJMSClient.createConnectionFactoryWithHA(JMSFactoryType.XA_CF, liveConnector);
               cf.getServerLocator().setReconnectAttempts(15);
               return cf;
            }
         };

         return cff;
      }

      public DestinationFactory getDestinationFactory(final String queueName) {

         DestinationFactory destFactory = new DestinationFactory() {
            @Override
            public Destination createDestination() throws Exception {
               return ActiveMQDestination.createDestination(queueName, ActiveMQDestination.TYPE.QUEUE);
            }
         };
         return destFactory;
      }

      public void sendMessages(String queueName, int num) throws ActiveMQException {
         ClientSession session = sessionFactory.createSession();
         ClientProducer producer = session.createProducer(queueName);
         for (int i = 0; i < num; i++) {
            ClientMessage m = session.createMessage(true);
            m.putStringProperty("bridge-message", "hello " + index);
            index++;
            producer.send(m);
         }
         session.close();
      }

      public void receiveMessages(String queueName, int num, boolean checkDup) throws ActiveMQException {
         ClientSession session = sessionFactory.createSession();
         session.start();
         ClientConsumer consumer = session.createConsumer(queueName);
         for (int i = 0; i < num; i++) {
            ClientMessage m = consumer.receive(30000);
            assertNotNull("i=" + i, m);
            assertNotNull(m.getStringProperty("bridge-message"));
            m.acknowledge();
         }

         ClientMessage m = consumer.receive(500);
         if (checkDup) {
            assertNull(m);
         } else {
            //drain messages
            while (m != null) {
               m = consumer.receive(200);
            }
         }

         session.close();
      }

      public void crashLive() throws Exception {
         final CountDownLatch latch = new CountDownLatch(1);
         sessionFactory.addFailoverListener(new FailoverEventListener() {

            @Override
            public void failoverEvent(FailoverEventType eventType) {
               if (eventType == FailoverEventType.FAILOVER_COMPLETED) {
                  latch.countDown();
               }
            }
         });

         liveNode.stop();

         boolean ok = latch.await(10000, TimeUnit.MILLISECONDS);
         assertTrue(ok);
      }
   }
}
