/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.hornetq.tests.integration.jms.bridge;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.naming.Context;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.arjuna.ats.arjuna.coordinator.TransactionReaper;
import com.arjuna.ats.arjuna.coordinator.TxControl;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.FailoverEventListener;
import org.hornetq.api.core.client.FailoverEventType;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.api.jms.JMSFactoryType;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.ha.ReplicaPolicyConfiguration;
import org.hornetq.core.config.ha.ReplicatedPolicyConfiguration;
import org.hornetq.core.remoting.impl.invm.TransportConstants;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.jms.bridge.ConnectionFactoryFactory;
import org.hornetq.jms.bridge.DestinationFactory;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.server.JMSServerManager;
import org.hornetq.jms.server.impl.JMSServerManagerImpl;
import org.hornetq.tests.unit.util.InVMContext;
import org.hornetq.tests.util.ServiceTestBase;
import org.junit.After;
import org.junit.Before;

/**
 * A ClusteredBridgeTestBase
 * This class serves as a base class for jms bridge tests in
 * clustered scenarios.
 *
 * @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
 */
public abstract class ClusteredBridgeTestBase extends ServiceTestBase
{
   private static int index = 0;

   protected Map<String, ServerGroup> groups = new HashMap<String, ServerGroup>();

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      Iterator<ServerGroup> iter = groups.values().iterator();
      while (iter.hasNext())
      {
         iter.next().start();
      }
      TxControl.enable();
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      Iterator<ServerGroup> iter = groups.values().iterator();
      while (iter.hasNext())
      {
         iter.next().stop();
      }

      TxControl.disable(true);

      TransactionReaper.terminate(false);

      super.tearDown();
   }

   //create a live/backup pair.
   protected ServerGroup createServerGroup(String name) throws Exception
   {
      ServerGroup server = groups.get(name);
      if (server == null)
      {
         server = new ServerGroup(name, groups.size());
         server.create();
         groups.put(name, server);
      }
      return server;
   }

   //each ServerGroup represents a live/backup pair
   protected class ServerGroup
   {
      private static final int ID_OFFSET = 100;
      private String name;
      private int id;

      private JMSServerManager liveNode;
      private JMSServerManager backupNode;

      private TransportConfiguration liveConnector;
      private TransportConfiguration backupConnector;

      private Context liveContext;

      private ServerLocator locator;
      private ClientSessionFactory sessionFactory;

      /**
       * @param name - name of the group
       * @param id   - id of the live (should be < 100)
       */
      public ServerGroup(String name, int id)
      {
         this.name = name;
         this.id = id;
      }

      public String getName()
      {
         return name;
      }

      public void create() throws Exception
      {
         Map<String, Object> params0 = new HashMap<String, Object>();
         params0.put(TransportConstants.SERVER_ID_PROP_NAME, id);
         liveConnector = new TransportConfiguration(INVM_CONNECTOR_FACTORY, params0, "in-vm-live");

         Map<String, Object> params = new HashMap<String, Object>();
         params.put(TransportConstants.SERVER_ID_PROP_NAME, id + ID_OFFSET);
         backupConnector = new TransportConfiguration(INVM_CONNECTOR_FACTORY, params, "in-vm-backup");

         //live
         Configuration conf0 = createBasicConfig()
            .setJournalDirectory(getJournalDir(id, false))
            .setBindingsDirectory(getBindingsDir(id, false))
            .addAcceptorConfiguration(new TransportConfiguration(INVM_ACCEPTOR_FACTORY, params0))
            .addConnectorConfiguration(liveConnector.getName(), liveConnector)
            .setHAPolicyConfiguration(new ReplicatedPolicyConfiguration())
            .addClusterConfiguration(basicClusterConnectionConfig(liveConnector.getName()));

         HornetQServer server0 = addServer(HornetQServers.newHornetQServer(conf0, true));

         liveContext = new InVMContext();
         liveNode = new JMSServerManagerImpl(server0);
         liveNode.setContext(liveContext);

         //backup
         Configuration conf = createBasicConfig()
            .setJournalDirectory(getJournalDir(id, true))
            .setBindingsDirectory(getBindingsDir(id, true))
            .addAcceptorConfiguration(new TransportConfiguration(INVM_ACCEPTOR_FACTORY, params))
            .addConnectorConfiguration(backupConnector.getName(), backupConnector)
            .addConnectorConfiguration(liveConnector.getName(), liveConnector)
            .setHAPolicyConfiguration(new ReplicaPolicyConfiguration())
            .addClusterConfiguration(basicClusterConnectionConfig(backupConnector.getName(), liveConnector.getName()));

         HornetQServer backup = addServer(HornetQServers.newHornetQServer(conf, true));

         Context context = new InVMContext();

         backupNode = new JMSServerManagerImpl(backup);
         backupNode.setContext(context);
      }

      public void start() throws Exception
      {
         liveNode.start();
         waitForServer(liveNode.getHornetQServer());
         backupNode.start();
         waitForRemoteBackupSynchronization(backupNode.getHornetQServer());

         locator = HornetQClient.createServerLocatorWithHA(liveConnector);
         locator.setReconnectAttempts(-1);
         sessionFactory = locator.createSessionFactory();
      }

      public void stop() throws Exception
      {
         sessionFactory.close();
         locator.close();
         liveNode.stop();
         backupNode.stop();
      }

      public void createQueue(String queueName) throws Exception
      {
         liveNode.createQueue(true, queueName, null, true, "/queue/" + queueName);
      }

      public ConnectionFactoryFactory getConnectionFactoryFactory()
      {
         ConnectionFactoryFactory cff = new ConnectionFactoryFactory()
         {
            public ConnectionFactory createConnectionFactory() throws Exception
            {
               HornetQConnectionFactory cf = HornetQJMSClient.createConnectionFactoryWithHA(JMSFactoryType.XA_CF,
                                                                                            liveConnector);
               cf.getServerLocator().setReconnectAttempts(-1);
               return cf;
            }
         };

         return cff;
      }

      public DestinationFactory getDestinationFactory(final String queueName)
      {

         DestinationFactory destFactory = new DestinationFactory()
         {
            public Destination createDestination() throws Exception
            {
               return (Destination) liveContext.lookup("/queue/" + queueName);
            }
         };
         return destFactory;
      }

      public void sendMessages(String queueName, int num) throws HornetQException
      {
         ClientSession session = sessionFactory.createSession();
         ClientProducer producer = session.createProducer("jms.queue." + queueName);
         for (int i = 0; i < num; i++)
         {
            ClientMessage m = session.createMessage(true);
            m.putStringProperty("bridge-message", "hello " + index);
            index++;
            producer.send(m);
         }
         session.close();
      }

      public void receiveMessages(String queueName, int num, boolean checkDup) throws HornetQException
      {
         ClientSession session = sessionFactory.createSession();
         session.start();
         ClientConsumer consumer = session.createConsumer("jms.queue." + queueName);
         for (int i = 0; i < num; i++)
         {
            ClientMessage m = consumer.receive(30000);
            assertNotNull("i=" + i, m);
            assertNotNull(m.getStringProperty("bridge-message"));
            m.acknowledge();
         }

         ClientMessage m = consumer.receive(500);
         if (checkDup)
         {
            assertNull(m);
         }
         else
         {
            //drain messages
            while (m != null)
            {
               m = consumer.receive(200);
            }
         }

         session.close();
      }

      public void crashLive() throws Exception
      {
         final CountDownLatch latch = new CountDownLatch(1);
         sessionFactory.addFailoverListener(new FailoverEventListener()
         {

            @Override
            public void failoverEvent(FailoverEventType eventType)
            {
               if (eventType == FailoverEventType.FAILOVER_COMPLETED)
               {
                  latch.countDown();
               }
            }
         });

         liveNode.getHornetQServer().stop();

         boolean ok = latch.await(10000, TimeUnit.MILLISECONDS);
         assertTrue(ok);
      }
   }
}
