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
package org.hornetq.tests.integration.cluster.failover;


import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.SendAcknowledgementHandler;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.config.BackupStrategy;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.CoreQueueConfiguration;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.Bindings;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.NodeManager;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.cluster.ClusterConnection;
import org.hornetq.core.server.cluster.ha.HAPolicy;
import org.hornetq.core.server.impl.HornetQServerImpl;
import org.hornetq.core.server.impl.InVMNodeManager;
import org.hornetq.core.server.impl.QueueImpl;
import org.hornetq.core.settings.impl.AddressFullMessagePolicy;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.core.transaction.impl.XidImpl;
import org.hornetq.tests.integration.cluster.util.SameProcessHornetQServer;
import org.hornetq.tests.integration.cluster.util.TestableServer;
import org.hornetq.tests.util.ColocatedHornetQServer;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.UUIDGenerator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ColocatedFailoverTest extends ServiceTestBase
{
   private TestableServer liveServer1;

   private TestableServer liveServer2;

   protected NodeManager nodeManagerLive1;

   protected NodeManager nodeManagerLive2;

   private ServerLocator locator;

   private SimpleString queue;

   private SimpleString topic;

   private ClientSessionFactory factory1;

   private ClientSessionFactory factory2;

   private ClientSession session1;

   private ClientSession session2;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      deleteDirectory(new File(getTestDir() + ""));
      createConfigs();

      liveServer1.start();
      liveServer2.start();
      waitForServer(liveServer1.getServer());
      waitForServer(liveServer2.getServer());

      HashMap<String, Object> params = new HashMap<>();
      params.put("server-id", "1");
      TransportConfiguration transportConfiguration1 = new TransportConfiguration(INVM_CONNECTOR_FACTORY, params);
      locator = HornetQClient.createServerLocator(true, transportConfiguration1);
      locator.setReconnectAttempts(-1);
      locator.setConfirmationWindowSize(0);
      factory1 = locator.createSessionFactory(transportConfiguration1);

      HashMap<String, Object> params2 = new HashMap<>();
      params2.put("server-id", "2");

      TransportConfiguration transportConfiguration2 = new TransportConfiguration(INVM_CONNECTOR_FACTORY, params2);
      factory2 = locator.createSessionFactory(transportConfiguration2);

      session1 = factory1.createSession(false, true, true);
      session2 = factory2.createSession(false, true, true);
      queue = new SimpleString("jms.queue.testQueue");
      topic = new SimpleString("jms.topic.testTopic");
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      if (locator != null)
         locator.close();
      liveServer1.stop();
      liveServer2.stop();
      super.tearDown();
   }

   @Test
   public void testSend() throws Exception
   {
      int numMessages = 100;
      ClientProducer producer = session1.createProducer();
      final CountDownLatch latch = new CountDownLatch(numMessages);
      ClientConsumer consumer = session2.createConsumer(queue);
      session2.start();
      session1.setSendAcknowledgementHandler(new SendAcknowledgementHandler()
      {
         @Override
         public void sendAcknowledged(org.hornetq.api.core.Message message)
         {

            latch.countDown();
         }
      });
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session1.createMessage(true);
         message.getBodyBuffer().writeString("message:" + i);
         producer.send(queue, message);
      }
      latch.await(10, TimeUnit.SECONDS);
      System.out.println(locator.getTopology().describe());
      liveServer1.crash(true, session1);
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage cMessage = consumer.receive(5000);
         assertNotNull(cMessage.getBodyBuffer().readString());
         //NB we dont test for order as they will be round robined and out of order
      }
   }

   @Test
   public void testSendPagingOnReload() throws Exception
   {
      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setMaxSizeBytes(5000);
      addressSettings.setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);
      addressSettings.setPageSizeBytes(1000);
      String pagedQueue = "jms.queue.pagedQueue";
      SimpleString ssPagedQueue = new SimpleString(pagedQueue);
      liveServer2.getServer().getAddressSettingsRepository().addMatch(pagedQueue, addressSettings);
      session1.createQueue(pagedQueue, pagedQueue, true);
      ClientProducer producer = session1.createProducer();
      final CountDownLatch latch = new CountDownLatch(2);
      session1.setSendAcknowledgementHandler(new SendAcknowledgementHandler()
      {
         @Override
         public void sendAcknowledged(org.hornetq.api.core.Message message)
         {

            latch.countDown();
         }
      });
      byte[] bytes = new byte[10000];
      ClientMessage message = session1.createMessage(true);
      message.getBodyBuffer().writeBytes(bytes);
      producer.send(ssPagedQueue, message);
      message = session1.createMessage(true);
      message.getBodyBuffer().writeBytes(bytes);
      producer.send(ssPagedQueue, message);
      latch.await(10, TimeUnit.SECONDS);
      liveServer1.crash(true, session1);
      ColocatedHornetQServer qServer = (ColocatedHornetQServer) liveServer2.getServer();
      qServer.backupServer.waitForActivation(5, TimeUnit.SECONDS);
   }

   @Test
   public void testLargeMessage() throws Exception
   {
      ClientProducer producer = session1.createProducer();
      final CountDownLatch latch = new CountDownLatch(2);
      session1.setSendAcknowledgementHandler(new SendAcknowledgementHandler()
      {
         @Override
         public void sendAcknowledged(org.hornetq.api.core.Message message)
         {

            latch.countDown();
         }
      });
      ClientMessage message = session1.createMessage(true);

      message.setBodyInputStream(UnitTestCase.createFakeLargeStream(10000));
      producer.send(queue, message);
      ClientConsumer consumer = session1.createConsumer(queue);
      session1.start();
      message = consumer.receive(5000);
      assertNotNull(message);
      assertTrue(message.isLargeMessage());
      message.getBodyBuffer().readBytes(10000);
      session1.close();
      liveServer1.crash(true, session1);
      consumer = session2.createConsumer(queue);
      session2.start();
      message = consumer.receive(5000);
      assertNotNull(message);
      assertTrue(message.isLargeMessage());
      message.getBodyBuffer().readBytes(10000);
   }

   @Test
   public void testSendScheduled() throws Exception
   {
      ClientProducer producer = session1.createProducer();
      final CountDownLatch latch = new CountDownLatch(1);
      ClientConsumer consumer = session2.createConsumer(queue);
      session2.start();
      session1.setSendAcknowledgementHandler(new SendAcknowledgementHandler()
      {
         @Override
         public void sendAcknowledged(org.hornetq.api.core.Message message)
         {

            latch.countDown();
         }
      });
      ClientMessage message = session1.createMessage(true);
      long time = System.currentTimeMillis() + 10000;
      message.putLongProperty(Message.HDR_SCHEDULED_DELIVERY_TIME, time);
      message.getBodyBuffer().writeString("message:1");
      producer.send(queue, message);
      latch.await(10, TimeUnit.SECONDS);

      liveServer1.crash(true, session1);
      ClientMessage cMessage = consumer.receive(10000);
      assertNotNull(cMessage);
      assertTrue(System.currentTimeMillis() >= time);
      assertNotNull(cMessage.getBodyBuffer().readString());
   }


   @Test
   public void testSendQueueNotExistOnLive() throws Exception
   {
      int numMessages = 100;
      ClientProducer producer = session1.createProducer();
      final CountDownLatch latch = new CountDownLatch(numMessages);
      SimpleString dur1 = new SimpleString("myDurSub1");
      session1.createQueue(topic, dur1, true);
      SimpleString dur2 = new SimpleString("myDurSub2");
      session2.createQueue(topic, dur2, true);
      waitForBindings(liveServer1.getServer(), topic.toString(), false, 2, 0, 5000L);
      waitForBindings(liveServer2.getServer(), topic.toString(), false, 2, 0, 5000L);
      session1.setSendAcknowledgementHandler(new SendAcknowledgementHandler()
      {
         @Override
         public void sendAcknowledged(org.hornetq.api.core.Message message)
         {
            latch.countDown();
         }
      });
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session1.createMessage(true);
         message.getBodyBuffer().writeString("message:" + i);
         producer.send(topic, message);
      }
      latch.await(10, TimeUnit.SECONDS);
      liveServer1.crash(true, session1);

      ColocatedHornetQServer qServer = (ColocatedHornetQServer) liveServer2.getServer();
      qServer.backupServer.waitForActivation(5, TimeUnit.SECONDS);
      ClientConsumer consumer = session2.createConsumer(dur1);
      ClientConsumer consumer2 = session2.createConsumer(dur2);
      session2.start();
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage cMessage = consumer.receive(5000);
         assertNotNull(cMessage.getBodyBuffer().readString());
      }
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage cMessage = consumer2.receive(5000);
         String s = cMessage.getBodyBuffer().readString();
         System.out.println("s = " + s);
         assertNotNull(s);
      }
   }

   @Test
   public void testSendQueueMessageStillInForwardQueue() throws Exception
   {
      int numMessages = 100;
      ClientProducer producer = session1.createProducer();
      final CountDownLatch latch = new CountDownLatch(numMessages);
      SimpleString dur1 = new SimpleString("myDurSub1");
      session2.createQueue(topic, dur1, true);
      waitForBindings(liveServer1.getServer(), topic.toString(), false, 2, 0, 5000L);
      waitForBindings(liveServer2.getServer(), topic.toString(), false, 1, 0, 5000L);
      session1.setSendAcknowledgementHandler(new SendAcknowledgementHandler()
      {
         @Override
         public void sendAcknowledged(org.hornetq.api.core.Message message)
         {
            latch.countDown();
         }
      });
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session1.createMessage(true);
         message.getBodyBuffer().writeString("message:" + i);
         producer.send(topic, message);
      }
      latch.await(10, TimeUnit.SECONDS);
      //now disconnect the bridge but readd the binding so we can make sure some messages get marooned
      Bindings bindingsForAddress = liveServer1.getServer().getPostOffice().getBindingsForAddress(topic);
      Collection<Binding> bindings = bindingsForAddress.getBindings();
      Binding binding = null;
      for (Binding thebinding : bindings)
      {
         if (thebinding.getRoutingName().equals(dur1))
         {
            binding = thebinding;
            break;
         }
      }

      assertNotNull(binding);
      Set<ClusterConnection> clusterConnections = liveServer1.getServer().getClusterManager().getClusterConnections();
      for (ClusterConnection clusterConnection : clusterConnections)
      {
         clusterConnection.stop();
      }
      liveServer1.getServer().getPostOffice().addBinding(binding);
      for (int i = 100; i < numMessages + 100; i++)
      {
         ClientMessage message = session1.createMessage(true);
         message.getBodyBuffer().writeString("message:" + i);
         producer.send(topic, message);
      }
      liveServer1.crash(true, session1);

      ColocatedHornetQServer qServer = (ColocatedHornetQServer) liveServer2.getServer();
      qServer.backupServer.waitForActivation(5, TimeUnit.SECONDS);
      ClientConsumer consumer = session2.createConsumer(dur1);
      session2.start();
      for (int i = 0; i < numMessages * 2; i++)
      {
         ClientMessage cMessage = consumer.receive(5000);
         assertNotNull(cMessage.getBodyBuffer().readString());
      }
   }

   @Test
   public void testSendQueueMessageStillInForwardQueue2Queues() throws Exception
   {
      int numMessages = 100;
      ClientProducer producer = session1.createProducer();
      final CountDownLatch latch = new CountDownLatch(numMessages);
      SimpleString dur1 = new SimpleString("myDurSub1");
      session2.createQueue(topic, dur1, true);
      SimpleString dur2 = new SimpleString("myDurSub2");
      session1.createQueue(topic, dur2, true);
      waitForBindings(liveServer1.getServer(), topic.toString(), false, 2, 0, 5000L);
      waitForBindings(liveServer2.getServer(), topic.toString(), false, 2, 0, 5000L);
      session1.setSendAcknowledgementHandler(new SendAcknowledgementHandler()
      {
         @Override
         public void sendAcknowledged(org.hornetq.api.core.Message message)
         {
            latch.countDown();
         }
      });
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session1.createMessage(true);
         message.getBodyBuffer().writeString("message:" + i);
         producer.send(topic, message);
      }
      latch.await(10, TimeUnit.SECONDS);
      //now disconnect the bridge but readd the binding so we can make sure some messages get marooned
      Bindings bindingsForAddress = liveServer1.getServer().getPostOffice().getBindingsForAddress(topic);
      Collection<Binding> bindings = bindingsForAddress.getBindings();
      Binding binding = null;
      for (Binding thebinding : bindings)
      {
         if (thebinding.getRoutingName().equals(dur1))
         {
            binding = thebinding;
            break;
         }
      }

      assertNotNull(binding);
      Set<ClusterConnection> clusterConnections = liveServer1.getServer().getClusterManager().getClusterConnections();
      for (ClusterConnection clusterConnection : clusterConnections)
      {
         clusterConnection.stop();
      }
      liveServer1.getServer().getPostOffice().addBinding(binding);
      for (int i = 100; i < numMessages + 100; i++)
      {
         ClientMessage message = session1.createMessage(true);
         message.getBodyBuffer().writeString("message:" + i);
         producer.send(topic, message);
      }
      liveServer1.crash(true, session1);

      ColocatedHornetQServer qServer = (ColocatedHornetQServer) liveServer2.getServer();
      qServer.backupServer.waitForActivation(5, TimeUnit.SECONDS);
      ClientConsumer consumer = session2.createConsumer(dur1);
      session2.start();
      for (int i = 0; i < numMessages * 2; i++)
      {
         ClientMessage cMessage = consumer.receive(5000);
         String s = cMessage.getBodyBuffer().readString();
         System.out.println("s = " + s);
         assertNotNull(s);
      }
      consumer = session2.createConsumer(dur2);
      for (int i = 0; i < numMessages * 2; i++)
      {
         ClientMessage cMessage = consumer.receive(5000);
         String s = cMessage.getBodyBuffer().readString();
         System.out.println("s = " + s);
         assertNotNull(s);
      }
   }

   @Test
   public void testSendQueueMessageStillInForwardQueue3Servers() throws Exception
   {
      TransportConfiguration liveConnector1 = getConnectorTransportConfiguration(1);
      TransportConfiguration liveConnector3 = getConnectorTransportConfiguration(3);
      Configuration liveConfiguration3 = super.createDefaultConfig();
      liveConfiguration3.getAcceptorConfigurations().clear();
      liveConfiguration3.getAcceptorConfigurations().add(getAcceptorTransportConfiguration(3));
      liveConfiguration3.getHAPolicy().setPolicyType(HAPolicy.POLICY_TYPE.SHARED_STORE);
      liveConfiguration3.getHAPolicy().setFailbackDelay(1000);
      liveConfiguration3.setJournalDirectory(getTestDir() + "/journal3");
      liveConfiguration3.setBindingsDirectory(getTestDir() + "/bindings3");
      liveConfiguration3.setLargeMessagesDirectory(getTestDir() + "/largemessage3");
      liveConfiguration3.setPagingDirectory(getTestDir() + "/paging3");
      liveConfiguration3.getQueueConfigurations().add(new CoreQueueConfiguration("jms.queue.testQueue", "jms.queue.testQueue", null, true));
      liveConfiguration3.getQueueConfigurations().add(new CoreQueueConfiguration("jms.topic.testTopic", "jms.topic.testTopic", HornetQServerImpl.GENERIC_IGNORED_FILTER, true));

      basicClusterConnectionConfig(liveConfiguration3, liveConnector3.getName(), liveConnector1.getName());
      liveConfiguration3.getConnectorConfigurations().put(liveConnector1.getName(), liveConnector1);
      liveConfiguration3.getConnectorConfigurations().put(liveConnector3.getName(), liveConnector3);

      HornetQServer server3 = new HornetQServerImpl(liveConfiguration3);
      server3.setIdentity("server3");
      try
      {
         server3.start();
         waitForServer(server3);

         ClientSessionFactory sessionFactory3 = locator.createSessionFactory(liveConnector3);
         ClientSession session3 = sessionFactory3.createSession();
         SimpleString dursub3 = new SimpleString("dursub3");
         session3.createQueue(topic, dursub3, true);

         int numMessages = 100;
         ClientProducer producer = session1.createProducer();
         final CountDownLatch latch = new CountDownLatch(numMessages);
         SimpleString dur1 = new SimpleString("myDurSub1");
         session2.createQueue(topic, dur1, true);
         waitForBindings(liveServer1.getServer(), topic.toString(), false, 4, 0, 5000L);
         waitForBindings(liveServer2.getServer(), topic.toString(), false, 3, 0, 5000L);
         session1.setSendAcknowledgementHandler(new SendAcknowledgementHandler()
         {
            @Override
            public void sendAcknowledged(org.hornetq.api.core.Message message)
            {
               latch.countDown();
            }
         });
         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage message = session1.createMessage(true);
            message.getBodyBuffer().writeString("message:" + i);
            producer.send(topic, message);
         }
         latch.await(10, TimeUnit.SECONDS);
         //now disconnect the bridge but readd the binding so we can make sure some messages get marooned
         Bindings bindingsForAddress = liveServer1.getServer().getPostOffice().getBindingsForAddress(topic);
         Collection<Binding> bindings = bindingsForAddress.getBindings();
         Binding binding = null;
         for (Binding thebinding : bindings)
         {
            if (thebinding.getRoutingName().equals(dur1))
            {
               binding = thebinding;
               break;
            }
         }

         assertNotNull(binding);

         Binding binding2 = null;
         for (Binding thebinding : bindings)
         {
            if (thebinding.getRoutingName().equals(dursub3))
            {
               binding2 = thebinding;
               break;
            }
         }

         assertNotNull(binding2);

         Set<ClusterConnection> clusterConnections = liveServer1.getServer().getClusterManager().getClusterConnections();
         for (ClusterConnection clusterConnection : clusterConnections)
         {
            clusterConnection.stop();
         }
         liveServer1.getServer().getPostOffice().addBinding(binding);
         liveServer1.getServer().getPostOffice().addBinding(binding2);
         for (int i = 100; i < numMessages + 100; i++)
         {
            ClientMessage message = session1.createMessage(true);
            message.getBodyBuffer().writeString("message:" + i);
            producer.send(topic, message);
         }
         liveServer1.crash(true, session1);

         ColocatedHornetQServer qServer = (ColocatedHornetQServer) liveServer2.getServer();
         qServer.backupServer.waitForActivation(5, TimeUnit.SECONDS);
         ClientConsumer consumer = session2.createConsumer(dur1);
         session2.start();
         for (int i = 0; i < numMessages * 2; i++)
         {
            ClientMessage cMessage = consumer.receive(5000);
            assertNotNull(cMessage.getBodyBuffer().readString());
         }
         consumer = session3.createConsumer(dursub3);
         session3.start();
         for (int i = 0; i < numMessages * 2; i++)
         {
            ClientMessage cMessage = consumer.receive(5000);
            assertNotNull(cMessage.getBodyBuffer().readString());
         }
      }
      finally
      {
         server3.stop();
      }
   }

   @Test
   public void testSendTransacted() throws Exception
   {
      int numMessages = 100;
      try
      (
         ClientSession session = factory1.createSession(true, false, false)
      )
      {
         ClientProducer producer = session.createProducer();
         Xid xid = new XidImpl("bq1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());
         session.start(xid, XAResource.TMNOFLAGS);
         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage message = session1.createMessage(true);
            message.getBodyBuffer().writeString("message:" + i);
            producer.send(queue, message);
            System.out.println("i = " + i);
         }
         session.end(xid, XAResource.TMSUCCESS);
         session.prepare(xid);
         liveServer1.crash(true, session);
         ColocatedHornetQServer qServer = (ColocatedHornetQServer) liveServer2.getServer();
         qServer.backupServer.waitForActivation(5, TimeUnit.SECONDS);
         try
         (
               ClientSession session2 = factory2.createSession(true, true, true)
         )
         {
            session2.getXAResource().commit(xid, false);
         }
      }
      ClientConsumer consumer = session2.createConsumer(queue);
      session2.start();
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage cMessage = consumer.receive(5000);
         assertNotNull(cMessage.getBodyBuffer().readString());
         //NB we dont test for order as they will be round robined and out of order
      }
   }

   @Test
   public void testReceiveTransacted() throws Exception
   {
      int numMessages = 100;
      ClientProducer producer = session1.createProducer();
      ClientSession session = factory1.createSession(true, false, false);
      ClientConsumer consumer = session.createConsumer(queue);
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session1.createMessage(true);
         message.getBodyBuffer().writeString("message:" + i);
         producer.send(queue, message);
         System.out.println("i = " + i);
      }
      producer.close();

      Xid xid = new XidImpl("bq1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());
      session.start(xid, XAResource.TMNOFLAGS);
      session.start();
      for (int i = 0; i < numMessages / 2; i++)
      {
         ClientMessage cMessage = consumer.receive(5000);
         cMessage.acknowledge();
      }
      session.end(xid, XAResource.TMSUCCESS);
      session.prepare(xid);
      session.close();
      liveServer1.crash(true, session);
      ColocatedHornetQServer qServer = (ColocatedHornetQServer) liveServer2.getServer();
      qServer.backupServer.waitForActivation(5, TimeUnit.SECONDS);
      try
         (
            ClientSession session2 = factory2.createSession(true, true, true)
         )
      {
         session2.getXAResource().commit(xid, false);
      }
   }

   @Test
   public void testSendAndReceiveSameQueueTransacted() throws Exception
   {
      ClientProducer producer = session1.createProducer();
      ClientSession session = factory1.createSession(true, false, false);
      ClientConsumer consumer = session.createConsumer(queue);
      ClientMessage message = session1.createMessage(true);
      message.getBodyBuffer().writeString("message:1");
      producer.send(queue, message);
      producer.close();

      Xid xid = new XidImpl("bq1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());
      session.start(xid, XAResource.TMNOFLAGS);
      session.start();
      ClientMessage cMessage = consumer.receive(5000);
      cMessage.acknowledge();
      session.end(xid, XAResource.TMSUCCESS);
      session.prepare(xid);
      session.close();
      liveServer1.crash(true, session);
      ColocatedHornetQServer qServer = (ColocatedHornetQServer) liveServer2.getServer();
      qServer.backupServer.waitForActivation(5, TimeUnit.SECONDS);
      try
      (
            ClientSession session2 = factory2.createSession(true, false, false)
      )
      {
         session2.getXAResource().commit(xid, false);
      }
      Queue q = (Queue) liveServer2.getServer().getPostOffice().getBinding(queue).getBindable();
      assertEquals(q.getMessageCount(), 0);
      assertEquals(q.getMessagesAdded(), 1);
   }

   @Test
   public void testSendAndReceiveSameQueueTransactedRollback() throws Exception
   {
      ClientProducer producer = session1.createProducer();
      ClientSession session = factory1.createSession(true, false, false);
      ClientConsumer consumer = session.createConsumer(queue);
      ClientMessage message = session1.createMessage(true);
      message.getBodyBuffer().writeString("message:1");
      producer.send(queue, message);
      producer.close();

      Xid xid = new XidImpl("bq1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());
      session.start(xid, XAResource.TMNOFLAGS);
      session.start();
      ClientMessage cMessage = consumer.receive(5000);
      cMessage.acknowledge();
      session.end(xid, XAResource.TMSUCCESS);
      session.prepare(xid);
      session.close();
      liveServer1.crash(true, session);
      ColocatedHornetQServer qServer = (ColocatedHornetQServer) liveServer2.getServer();
      qServer.backupServer.waitForActivation(5, TimeUnit.SECONDS);
      try
            (
                  ClientSession session2 = factory2.createSession(true, false, false)
            )
      {
         session2.getXAResource().rollback(xid);
      }
      Queue q = (Queue) liveServer2.getServer().getPostOffice().getBinding(queue).getBindable();
      assertEquals(q.getMessageCount(), 1);
      assertEquals(q.getMessagesAdded(), 1);
      ClientConsumer consumer1 = session2.createConsumer(queue);
      session2.start();
      ClientMessage clientMessage = consumer1.receiveImmediate();
      assertNotNull(clientMessage);
      String s = clientMessage.getBodyBuffer().readString();
      assertEquals(s, "message:1");
   }

   @Test
   public void testSendAndReceiveSameQueueTransactedRollbackRestart() throws Exception
   {
      ClientProducer producer = session1.createProducer();
      ClientSession session = factory1.createSession(true, false, false);
      ClientConsumer consumer = session.createConsumer(queue);
      ClientMessage message = session1.createMessage(true);
      message.getBodyBuffer().writeString("message:1");
      producer.send(queue, message);
      producer.close();

      Xid xid = new XidImpl("bq1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());
      session.start(xid, XAResource.TMNOFLAGS);
      session.start();
      ClientMessage cMessage = consumer.receive(5000);
      cMessage.acknowledge();
      session.end(xid, XAResource.TMSUCCESS);
      session.prepare(xid);
      session.close();
      liveServer1.crash(true, session);
      ColocatedHornetQServer qServer = (ColocatedHornetQServer) liveServer2.getServer();
      qServer.backupServer.waitForActivation(5, TimeUnit.SECONDS);
      try
            (
                  ClientSession session2 = factory2.createSession(true, false, false)
            )
      {
         session2.getXAResource().rollback(xid);
      }
      Queue q = (Queue) liveServer2.getServer().getPostOffice().getBinding(queue).getBindable();
      assertEquals(q.getMessageCount(), 1);
      assertEquals(q.getMessagesAdded(), 1);
      liveServer2.stop();
      liveServer2.start();
      ClientConsumer consumer1 = session2.createConsumer(queue);
      session2.start();
      ClientMessage clientMessage = consumer1.receiveImmediate();
      assertNotNull(clientMessage);
      String s = clientMessage.getBodyBuffer().readString();
      assertEquals(s, "message:1");
   }

   @Test
   public void testSendAndReceiveSameQueueTransactedManyMessages() throws Exception
   {
      int numMessage = 1000;
      ClientProducer producer = session1.createProducer();
      ClientSession session = factory1.createSession(true, false, false);
      ClientConsumer consumer = session.createConsumer(queue);
      for (int i = 0; i < numMessage; i++)
      {
         ClientMessage message = session1.createMessage(true);
         message.getBodyBuffer().writeString("message:" + i);
         producer.send(queue, message);

      }
      producer.close();

      Xid xid = new XidImpl("bq1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());
      session.start(xid, XAResource.TMNOFLAGS);
      session.start();
      for (int i = 0; i < numMessage / 2; i++)
      {
         ClientMessage cMessage = consumer.receive(5000);
         cMessage.acknowledge();
      }
      session.end(xid, XAResource.TMSUCCESS);
      session.prepare(xid);
      session.close();
      liveServer1.crash(true, session);
      ColocatedHornetQServer qServer = (ColocatedHornetQServer) liveServer2.getServer();
      qServer.backupServer.waitForActivation(5, TimeUnit.SECONDS);
      try
            (
                  ClientSession session2 = factory2.createSession(true, false, false)
            )
      {
         session2.getXAResource().commit(xid, false);
      }
      Queue q = (Queue) liveServer2.getServer().getPostOffice().getBinding(queue).getBindable();
      assertEquals(500, q.getMessageCount());
      assertEquals(1000, q.getMessagesAdded());
   }

   @Test
   public void testSendAndReceiveSameQueueTransactedManyMessagesRollback() throws Exception
   {
      int numMessage = 1000;
      ClientProducer producer = session1.createProducer();
      ClientSession session = factory1.createSession(true, false, false);
      ClientConsumer consumer = session.createConsumer(queue);
      for (int i = 0; i < numMessage; i++)
      {
         ClientMessage message = session1.createMessage(true);
         message.getBodyBuffer().writeString("message:" + i);
         producer.send(queue, message);

      }
      producer.close();

      Xid xid = new XidImpl("bq1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());
      session.start(xid, XAResource.TMNOFLAGS);
      session.start();
      for (int i = 0; i < numMessage / 2; i++)
      {
         ClientMessage cMessage = consumer.receive(5000);
         cMessage.acknowledge();
      }
      session.end(xid, XAResource.TMSUCCESS);
      session.prepare(xid);
      session.close();
      liveServer1.crash(true, session);
      ColocatedHornetQServer qServer = (ColocatedHornetQServer) liveServer2.getServer();
      qServer.backupServer.waitForActivation(5, TimeUnit.SECONDS);
      try
            (
                  ClientSession session2 = factory2.createSession(true, false, false)
            )
      {
         session2.getXAResource().rollback(xid);
      }
      Queue q = (Queue) liveServer2.getServer().getPostOffice().getBinding(queue).getBindable();
      assertEquals(1000, q.getMessageCount());
      assertEquals(1000, q.getMessagesAdded());
   }

   @Test
   public void testSendAndReceiveSameQueueTransactedManyMessagesRollbackRestart() throws Exception
   {
      int numMessage = 1000;
      ClientProducer producer = session1.createProducer();
      ClientSession session = factory1.createSession(true, false, false);
      ClientConsumer consumer = session.createConsumer(queue);
      for (int i = 0; i < numMessage; i++)
      {
         ClientMessage message = session1.createMessage(true);
         message.getBodyBuffer().writeString("message:" + i);
         producer.send(queue, message);

      }
      producer.close();

      Xid xid = new XidImpl("bq1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());
      session.start(xid, XAResource.TMNOFLAGS);
      session.start();
      for (int i = 0; i < numMessage / 2; i++)
      {
         ClientMessage cMessage = consumer.receive(5000);
         cMessage.acknowledge();
      }
      session.end(xid, XAResource.TMSUCCESS);
      session.prepare(xid);
      session.close();
      liveServer1.crash(true, session);
      ColocatedHornetQServer qServer = (ColocatedHornetQServer) liveServer2.getServer();
      qServer.backupServer.waitForActivation(5, TimeUnit.SECONDS);
      try
            (
                  ClientSession session2 = factory2.createSession(true, false, false)
            )
      {
         session2.getXAResource().rollback(xid);
      }
      liveServer2.stop();
      liveServer2.start();
      Queue q = (Queue) liveServer2.getServer().getPostOffice().getBinding(queue).getBindable();
      assertEquals(1000, q.getMessageCount());
      assertEquals(1000, q.getMessagesAdded());
   }

   @Test
   public void testReceiveTransacted2Queues1acked() throws Exception
   {
      ClientProducer producer = session1.createProducer();
      SimpleString queue2 = new SimpleString("jms.queue.testQueue2");
      session1.createQueue(queue, queue2, true);
      ClientSession session = factory1.createSession(true, false, false);
      ClientConsumer consumer = session.createConsumer(queue);
      ClientMessage message = session1.createMessage(true);
      message.getBodyBuffer().writeString("message:1");
      producer.send(queue, message);
      producer.close();

      Xid xid = new XidImpl("bq1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());
      session.start(xid, XAResource.TMNOFLAGS);
      session.start();
      ClientMessage cMessage = consumer.receive(5000);
      cMessage.acknowledge();
      session.end(xid, XAResource.TMSUCCESS);
      session.prepare(xid);
      session.close();
      liveServer1.crash(true, session);
      ColocatedHornetQServer qServer = (ColocatedHornetQServer) liveServer2.getServer();
      qServer.backupServer.waitForActivation(5, TimeUnit.SECONDS);
      try
      (
            ClientSession session2 = factory2.createSession(true, false, false)
      )
      {
         session2.getXAResource().commit(xid, false);
      }
      Queue q = (Queue) liveServer2.getServer().getPostOffice().getBinding(queue).getBindable();
      assertEquals(0, q.getMessageCount());
      assertEquals(1, q.getMessagesAdded());
      q = (Queue) liveServer2.getServer().getPostOffice().getBinding(queue2).getBindable();
      assertEquals(1, q.getMessageCount());
      assertEquals(1, q.getMessagesAdded());
   }

   @Test
   public void testReceiveTransacted2Queues1ackedRollback() throws Exception
   {
      ClientProducer producer = session1.createProducer();
      SimpleString queue2 = new SimpleString("jms.queue.testQueue2");
      session1.createQueue(queue, queue2, true);
      ClientSession session = factory1.createSession(true, false, false);
      ClientConsumer consumer = session.createConsumer(queue);
      ClientMessage message = session1.createMessage(true);
      message.getBodyBuffer().writeString("message:1");
      producer.send(queue, message);
      producer.close();

      Xid xid = new XidImpl("bq1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());
      session.start(xid, XAResource.TMNOFLAGS);
      session.start();
      ClientMessage cMessage = consumer.receive(5000);
      cMessage.acknowledge();
      session.end(xid, XAResource.TMSUCCESS);
      session.prepare(xid);
      session.close();
      liveServer1.crash(true, session);
      ColocatedHornetQServer qServer = (ColocatedHornetQServer) liveServer2.getServer();
      qServer.backupServer.waitForActivation(5, TimeUnit.SECONDS);
      try
            (
                  ClientSession session2 = factory2.createSession(true, false, false)
            )
      {
         session2.getXAResource().rollback(xid);
      }
      Queue q = (Queue) liveServer2.getServer().getPostOffice().getBinding(queue).getBindable();
      assertEquals(1, q.getMessageCount());
      assertEquals(1, q.getMessagesAdded());
      q = (Queue) liveServer2.getServer().getPostOffice().getBinding(queue2).getBindable();
      assertEquals(1, q.getMessageCount());
      assertEquals(1, q.getMessagesAdded());
   }

   @Test
   public void testReceiveTransacted2Queues1ackedRollbackRestart() throws Exception
   {
      ClientProducer producer = session1.createProducer();
      SimpleString queue2 = new SimpleString("jms.queue.testQueue2");
      session1.createQueue(queue, queue2, true);
      ClientSession session = factory1.createSession(true, false, false);
      ClientConsumer consumer = session.createConsumer(queue);
      ClientMessage message = session1.createMessage(true);
      message.getBodyBuffer().writeString("message:1");
      producer.send(queue, message);
      producer.close();

      Xid xid = new XidImpl("bq1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());
      session.start(xid, XAResource.TMNOFLAGS);
      session.start();
      ClientMessage cMessage = consumer.receive(5000);
      cMessage.acknowledge();
      session.end(xid, XAResource.TMSUCCESS);
      session.prepare(xid);
      session.close();
      liveServer1.crash(true, session);
      ColocatedHornetQServer qServer = (ColocatedHornetQServer) liveServer2.getServer();
      qServer.backupServer.waitForActivation(5, TimeUnit.SECONDS);
      try
            (
                  ClientSession session2 = factory2.createSession(true, false, false)
            )
      {
         session2.getXAResource().rollback(xid);
      }
      liveServer2.stop();
      liveServer2.start();
      Queue q = (Queue) liveServer2.getServer().getPostOffice().getBinding(queue).getBindable();
      assertEquals(1, q.getMessageCount());
      assertEquals(1, q.getMessagesAdded());
      q = (Queue) liveServer2.getServer().getPostOffice().getBinding(queue2).getBindable();
      assertEquals(1, q.getMessageCount());
      assertEquals(1, q.getMessagesAdded());
   }

   @Test
   public void testReceiveTransacted2Queues1ackedAndExtraSend() throws Exception
   {
      ClientProducer producer = session1.createProducer();
      SimpleString queue2 = new SimpleString("jms.queue.testQueue2");
      session1.createQueue(queue, queue2, true);
      ClientSession session = factory1.createSession(true, false, false);
      ClientConsumer consumer = session.createConsumer(queue);
      ClientMessage message = session1.createMessage(true);
      message.getBodyBuffer().writeString("message:1");
      producer.send(queue, message);
      producer.close();

      Xid xid = new XidImpl("bq1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());
      session.start(xid, XAResource.TMNOFLAGS);
      session.start();
      ClientMessage cMessage = consumer.receive(5000);
      cMessage.acknowledge();
      producer = session.createProducer();
      ClientMessage m = session.createMessage(true);
      m.getBodyBuffer().writeString("message:2");
      producer.send(queue, m);
      session.end(xid, XAResource.TMSUCCESS);
      session.prepare(xid);
      session.close();
      liveServer1.crash(true, session);
      ColocatedHornetQServer qServer = (ColocatedHornetQServer) liveServer2.getServer();
      qServer.backupServer.waitForActivation(5, TimeUnit.SECONDS);
      try
            (
                  ClientSession session2 = factory2.createSession(true, false, false)
            )
      {
         session2.getXAResource().commit(xid, false);
      }
      Queue q = (Queue) liveServer2.getServer().getPostOffice().getBinding(queue).getBindable();
      assertEquals(1, q.getMessageCount());
      assertEquals(2, q.getMessagesAdded());
      q = (Queue) liveServer2.getServer().getPostOffice().getBinding(queue2).getBindable();
      assertEquals(2, q.getMessageCount());
      assertEquals(2, q.getMessagesAdded());
   }

   @Test
   public void testReceiveTransacted2Queues1ackedAndExtraSendRollback() throws Exception
   {
      ClientProducer producer = session1.createProducer();
      SimpleString queue2 = new SimpleString("jms.queue.testQueue2");
      session1.createQueue(queue, queue2, true);
      ClientSession session = factory1.createSession(true, false, false);
      ClientConsumer consumer = session.createConsumer(queue);
      ClientMessage message = session1.createMessage(true);
      message.getBodyBuffer().writeString("message:1");
      producer.send(queue, message);
      producer.close();

      Xid xid = new XidImpl("bq1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());
      session.start(xid, XAResource.TMNOFLAGS);
      session.start();
      ClientMessage cMessage = consumer.receive(5000);
      cMessage.acknowledge();
      producer = session.createProducer();
      ClientMessage m = session.createMessage(true);
      m.getBodyBuffer().writeString("message:2");
      producer.send(queue, m);
      session.end(xid, XAResource.TMSUCCESS);
      session.prepare(xid);
      session.close();
      liveServer1.crash(true, session);
      ColocatedHornetQServer qServer = (ColocatedHornetQServer) liveServer2.getServer();
      qServer.backupServer.waitForActivation(5, TimeUnit.SECONDS);
      try
      (
            ClientSession session2 = factory2.createSession(true, false, false)
      )
      {
         session2.getXAResource().rollback(xid);
      }
      Queue q = (Queue) liveServer2.getServer().getPostOffice().getBinding(queue).getBindable();
      assertEquals(1, q.getMessageCount());
      assertEquals(1, q.getMessagesAdded());
      q = (Queue) liveServer2.getServer().getPostOffice().getBinding(queue2).getBindable();
      assertEquals(1, q.getMessageCount());
      assertEquals(1, q.getMessagesAdded());
   }

   @Test
   public void testReceiveTransacted2Queues1ackedAndExtraSendRollbackRestart() throws Exception
   {
      ClientProducer producer = session1.createProducer();
      SimpleString queue2 = new SimpleString("jms.queue.testQueue2");
      session1.createQueue(queue, queue2, true);
      ClientSession session = factory1.createSession(true, false, false);
      ClientConsumer consumer = session.createConsumer(queue);
      ClientMessage message = session1.createMessage(true);
      message.getBodyBuffer().writeString("message:1");
      producer.send(queue, message);
      producer.close();

      Xid xid = new XidImpl("bq1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());
      session.start(xid, XAResource.TMNOFLAGS);
      session.start();
      ClientMessage cMessage = consumer.receive(5000);
      cMessage.acknowledge();
      producer = session.createProducer();
      ClientMessage m = session.createMessage(true);
      m.getBodyBuffer().writeString("message:2");
      producer.send(queue, m);
      session.end(xid, XAResource.TMSUCCESS);
      session.prepare(xid);
      session.close();
      liveServer1.crash(true, session);
      ColocatedHornetQServer qServer = (ColocatedHornetQServer) liveServer2.getServer();
      qServer.backupServer.waitForActivation(5, TimeUnit.SECONDS);
      try
            (
                  ClientSession session2 = factory2.createSession(true, false, false)
            )
      {
         session2.getXAResource().rollback(xid);
      }
      liveServer2.stop();
      liveServer2.start();
      Queue q = (Queue) liveServer2.getServer().getPostOffice().getBinding(queue).getBindable();
      assertEquals(1, q.getMessageCount());
      assertEquals(1, q.getMessagesAdded());
      q = (Queue) liveServer2.getServer().getPostOffice().getBinding(queue2).getBindable();
      assertEquals(1, q.getMessageCount());
      assertEquals(1, q.getMessagesAdded());
   }

   @Test
   public void testReceive() throws Exception
   {
      int numMessages = 100;
      ClientProducer producer = session1.createProducer();
      final CountDownLatch latch = new CountDownLatch(numMessages);
      session1.setSendAcknowledgementHandler(new SendAcknowledgementHandler()
      {
         @Override
         public void sendAcknowledged(org.hornetq.api.core.Message message)
         {
            latch.countDown();
         }
      });
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session1.createMessage(true);
         message.getBodyBuffer().writeString("message:" + i);
         producer.send(queue, message);
      }
      latch.await(10, TimeUnit.SECONDS);

      ClientConsumer consumer1 = session1.createConsumer(queue);
      session1.start();
      for (int i = 0; i < numMessages; i += 2)
      {
         ClientMessage cMessage = consumer1.receive(5000);
         assertNotNull(cMessage.getBodyBuffer().readString());
      }
      session1.close();
      liveServer1.crash();
      ClientConsumer consumer = session2.createConsumer(queue);
      session2.start();
      for (int i = 50; i < numMessages; i++)
      {
         ClientMessage cMessage = consumer.receive(5000);
         String s = cMessage.getBodyBuffer().readString();
         System.out.println("s = " + s);
         assertNotNull(s);
      }
   }

   @Test
   public void testReceiveRedelivery() throws Exception
   {
      ClientSession session = factory1.createTransactedSession();
      ClientProducer producer = session1.createProducer();
      final CountDownLatch latch = new CountDownLatch(1);
      session.setSendAcknowledgementHandler(new SendAcknowledgementHandler()
      {
         @Override
         public void sendAcknowledged(org.hornetq.api.core.Message message)
         {
            latch.countDown();
         }
      });
      ClientMessage message = session.createMessage(true);
      message.getBodyBuffer().writeString("message:1");
      producer.send(queue, message);
      latch.await(10, TimeUnit.SECONDS);

      ClientConsumer consumer1 = session.createConsumer(queue);
      session.start();
      ClientMessage cMessage = consumer1.receive(5000);
      assertNotNull(cMessage.getBodyBuffer().readString());
      cMessage.acknowledge();
      assertEquals(1, cMessage.getDeliveryCount());
      session.rollback();
      cMessage = consumer1.receive(5000);
      assertNotNull(cMessage.getBodyBuffer().readString());
      cMessage.acknowledge();
      assertEquals(2, cMessage.getDeliveryCount());
      session.rollback();
      session.close();
      liveServer1.crash();
      ClientConsumer consumer = session2.createConsumer(queue);
      session2.start();
      cMessage = consumer.receive(5000);
      String s = cMessage.getBodyBuffer().readString();
      System.out.println("s = " + s);
      assertNotNull(s);
      assertEquals(3, cMessage.getDeliveryCount());
   }

   @Test
   public void testSendDuplicateIDs() throws Exception
   {
      int numMessages = 100;
      ClientProducer producer = session1.createProducer();
      ClientProducer producer2 = session2.createProducer();
      final CountDownLatch latch = new CountDownLatch(numMessages * 2);
      session1.setSendAcknowledgementHandler(new SendAcknowledgementHandler()
      {
         @Override
         public void sendAcknowledged(org.hornetq.api.core.Message message)
         {
            latch.countDown();
         }
      });

      session2.setSendAcknowledgementHandler(new SendAcknowledgementHandler()
      {
         @Override
         public void sendAcknowledged(org.hornetq.api.core.Message message)
         {
            latch.countDown();
         }
      });
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session1.createMessage(true);
         message.getBodyBuffer().writeString("message:" + i);
         message.putStringProperty(org.hornetq.api.core.Message.HDR_DUPLICATE_DETECTION_ID.toString(), "prod1" + ":" + i);
         producer.send(queue, message);

         message = session2.createMessage(true);
         message.getBodyBuffer().writeString("message:" + i);
         message.putStringProperty(org.hornetq.api.core.Message.HDR_DUPLICATE_DETECTION_ID.toString(), "prod2" + ":" + i);
         producer2.send(queue, message);
      }
      latch.await(10, TimeUnit.SECONDS);

      liveServer1.crash();

      final CountDownLatch latch2 = new CountDownLatch(numMessages * 2);
      session1.setSendAcknowledgementHandler(new SendAcknowledgementHandler()
      {
         @Override
         public void sendAcknowledged(org.hornetq.api.core.Message message)
         {
            latch2.countDown();
         }
      });
      session2.setSendAcknowledgementHandler(new SendAcknowledgementHandler()
      {
         @Override
         public void sendAcknowledged(org.hornetq.api.core.Message message)
         {
            latch2.countDown();
         }
      });

      Thread.sleep(5000);
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session1.createMessage(true);
         message.getBodyBuffer().writeString("message:" + i);
         message.putStringProperty(org.hornetq.api.core.Message.HDR_DUPLICATE_DETECTION_ID.toString(), "prod1" + ":" + i);
         producer.send(queue, message);

         message = session2.createMessage(true);
         message.getBodyBuffer().writeString("message:" + i);
         message.putStringProperty(org.hornetq.api.core.Message.HDR_DUPLICATE_DETECTION_ID.toString(), "prod2" + ":" + i);
         producer2.send(queue, message);
      }
      latch2.await(10, TimeUnit.SECONDS);
      Binding binding = liveServer2.getServer().getPostOffice().getBinding(new SimpleString("jms.queue.testQueue"));
      QueueImpl q = (QueueImpl) binding.getBindable();
      assertEquals(numMessages * 2, q.getMessageCount());
   }



   protected void createConfigs() throws Exception
   {
      nodeManagerLive1 = new InVMNodeManager(false);
      nodeManagerLive2 = new InVMNodeManager(false);

      TransportConfiguration liveConnector1 = getConnectorTransportConfiguration(1);
      Configuration liveConfiguration1 = super.createDefaultConfig();
      liveConfiguration1.getAcceptorConfigurations().clear();
      liveConfiguration1.getAcceptorConfigurations().add(getAcceptorTransportConfiguration(1));
      liveConfiguration1.getHAPolicy().setPolicyType(HAPolicy.POLICY_TYPE.SHARED_STORE);
      liveConfiguration1.getHAPolicy().setFailbackDelay(1000);
      liveConfiguration1.setJournalDirectory(getTestDir() + "/journal1");
      liveConfiguration1.setBindingsDirectory(getTestDir() + "/bindings1");
      liveConfiguration1.setLargeMessagesDirectory(getTestDir() + "/largemessage1");
      liveConfiguration1.setPagingDirectory(getTestDir() + "/paging1");
      liveConfiguration1.getQueueConfigurations().add(new CoreQueueConfiguration("jms.queue.testQueue", "jms.queue.testQueue", null, true));
      liveConfiguration1.getQueueConfigurations().add(new CoreQueueConfiguration("jms.topic.testTopic", "jms.topic.testTopic", HornetQServerImpl.GENERIC_IGNORED_FILTER, true));

      TransportConfiguration liveConnector2 = getConnectorTransportConfiguration(2);
      basicClusterConnectionConfig(liveConfiguration1, liveConnector1.getName(), liveConnector2.getName());
      liveConfiguration1.getConnectorConfigurations().put(liveConnector1.getName(), liveConnector1);
      liveConfiguration1.getConnectorConfigurations().put(liveConnector2.getName(), liveConnector2);

      Configuration backupConfiguration1 = liveConfiguration1.copy();

      backupConfiguration1.setJournalDirectory(getTestDir() + "/journal2");
      backupConfiguration1.setBindingsDirectory(getTestDir() + "/bindings2");
      backupConfiguration1.setLargeMessagesDirectory(getTestDir() + "/largemessage2");
      backupConfiguration1.setPagingDirectory(getTestDir() + "/paging2");
      backupConfiguration1.setBackupStrategy(BackupStrategy.SCALE_DOWN);
      backupConfiguration1.getHAPolicy().setPolicyType(HAPolicy.POLICY_TYPE.BACKUP_SHARED_STORE);
      HAPolicy haPolicy = new HAPolicy();
      ArrayList<String> scaleDownConnectors = new ArrayList<>();
      scaleDownConnectors.add(liveConnector1.getName());
      haPolicy.setScaleDownConnectors(scaleDownConnectors);
      backupConfiguration1.setHAPolicy(haPolicy);
      liveConfiguration1.getBackupServerConfigurations().add(backupConfiguration1);

      liveServer1 = createTestableServer(liveConfiguration1, nodeManagerLive1, nodeManagerLive2, 1);

      Configuration liveConfiguration2 = super.createDefaultConfig();
      liveConfiguration2.getAcceptorConfigurations().clear();
      liveConfiguration2.getAcceptorConfigurations().add(getAcceptorTransportConfiguration(2));
      liveConfiguration2.getHAPolicy().setPolicyType(HAPolicy.POLICY_TYPE.SHARED_STORE);
      liveConfiguration2.getHAPolicy().setFailbackDelay(1000);
      liveConfiguration2.setJournalDirectory(getTestDir() + "/journal2");
      liveConfiguration2.setBindingsDirectory(getTestDir() + "/bindings2");
      liveConfiguration2.setLargeMessagesDirectory(getTestDir() + "/largemessage2");
      liveConfiguration2.setPagingDirectory(getTestDir() + "/paging2");
      liveConfiguration2.getQueueConfigurations().add(new CoreQueueConfiguration("jms.queue.testQueue", "jms.queue.testQueue", null, true));
      liveConfiguration2.getQueueConfigurations().add(new CoreQueueConfiguration("jms.topic.testTopic", "jms.topic.testTopic", HornetQServerImpl.GENERIC_IGNORED_FILTER, true));

      basicClusterConnectionConfig(liveConfiguration2, liveConnector2.getName(), liveConnector1.getName());
      liveConfiguration2.getConnectorConfigurations().put(liveConnector1.getName(), liveConnector1);
      liveConfiguration2.getConnectorConfigurations().put(liveConnector2.getName(), liveConnector2);

      Configuration backupConfiguration2 = liveConfiguration2.copy();
      backupConfiguration2.setJournalDirectory(getTestDir() + "/journal1");
      backupConfiguration2.setBindingsDirectory(getTestDir() + "/bindings1");
      backupConfiguration2.setLargeMessagesDirectory(getTestDir() + "/largemessage1");
      backupConfiguration2.setPagingDirectory(getTestDir() + "/paging1");
      backupConfiguration2.setBackupStrategy(BackupStrategy.SCALE_DOWN);
      backupConfiguration2.getHAPolicy().setPolicyType(HAPolicy.POLICY_TYPE.BACKUP_SHARED_STORE);
      HAPolicy haPolicy2 = new HAPolicy();
      ArrayList<String> scaleDownConnectors2 = new ArrayList<>();
      scaleDownConnectors2.add(liveConnector2.getName());
      haPolicy2.setScaleDownConnectors(scaleDownConnectors2);
      backupConfiguration2.setHAPolicy(haPolicy2);
      liveConfiguration2.getBackupServerConfigurations().add(backupConfiguration2);

      liveServer2 = createTestableServer(liveConfiguration2, nodeManagerLive2, nodeManagerLive1, 2);
   }

   private TransportConfiguration getAcceptorTransportConfiguration(int node)
   {
      HashMap<String, Object> params = new HashMap<>();
      params.put("server-id", "" + node);
      return new TransportConfiguration(INVM_ACCEPTOR_FACTORY, params);
   }

   private TransportConfiguration getConnectorTransportConfiguration(int node)
   {
      HashMap<String, Object> params = new HashMap<>();
      params.put("server-id", "" + node);
      return new TransportConfiguration(INVM_CONNECTOR_FACTORY, params);
   }

   protected TestableServer createTestableServer(Configuration config, NodeManager liveNodeManager, NodeManager backupNodeManager, int id)
   {
      return new SameProcessHornetQServer(
            createColocatedInVMFailoverServer(true, config, liveNodeManager, backupNodeManager, id));
   }
}
