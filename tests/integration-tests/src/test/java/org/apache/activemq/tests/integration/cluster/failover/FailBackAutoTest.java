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
package org.apache.activemq.tests.integration.cluster.failover;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.api.core.client.ClientConsumer;
import org.apache.activemq.api.core.client.ClientMessage;
import org.apache.activemq.api.core.client.ClientProducer;
import org.apache.activemq.api.core.client.ClientSession;
import org.apache.activemq.api.core.client.ClientSessionFactory;
import org.apache.activemq.api.core.client.ServerLocator;
import org.apache.activemq.core.client.impl.ClientSessionFactoryInternal;
import org.apache.activemq.core.client.impl.ServerLocatorInternal;
import org.apache.activemq.core.config.ha.SharedStoreMasterPolicyConfiguration;
import org.apache.activemq.core.config.ha.SharedStoreSlavePolicyConfiguration;
import org.apache.activemq.core.server.cluster.ha.SharedStoreSlavePolicy;
import org.apache.activemq.core.server.impl.InVMNodeManager;
import org.apache.activemq.jms.client.ActiveMQTextMessage;
import org.apache.activemq.tests.integration.IntegrationTestLogger;
import org.apache.activemq.tests.util.CountDownSessionFailureListener;
import org.apache.activemq.tests.util.TransportConfigurationUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author <a href="mailto:andy.taylor@jboss.com">Andy Taylor</a>
 */
public class FailBackAutoTest extends FailoverTestBase
{
   private final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;
   private static final int NUM_MESSAGES = 100;
   private ServerLocatorInternal locator;
   private ClientSessionFactoryInternal sf;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      locator = getServerLocator();
   }

   @Test
   public void testAutoFailback() throws Exception
   {
      ((SharedStoreSlavePolicy)backupServer.getServer().getHAPolicy()).setRestartBackup(false);
      createSessionFactory();
      final CountDownLatch latch = new CountDownLatch(1);

      ClientSession session = sendAndConsume(sf, true);

      CountDownSessionFailureListener listener = new CountDownSessionFailureListener(latch, session);

      session.addFailureListener(listener);

      liveServer.crash();

      assertTrue(latch.await(5, TimeUnit.SECONDS));

      log.info("backup (nowLive) topology = " + backupServer.getServer().getClusterManager().getDefaultConnection(null).getTopology().describe());

      log.info("Server Crash!!!");

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      ClientMessage message = session.createMessage(true);

      setBody(0, message);

      producer.send(message);

      verifyMessageOnServer(1, 1);

      session.removeFailureListener(listener);

      final CountDownLatch latch2 = new CountDownLatch(1);

      listener = new CountDownSessionFailureListener(latch2, session);

      session.addFailureListener(listener);

      log.info("******* starting live server back");
      liveServer.start();

      Thread.sleep(1000);

      System.out.println("After failback: " + locator.getTopology().describe());

      assertTrue(latch2.await(5, TimeUnit.SECONDS));

      message = session.createMessage(true);

      setBody(1, message);

      producer.send(message);

      session.close();

      verifyMessageOnServer(0, 1);

      wrapUpSessionFactory();
   }

   /**
    * @throws Exception
    * @throws Exception
    */
   private void verifyMessageOnServer(final int server, final int numberOfMessages) throws Exception
   {
      ServerLocator backupLocator = createInVMLocator(server);
      ClientSessionFactory factorybkp = addSessionFactory(createSessionFactory(backupLocator));
      ClientSession sessionbkp = factorybkp.createSession(false, false);
      sessionbkp.start();
      ClientConsumer consumerbkp = sessionbkp.createConsumer(ADDRESS);
      for (int i = 0; i < numberOfMessages; i++)
      {
         ClientMessage msg = consumerbkp.receive(1000);
         assertNotNull(msg);
         msg.acknowledge();
         sessionbkp.commit();
      }
      sessionbkp.close();
      factorybkp.close();
      backupLocator.close();
   }

   @Test
   public void testAutoFailbackThenFailover() throws Exception
   {
      createSessionFactory();
      ClientSession session = sendAndConsume(sf, true);

      CountDownSessionFailureListener listener = new CountDownSessionFailureListener(session);

      session.addFailureListener(listener);

      IntegrationTestLogger.LOGGER.info("Crashing live server...");

      liveServer.crash(session);

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      ClientMessage message = session.createMessage(true);

      setBody(0, message);

      producer.send(message);

      session.removeFailureListener(listener);

      listener = new CountDownSessionFailureListener(session);

      session.addFailureListener(listener);
      log.info("restarting live node now");
      liveServer.start();

      assertTrue("expected a session failure 1", listener.getLatch().await(5, TimeUnit.SECONDS));

      message = session.createMessage(true);

      setBody(1, message);

      producer.send(message);

      session.removeFailureListener(listener);

      listener = new CountDownSessionFailureListener(session);

      session.addFailureListener(listener);

      waitForBackup(sf, 10);

      IntegrationTestLogger.LOGGER.info("Crashing live server again...");

      liveServer.crash();

      assertTrue("expected a session failure 2", listener.getLatch().await(5, TimeUnit.SECONDS));

      session.close();

      wrapUpSessionFactory();
   }

   /**
    * Basic fail-back test.
    *
    * @throws Exception
    */
   @Test
   public void testFailBack() throws Exception
   {
      ((SharedStoreSlavePolicy)backupServer.getServer().getHAPolicy()).setRestartBackup(false);
      createSessionFactory();
      ClientSession session = sendAndConsume(sf, true);

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      sendMessages(session, producer, NUM_MESSAGES);
      session.commit();

      crash(session);

      session.start();
      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);
      receiveMessages(consumer, 0, NUM_MESSAGES, true);
      producer = session.createProducer(FailoverTestBase.ADDRESS);
      sendMessages(session, producer, 2 * NUM_MESSAGES);
      session.commit();
      assertFalse("must NOT be a backup", liveServer.getServer().getHAPolicy().isBackup());
      adaptLiveConfigForReplicatedFailBack(liveServer);

      CountDownSessionFailureListener listener = new CountDownSessionFailureListener(session);
      session.addFailureListener(listener);

      liveServer.start();

      assertTrue(listener.getLatch().await(5, TimeUnit.SECONDS));
      assertTrue("live initialized after restart", liveServer.getServer().waitForActivation(15, TimeUnit.SECONDS));

      session.start();
      receiveMessages(consumer, 0, NUM_MESSAGES, true);
   }

   private void createSessionFactory() throws Exception
   {
      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);
      locator.setFailoverOnInitialConnection(true); // unnecessary?
      locator.setReconnectAttempts(-1);
      sf = createSessionFactoryAndWaitForTopology(locator, 2);
   }

   private void wrapUpSessionFactory()
   {
      sf.close();
      Assert.assertEquals(0, sf.numSessions());
      Assert.assertEquals(0, sf.numConnections());
   }

   @Override
   protected void createConfigs() throws Exception
   {
      nodeManager = new InVMNodeManager(false);

      TransportConfiguration liveConnector = getConnectorTransportConfiguration(true);
      TransportConfiguration backupConnector = getConnectorTransportConfiguration(false);

      backupConfig = super.createDefaultConfig()
         .clearAcceptorConfigurations()
         .addAcceptorConfiguration(getAcceptorTransportConfiguration(false))
         .setSecurityEnabled(false)
         .setHAPolicyConfiguration(new SharedStoreSlavePolicyConfiguration()
                                      .setFailbackDelay(1000)
                                      .setRestartBackup(true))
         .addConnectorConfiguration(liveConnector.getName(), liveConnector)
         .addConnectorConfiguration(backupConnector.getName(), backupConnector)
         .addClusterConfiguration(basicClusterConnectionConfig(backupConnector.getName(), liveConnector.getName()));

      backupServer = createTestableServer(backupConfig);

      liveConfig = super.createDefaultConfig()
         .clearAcceptorConfigurations()
         .addAcceptorConfiguration(getAcceptorTransportConfiguration(true))
         .setSecurityEnabled(false)
         .setHAPolicyConfiguration(new SharedStoreMasterPolicyConfiguration()
                                      .setFailbackDelay(100))
         .addClusterConfiguration(basicClusterConnectionConfig(liveConnector.getName(), backupConnector.getName()))
         .addConnectorConfiguration(liveConnector.getName(), liveConnector)
         .addConnectorConfiguration(backupConnector.getName(), backupConnector);

      liveServer = createTestableServer(liveConfig);
   }

   @Override
   protected TransportConfiguration getAcceptorTransportConfiguration(final boolean live)
   {
      return TransportConfigurationUtils.getInVMAcceptor(live);
   }

   @Override
   protected TransportConfiguration getConnectorTransportConfiguration(final boolean live)
   {
      return TransportConfigurationUtils.getInVMConnector(live);
   }


   private ClientSession sendAndConsume(final ClientSessionFactory sf, final boolean createQueue) throws Exception
   {
      ClientSession session = sf.createSession(false, true, true);

      if (createQueue)
      {
         session.createQueue(FailoverTestBase.ADDRESS, FailoverTestBase.ADDRESS, null, true);
      }

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      final int numMessages = 1000;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createMessage(ActiveMQTextMessage.TYPE,
                                                       false,
                                                       0,
                                                       System.currentTimeMillis(),
                                                       (byte) 1);
         message.putIntProperty(new SimpleString("count"), i);
         message.getBodyBuffer().writeString("aardvarks");
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);

      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message2 = consumer.receive();

         Assert.assertEquals("aardvarks", message2.getBodyBuffer().readString());

         Assert.assertEquals(i, message2.getObjectProperty(new SimpleString("count")));

         message2.acknowledge();
      }

      ClientMessage message3 = consumer.receiveImmediate();

      consumer.close();

      Assert.assertNull(message3);

      return session;
   }
}
