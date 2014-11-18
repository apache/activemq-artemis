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

import java.util.ArrayList;

import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.api.core.client.ClientConsumer;
import org.apache.activemq.api.core.client.ClientMessage;
import org.apache.activemq.api.core.client.ClientProducer;
import org.apache.activemq.api.core.client.ClientSession;
import org.apache.activemq.api.core.client.ClientSessionFactory;
import org.apache.activemq.api.core.client.FailoverEventListener;
import org.apache.activemq.api.core.client.FailoverEventType;
import org.apache.activemq.api.core.client.ServerLocator;
import org.apache.activemq.core.client.impl.ClientSessionFactoryInternal;
import org.apache.activemq.core.client.impl.ServerLocatorInternal;
import org.apache.activemq.core.config.ha.SharedStoreMasterPolicyConfiguration;
import org.apache.activemq.core.config.ha.SharedStoreSlavePolicyConfiguration;
import org.apache.activemq.core.server.impl.InVMNodeManager;
import org.apache.activemq.jms.client.ActiveMQTextMessage;
import org.apache.activemq.tests.integration.IntegrationTestLogger;
import org.apache.activemq.tests.util.TransportConfigurationUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author <a href="mailto:andy.taylor@jboss.com">Andy Taylor</a>
 * @author <a href="mailto:flemming.harms@gmail.com">Flemming Harms</a>
 */
public class FailoverListenerTest extends FailoverTestBase
{
   private final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;
   private ServerLocatorInternal locator;
   private ClientSessionFactoryInternal sf;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      locator = getServerLocator();
   }

   /**
    * Test if two servers are running and one of them is failing, that we trigger the expected
    * events for {@link FailoverEventListener}
    *
    * @throws Exception
    */
   @Test
   public void testFailoverListenerCall() throws Exception
   {
      createSessionFactory(2);

      SessionFactoryFailoverListener listener = new SessionFactoryFailoverListener();
      sf.addFailoverListener(listener);
      ClientSession session = sendAndConsume(sf, true);

      liveServer.crash();
      assertEquals(FailoverEventType.FAILURE_DETECTED, listener.getFailoverEventType().get(0));

      log.info("backup (nowLive) topology = " + backupServer.getServer().getClusterManager().getDefaultConnection(null).getTopology().describe());

      log.info("Server Crash!!!");

      Thread.sleep(1000);
      //the backup server should be online by now
      assertEquals(FailoverEventType.FAILOVER_COMPLETED, listener.getFailoverEventType().get(1));

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      ClientMessage message = session.createMessage(true);

      setBody(0, message);

      producer.send(message);

      verifyMessageOnServer(1, 1);

      log.info("******* starting live server back");
      liveServer.start();
      Thread.sleep(1000);
      //starting the live server trigger a failover event
      assertEquals(FailoverEventType.FAILURE_DETECTED, listener.getFailoverEventType().get(2));

      //the life server should be online by now
      assertEquals(FailoverEventType.FAILOVER_COMPLETED, listener.getFailoverEventType().get(3));

      System.out.println("After failback: " + locator.getTopology().describe());

      message = session.createMessage(true);

      setBody(1, message);

      producer.send(message);

      session.close();

      verifyMessageOnServer(0, 1);

      wrapUpSessionFactory();
      assertEquals("Expected 4 FailoverEvents to be triggered", 4, listener.getFailoverEventType().size());
   }

   /**
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

   /**
    * Test that if the only server is running and failing we trigger
    * the event FailoverEventType.FAILOVER_FAILED in the end
    *
    * @throws Exception
    */
   @Test
   public void testFailoverFailed() throws Exception
   {
      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);
      locator.setFailoverOnInitialConnection(true); // unnecessary?
      locator.setReconnectAttempts(1);
      sf = createSessionFactoryAndWaitForTopology(locator, 2);

      //make sure no backup server is running
      backupServer.stop();

      SessionFactoryFailoverListener listener = new SessionFactoryFailoverListener();
      sf.addFailoverListener(listener);
      ClientSession session = sendAndConsume(sf, true);

      liveServer.crash(session);
      assertEquals(FailoverEventType.FAILURE_DETECTED, listener.getFailoverEventType().get(0));

      assertEquals(FailoverEventType.FAILOVER_FAILED, listener.getFailoverEventType().get(1));

      assertEquals("Expected 2 FailoverEvents to be triggered", 2, listener.getFailoverEventType().size());
      session.close();

      wrapUpSessionFactory();
   }

   private void createSessionFactory(int members) throws Exception
   {
      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);
      locator.setFailoverOnInitialConnection(true); // unnecessary?
      locator.setReconnectAttempts(-1);
      sf = createSessionFactoryAndWaitForTopology(locator, members);
   }

   private void wrapUpSessionFactory()
   {
      sf.close();
      Assert.assertEquals("Expecting 0 sessions", 0, sf.numSessions());
      Assert.assertEquals("Expecting 0 connections", 0, sf.numConnections());
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
                                      .setFailbackDelay(1000))
         .addConnectorConfiguration(liveConnector.getName(), liveConnector)
         .addConnectorConfiguration(backupConnector.getName(), backupConnector)
         .addClusterConfiguration(basicClusterConnectionConfig(backupConnector.getName(), liveConnector.getName()));

      backupServer = createTestableServer(backupConfig);

      liveConfig = super.createDefaultConfig()
         .clearAcceptorConfigurations()
         .addAcceptorConfiguration(getAcceptorTransportConfiguration(true))
         .setSecurityEnabled(false)
         .setHAPolicyConfiguration(new SharedStoreMasterPolicyConfiguration()
                                      .setFailbackDelay(1000))
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


   public class SessionFactoryFailoverListener implements FailoverEventListener
   {

      private final ArrayList<FailoverEventType> failoverTypeEvent = new ArrayList<FailoverEventType>();

      public ArrayList<FailoverEventType> getFailoverEventType()
      {
         return this.failoverTypeEvent;
      }

      @Override
      public void failoverEvent(FailoverEventType eventType)
      {
         this.failoverTypeEvent.add(eventType);
         log.info("Failover event just happen : " + eventType.toString());
      }

   }

}
