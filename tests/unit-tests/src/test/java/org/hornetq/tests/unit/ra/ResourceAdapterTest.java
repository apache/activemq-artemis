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
package org.hornetq.tests.unit.ra;

import javax.jms.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.hornetq.api.core.DiscoveryGroupConfiguration;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.UDPBroadcastGroupConfiguration;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.client.HornetQDestination;
import org.hornetq.ra.ConnectionFactoryProperties;
import org.hornetq.ra.HornetQRAManagedConnectionFactory;
import org.hornetq.ra.HornetQResourceAdapter;
import org.hornetq.ra.inflow.HornetQActivation;
import org.hornetq.ra.inflow.HornetQActivationSpec;
import org.hornetq.tests.util.ServiceTestBase;
import org.junit.Assert;
import org.junit.Test;

/**
 * A ResourceAdapterTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class ResourceAdapterTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testDefaultConnectionFactory() throws Exception
   {
      HornetQResourceAdapter ra = new HornetQResourceAdapter();
      ra.setConnectorClassName(InVMConnectorFactory.class.getName());
      HornetQConnectionFactory factory = ra.getDefaultHornetQConnectionFactory();
      Assert.assertEquals(factory.getCallTimeout(), HornetQClient.DEFAULT_CALL_TIMEOUT);
      Assert.assertEquals(factory.getClientFailureCheckPeriod(), HornetQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD);
      Assert.assertEquals(factory.getClientID(), null);
      Assert.assertEquals(factory.getConnectionLoadBalancingPolicyClassName(),
                          HornetQClient.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME);
      Assert.assertEquals(factory.getConnectionTTL(), HornetQClient.DEFAULT_CONNECTION_TTL);
      Assert.assertEquals(factory.getConsumerMaxRate(), HornetQClient.DEFAULT_CONSUMER_MAX_RATE);
      Assert.assertEquals(factory.getConsumerWindowSize(), HornetQClient.DEFAULT_CONSUMER_WINDOW_SIZE);
      Assert.assertEquals(factory.getDupsOKBatchSize(), HornetQClient.DEFAULT_ACK_BATCH_SIZE);
      Assert.assertEquals(factory.getMinLargeMessageSize(), HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE);
      Assert.assertEquals(factory.getProducerMaxRate(), HornetQClient.DEFAULT_PRODUCER_MAX_RATE);
      Assert.assertEquals(factory.getConfirmationWindowSize(), HornetQClient.DEFAULT_CONFIRMATION_WINDOW_SIZE);
      // by default, reconnect attempts is set to -1
      Assert.assertEquals(-1, factory.getReconnectAttempts());
      Assert.assertEquals(factory.getRetryInterval(), HornetQClient.DEFAULT_RETRY_INTERVAL);
      Assert.assertEquals(factory.getRetryIntervalMultiplier(), HornetQClient.DEFAULT_RETRY_INTERVAL_MULTIPLIER,
                          0.00001);
      Assert.assertEquals(factory.getScheduledThreadPoolMaxSize(), HornetQClient.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE);
      Assert.assertEquals(factory.getThreadPoolMaxSize(), HornetQClient.DEFAULT_THREAD_POOL_MAX_SIZE);
      Assert.assertEquals(factory.getTransactionBatchSize(), HornetQClient.DEFAULT_ACK_BATCH_SIZE);
      Assert.assertEquals(factory.isAutoGroup(), HornetQClient.DEFAULT_AUTO_GROUP);
      Assert.assertEquals(factory.isBlockOnAcknowledge(), HornetQClient.DEFAULT_BLOCK_ON_ACKNOWLEDGE);
      Assert.assertEquals(factory.isBlockOnNonDurableSend(), HornetQClient.DEFAULT_BLOCK_ON_NON_DURABLE_SEND);
      Assert.assertEquals(factory.isBlockOnDurableSend(), HornetQClient.DEFAULT_BLOCK_ON_DURABLE_SEND);
      Assert.assertEquals(factory.isPreAcknowledge(), HornetQClient.DEFAULT_PRE_ACKNOWLEDGE);
      Assert.assertEquals(factory.isUseGlobalPools(), HornetQClient.DEFAULT_USE_GLOBAL_POOLS);
   }

   @Test
   public void test2DefaultConnectionFactorySame() throws Exception
   {
      HornetQResourceAdapter ra = new HornetQResourceAdapter();
      ra.setConnectorClassName(InVMConnectorFactory.class.getName());
      HornetQConnectionFactory factory = ra.getDefaultHornetQConnectionFactory();
      HornetQConnectionFactory factory2 = ra.getDefaultHornetQConnectionFactory();
      Assert.assertEquals(factory, factory2);
   }

   @Test
   public void testCreateConnectionFactoryNoOverrides() throws Exception
   {
      HornetQResourceAdapter ra = new HornetQResourceAdapter();
      ra.setConnectorClassName(InVMConnectorFactory.class.getName());
      HornetQConnectionFactory factory = ra.createHornetQConnectionFactory(new ConnectionFactoryProperties());
      Assert.assertEquals(factory.getCallTimeout(), HornetQClient.DEFAULT_CALL_TIMEOUT);
      Assert.assertEquals(factory.getClientFailureCheckPeriod(), HornetQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD);
      Assert.assertEquals(factory.getClientID(), null);
      Assert.assertEquals(factory.getConnectionLoadBalancingPolicyClassName(),
                          HornetQClient.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME);
      Assert.assertEquals(factory.getConnectionTTL(), HornetQClient.DEFAULT_CONNECTION_TTL);
      Assert.assertEquals(factory.getConsumerMaxRate(), HornetQClient.DEFAULT_CONSUMER_MAX_RATE);
      Assert.assertEquals(factory.getConsumerWindowSize(), HornetQClient.DEFAULT_CONSUMER_WINDOW_SIZE);
      Assert.assertEquals(factory.getDupsOKBatchSize(), HornetQClient.DEFAULT_ACK_BATCH_SIZE);
      Assert.assertEquals(factory.getMinLargeMessageSize(), HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE);
      Assert.assertEquals(factory.getProducerMaxRate(), HornetQClient.DEFAULT_PRODUCER_MAX_RATE);
      Assert.assertEquals(factory.getConfirmationWindowSize(), HornetQClient.DEFAULT_CONFIRMATION_WINDOW_SIZE);
      // by default, reconnect attempts is set to -1
      Assert.assertEquals(-1, factory.getReconnectAttempts());
      Assert.assertEquals(factory.getRetryInterval(), HornetQClient.DEFAULT_RETRY_INTERVAL);
      Assert.assertEquals(factory.getRetryIntervalMultiplier(), HornetQClient.DEFAULT_RETRY_INTERVAL_MULTIPLIER,
                          0.000001);
      Assert.assertEquals(factory.getScheduledThreadPoolMaxSize(), HornetQClient.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE);
      Assert.assertEquals(factory.getThreadPoolMaxSize(), HornetQClient.DEFAULT_THREAD_POOL_MAX_SIZE);
      Assert.assertEquals(factory.getTransactionBatchSize(), HornetQClient.DEFAULT_ACK_BATCH_SIZE);
      Assert.assertEquals(factory.isAutoGroup(), HornetQClient.DEFAULT_AUTO_GROUP);
      Assert.assertEquals(factory.isBlockOnAcknowledge(), HornetQClient.DEFAULT_BLOCK_ON_ACKNOWLEDGE);
      Assert.assertEquals(factory.isBlockOnNonDurableSend(), HornetQClient.DEFAULT_BLOCK_ON_NON_DURABLE_SEND);
      Assert.assertEquals(factory.isBlockOnDurableSend(), HornetQClient.DEFAULT_BLOCK_ON_DURABLE_SEND);
      Assert.assertEquals(factory.isPreAcknowledge(), HornetQClient.DEFAULT_PRE_ACKNOWLEDGE);
      Assert.assertEquals(factory.isUseGlobalPools(), HornetQClient.DEFAULT_USE_GLOBAL_POOLS);
   }

   @Test
   public void testDefaultConnectionFactoryOverrides() throws Exception
   {
      HornetQResourceAdapter ra = new HornetQResourceAdapter();
      ra.setConnectorClassName(InVMConnectorFactory.class.getName());
      ra.setAutoGroup(!HornetQClient.DEFAULT_AUTO_GROUP);
      ra.setBlockOnAcknowledge(!HornetQClient.DEFAULT_BLOCK_ON_ACKNOWLEDGE);
      ra.setBlockOnNonDurableSend(!HornetQClient.DEFAULT_BLOCK_ON_NON_DURABLE_SEND);
      ra.setBlockOnDurableSend(!HornetQClient.DEFAULT_BLOCK_ON_DURABLE_SEND);
      ra.setCallTimeout(1L);
      ra.setClientFailureCheckPeriod(2L);
      ra.setClientID("myid");
      ra.setConnectionLoadBalancingPolicyClassName("mlbcn");
      ra.setConnectionTTL(3L);
      ra.setConsumerMaxRate(4);
      ra.setConsumerWindowSize(5);
      ra.setDiscoveryInitialWaitTimeout(6L);
      ra.setDiscoveryRefreshTimeout(7L);
      ra.setDupsOKBatchSize(8);
      ra.setMinLargeMessageSize(10);
      ra.setPreAcknowledge(!HornetQClient.DEFAULT_PRE_ACKNOWLEDGE);
      ra.setProducerMaxRate(11);
      ra.setConfirmationWindowSize(12);
      ra.setReconnectAttempts(13);
      ra.setRetryInterval(14L);
      ra.setRetryIntervalMultiplier(15d);
      ra.setScheduledThreadPoolMaxSize(16);
      ra.setThreadPoolMaxSize(17);
      ra.setTransactionBatchSize(18);
      ra.setUseGlobalPools(!HornetQClient.DEFAULT_USE_GLOBAL_POOLS);
      HornetQConnectionFactory factory = ra.getDefaultHornetQConnectionFactory();
      Assert.assertEquals(factory.getCallTimeout(), 1);
      Assert.assertEquals(factory.getClientFailureCheckPeriod(), 2);
      Assert.assertEquals(factory.getClientID(), "myid");
      Assert.assertEquals(factory.getConnectionLoadBalancingPolicyClassName(), "mlbcn");
      Assert.assertEquals(factory.getConnectionTTL(), 3);
      Assert.assertEquals(factory.getConsumerMaxRate(), 4);
      Assert.assertEquals(factory.getConsumerWindowSize(), 5);
      Assert.assertEquals(factory.getDupsOKBatchSize(), 8);
      Assert.assertEquals(factory.getMinLargeMessageSize(), 10);
      Assert.assertEquals(factory.getProducerMaxRate(), 11);
      Assert.assertEquals(factory.getConfirmationWindowSize(), 12);
      Assert.assertEquals(factory.getReconnectAttempts(), 13);
      Assert.assertEquals(factory.getRetryInterval(), 14);
      Assert.assertEquals(factory.getRetryIntervalMultiplier(), 15d, 0.00001);
      Assert.assertEquals(factory.getScheduledThreadPoolMaxSize(), 16);
      Assert.assertEquals(factory.getThreadPoolMaxSize(), 17);
      Assert.assertEquals(factory.getTransactionBatchSize(), 18);
      Assert.assertEquals(factory.isAutoGroup(), !HornetQClient.DEFAULT_AUTO_GROUP);
      Assert.assertEquals(factory.isBlockOnAcknowledge(), !HornetQClient.DEFAULT_BLOCK_ON_ACKNOWLEDGE);
      Assert.assertEquals(factory.isBlockOnNonDurableSend(), !HornetQClient.DEFAULT_BLOCK_ON_NON_DURABLE_SEND);
      Assert.assertEquals(factory.isBlockOnDurableSend(), !HornetQClient.DEFAULT_BLOCK_ON_DURABLE_SEND);
      Assert.assertEquals(factory.isPreAcknowledge(), !HornetQClient.DEFAULT_PRE_ACKNOWLEDGE);
      Assert.assertEquals(factory.isUseGlobalPools(), !HornetQClient.DEFAULT_USE_GLOBAL_POOLS);
   }

   @Test
   public void testCreateConnectionFactoryOverrides() throws Exception
   {
      HornetQResourceAdapter ra = new HornetQResourceAdapter();
      ra.setConnectorClassName(InVMConnectorFactory.class.getName());
      ConnectionFactoryProperties connectionFactoryProperties = new ConnectionFactoryProperties();
      connectionFactoryProperties.setAutoGroup(!HornetQClient.DEFAULT_AUTO_GROUP);
      connectionFactoryProperties.setBlockOnAcknowledge(!HornetQClient.DEFAULT_BLOCK_ON_ACKNOWLEDGE);
      connectionFactoryProperties.setBlockOnNonDurableSend(!HornetQClient.DEFAULT_BLOCK_ON_NON_DURABLE_SEND);
      connectionFactoryProperties.setBlockOnDurableSend(!HornetQClient.DEFAULT_BLOCK_ON_DURABLE_SEND);
      connectionFactoryProperties.setCallTimeout(1L);
      connectionFactoryProperties.setClientFailureCheckPeriod(2L);
      connectionFactoryProperties.setClientID("myid");
      connectionFactoryProperties.setConnectionLoadBalancingPolicyClassName("mlbcn");
      connectionFactoryProperties.setConnectionTTL(3L);
      connectionFactoryProperties.setConsumerMaxRate(4);
      connectionFactoryProperties.setConsumerWindowSize(5);
      connectionFactoryProperties.setDiscoveryInitialWaitTimeout(6L);
      connectionFactoryProperties.setDiscoveryRefreshTimeout(7L);
      connectionFactoryProperties.setDupsOKBatchSize(8);
      connectionFactoryProperties.setMinLargeMessageSize(10);
      connectionFactoryProperties.setPreAcknowledge(!HornetQClient.DEFAULT_PRE_ACKNOWLEDGE);
      connectionFactoryProperties.setProducerMaxRate(11);
      connectionFactoryProperties.setConfirmationWindowSize(12);
      connectionFactoryProperties.setReconnectAttempts(13);
      connectionFactoryProperties.setRetryInterval(14L);
      connectionFactoryProperties.setRetryIntervalMultiplier(15d);
      connectionFactoryProperties.setScheduledThreadPoolMaxSize(16);
      connectionFactoryProperties.setThreadPoolMaxSize(17);
      connectionFactoryProperties.setTransactionBatchSize(18);
      connectionFactoryProperties.setUseGlobalPools(!HornetQClient.DEFAULT_USE_GLOBAL_POOLS);
      HornetQConnectionFactory factory = ra.createHornetQConnectionFactory(connectionFactoryProperties);
      Assert.assertEquals(factory.getCallTimeout(), 1);
      Assert.assertEquals(factory.getClientFailureCheckPeriod(), 2);
      Assert.assertEquals(factory.getClientID(), "myid");
      Assert.assertEquals(factory.getConnectionLoadBalancingPolicyClassName(), "mlbcn");
      Assert.assertEquals(factory.getConnectionTTL(), 3);
      Assert.assertEquals(factory.getConsumerMaxRate(), 4);
      Assert.assertEquals(factory.getConsumerWindowSize(), 5);
      Assert.assertEquals(factory.getDupsOKBatchSize(), 8);
      Assert.assertEquals(factory.getMinLargeMessageSize(), 10);
      Assert.assertEquals(factory.getProducerMaxRate(), 11);
      Assert.assertEquals(factory.getConfirmationWindowSize(), 12);
      Assert.assertEquals(factory.getReconnectAttempts(), 13);
      Assert.assertEquals(factory.getRetryInterval(), 14);
      Assert.assertEquals(factory.getRetryIntervalMultiplier(), 15d, 0.000001);
      Assert.assertEquals(factory.getScheduledThreadPoolMaxSize(), 16);
      Assert.assertEquals(factory.getThreadPoolMaxSize(), 17);
      Assert.assertEquals(factory.getTransactionBatchSize(), 18);
      Assert.assertEquals(factory.isAutoGroup(), !HornetQClient.DEFAULT_AUTO_GROUP);
      Assert.assertEquals(factory.isBlockOnAcknowledge(), !HornetQClient.DEFAULT_BLOCK_ON_ACKNOWLEDGE);
      Assert.assertEquals(factory.isBlockOnNonDurableSend(), !HornetQClient.DEFAULT_BLOCK_ON_NON_DURABLE_SEND);
      Assert.assertEquals(factory.isBlockOnDurableSend(), !HornetQClient.DEFAULT_BLOCK_ON_DURABLE_SEND);
      Assert.assertEquals(factory.isPreAcknowledge(), !HornetQClient.DEFAULT_PRE_ACKNOWLEDGE);
      Assert.assertEquals(factory.isUseGlobalPools(), !HornetQClient.DEFAULT_USE_GLOBAL_POOLS);
   }

   @Test
   public void testCreateConnectionFactoryOverrideConnector() throws Exception
   {
      HornetQResourceAdapter ra = new HornetQResourceAdapter();
      ra.setConnectorClassName(InVMConnectorFactory.class.getName());
      ConnectionFactoryProperties connectionFactoryProperties = new ConnectionFactoryProperties();
      ArrayList<String> value = new ArrayList<String>();
      value.add(NettyConnectorFactory.class.getName());
      connectionFactoryProperties.setParsedConnectorClassNames(value);
      HornetQConnectionFactory factory = ra.createHornetQConnectionFactory(connectionFactoryProperties);
      HornetQConnectionFactory defaultFactory = ra.getDefaultHornetQConnectionFactory();
      Assert.assertNotSame(factory, defaultFactory);
   }

   @Test
   public void testCreateConnectionFactoryOverrideDiscovery() throws Exception
   {
      HornetQResourceAdapter ra = new HornetQResourceAdapter();
      ra.setConnectorClassName(InVMConnectorFactory.class.getName());
      ConnectionFactoryProperties connectionFactoryProperties = new ConnectionFactoryProperties();
      connectionFactoryProperties.setDiscoveryAddress("myhost");
      connectionFactoryProperties.setDiscoveryPort(5678);
      connectionFactoryProperties.setDiscoveryLocalBindAddress("newAddress");
      HornetQConnectionFactory factory = ra.createHornetQConnectionFactory(connectionFactoryProperties);
      HornetQConnectionFactory defaultFactory = ra.getDefaultHornetQConnectionFactory();
      Assert.assertNotSame(factory, defaultFactory);
      DiscoveryGroupConfiguration dc = factory.getServerLocator().getDiscoveryGroupConfiguration();
      UDPBroadcastGroupConfiguration udpDg = (UDPBroadcastGroupConfiguration) dc.getBroadcastEndpointFactoryConfiguration();
      Assert.assertEquals(udpDg.getLocalBindAddress(), "newAddress");
      Assert.assertEquals(udpDg.getGroupAddress(), "myhost");
      Assert.assertEquals(udpDg.getGroupPort(), 5678);
   }

   @Test
   public void testCreateConnectionFactoryMultipleConnectors()
   {
      HornetQResourceAdapter ra = new HornetQResourceAdapter();
      ra.setConnectorClassName(NETTY_CONNECTOR_FACTORY + "," + INVM_CONNECTOR_FACTORY + "," + NETTY_CONNECTOR_FACTORY);
      HornetQConnectionFactory factory = ra.createHornetQConnectionFactory(new ConnectionFactoryProperties());
      TransportConfiguration[] configurations = factory.getServerLocator().getStaticTransportConfigurations();
      assertNotNull(configurations);
      assertEquals(3, configurations.length);
      assertEquals(NETTY_CONNECTOR_FACTORY, configurations[0].getFactoryClassName());
      assertEquals(2, configurations[0].getParams().size());
      assertEquals(INVM_CONNECTOR_FACTORY, configurations[1].getFactoryClassName());
      assertEquals(1, configurations[1].getParams().size());
      assertEquals(NETTY_CONNECTOR_FACTORY, configurations[2].getFactoryClassName());
      assertEquals(2, configurations[2].getParams().size());
   }

   @Test
   public void testCreateConnectionFactoryMultipleConnectorsAndParams()
   {
      HornetQResourceAdapter ra = new HornetQResourceAdapter();
      ra.setConnectorClassName(NETTY_CONNECTOR_FACTORY + "," + INVM_CONNECTOR_FACTORY + "," + NETTY_CONNECTOR_FACTORY);
      ra.setConnectionParameters("host=host1;port=5445, serverid=0, host=host2;port=5446");
      HornetQConnectionFactory factory = ra.createHornetQConnectionFactory(new ConnectionFactoryProperties());
      TransportConfiguration[] configurations = factory.getServerLocator().getStaticTransportConfigurations();
      assertNotNull(configurations);
      assertEquals(3, configurations.length);
      assertEquals(NETTY_CONNECTOR_FACTORY, configurations[0].getFactoryClassName());
      assertEquals(2, configurations[0].getParams().size());
      assertEquals("host1", configurations[0].getParams().get("host"));
      assertEquals("5445", configurations[0].getParams().get("port"));
      assertEquals(INVM_CONNECTOR_FACTORY, configurations[1].getFactoryClassName());
      assertEquals(1, configurations[1].getParams().size());
      assertEquals("0", configurations[1].getParams().get("serverid"));
      assertEquals(NETTY_CONNECTOR_FACTORY, configurations[2].getFactoryClassName());
      assertEquals(2, configurations[2].getParams().size());
      assertEquals("host2", configurations[2].getParams().get("host"));
      assertEquals("5446", configurations[2].getParams().get("port"));
   }

   @Test
   public void testCreateConnectionFactoryMultipleConnectorsOverride()
   {
      HornetQResourceAdapter ra = new HornetQResourceAdapter();
      ra.setConnectorClassName(NETTY_CONNECTOR_FACTORY + "," + INVM_CONNECTOR_FACTORY + "," + NETTY_CONNECTOR_FACTORY);
      ConnectionFactoryProperties overrideProperties = new ConnectionFactoryProperties();
      ArrayList<String> value = new ArrayList<String>();
      value.add(INVM_CONNECTOR_FACTORY);
      value.add(NETTY_CONNECTOR_FACTORY);
      value.add(INVM_CONNECTOR_FACTORY);
      overrideProperties.setParsedConnectorClassNames(value);
      HornetQConnectionFactory factory = ra.createHornetQConnectionFactory(overrideProperties);
      TransportConfiguration[] configurations = factory.getServerLocator().getStaticTransportConfigurations();
      assertNotNull(configurations);
      assertEquals(3, configurations.length);
      assertEquals(INVM_CONNECTOR_FACTORY, configurations[0].getFactoryClassName());
      assertEquals(1, configurations[0].getParams().size());
      assertEquals(NETTY_CONNECTOR_FACTORY, configurations[1].getFactoryClassName());
      assertEquals(2, configurations[1].getParams().size());
      assertEquals(INVM_CONNECTOR_FACTORY, configurations[2].getFactoryClassName());
      assertEquals(1, configurations[2].getParams().size());
   }

   @Test
   public void testCreateConnectionFactoryMultipleConnectorsOverrideAndParams()
   {
      HornetQResourceAdapter ra = new HornetQResourceAdapter();
      ra.setConnectorClassName(NETTY_CONNECTOR_FACTORY + "," + INVM_CONNECTOR_FACTORY + "," + NETTY_CONNECTOR_FACTORY);
      ra.setConnectionParameters("host=host1;port=5445, serverid=0, host=host2;port=5446");
      ConnectionFactoryProperties overrideProperties = new ConnectionFactoryProperties();
      ArrayList<String> value = new ArrayList<String>();
      value.add(INVM_CONNECTOR_FACTORY);
      value.add(NETTY_CONNECTOR_FACTORY);
      value.add(INVM_CONNECTOR_FACTORY);
      overrideProperties.setParsedConnectorClassNames(value);
      ArrayList<Map<String, Object>> connectionParameters = new ArrayList<Map<String, Object>>();
      Map<String, Object> map1 = new HashMap<String, Object>();
      map1.put("serverid", "0");
      connectionParameters.add(map1);
      Map<String, Object> map2 = new HashMap<String, Object>();
      map2.put("host", "myhost");
      map2.put("port", "5445");
      connectionParameters.add(map2);
      Map<String, Object> map3 = new HashMap<String, Object>();
      map3.put("serverid", "1");
      connectionParameters.add(map3);
      overrideProperties.setParsedConnectionParameters(connectionParameters);
      HornetQConnectionFactory factory = ra.createHornetQConnectionFactory(overrideProperties);
      TransportConfiguration[] configurations = factory.getServerLocator().getStaticTransportConfigurations();
      assertNotNull(configurations);
      assertEquals(3, configurations.length);
      assertEquals(INVM_CONNECTOR_FACTORY, configurations[0].getFactoryClassName());
      assertEquals(1, configurations[0].getParams().size());
      assertEquals("0", configurations[0].getParams().get("serverid"));
      assertEquals(NETTY_CONNECTOR_FACTORY, configurations[1].getFactoryClassName());
      assertEquals(2, configurations[1].getParams().size());
      assertEquals("myhost", configurations[1].getParams().get("host"));
      assertEquals("5445", configurations[1].getParams().get("port"));
      assertEquals(INVM_CONNECTOR_FACTORY, configurations[2].getFactoryClassName());
      assertEquals(1, configurations[2].getParams().size());
      assertEquals("1", configurations[2].getParams().get("serverid"));
   }

   @Test
   public void testCreateConnectionFactoryThrowsException() throws Exception
   {
      HornetQResourceAdapter ra = new HornetQResourceAdapter();
      ConnectionFactoryProperties connectionFactoryProperties = new ConnectionFactoryProperties();
      try
      {
         ra.createHornetQConnectionFactory(connectionFactoryProperties);
         Assert.fail("should throw exception");
      }
      catch (IllegalArgumentException e)
      {
         // pass
      }
   }

   @Test
   public void testValidateProperties() throws Exception
   {
      validateGettersAndSetters(new HornetQResourceAdapter(),
                                "backupTransportConfiguration",
                                "connectionParameters",
                                "jndiParams");
      validateGettersAndSetters(new HornetQRAManagedConnectionFactory(),
                                "connectionParameters",
                                "sessionDefaultType",
                                "backupConnectionParameters",
                                "jndiParams");
      validateGettersAndSetters(new HornetQActivationSpec(),
                                "connectionParameters",
                                "acknowledgeMode",
                                "subscriptionDurability",
                                "jndiParams");

      HornetQActivationSpec spec = new HornetQActivationSpec();

      spec.setAcknowledgeMode("DUPS_OK_ACKNOWLEDGE");
      Assert.assertEquals("Dups-ok-acknowledge", spec.getAcknowledgeMode());

      spec.setSubscriptionDurability("Durable");
      Assert.assertEquals("Durable", spec.getSubscriptionDurability());

      spec.setSubscriptionDurability("NonDurable");
      Assert.assertEquals("NonDurable", spec.getSubscriptionDurability());

      spec = new HornetQActivationSpec();
      HornetQResourceAdapter adapter = new HornetQResourceAdapter();

      adapter.setUserName("us1");
      adapter.setPassword("ps1");
      adapter.setClientID("cl1");

      spec.setResourceAdapter(adapter);

      Assert.assertEquals("us1", spec.getUser());
      Assert.assertEquals("ps1", spec.getPassword());

      spec.setUser("us2");
      spec.setPassword("ps2");
      spec.setClientID("cl2");

      Assert.assertEquals("us2", spec.getUser());
      Assert.assertEquals("ps2", spec.getPassword());
      Assert.assertEquals("cl2", spec.getClientID());

   }

   @Test
   public void testStartActivation() throws Exception
   {
      HornetQServer server = createServer(false);

      try
      {

         server.start();
         ServerLocator locator = createInVMNonHALocator();
         ClientSessionFactory factory = createSessionFactory(locator);
         ClientSession session = factory.createSession(false, false, false);
         HornetQDestination queue = (HornetQDestination) HornetQJMSClient.createQueue("test");
         session.createQueue(queue.getSimpleAddress(), queue.getSimpleAddress(), true);
         session.close();

         HornetQResourceAdapter ra = new HornetQResourceAdapter();

         ra.setConnectorClassName(INVM_CONNECTOR_FACTORY);
         ra.setUserName("userGlobal");
         ra.setPassword("passwordGlobal");
         ra.setTransactionManagerLocatorClass("");
         ra.setTransactionManagerLocatorMethod("");
         ra.start(new org.hornetq.tests.unit.ra.BootstrapContext());

         Connection conn = ra.getDefaultHornetQConnectionFactory().createConnection();

         conn.close();

         HornetQActivationSpec spec = new HornetQActivationSpec();

         spec.setResourceAdapter(ra);

         spec.setUseJNDI(false);

         spec.setUser("user");
         spec.setPassword("password");

         spec.setDestinationType("javax.jms.Topic");
         spec.setDestination("test");

         spec.setMinSession(1);
         spec.setMaxSession(1);

         HornetQActivation activation = new HornetQActivation(ra, new MessageEndpointFactory(), spec);

         activation.start();
         activation.stop();

         ra.stop();

         locator.close();

      }
      finally
      {
         server.stop();
      }
   }

   @Test
   public void testForConnectionLeakDuringActivationWhenSessionCreationFails() throws Exception
   {
      HornetQServer server = createServer(false);
      HornetQResourceAdapter ra = null;
      HornetQActivation activation = null;

      try
      {
         server.getConfiguration().setSecurityEnabled(true);
         server.start();

         ra = new HornetQResourceAdapter();

         ra.setConnectorClassName(INVM_CONNECTOR_FACTORY);
         ra.setUserName("badUser");
         ra.setPassword("badPassword");
         ra.setTransactionManagerLocatorClass("");
         ra.setTransactionManagerLocatorMethod("");
         ra.start(new org.hornetq.tests.unit.ra.BootstrapContext());

         HornetQActivationSpec spec = new HornetQActivationSpec();

         spec.setResourceAdapter(ra);

         spec.setUseJNDI(false);

         spec.setUser("user");
         spec.setPassword("password");

         spec.setDestinationType("javax.jms.Topic");
         spec.setDestination("test");

         spec.setMinSession(1);
         spec.setMaxSession(1);
         spec.setSetupAttempts(1);

         activation = new HornetQActivation(ra, new MessageEndpointFactory(), spec);

         try
         {
            activation.start();
         }
         catch (Exception e)
         {
            // ignore
         }

         assertEquals(0, server.getRemotingService().getConnections().size());
      }
      finally
      {
         if (activation != null)
            activation.stop();
         if (ra != null)
            ra.stop();
         server.stop();
      }
   }
}
