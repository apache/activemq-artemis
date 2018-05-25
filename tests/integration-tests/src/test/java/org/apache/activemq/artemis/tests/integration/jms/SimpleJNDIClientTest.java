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
package org.apache.activemq.artemis.tests.integration.jms;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.JMSSecurityException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.BroadcastEndpoint;
import org.apache.activemq.artemis.api.core.BroadcastEndpointFactory;
import org.apache.activemq.artemis.api.core.BroadcastGroupConfiguration;
import org.apache.activemq.artemis.api.core.DiscoveryGroupConfiguration;
import org.apache.activemq.artemis.api.core.JGroupsFileBroadcastEndpoint;
import org.apache.activemq.artemis.api.core.JGroupsPropertiesBroadcastEndpointFactory;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.UDPBroadcastEndpointFactory;
import org.apache.activemq.artemis.api.jms.JMSFactoryType;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.ha.SharedStoreMasterPolicyConfiguration;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * ActiveMQConnectionFactoryTest
 */
public class SimpleJNDIClientTest extends ActiveMQTestBase {

   private final String groupAddress = getUDPDiscoveryAddress();

   private final int groupPort = getUDPDiscoveryPort();

   private ActiveMQServer liveService;

   private TransportConfiguration liveTC;

   @Test
   public void testMultipleConnectionFactories() throws NamingException, JMSException {
      Hashtable<String, Object> props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory");
      props.put("connectionFactory.VmConnectionFactory", "vm://0");
      props.put("connectionFactory.TCPConnectionFactory", "tcp://localhost:61616");
      props.put("connectionFactory.UDPConnectionFactory", "udp://" + getUDPDiscoveryAddress() + ":" + getUDPDiscoveryPort());
      props.put("connectionFactory.JGroupsConnectionFactory", "jgroups://mychannelid?file=test-jgroups-file_ping.xml");
      Context ctx = new InitialContext(props);
      ctx.lookup("VmConnectionFactory");
      ctx.lookup("TCPConnectionFactory");
      ctx.lookup("UDPConnectionFactory");
      ctx.lookup("JGroupsConnectionFactory");
   }

   @Test
   public void testVMCF0() throws NamingException, JMSException {
      Hashtable<String, String> props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory");
      props.put("connectionFactory.ConnectionFactory", "vm://0");
      Context ctx = new InitialContext(props);

      ConnectionFactory connectionFactory = (ConnectionFactory) ctx.lookup("ConnectionFactory");

      connectionFactory.createConnection().close();
   }

   @Test
   public void testVMCF1() throws NamingException, JMSException {
      Hashtable<String, String> props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory");
      props.put("connectionFactory.ConnectionFactory", "vm://1");
      Context ctx = new InitialContext(props);

      ConnectionFactory connectionFactory = (ConnectionFactory) ctx.lookup("ConnectionFactory");

      connectionFactory.createConnection().close();
   }

   @Test
   public void testXACF() throws NamingException, JMSException {
      Hashtable<String, String> props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory");
      props.put("connectionFactory.myConnectionFactory", "vm://0?type=XA_CF");
      Context ctx = new InitialContext(props);

      ActiveMQConnectionFactory connectionFactory = (ActiveMQConnectionFactory) ctx.lookup("myConnectionFactory");

      Assert.assertEquals(JMSFactoryType.XA_CF.intValue(), connectionFactory.getFactoryType());
   }

   @Test
   public void testQueueCF() throws NamingException, JMSException {
      Hashtable<String, String> props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory");
      props.put("connectionFactory.myConnectionFactory", "vm://0?type=QUEUE_CF");
      Context ctx = new InitialContext(props);

      ActiveMQConnectionFactory connectionFactory = (ActiveMQConnectionFactory) ctx.lookup("myConnectionFactory");

      Assert.assertEquals(JMSFactoryType.QUEUE_CF.intValue(), connectionFactory.getFactoryType());
   }

   @Test
   public void testQueueXACF() throws NamingException, JMSException {
      Hashtable<String, String> props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory");
      props.put("connectionFactory.myConnectionFactory", "vm://0?type=QUEUE_XA_CF");
      Context ctx = new InitialContext(props);

      ActiveMQConnectionFactory connectionFactory = (ActiveMQConnectionFactory) ctx.lookup("myConnectionFactory");

      Assert.assertEquals(JMSFactoryType.QUEUE_XA_CF.intValue(), connectionFactory.getFactoryType());
   }

   @Test
   public void testTopicCF() throws NamingException, JMSException {
      Hashtable<String, String> props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory");
      props.put("connectionFactory.myConnectionFactory", "vm://0?type=TOPIC_CF");
      Context ctx = new InitialContext(props);

      ActiveMQConnectionFactory connectionFactory = (ActiveMQConnectionFactory) ctx.lookup("myConnectionFactory");

      Assert.assertEquals(JMSFactoryType.TOPIC_CF.intValue(), connectionFactory.getFactoryType());
   }

   @Test
   public void testTopicXACF() throws NamingException, JMSException {
      Hashtable<String, String> props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory");
      props.put("connectionFactory.myConnectionFactory", "vm://0?type=TOPIC_XA_CF");
      Context ctx = new InitialContext(props);

      ActiveMQConnectionFactory connectionFactory = (ActiveMQConnectionFactory) ctx.lookup("myConnectionFactory");

      Assert.assertEquals(JMSFactoryType.TOPIC_XA_CF.intValue(), connectionFactory.getFactoryType());
   }

   @Test
   public void testRemoteCFWithTCP() throws NamingException, JMSException {
      Hashtable<String, String> props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory");
      props.put("connectionFactory.myConnectionFactory", "tcp://127.0.0.1:61616");
      Context ctx = new InitialContext(props);

      ConnectionFactory connectionFactory = (ConnectionFactory) ctx.lookup("myConnectionFactory");

      connectionFactory.createConnection().close();
   }

   @Test
   public void testRemoteCFWithTCPandHA() throws NamingException, JMSException {
      Hashtable<String, String> props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory");
      props.put("connectionFactory.myConnectionFactory", "tcp://127.0.0.1:61616?ha=true");
      Context ctx = new InitialContext(props);

      ActiveMQConnectionFactory cf = (ActiveMQConnectionFactory) ctx.lookup("myConnectionFactory");

      Assert.assertEquals(true, cf.isHA());
   }

   @Test
   public void testRemoteCFWithJGroups() throws Exception {
      Hashtable<String, String> props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory");
      props.put("connectionFactory.myConnectionFactory", "jgroups://mychannelid?file=test-jgroups-file_ping.xml");
      Context ctx = new InitialContext(props);

      ActiveMQConnectionFactory connectionFactory = (ActiveMQConnectionFactory) ctx.lookup("myConnectionFactory");
      connectionFactory.getDiscoveryGroupConfiguration().getBroadcastEndpointFactory().createBroadcastEndpoint().close(false);
   }

   @Test
   public void testRemoteCFWithJgroupsWithTransportConfigFile() throws Exception {
      Hashtable<String, String> props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory.class.getCanonicalName());
      props.put("connectionFactory.myConnectionFactory", "jgroups://testChannelName?file=test-jgroups-file_ping.xml&" +
         ActiveMQInitialContextFactory.REFRESH_TIMEOUT + "=5000&" +
         ActiveMQInitialContextFactory.DISCOVERY_INITIAL_WAIT_TIMEOUT + "=6000");
      Context ctx = new InitialContext(props);

      ActiveMQConnectionFactory cf = (ActiveMQConnectionFactory) ctx.lookup("myConnectionFactory");

      DiscoveryGroupConfiguration discoveryGroupConfiguration = cf.getDiscoveryGroupConfiguration();
      Assert.assertEquals(5000, discoveryGroupConfiguration.getRefreshTimeout());
      Assert.assertEquals(6000, discoveryGroupConfiguration.getDiscoveryInitialWaitTimeout());

      BroadcastEndpoint broadcastEndpoint = cf.getDiscoveryGroupConfiguration().getBroadcastEndpointFactory().createBroadcastEndpoint();
      Assert.assertTrue(broadcastEndpoint instanceof JGroupsFileBroadcastEndpoint);
      broadcastEndpoint.close(false);
   }

   @Test
   public void testRemoteCFWithJgroupsWithTransportConfigProps() throws Exception {
      Hashtable<String, String> props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, ActiveMQInitialContextFactory.class.getCanonicalName());
      props.put("connectionFactory.ConnectionFactory", "jgroups://testChannelName?properties=param=value&" +
         ActiveMQInitialContextFactory.REFRESH_TIMEOUT + "=5000&" +
         ActiveMQInitialContextFactory.DISCOVERY_INITIAL_WAIT_TIMEOUT + "=6000");
      Context ctx = new InitialContext(props);

      ActiveMQConnectionFactory cf = (ActiveMQConnectionFactory) ctx.lookup("ConnectionFactory");

      DiscoveryGroupConfiguration discoveryGroupConfiguration = cf.getDiscoveryGroupConfiguration();
      Assert.assertEquals(5000, discoveryGroupConfiguration.getRefreshTimeout());
      Assert.assertEquals(6000, discoveryGroupConfiguration.getDiscoveryInitialWaitTimeout());

      BroadcastEndpointFactory broadcastEndpointFactory = cf.getDiscoveryGroupConfiguration().getBroadcastEndpointFactory();
      Assert.assertTrue(broadcastEndpointFactory instanceof JGroupsPropertiesBroadcastEndpointFactory);
      JGroupsPropertiesBroadcastEndpointFactory endpointFactory = (JGroupsPropertiesBroadcastEndpointFactory) broadcastEndpointFactory;
      Assert.assertEquals(endpointFactory.getProperties(), "param=value");
      Assert.assertEquals(endpointFactory.getChannelName(), "testChannelName");
   }

   @Test
   public void testRemoteCFWithJgroupsWithTransportConfigNullProps() throws Exception {
      Hashtable<String, String> props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, ActiveMQInitialContextFactory.class.getCanonicalName());
      props.put("connectionFactory.ConnectionFactory", "jgroups://testChannelName?" +
         ActiveMQInitialContextFactory.REFRESH_TIMEOUT + "=5000&" +
         ActiveMQInitialContextFactory.DISCOVERY_INITIAL_WAIT_TIMEOUT + "=6000");
      Context ctx = new InitialContext(props);

      ActiveMQConnectionFactory cf = (ActiveMQConnectionFactory) ctx.lookup("ConnectionFactory");

      DiscoveryGroupConfiguration discoveryGroupConfiguration = cf.getDiscoveryGroupConfiguration();
      Assert.assertEquals(5000, discoveryGroupConfiguration.getRefreshTimeout());
      Assert.assertEquals(6000, discoveryGroupConfiguration.getDiscoveryInitialWaitTimeout());

      BroadcastEndpointFactory broadcastEndpointFactory = cf.getDiscoveryGroupConfiguration().getBroadcastEndpointFactory();
      Assert.assertTrue(broadcastEndpointFactory instanceof JGroupsPropertiesBroadcastEndpointFactory);
      JGroupsPropertiesBroadcastEndpointFactory endpointFactory = (JGroupsPropertiesBroadcastEndpointFactory) broadcastEndpointFactory;
      Assert.assertEquals(endpointFactory.getProperties(), null);
      Assert.assertEquals(endpointFactory.getChannelName(), "testChannelName");
   }

   @Test
   public void testRemoteCFWithUDP() throws NamingException, JMSException {
      Hashtable<String, String> props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory");
      props.put("connectionFactory.myConnectionFactory", "udp://" + getUDPDiscoveryAddress() + ":" + getUDPDiscoveryPort());
      Context ctx = new InitialContext(props);

      ConnectionFactory connectionFactory = (ConnectionFactory) ctx.lookup("myConnectionFactory");

      connectionFactory.createConnection().close();
   }

   @Test
   public void testRemoteCFWithUDPWithTransportConfig() throws NamingException, JMSException {
      Hashtable<String, String> props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, ActiveMQInitialContextFactory.class.getCanonicalName());
      props.put("connectionFactory.myConnectionFactory", "udp://" + getUDPDiscoveryAddress() + ":" + getUDPDiscoveryPort() + "?" +
         TransportConstants.LOCAL_ADDRESS_PROP_NAME + "=Server1&" +
         TransportConstants.LOCAL_PORT_PROP_NAME + "=1198&" +
         ActiveMQInitialContextFactory.REFRESH_TIMEOUT + "=5000&" +
         ActiveMQInitialContextFactory.DISCOVERY_INITIAL_WAIT_TIMEOUT + "=6000");
      Context ctx = new InitialContext(props);

      ActiveMQConnectionFactory cf = (ActiveMQConnectionFactory) ctx.lookup("myConnectionFactory");

      DiscoveryGroupConfiguration discoveryGroupConfiguration = cf.getDiscoveryGroupConfiguration();
      Assert.assertEquals(5000, discoveryGroupConfiguration.getRefreshTimeout());
      Assert.assertEquals(6000, discoveryGroupConfiguration.getDiscoveryInitialWaitTimeout());

      UDPBroadcastEndpointFactory udpBroadcastEndpointFactory = (UDPBroadcastEndpointFactory) discoveryGroupConfiguration.getBroadcastEndpointFactory();
      //these 2 are transient so are ignored
      Assert.assertNotEquals("Server1", udpBroadcastEndpointFactory.getLocalBindAddress());
      Assert.assertNotEquals(1198, udpBroadcastEndpointFactory.getLocalBindPort());
      Assert.assertEquals(getUDPDiscoveryAddress(), udpBroadcastEndpointFactory.getGroupAddress());
      Assert.assertEquals(getUDPDiscoveryPort(), udpBroadcastEndpointFactory.getGroupPort());
   }

   @Test
   public void testRemoteCFWithMultipleHosts() throws NamingException, JMSException {
      Hashtable<String, String> props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory");
      props.put("connectionFactory.myConnectionFactory", "tcp://127.0.0.1:61616/httpEnabled=true&foo=bar,tcp://127.0.0.2:61617?httpEnabled=false?clientID=myClientID");
      Context ctx = new InitialContext(props);

      ConnectionFactory connectionFactory = (ConnectionFactory) ctx.lookup("myConnectionFactory");

      connectionFactory.createConnection().close();
   }

   @Test
   public void testRemoteCFWithTransportConfig() throws NamingException, JMSException {
      Hashtable<String, String> props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory");
      props.put("connectionFactory.myConnectionFactory", "tcp://127.0.0.1:61616?" +
         TransportConstants.SSL_ENABLED_PROP_NAME + "=mySSLEnabledPropValue&" +
         TransportConstants.HTTP_ENABLED_PROP_NAME + "=myHTTPEnabledPropValue&" +
         TransportConstants.HTTP_CLIENT_IDLE_PROP_NAME + "=myHTTPClientIdlePropValue&" +
         TransportConstants.HTTP_CLIENT_IDLE_SCAN_PERIOD + "=myHTTPClientIdleScanPeriodValue&" +
         TransportConstants.HTTP_REQUIRES_SESSION_ID + "=myHTTPRequiresSessionIDValue&" +
         TransportConstants.HTTP_UPGRADE_ENABLED_PROP_NAME + "=myHTTPUpgradeEnabledPropValue&" +
         TransportConstants.HTTP_UPGRADE_ENDPOINT_PROP_NAME + "=myHTTPUpgradeEndpointPropValue&" +
         TransportConstants.USE_SERVLET_PROP_NAME + "=myUseServletPropValue&" +
         TransportConstants.SERVLET_PATH + "=myServletPathValue&" +
         TransportConstants.USE_NIO_PROP_NAME + "=myUseNIOPropValue&" +
         TransportConstants.USE_NIO_GLOBAL_WORKER_POOL_PROP_NAME + "=myUseNIOGlobalWorkerPoolPropValue&" +
         TransportConstants.LOCAL_ADDRESS_PROP_NAME + "=myLocalAddressPropValue&" +
         TransportConstants.LOCAL_PORT_PROP_NAME + "=myLocalPortPropValue&" +
         TransportConstants.KEYSTORE_PROVIDER_PROP_NAME + "=myKeystoreProviderPropValue&" +
         TransportConstants.KEYSTORE_PATH_PROP_NAME + "=myKeystorePathPropValue&" +
         TransportConstants.KEYSTORE_PASSWORD_PROP_NAME + "=myKeystorePasswordPropValue&" +
         TransportConstants.TRUSTSTORE_PROVIDER_PROP_NAME + "=myTruststoreProviderPropValue&" +
         TransportConstants.TRUSTSTORE_PATH_PROP_NAME + "=myTruststorePathPropValue&" +
         TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME + "=myTruststorePasswordPropValue&" +
         TransportConstants.ENABLED_CIPHER_SUITES_PROP_NAME + "=myEnabledCipherSuitesPropValue&" +
         TransportConstants.ENABLED_PROTOCOLS_PROP_NAME + "=myEnabledProtocolsPropValue&" +
         TransportConstants.TCP_NODELAY_PROPNAME + "=myTCPNoDelayPropValue&" +
         TransportConstants.TCP_SENDBUFFER_SIZE_PROPNAME + "=myTCPSendbufferSizePropValue&" +
         TransportConstants.TCP_RECEIVEBUFFER_SIZE_PROPNAME + "=myTCPReceivebufferSizePropValue&" +
         TransportConstants.NIO_REMOTING_THREADS_PROPNAME + "=myNIORemotingThreadsPropValue&" +
         TransportConstants.BATCH_DELAY + "=myBatchDelay&" +
         ActiveMQDefaultConfiguration.getPropMaskPassword() + "=myPropMaskPassword&" +
         ActiveMQDefaultConfiguration.getPropPasswordCodec() + "=myPropPasswordCodec&" +
         TransportConstants.NETTY_CONNECT_TIMEOUT + "=myNettyConnectTimeout&");
      Context ctx = new InitialContext(props);

      ActiveMQConnectionFactory cf = (ActiveMQConnectionFactory) ctx.lookup("myConnectionFactory");

      Map<String, Object> parametersFromJNDI = cf.getServerLocator().getStaticTransportConfigurations()[0].getParams();

      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.SSL_ENABLED_PROP_NAME), "mySSLEnabledPropValue");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.HTTP_ENABLED_PROP_NAME), "myHTTPEnabledPropValue");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.HTTP_CLIENT_IDLE_PROP_NAME), "myHTTPClientIdlePropValue");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.HTTP_CLIENT_IDLE_SCAN_PERIOD), "myHTTPClientIdleScanPeriodValue");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.HTTP_REQUIRES_SESSION_ID), "myHTTPRequiresSessionIDValue");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.HTTP_UPGRADE_ENABLED_PROP_NAME), "myHTTPUpgradeEnabledPropValue");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.HTTP_UPGRADE_ENDPOINT_PROP_NAME), "myHTTPUpgradeEndpointPropValue");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.USE_SERVLET_PROP_NAME), "myUseServletPropValue");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.SERVLET_PATH), "myServletPathValue");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.USE_NIO_PROP_NAME), "myUseNIOPropValue");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.USE_NIO_GLOBAL_WORKER_POOL_PROP_NAME), "myUseNIOGlobalWorkerPoolPropValue");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.LOCAL_ADDRESS_PROP_NAME), "myLocalAddressPropValue");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.LOCAL_PORT_PROP_NAME), "myLocalPortPropValue");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.KEYSTORE_PROVIDER_PROP_NAME), "myKeystoreProviderPropValue");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.KEYSTORE_PATH_PROP_NAME), "myKeystorePathPropValue");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME), "myKeystorePasswordPropValue");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.TRUSTSTORE_PROVIDER_PROP_NAME), "myTruststoreProviderPropValue");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.TRUSTSTORE_PATH_PROP_NAME), "myTruststorePathPropValue");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME), "myTruststorePasswordPropValue");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.ENABLED_CIPHER_SUITES_PROP_NAME), "myEnabledCipherSuitesPropValue");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.ENABLED_PROTOCOLS_PROP_NAME), "myEnabledProtocolsPropValue");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.TCP_NODELAY_PROPNAME), "myTCPNoDelayPropValue");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.TCP_SENDBUFFER_SIZE_PROPNAME), "myTCPSendbufferSizePropValue");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.TCP_RECEIVEBUFFER_SIZE_PROPNAME), "myTCPReceivebufferSizePropValue");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.NIO_REMOTING_THREADS_PROPNAME), "myNIORemotingThreadsPropValue");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.BATCH_DELAY), "myBatchDelay");
      Assert.assertEquals(parametersFromJNDI.get(ActiveMQDefaultConfiguration.getPropMaskPassword()), "myPropMaskPassword");
      Assert.assertEquals(parametersFromJNDI.get(ActiveMQDefaultConfiguration.getPropPasswordCodec()), "myPropPasswordCodec");
      Assert.assertEquals(parametersFromJNDI.get(TransportConstants.NETTY_CONNECT_TIMEOUT), "myNettyConnectTimeout");
   }

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      startServer();
   }

   private void startServer() throws Exception {
      liveTC = new TransportConfiguration(INVM_CONNECTOR_FACTORY);
      Map<String, TransportConfiguration> connectors = new HashMap<>();
      connectors.put(liveTC.getName(), liveTC);
      List<String> connectorNames = new ArrayList<>();
      connectorNames.add(liveTC.getName());

      Map<String, Object> params = new HashMap<>();
      params.put(org.apache.activemq.artemis.core.remoting.impl.invm.TransportConstants.SERVER_ID_PROP_NAME, 1);

      Configuration liveConf = createBasicConfig().addAcceptorConfiguration(new TransportConfiguration(INVM_ACCEPTOR_FACTORY)).addAcceptorConfiguration(new TransportConfiguration(INVM_ACCEPTOR_FACTORY, params)).addAcceptorConfiguration(new TransportConfiguration(NETTY_ACCEPTOR_FACTORY)).setConnectorConfigurations(connectors).setHAPolicyConfiguration(new SharedStoreMasterPolicyConfiguration()).setSecurityEnabled(true);

      final long broadcastPeriod = 250;

      final String bcGroupName = "bc1";

      final int localBindPort = 5432;

      BroadcastGroupConfiguration bcConfig1 = new BroadcastGroupConfiguration().setName(bcGroupName).setBroadcastPeriod(broadcastPeriod).setConnectorInfos(connectorNames).setEndpointFactory(new UDPBroadcastEndpointFactory().setGroupAddress(groupAddress).setGroupPort(groupPort).setLocalBindPort(localBindPort));

      List<BroadcastGroupConfiguration> bcConfigs1 = new ArrayList<>();
      bcConfigs1.add(bcConfig1);
      liveConf.setBroadcastGroupConfigurations(bcConfigs1);

      liveService = addServer(ActiveMQServers.newActiveMQServer(liveConf, false));
      ((ActiveMQJAASSecurityManager) liveService.getSecurityManager()).getConfiguration().addUser("guest", "guest");
      ((ActiveMQJAASSecurityManager) liveService.getSecurityManager()).getConfiguration().setDefaultUser("guest");
      liveService.start();
   }

   @Test
   public void testQueue() throws NamingException, JMSException {
      Hashtable<String, String> props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory");
      props.put("queue.myQueue", "myQueue");
      props.put("queue.queues/myQueue", "myQueue");
      Context ctx = new InitialContext(props);

      Destination destination = (Destination) ctx.lookup("myQueue");
      Assert.assertTrue(destination instanceof Queue);

      destination = (Destination) ctx.lookup("queues/myQueue");
      Assert.assertTrue(destination instanceof Queue);
   }

   @Test
   public void testDynamicQueue() throws NamingException, JMSException {
      Hashtable<String, String> props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory");
      Context ctx = new InitialContext(props);

      Destination destination = (Destination) ctx.lookup("dynamicQueues/myQueue");
      Assert.assertTrue(destination instanceof Queue);
   }

   @Test
   public void testTopic() throws NamingException, JMSException {
      Hashtable<String, String> props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory");
      props.put("topic.myTopic", "myTopic");
      props.put("topic.topics/myTopic", "myTopic");
      Context ctx = new InitialContext(props);

      Destination destination = (Destination) ctx.lookup("myTopic");
      Assert.assertTrue(destination instanceof Topic);

      destination = (Destination) ctx.lookup("topics/myTopic");
      Assert.assertTrue(destination instanceof Topic);
   }

   @Test
   public void testDynamicTopic() throws NamingException, JMSException {
      Hashtable<String, String> props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory");
      Context ctx = new InitialContext(props);

      Destination destination = (Destination) ctx.lookup("dynamicTopics/myTopic");
      Assert.assertTrue(destination instanceof Topic);
   }

   @Test
   public void testRemoteCFWithTCPUserPassword() throws Exception {

      //setup user and role on broker
      ((ActiveMQJAASSecurityManager) liveService.getSecurityManager()).getConfiguration().addUser("myUser", "myPassword");
      ((ActiveMQJAASSecurityManager) liveService.getSecurityManager()).getConfiguration().addRole("myUser", "consumeCreateRole");
      Role consumeCreateRole = new Role("consumeCreateRole", false, true, true, true, true, true, true, true, true, true);
      Set<Role> consumerCreateRoles = new HashSet<>();
      consumerCreateRoles.add(consumeCreateRole);
      liveService.getSecurityRepository().addMatch("test.queue", consumerCreateRoles);

      Hashtable<String, String> props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory");

      //user and password set on URL
      props.put("connectionFactory.myConnectionFactory", "tcp://127.0.0.1:61616?user=myUser&password=myPassword");
      Context ctx = new InitialContext(props);

      //create a connection factory
      ActiveMQConnectionFactory connectionFactory = (ActiveMQConnectionFactory) ctx.lookup("myConnectionFactory");
      Assert.assertEquals("ensure user is set", "myUser", connectionFactory.getUser());
      Assert.assertEquals("ensure password is set", "myPassword", connectionFactory.getPassword());

      //Connect to broker to verify credentials are used with connection
      Connection connection = connectionFactory.createConnection();
      connection.start();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      javax.jms.Queue queue = session.createQueue("test.queue");

      try {

         try {
            MessageProducer producer = session.createProducer(queue);
            producer.send(session.createTextMessage("test Msg"));
            Assert.fail("Sending message should throw a JMSSecurityException");
         } catch (JMSSecurityException e) {
            //expected
         }

         MessageConsumer consumer = session.createConsumer(queue);
      } finally {
         connection.close();
      }

   }

   public void testContext(Context ctx, String jndiName, JMSFactoryType expectedFactoryType) {
      try {
         ActiveMQConnectionFactory connectionFactory = (ActiveMQConnectionFactory) ctx.lookup(jndiName);
         if (expectedFactoryType == null) {
            Assert.fail("expected no factory, should have thrown NamingException");
         } else {
            Assert.assertEquals(expectedFactoryType.intValue(), connectionFactory.getFactoryType());
         }
      } catch (NamingException namingException) {
         Assert.assertNull("NamingException should only occur when no ExpectedFactoryType, but one existed", expectedFactoryType);
      }
   }


   @Test
   public void testProviderUrlDefault() throws NamingException, JMSException {
      Hashtable<String, String> props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory");
      props.put(Context.PROVIDER_URL, "vm://0");
      Context ctx = new InitialContext(props);

      testContext(ctx, "ConnectionFactory", JMSFactoryType.CF);
      testContext(ctx, "QueueConnectionFactory", JMSFactoryType.QUEUE_CF);
      testContext(ctx, "TopicConnectionFactory", JMSFactoryType.TOPIC_CF);
      testContext(ctx, "XAConnectionFactory", JMSFactoryType.XA_CF);
      testContext(ctx, "XAQueueConnectionFactory", JMSFactoryType.QUEUE_XA_CF);
      testContext(ctx, "XATopicConnectionFactory", JMSFactoryType.TOPIC_XA_CF);
   }

   @Test
   public void testProviderUrlCF() throws NamingException, JMSException {
      Hashtable<String, String> props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory");
      props.put(Context.PROVIDER_URL, "vm://0?type=CF");
      Context ctx = new InitialContext(props);

      testContext(ctx, "ConnectionFactory", JMSFactoryType.CF);
      testContext(ctx, "QueueConnectionFactory", null);
      testContext(ctx, "TopicConnectionFactory", null);
      testContext(ctx, "XAConnectionFactory", null);
      testContext(ctx, "XAQueueConnectionFactory", null);
      testContext(ctx, "XATopicConnectionFactory", null);
   }

   @Test
   public void testProviderUrlXACF() throws NamingException, JMSException {
      Hashtable<String, String> props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory");
      props.put(Context.PROVIDER_URL, "vm://0?type=XA_CF");
      Context ctx = new InitialContext(props);

      testContext(ctx, "ConnectionFactory", null);
      testContext(ctx, "QueueConnectionFactory", null);
      testContext(ctx, "TopicConnectionFactory", null);
      testContext(ctx, "XAConnectionFactory", JMSFactoryType.XA_CF);
      testContext(ctx, "XAQueueConnectionFactory", null);
      testContext(ctx, "XATopicConnectionFactory", null);
   }

   @Test
   public void testProviderUrlQueueCF() throws NamingException, JMSException {
      Hashtable<String, String> props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory");
      props.put(Context.PROVIDER_URL, "vm://0?type=QUEUE_CF");
      Context ctx = new InitialContext(props);

      testContext(ctx, "ConnectionFactory", null);
      testContext(ctx, "QueueConnectionFactory", JMSFactoryType.QUEUE_CF);
      testContext(ctx, "TopicConnectionFactory", null);
      testContext(ctx, "XAConnectionFactory", null);
      testContext(ctx, "XAQueueConnectionFactory", null);
      testContext(ctx, "XATopicConnectionFactory", null);
   }

   @Test
   public void testProviderUrlQueueXACF() throws NamingException, JMSException {
      Hashtable<String, String> props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory");
      props.put(Context.PROVIDER_URL, "vm://0?type=QUEUE_XA_CF");
      Context ctx = new InitialContext(props);

      testContext(ctx, "ConnectionFactory", null);
      testContext(ctx, "QueueConnectionFactory", null);
      testContext(ctx, "TopicConnectionFactory", null);
      testContext(ctx, "XAConnectionFactory", null);
      testContext(ctx, "XAQueueConnectionFactory", JMSFactoryType.QUEUE_XA_CF);
      testContext(ctx, "XATopicConnectionFactory", null);
   }

   @Test
   public void testProviderUrlTopicCF() throws NamingException, JMSException {
      Hashtable<String, String> props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory");
      props.put(Context.PROVIDER_URL, "vm://0?type=TOPIC_CF");
      Context ctx = new InitialContext(props);

      testContext(ctx, "ConnectionFactory", null);
      testContext(ctx, "QueueConnectionFactory", null);
      testContext(ctx, "TopicConnectionFactory", JMSFactoryType.TOPIC_CF);
      testContext(ctx, "XAConnectionFactory", null);
      testContext(ctx, "XAQueueConnectionFactory", null);
      testContext(ctx, "XATopicConnectionFactory", null);
   }

   @Test
   public void testProviderUrlTopicXACF() throws NamingException, JMSException {
      Hashtable<String, String> props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory");
      props.put(Context.PROVIDER_URL, "vm://0?type=TOPIC_XA_CF");
      Context ctx = new InitialContext(props);

      testContext(ctx, "ConnectionFactory", null);
      testContext(ctx, "QueueConnectionFactory", null);
      testContext(ctx, "TopicConnectionFactory", null);
      testContext(ctx, "XAConnectionFactory", null);
      testContext(ctx, "XAQueueConnectionFactory", null);
      testContext(ctx, "XATopicConnectionFactory", JMSFactoryType.TOPIC_XA_CF);
   }

   @Test
   public void testProviderUrlDefaultAndCustom() throws NamingException, JMSException {
      Hashtable<String, String> props = new Hashtable<>();
      props.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory");
      props.put(Context.PROVIDER_URL, "vm://0");
      props.put("connectionFactory.myConnectionFactory", "vm://0");
      Context ctx = new InitialContext(props);

      testContext(ctx, "ConnectionFactory", JMSFactoryType.CF);
      testContext(ctx, "QueueConnectionFactory", JMSFactoryType.QUEUE_CF);
      testContext(ctx, "TopicConnectionFactory", JMSFactoryType.TOPIC_CF);
      testContext(ctx, "XAConnectionFactory", JMSFactoryType.XA_CF);
      testContext(ctx, "XAQueueConnectionFactory", JMSFactoryType.QUEUE_XA_CF);
      testContext(ctx, "XATopicConnectionFactory", JMSFactoryType.TOPIC_XA_CF);

      testContext(ctx, "myConnectionFactory", JMSFactoryType.CF);

   }
}