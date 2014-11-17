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
package org.apache.activemq6.tests.integration.jms.connection;
import org.junit.Before;
import org.junit.After;

import org.junit.Test;

import org.apache.activemq6.api.core.TransportConfiguration;
import org.apache.activemq6.api.core.client.HornetQClient;
import org.apache.activemq6.api.jms.JMSFactoryType;
import org.apache.activemq6.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq6.jms.client.HornetQConnectionFactory;
import org.apache.activemq6.tests.util.JMSTestBase;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Justin Bertram
 */
public class InvalidConnectorTest extends JMSTestBase
{
   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      cf = null;

      super.tearDown();
   }

   @Test
   public void testInvalidConnector() throws Exception
   {
      Map<String, Object> params = new HashMap<String, Object>();
      params.put(TransportConstants.HOST_PROP_NAME, "0.0.0.0");

      List<TransportConfiguration> connectorConfigs = new ArrayList<TransportConfiguration>();
      connectorConfigs.add(new TransportConfiguration(NETTY_CONNECTOR_FACTORY, params));


      int retryInterval = 1000;
      double retryIntervalMultiplier = 1.0;
      int reconnectAttempts = -1;
      int callTimeout = 30000;

      jmsServer.createConnectionFactory("invalid-cf",
            false,
            JMSFactoryType.CF,
            registerConnectors(server, connectorConfigs),
            null,
            HornetQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD,
            HornetQClient.DEFAULT_CONNECTION_TTL,
            callTimeout,
            HornetQClient.DEFAULT_CALL_FAILOVER_TIMEOUT,
            HornetQClient.DEFAULT_CACHE_LARGE_MESSAGE_CLIENT,
            HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
            HornetQClient.DEFAULT_COMPRESS_LARGE_MESSAGES,
            HornetQClient.DEFAULT_CONSUMER_WINDOW_SIZE,
            HornetQClient.DEFAULT_CONSUMER_MAX_RATE,
            HornetQClient.DEFAULT_CONFIRMATION_WINDOW_SIZE,
            HornetQClient.DEFAULT_PRODUCER_WINDOW_SIZE,
            HornetQClient.DEFAULT_PRODUCER_MAX_RATE,
            HornetQClient.DEFAULT_BLOCK_ON_ACKNOWLEDGE,
            HornetQClient.DEFAULT_BLOCK_ON_DURABLE_SEND,
            HornetQClient.DEFAULT_BLOCK_ON_NON_DURABLE_SEND,
            HornetQClient.DEFAULT_AUTO_GROUP,
            HornetQClient.DEFAULT_PRE_ACKNOWLEDGE,
            HornetQClient.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME,
            HornetQClient.DEFAULT_ACK_BATCH_SIZE,
            HornetQClient.DEFAULT_ACK_BATCH_SIZE,
            HornetQClient.DEFAULT_USE_GLOBAL_POOLS,
            HornetQClient.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE,
            HornetQClient.DEFAULT_THREAD_POOL_MAX_SIZE,
            retryInterval,
            retryIntervalMultiplier,
            HornetQClient.DEFAULT_MAX_RETRY_INTERVAL,
            reconnectAttempts,
            HornetQClient.DEFAULT_FAILOVER_ON_INITIAL_CONNECTION,
            null,
            "/invalid-cf");

      HornetQConnectionFactory invalidCf = (HornetQConnectionFactory) namingContext.lookup("/invalid-cf");

      TransportConfiguration[] tcs = invalidCf.getServerLocator().getStaticTransportConfigurations();

      TransportConfiguration tc = tcs[0];

      assertNotSame(tc.getParams().get(TransportConstants.HOST_PROP_NAME), "0.0.0.0");
      assertEquals(tc.getParams().get(TransportConstants.HOST_PROP_NAME), InetAddress.getLocalHost().getHostName());
   }
}