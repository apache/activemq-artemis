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
package org.apache.activemq.artemis.tests.integration.client;

import io.netty.bootstrap.Bootstrap;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.client.impl.ClientSessionFactoryImpl;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnector;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Before;
import org.junit.Test;

public class NettyConnectorTest extends ActiveMQTestBase {

   private ActiveMQServer server;

   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();

      server = createServer(false, createDefaultNettyConfig());
      server.start();
   }

   //make sure the 'connect-timeout' passed to netty.
   @Test
   public void testConnectionTimeoutConfig() throws Exception {
      final int timeout = 23456;
      TransportConfiguration transport = new TransportConfiguration(NETTY_CONNECTOR_FACTORY);
      transport.getParams().put(TransportConstants.NETTY_CONNECT_TIMEOUT, timeout);

      ServerLocator locator = ActiveMQClient.createServerLocatorWithoutHA(transport);

      ClientSessionFactoryImpl factory = (ClientSessionFactoryImpl) locator.createSessionFactory();
      NettyConnector connector = (NettyConnector) factory.getConnector();

      Bootstrap bootstrap = connector.getBootStrap();

      assertEquals(timeout, bootstrap.register().channel().config().getConnectTimeoutMillis());

      factory.close();
      locator.close();
   }
}
