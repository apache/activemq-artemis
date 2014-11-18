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
package org.apache.activemq.tests.integration.client;

import io.netty.bootstrap.Bootstrap;
import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.api.core.client.ActiveMQClient;
import org.apache.activemq.api.core.client.ServerLocator;
import org.apache.activemq.core.client.impl.ClientSessionFactoryImpl;
import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.core.remoting.impl.netty.NettyConnector;
import org.apache.activemq.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.core.server.ActiveMQServer;
import org.apache.activemq.tests.util.ServiceTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
 */
public class NettyConnectorTest extends ServiceTestBase
{
   private ActiveMQServer server;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      Configuration config = this.createDefaultConfig(true);
      server = this.createServer(false, config);
      server.start();
   }

   //make sure the 'connect-timeout' passed to netty.
   @Test
   public void testConnectionTimeoutConfig() throws Exception
   {
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

   @Override
   @After
   public void tearDown() throws Exception
   {
      super.tearDown();
      server.stop();
   }
}
