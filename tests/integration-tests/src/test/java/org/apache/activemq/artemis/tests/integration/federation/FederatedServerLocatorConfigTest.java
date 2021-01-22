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
package org.apache.activemq.artemis.tests.integration.federation;

import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.FederationConfiguration;
import org.apache.activemq.artemis.core.config.federation.FederationUpstreamConfiguration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.server.federation.Federation;
import org.apache.activemq.artemis.core.server.federation.FederationUpstream;
import org.junit.Before;
import org.junit.Test;

public class FederatedServerLocatorConfigTest extends FederatedTestBase {


   @Override
   @Before
   public void setUp() throws Exception {
      super.setUp();
   }

   @Override
   protected int numberOfServers() {
      return 2;
   }

   @Override
   protected Configuration createDefaultConfig(final int serverID, final boolean netty) throws Exception {

      ConfigurationImpl configuration = createBasicConfig(serverID).setJMXManagementEnabled(false).addAcceptorConfiguration(new TransportConfiguration(INVM_ACCEPTOR_FACTORY, generateInVMParams(serverID), "invm"));

      HashMap<String, Object> params = new HashMap<>();
      params.put(org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants.PORT_PROP_NAME, org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants.DEFAULT_PORT + serverID);

      configuration.addAcceptorConfiguration(new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, params, "netty", new HashMap<>()));

      return configuration;
   }

   @Test
   public void testFederatedAddressServerLocatorConfigFromUrl() throws Exception {
      String address = getName();

      // we still won't pick up url params, restricted locator config is explicit
      // bundling getUseTopologyForLoadBalancing with HA=false is consistent with simplification
      String connectorName = "server1WithUrlParams";
      Map<String, Object> params = new HashMap<>();
      params.put("host", "localhost");
      params.put("port",  org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants.DEFAULT_PORT + 1);

      getServer(0).getConfiguration().addConnectorConfiguration(connectorName, new TransportConfiguration(NETTY_CONNECTOR_FACTORY, params));

      FederationConfiguration federationConfiguration0 = FederatedTestUtil.createAddressUpstreamFederationConfiguration("server1WithUrlParams", address, 2);
      for (FederationUpstreamConfiguration upstreamConfiguration : federationConfiguration0.getUpstreamConfigurations()) {
         upstreamConfiguration.getConnectionConfiguration().setHA(false);
      }
      getServer(0).getConfiguration().getFederationConfigurations().add(federationConfiguration0);
      getServer(0).getFederationManager().deploy();


      // lets peek at the server locator
      Federation fed = getServer(0).getFederationManager().get(federationConfiguration0.getName());
      assertNotNull(fed);
      FederationUpstream federationUpstream = fed.get(connectorName);
      assertFalse(federationUpstream.getConnection().clientSessionFactory().getServerLocator().getUseTopologyForLoadBalancing());
   }
}
