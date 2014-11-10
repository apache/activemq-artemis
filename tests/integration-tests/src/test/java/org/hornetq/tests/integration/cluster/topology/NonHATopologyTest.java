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
package org.hornetq.tests.integration.cluster.topology;

import org.junit.Test;

import java.util.ArrayList;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.core.client.impl.ServerLocatorInternal;
import org.hornetq.core.client.impl.Topology;
import org.hornetq.core.client.impl.TopologyMemberImpl;
import org.hornetq.core.config.ClusterConnectionConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * I have added this test to help validate if the connectors from Recovery will be
 * properly updated
 *
 * Created to verify HORNETQ-913 / AS7-4548
 *
 * @author clebertsuconic
 *
 *
 */
public class NonHATopologyTest extends ServiceTestBase
{

   @Test
   public void testNetty() throws Exception
   {
      internalTest(true);
   }

   @Test
   public void testInVM() throws Exception
   {
      internalTest(false);
   }

   public void internalTest(boolean isNetty) throws Exception
   {

      HornetQServer server = null;
      ServerLocatorInternal locator = null;

      try
      {

         server = createServer(false, isNetty);

         if (!isNetty)
         {
            server.getConfiguration()
                  .getAcceptorConfigurations()
                  .add(new TransportConfiguration(NETTY_ACCEPTOR_FACTORY));
            server.getConfiguration()
                  .getConnectorConfigurations()
                  .put("netty", new TransportConfiguration(NETTY_CONNECTOR_FACTORY));

            ArrayList<String> list = new ArrayList<String>();
            list.add("netty");
            Configuration config = server.getConfiguration();
            config.getClusterConfigurations().add(new ClusterConnectionConfiguration()
               .setName("tst")
               .setAddress("jms")
               .setConnectorName("netty")
               .setRetryInterval(1000)
               .setConfirmationWindowSize(1000)
               .setStaticConnectors(list)
               .setAllowDirectConnectionsOnly(true));
         }

         server.start();

         locator = (ServerLocatorInternal)createNonHALocator(isNetty);

         ClientSessionFactory factory = createSessionFactory(locator);

         Topology topology = locator.getTopology();

         assertEquals(1, topology.getMembers().size());

         factory.close();

         if (!isNetty)
         {
            TopologyMemberImpl member = topology.getMembers().iterator().next();
            if (isNetty)
            {
               assertEquals(NettyConnectorFactory.class.getName(), member.getLive().getFactoryClassName());
            }
            else
            {
               assertEquals(InVMConnectorFactory.class.getName(), member.getLive().getFactoryClassName());
            }
         }

      }
      finally
      {
         try
         {
            locator.close();
         }
         catch (Exception ignored)
         {
         }

         try
         {
            server.stop();
         }
         catch (Exception ignored)
         {
         }

         server = null;

         locator = null;
      }

   }
}
