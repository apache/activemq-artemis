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
package org.apache.activemq.tests.integration.cluster.failover.remote;

import org.apache.activemq.api.core.ActiveMQException;
import org.apache.activemq.api.core.ActiveMQNotConnectedException;
import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.api.core.client.HornetQClient;
import org.apache.activemq.api.core.client.ServerLocator;
import org.apache.activemq.core.remoting.impl.netty.NettyConnectorFactory;
import org.apache.activemq.tests.integration.cluster.distribution.ClusterTestBase;
import org.junit.Test;

/**
 * A ServerTest
 *
 * @author jmesnil
 */
public class FailoverWithSharedStoreTest extends ClusterTestBase
{

   @Test
   public void testNoConnection() throws Exception
   {
      ServerLocator locator = HornetQClient.createServerLocatorWithHA(new TransportConfiguration(NettyConnectorFactory.class.getName()));
      try
      {
         createSessionFactory(locator);
         fail();
      }
      catch (ActiveMQNotConnectedException nce)
      {
         //ok
      }
      catch (ActiveMQException e)
      {
         fail("Invalid Exception type:" + e.getType());
      }
   }
}
