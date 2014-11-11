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
package org.apache.activemq6.tests.integration.remoting;

import org.apache.activemq6.core.remoting.impl.netty.NettyAcceptorFactory;
import org.apache.activemq6.core.remoting.impl.netty.NettyConnectorFactory;
import org.apache.activemq6.core.remoting.impl.netty.TransportConstants;

/**
 * A NettyNetworkAddressTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 *
 */
public class NettyNetworkAddressTest extends NetworkAddressTestBase
{

   @Override
   protected String getAcceptorFactoryClassName()
   {
      return NettyAcceptorFactory.class.getName();
   }

   @Override
   protected String getConnectorFactoryClassName()
   {
      return NettyConnectorFactory.class.getName();
   }

   @Override
   protected String getHostPropertyKey()
   {
      return TransportConstants.HOST_PROP_NAME;
   }

   @Override
   protected String getLocalPortProperty()
   {
      return TransportConstants.LOCAL_PORT_PROP_NAME;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
