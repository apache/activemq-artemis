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
package org.apache.activemq.tests.unit.core.remoting.impl.netty;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

import org.apache.activemq.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.api.core.ActiveMQBuffer;
import org.apache.activemq.api.core.ActiveMQException;
import org.apache.activemq.core.remoting.impl.netty.NettyAcceptor;
import org.apache.activemq.core.remoting.impl.netty.NettyAcceptorFactory;
import org.apache.activemq.core.server.ActiveMQComponent;
import org.apache.activemq.spi.core.remoting.Acceptor;
import org.apache.activemq.spi.core.remoting.BufferHandler;
import org.apache.activemq.spi.core.remoting.Connection;
import org.apache.activemq.spi.core.remoting.ConnectionLifeCycleListener;
import org.apache.activemq.tests.util.UnitTestCase;
import org.junit.Assert;
import org.junit.Test;

/**
 * A NettyAcceptorFactoryTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class NettyAcceptorFactoryTest extends UnitTestCase
{
   @Test
   public void testCreateAcceptor() throws Exception
   {
      NettyAcceptorFactory factory = new NettyAcceptorFactory();

      Map<String, Object> params = new HashMap<String, Object>();
      BufferHandler handler = new BufferHandler()
      {

         public void bufferReceived(final Object connectionID, final ActiveMQBuffer buffer)
         {
         }
      };

      ConnectionLifeCycleListener listener = new ConnectionLifeCycleListener()
      {

         public void connectionException(final Object connectionID, final ActiveMQException me)
         {
         }

         public void connectionDestroyed(final Object connectionID)
         {
         }

         public void connectionCreated(ActiveMQComponent component, final Connection connection, final String protocol)
         {
         }

         public void connectionReadyForWrites(Object connectionID, boolean ready)
         {
         }


      };

      Acceptor acceptor = factory.createAcceptor("netty",
                                                 null,
                                                 params,
                                                 handler,
                                                 listener,
                                                 Executors.newCachedThreadPool(),
                                                 Executors.newScheduledThreadPool(ActiveMQDefaultConfiguration.getDefaultScheduledThreadPoolMaxSize()),
                                                 null);

      Assert.assertTrue(acceptor instanceof NettyAcceptor);
   }
}
