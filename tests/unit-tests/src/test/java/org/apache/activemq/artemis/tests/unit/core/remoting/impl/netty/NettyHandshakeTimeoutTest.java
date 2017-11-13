/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.unit.core.remoting.impl.netty;

import io.netty.buffer.ByteBuf;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.junit.Wait;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.transport.netty.NettyTransport;
import org.apache.activemq.transport.netty.NettyTransportFactory;
import org.apache.activemq.transport.netty.NettyTransportListener;
import org.junit.Test;

import java.net.URI;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

public class NettyHandshakeTimeoutTest extends ActiveMQTestBase {

   protected ActiveMQServer server;
   private Configuration conf;

   @Test
   public void testHandshakeTimeout() throws Exception {
      int handshakeTimeout = 3;

      setUp();
      ActiveMQTestBase.checkFreePort(TransportConstants.DEFAULT_PORT);
      HashMap<String, Object> params = new HashMap<>();
      params.put(TransportConstants.HANDSHAKE_TIMEOUT, handshakeTimeout);

      conf = createDefaultInVMConfig().clearAcceptorConfigurations().addAcceptorConfiguration(new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, params));
      server = addServer(ActiveMQServers.newActiveMQServer(conf, false));
      server.start();
      NettyTransport transport = NettyTransportFactory.createTransport(new URI("tcp://127.0.0.1:61616"));
      transport.setTransportListener(new NettyTransportListener() {
         @Override
         public void onData(ByteBuf incoming) {

         }

         @Override
         public void onTransportClosed() {
         }

         @Override
         public void onTransportError(Throwable cause) {
         }

      });

      try {
         transport.connect();
         assertTrue("Connection should be closed now", Wait.waitFor(() -> !transport.isConnected(), TimeUnit.SECONDS.toMillis(handshakeTimeout + 1)));
      } finally {
         transport.close();
         tearDown();
      }
   }
}
