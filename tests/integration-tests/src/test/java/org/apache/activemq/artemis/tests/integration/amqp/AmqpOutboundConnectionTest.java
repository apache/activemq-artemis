/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.amqp;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnector;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.protocol.amqp.broker.ProtonProtocolManagerFactory;
import org.apache.activemq.artemis.protocol.amqp.client.AMQPClientConnectionFactory;
import org.apache.activemq.artemis.protocol.amqp.client.ProtonClientConnectionManager;
import org.apache.activemq.artemis.protocol.amqp.client.ProtonClientProtocolManager;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.qpid.proton.amqp.Symbol;
import org.junit.Test;

public class AmqpOutboundConnectionTest extends AmqpClientTestSupport {

   @Test(timeout = 60000)
   public void testOutboundConnection() throws Throwable {
      final ActiveMQServer remote = createServer(AMQP_PORT + 1);
      remote.start();
      try {
         Wait.waitFor(remote::isActive);
      } catch (Exception e) {
         remote.stop();
         throw e;
      }

      final Map<String, Object> config = new LinkedHashMap<>();
      config.put(TransportConstants.HOST_PROP_NAME, "localhost");
      config.put(TransportConstants.PORT_PROP_NAME, String.valueOf(AMQP_PORT + 1));
      ProtonClientConnectionManager lifeCycleListener = new ProtonClientConnectionManager(new AMQPClientConnectionFactory(server, "myid", Collections.singletonMap(Symbol.getSymbol("myprop"), "propvalue"), 5000), Optional.empty());
      ProtonClientProtocolManager protocolManager = new ProtonClientProtocolManager(new ProtonProtocolManagerFactory(), server);
      NettyConnector connector = new NettyConnector(config, lifeCycleListener, lifeCycleListener, server.getExecutorFactory().getExecutor(), server.getExecutorFactory().getExecutor(), server.getScheduledPool(), protocolManager);
      connector.start();
      connector.createConnection();

      try {
         Wait.waitFor(() -> remote.getConnectionCount() > 0);
         assertEquals(1, remote.getConnectionCount());

         lifeCycleListener.stop();

         Wait.waitFor(() -> remote.getConnectionCount() == 0);
         assertEquals(0, remote.getConnectionCount());
      } finally {
         lifeCycleListener.stop();
         remote.stop();
      }
   }
}
