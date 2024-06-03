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
package org.apache.activemq.artemis.protocol.amqp.proton;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.EventLoop;
import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnection;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPConnectionCallback;
import org.apache.activemq.artemis.protocol.amqp.broker.ProtonProtocolManager;
import org.apache.activemq.artemis.utils.ExecutorFactory;
import org.apache.activemq.artemis.utils.actors.ArtemisExecutor;
import org.apache.qpid.proton.engine.Connection;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class AMQPConnectionContextTest {

   @Test
   public void testLeakAfterClose() throws Exception {
      ArtemisExecutor executor = Mockito.mock(ArtemisExecutor.class);
      ExecutorFactory executorFactory = Mockito.mock(ExecutorFactory.class);
      Mockito.when(executorFactory.getExecutor()).thenReturn(executor);

      ActiveMQServer server = Mockito.mock(ActiveMQServer.class);
      Mockito.when(server.getExecutorFactory()).thenReturn(executorFactory);

      ProtonProtocolManager manager = Mockito.mock(ProtonProtocolManager.class);
      Mockito.when(manager.getServer()).thenReturn(server);

      EventLoop eventLoop = Mockito.mock(EventLoop.class);
      Channel transportChannel = Mockito.mock(Channel.class);
      Mockito.when(transportChannel.config()).thenReturn(Mockito.mock(ChannelConfig.class));
      Mockito.when(transportChannel.eventLoop()).thenReturn(eventLoop);
      Mockito.when(eventLoop.inEventLoop()).thenReturn(true);
      NettyConnection transportConnection = new NettyConnection(new HashMap<>(), transportChannel, null, false, false);

      Connection connection = Mockito.mock(Connection.class);
      AMQPConnectionCallback protonSPI = Mockito.mock(AMQPConnectionCallback.class);
      Mockito.when(protonSPI.getTransportConnection()).thenReturn(transportConnection);
      Mockito.when(protonSPI.validateConnection(connection, null)).thenReturn(true);

      ScheduledThreadPoolExecutor scheduledPool = new ScheduledThreadPoolExecutor(
         ActiveMQDefaultConfiguration.getDefaultScheduledThreadPoolMaxSize());

      AMQPConnectionContext connectionContext = new AMQPConnectionContext(
         manager,
         protonSPI,
         null,
         (int) ActiveMQClient.DEFAULT_CONNECTION_TTL,
         manager.getMaxFrameSize(),
         AMQPConstants.Connection.DEFAULT_CHANNEL_MAX,
         false,
         scheduledPool,
         false,
         null,
         null);

      connectionContext.onRemoteOpen(connection);

      connectionContext.close(null);

      assertEquals(0, scheduledPool.getQueue().size());
   }
}
