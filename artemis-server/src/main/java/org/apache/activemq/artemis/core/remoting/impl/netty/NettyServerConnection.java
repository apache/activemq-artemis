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
package org.apache.activemq.artemis.core.remoting.impl.netty;

import java.util.Map;

import io.netty.channel.Channel;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.core.buffers.impl.ChannelBufferWrapper;
import org.apache.activemq.artemis.spi.core.remoting.ServerConnectionLifeCycleListener;

public class NettyServerConnection extends NettyConnection {

   public NettyServerConnection(Map<String, Object> configuration,
                                Channel channel,
                                ServerConnectionLifeCycleListener listener,
                                boolean batchingEnabled,
                                boolean directDeliver) {
      super(configuration, channel, listener, batchingEnabled, directDeliver);
   }

   @Override
   public ActiveMQBuffer createTransportBuffer(int size) {
      return new ChannelBufferWrapper(channel.alloc().directBuffer(size), true);
   }
}
