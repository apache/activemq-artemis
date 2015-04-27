/**
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
package org.apache.activemq.spi.core.protocol;

import java.util.List;

import io.netty.channel.ChannelPipeline;
import org.apache.activemq.api.core.ActiveMQBuffer;
import org.apache.activemq.api.core.BaseInterceptor;
import org.apache.activemq.core.remoting.impl.netty.NettyServerConnection;
import org.apache.activemq.spi.core.remoting.Acceptor;
import org.apache.activemq.spi.core.remoting.Connection;

public interface ProtocolManager<P extends BaseInterceptor>
{
   ProtocolManagerFactory<P> getFactory();

   /**
    * This method will receive all the interceptors on the system and you should filter them out *
    *
    * @param incomingInterceptors
    * @param outgoingInterceptors
    */
   void updateInterceptors(List<BaseInterceptor> incomingInterceptors, List<BaseInterceptor> outgoingInterceptors);

   ConnectionEntry createConnectionEntry(Acceptor acceptorUsed, Connection connection);

   void removeHandler(final String name);

   void handleBuffer(RemotingConnection connection, ActiveMQBuffer buffer);

   void addChannelHandlers(ChannelPipeline pipeline);

   boolean isProtocol(byte[] array);

   /**
    * Gets the Message Converter towards ActiveMQ.
    * Notice this being null means no need to convert
    *
    * @return
    */
   MessageConverter getConverter();

   void handshake(NettyServerConnection connection, ActiveMQBuffer buffer);
}
