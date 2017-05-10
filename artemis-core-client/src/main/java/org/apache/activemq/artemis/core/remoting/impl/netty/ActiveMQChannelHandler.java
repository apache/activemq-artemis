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

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.group.ChannelGroup;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.core.buffers.impl.ChannelBufferWrapper;
import org.apache.activemq.artemis.core.client.ActiveMQClientLogger;
import org.apache.activemq.artemis.core.client.ActiveMQClientMessageBundle;
import org.apache.activemq.artemis.spi.core.remoting.BaseConnectionLifeCycleListener;
import org.apache.activemq.artemis.spi.core.remoting.BufferHandler;

/**
 * Common handler implementation for client and server side handler.
 */
public class ActiveMQChannelHandler extends ChannelDuplexHandler {

   private final ChannelGroup group;

   private final BufferHandler handler;

   private final BaseConnectionLifeCycleListener<?> listener;

   volatile boolean active;

   protected ActiveMQChannelHandler(final ChannelGroup group,
                                    final BufferHandler handler,
                                    final BaseConnectionLifeCycleListener<?> listener) {
      this.group = group;
      this.handler = handler;
      this.listener = listener;
   }

   @Override
   public void channelActive(final ChannelHandlerContext ctx) throws Exception {
      group.add(ctx.channel());
      ctx.fireChannelActive();
   }

   @Override
   public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
      listener.connectionReadyForWrites(channelId(ctx.channel()), ctx.channel().isWritable());
   }

   @Override
   public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
      ByteBuf buffer = (ByteBuf) msg;

      try {
         handler.bufferReceived(channelId(ctx.channel()), new ChannelBufferWrapper(buffer));
      } finally {
         buffer.release();
      }
   }

   @Override
   public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
      synchronized (this) {
         if (active) {
            listener.connectionDestroyed(channelId(ctx.channel()));

            active = false;
         }
      }

      super.channelInactive(ctx);
   }

   @Override
   public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) throws Exception {
      if (!active) {
         return;
      }
      // We don't want to log this - since it is normal for this to happen during failover/reconnect
      // and we don't want to spew out stack traces in that event
      // The user has access to this exeception anyway via the ActiveMQException initial cause

      ActiveMQException me = ActiveMQClientMessageBundle.BUNDLE.nettyError();
      me.initCause(cause);

      synchronized (listener) {
         try {
            listener.connectionException(channelId(ctx.channel()), me);
            active = false;
         } catch (Exception ex) {
            ActiveMQClientLogger.LOGGER.errorCallingLifeCycleListener(ex);
         }
      }
   }

   protected static Object channelId(Channel channel) {
      return channel.id();
   }
}
