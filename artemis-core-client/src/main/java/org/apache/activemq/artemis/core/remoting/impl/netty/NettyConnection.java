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

import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Semaphore;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.handler.ssl.SslHandler;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.api.core.ActiveMQInterruptedException;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.buffers.impl.ChannelBufferWrapper;
import org.apache.activemq.artemis.core.client.ActiveMQClientLogger;
import org.apache.activemq.artemis.core.security.ActiveMQPrincipal;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.apache.activemq.artemis.spi.core.remoting.ConnectionLifeCycleListener;
import org.apache.activemq.artemis.spi.core.remoting.ReadyListener;
import org.apache.activemq.artemis.utils.IPV6Util;

public class NettyConnection implements Connection {

   // Constants -----------------------------------------------------
   private static final int BATCHING_BUFFER_SIZE = 8192;

   // Attributes ----------------------------------------------------

   protected final Channel channel;

   private boolean closed;

   private final ConnectionLifeCycleListener listener;

   private final boolean batchingEnabled;

   private final boolean directDeliver;

   private volatile ActiveMQBuffer batchBuffer;

   private final Map<String, Object> configuration;

   private final Semaphore writeLock = new Semaphore(1);

   private RemotingConnection protocolConnection;

   private boolean ready = true;

   /** if {@link #isWritable(ReadyListener)} returns false, we add a callback
    *  here for when the connection (or Netty Channel) becomes available again. */
   private final ConcurrentLinkedDeque<ReadyListener> readyListeners = new ConcurrentLinkedDeque<>();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public NettyConnection(final Map<String, Object> configuration,
                          final Channel channel,
                          final ConnectionLifeCycleListener listener,
                          boolean batchingEnabled,
                          boolean directDeliver) {
      this.configuration = configuration;

      this.channel = channel;

      this.listener = listener;

      this.batchingEnabled = batchingEnabled;

      this.directDeliver = directDeliver;
   }

   // Public --------------------------------------------------------

   public Channel getNettyChannel() {
      return channel;
   }
   // Connection implementation ----------------------------


   @Override
   public boolean isWritable(ReadyListener callback) {
      synchronized (readyListeners) {
         readyListeners.push(callback);

         return ready;
      }
   }

   @Override
   public void fireReady(final boolean ready) {
      synchronized (readyListeners) {
         this.ready = ready;

         if (ready) {
            for (;;) {
               ReadyListener readyListener = readyListeners.poll();
               if (readyListener == null) {
                  return;
               }

               try {
                  readyListener.readyForWriting();
               }
               catch (Throwable logOnly) {
                  ActiveMQClientLogger.LOGGER.warn(logOnly.getMessage(), logOnly);
               }
            }
         }
      }
   }

   @Override
   public void forceClose() {
      if (channel != null) {
         try {
            channel.close();
         }
         catch (Throwable e) {
            ActiveMQClientLogger.LOGGER.warn(e.getMessage(), e);
         }
      }
   }

   /**
    * This is exposed so users would have the option to look at any data through interceptors
    *
    * @return
    */
   public Channel getChannel() {
      return channel;
   }

   @Override
   public RemotingConnection getProtocolConnection() {
      return protocolConnection;
   }

   @Override
   public void setProtocolConnection(RemotingConnection protocolConnection) {
      this.protocolConnection = protocolConnection;
   }

   @Override
   public void close() {
      if (closed) {
         return;
      }

      final SslHandler sslHandler = (SslHandler) channel.pipeline().get("ssl");
      EventLoop eventLoop = channel.eventLoop();
      boolean inEventLoop = eventLoop.inEventLoop();
      //if we are in an event loop we need to close the channel after the writes have finished
      if (!inEventLoop) {
         closeSSLAndChannel(sslHandler, channel);
      }
      else {
         eventLoop.execute(new Runnable() {
            @Override
            public void run() {
               closeSSLAndChannel(sslHandler, channel);
            }
         });
      }

      closed = true;

      listener.connectionDestroyed(getID());
   }

   @Override
   public ActiveMQBuffer createTransportBuffer(final int size) {
      return new ChannelBufferWrapper(PartialPooledByteBufAllocator.INSTANCE.directBuffer(size), true);
   }

   @Override
   public Object getID() {
      // TODO: Think of it
      return channel.hashCode();
   }

   // This is called periodically to flush the batch buffer
   @Override
   public void checkFlushBatchBuffer() {
      if (!batchingEnabled) {
         return;
      }

      if (writeLock.tryAcquire()) {
         try {
            if (batchBuffer != null && batchBuffer.readable()) {
               channel.writeAndFlush(batchBuffer.byteBuf());

               batchBuffer = createTransportBuffer(BATCHING_BUFFER_SIZE);
            }
         }
         finally {
            writeLock.release();
         }
      }
   }

   @Override
   public void write(final ActiveMQBuffer buffer) {
      write(buffer, false, false);
   }

   @Override
   public void write(ActiveMQBuffer buffer, final boolean flush, final boolean batched) {
      write(buffer, flush, batched, null);
   }

   @Override
   public void write(ActiveMQBuffer buffer,
                     final boolean flush,
                     final boolean batched,
                     final ChannelFutureListener futureListener) {

      try {
         writeLock.acquire();

         try {
            if (batchBuffer == null && batchingEnabled && batched && !flush) {
               // Lazily create batch buffer

               batchBuffer = ActiveMQBuffers.dynamicBuffer(BATCHING_BUFFER_SIZE);
            }

            if (batchBuffer != null) {
               batchBuffer.writeBytes(buffer, 0, buffer.writerIndex());

               if (batchBuffer.writerIndex() >= BATCHING_BUFFER_SIZE || !batched || flush) {
                  // If the batch buffer is full or it's flush param or not batched then flush the buffer

                  buffer = batchBuffer;
               }
               else {
                  return;
               }

               if (!batched || flush) {
                  batchBuffer = null;
               }
               else {
                  // Create a new buffer

                  batchBuffer = ActiveMQBuffers.dynamicBuffer(BATCHING_BUFFER_SIZE);
               }
            }

            // depending on if we need to flush or not we can use a voidPromise or
            // use a normal promise
            final ByteBuf buf = buffer.byteBuf();
            final ChannelPromise promise;
            if (flush || futureListener != null) {
               promise = channel.newPromise();
            }
            else {
               promise = channel.voidPromise();
            }

            EventLoop eventLoop = channel.eventLoop();
            boolean inEventLoop = eventLoop.inEventLoop();
            if (!inEventLoop) {
               if (futureListener != null) {
                  channel.writeAndFlush(buf, promise).addListener(futureListener);
               }
               else {
                  channel.writeAndFlush(buf, promise);
               }
            }
            else {
               // create a task which will be picked up by the eventloop and trigger the write.
               // This is mainly needed as this method is triggered by different threads for the same channel.
               // if we not do this we may produce out of order writes.
               final Runnable task = new Runnable() {
                  @Override
                  public void run() {
                     if (futureListener != null) {
                        channel.writeAndFlush(buf, promise).addListener(futureListener);
                     }
                     else {
                        channel.writeAndFlush(buf, promise);
                     }
                  }
               };
               // execute the task on the eventloop
               eventLoop.execute(task);
            }

            // only try to wait if not in the eventloop otherwise we will produce a deadlock
            if (flush && !inEventLoop) {
               while (true) {
                  try {
                     boolean ok = promise.await(10000);

                     if (!ok) {
                        ActiveMQClientLogger.LOGGER.timeoutFlushingPacket();
                     }

                     break;
                  }
                  catch (InterruptedException e) {
                     throw new ActiveMQInterruptedException(e);
                  }
               }
            }
         }
         finally {
            writeLock.release();
         }
      }
      catch (InterruptedException e) {
         throw new ActiveMQInterruptedException(e);
      }
   }

   @Override
   public String getRemoteAddress() {
      SocketAddress address = channel.remoteAddress();
      if (address == null) {
         return null;
      }
      return address.toString();
   }

   @Override
   public String getLocalAddress() {
      SocketAddress address = channel.localAddress();
      if (address == null) {
         return null;
      }
      return "tcp://" + IPV6Util.encloseHost(address.toString());
   }

   public boolean isDirectDeliver() {
      return directDeliver;
   }

   //never allow this
   @Override
   public ActiveMQPrincipal getDefaultActiveMQPrincipal() {
      return null;
   }

   @Override
   public TransportConfiguration getConnectorConfig() {
      if (configuration != null) {
         return new TransportConfiguration(NettyConnectorFactory.class.getName(), this.configuration);
      }
      else {
         return null;
      }
   }

   @Override
   public boolean isUsingProtocolHandling() {
      return true;
   }

   // Public --------------------------------------------------------

   @Override
   public String toString() {
      return super.toString() + "[local= " + channel.localAddress() + ", remote=" + channel.remoteAddress() + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void closeSSLAndChannel(SslHandler sslHandler, Channel channel) {
      if (sslHandler != null) {
         try {
            ChannelFuture sslCloseFuture = sslHandler.close();

            if (!sslCloseFuture.awaitUninterruptibly(10000)) {
               ActiveMQClientLogger.LOGGER.timeoutClosingSSL();
            }
         }
         catch (Throwable t) {
            // ignore
         }
      }

      ChannelFuture closeFuture = channel.close();
      if (!closeFuture.awaitUninterruptibly(10000)) {
         ActiveMQClientLogger.LOGGER.timeoutClosingNettyChannel();
      }
   }
   // Inner classes -------------------------------------------------

}
