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
package org.hornetq.core.remoting.impl.invm;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import io.netty.channel.ChannelFutureListener;
import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQBuffers;
import org.hornetq.api.core.HornetQInterruptedException;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.security.HornetQPrincipal;
import org.hornetq.core.server.HornetQServerLogger;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.spi.core.remoting.BufferHandler;
import org.hornetq.spi.core.remoting.Connection;
import org.hornetq.spi.core.remoting.ConnectionLifeCycleListener;
import org.hornetq.spi.core.remoting.ReadyListener;
import org.hornetq.utils.UUIDGenerator;

/**
 * A InVMConnection
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class InVMConnection implements Connection
{
   private static final boolean isTrace = HornetQServerLogger.LOGGER.isTraceEnabled();

   private final BufferHandler handler;

   private final ConnectionLifeCycleListener listener;

   private final String id;

   private boolean closed;

   // Used on tests
   private static boolean flushEnabled = true;

   private final int serverID;

   private final Executor executor;

   private volatile boolean closing;

   private final HornetQPrincipal defaultHornetQPrincipal;

   private RemotingConnection protocolConnection;

   public InVMConnection(final int serverID,
                         final BufferHandler handler,
                         final ConnectionLifeCycleListener listener,
                         final Executor executor)
   {
      this(serverID, UUIDGenerator.getInstance().generateSimpleStringUUID().toString(), handler, listener, executor);
   }

   public InVMConnection(final int serverID,
                         final String id,
                         final BufferHandler handler,
                         final ConnectionLifeCycleListener listener,
                         final Executor executor)
   {
      this(serverID, id, handler, listener, executor, null);
   }

   public InVMConnection(final int serverID,
                         final String id,
                         final BufferHandler handler,
                         final ConnectionLifeCycleListener listener,
                         final Executor executor,
                         final HornetQPrincipal defaultHornetQPrincipal)
   {
      this.serverID = serverID;

      this.handler = handler;

      this.listener = listener;

      this.id = id;

      this.executor = executor;

      this.defaultHornetQPrincipal = defaultHornetQPrincipal;
   }


   public void forceClose()
   {
      // no op
   }

   public RemotingConnection getProtocolConnection()
   {
      return this.protocolConnection;
   }

   public void setProtocolConnection(RemotingConnection connection)
   {
      this.protocolConnection = connection;
   }



   public void close()
   {
      if (closing)
      {
         return;
      }

      closing = true;

      synchronized (this)
      {
         if (!closed)
         {
            listener.connectionDestroyed(id);

            closed = true;
         }
      }
   }

   public HornetQBuffer createBuffer(final int size)
   {
      return HornetQBuffers.dynamicBuffer(size);
   }

   public Object getID()
   {
      return id;
   }

   public void checkFlushBatchBuffer()
   {
   }

   public void write(final HornetQBuffer buffer)
   {
      write(buffer, false, false, null);
   }

   public void write(final HornetQBuffer buffer, final boolean flush, final boolean batch)
   {
      write(buffer, flush, batch, null);
   }

   public void write(final HornetQBuffer buffer, final boolean flush, final boolean batch, final ChannelFutureListener futureListener)
   {
      final HornetQBuffer copied = buffer.copy(0, buffer.capacity());

      copied.setIndex(buffer.readerIndex(), buffer.writerIndex());

      try
      {
         executor.execute(new Runnable()
         {
            public void run()
            {
               try
               {
                  if (!closed)
                  {
                     copied.readInt(); // read and discard
                     if (isTrace)
                     {
                        HornetQServerLogger.LOGGER.trace(InVMConnection.this + "::Sending inVM packet");
                     }
                     handler.bufferReceived(id, copied);
                     if (futureListener != null)
                     {
                         // TODO BEFORE MERGE: (is null a good option here?)
                        futureListener.operationComplete(null);
                     }
                  }
               }
               catch (Exception e)
               {
                  final String msg = "Failed to write to handler on connector " + this;
                  HornetQServerLogger.LOGGER.errorWritingToInvmConnector(e, this);
                  throw new IllegalStateException(msg, e);
               }
               finally
               {
                  if (isTrace)
                  {
                     HornetQServerLogger.LOGGER.trace(InVMConnection.this + "::packet sent done");
                  }
               }
            }
         });

         if (flush && flushEnabled)
         {
            final CountDownLatch latch = new CountDownLatch(1);
            executor.execute(new Runnable()
            {
               public void run()
               {
                  latch.countDown();
               }
            });

            try
            {
               if (!latch.await(10, TimeUnit.SECONDS))
               {
                  HornetQServerLogger.LOGGER.timedOutFlushingInvmChannel();
               }
            }
            catch (InterruptedException e)
            {
               throw new HornetQInterruptedException(e);
            }
         }
      }
      catch (RejectedExecutionException e)
      {
         // Ignore - this can happen if server/client is shutdown and another request comes in
      }

   }

   public String getRemoteAddress()
   {
      return "invm:" + serverID;
   }

   public int getBatchingBufferSize()
   {
      return -1;
   }

   public void addReadyListener(ReadyListener listener)
   {
   }

   public void removeReadyListener(ReadyListener listener)
   {
   }

   @Override
   public boolean isUsingProtocolHandling()
   {
      return false;
   }

   public HornetQPrincipal getDefaultHornetQPrincipal()
   {
      return defaultHornetQPrincipal;
   }

   public static void setFlushEnabled(boolean enable)
   {
      flushEnabled = enable;
   }

   public Executor getExecutor()
   {
      return executor;
   }


   @Override
   public TransportConfiguration getConnectorConfig()
   {
      Map<String, Object> params = new HashMap<String, Object>();

      params.put(org.hornetq.core.remoting.impl.invm.TransportConstants.SERVER_ID_PROP_NAME, serverID);

      return new TransportConfiguration(InVMConnectorFactory.class.getName(), params);
   }

   @Override
   public String toString()
   {
      return "InVMConnection [serverID=" + serverID + ", id=" + id + "]";
   }


}
