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
package org.apache.activemq.core.protocol.core.impl;

import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.activemq.api.core.ActiveMQBuffer;
import org.apache.activemq.api.core.ActiveMQException;
import org.apache.activemq.api.core.ActiveMQInterruptedException;
import org.apache.activemq.api.core.Interceptor;
import org.apache.activemq.core.client.HornetQClientLogger;
import org.apache.activemq.core.client.HornetQClientMessageBundle;
import org.apache.activemq.core.protocol.core.Channel;
import org.apache.activemq.core.protocol.core.ChannelHandler;
import org.apache.activemq.core.protocol.core.CommandConfirmationHandler;
import org.apache.activemq.core.protocol.core.CoreRemotingConnection;
import org.apache.activemq.core.protocol.core.Packet;
import org.apache.activemq.core.protocol.core.impl.wireformat.HornetQExceptionMessage;
import org.apache.activemq.core.protocol.core.impl.wireformat.PacketsConfirmedMessage;
import org.apache.activemq.spi.core.protocol.RemotingConnection;

/**
 * A ChannelImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public final class ChannelImpl implements Channel
{
   public enum CHANNEL_ID
   {
      /**
       * Used for core protocol management.
       */
      PING(0),
      /**
       * Session creation and attachment.
       */
      SESSION(1),
      /**
       * Replication, i.e. for backups that do not share the journal.
       */
      REPLICATION(2),
      /**
       * cluster used for controlling nodes in a cluster remotely
       */
      CLUSTER(3),
      /**
       * Channels [0-9] are reserved for the system, user channels must be greater than that.
       */
      USER(10);

      public final long id;

      CHANNEL_ID(long id)
      {
         this.id = id;
      }

      protected static String idToString(long code)
      {
         for (CHANNEL_ID channel : EnumSet.allOf(CHANNEL_ID.class))
         {
            if (channel.id == code) return channel.toString();
         }
         return Long.toString(code);
      }
   }

   private static final boolean isTrace = HornetQClientLogger.LOGGER.isTraceEnabled();

   private volatile long id;

   private ChannelHandler handler;

   private Packet response;

   private final java.util.Queue<Packet> resendCache;

   private volatile int firstStoredCommandID;

   private final AtomicInteger lastConfirmedCommandID = new AtomicInteger(-1);

   private volatile CoreRemotingConnection connection;

   private volatile boolean closed;

   private final Lock lock = new ReentrantLock();

   private final Condition sendCondition = lock.newCondition();

   private final Condition failoverCondition = lock.newCondition();

   private final Object sendLock = new Object();

   private final Object sendBlockingLock = new Object();

   private boolean failingOver;

   private final int confWindowSize;

   private int receivedBytes;

   private CommandConfirmationHandler commandConfirmationHandler;

   private volatile boolean transferring;

   private final List<Interceptor> interceptors;

   public ChannelImpl(final CoreRemotingConnection connection, final long id, final int confWindowSize, final List<Interceptor> interceptors)
   {
      this.connection = connection;

      this.id = id;

      this.confWindowSize = confWindowSize;

      if (confWindowSize != -1)
      {
         resendCache = new ConcurrentLinkedQueue<Packet>();
      }
      else
      {
         resendCache = null;
      }

      this.interceptors = interceptors;
   }

   public boolean supports(final byte packetType)
   {
      int version = connection.getClientVersion();

      switch (packetType)
      {
         case PacketImpl.CLUSTER_TOPOLOGY_V2:
            return version >= 122;
         case PacketImpl.DISCONNECT_CONSUMER:
            return version >= 124;
         case PacketImpl.CLUSTER_TOPOLOGY_V3:
            return version >= 125;
         case PacketImpl.DISCONNECT_V2:
            return version >= 125;
         default:
            return true;
      }
   }

   public long getID()
   {
      return id;
   }

   public int getLastConfirmedCommandID()
   {
      return lastConfirmedCommandID.get();
   }

   public Lock getLock()
   {
      return lock;
   }

   public int getConfirmationWindowSize()
   {
      return confWindowSize;
   }

   public void returnBlocking()
   {
      returnBlocking(null);
   }

   public void returnBlocking(Throwable cause)
   {
      lock.lock();

      try
      {
         response = new HornetQExceptionMessage(HornetQClientMessageBundle.BUNDLE.unblockingACall(cause));

         sendCondition.signal();
      }
      finally
      {
         lock.unlock();
      }
   }

   public boolean sendAndFlush(final Packet packet)
   {
      return send(packet, true, false);
   }

   public boolean send(final Packet packet)
   {
      return send(packet, false, false);
   }

   public boolean sendBatched(final Packet packet)
   {
      return send(packet, false, true);
   }

   public void setTransferring(boolean transferring)
   {
      this.transferring = transferring;
   }

   // This must never called by more than one thread concurrently
   public boolean send(final Packet packet, final boolean flush, final boolean batch)
   {
      if (invokeInterceptors(packet, interceptors, connection) != null)
      {
         return false;
      }

      synchronized (sendLock)
      {
         packet.setChannelID(id);

         if (isTrace)
         {
            HornetQClientLogger.LOGGER.trace("Sending packet nonblocking " + packet + " on channeID=" + id);
         }

         ActiveMQBuffer buffer = packet.encode(connection);

         lock.lock();

         try
         {
            if (failingOver)
            {
               // TODO - don't hardcode this timeout
               try
               {
                  failoverCondition.await(10000, TimeUnit.MILLISECONDS);
               }
               catch (InterruptedException e)
               {
                  throw new ActiveMQInterruptedException(e);
               }
            }

            // Sanity check
            if (transferring)
            {
               throw new IllegalStateException("Cannot send a packet while channel is doing failover");
            }

            if (resendCache != null && packet.isRequiresConfirmations())
            {
               resendCache.add(packet);
            }
         }
         finally
         {
            lock.unlock();
         }

         if (isTrace)
         {
            HornetQClientLogger.LOGGER.trace("Writing buffer for channelID=" + id);
         }


         // The actual send must be outside the lock, or with OIO transport, the write can block if the tcp
         // buffer is full, preventing any incoming buffers being handled and blocking failover
         connection.getTransportConnection().write(buffer, flush, batch);

         return true;
      }
   }

   /**
    * Due to networking issues or server issues the server may take longer to answer than expected.. the client may timeout the call throwing an exception
    * and the client could eventually retry another call, but the server could then answer a previous command issuing a class-cast-exception.
    * The expectedPacket will be used to filter out undesirable packets that would belong to previous calls.
    */
   public Packet sendBlocking(final Packet packet, byte expectedPacket) throws ActiveMQException
   {
      String interceptionResult = invokeInterceptors(packet, interceptors, connection);

      if (interceptionResult != null)
      {
         // if we don't throw an exception here the client might not unblock
         throw HornetQClientMessageBundle.BUNDLE.interceptorRejectedPacket(interceptionResult);
      }

      if (closed)
      {
         throw HornetQClientMessageBundle.BUNDLE.connectionDestroyed();
      }

      if (connection.getBlockingCallTimeout() == -1)
      {
         throw new IllegalStateException("Cannot do a blocking call timeout on a server side connection");
      }

      // Synchronized since can't be called concurrently by more than one thread and this can occur
      // E.g. blocking acknowledge() from inside a message handler at some time as other operation on main thread
      synchronized (sendBlockingLock)
      {
         packet.setChannelID(id);

         final ActiveMQBuffer buffer = packet.encode(connection);

         lock.lock();

         try
         {
            if (failingOver)
            {
               try
               {
                  if (connection.getBlockingCallFailoverTimeout() < 0)
                  {
                     while (failingOver)
                     {
                        failoverCondition.await();
                     }
                  }
                  else
                  {
                     if (!failoverCondition.await(connection.getBlockingCallFailoverTimeout(), TimeUnit.MILLISECONDS))
                     {
                        HornetQClientLogger.LOGGER.debug("timed-out waiting for failover condition");
                     }
                  }
               }
               catch (InterruptedException e)
               {
                  throw new ActiveMQInterruptedException(e);
               }
            }

            response = null;

            if (resendCache != null && packet.isRequiresConfirmations())
            {
               resendCache.add(packet);
            }

            connection.getTransportConnection().write(buffer, false, false);

            long toWait = connection.getBlockingCallTimeout();

            long start = System.currentTimeMillis();

            while (!closed && (response == null || (response.getType() != PacketImpl.EXCEPTION &&
               response.getType() != expectedPacket)) && toWait > 0)
            {
               try
               {
                  sendCondition.await(toWait, TimeUnit.MILLISECONDS);
               }
               catch (InterruptedException e)
               {
                  throw new ActiveMQInterruptedException(e);
               }

               if (response != null && response.getType() != PacketImpl.EXCEPTION && response.getType() != expectedPacket)
               {
                  HornetQClientLogger.LOGGER.packetOutOfOrder(response, new Exception("trace"));
               }

               if (closed)
               {
                  break;
               }

               final long now = System.currentTimeMillis();

               toWait -= now - start;

               start = now;
            }

            if (response == null)
            {
               throw HornetQClientMessageBundle.BUNDLE.timedOutSendingPacket(packet.getType());
            }

            if (response.getType() == PacketImpl.EXCEPTION)
            {
               final HornetQExceptionMessage mem = (HornetQExceptionMessage) response;

               ActiveMQException e = mem.getException();

               e.fillInStackTrace();

               throw e;
            }
         }
         finally
         {
            lock.unlock();
         }

         return response;
      }
   }

   /**
    * @param packet the packet to intercept
    * @return the name of the interceptor that returned <code>false</code> or <code>null</code> if no interceptors
    * returned <code>false</code>.
    */
   public static String invokeInterceptors(final Packet packet, final List<Interceptor> interceptors, final RemotingConnection connection)
   {
      if (interceptors != null)
      {
         for (final Interceptor interceptor : interceptors)
         {
            try
            {
               boolean callNext = interceptor.intercept(packet, connection);

               if (HornetQClientLogger.LOGGER.isDebugEnabled())
               {
                  // use a StringBuilder for speed since this may be executed a lot
                  StringBuilder msg = new StringBuilder();
                  msg.append("Invocation of interceptor ").append(interceptor.getClass().getName()).append(" on ").
                     append(packet).append(" returned ").append(callNext);
                  HornetQClientLogger.LOGGER.debug(msg.toString());
               }

               if (!callNext)
               {
                  return interceptor.getClass().getName();
               }
            }
            catch (final Throwable e)
            {
               HornetQClientLogger.LOGGER.errorCallingInterceptor(e, interceptor);
            }
         }
      }

      return null;
   }

   public void setCommandConfirmationHandler(final CommandConfirmationHandler handler)
   {
      if (confWindowSize < 0)
      {
         final String msg =
            "You can't set confirmationHandler on a connection with confirmation-window-size < 0."
               + " Look at the documentation for more information.";
         throw new IllegalStateException(msg);
      }
      commandConfirmationHandler = handler;
   }

   public void setHandler(final ChannelHandler handler)
   {
      this.handler = handler;
   }

   public ChannelHandler getHandler()
   {
      return handler;
   }

   public void close()
   {
      if (closed)
      {
         return;
      }

      if (!connection.isDestroyed() && !connection.removeChannel(id))
      {
         throw HornetQClientMessageBundle.BUNDLE.noChannelToClose(id);
      }

      if (failingOver)
      {
         unlock();
      }
      closed = true;
   }

   public void transferConnection(final CoreRemotingConnection newConnection)
   {
      // Needs to synchronize on the connection to make sure no packets from
      // the old connection get processed after transfer has occurred
      synchronized (connection.getTransferLock())
      {
         connection.removeChannel(id);

         // And switch it

         final CoreRemotingConnection rnewConnection = newConnection;

         rnewConnection.putChannel(id, this);

         connection = rnewConnection;

         transferring = true;
      }
   }

   public void replayCommands(final int otherLastConfirmedCommandID)
   {
      if (resendCache != null)
      {
         if (isTrace)
         {
            HornetQClientLogger.LOGGER.trace("Replaying commands on channelID=" + id);
         }
         clearUpTo(otherLastConfirmedCommandID);

         for (final Packet packet : resendCache)
         {
            doWrite(packet);
         }
      }
   }

   public void lock()
   {
      lock.lock();

      failingOver = true;

      lock.unlock();
   }

   public void unlock()
   {
      lock.lock();

      failingOver = false;

      failoverCondition.signalAll();

      lock.unlock();
   }

   public CoreRemotingConnection getConnection()
   {
      return connection;
   }

   // Needs to be synchronized since can be called by remoting service timer thread too for timeout flush
   public synchronized void flushConfirmations()
   {
      if (resendCache != null && receivedBytes != 0)
      {
         receivedBytes = 0;

         final Packet confirmed = new PacketsConfirmedMessage(lastConfirmedCommandID.get());

         confirmed.setChannelID(id);

         doWrite(confirmed);
      }
   }

   public void confirm(final Packet packet)
   {
      if (resendCache != null && packet.isRequiresConfirmations())
      {
         lastConfirmedCommandID.incrementAndGet();

         receivedBytes += packet.getPacketSize();

         if (receivedBytes >= confWindowSize)
         {
            receivedBytes = 0;

            final Packet confirmed = new PacketsConfirmedMessage(lastConfirmedCommandID.get());

            confirmed.setChannelID(id);

            doWrite(confirmed);
         }
      }
   }

   public void clearCommands()
   {
      if (resendCache != null)
      {
         lastConfirmedCommandID.set(-1);

         firstStoredCommandID = 0;

         resendCache.clear();
      }
   }

   public void handlePacket(final Packet packet)
   {
      if (packet.getType() == PacketImpl.PACKETS_CONFIRMED)
      {
         if (resendCache != null)
         {
            final PacketsConfirmedMessage msg = (PacketsConfirmedMessage) packet;

            clearUpTo(msg.getCommandID());
         }

         if (!connection.isClient())
         {
            handler.handlePacket(packet);
         }

         return;
      }
      else
      {
         if (packet.isResponse())
         {
            confirm(packet);

            lock.lock();

            try
            {
               response = packet;
               sendCondition.signal();
            }
            finally
            {
               lock.unlock();
            }
         }
         else if (handler != null)
         {
            handler.handlePacket(packet);
         }
      }
   }

   private void doWrite(final Packet packet)
   {
      final ActiveMQBuffer buffer = packet.encode(connection);

      connection.getTransportConnection().write(buffer, false, false);
   }

   private void clearUpTo(final int lastReceivedCommandID)
   {
      final int numberToClear = 1 + lastReceivedCommandID - firstStoredCommandID;

      if (numberToClear == -1)
      {
         throw HornetQClientMessageBundle.BUNDLE.invalidCommandID(lastReceivedCommandID);
      }

      int sizeToFree = 0;

      for (int i = 0; i < numberToClear; i++)
      {
         final Packet packet = resendCache.poll();

         if (packet == null)
         {
            if (lastReceivedCommandID > 0)
            {
               HornetQClientLogger.LOGGER.cannotFindPacketToClear(lastReceivedCommandID, firstStoredCommandID);
            }
            firstStoredCommandID = lastReceivedCommandID + 1;
            return;
         }

         if (packet.getType() != PacketImpl.PACKETS_CONFIRMED)
         {
            sizeToFree += packet.getPacketSize();
         }

         if (commandConfirmationHandler != null)
         {
            commandConfirmationHandler.commandConfirmed(packet);
         }
      }

      firstStoredCommandID += numberToClear;
   }

   @Override
   public String toString()
   {
      return "Channel[id=" + CHANNEL_ID.idToString(id) + ", handler=" + handler + "]";
   }
}
