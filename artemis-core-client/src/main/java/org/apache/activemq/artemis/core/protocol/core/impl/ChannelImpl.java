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
package org.apache.activemq.artemis.core.protocol.core.impl;

import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQInterruptedException;
import org.apache.activemq.artemis.api.core.Interceptor;
import org.apache.activemq.artemis.core.client.ActiveMQClientLogger;
import org.apache.activemq.artemis.core.client.ActiveMQClientMessageBundle;
import org.apache.activemq.artemis.core.protocol.core.Channel;
import org.apache.activemq.artemis.core.protocol.core.ChannelHandler;
import org.apache.activemq.artemis.core.protocol.core.CommandConfirmationHandler;
import org.apache.activemq.artemis.core.protocol.core.CoreRemotingConnection;
import org.apache.activemq.artemis.core.protocol.core.Packet;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.ActiveMQExceptionMessage;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.PacketsConfirmedMessage;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.utils.ConcurrentUtil;
import org.jboss.logging.Logger;

public final class ChannelImpl implements Channel {

   private static final Logger logger = Logger.getLogger(ChannelImpl.class);

   public enum CHANNEL_ID {
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

      CHANNEL_ID(long id) {
         this.id = id;
      }

      protected static String idToString(long code) {
         for (CHANNEL_ID channel : EnumSet.allOf(CHANNEL_ID.class)) {
            if (channel.id == code)
               return channel.toString();
         }
         return Long.toString(code);
      }
   }

   private volatile long id;

   /**
    * This is used in
    */
   private final AtomicInteger reconnectID = new AtomicInteger(0);

   private ChannelHandler handler;

   private Packet response;

   private final java.util.Queue<Packet> resendCache;

   private int firstStoredCommandID;

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

   public ChannelImpl(final CoreRemotingConnection connection,
                      final long id,
                      final int confWindowSize,
                      final List<Interceptor> interceptors) {
      this.connection = connection;

      this.id = id;

      this.confWindowSize = confWindowSize;

      if (confWindowSize != -1) {
         resendCache = new ConcurrentLinkedQueue<>();
      } else {
         resendCache = null;
      }

      this.interceptors = interceptors;
   }

   @Override
   public int getReconnectID() {
      return reconnectID.get();
   }

   @Override
   public boolean supports(final byte packetType) {
      return supports(packetType, connection.getChannelVersion());
   }

   @Override
   public boolean supports(final byte packetType, int version) {
      switch (packetType) {
         case PacketImpl.CLUSTER_TOPOLOGY_V2:
            return version >= 122;
         case PacketImpl.DISCONNECT_CONSUMER:
            return version >= 124;
         case PacketImpl.CLUSTER_TOPOLOGY_V3:
            return version >= 125;
         case PacketImpl.DISCONNECT_V2:
            return version >= 125;
         case PacketImpl.SESS_QUEUEQUERY_RESP_V2:
            return version >= 126;
         case PacketImpl.SESS_BINDINGQUERY_RESP_V2:
            return version >= 126;
         case PacketImpl.SESS_BINDINGQUERY_RESP_V3:
            return version >= 127;
         case PacketImpl.SESS_QUEUEQUERY_RESP_V3:
            return version >= 129;
         case PacketImpl.SESS_BINDINGQUERY_RESP_V4:
            return version >= 129;
         default:
            return true;
      }
   }

   @Override
   public long getID() {
      return id;
   }

   @Override
   public int getLastConfirmedCommandID() {
      return lastConfirmedCommandID.get();
   }

   @Override
   public Lock getLock() {
      return lock;
   }

   @Override
   public int getConfirmationWindowSize() {
      return confWindowSize;
   }

   @Override
   public void returnBlocking() {
      returnBlocking(null);
   }

   @Override
   public void returnBlocking(Throwable cause) {
      lock.lock();

      try {
         response = new ActiveMQExceptionMessage(ActiveMQClientMessageBundle.BUNDLE.unblockingACall(cause));

         sendCondition.signal();
      } finally {
         lock.unlock();
      }
   }

   @Override
   public boolean sendAndFlush(final Packet packet) {
      return send(packet, -1, true, false);
   }

   @Override
   public boolean send(final Packet packet) {
      return send(packet, -1, false, false);
   }

   @Override
   public boolean send(Packet packet, final int reconnectID) {
      return send(packet, reconnectID, false, false);
   }

   @Override
   public boolean sendBatched(final Packet packet) {
      return send(packet, -1, false, true);
   }

   @Override
   public void setTransferring(boolean transferring) {
      this.transferring = transferring;
   }

   /**
    * @param timeoutMsg message to log on blocking call failover timeout
    */
   private void waitForFailOver(String timeoutMsg) {
      try {
         if (connection.getBlockingCallFailoverTimeout() < 0) {
            while (failingOver) {
               failoverCondition.await();
            }
         } else if (!ConcurrentUtil.await(failoverCondition, connection.getBlockingCallFailoverTimeout())) {
            logger.debug(timeoutMsg);
         }
      } catch (InterruptedException e) {
         throw new ActiveMQInterruptedException(e);
      }
   }

   // This must never called by more than one thread concurrently
   private boolean send(final Packet packet, final int reconnectID, final boolean flush, final boolean batch) {
      if (invokeInterceptors(packet, interceptors, connection) != null) {
         return false;
      }

      synchronized (sendLock) {
         packet.setChannelID(id);

         if (logger.isTraceEnabled()) {
            logger.trace("RemotingConnectionID=" + (connection == null ? "NULL" : connection.getID()) + " Sending packet nonblocking " + packet + " on channelID=" + id);
         }

         ActiveMQBuffer buffer = packet.encode(connection);

         lock.lock();

         try {
            if (failingOver) {
               waitForFailOver("RemotingConnectionID=" + (connection == null ? "NULL" : connection.getID()) + " timed-out waiting for fail-over condition on non-blocking send");
            }

            // Sanity check
            if (transferring) {
               throw ActiveMQClientMessageBundle.BUNDLE.cannotSendPacketDuringFailover();
            }

            if (resendCache != null && packet.isRequiresConfirmations()) {
               addResendPacket(packet);
            }
         } finally {
            lock.unlock();
         }

         if (logger.isTraceEnabled()) {
            logger.trace("RemotingConnectionID=" + (connection == null ? "NULL" : connection.getID()) + " Writing buffer for channelID=" + id);
         }

         checkReconnectID(reconnectID);

         // The actual send must be outside the lock, or with OIO transport, the write can block if the tcp
         // buffer is full, preventing any incoming buffers being handled and blocking failover
         connection.getTransportConnection().write(buffer, flush, batch);
         return true;
      }
   }

   private void checkReconnectID(int reconnectID) {
      if (reconnectID >= 0 && reconnectID != this.reconnectID.get()) {
         throw ActiveMQClientMessageBundle.BUNDLE.packetTransmissionInterrupted();
      }
   }

   @Override
   public Packet sendBlocking(final Packet packet, byte expectedPacket) throws ActiveMQException {
      return sendBlocking(packet, -1, expectedPacket);
   }

   /**
    * Due to networking issues or server issues the server may take longer to answer than expected.. the client may timeout the call throwing an exception
    * and the client could eventually retry another call, but the server could then answer a previous command issuing a class-cast-exception.
    * The expectedPacket will be used to filter out undesirable packets that would belong to previous calls.
    */
   @Override
   public Packet sendBlocking(final Packet packet,
                              final int reconnectID,
                              byte expectedPacket) throws ActiveMQException {
      String interceptionResult = invokeInterceptors(packet, interceptors, connection);

      if (interceptionResult != null) {
         if (logger.isTraceEnabled()) {
            logger.trace("RemotingConnectionID=" + (connection == null ? "NULL" : connection.getID()) + " interceptionResult=" + interceptionResult);
         }
         // if we don't throw an exception here the client might not unblock
         throw ActiveMQClientMessageBundle.BUNDLE.interceptorRejectedPacket(interceptionResult);
      }

      if (closed) {
         if (logger.isTraceEnabled()) {
            logger.trace("RemotingConnectionID=" + (connection == null ? "NULL" : connection.getID()) + " closed.");
         }
         throw ActiveMQClientMessageBundle.BUNDLE.connectionDestroyed();
      }

      if (connection.getBlockingCallTimeout() == -1) {
         if (logger.isTraceEnabled()) {
            logger.trace("RemotingConnectionID=" + (connection == null ? "NULL" : connection.getID()) + " Cannot do a blocking call timeout on a server side connection");
         }
         throw new IllegalStateException("Cannot do a blocking call timeout on a server side connection");
      }

      // Synchronized since can't be called concurrently by more than one thread and this can occur
      // E.g. blocking acknowledge() from inside a message handler at some time as other operation on main thread
      synchronized (sendBlockingLock) {
         packet.setChannelID(id);

         final ActiveMQBuffer buffer = packet.encode(connection);

         lock.lock();

         try {
            if (failingOver) {
               waitForFailOver("RemotingConnectionID=" + (connection == null ? "NULL" : connection.getID()) + " timed-out waiting for fail-over condition on blocking send");
            }

            response = null;

            if (resendCache != null && packet.isRequiresConfirmations()) {
               addResendPacket(packet);
            }

            checkReconnectID(reconnectID);

            if (logger.isTraceEnabled()) {
               logger.trace("RemotingConnectionID=" + (connection == null ? "NULL" : connection.getID()) + " Sending blocking " + packet);
            }

            connection.getTransportConnection().write(buffer, false, false);

            long toWait = connection.getBlockingCallTimeout();

            long start = System.currentTimeMillis();

            while (!closed && (response == null || (response.getType() != PacketImpl.EXCEPTION && response.getType() != expectedPacket)) && toWait > 0) {
               try {
                  sendCondition.await(toWait, TimeUnit.MILLISECONDS);
               } catch (InterruptedException e) {
                  throw new ActiveMQInterruptedException(e);
               }

               if (response != null && response.getType() != PacketImpl.EXCEPTION && response.getType() != expectedPacket) {
                  ActiveMQClientLogger.LOGGER.packetOutOfOrder(response, new Exception("trace"));
               }

               if (closed) {
                  break;
               }

               final long now = System.currentTimeMillis();

               toWait -= now - start;

               start = now;
            }

            if (closed && toWait > 0 && response == null) {
               Throwable cause = ActiveMQClientMessageBundle.BUNDLE.connectionDestroyed();
               throw ActiveMQClientMessageBundle.BUNDLE.unblockingACall(cause);
            }

            if (response == null) {
               throw ActiveMQClientMessageBundle.BUNDLE.timedOutSendingPacket(connection.getBlockingCallTimeout(), packet.getType());
            }

            if (response.getType() == PacketImpl.EXCEPTION) {
               final ActiveMQExceptionMessage mem = (ActiveMQExceptionMessage) response;

               ActiveMQException e = mem.getException();

               e.fillInStackTrace();

               throw e;
            }
         } finally {
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
   public static String invokeInterceptors(final Packet packet,
                                           final List<Interceptor> interceptors,
                                           final RemotingConnection connection) {
      if (interceptors != null) {
         for (final Interceptor interceptor : interceptors) {
            try {
               boolean callNext = interceptor.intercept(packet, connection);

               if (logger.isDebugEnabled()) {
                  // use a StringBuilder for speed since this may be executed a lot
                  StringBuilder msg = new StringBuilder();
                  msg.append("Invocation of interceptor ").append(interceptor.getClass().getName()).append(" on ").
                     append(packet).append(" returned ").append(callNext);
                  logger.debug(msg.toString());
               }

               if (!callNext) {
                  return interceptor.getClass().getName();
               }
            } catch (final Throwable e) {
               ActiveMQClientLogger.LOGGER.errorCallingInterceptor(e, interceptor);
            }
         }
      }

      return null;
   }

   @Override
   public void setCommandConfirmationHandler(final CommandConfirmationHandler handler) {
      if (confWindowSize < 0) {
         final String msg = "You can't set confirmationHandler on a connection with confirmation-window-size < 0." + " Look at the documentation for more information.";
         if (logger.isTraceEnabled()) {
            logger.trace("RemotingConnectionID=" + (connection == null ? "NULL" : connection.getID()) + " " + msg);
         }
         throw new IllegalStateException(msg);
      }
      commandConfirmationHandler = handler;
   }

   @Override
   public void setHandler(final ChannelHandler handler) {
      if (logger.isTraceEnabled()) {
         logger.trace("RemotingConnectionID=" + (connection == null ? "NULL" : connection.getID()) + " Setting handler on " + this + " as " + handler);
      }

      this.handler = handler;
   }

   @Override
   public ChannelHandler getHandler() {
      return handler;
   }

   @Override
   public void close() {
      if (closed) {
         return;
      }

      if (!connection.isDestroyed() && !connection.removeChannel(id)) {
         throw ActiveMQClientMessageBundle.BUNDLE.noChannelToClose(id);
      }

      if (failingOver) {
         unlock();
      }
      closed = true;
   }

   @Override
   public void transferConnection(final CoreRemotingConnection newConnection) {
      // Needs to synchronize on the connection to make sure no packets from
      // the old connection get processed after transfer has occurred
      synchronized (connection.getTransferLock()) {
         connection.removeChannel(id);

         if (logger.isTraceEnabled()) {
            logger.trace("RemotingConnectionID=" + (connection == null ? "NULL" : connection.getID()) + " transferConnection to new RemotingConnectionID=" + (newConnection == null ? "NULL" : newConnection.getID()));
         }

         // And switch it

         final CoreRemotingConnection rnewConnection = newConnection;

         rnewConnection.putChannel(id, this);

         connection = rnewConnection;

         transferring = true;
      }
   }

   @Override
   public void replayCommands(final int otherLastConfirmedCommandID) {
      if (resendCache != null) {
         if (logger.isTraceEnabled()) {
            logger.trace("RemotingConnectionID=" + (connection == null ? "NULL" : connection.getID()) + " Replaying commands on channelID=" + id);
         }
         clearUpTo(otherLastConfirmedCommandID);

         for (final Packet packet : resendCache) {
            doWrite(packet);
         }
      }
   }

   @Override
   public void lock() {
      if (logger.isTraceEnabled()) {
         logger.trace("RemotingConnectionID=" + (connection == null ? "NULL" : connection.getID()) + " lock channel " + this);
      }
      lock.lock();

      reconnectID.incrementAndGet();

      failingOver = true;

      lock.unlock();
   }

   @Override
   public void unlock() {
      if (logger.isTraceEnabled()) {
         logger.trace("RemotingConnectionID=" + (connection == null ? "NULL" : connection.getID()) + " unlock channel " + this);
      }
      lock.lock();

      failingOver = false;

      failoverCondition.signalAll();

      lock.unlock();
   }

   @Override
   public CoreRemotingConnection getConnection() {
      return connection;
   }

   // Needs to be synchronized since can be called by remoting service timer thread too for timeout flush
   @Override
   public synchronized void flushConfirmations() {
      if (resendCache != null && receivedBytes != 0) {
         receivedBytes = 0;

         final Packet confirmed = new PacketsConfirmedMessage(lastConfirmedCommandID.get());

         confirmed.setChannelID(id);

         if (logger.isTraceEnabled()) {
            logger.trace("RemotingConnectionID=" + (connection == null ? "NULL" : connection.getID()) + " ChannelImpl::flushConfirmation flushing confirmation " + confirmed);
         }

         doWrite(confirmed);
      }
   }

   @Override
   public void confirm(final Packet packet) {
      if (resendCache != null && packet.isRequiresConfirmations()) {
         lastConfirmedCommandID.incrementAndGet();

         if (logger.isTraceEnabled()) {
            logger.trace("RemotingConnectionID=" + (connection == null ? "NULL" : connection.getID()) + " ChannelImpl::confirming packet " + packet + " last commandID=" + lastConfirmedCommandID);
         }

         receivedBytes += packet.getPacketSize();

         if (receivedBytes >= confWindowSize) {
            receivedBytes = 0;

            final Packet confirmed = new PacketsConfirmedMessage(lastConfirmedCommandID.get());

            confirmed.setChannelID(id);

            doWrite(confirmed);
         }
      }
   }

   @Override
   public void clearCommands() {
      if (resendCache != null) {
         lastConfirmedCommandID.set(-1);

         firstStoredCommandID = 0;

         resendCache.clear();
      }
   }

   @Override
   public void handlePacket(final Packet packet) {
      if (packet.getType() == PacketImpl.PACKETS_CONFIRMED) {
         if (resendCache != null) {
            final PacketsConfirmedMessage msg = (PacketsConfirmedMessage) packet;

            clearUpTo(msg.getCommandID());
         }

         if (!connection.isClient()) {
            handler.handlePacket(packet);
         }

         return;
      } else {
         if (packet.isResponse()) {
            confirm(packet);

            lock.lock();

            try {
               response = packet;
               sendCondition.signal();
            } finally {
               lock.unlock();
            }
         } else if (handler != null) {
            handler.handlePacket(packet);
         }
      }
   }

   private void doWrite(final Packet packet) {
      final ActiveMQBuffer buffer = packet.encode(connection);

      connection.getTransportConnection().write(buffer, false, false);

   }

   private void addResendPacket(Packet packet) {
      resendCache.add(packet);

      if (logger.isTraceEnabled()) {
         logger.trace("RemotingConnectionID=" + (connection == null ? "NULL" : connection.getID()) + " ChannelImpl::addResendPacket adding packet " + packet + " stored commandID=" + firstStoredCommandID + " possible commandIDr=" + (firstStoredCommandID + resendCache.size()));
      }
   }

   private void clearUpTo(final int lastReceivedCommandID) {
      final int numberToClear = 1 + lastReceivedCommandID - firstStoredCommandID;

      if (logger.isTraceEnabled()) {
         logger.trace("RemotingConnectionID=" + (connection == null ? "NULL" : connection.getID()) + " ChannelImpl::clearUpTo lastReceived commandID=" + lastReceivedCommandID + " first commandID=" + firstStoredCommandID + " number to clear " + numberToClear);
      }

      for (int i = 0; i < numberToClear; i++) {
         final Packet packet = resendCache.poll();

         if (packet == null) {
            ActiveMQClientLogger.LOGGER.cannotFindPacketToClear(lastReceivedCommandID, firstStoredCommandID);
            firstStoredCommandID = lastReceivedCommandID + 1;
            return;
         }

         if (logger.isTraceEnabled()) {
            logger.trace("RemotingConnectionID=" + connection.getID() + " ChannelImpl::clearUpTo confirming " + packet + " towards " + commandConfirmationHandler);
         }
         if (commandConfirmationHandler != null) {
            commandConfirmationHandler.commandConfirmed(packet);
         }
      }

      firstStoredCommandID += numberToClear;
   }

   @Override
   public String toString() {
      return "Channel[id=" + CHANNEL_ID.idToString(id) + ", RemotingConnectionID=" + (connection == null ? "NULL" : connection.getID()) + ", handler=" + handler + "]";
   }
}
