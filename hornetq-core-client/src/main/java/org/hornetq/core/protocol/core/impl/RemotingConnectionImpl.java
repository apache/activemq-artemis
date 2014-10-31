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
package org.hornetq.core.protocol.core.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.HornetQInterruptedException;
import org.hornetq.api.core.Interceptor;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.client.HornetQClientLogger;
import org.hornetq.core.client.HornetQClientMessageBundle;
import org.hornetq.core.protocol.core.Channel;
import org.hornetq.core.protocol.core.CoreRemotingConnection;
import org.hornetq.core.protocol.core.Packet;
import org.hornetq.core.protocol.core.impl.ChannelImpl.CHANNEL_ID;
import org.hornetq.core.protocol.core.impl.wireformat.DisconnectMessage;
import org.hornetq.core.protocol.core.impl.wireformat.DisconnectMessage_V2;
import org.hornetq.core.remoting.CloseListener;
import org.hornetq.core.remoting.FailureListener;
import org.hornetq.core.security.HornetQPrincipal;
import org.hornetq.spi.core.remoting.Connection;
import org.hornetq.utils.SimpleIDGenerator;

/**
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class RemotingConnectionImpl implements CoreRemotingConnection
{
   // Constants
   // ------------------------------------------------------------------------------------

   private static final boolean isTrace = HornetQClientLogger.LOGGER.isTraceEnabled();

   // Static
   // ---------------------------------------------------------------------------------------

   // Attributes
   // -----------------------------------------------------------------------------------
   private final PacketDecoder packetDecoder;

   private final Connection transportConnection;

   private final Map<Long, Channel> channels = new ConcurrentHashMap<Long, Channel>();

   private final List<FailureListener> failureListeners = new CopyOnWriteArrayList<FailureListener>();

   private final List<CloseListener> closeListeners = new CopyOnWriteArrayList<CloseListener>();

   private final long blockingCallTimeout;

   private final long blockingCallFailoverTimeout;

   private final List<Interceptor> incomingInterceptors;

   private final List<Interceptor> outgoingInterceptors;

   private volatile boolean destroyed;

   private final boolean client;

   private int clientVersion;

   private volatile SimpleIDGenerator idGenerator = new SimpleIDGenerator(CHANNEL_ID.USER.id);

   private boolean idGeneratorSynced = false;

   private final Object transferLock = new Object();

   private final Object failLock = new Object();

   private volatile boolean dataReceived;

   private final Executor executor;

   private volatile boolean executing;

   private final SimpleString nodeID;

   private final long creationTime;

   private String clientID;

   // Constructors
   // ---------------------------------------------------------------------------------

   /*
    * Create a client side connection
    */
   public RemotingConnectionImpl(final PacketDecoder packetDecoder,
                                 final Connection transportConnection,
                                 final long blockingCallTimeout,
                                 final long blockingCallFailoverTimeout,
                                 final List<Interceptor> incomingInterceptors,
                                 final List<Interceptor> outgoingInterceptors)
   {
      this(packetDecoder, transportConnection, blockingCallTimeout, blockingCallFailoverTimeout, incomingInterceptors, outgoingInterceptors, true, null, null);
   }

   /*
    * Create a server side connection
    */
   RemotingConnectionImpl(final PacketDecoder packetDecoder,
                          final Connection transportConnection,
                          final List<Interceptor> incomingInterceptors,
                          final List<Interceptor> outgoingInterceptors,
                          final Executor executor,
                          final SimpleString nodeID)

   {
      this(packetDecoder, transportConnection, -1, -1, incomingInterceptors, outgoingInterceptors, false, executor, nodeID);
   }

   private RemotingConnectionImpl(final PacketDecoder packetDecoder,
                                  final Connection transportConnection,
                                  final long blockingCallTimeout,
                                  final long blockingCallFailoverTimeout,
                                  final List<Interceptor> incomingInterceptors,
                                  final List<Interceptor> outgoingInterceptors,
                                  final boolean client,
                                  final Executor executor,
                                  final SimpleString nodeID)

   {
      this.packetDecoder = packetDecoder;

      this.transportConnection = transportConnection;

      this.blockingCallTimeout = blockingCallTimeout;

      this.blockingCallFailoverTimeout = blockingCallFailoverTimeout;

      this.incomingInterceptors = incomingInterceptors;

      this.outgoingInterceptors = outgoingInterceptors;

      this.client = client;

      this.executor = executor;

      this.nodeID = nodeID;

      this.creationTime = System.currentTimeMillis();
   }


   // RemotingConnection implementation
   // ------------------------------------------------------------

   @Override
   public String toString()
   {
      return "RemotingConnectionImpl [clientID=" + clientID +
         ", nodeID=" +
         nodeID +
         ", transportConnection=" +
         transportConnection +
         "]";
   }

   public Connection getTransportConnection()
   {
      return transportConnection;
   }

   public List<FailureListener> getFailureListeners()
   {
      return new ArrayList<FailureListener>(failureListeners);
   }

   public void setFailureListeners(final List<FailureListener> listeners)
   {
      failureListeners.clear();

      failureListeners.addAll(listeners);
   }

   /**
    * @return the clientVersion
    */
   public int getClientVersion()
   {
      return clientVersion;
   }

   /**
    * @param clientVersion the clientVersion to set
    */
   public void setClientVersion(int clientVersion)
   {
      this.clientVersion = clientVersion;
   }

   public Object getID()
   {
      return transportConnection.getID();
   }

   public String getRemoteAddress()
   {
      return transportConnection.getRemoteAddress();
   }

   public long getCreationTime()
   {
      return creationTime;
   }

   public synchronized Channel getChannel(final long channelID, final int confWindowSize)
   {
      Channel channel = channels.get(channelID);

      if (channel == null)
      {
         channel = new ChannelImpl(this, channelID, confWindowSize, outgoingInterceptors);

         channels.put(channelID, channel);
      }

      return channel;
   }

   public synchronized boolean removeChannel(final long channelID)
   {
      return channels.remove(channelID) != null;
   }

   public synchronized void putChannel(final long channelID, final Channel channel)
   {
      channels.put(channelID, channel);
   }

   public void addFailureListener(final FailureListener listener)
   {
      if (listener == null)
      {
         throw HornetQClientMessageBundle.BUNDLE.failListenerCannotBeNull();
      }
      failureListeners.add(listener);
   }

   public boolean removeFailureListener(final FailureListener listener)
   {
      if (listener == null)
      {
         throw HornetQClientMessageBundle.BUNDLE.failListenerCannotBeNull();
      }

      return failureListeners.remove(listener);
   }

   public void addCloseListener(final CloseListener listener)
   {
      if (listener == null)
      {
         throw HornetQClientMessageBundle.BUNDLE.closeListenerCannotBeNull();
      }

      closeListeners.add(listener);
   }

   public boolean removeCloseListener(final CloseListener listener)
   {
      if (listener == null)
      {
         throw HornetQClientMessageBundle.BUNDLE.closeListenerCannotBeNull();
      }

      return closeListeners.remove(listener);
   }

   public List<CloseListener> removeCloseListeners()
   {
      List<CloseListener> ret = new ArrayList<CloseListener>(closeListeners);

      closeListeners.clear();

      return ret;
   }

   public List<FailureListener> removeFailureListeners()
   {
      List<FailureListener> ret = new ArrayList<FailureListener>(failureListeners);

      failureListeners.clear();

      return ret;
   }

   public void setCloseListeners(List<CloseListener> listeners)
   {
      closeListeners.clear();

      closeListeners.addAll(listeners);
   }

   public HornetQBuffer createBuffer(final int size)
   {
      return transportConnection.createBuffer(size);
   }

   /*
    * This can be called concurrently by more than one thread so needs to be locked
    */
   public void fail(final HornetQException me)
   {
      fail(me, null);
   }

   public void fail(final HornetQException me, String scaleDownTargetNodeID)
   {
      synchronized (failLock)
      {
         if (destroyed)
         {
            return;
         }

         destroyed = true;
      }

      HornetQClientLogger.LOGGER.connectionFailureDetected(me.getMessage(), me.getType());

      // Then call the listeners
      callFailureListeners(me, scaleDownTargetNodeID);

      callClosingListeners();

      internalClose();

      for (Channel channel : channels.values())
      {
         channel.returnBlocking();
      }
   }

   public void destroy()
   {
      synchronized (failLock)
      {
         if (destroyed)
         {
            return;
         }

         destroyed = true;
      }

      internalClose();

      callClosingListeners();
   }

   public void disconnect(final boolean criticalError)
   {
      disconnect(null, criticalError);
   }

   public void disconnect(String scaleDownNodeID, final boolean criticalError)
   {
      Channel channel0 = getChannel(ChannelImpl.CHANNEL_ID.PING.id, -1);

      // And we remove all channels from the connection, this ensures no more packets will be processed after this
      // method is
      // complete

      Set<Channel> allChannels = new HashSet<Channel>(channels.values());

      if (!criticalError)
      {
         removeAllChannels();
      }
      else
      {
         // We can't hold a lock if a critical error is happening...
         // as other threads will be holding the lock while hanging on IO
         channels.clear();
      }

      // Now we are 100% sure that no more packets will be processed we can flush then send the disconnect

      if (!criticalError)
      {
         for (Channel channel : allChannels)
         {
            channel.flushConfirmations();
         }
      }
      Packet disconnect;

      if (channel0.supports(PacketImpl.DISCONNECT_V2))
      {
         disconnect = new DisconnectMessage_V2(nodeID, scaleDownNodeID);
      }
      else
      {
         disconnect = new DisconnectMessage(nodeID);
      }
      channel0.sendAndFlush(disconnect);
   }

   public long generateChannelID()
   {
      return idGenerator.generateID();
   }

   public synchronized void syncIDGeneratorSequence(final long id)
   {
      if (!idGeneratorSynced)
      {
         idGenerator = new SimpleIDGenerator(id);

         idGeneratorSynced = true;
      }
   }

   public long getIDGeneratorSequence()
   {
      return idGenerator.getCurrentID();
   }

   public Object getTransferLock()
   {
      return transferLock;
   }

   public boolean isClient()
   {
      return client;
   }

   public boolean isDestroyed()
   {
      return destroyed;
   }

   public long getBlockingCallTimeout()
   {
      return blockingCallTimeout;
   }

   @Override
   public long getBlockingCallFailoverTimeout()
   {
      return blockingCallFailoverTimeout;
   }

   public boolean checkDataReceived()
   {
      boolean res = dataReceived;

      dataReceived = false;

      return res;
   }

   //We flush any confirmations on the connection - this prevents idle bridges for example
   //sitting there with many unacked messages
   public void flush()
   {
      synchronized (transferLock)
      {
         for (Channel channel : channels.values())
         {
            channel.flushConfirmations();
         }
      }
   }

   public HornetQPrincipal getDefaultHornetQPrincipal()
   {
      return transportConnection.getDefaultHornetQPrincipal();
   }

   // Buffer Handler implementation
   // ----------------------------------------------------

   public void bufferReceived(final Object connectionID, final HornetQBuffer buffer)
   {
      try
      {
         final Packet packet = packetDecoder.decode(buffer);

         if (isTrace)
         {
            HornetQClientLogger.LOGGER.trace("handling packet " + packet);
         }

         if (packet.isAsyncExec() && executor != null)
         {
            executing = true;

            executor.execute(new Runnable()
            {
               public void run()
               {
                  try
                  {
                     doBufferReceived(packet);
                  }
                  catch (Throwable t)
                  {
                     HornetQClientLogger.LOGGER.errorHandlingPacket(t, packet);
                  }

                  executing = false;
               }
            });
         }
         else
         {
            //To prevent out of order execution if interleaving sync and async operations on same connection
            while (executing)
            {
               Thread.yield();
            }

            // Pings must always be handled out of band so we can send pings back to the client quickly
            // otherwise they would get in the queue with everything else which might give an intolerable delay
            doBufferReceived(packet);
         }

         dataReceived = true;
      }
      catch (Exception e)
      {
         HornetQClientLogger.LOGGER.errorDecodingPacket(e);
      }
   }

   private void doBufferReceived(final Packet packet)
   {
      if (ChannelImpl.invokeInterceptors(packet, incomingInterceptors, this) != null)
      {
         return;
      }

      synchronized (transferLock)
      {
         final Channel channel = channels.get(packet.getChannelID());

         if (channel != null)
         {
            channel.handlePacket(packet);
         }
      }
   }

   private void removeAllChannels()
   {
      // We get the transfer lock first - this ensures no packets are being processed AND
      // it's guaranteed no more packets will be processed once this method is complete
      synchronized (transferLock)
      {
         channels.clear();
      }
   }

   private void callFailureListeners(final HornetQException me, String scaleDownTargetNodeID)
   {
      final List<FailureListener> listenersClone = new ArrayList<FailureListener>(failureListeners);

      for (final FailureListener listener : listenersClone)
      {
         try
         {
            listener.connectionFailed(me, false, scaleDownTargetNodeID);
         }
         catch (HornetQInterruptedException interrupted)
         {
            // this is an expected behaviour.. no warn or error here
            HornetQClientLogger.LOGGER.debug("thread interrupted", interrupted);
         }
         catch (final Throwable t)
         {
            // Failure of one listener to execute shouldn't prevent others
            // from
            // executing
            HornetQClientLogger.LOGGER.errorCallingFailureListener(t);
         }
      }
   }

   private void callClosingListeners()
   {
      final List<CloseListener> listenersClone = new ArrayList<CloseListener>(closeListeners);

      for (final CloseListener listener : listenersClone)
      {
         try
         {
            listener.connectionClosed();
         }
         catch (final Throwable t)
         {
            // Failure of one listener to execute shouldn't prevent others
            // from
            // executing
            HornetQClientLogger.LOGGER.errorCallingFailureListener(t);
         }
      }
   }

   private void internalClose()
   {
      // We close the underlying transport connection
      transportConnection.close();

      for (Channel channel : channels.values())
      {
         channel.close();
      }
   }

   public void setClientID(String cID)
   {
      clientID = cID;
   }

   public String getClientID()
   {
      return clientID;
   }
}
