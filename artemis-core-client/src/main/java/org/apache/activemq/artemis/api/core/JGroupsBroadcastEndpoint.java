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
package org.apache.activemq.artemis.api.core;

import org.jgroups.JChannel;
import org.jgroups.ReceiverAdapter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * This class is the implementation of ActiveMQ Artemis members discovery that will use JGroups.
 */
public abstract class JGroupsBroadcastEndpoint implements BroadcastEndpoint
{
   private final String channelName;

   private boolean clientOpened;

   private boolean broadcastOpened;

   private JChannelWrapper channel;

   private JGroupsReceiver receiver;

   public JGroupsBroadcastEndpoint(String channelName)
   {
      this.channelName = channelName;
   }

   public void broadcast(final byte[] data) throws Exception
   {
      if (broadcastOpened)
      {
         org.jgroups.Message msg = new org.jgroups.Message();

         msg.setBuffer(data);

         channel.send(msg);
      }
   }

   public byte[] receiveBroadcast() throws Exception
   {
      if (clientOpened)
      {
         return receiver.receiveBroadcast();
      }
      else
      {
         return null;
      }
   }

   public byte[] receiveBroadcast(long time, TimeUnit unit) throws Exception
   {
      if (clientOpened)
      {
         return receiver.receiveBroadcast(time, unit);
      }
      else
      {
         return null;
      }
   }

   public synchronized void openClient() throws Exception
   {
      if (clientOpened)
      {
         return;
      }
      internalOpen();
      receiver = new JGroupsReceiver();
      channel.addReceiver(receiver);
      clientOpened = true;
   }

   public synchronized void openBroadcaster() throws Exception
   {
      if (broadcastOpened) return;
      internalOpen();
      broadcastOpened = true;
   }

   public abstract JChannel createChannel() throws Exception;

   public JGroupsBroadcastEndpoint initChannel() throws Exception
   {
      this.channel = JChannelManager.getJChannel(channelName, this);
      return this;
   }

   protected void internalOpen() throws Exception
   {
      channel.connect();
   }

   public synchronized void close(boolean isBroadcast) throws Exception
   {
      if (isBroadcast)
      {
         broadcastOpened = false;
      }
      else
      {
         channel.removeReceiver(receiver);
         clientOpened = false;
      }
      channel.close();
   }

   /**
    * This class is used to receive messages from a JGroups channel.
    * Incoming messages are put into a queue.
    */
   private static final class JGroupsReceiver extends ReceiverAdapter
   {
      private final BlockingQueue<byte[]> dequeue = new LinkedBlockingDeque<byte[]>();

      @Override
      public void receive(org.jgroups.Message msg)
      {
         dequeue.add(msg.getBuffer());
      }

      public byte[] receiveBroadcast() throws Exception
      {
         return dequeue.take();
      }

      public byte[] receiveBroadcast(long time, TimeUnit unit) throws Exception
      {
         return dequeue.poll(time, unit);
      }
   }

   /**
    * This class wraps a JChannel with a reference counter. The reference counter
    * controls the life of the JChannel. When reference count is zero, the channel
    * will be disconnected.
    */
   protected static class JChannelWrapper
   {
      int refCount = 1;
      JChannel channel;
      String channelName;
      final List<JGroupsReceiver> receivers = new ArrayList<JGroupsReceiver>();

      public JChannelWrapper(String channelName, JChannel channel) throws Exception
      {
         this.refCount = 1;
         this.channelName = channelName;
         this.channel = channel;
      }

      public synchronized void close()
      {
         refCount--;
         if (refCount == 0)
         {
            JChannelManager.closeChannel(this.channelName, channel);
         }
      }

      public void removeReceiver(JGroupsReceiver receiver)
      {
         synchronized (receivers)
         {
            receivers.remove(receiver);
         }
      }

      public synchronized void connect() throws Exception
      {
         if (channel.isConnected()) return;
         channel.setReceiver(new ReceiverAdapter()
         {

            @Override
            public void receive(org.jgroups.Message msg)
            {
               synchronized (receivers)
               {
                  for (JGroupsReceiver r : receivers)
                  {
                     r.receive(msg);
                  }
               }
            }
         });
         channel.connect(channelName);
      }

      public void addReceiver(JGroupsReceiver jGroupsReceiver)
      {
         synchronized (receivers)
         {
            receivers.add(jGroupsReceiver);
         }
      }

      public void send(org.jgroups.Message msg) throws Exception
      {
         channel.send(msg);
      }

      public JChannelWrapper addRef()
      {
         this.refCount++;
         return this;
      }

      @Override
      public String toString()
      {
         return "JChannelWrapper of [" + channel + "] " + refCount + " " + channelName;
      }
   }

   /**
    * This class maintain a global Map of JChannels wrapped in JChannelWrapper for
    * the purpose of reference counting.
    * <p/>
    * Wherever a JChannel is needed it should only get it by calling the getChannel()
    * method of this class. The real disconnect of channels are also done here only.
    */
   protected static class JChannelManager
   {
      private static Map<String, JChannelWrapper> channels;

      public static synchronized JChannelWrapper getJChannel(String channelName, JGroupsBroadcastEndpoint endpoint) throws Exception
      {
         if (channels == null)
         {
            channels = new HashMap<>();
         }
         JChannelWrapper wrapper = channels.get(channelName);
         if (wrapper == null)
         {
            wrapper = new JChannelWrapper(channelName, endpoint.createChannel());
            channels.put(channelName, wrapper);
            return wrapper;
         }
         return wrapper.addRef();
      }

      public static synchronized void closeChannel(String channelName, JChannel channel)
      {
         channel.setReceiver(null);
         channel.disconnect();
         channel.close();
         JChannelWrapper wrapper = channels.remove(channelName);
         if (wrapper == null)
         {
            throw new IllegalStateException("Did not find channel " + channelName);
         }
      }
   }
}
