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
package org.apache.activemq.api.core;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.conf.PlainConfigurator;

/**
 * The configuration for creating broadcasting/discovery groups using JGroups channels
 * There are two ways to constructing a JGroups channel (JChannel):
 * <ol>
 * <li> by passing in a JGroups configuration file<br>
 * The file must exists in the activemq classpath. ActiveMQ creates a JChannel with the
 * configuration file and use it for broadcasting and discovery. In standalone server
 * mode ActiveMQ uses this way for constructing JChannels.</li>
 * <li> by passing in a JChannel instance<br>
 * This is useful when ActiveMQ needs to get a JChannel from a running JGroups service as in the
 * case of AS7 integration.</li>
 * </ol>
 * <p>
 * Note only one JChannel is needed in a VM. To avoid the channel being prematurely disconnected
 * by any party, a wrapper class is used.
 *
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 * @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
 * @see JChannelWrapper, JChannelManager
 */
public final class JGroupsBroadcastGroupConfiguration implements BroadcastEndpointFactoryConfiguration, DiscoveryGroupConfigurationCompatibilityHelper
{
   private static final long serialVersionUID = 8952238567248461285L;

   private final BroadcastEndpointFactory factory;

   public JGroupsBroadcastGroupConfiguration(final String jgroupsFile, final String channelName)
   {
      factory = new BroadcastEndpointFactory()
      {
         private static final long serialVersionUID = 1047956472941098435L;

         @Override
         public BroadcastEndpoint createBroadcastEndpoint() throws Exception
         {
            JGroupsBroadcastEndpoint endpoint = new JGroupsBroadcastEndpoint();
            endpoint.initChannel(jgroupsFile, channelName);
            return endpoint;
         }
      };
   }

   public JGroupsBroadcastGroupConfiguration(final JChannel channel, final String channelName)
   {
      factory = new BroadcastEndpointFactory()
      {
         private static final long serialVersionUID = 5110372849181145377L;

         @Override
         public BroadcastEndpoint createBroadcastEndpoint() throws Exception
         {
            JGroupsBroadcastEndpoint endpoint = new JGroupsBroadcastEndpoint();
            endpoint.initChannel(channel, channelName);
            return endpoint;
         }
      };
   }

   @Override
   public BroadcastEndpointFactory createBroadcastEndpointFactory()
   {
      return factory;
   }

   @Override
   public String getLocalBindAddress()
   {
      return null;
   }

   @Override
   /*
   * return -1 to force deserialization of object
   * */
   public int getLocalBindPort()
   {
      return -1;
   }

   @Override
   public String getGroupAddress()
   {
      return null;
   }

   @Override
   public int getGroupPort()
   {
      return -1;
   }

   /**
    * This class is the implementation of ActiveMQ members discovery that will use JGroups.
    *
    * @author Howard Gao
    */
   private static final class JGroupsBroadcastEndpoint implements BroadcastEndpoint
   {
      private boolean clientOpened;

      private boolean broadcastOpened;

      private JChannelWrapper<?> channel;

      private JGroupsReceiver receiver;

      public void broadcast(final byte[] data) throws Exception
      {
         if (broadcastOpened)
         {
            Message msg = new Message();

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
         channel.setReceiver(receiver);
         clientOpened = true;
      }

      public synchronized void openBroadcaster() throws Exception
      {
         if (broadcastOpened) return;
         internalOpen();
         broadcastOpened = true;
      }

      private void initChannel(final String jgroupsConfig, final String channelName) throws Exception
      {
         PlainConfigurator configurator = new PlainConfigurator(jgroupsConfig);
         try
         {
            this.channel = JChannelManager.getJChannel(channelName, configurator);
            return;
         }
         catch (Exception e)
         {
            this.channel = null;
         }
         URL configURL = Thread.currentThread().getContextClassLoader().getResource(jgroupsConfig);

         if (configURL == null)
         {
            throw new RuntimeException("couldn't find JGroups configuration " + jgroupsConfig);
         }
         this.channel = JChannelManager.getJChannel(channelName, configURL);
      }

      private void initChannel(final JChannel channel1, final String channelName) throws Exception
      {
         this.channel = JChannelManager.getJChannel(channelName, channel1);
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
       *
       * @param <T>
       */
      private static class JChannelWrapper<T>
      {
         int refCount = 1;
         JChannel channel;
         String channelName;
         List<JGroupsReceiver> receivers = new ArrayList<JGroupsReceiver>();

         public JChannelWrapper(String channelName, T t) throws Exception
         {
            this.refCount = 1;
            this.channelName = channelName;
            if (t instanceof URL)
            {
               this.channel = new JChannel((URL) t);
            }
            else if (t instanceof JChannel)
            {
               this.channel = (JChannel) t;
            }
            else if (t instanceof PlainConfigurator)
            {
               this.channel = new JChannel((PlainConfigurator)t);
            }
            else
            {
               throw new IllegalArgumentException("Unsupported type " + t);
            }
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
               public void receive(Message msg)
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

         public void setReceiver(JGroupsReceiver jGroupsReceiver)
         {
            synchronized (receivers)
            {
               receivers.add(jGroupsReceiver);
            }
         }

         public void send(Message msg) throws Exception
         {
            channel.send(msg);
         }

         public JChannelWrapper<T> addRef()
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
      private static class JChannelManager
      {
         private static Map<String, JChannelWrapper<?>> channels;

         public static synchronized <T> JChannelWrapper<?> getJChannel(String channelName, T t) throws Exception
         {
            if (channels == null)
            {
               channels = new HashMap<String, JChannelWrapper<?>>();
            }
            JChannelWrapper<?> wrapper = channels.get(channelName);
            if (wrapper == null)
            {
               wrapper = new JChannelWrapper<T>(channelName, t);
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
            JChannelWrapper<?> wrapper = channels.remove(channelName);
            if (wrapper == null)
            {
               throw new IllegalStateException("Did not find channel " + channelName);
            }
         }
      }
   }
}
