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
package org.apache.activemq.api.core;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.core.client.HornetQClientLogger;


/**
 * The configuration used to determine how the server will broadcast members.
 * <p>
 * This is analogous to {@link org.apache.activemq.api.core.DiscoveryGroupConfiguration}
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a> Created 18 Nov 2008 08:44:30
 */
public final class UDPBroadcastGroupConfiguration implements BroadcastEndpointFactoryConfiguration, DiscoveryGroupConfigurationCompatibilityHelper
{
   private static final long serialVersionUID = 1052413739064253955L;

   private transient String localBindAddress = null;

   private transient int localBindPort = -1;

   private String groupAddress = null;

   private int groupPort = -1;

   public UDPBroadcastGroupConfiguration()
   {
   }

   public BroadcastEndpointFactory createBroadcastEndpointFactory()
   {
      return new BroadcastEndpointFactory()
      {
         @Override
         public BroadcastEndpoint createBroadcastEndpoint() throws Exception
         {
            return new UDPBroadcastEndpoint()
               .setGroupAddress(groupAddress != null ? InetAddress.getByName(groupAddress) : null)
               .setGroupPort(groupPort)
               .setLocalBindAddress(localBindAddress != null ? InetAddress.getByName(localBindAddress) : null)
               .setLocalBindPort(localBindPort);
         }
      };
   }

   public String getGroupAddress()
   {
      return groupAddress;
   }

   public UDPBroadcastGroupConfiguration setGroupAddress(String groupAddress)
   {
      this.groupAddress = groupAddress;
      return this;
   }

   public int getGroupPort()
   {
      return groupPort;
   }

   public UDPBroadcastGroupConfiguration setGroupPort(int groupPort)
   {
      this.groupPort = groupPort;
      return this;
   }

   public int getLocalBindPort()
   {
      return localBindPort;
   }

   public UDPBroadcastGroupConfiguration setLocalBindPort(int localBindPort)
   {
      this.localBindPort = localBindPort;
      return this;
   }

   public String getLocalBindAddress()
   {
      return localBindAddress;
   }

   public UDPBroadcastGroupConfiguration setLocalBindAddress(String localBindAddress)
   {
      this.localBindAddress = localBindAddress;
      return this;
   }

   /**
    * <p> This is the member discovery implementation using direct UDP. It was extracted as a refactoring from
    * {@link org.apache.activemq.core.cluster.DiscoveryGroup}</p>
    *
    * @author Tomohisa
    * @author Howard Gao
    * @author Clebert Suconic
    */
   private static class UDPBroadcastEndpoint implements BroadcastEndpoint
   {
      private static final int SOCKET_TIMEOUT = 500;

      private InetAddress localAddress;

      private int localBindPort;

      private InetAddress groupAddress;

      private int groupPort;

      private DatagramSocket broadcastingSocket;

      private MulticastSocket receivingSocket;

      private volatile boolean open;

      public UDPBroadcastEndpoint()
      {
      }

      public UDPBroadcastEndpoint setGroupAddress(InetAddress groupAddress)
      {
         this.groupAddress = groupAddress;
         return this;
      }

      public UDPBroadcastEndpoint setGroupPort(int groupPort)
      {
         this.groupPort = groupPort;
         return this;
      }

      public UDPBroadcastEndpoint setLocalBindAddress(InetAddress localAddress)
      {
         this.localAddress = localAddress;
         return this;
      }

      public UDPBroadcastEndpoint setLocalBindPort(int localBindPort)
      {
         this.localBindPort = localBindPort;
         return this;
      }


      public void broadcast(byte[] data) throws Exception
      {
         DatagramPacket packet = new DatagramPacket(data, data.length, groupAddress, groupPort);
         broadcastingSocket.send(packet);
      }

      public byte[] receiveBroadcast() throws Exception
      {
         final byte[] data = new byte[65535];
         final DatagramPacket packet = new DatagramPacket(data, data.length);

         while (open)
         {
            try
            {
               receivingSocket.receive(packet);
            }
            // TODO: Do we need this?
            catch (InterruptedIOException e)
            {
               continue;
            }
            catch (IOException e)
            {
               if (open)
               {
                  HornetQClientLogger.LOGGER.warn(this + " getting exception when receiving broadcasting.", e);
               }
            }
            break;
         }
         return data;
      }

      public byte[] receiveBroadcast(long time, TimeUnit unit) throws Exception
      {
         // We just use the regular method on UDP, there's no timeout support
         // and this is basically for tests only
         return receiveBroadcast();
      }

      public void openBroadcaster() throws Exception
      {
         if (localBindPort != -1)
         {
            broadcastingSocket = new DatagramSocket(localBindPort, localAddress);
         }
         else
         {
            if (localAddress != null)
            {
               HornetQClientLogger.LOGGER.broadcastGroupBindError();
            }
            broadcastingSocket = new DatagramSocket();
         }

         open = true;
      }

      public void openClient() throws Exception
      {
         // HORNETQ-874
         if (checkForLinux() || checkForSolaris() || checkForHp())
         {
            try
            {
               receivingSocket = new MulticastSocket(new InetSocketAddress(groupAddress, groupPort));
            }
            catch (IOException e)
            {
               HornetQClientLogger.LOGGER.ioDiscoveryError(groupAddress.getHostAddress(), groupAddress instanceof Inet4Address ? "IPv4" : "IPv6");

               receivingSocket = new MulticastSocket(groupPort);
            }
         }
         else
         {
            receivingSocket = new MulticastSocket(groupPort);
         }

         if (localAddress != null)
         {
            receivingSocket.setInterface(localAddress);
         }

         receivingSocket.joinGroup(groupAddress);

         receivingSocket.setSoTimeout(SOCKET_TIMEOUT);

         open = true;
      }

      //@Todo: using isBroadcast to share endpoint between broadcast and receiving
      public void close(boolean isBroadcast) throws Exception
      {
         open = false;

         if (broadcastingSocket != null)
         {
            broadcastingSocket.close();
         }

         if (receivingSocket != null)
         {
            receivingSocket.close();
         }
      }

      private static boolean checkForLinux()
      {
         return checkForPresence("os.name", "linux");
      }

      private static boolean checkForHp()
      {
         return checkForPresence("os.name", "hp");
      }

      private static boolean checkForSolaris()
      {
         return checkForPresence("os.name", "sun");
      }

      private static boolean checkForPresence(String key, String value)
      {
         try
         {
            String tmp = System.getProperty(key);
            return tmp != null && tmp.trim().toLowerCase().startsWith(value);
         }
         catch (Throwable t)
         {
            return false;
         }
      }

   }

   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((groupAddress == null) ? 0 : groupAddress.hashCode());
      result = prime * result + groupPort;
      return result;
   }

   @Override
   public boolean equals(Object obj)
   {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      UDPBroadcastGroupConfiguration other = (UDPBroadcastGroupConfiguration) obj;
      if (groupAddress == null)
      {
         if (other.groupAddress != null)
            return false;
      }
      else if (!groupAddress.equals(other.groupAddress))
         return false;
      if (groupPort != other.groupPort)
         return false;
      return true;
   }
}
