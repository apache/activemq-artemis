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
package org.apache.activemq6.core.protocol.core;

import java.util.concurrent.locks.Lock;

import org.apache.activemq6.api.core.HornetQException;

/**
 * A channel is a way of interleaving data meant for different endpoints over the same {@link org.apache.activemq6.core.protocol.core.CoreRemotingConnection}.
 * <p>
 * Any packet sent will have its channel id set to the specific channel sending so it can be routed to its correct channel
 * when received by the {@link org.apache.activemq6.core.protocol.core.CoreRemotingConnection}. see {@link org.hornetq.core.protocol.core.Packet#setChannelID(long)}.
 * <p>
 * Each Channel should will forward any packets received to its {@link org.apache.activemq6.core.protocol.core.ChannelHandler}.
 * <p>
 * A Channel *does not* support concurrent access by more than one thread!
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public interface Channel
{
   /**
    * Returns the id of this channel.
    * @return the id
    */
   long getID();

   /** For protocol check */
   boolean supports(byte packetID);

   /**
    * Sends a packet on this channel.
    * @param packet the packet to send
    * @return false if the packet was rejected by an outgoing interceptor; true if the send was
    *         successful
    */
   boolean send(Packet packet);

   /**
    * Sends a packet on this channel using batching algorithm if appropriate
    * @param packet the packet to send
    * @return false if the packet was rejected by an outgoing interceptor; true if the send was
    *         successful
    */
   boolean sendBatched(Packet packet);

   /**
    * Sends a packet on this channel and then blocks until it has been written to the connection.
    * @param packet the packet to send
    * @return false if the packet was rejected by an outgoing interceptor; true if the send was
    *         successful
    */
   boolean sendAndFlush(Packet packet);

   /**
    * Sends a packet on this channel and then blocks until a response is received or a timeout
    * occurs.
    * @param packet the packet to send
    * @param expectedPacket the packet being expected.
    * @return the response
    * @throws HornetQException if an error occurs during the send
    */
   Packet sendBlocking(Packet packet, byte expectedPacket) throws HornetQException;

   /**
    * Sets the {@link org.apache.activemq6.core.protocol.core.ChannelHandler} that this channel should
    * forward received packets to.
    * @param handler the handler
    */
   void setHandler(ChannelHandler handler);

   /**
    * Gets the {@link org.apache.activemq6.core.protocol.core.ChannelHandler} that this channel should
    * forward received packets to.
    * @return the current channel handler
    */
   ChannelHandler getHandler();

   /**
    * Closes this channel.
    * <p>
    * once closed no packets can be sent.
    */
   void close();

   /**
    * Transfers the connection used by this channel to the one specified.
    * <p>
    * All new packets will be sent via this connection.
    * @param newConnection the new connection
    */
   void transferConnection(CoreRemotingConnection newConnection);

   /**
    * resends any packets that have not received confirmations yet.
    * <p>
    * Typically called after a connection has been transferred.
    *
    * @param lastConfirmedCommandID the last confirmed packet
    */
   void replayCommands(int lastConfirmedCommandID);

   /**
    * returns the last confirmed packet command id
    *
    * @return the id
    */
   int getLastConfirmedCommandID();

   /**
    * locks the channel.
    * <p>
    * While locked no packets can be sent or received
    */
   void lock();

   /**
    * unlocks the channel.
    */
   void unlock();

   /**
    * forces any {@link org.apache.activemq6.core.protocol.core.Channel#sendBlocking(Packet, byte)} request to return with an exception.
    */
   void returnBlocking();

   /**
    * forces any {@link org.apache.activemq6.core.protocol.core.Channel#sendBlocking(Packet, byte)} request to return with an exception.
    */
   void returnBlocking(Throwable cause);

   /**
    * returns the channel lock
    *
    * @return the lock
    */
   Lock getLock();

   /**
    * returns the {@link CoreRemotingConnection} being used by the channel
    */
   CoreRemotingConnection getConnection();

   /**
    * sends a confirmation of a packet being received.
    *
    * @param packet the packet to confirm
    */
   void confirm(Packet packet);

   /**
    * sets the handler to use when a confirmation is received.
    *
    * @param handler the handler to call
    */
   void setCommandConfirmationHandler(CommandConfirmationHandler handler);

   /**
    * flushes any confirmations on to the connection.
    */
   void flushConfirmations();

   /**
    * Called by {@link org.apache.activemq6.core.protocol.core.CoreRemotingConnection} when a packet is received.
    * <p>
    * This method should then call its {@link org.apache.activemq6.core.protocol.core.ChannelHandler} after appropriate processing of
    * the packet
    *
    * @param packet the packet to process.
    */
   void handlePacket(Packet packet);

   /**
    * clears any commands from the cache that are yet to be confirmed.
    */
   void clearCommands();

   /**
    * returns the confirmation window size this channel is using.
    *
    * @return the window size
    */
   int getConfirmationWindowSize();

   /**
    * notifies the channel if it is transferring its connection. When true it is illegal to send messages.
    *
    * @param transferring whether the channel is transferring
    */
   void setTransferring(boolean transferring);
}
