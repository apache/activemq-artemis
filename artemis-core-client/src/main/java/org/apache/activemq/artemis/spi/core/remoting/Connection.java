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
package org.apache.activemq.artemis.spi.core.remoting;

import io.netty.channel.ChannelFutureListener;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.security.ActiveMQPrincipal;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;

/**
 * The connection used by a channel to write data to.
 */
public interface Connection {

   /**
    * Create a new ActiveMQBuffer of the given size.
    *
    * @param size the size of buffer to create
    * @return the new buffer.
    */
   ActiveMQBuffer createTransportBuffer(int size);

   RemotingConnection getProtocolConnection();

   void setProtocolConnection(RemotingConnection connection);

   boolean isWritable(ReadyListener listener);

   void fireReady(boolean ready);

   /**
    * This will disable reading from the channel.
    * This is basically the same as blocking the reading.
    * */
   void setAutoRead(boolean autoRead);

   /**
    * returns the unique id of this wire.
    *
    * @return the id
    */
   Object getID();

   /**
    * writes the buffer to the connection and if flush is true returns only when the buffer has been physically written to the connection.
    *
    * @param buffer  the buffer to write
    * @param flush   whether to flush the buffers onto the wire
    * @param batched whether the packet is allowed to batched for better performance
    */
   void write(ActiveMQBuffer buffer, boolean flush, boolean batched);

   /**
    * writes the buffer to the connection and if flush is true returns only when the buffer has been physically written to the connection.
    *
    * @param buffer  the buffer to write
    * @param flush   whether to flush the buffers onto the wire
    * @param batched whether the packet is allowed to batched for better performance
    */
   void write(ActiveMQBuffer buffer, boolean flush, boolean batched, ChannelFutureListener futureListener);

   /**
    * writes the buffer to the connection with no flushing or batching
    *
    * @param buffer the buffer to write
    */
   void write(ActiveMQBuffer buffer);

   /**
    * This should close the internal channel without calling any listeners.
    * This is to avoid a situation where the broker is busy writing on an internal thread.
    * This should close the socket releasing any pending threads.
    */
   void forceClose();

   /**
    * Closes the connection.
    */
   void close();

   /**
    * Returns a string representation of the remote address this connection is connected to.
    *
    * @return the remote address
    */
   String getRemoteAddress();

   /**
    * Returns a string representation of the local address this connection is connected to.
    * This is useful when the server is configured at 0.0.0.0 (or multiple IPs).
    * This will give you the actual IP that's being used.
    *
    * @return the local address
    */
   String getLocalAddress();

   /**
    * Called periodically to flush any data in the batch buffer
    */
   void checkFlushBatchBuffer();

   /**
    * Generates a {@link TransportConfiguration} to be used to connect to the same target this is
    * connected to.
    *
    * @return TransportConfiguration
    */
   TransportConfiguration getConnectorConfig();

   ActiveMQPrincipal getDefaultActiveMQPrincipal();

   /**
    * the InVM Connection has some special handling as it doesn't use Netty ProtocolChannel
    * we will use this method Instead of using instanceof
    *
    * @return
    */
   boolean isUsingProtocolHandling();
}