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

import javax.security.auth.Subject;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.buffers.impl.ChannelBufferWrapper;
import org.apache.activemq.artemis.core.protocol.core.Channel;
import org.apache.activemq.artemis.core.protocol.core.CommandConfirmationHandler;
import org.apache.activemq.artemis.core.protocol.core.CoreRemotingConnection;
import org.apache.activemq.artemis.core.protocol.core.Packet;
import org.apache.activemq.artemis.core.protocol.core.ResponseHandler;
import org.apache.activemq.artemis.core.protocol.core.impl.wireformat.PacketsConfirmedMessage;
import org.apache.activemq.artemis.core.remoting.CloseListener;
import org.apache.activemq.artemis.core.remoting.FailureListener;
import org.apache.activemq.artemis.core.security.ActiveMQPrincipal;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.apache.activemq.artemis.spi.core.remoting.ReadyListener;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ChannelImplTest {

   ChannelImpl channel;

   @Before
   public void setUp() {
      channel = new ChannelImpl(new CoreRR(), 1, 4000, null);
   }

   @Test
   public void testCorrelation() {

      AtomicInteger handleResponseCount = new AtomicInteger();

      RequestPacket requestPacket = new RequestPacket((byte) 1);
      setResponseHandlerAsPerActiveMQSessionContext((packet, response) -> handleResponseCount.incrementAndGet());

      channel.send(requestPacket);

      assertEquals(1, channel.getCache().size());

      ResponsePacket responsePacket = new ResponsePacket((byte) 1);
      responsePacket.setCorrelationID(requestPacket.getCorrelationID());

      channel.handlePacket(responsePacket);

      assertEquals(1, handleResponseCount.get());
      assertEquals(0, channel.getCache().size());
   }

   private void setResponseHandlerAsPerActiveMQSessionContext(ResponseHandler responseHandler) {
      channel.setResponseHandler(responseHandler);
      channel.setCommandConfirmationHandler(wrapAsPerActiveMQSessionContext(responseHandler));
   }

   private CommandConfirmationHandler wrapAsPerActiveMQSessionContext(ResponseHandler responseHandler) {
      return new CommandConfirmationHandler() {
         @Override
         public void commandConfirmed(Packet packet) {
            responseHandler.handleResponse(packet, null);
         }
      };
   }

   @Test
   public void testPacketsConfirmedMessage() {

      AtomicInteger handleResponseCount = new AtomicInteger();

      RequestPacket requestPacket = new RequestPacket((byte) 1);
      setResponseHandlerAsPerActiveMQSessionContext((packet, response) -> handleResponseCount.incrementAndGet());

      channel.send(requestPacket);

      PacketsConfirmedMessage responsePacket = new PacketsConfirmedMessage((byte) 2);

      channel.handlePacket(responsePacket);

      assertEquals(0, channel.getCache().size());
   }

   class RequestPacket extends PacketImpl {

      private long id;

      RequestPacket(byte type) {
         super(type);
      }

      @Override
      public boolean isRequiresResponse() {
         return true;
      }

      @Override
      public boolean isResponseAsync() {
         return true;
      }

      @Override
      public long getCorrelationID() {
         return id;
      }

      @Override
      public void setCorrelationID(long id) {
         this.id = id;
      }

      @Override
      public int getPacketSize() {
         return 0;
      }
   }

   class ResponsePacket extends PacketImpl {

      private long id;

      ResponsePacket(byte type) {
         super(type);
      }

      @Override
      public boolean isResponseAsync() {
         return true;
      }

      @Override
      public boolean isResponse() {
         return true;
      }

      @Override
      public long getCorrelationID() {
         return id;
      }

      @Override
      public void setCorrelationID(long id) {
         this.id = id;
      }

      @Override
      public int getPacketSize() {
         return 0;
      }
   }

   class CoreRR implements CoreRemotingConnection {

      @Override
      public int getChannelVersion() {
         return 0;
      }

      @Override
      public void setChannelVersion(int clientVersion) {

      }

      @Override
      public Channel getChannel(long channelID, int confWindowSize) {
         return null;
      }

      @Override
      public void putChannel(long channelID, Channel channel) {

      }

      @Override
      public boolean removeChannel(long channelID) {
         return false;
      }

      @Override
      public long generateChannelID() {
         return 0;
      }

      @Override
      public void syncIDGeneratorSequence(long id) {

      }

      @Override
      public long getIDGeneratorSequence() {
         return 0;
      }

      @Override
      public long getBlockingCallTimeout() {
         return 0;
      }

      @Override
      public long getBlockingCallFailoverTimeout() {
         return 0;
      }

      @Override
      public Object getTransferLock() {
         return null;
      }

      @Override
      public ActiveMQPrincipal getDefaultActiveMQPrincipal() {
         return null;
      }

      @Override
      public boolean blockUntilWritable(long timeout) {
         return false;
      }

      @Override
      public Object getID() {
         return null;
      }

      @Override
      public long getCreationTime() {
         return 0;
      }

      @Override
      public String getRemoteAddress() {
         return null;
      }

      @Override
      public void scheduledFlush() {

      }

      @Override
      public void addFailureListener(FailureListener listener) {

      }

      @Override
      public boolean removeFailureListener(FailureListener listener) {
         return false;
      }

      @Override
      public void addCloseListener(CloseListener listener) {

      }

      @Override
      public boolean removeCloseListener(CloseListener listener) {
         return false;
      }

      @Override
      public List<CloseListener> removeCloseListeners() {
         return null;
      }

      @Override
      public void setCloseListeners(List<CloseListener> listeners) {

      }

      @Override
      public List<FailureListener> getFailureListeners() {
         return null;
      }

      @Override
      public List<FailureListener> removeFailureListeners() {
         return null;
      }

      @Override
      public void setFailureListeners(List<FailureListener> listeners) {

      }

      @Override
      public ActiveMQBuffer createTransportBuffer(int size) {
         return new ChannelBufferWrapper(Unpooled.buffer(size));
      }

      @Override
      public void fail(ActiveMQException me) {

      }

      @Override
      public Future asyncFail(ActiveMQException me) {
         return null;
      }

      @Override
      public void fail(ActiveMQException me, String scaleDownTargetNodeID) {

      }

      @Override
      public void destroy() {

      }

      @Override
      public Connection getTransportConnection() {
         return new Connection() {
            @Override
            public ActiveMQBuffer createTransportBuffer(int size) {
               return null;
            }

            @Override
            public RemotingConnection getProtocolConnection() {
               return null;
            }

            @Override
            public void setProtocolConnection(RemotingConnection connection) {

            }

            @Override
            public boolean isOpen() {
               return true;
            }

            @Override
            public boolean isWritable(ReadyListener listener) {
               return false;
            }

            @Override
            public void fireReady(boolean ready) {

            }

            @Override
            public void setAutoRead(boolean autoRead) {

            }

            @Override
            public Object getID() {
               return null;
            }

            @Override
            public void write(ActiveMQBuffer buffer, boolean requestFlush) {

            }

            @Override
            public void write(ActiveMQBuffer buffer, boolean flush, boolean batched) {

            }

            @Override
            public void write(ActiveMQBuffer buffer,
                              boolean flush,
                              boolean batched,
                              ChannelFutureListener futureListener) {

            }

            @Override
            public void write(ActiveMQBuffer buffer) {

            }

            @Override
            public void forceClose() {

            }

            @Override
            public void close() {

            }

            @Override
            public String getRemoteAddress() {
               return null;
            }

            @Override
            public String getLocalAddress() {
               return null;
            }

            @Override
            public void checkFlushBatchBuffer() {

            }

            @Override
            public TransportConfiguration getConnectorConfig() {
               return null;
            }

            @Override
            public boolean isDirectDeliver() {
               return false;
            }

            @Override
            public ActiveMQPrincipal getDefaultActiveMQPrincipal() {
               return null;
            }

            @Override
            public boolean isUsingProtocolHandling() {
               return false;
            }

            @Override
            public boolean isSameTarget(TransportConfiguration... configs) {
               return false;
            }
         };
      }

      @Override
      public boolean isClient() {
         return true;
      }

      @Override
      public boolean isDestroyed() {
         return false;
      }

      @Override
      public void disconnect(boolean criticalError) {

      }

      @Override
      public void disconnect(String scaleDownNodeID, boolean criticalError) {

      }

      @Override
      public boolean checkDataReceived() {
         return false;
      }

      @Override
      public void flush() {

      }

      @Override
      public boolean isWritable(ReadyListener callback) {
         return false;
      }

      @Override
      public void killMessage(SimpleString nodeID) {

      }

      @Override
      public boolean isSupportReconnect() {
         return false;
      }

      @Override
      public boolean isSupportsFlowControl() {
         return false;
      }

      @Override
      public void setAuditSubject(Subject subject) {

      }

      @Override
      public Subject getAuditSubject() {
         return null;
      }

      @Override
      public Subject getSubject() {
         return null;
      }

      @Override
      public String getProtocolName() {
         return null;
      }

      @Override
      public void setClientID(String cID) {

      }

      @Override
      public String getClientID() {
         return null;
      }

      @Override
      public String getTransportLocalAddress() {
         return null;
      }

      @Override
      public void bufferReceived(Object connectionID, ActiveMQBuffer buffer) {

      }
   }

}