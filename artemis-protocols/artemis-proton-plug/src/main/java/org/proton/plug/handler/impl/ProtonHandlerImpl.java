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
package org.proton.plug.handler.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Collector;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Event;
import org.apache.qpid.proton.engine.Sasl;
import org.apache.qpid.proton.engine.Transport;
import org.proton.plug.ClientSASL;
import org.proton.plug.ServerSASL;
import org.proton.plug.handler.EventHandler;
import org.proton.plug.handler.Events;
import org.proton.plug.handler.ProtonHandler;
import org.proton.plug.context.ProtonInitializable;
import org.proton.plug.SASLResult;
import org.proton.plug.util.ByteUtil;
import org.proton.plug.util.DebugInfo;

/**
 * Clebert Suconic
 */
public class ProtonHandlerImpl extends ProtonInitializable implements ProtonHandler {


   private final Transport transport = Proton.transport();

   private final Connection connection = Proton.connection();

   private final Collector collector = Proton.collector();

   private final Executor dispatchExecutor;

   private final Runnable dispatchRunnable = new Runnable() {
      @Override
      public void run() {
         dispatch();
      }
   };

   private ArrayList<EventHandler> handlers = new ArrayList<>();

   private Sasl serverSasl;

   private Sasl clientSasl;

   private final Object lock = new Object();

   private final long creationTime;

   private Map<String, ServerSASL> saslHandlers;

   private SASLResult saslResult;

   protected volatile boolean dataReceived;

   protected boolean receivedFirstPacket = false;

   private int offset = 0;

   public ProtonHandlerImpl(Executor dispatchExecutor) {
      this.dispatchExecutor = dispatchExecutor;
      this.creationTime = System.currentTimeMillis();
      transport.bind(connection);
      connection.collect(collector);
   }

   @Override
   public long tick(boolean firstTick) {
      if (!firstTick) {
         try {
            if (connection.getLocalState() != EndpointState.CLOSED) {
               long rescheduleAt = transport.tick(TimeUnit.NANOSECONDS.toMillis(System.nanoTime()));
               if (transport.isClosed()) {
                  throw new IllegalStateException("Channel was inactive for to long");
               }
               return rescheduleAt;
            }
         }
         catch (Exception e) {
            transport.close();
            connection.setCondition(new ErrorCondition());
         }
         return 0;
      }
      return transport.tick(TimeUnit.NANOSECONDS.toMillis(System.nanoTime()));
   }

   @Override
   public int capacity() {
      synchronized (lock) {
         return transport.capacity();
      }
   }

   @Override
   public Object getLock() {
      return lock;
   }

   @Override
   public Transport getTransport() {
      return transport;
   }

   @Override
   public Connection getConnection() {
      return connection;
   }

   @Override
   public ProtonHandler addEventHandler(EventHandler handler) {
      handlers.add(handler);
      return this;
   }

   @Override
   public void createServerSASL(ServerSASL[] handlers) {
      this.serverSasl = transport.sasl();
      saslHandlers = new HashMap<>();
      String[] names = new String[handlers.length];
      int count = 0;
      for (ServerSASL handler : handlers) {
         saslHandlers.put(handler.getName(), handler);
         names[count++] = handler.getName();
      }
      this.serverSasl.server();
      serverSasl.setMechanisms(names);

   }

   @Override
   public SASLResult getSASLResult() {
      return saslResult;
   }

   @Override
   public void inputBuffer(ByteBuf buffer) {
      dataReceived = true;
      synchronized (lock) {
         while (buffer.readableBytes() > 0) {
            int capacity = transport.capacity();

            if (!receivedFirstPacket) {
               try {
                  if (buffer.getByte(4) == 0x03) {
                     dispatchSASL();
                  }
               }
               catch (Throwable ignored) {
                  ignored.printStackTrace();
               }

               receivedFirstPacket = true;
            }

            if (capacity > 0) {
               ByteBuffer tail = transport.tail();
               int min = Math.min(capacity, buffer.readableBytes());
               tail.limit(min);
               buffer.readBytes(tail);

               flush();
            }
            else {
               if (capacity == 0) {
                  System.out.println("abandoning: " + buffer.readableBytes());
               }
               else {
                  System.out.println("transport closed, discarding: " + buffer.readableBytes() + " capacity = " + transport.capacity());
               }
               break;
            }
         }
      }
   }

   @Override
   public boolean checkDataReceived() {
      boolean res = dataReceived;

      dataReceived = false;

      return res;
   }

   @Override
   public long getCreationTime() {
      return creationTime;
   }

   @Override
   public void outputDone(int bytes) {
      synchronized (lock) {
         transport.pop(bytes);
         offset -= bytes;

         if (offset < 0) {
            throw new IllegalStateException("You called outputDone for more bytes than you actually received. numberOfBytes=" + bytes +
                                               ", outcome result=" + offset);
         }
      }

      flush();
   }

   @Override
   public ByteBuf outputBuffer() {

      synchronized (lock) {
         int pending = transport.pending();

         if (pending < 0) {
            return null;//throw new IllegalStateException("xxx need to close the connection");
         }

         int size = pending - offset;

         if (size < 0) {
            throw new IllegalStateException("negative size: " + pending);
         }

         if (size == 0) {
            return null;
         }

         // For returning PooledBytes
         ByteBuf buffer = PooledByteBufAllocator.DEFAULT.buffer(size);
         ByteBuffer head = transport.head();
         head.position(offset);
         buffer.writeBytes(head);
         offset += size; // incrementing offset for future calls
         return buffer;
      }
   }

   @Override
   public void createClientSasl(ClientSASL clientSASL) {
      if (clientSASL != null) {
         clientSasl = transport.sasl();
         clientSasl.setMechanisms(clientSASL.getName());
         byte[] initialSasl = clientSASL.getBytes();
         clientSasl.send(initialSasl, 0, initialSasl.length);
      }
   }

   @Override
   public void flush() {
      synchronized (lock) {
         transport.process();

         checkServerSASL();

      }

      dispatchExecutor.execute(dispatchRunnable);
   }

   @Override
   public void close() {
      synchronized (lock) {
         connection.close();
      }
      flush();
   }

   protected void checkServerSASL() {
      if (serverSasl != null && serverSasl.getRemoteMechanisms().length > 0) {
         // TODO: should we look at the first only?
         ServerSASL mechanism = saslHandlers.get(serverSasl.getRemoteMechanisms()[0]);
         if (mechanism != null) {

            byte[] dataSASL = new byte[serverSasl.pending()];
            serverSasl.recv(dataSASL, 0, dataSASL.length);

            if (DebugInfo.debug) {
               System.out.println("Working on sasl::" + ByteUtil.bytesToHex(dataSASL, 2));
            }

            saslResult = mechanism.processSASL(dataSASL);

            if (saslResult != null && saslResult.isSuccess()) {
               serverSasl.done(Sasl.SaslOutcome.PN_SASL_OK);
               serverSasl = null;
               saslHandlers.clear();
               saslHandlers = null;
            }
            else {
               serverSasl.done(Sasl.SaslOutcome.PN_SASL_AUTH);
            }
            serverSasl = null;
         }
         else {
            // no auth available, system error
            serverSasl.done(Sasl.SaslOutcome.PN_SASL_SYS);
         }
      }
   }

   private Event popEvent() {
      synchronized (lock) {
         Event ev = collector.peek();
         if (ev != null) {
            // pop will invalidate the event
            // for that reason we make a new one
            // Events are reused inside the collector, so we need to make a new one here
            ev = ev.copy();
            collector.pop();
         }
         return ev;
      }
   }

   private void dispatchSASL() {
      for (EventHandler h : handlers) {
         h.onSASLInit(this, getConnection());
      }
   }

   private void dispatch() {
      Event ev;
      // We don't hold a lock on the entire event processing
      // because we could have a distributed deadlock
      // while processing events (for instance onTransport)
      // while a client is also trying to write here
      while ((ev = popEvent()) != null) {
         for (EventHandler h : handlers) {
            if (DebugInfo.debug) {
               System.out.println("Handling " + ev + " towards " + h);
            }
            try {
               Events.dispatch(ev, h);
            }
            catch (Exception e) {
               // TODO: logs
               e.printStackTrace();
               connection.setCondition(new ErrorCondition());
            }
         }
      }

      for (EventHandler h : handlers) {
         try {
            h.onTransport(transport);
         }
         catch (Exception e) {
            // TODO: logs
            e.printStackTrace();
            connection.setCondition(new ErrorCondition());
         }
      }

   }

}
