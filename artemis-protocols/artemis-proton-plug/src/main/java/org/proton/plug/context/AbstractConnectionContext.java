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
package org.proton.plug.context;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.netty.buffer.ByteBuf;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Session;
import org.apache.qpid.proton.engine.Transport;
import org.proton.plug.AMQPConnectionCallback;
import org.proton.plug.AMQPConnectionContext;
import org.proton.plug.SASLResult;
import org.proton.plug.exceptions.ActiveMQAMQPException;
import org.proton.plug.handler.ProtonHandler;
import org.proton.plug.handler.impl.DefaultEventHandler;
import org.proton.plug.util.ByteUtil;
import org.proton.plug.util.DebugInfo;

import static org.proton.plug.context.AMQPConstants.Connection.DEFAULT_IDLE_TIMEOUT;
import static org.proton.plug.context.AMQPConstants.Connection.DEFAULT_CHANNEL_MAX;
import static org.proton.plug.context.AMQPConstants.Connection.DEFAULT_MAX_FRAME_SIZE;

public abstract class AbstractConnectionContext extends ProtonInitializable implements AMQPConnectionContext
{

   protected ProtonHandler handler = ProtonHandler.Factory.create();

   protected AMQPConnectionCallback connectionCallback;

   private final Map<Session, AbstractProtonSessionContext> sessions = new ConcurrentHashMap<>();

   protected LocalListener listener = new LocalListener();

   public AbstractConnectionContext(AMQPConnectionCallback connectionCallback)
   {
      this(connectionCallback, DEFAULT_IDLE_TIMEOUT, DEFAULT_MAX_FRAME_SIZE, DEFAULT_CHANNEL_MAX);
   }

   public AbstractConnectionContext(AMQPConnectionCallback connectionCallback, int idleTimeout, int maxFrameSize, int channelMax)
   {
      this.connectionCallback = connectionCallback;
      connectionCallback.setConnection(this);
      Transport transport = handler.getTransport();
      if (idleTimeout > 0)
      {
         transport.setIdleTimeout(idleTimeout);
         transport.tick(idleTimeout / 2);
      }
      transport.setChannelMax(channelMax);
      transport.setMaxFrameSize(maxFrameSize);
      handler.addEventHandler(listener);
   }

   public SASLResult getSASLResult()
   {
      return handler.getSASLResult();
   }

   @Override
   public void inputBuffer(ByteBuf buffer)
   {
      if (DebugInfo.debug)
      {
         ByteUtil.debugFrame("Buffer Received ", buffer);
      }

      handler.inputBuffer(buffer);
   }

   public void destroy()
   {
      connectionCallback.close();
   }

   /**
    * See comment at {@link org.proton.plug.AMQPConnectionContext#isSyncOnFlush()}
    */
   public boolean isSyncOnFlush()
   {
      return false;
   }


   public Object getLock()
   {
      return handler.getLock();
   }

   @Override
   public int capacity()
   {
      return handler.capacity();
   }

   @Override
   public void outputDone(int bytes)
   {
      handler.outputDone(bytes);
   }

   public void flush()
   {
      handler.flush();
   }

   public void close()
   {
      handler.close();
   }

   protected AbstractProtonSessionContext getSessionExtension(Session realSession) throws ActiveMQAMQPException
   {
      AbstractProtonSessionContext sessionExtension = sessions.get(realSession);
      if (sessionExtension == null)
      {
         // how this is possible? Log a warn here
         sessionExtension = newSessionExtension(realSession);
         realSession.setContext(sessionExtension);
         sessions.put(realSession, sessionExtension);
      }
      return sessionExtension;
   }

   protected abstract void remoteLinkOpened(Link link) throws Exception;


   protected abstract AbstractProtonSessionContext newSessionExtension(Session realSession) throws ActiveMQAMQPException;

   @Override
   public boolean checkDataReceived()
   {
      return handler.checkDataReceived();
   }

   @Override
   public long getCreationTime()
   {
      return handler.getCreationTime();
   }

   protected void flushBytes()
   {
      ByteBuf bytes;
      // handler.outputBuffer has the lock
      while ((bytes = handler.outputBuffer()) != null)
      {
         connectionCallback.onTransport(bytes, AbstractConnectionContext.this);
      }
   }


   // This listener will perform a bunch of things here
   class LocalListener extends DefaultEventHandler
   {

      @Override
      public void onSASLInit(ProtonHandler handler, Connection connection)
      {
         handler.createServerSASL(connectionCallback.getSASLMechnisms());
      }

      @Override
      public void onTransport(Transport transport)
      {
         flushBytes();
      }

      @Override
      public void onRemoteOpen(Connection connection) throws Exception
      {
         synchronized (getLock())
         {
            connection.setContext(AbstractConnectionContext.this);
            connection.open();
         }
         initialise();
      }


      @Override
      public void onRemoteClose(Connection connection)
      {
         synchronized (getLock())
         {
            connection.close();
            for (AbstractProtonSessionContext protonSession : sessions.values())
            {
               protonSession.close();
            }
            sessions.clear();
         }
         // We must force write the channel before we actually destroy the connection
         onTransport(handler.getTransport());
         destroy();
      }

      @Override
      public void onLocalOpen(Session session) throws Exception
      {
         getSessionExtension(session);
      }

      @Override
      public void onRemoteOpen(Session session) throws Exception
      {
         getSessionExtension(session).initialise();
         synchronized (getLock())
         {
            session.open();
         }
      }


      @Override
      public void onLocalClose(Session session) throws Exception
      {
      }

      @Override
      public void onRemoteClose(Session session) throws Exception
      {
         synchronized (getLock())
         {
            session.close();
         }

         AbstractProtonSessionContext sessionContext = (AbstractProtonSessionContext) session.getContext();
         if (sessionContext != null)
         {
            sessionContext.close();
            sessions.remove(session);
            session.setContext(null);
         }
      }

      @Override
      public void onRemoteOpen(Link link) throws Exception
      {
         remoteLinkOpened(link);
      }

      @Override
      public void onFlow(Link link) throws Exception
      {
         ((ProtonDeliveryHandler) link.getContext()).onFlow(link.getCredit());
      }

      @Override
      public void onRemoteClose(Link link) throws Exception
      {
         link.close();
         ProtonDeliveryHandler linkContext = (ProtonDeliveryHandler) link.getContext();
         if (linkContext != null)
         {
            linkContext.close();
         }
      }


      public void onRemoteDetach(Link link) throws Exception
      {
         link.detach();
      }

      public void onDelivery(Delivery delivery) throws Exception
      {
         ProtonDeliveryHandler handler = (ProtonDeliveryHandler) delivery.getLink().getContext();
         if (handler != null)
         {
            handler.onMessage(delivery);
         }
         else
         {
            // TODO: logs

            System.err.println("Handler is null, can't delivery " + delivery);
         }
      }


   }

}
