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

package org.hornetq.core.protocol.proton;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.qpid.proton.amqp.transport.AmqpError;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Sasl;
import org.apache.qpid.proton.engine.Session;
import org.apache.qpid.proton.engine.impl.ConnectionImpl;
import org.apache.qpid.proton.engine.impl.DeliveryImpl;
import org.apache.qpid.proton.engine.impl.LinkImpl;
import org.apache.qpid.proton.engine.impl.TransportImpl;
import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQException;
import org.hornetq.core.protocol.proton.exceptions.HornetQAMQPException;
import org.hornetq.core.remoting.CloseListener;
import org.hornetq.core.remoting.FailureListener;
import org.hornetq.core.server.HornetQServerLogger;
import org.hornetq.core.server.impl.ServerMessageImpl;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.spi.core.remoting.Acceptor;
import org.hornetq.spi.core.remoting.Connection;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class ProtonRemotingConnection implements RemotingConnection
{
   private TransportImpl protonTransport;

   private ConnectionImpl protonConnection;

   private final Map<Object, ProtonSession> sessions = new HashMap<Object, ProtonSession>();

   /*
   * Proton is not thread safe therefore we need to make sure we aren't updating the deliveries on the connection from
   * the input of proton transport and asynchronously back from HornetQ at the same time.
   * (this probably needs to be fixed on Proton)
   * */
   private final Object deliveryLock = new Object();

   private boolean destroyed = false;

   private String clientId;

   private final Acceptor acceptorUsed;

   private final long creationTime;

   private final Connection connection;

   private final ProtonProtocolManager protonProtocolManager;

   private final List<FailureListener> failureListeners = new CopyOnWriteArrayList<FailureListener>();

   private final List<CloseListener> closeListeners = new CopyOnWriteArrayList<CloseListener>();

   private boolean initialised = false;

   private static final byte[] VERSION_HEADER = new byte[]{
      'A', 'M', 'Q', 'P', 0, 1, 0, 0
   };
   private Sasl sasl;

   private String username;

   private String passcode;

   private boolean dataReceived;

   public ProtonRemotingConnection(Acceptor acceptorUsed, Connection connection, ProtonProtocolManager protonProtocolManager)
   {
      this.protonProtocolManager = protonProtocolManager;

      this.connection = connection;

      this.creationTime = System.currentTimeMillis();

      this.acceptorUsed = acceptorUsed;

      this.protonTransport = new TransportImpl();

      this.protonConnection = new ConnectionImpl();

      protonTransport.bind(protonConnection);
   }

   @Override
   public Object getID()
   {
      return connection.getID();
   }

   @Override
   public long getCreationTime()
   {
      return creationTime;
   }

   @Override
   public String getRemoteAddress()
   {
      return connection.getRemoteAddress();
   }

   @Override
   public void addFailureListener(final FailureListener listener)
   {
      if (listener == null)
      {
         throw new IllegalStateException("FailureListener cannot be null");
      }

      failureListeners.add(listener);
   }

   @Override
   public boolean removeFailureListener(final FailureListener listener)
   {
      if (listener == null)
      {
         throw new IllegalStateException("FailureListener cannot be null");
      }

      return failureListeners.remove(listener);
   }

   @Override
   public void addCloseListener(final CloseListener listener)
   {
      if (listener == null)
      {
         throw new IllegalStateException("CloseListener cannot be null");
      }

      closeListeners.add(listener);
   }

   @Override
   public boolean removeCloseListener(final CloseListener listener)
   {
      if (listener == null)
      {
         throw new IllegalStateException("CloseListener cannot be null");
      }

      return closeListeners.remove(listener);
   }

   @Override
   public List<CloseListener> removeCloseListeners()
   {
      List<CloseListener> ret = new ArrayList<CloseListener>(closeListeners);

      closeListeners.clear();

      return ret;
   }

   @Override
   public List<FailureListener> removeFailureListeners()
   {
      List<FailureListener> ret = new ArrayList<FailureListener>(failureListeners);

      failureListeners.clear();

      return ret;
   }

   @Override
   public void setCloseListeners(List<CloseListener> listeners)
   {
      closeListeners.clear();

      closeListeners.addAll(listeners);
   }

   @Override
   public void setFailureListeners(final List<FailureListener> listeners)
   {
      failureListeners.clear();

      failureListeners.addAll(listeners);
   }

   public List<FailureListener> getFailureListeners()
   {
      // we do not return the listeners otherwise the remoting service
      // would NOT destroy the connection.
      return Collections.emptyList();
   }

   @Override
   public HornetQBuffer createBuffer(int size)
   {
      return connection.createBuffer(size);
   }

   @Override
   public void fail(HornetQException me)
   {
      HornetQServerLogger.LOGGER.connectionFailureDetected(me.getMessage(), me.getType());
      // Then call the listeners
      callFailureListeners(me);

      callClosingListeners();

      destroyed = true;

      connection.close();
   }

   @Override
   public void fail(HornetQException me, String scaleDownTargetNodeID)
   {
      fail(me);
   }

   @Override
   public void destroy()
   {
      destroyed = true;

      connection.close();

      synchronized (deliveryLock)
      {
         callClosingListeners();
      }
   }

   @Override
   public Connection getTransportConnection()
   {
      return connection;
   }

   @Override
   public boolean isClient()
   {
      return false;
   }

   @Override
   public boolean isDestroyed()
   {
      return destroyed;
   }

   @Override
   public void disconnect(final boolean criticalError)
   {
      disconnect(null, criticalError);
   }

   @Override
   public void disconnect(final String scaleDownNodeID, final boolean criticalError)
   {
      destroy();
   }

   @Override
   public boolean checkDataReceived()
   {
      boolean res = dataReceived;

      dataReceived = false;

      return res;
   }

   @Override
   public void flush()
   {
      //no op
   }

   @Override
   public void bufferReceived(Object connectionID, HornetQBuffer buffer)
   {
      if (initialised)
      {
         protonProtocolManager.handleBuffer(this, buffer);
      }
      else
      {
         byte[] prot = new byte[4];
         buffer.readBytes(prot);
         String headerProt = new String(prot);
         checkProtocol(headerProt);
         int protocolId = buffer.readByte();
         int major = buffer.readByte();
         int minor = buffer.readByte();
         int revision = buffer.readByte();
         if (!(checkVersion(major, minor, revision) && checkProtocol(headerProt)))
         {
            protonTransport.close();
            protonConnection.close();
            write();
            destroy();
            return;
         }
         if (protocolId == 3)
         {
            sasl = protonTransport.sasl();
            sasl.setMechanisms(new String[]{"ANONYMOUS", "PLAIN"});
            sasl.server();
         }

         ///its only 8 bytes, there's always going to always be enough in the buffer, isn't there?
         protonTransport.input(VERSION_HEADER, 0, VERSION_HEADER.length);

         write();

         initialised = true;

         if (buffer.readableBytes() > 0)
         {
            protonProtocolManager.handleBuffer(this, buffer.copy(buffer.readerIndex(), buffer.readableBytes()));
         }

         if (sasl != null)
         {
            if (sasl.getRemoteMechanisms().length > 0)
            {
               if ("PLAIN".equals(sasl.getRemoteMechanisms()[0]))
               {
                  byte[] data = new byte[sasl.pending()];
                  sasl.recv(data, 0, data.length);
                  sasl.done(Sasl.SaslOutcome.PN_SASL_OK);
                  sasl = null;
               }
               else if ("ANONYMOUS".equals(sasl.getRemoteMechanisms()[0]))
               {
                  sasl.done(Sasl.SaslOutcome.PN_SASL_OK);
                  sasl = null;
               }
            }

            write();
         }
      }
   }

   private boolean checkProtocol(String headerProt)
   {
      boolean ok = "AMQP".equals(headerProt);
      if (!ok)
      {
         protonConnection.setCondition(new ErrorCondition(AmqpError.ILLEGAL_STATE, "Unknown Protocol " + headerProt));
      }
      return ok;
   }

   private boolean checkVersion(int major, int minor, int revision)
   {
      if (major < 1)
      {
         protonConnection.setCondition(new ErrorCondition(AmqpError.ILLEGAL_STATE,
                                                          "Version not supported " + major + "." + minor + "." + revision));
         return false;
      }
      return true;
   }

   void write()
   {
      synchronized (deliveryLock)
      {
         int size = 1024 * 64;
         byte[] data = new byte[size];
         boolean done = false;
         while (!done)
         {
            int count = protonTransport.output(data, 0, size);
            if (count > 0)
            {
               final HornetQBuffer buffer;
               buffer = connection.createBuffer(count);
               buffer.writeBytes(data, 0, count);
               connection.write(buffer);
            }
            else
            {
               done = true;
            }
         }
      }
   }

   public String getLogin()
   {
      return username;
   }

   public String getPasscode()
   {
      return passcode;
   }

   public ServerMessageImpl createServerMessage()
   {
      return protonProtocolManager.createServerMessage();
   }

   protected synchronized void setDataReceived()
   {
      dataReceived = true;
   }

   public void handleFrame(byte[] frame)
   {
      int read = 0;
      while (read < frame.length)
      {
         synchronized (deliveryLock)
         {
            try
            {
               int count = protonTransport.input(frame, read, frame.length - read);
               read += count;
            }
            catch (Exception e)
            {
               protonTransport.setCondition(new ErrorCondition(AmqpError.DECODE_ERROR, HornetQAMQPProtocolMessageBundle.BUNDLE.decodeError()));
               write();
               protonConnection.close();
               return;
            }
         }

         if (sasl != null)
         {
            if (sasl.getRemoteMechanisms().length > 0)
            {
               if ("PLAIN".equals(sasl.getRemoteMechanisms()[0]))
               {
                  byte[] data = new byte[sasl.pending()];
                  sasl.recv(data, 0, data.length);
                  setUserPass(data);
                  sasl.done(Sasl.SaslOutcome.PN_SASL_OK);
                  sasl = null;
               }
               else if ("ANONYMOUS".equals(sasl.getRemoteMechanisms()[0]))
               {
                  sasl.done(Sasl.SaslOutcome.PN_SASL_OK);
                  sasl = null;
               }
            }
         }

         //handle opening of connection
         if (protonConnection.getLocalState() == EndpointState.UNINITIALIZED && protonConnection.getRemoteState() != EndpointState.UNINITIALIZED)
         {
            clientId = protonConnection.getRemoteContainer();
            protonConnection.open();
            write();
         }

         //handle any new sessions
         Session session = protonConnection.sessionHead(ProtonProtocolManager.UNINITIALIZED, ProtonProtocolManager.INITIALIZED);
         while (session != null)
         {
            try
            {
               ProtonSession protonSession = getSession(session);
               session.setContext(protonSession);
               session.open();

            }
            catch (HornetQAMQPException e)
            {
               protonConnection.setCondition(new ErrorCondition(e.getAmqpError(), e.getMessage()));
               session.close();
            }
            write();
            session = protonConnection.sessionHead(ProtonProtocolManager.UNINITIALIZED, ProtonProtocolManager.INITIALIZED);
         }

         //handle new link (producer or consumer
         LinkImpl link = (LinkImpl) protonConnection.linkHead(ProtonProtocolManager.UNINITIALIZED, ProtonProtocolManager.INITIALIZED);
         while (link != null)
         {
            try
            {
               protonProtocolManager.handleNewLink(link, getSession(link.getSession()));
            }
            catch (HornetQAMQPException e)
            {
               link.setCondition(new ErrorCondition(e.getAmqpError(), e.getMessage()));
               link.close();
            }
            link = (LinkImpl) protonConnection.linkHead(ProtonProtocolManager.UNINITIALIZED, ProtonProtocolManager.INITIALIZED);
         }

         //handle any deliveries
         DeliveryImpl delivery;

         Iterator<DeliveryImpl> iterator = protonConnection.getWorkSequence();

         while (iterator.hasNext())
         {
            delivery = iterator.next();
            ProtonDeliveryHandler handler = (ProtonDeliveryHandler) delivery.getLink().getContext();
            try
            {
               handler.onMessage(delivery);
            }
            catch (HornetQAMQPException e)
            {
               delivery.getLink().setCondition(new ErrorCondition(e.getAmqpError(), e.getMessage()));
            }
         }

         link = (LinkImpl) protonConnection.linkHead(ProtonProtocolManager.ACTIVE, ProtonProtocolManager.ANY_ENDPOINT_STATE);
         while (link != null)
         {
            try
            {
               protonProtocolManager.handleActiveLink(link);
            }
            catch (HornetQAMQPException e)
            {
               link.setCondition(new ErrorCondition(e.getAmqpError(), e.getMessage()));
            }
            link = (LinkImpl) link.next(ProtonProtocolManager.ACTIVE, ProtonProtocolManager.ANY_ENDPOINT_STATE);
         }

         link = (LinkImpl) protonConnection.linkHead(ProtonProtocolManager.ACTIVE, ProtonProtocolManager.CLOSED);
         while (link != null)
         {
            try
            {
               ((ProtonDeliveryHandler) link.getContext()).close();
            }
            catch (HornetQAMQPException e)
            {
               link.setCondition(new ErrorCondition(e.getAmqpError(), e.getMessage()));
            }
            link.close();

            link = (LinkImpl) link.next(ProtonProtocolManager.ACTIVE, ProtonProtocolManager.CLOSED);
         }

         session = protonConnection.sessionHead(ProtonProtocolManager.ACTIVE, ProtonProtocolManager.CLOSED);
         while (session != null)
         {
            ProtonSession protonSession = (ProtonSession) session.getContext();
            protonSession.close();
            sessions.remove(session);
            session.close();
            session = session.next(ProtonProtocolManager.ACTIVE, ProtonProtocolManager.CLOSED);
         }

         if (protonConnection.getLocalState() == EndpointState.ACTIVE && protonConnection.getRemoteState() == EndpointState.CLOSED)
         {
            for (ProtonSession protonSession : sessions.values())
            {
               protonSession.close();
            }
            sessions.clear();
            protonConnection.close();
            write();
            destroy();
         }

         write();
      }
   }

   private void setUserPass(byte[] data)
   {
      String bytes = new String(data);
      String[] credentials = bytes.split(Character.toString((char) 0));
      int offSet = 0;
      if (credentials.length > 0)
      {
         if (credentials[0].length() == 0)
         {
            offSet = 1;
         }

         if (credentials.length >= offSet)
         {
            username = credentials[offSet];
         }
         if (credentials.length >= (offSet + 1))
         {
            passcode = credentials[offSet + 1];
         }
      }
   }

   private ProtonSession getSession(Session realSession) throws HornetQAMQPException
   {
      ProtonSession protonSession = sessions.get(realSession);
      if (protonSession == null)
      {
         protonSession = protonProtocolManager.createSession(this, protonTransport);
         sessions.put(realSession, protonSession);
      }
      return protonSession;

   }

   private void callFailureListeners(final HornetQException me)
   {
      final List<FailureListener> listenersClone = new ArrayList<FailureListener>(failureListeners);

      for (final FailureListener listener : listenersClone)
      {
         try
         {
            listener.connectionFailed(me, false);
         }
         catch (final Throwable t)
         {
            // Failure of one listener to execute shouldn't prevent others
            // from
            // executing
            HornetQServerLogger.LOGGER.errorCallingFailureListener(t);
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
            HornetQServerLogger.LOGGER.errorCallingFailureListener(t);
         }
      }
   }

   public Object getDeliveryLock()
   {
      return deliveryLock;
   }
}
