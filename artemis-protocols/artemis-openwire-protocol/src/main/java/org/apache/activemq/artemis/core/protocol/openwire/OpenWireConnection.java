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
package org.apache.activemq.artemis.core.protocol.openwire;

import javax.jms.InvalidDestinationException;
import javax.jms.JMSSecurityException;
import javax.jms.ResourceAllocationException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQNonExistentQueueException;
import org.apache.activemq.artemis.api.core.ActiveMQSecurityException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.ConnectionControl;
import org.apache.activemq.command.ConnectionError;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerControl;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.ControlCommand;
import org.apache.activemq.command.DataArrayResponse;
import org.apache.activemq.command.DestinationInfo;
import org.apache.activemq.command.ExceptionResponse;
import org.apache.activemq.command.FlushCommand;
import org.apache.activemq.command.KeepAliveInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageDispatchNotification;
import org.apache.activemq.command.MessagePull;
import org.apache.activemq.command.ProducerAck;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.RemoveInfo;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.command.ShutdownInfo;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.TransactionInfo;
import org.apache.activemq.command.WireFormatInfo;
import org.apache.activemq.artemis.core.protocol.openwire.amq.AMQBrokerStoppedException;
import org.apache.activemq.artemis.core.protocol.openwire.amq.AMQConnectionContext;
import org.apache.activemq.artemis.core.protocol.openwire.amq.AMQConsumerBrokerExchange;
import org.apache.activemq.artemis.core.protocol.openwire.amq.AMQMapTransportConnectionStateRegister;
import org.apache.activemq.artemis.core.protocol.openwire.amq.AMQMessageAuthorizationPolicy;
import org.apache.activemq.artemis.core.protocol.openwire.amq.AMQProducerBrokerExchange;
import org.apache.activemq.artemis.core.protocol.openwire.amq.AMQSession;
import org.apache.activemq.artemis.core.protocol.openwire.amq.AMQSingleTransportConnectionStateRegister;
import org.apache.activemq.artemis.core.protocol.openwire.amq.AMQTransaction;
import org.apache.activemq.artemis.core.protocol.openwire.amq.AMQTransportConnectionState;
import org.apache.activemq.artemis.core.protocol.openwire.amq.AMQTransportConnectionStateRegister;
import org.apache.activemq.artemis.core.remoting.CloseListener;
import org.apache.activemq.artemis.core.remoting.FailureListener;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.remoting.Acceptor;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.apache.activemq.state.CommandVisitor;
import org.apache.activemq.state.ConnectionState;
import org.apache.activemq.state.ConsumerState;
import org.apache.activemq.state.ProducerState;
import org.apache.activemq.state.SessionState;
import org.apache.activemq.thread.TaskRunner;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.transport.TransmitCallback;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.artemis.utils.ConcurrentHashSet;
import org.apache.activemq.wireformat.WireFormat;

/**
 * Represents an activemq connection.
 */
public class OpenWireConnection implements RemotingConnection, CommandVisitor
{
   private final OpenWireProtocolManager protocolManager;

   private final Connection transportConnection;

   private final AMQConnectorImpl acceptorUsed;

   private final long creationTime;

   private final List<FailureListener> failureListeners = new CopyOnWriteArrayList<FailureListener>();

   private final List<CloseListener> closeListeners = new CopyOnWriteArrayList<CloseListener>();

   private boolean destroyed = false;

   private final Object sendLock = new Object();

   private boolean dataReceived;

   private OpenWireFormat wireFormat;

   private AMQTransportConnectionStateRegister connectionStateRegister = new AMQSingleTransportConnectionStateRegister();

   private boolean faultTolerantConnection;

   private AMQConnectionContext context;

   private AMQMessageAuthorizationPolicy messageAuthorizationPolicy;

   private boolean networkConnection;

   private boolean manageable;

   private boolean pendingStop;

   private Throwable stopError = null;

   // should come from activemq server
   private final TaskRunnerFactory stopTaskRunnerFactory = null;

   private boolean starting;

   private final AtomicBoolean stopping = new AtomicBoolean(false);

   private final ReentrantReadWriteLock serviceLock = new ReentrantReadWriteLock();

   private final CountDownLatch stopped = new CountDownLatch(1);

   protected TaskRunner taskRunner;

   private boolean active;

   protected final List<Command> dispatchQueue = new LinkedList<Command>();

   private boolean markedCandidate;

   private boolean blockedCandidate;

   private long timeStamp;

   private boolean inServiceException;

   private final AtomicBoolean asyncException = new AtomicBoolean(false);

   private final Map<ConsumerId, AMQConsumerBrokerExchange> consumerExchanges = new HashMap<ConsumerId, AMQConsumerBrokerExchange>();
   private final Map<ProducerId, AMQProducerBrokerExchange> producerExchanges = new HashMap<ProducerId, AMQProducerBrokerExchange>();

   private AMQTransportConnectionState state;

   private final Set<String> tempQueues = new ConcurrentHashSet<String>();

   protected final Map<ConnectionId, ConnectionState> brokerConnectionStates;

   private DataInputWrapper dataInput = new DataInputWrapper();

   private Map<TransactionId, TransactionInfo> txMap = new ConcurrentHashMap<TransactionId, TransactionInfo>();

   private volatile AMQSession advisorySession;

   public OpenWireConnection(Acceptor acceptorUsed, Connection connection,
         OpenWireProtocolManager openWireProtocolManager, OpenWireFormat wf)
   {
      this.protocolManager = openWireProtocolManager;
      this.transportConnection = connection;
      this.acceptorUsed = new AMQConnectorImpl(acceptorUsed);
      this.wireFormat = wf;
      brokerConnectionStates = protocolManager.getConnectionStates();
      this.creationTime = System.currentTimeMillis();
   }

   @Override
   public void bufferReceived(Object connectionID, ActiveMQBuffer buffer)
   {
      try
      {
         dataInput.receiveData(buffer);
      }
      catch (Throwable t)
      {
         ActiveMQServerLogger.LOGGER.error("decoding error", t);
         return;
      }

      // this.setDataReceived();
      while (dataInput.readable())
      {
         try
         {
            Object object = null;
            try
            {
               object = wireFormat.unmarshal(dataInput);
               dataInput.mark();
            }
            catch (NotEnoughBytesException e)
            {
               //meaning the dataInput hasn't enough bytes for a command.
               //in that case we just return and waiting for the next
               //call of bufferReceived()
               return;
            }

            Command command = (Command) object;
            boolean responseRequired = command.isResponseRequired();
            int commandId = command.getCommandId();
            // the connection handles pings, negotiations directly.
            // and delegate all other commands to manager.
            if (command.getClass() == KeepAliveInfo.class)
            {
               KeepAliveInfo info = (KeepAliveInfo) command;
               if (info.isResponseRequired())
               {
                  info.setResponseRequired(false);
                  protocolManager.sendReply(this, info);
               }
            }
            else if (command.getClass() == WireFormatInfo.class)
            {
               // amq here starts a read/write monitor thread (detect ttl?)
               negotiate((WireFormatInfo) command);
            }
            else if (command.getClass() == ConnectionInfo.class
                  || command.getClass() == ConsumerInfo.class
                  || command.getClass() == RemoveInfo.class
                  || command.getClass() == SessionInfo.class
                  || command.getClass() == ProducerInfo.class
                  || ActiveMQMessage.class.isAssignableFrom(command.getClass())
                  || command.getClass() == MessageAck.class
                  || command.getClass() == TransactionInfo.class
                  || command.getClass() == DestinationInfo.class
                  || command.getClass() == ShutdownInfo.class)
            {
               Response response = null;

               if (pendingStop)
               {
                  response = new ExceptionResponse(this.stopError);
               }
               else
               {
                  response = ((Command) command).visit(this);

                  if (response instanceof ExceptionResponse)
                  {
                     if (!responseRequired)
                     {
                        Throwable cause = ((ExceptionResponse)response).getException();
                        serviceException(cause);
                        response = null;
                     }
                  }
               }

               if (responseRequired)
               {
                  if (response == null)
                  {
                     response = new Response();
                  }
               }

               // The context may have been flagged so that the response is not
               // sent.
               if (context != null)
               {
                  if (context.isDontSendReponse())
                  {
                     context.setDontSendReponse(false);
                     response = null;
                  }
                  context = null;
               }

               if (response != null && !protocolManager.isStopping())
               {
                  response.setCorrelationId(commandId);
                  dispatchSync(response);
               }

            }
            else
            {
               // note!!! wait for negotiation (e.g. use a countdown latch)
               // before handling any other commands
               this.protocolManager.handleCommand(this, command);
            }
         }
         catch (IOException e)
         {
            ActiveMQServerLogger.LOGGER.error("error decoding", e);
         }
         catch (Throwable t)
         {
            ActiveMQServerLogger.LOGGER.error("error decoding", t);
         }
      }
   }

   private void negotiate(WireFormatInfo command) throws IOException
   {
      this.wireFormat.renegotiateWireFormat(command);
   }

   @Override
   public Object getID()
   {
      return transportConnection.getID();
   }

   @Override
   public long getCreationTime()
   {
      return creationTime;
   }

   @Override
   public String getRemoteAddress()
   {
      return transportConnection.getRemoteAddress();
   }

   @Override
   public void addFailureListener(FailureListener listener)
   {
      if (listener == null)
      {
         throw new IllegalStateException("FailureListener cannot be null");
      }

      failureListeners.add(listener);
   }

   @Override
   public boolean removeFailureListener(FailureListener listener)
   {
      if (listener == null)
      {
         throw new IllegalStateException("FailureListener cannot be null");
      }

      return failureListeners.remove(listener);
   }

   @Override
   public void addCloseListener(CloseListener listener)
   {
      if (listener == null)
      {
         throw new IllegalStateException("CloseListener cannot be null");
      }

      closeListeners.add(listener);
   }

   @Override
   public boolean removeCloseListener(CloseListener listener)
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
   public void setCloseListeners(List<CloseListener> listeners)
   {
      closeListeners.clear();

      closeListeners.addAll(listeners);
   }

   @Override
   public List<FailureListener> getFailureListeners()
   {
      // we do not return the listeners otherwise the remoting service
      // would NOT destroy the connection.
      return Collections.emptyList();
   }

   @Override
   public List<FailureListener> removeFailureListeners()
   {
      List<FailureListener> ret = new ArrayList<FailureListener>(
            failureListeners);

      failureListeners.clear();

      return ret;
   }

   @Override
   public void setFailureListeners(List<FailureListener> listeners)
   {
      failureListeners.clear();

      failureListeners.addAll(listeners);
   }

   @Override
   public ActiveMQBuffer createTransportBuffer(int size)
   {
      return ActiveMQBuffers.dynamicBuffer(size);
   }

   @Override
   public void fail(ActiveMQException me)
   {
      if (me != null)
      {
         ActiveMQServerLogger.LOGGER.connectionFailureDetected(me.getMessage(), me.getType());
      }

      // Then call the listeners
      callFailureListeners(me);

      callClosingListeners();

      destroyed = true;

      transportConnection.close();
   }

   @Override
   public void destroy()
   {
      destroyed = true;

      transportConnection.close();

      try
      {
         deleteTempQueues();
      }
      catch (Exception e)
      {
         //log warning
      }

      synchronized (sendLock)
      {
         callClosingListeners();
      }
   }

   private void deleteTempQueues() throws Exception
   {
      Iterator<String> queueNames = tempQueues.iterator();
      while (queueNames.hasNext())
      {
         String q = queueNames.next();
         protocolManager.deleteQueue(q);
      }
   }

   @Override
   public Connection getTransportConnection()
   {
      return this.transportConnection;
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
   public void disconnect(boolean criticalError)
   {
      fail(null);
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
   }

   private void callFailureListeners(final ActiveMQException me)
   {
      final List<FailureListener> listenersClone = new ArrayList<FailureListener>(
            failureListeners);

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
            ActiveMQServerLogger.LOGGER.errorCallingFailureListener(t);
         }
      }
   }

   private void callClosingListeners()
   {
      final List<CloseListener> listenersClone = new ArrayList<CloseListener>(
            closeListeners);

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
            ActiveMQServerLogger.LOGGER.errorCallingFailureListener(t);
         }
      }
   }

   // throw a WireFormatInfo to the peer
   public void init()
   {
      WireFormatInfo info = wireFormat.getPreferedWireFormatInfo();
      protocolManager.send(this, info);
   }

   public ConnectionState getState()
   {
      return state;
   }

   public void physicalSend(Command command) throws IOException
   {
      try
      {
         ByteSequence bytes = wireFormat.marshal(command);
         ActiveMQBuffer buffer = OpenWireUtil.toActiveMQBuffer(bytes);
         synchronized (sendLock)
         {
            getTransportConnection().write(buffer, false, false);
         }
      }
      catch (IOException e)
      {
         throw e;
      }
      catch (Throwable t)
      {
         ActiveMQServerLogger.LOGGER.error("error sending", t);
      }

   }

   @Override
   public Response processAddConnection(ConnectionInfo info) throws Exception
   {
      WireFormatInfo wireFormatInfo = wireFormat.getPreferedWireFormatInfo();
      // Older clients should have been defaulting this field to true.. but
      // they were not.
      if (wireFormatInfo != null && wireFormatInfo.getVersion() <= 2)
      {
         info.setClientMaster(true);
      }

      // Make sure 2 concurrent connections by the same ID only generate 1
      // TransportConnectionState object.
      synchronized (brokerConnectionStates)
      {
         state = (AMQTransportConnectionState) brokerConnectionStates.get(info
               .getConnectionId());
         if (state == null)
         {
            state = new AMQTransportConnectionState(info, this);
            brokerConnectionStates.put(info.getConnectionId(), state);
         }
         state.incrementReference();
      }
      // If there are 2 concurrent connections for the same connection id,
      // then last one in wins, we need to sync here
      // to figure out the winner.
      synchronized (state.getConnectionMutex())
      {
         if (state.getConnection() != this)
         {
            state.getConnection().disconnect(true);
            state.setConnection(this);
            state.reset(info);
         }
      }

      registerConnectionState(info.getConnectionId(), state);

      this.faultTolerantConnection = info.isFaultTolerant();
      // Setup the context.
      String clientId = info.getClientId();
      context = new AMQConnectionContext();
      context.setBroker(protocolManager);
      context.setClientId(clientId);
      context.setClientMaster(info.isClientMaster());
      context.setConnection(this);
      context.setConnectionId(info.getConnectionId());
      // for now we pass the manager as the connector and see what happens
      // it should be related to activemq's Acceptor
      context.setConnector(this.acceptorUsed);
      context.setMessageAuthorizationPolicy(getMessageAuthorizationPolicy());
      context.setNetworkConnection(networkConnection);
      context.setFaultTolerant(faultTolerantConnection);
      context
            .setTransactions(new ConcurrentHashMap<TransactionId, AMQTransaction>());
      context.setUserName(info.getUserName());
      context.setWireFormatInfo(wireFormatInfo);
      context.setReconnect(info.isFailoverReconnect());
      this.manageable = info.isManageable();
      context.setConnectionState(state);
      state.setContext(context);
      state.setConnection(this);
      if (info.getClientIp() == null)
      {
         info.setClientIp(getRemoteAddress());
      }

      try
      {
         protocolManager.addConnection(context, info);
      }
      catch (Exception e)
      {
         synchronized (brokerConnectionStates)
         {
            brokerConnectionStates.remove(info.getConnectionId());
         }
         unregisterConnectionState(info.getConnectionId());

         if (e instanceof SecurityException)
         {
            // close this down - in case the peer of this transport doesn't play
            // nice
            delayedStop(2000,
                  "Failed with SecurityException: " + e.getLocalizedMessage(),
                  e);
         }
         Response resp = new ExceptionResponse(e);
         return resp;
      }
      if (info.isManageable())
      {
         // send ConnectionCommand
         ConnectionControl command = this.acceptorUsed.getConnectionControl();
         command.setFaultTolerant(protocolManager
               .isFaultTolerantConfiguration());
         if (info.isFailoverReconnect())
         {
            command.setRebalanceConnection(false);
         }
         dispatchAsync(command);
      }
      return null;
   }

   public void dispatchAsync(Command message)
   {
      if (!stopping.get())
      {
         if (taskRunner == null)
         {
            dispatchSync(message);
         }
         else
         {
            synchronized (dispatchQueue)
            {
               dispatchQueue.add(message);
            }
            try
            {
               taskRunner.wakeup();
            }
            catch (InterruptedException e)
            {
               Thread.currentThread().interrupt();
            }
         }
      }
      else
      {
         if (message.isMessageDispatch())
         {
            MessageDispatch md = (MessageDispatch) message;
            TransmitCallback sub = md.getTransmitCallback();
            protocolManager.postProcessDispatch(md);
            if (sub != null)
            {
               sub.onFailure();
            }
         }
      }
   }

   public void dispatchSync(Command message)
   {
      try
      {
         processDispatch(message);
      }
      catch (IOException e)
      {
         serviceExceptionAsync(e);
      }
   }

   public void serviceExceptionAsync(final IOException e)
   {
      if (asyncException.compareAndSet(false, true))
      {
         new Thread("Async Exception Handler")
         {
            @Override
            public void run()
            {
               serviceException(e);
            }
         }.start();
      }
   }

   public void serviceException(Throwable e)
   {
      // are we a transport exception such as not being able to dispatch
      // synchronously to a transport
      if (e instanceof IOException)
      {
         serviceTransportException((IOException) e);
      }
      else if (e.getClass() == AMQBrokerStoppedException.class)
      {
         // Handle the case where the broker is stopped
         // But the client is still connected.
         if (!stopping.get())
         {
            ConnectionError ce = new ConnectionError();
            ce.setException(e);
            dispatchSync(ce);
            // Record the error that caused the transport to stop
            this.stopError = e;
            // Wait a little bit to try to get the output buffer to flush
            // the exception notification to the client.
            try
            {
               Thread.sleep(500);
            }
            catch (InterruptedException ie)
            {
               Thread.currentThread().interrupt();
            }
            // Worst case is we just kill the connection before the
            // notification gets to him.
            stopAsync();
         }
      }
      else if (!stopping.get() && !inServiceException)
      {
         inServiceException = true;
         try
         {
            ConnectionError ce = new ConnectionError();
            ce.setException(e);
            if (pendingStop)
            {
               dispatchSync(ce);
            }
            else
            {
               dispatchAsync(ce);
            }
         }
         finally
         {
            inServiceException = false;
         }
      }
   }

   public void serviceTransportException(IOException e)
   {
      /*
       * deal with it later BrokerService bService =
       * connector.getBrokerService(); if (bService.isShutdownOnSlaveFailure())
       * { if (brokerInfo != null) { if (brokerInfo.isSlaveBroker()) {
       * LOG.error("Slave has exception: {} shutting down master now.",
       * e.getMessage(), e); try { doStop(); bService.stop(); } catch (Exception
       * ex) { LOG.warn("Failed to stop the master", ex); } } } } if
       * (!stopping.get() && !pendingStop) { transportException.set(e); if
       * (TRANSPORTLOG.isDebugEnabled()) { TRANSPORTLOG.debug(this + " failed: "
       * + e, e); } else if (TRANSPORTLOG.isWarnEnabled() && !expected(e)) {
       * TRANSPORTLOG.warn(this + " failed: " + e); } stopAsync(); }
       */
   }

   public void setMarkedCandidate(boolean markedCandidate)
   {
      this.markedCandidate = markedCandidate;
      if (!markedCandidate)
      {
         timeStamp = 0;
         blockedCandidate = false;
      }
   }

   protected void dispatch(Command command) throws IOException
   {
      try
      {
         setMarkedCandidate(true);
         this.physicalSend(command);
      }
      finally
      {
         setMarkedCandidate(false);
      }
   }

   protected void processDispatch(Command command) throws IOException
   {
      MessageDispatch messageDispatch = (MessageDispatch) (command
            .isMessageDispatch() ? command : null);
      try
      {
         if (!stopping.get())
         {
            if (messageDispatch != null)
            {
               protocolManager.preProcessDispatch(messageDispatch);
            }
            dispatch(command);
         }
      }
      catch (IOException e)
      {
         if (messageDispatch != null)
         {
            TransmitCallback sub = messageDispatch.getTransmitCallback();
            protocolManager.postProcessDispatch(messageDispatch);
            if (sub != null)
            {
               sub.onFailure();
            }
            messageDispatch = null;
            throw e;
         }
      }
      finally
      {
         if (messageDispatch != null)
         {
            TransmitCallback sub = messageDispatch.getTransmitCallback();
            protocolManager.postProcessDispatch(messageDispatch);
            if (sub != null)
            {
               sub.onSuccess();
            }
         }
      }
   }

   private AMQMessageAuthorizationPolicy getMessageAuthorizationPolicy()
   {
      return this.messageAuthorizationPolicy;
   }

   protected synchronized AMQTransportConnectionState unregisterConnectionState(
         ConnectionId connectionId)
   {
      return connectionStateRegister.unregisterConnectionState(connectionId);
   }

   protected synchronized AMQTransportConnectionState registerConnectionState(
         ConnectionId connectionId, AMQTransportConnectionState state)
   {
      AMQTransportConnectionState cs = null;
      if (!connectionStateRegister.isEmpty()
            && !connectionStateRegister.doesHandleMultipleConnectionStates())
      {
         // swap implementations
         AMQTransportConnectionStateRegister newRegister = new AMQMapTransportConnectionStateRegister();
         newRegister.intialize(connectionStateRegister);
         connectionStateRegister = newRegister;
      }
      cs = connectionStateRegister.registerConnectionState(connectionId, state);
      return cs;
   }

   public void delayedStop(final int waitTime, final String reason,
         Throwable cause)
   {
      if (waitTime > 0)
      {
         synchronized (this)
         {
            pendingStop = true;
            stopError = cause;
         }
         try
         {
            stopTaskRunnerFactory.execute(new Runnable()
            {
               @Override
               public void run()
               {
                  try
                  {
                     Thread.sleep(waitTime);
                     stopAsync();
                  }
                  catch (InterruptedException e)
                  {
                  }
               }
            });
         }
         catch (Throwable t)
         {
            // log error
         }
      }
   }

   public void stopAsync()
   {
      // If we're in the middle of starting then go no further... for now.
      synchronized (this)
      {
         pendingStop = true;
         if (starting)
         {
            // log
            return;
         }
      }
      if (stopping.compareAndSet(false, true))
      {
         // Let all the connection contexts know we are shutting down
         // so that in progress operations can notice and unblock.
         List<AMQTransportConnectionState> connectionStates = listConnectionStates();
         for (AMQTransportConnectionState cs : connectionStates)
         {
            AMQConnectionContext connectionContext = cs.getContext();
            if (connectionContext != null)
            {
               connectionContext.getStopping().set(true);
            }
         }
         try
         {
            stopTaskRunnerFactory.execute(new Runnable()
            {
               @Override
               public void run()
               {
                  serviceLock.writeLock().lock();
                  try
                  {
                     doStop();
                  }
                  catch (Throwable e)
                  {
                     // LOG
                  }
                  finally
                  {
                     stopped.countDown();
                     serviceLock.writeLock().unlock();
                  }
               }
            });
         }
         catch (Throwable t)
         {
            // LOG
            stopped.countDown();
         }
      }
   }

   protected synchronized List<AMQTransportConnectionState> listConnectionStates()
   {
      return connectionStateRegister.listConnectionStates();
   }

   protected void doStop() throws Exception
   {
      this.acceptorUsed.onStopped(this);
      /*
       * What's a duplex bridge? try { synchronized (this) { if (duplexBridge !=
       * null) { duplexBridge.stop(); } } } catch (Exception ignore) {
       * LOG.trace("Exception caught stopping. This exception is ignored.",
       * ignore); }
       */
      try
      {
         getTransportConnection().close();
      }
      catch (Exception e)
      {
         // log
      }

      if (taskRunner != null)
      {
         taskRunner.shutdown(1);
         taskRunner = null;
      }

      active = false;
      // Run the MessageDispatch callbacks so that message references get
      // cleaned up.
      synchronized (dispatchQueue)
      {
         for (Iterator<Command> iter = dispatchQueue.iterator(); iter.hasNext();)
         {
            Command command = iter.next();
            if (command.isMessageDispatch())
            {
               MessageDispatch md = (MessageDispatch) command;
               TransmitCallback sub = md.getTransmitCallback();
               protocolManager.postProcessDispatch(md);
               if (sub != null)
               {
                  sub.onFailure();
               }
            }
         }
         dispatchQueue.clear();
      }
      //
      // Remove all logical connection associated with this connection
      // from the broker.
      if (!protocolManager.isStopped())
      {
         List<AMQTransportConnectionState> connectionStates = listConnectionStates();
         connectionStates = listConnectionStates();
         for (AMQTransportConnectionState cs : connectionStates)
         {
            cs.getContext().getStopping().set(true);
            try
            {
               processRemoveConnection(cs.getInfo().getConnectionId(), 0L);
            }
            catch (Throwable ignore)
            {
               ignore.printStackTrace();
            }
         }
      }
   }

   @Override
   public Response processAddConsumer(ConsumerInfo info)
   {
      Response resp = null;
      try
      {
         protocolManager.addConsumer(this, info);
      }
      catch (Exception e)
      {
         if (e instanceof ActiveMQSecurityException)
         {
            resp = new ExceptionResponse(new JMSSecurityException(e.getMessage()));
         }
         else
         {
            resp = new ExceptionResponse(e);
         }
      }
      return resp;
   }

   AMQConsumerBrokerExchange addConsumerBrokerExchange(ConsumerId id)
   {
      AMQConsumerBrokerExchange result = consumerExchanges.get(id);
      if (result == null)
      {
         synchronized (consumerExchanges)
         {
            result = new AMQConsumerBrokerExchange();
            AMQTransportConnectionState state = lookupConnectionState(id);
            context = state.getContext();
            result.setConnectionContext(context);
            SessionState ss = state.getSessionState(id.getParentId());
            if (ss != null)
            {
               ConsumerState cs = ss.getConsumerState(id);
               if (cs != null)
               {
                  ConsumerInfo info = cs.getInfo();
                  if (info != null)
                  {
                     if (info.getDestination() != null
                           && info.getDestination().isPattern())
                     {
                        result.setWildcard(true);
                     }
                  }
               }
            }
            consumerExchanges.put(id, result);
         }
      }
      return result;
   }

   protected synchronized AMQTransportConnectionState lookupConnectionState(
         ConsumerId id)
   {
      return connectionStateRegister.lookupConnectionState(id);
   }

   protected synchronized AMQTransportConnectionState lookupConnectionState(
         ProducerId id)
   {
      return connectionStateRegister.lookupConnectionState(id);
   }

   public int getConsumerCount(ConnectionId connectionId)
   {
      int result = 0;
      AMQTransportConnectionState cs = lookupConnectionState(connectionId);
      if (cs != null)
      {
         for (SessionId sessionId : cs.getSessionIds())
         {
            SessionState sessionState = cs.getSessionState(sessionId);
            if (sessionState != null)
            {
               result += sessionState.getConsumerIds().size();
            }
         }
      }
      return result;
   }

   public int getProducerCount(ConnectionId connectionId)
   {
      int result = 0;
      AMQTransportConnectionState cs = lookupConnectionState(connectionId);
      if (cs != null)
      {
         for (SessionId sessionId : cs.getSessionIds())
         {
            SessionState sessionState = cs.getSessionState(sessionId);
            if (sessionState != null)
            {
               result += sessionState.getProducerIds().size();
            }
         }
      }
      return result;
   }

   public synchronized AMQTransportConnectionState lookupConnectionState(
         ConnectionId connectionId)
   {
      return connectionStateRegister.lookupConnectionState(connectionId);
   }

   @Override
   public Response processAddDestination(DestinationInfo dest) throws Exception
   {
      Response resp = null;
      try
      {
         protocolManager.addDestination(this, dest);
      }
      catch (Exception e)
      {
         if (e instanceof ActiveMQSecurityException)
         {
            resp = new ExceptionResponse(new JMSSecurityException(e.getMessage()));
         }
         else
         {
            resp = new ExceptionResponse(e);
         }
      }
      return resp;
   }

   @Override
   public Response processAddProducer(ProducerInfo info) throws Exception
   {
      Response resp = null;
      try
      {
         protocolManager.addProducer(this, info);
      }
      catch (Exception e)
      {
         if (e instanceof ActiveMQSecurityException)
         {
            resp = new ExceptionResponse(new JMSSecurityException(e.getMessage()));
         }
         else if (e instanceof ActiveMQNonExistentQueueException)
         {
            resp = new ExceptionResponse(new InvalidDestinationException(e.getMessage()));
         }
         else
         {
            resp = new ExceptionResponse(e);
         }
      }
      return resp;
   }

   @Override
   public Response processAddSession(SessionInfo info) throws Exception
   {
      ConnectionId connectionId = info.getSessionId().getParentId();
      AMQTransportConnectionState cs = lookupConnectionState(connectionId);
      // Avoid replaying dup commands
      if (cs != null && !cs.getSessionIds().contains(info.getSessionId()))
      {
         protocolManager.addSession(this, info);
         try
         {
            cs.addSession(info);
         }
         catch (IllegalStateException e)
         {
            e.printStackTrace();
            protocolManager.removeSession(cs.getContext(), info);
         }
      }
      return null;
   }

   @Override
   public Response processBeginTransaction(TransactionInfo info) throws Exception
   {
      TransactionId txId = info.getTransactionId();

      if (!txMap.containsKey(txId))
      {
         txMap.put(txId, info);
      }
      return null;
   }

   @Override
   public Response processBrokerInfo(BrokerInfo arg0) throws Exception
   {
      throw new IllegalStateException("not implemented! ");
   }

   @Override
   public Response processCommitTransactionOnePhase(TransactionInfo info) throws Exception
   {
      protocolManager.commitTransactionOnePhase(info);
      TransactionId txId = info.getTransactionId();
      txMap.remove(txId);

      return null;
   }

   @Override
   public Response processCommitTransactionTwoPhase(TransactionInfo info) throws Exception
   {
      protocolManager.commitTransactionTwoPhase(info);
      TransactionId txId = info.getTransactionId();
      txMap.remove(txId);

      return null;
   }

   @Override
   public Response processConnectionControl(ConnectionControl arg0) throws Exception
   {
      throw new IllegalStateException("not implemented! ");
   }

   @Override
   public Response processConnectionError(ConnectionError arg0) throws Exception
   {
      throw new IllegalStateException("not implemented! ");
   }

   @Override
   public Response processConsumerControl(ConsumerControl arg0) throws Exception
   {
      throw new IllegalStateException("not implemented! ");
   }

   @Override
   public Response processControlCommand(ControlCommand arg0) throws Exception
   {
      throw new IllegalStateException("not implemented! ");
   }

   @Override
   public Response processEndTransaction(TransactionInfo info) throws Exception
   {
      protocolManager.endTransaction(info);
      TransactionId txId = info.getTransactionId();

      if (!txMap.containsKey(txId))
      {
         txMap.put(txId, info);
      }
      return null;
   }

   @Override
   public Response processFlush(FlushCommand arg0) throws Exception
   {
      throw new IllegalStateException("not implemented! ");
   }

   @Override
   public Response processForgetTransaction(TransactionInfo info) throws Exception
   {
      TransactionId txId = info.getTransactionId();
      txMap.remove(txId);

      protocolManager.forgetTransaction(info.getTransactionId());
      return null;
   }

   @Override
   public Response processKeepAlive(KeepAliveInfo arg0) throws Exception
   {
      throw new IllegalStateException("not implemented! ");
   }

   @Override
   public Response processMessage(Message messageSend)
   {
      Response resp = null;
      try
      {
         ProducerId producerId = messageSend.getProducerId();
         AMQProducerBrokerExchange producerExchange = getProducerBrokerExchange(producerId);
         final AMQConnectionContext pcontext = producerExchange.getConnectionContext();
         final ProducerInfo producerInfo = producerExchange.getProducerState().getInfo();
         boolean sendProducerAck = !messageSend.isResponseRequired() && producerInfo.getWindowSize() > 0
               && !pcontext.isInRecoveryMode();

         AMQSession session = protocolManager.getSession(producerId.getParentId());

         if (producerExchange.canDispatch(messageSend))
         {
            SendingResult result = session.send(producerExchange, messageSend, sendProducerAck);
            if (result.isBlockNextSend())
            {
               if (!context.isNetworkConnection() && result.isSendFailIfNoSpace())
               {
                  throw new ResourceAllocationException("Usage Manager Memory Limit reached. Stopping producer ("
                     + producerId + ") to prevent flooding "
                     + result.getBlockingAddress() + "."
                     + " See http://activemq.apache.org/producer-flow-control.html for more info");
               }

               if (producerInfo.getWindowSize() > 0 || messageSend.isResponseRequired())
               {
                  //in that case don't send the response
                  //this will force the client to wait until
                  //the response is got.
                  if (context == null)
                  {
                     this.context = new AMQConnectionContext();
                  }
                  context.setDontSendReponse(true);
               }
               else
               {
                  //hang the connection until the space is available
                  session.blockingWaitForSpace(producerExchange, result);
               }
            }
            else if (sendProducerAck)
            {
               ProducerAck ack = new ProducerAck(producerInfo.getProducerId(), messageSend.getSize());
               this.dispatchAsync(ack);
            }
         }
      }
      catch (Exception e)
      {
         if (e instanceof ActiveMQSecurityException)
         {
            resp = new ExceptionResponse(new JMSSecurityException(e.getMessage()));
         }
         else
         {
            resp = new ExceptionResponse(e);
         }
      }
      return resp;
   }

   private AMQProducerBrokerExchange getProducerBrokerExchange(ProducerId id) throws IOException
   {
      AMQProducerBrokerExchange result = producerExchanges.get(id);
      if (result == null)
      {
         synchronized (producerExchanges)
         {
            result = new AMQProducerBrokerExchange();
            AMQTransportConnectionState state = lookupConnectionState(id);
            context = state.getContext();
            result.setConnectionContext(context);
            if (context.isReconnect()
                  || (context.isNetworkConnection() && this.acceptorUsed
                        .isAuditNetworkProducers()))
            {
               if (protocolManager.getPersistenceAdapter() != null)
               {
                  result.setLastStoredSequenceId(protocolManager.getPersistenceAdapter().getLastProducerSequenceId(id));
               }
            }
            SessionState ss = state.getSessionState(id.getParentId());
            if (ss != null)
            {
               result.setProducerState(ss.getProducerState(id));
               ProducerState producerState = ss.getProducerState(id);
               if (producerState != null && producerState.getInfo() != null)
               {
                  ProducerInfo info = producerState.getInfo();
                  result.setMutable(info.getDestination() == null
                        || info.getDestination().isComposite());
               }
            }
            producerExchanges.put(id, result);
         }
      }
      else
      {
         context = result.getConnectionContext();
      }
      return result;
   }

   @Override
   public Response processMessageAck(MessageAck ack) throws Exception
   {
      ConsumerId consumerId = ack.getConsumerId();
      SessionId sessionId = consumerId.getParentId();
      AMQSession session = protocolManager.getSession(sessionId);
      session.acknowledge(ack);
      return null;
   }

   @Override
   public Response processMessageDispatch(MessageDispatch arg0) throws Exception
   {
      throw new IllegalStateException("not implemented! ");
   }

   @Override
   public Response processMessageDispatchNotification(
         MessageDispatchNotification arg0) throws Exception
   {
      throw new IllegalStateException("not implemented! ");
   }

   @Override
   public Response processMessagePull(MessagePull arg0) throws Exception
   {
      throw new IllegalStateException("not implemented! ");
   }

   @Override
   public Response processPrepareTransaction(TransactionInfo info) throws Exception
   {
      protocolManager.prepareTransaction(info);
      return null;
   }

   @Override
   public Response processProducerAck(ProducerAck arg0) throws Exception
   {
      throw new IllegalStateException("not implemented! ");
   }

   @Override
   public Response processRecoverTransactions(TransactionInfo info) throws Exception
   {
      AMQTransportConnectionState cs = lookupConnectionState(info.getConnectionId());
      Set<SessionId> sIds = cs.getSessionIds();
      TransactionId[] recovered = protocolManager.recoverTransactions(sIds);
      return new DataArrayResponse(recovered);
   }

   @Override
   public Response processRemoveConnection(ConnectionId id,
         long lastDeliveredSequenceId) throws Exception
   {
      AMQTransportConnectionState cs = lookupConnectionState(id);
      if (cs != null)
      {
         // Don't allow things to be added to the connection state while we
         // are shutting down.
         cs.shutdown();
         // Cascade the connection stop to the sessions.
         for (SessionId sessionId : cs.getSessionIds())
         {
            try
            {
               processRemoveSession(sessionId, lastDeliveredSequenceId);
            }
            catch (Throwable e)
            {
               // LOG
            }
         }

         try
         {
            protocolManager.removeConnection(cs.getContext(), cs.getInfo(),
                  null);
         }
         catch (Throwable e)
         {
            // log
         }
         AMQTransportConnectionState state = unregisterConnectionState(id);
         if (state != null)
         {
            synchronized (brokerConnectionStates)
            {
               // If we are the last reference, we should remove the state
               // from the broker.
               if (state.decrementReference() == 0)
               {
                  brokerConnectionStates.remove(id);
               }
            }
         }
      }
      return null;
   }

   @Override
   public Response processRemoveConsumer(ConsumerId id,
         long lastDeliveredSequenceId) throws Exception
   {
      SessionId sessionId = id.getParentId();
      ConnectionId connectionId = sessionId.getParentId();
      AMQTransportConnectionState cs = lookupConnectionState(connectionId);
      if (cs == null)
      {
         throw new IllegalStateException(
               "Cannot remove a consumer from a connection that had not been registered: "
                     + connectionId);
      }
      SessionState ss = cs.getSessionState(sessionId);
      if (ss == null)
      {
         throw new IllegalStateException(
               "Cannot remove a consumer from a session that had not been registered: "
                     + sessionId);
      }
      ConsumerState consumerState = ss.removeConsumer(id);
      if (consumerState == null)
      {
         throw new IllegalStateException(
               "Cannot remove a consumer that had not been registered: " + id);
      }
      ConsumerInfo info = consumerState.getInfo();
      info.setLastDeliveredSequenceId(lastDeliveredSequenceId);
      protocolManager.removeConsumer(cs.getContext(), consumerState.getInfo());
      removeConsumerBrokerExchange(id);
      return null;
   }

   private void removeConsumerBrokerExchange(ConsumerId id)
   {
      synchronized (consumerExchanges)
      {
         consumerExchanges.remove(id);
      }
   }

   @Override
   public Response processRemoveDestination(DestinationInfo info) throws Exception
   {
      ActiveMQDestination dest = info.getDestination();
      if (dest.isQueue())
      {
         String qName = "jms.queue." + dest.getPhysicalName();
         protocolManager.deleteQueue(qName);
      }
      return null;
   }

   @Override
   public Response processRemoveProducer(ProducerId id) throws Exception
   {
      protocolManager.removeProducer(id);
      return null;
   }

   @Override
   public Response processRemoveSession(SessionId id,
         long lastDeliveredSequenceId) throws Exception
   {
      ConnectionId connectionId = id.getParentId();
      AMQTransportConnectionState cs = lookupConnectionState(connectionId);
      if (cs == null)
      {
         throw new IllegalStateException(
               "Cannot remove session from connection that had not been registered: "
                     + connectionId);
      }
      SessionState session = cs.getSessionState(id);
      if (session == null)
      {
         throw new IllegalStateException(
               "Cannot remove session that had not been registered: " + id);
      }
      // Don't let new consumers or producers get added while we are closing
      // this down.
      session.shutdown();
      // Cascade the connection stop to the consumers and producers.
      for (ConsumerId consumerId : session.getConsumerIds())
      {
         try
         {
            processRemoveConsumer(consumerId, lastDeliveredSequenceId);
         }
         catch (Throwable e)
         {
            // LOG.warn("Failed to remove consumer: {}", consumerId, e);
         }
      }
      for (ProducerId producerId : session.getProducerIds())
      {
         try
         {
            processRemoveProducer(producerId);
         }
         catch (Throwable e)
         {
            // LOG.warn("Failed to remove producer: {}", producerId, e);
         }
      }
      cs.removeSession(id);
      protocolManager.removeSession(cs.getContext(), session.getInfo());
      return null;
   }

   @Override
   public Response processRemoveSubscription(RemoveSubscriptionInfo arg0) throws Exception
   {
      throw new IllegalStateException("not implemented! ");
   }

   @Override
   public Response processRollbackTransaction(TransactionInfo info) throws Exception
   {
      protocolManager.rollbackTransaction(info);
      TransactionId txId = info.getTransactionId();
      txMap.remove(txId);
      return null;
   }

   @Override
   public Response processShutdown(ShutdownInfo info) throws Exception
   {
      return null;
   }

   @Override
   public Response processWireFormat(WireFormatInfo arg0) throws Exception
   {
      throw new IllegalStateException("not implemented! ");
   }

   public int getMaximumConsumersAllowedPerConnection()
   {
      return this.acceptorUsed.getMaximumConsumersAllowedPerConnection();
   }

   public int getMaximumProducersAllowedPerConnection()
   {
      return this.acceptorUsed.getMaximumProducersAllowedPerConnection();
   }

   public void deliverMessage(MessageDispatch dispatch)
   {
      Message m = dispatch.getMessage();
      if (m != null)
      {
         long endTime = System.currentTimeMillis();
         m.setBrokerOutTime(endTime);
      }

      protocolManager.send(this, dispatch);
   }

   public WireFormat getMarshaller()
   {
      return this.wireFormat;
   }

   public void registerTempQueue(SimpleString qName)
   {
      tempQueues.add(qName.toString());
   }

   @Override
   public void disconnect(String reason, boolean fail)
   {
      destroy();
   }

   @Override
   public void fail(ActiveMQException e, String message)
   {
      destroy();
   }

   public void setAdvisorySession(AMQSession amqSession)
   {
      this.advisorySession = amqSession;
   }

   public AMQSession getAdvisorySession()
   {
      return this.advisorySession;
   }

   public AMQConnectionContext getConext()
   {
      return this.state.getContext();
   }

}
