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

import javax.jms.InvalidClientIDException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

import io.netty.channel.ChannelPipeline;
import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.BaseInterceptor;
import org.apache.activemq.artemis.api.core.Interceptor;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.management.CoreNotificationType;
import org.apache.activemq.artemis.api.core.management.ManagementHelper;
import org.apache.activemq.artemis.core.protocol.openwire.amq.AMQConsumer;
import org.apache.activemq.artemis.core.server.management.ManagementService;
import org.apache.activemq.artemis.core.server.management.Notification;
import org.apache.activemq.artemis.core.server.management.NotificationListener;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.BrokerId;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.CommandTypes;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.DestinationInfo;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.TransactionInfo;
import org.apache.activemq.command.WireFormatInfo;
import org.apache.activemq.command.XATransactionId;
import org.apache.activemq.artemis.core.journal.IOAsyncTask;
import org.apache.activemq.artemis.core.protocol.openwire.amq.AMQConnectionContext;
import org.apache.activemq.artemis.core.protocol.openwire.amq.AMQPersistenceAdapter;
import org.apache.activemq.artemis.core.protocol.openwire.amq.AMQProducerBrokerExchange;
import org.apache.activemq.artemis.core.protocol.openwire.amq.AMQServerSession;
import org.apache.activemq.artemis.core.protocol.openwire.amq.AMQSession;
import org.apache.activemq.artemis.core.protocol.openwire.amq.AMQTransportConnectionState;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyServerConnection;
import org.apache.activemq.artemis.core.security.CheckType;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.openwire.OpenWireFormatFactory;
import org.apache.activemq.artemis.spi.core.protocol.ConnectionEntry;
import org.apache.activemq.artemis.spi.core.protocol.MessageConverter;
import org.apache.activemq.artemis.spi.core.protocol.ProtocolManager;
import org.apache.activemq.artemis.spi.core.protocol.ProtocolManagerFactory;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.remoting.Acceptor;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager;
import org.apache.activemq.state.ConnectionState;
import org.apache.activemq.state.ProducerState;
import org.apache.activemq.state.SessionState;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.InetAddressUtil;
import org.apache.activemq.util.LongSequenceGenerator;

public class OpenWireProtocolManager implements ProtocolManager<Interceptor>, NotificationListener
{
   private static final IdGenerator BROKER_ID_GENERATOR = new IdGenerator();
   private static final IdGenerator ID_GENERATOR = new IdGenerator();

   private final LongSequenceGenerator messageIdGenerator = new LongSequenceGenerator();
   private final ActiveMQServer server;

   private final OpenWireProtocolManagerFactory factory;

   private OpenWireFormatFactory wireFactory;

   private boolean tightEncodingEnabled = true;

   private boolean prefixPacketSize = true;

   private BrokerState brokerState;

   private BrokerId brokerId;
   protected final ProducerId advisoryProducerId = new ProducerId();

   // from broker
   protected final Map<ConnectionId, ConnectionState> brokerConnectionStates = Collections
      .synchronizedMap(new HashMap<ConnectionId, ConnectionState>());

   private final CopyOnWriteArrayList<OpenWireConnection> connections = new CopyOnWriteArrayList<OpenWireConnection>();

   protected final ConcurrentMap<ConnectionId, ConnectionInfo> connectionInfos = new ConcurrentHashMap<ConnectionId, ConnectionInfo>();

   private final Map<String, AMQConnectionContext> clientIdSet = new HashMap<String, AMQConnectionContext>();

   private String brokerName;

   private Map<SessionId, AMQSession> sessions = new ConcurrentHashMap<SessionId, AMQSession>();

   private Map<TransactionId, AMQSession> transactions = new ConcurrentHashMap<TransactionId, AMQSession>();

   private Map<String, SessionId> sessionIdMap = new ConcurrentHashMap<String, SessionId>();

   public OpenWireProtocolManager(OpenWireProtocolManagerFactory factory, ActiveMQServer server)
   {
      this.factory = factory;
      this.server = server;
      this.wireFactory = new OpenWireFormatFactory();
      // preferred prop, should be done via config
      wireFactory.setCacheEnabled(false);
      brokerState = new BrokerState();
      advisoryProducerId.setConnectionId(ID_GENERATOR.generateId());
      ManagementService service = server.getManagementService();
      if (service != null)
      {
         service.addNotificationListener(this);
      }
   }


   public ProtocolManagerFactory<Interceptor> getFactory()
   {
      return factory;
   }


   @Override
   public void updateInterceptors(List<BaseInterceptor> incomingInterceptors, List<BaseInterceptor> outgoingInterceptors)
   {
      // NO-OP
   }

   @Override
   public ConnectionEntry createConnectionEntry(Acceptor acceptorUsed,
                                                Connection connection)
   {
      OpenWireFormat wf = (OpenWireFormat) wireFactory.createWireFormat();
      OpenWireConnection owConn = new OpenWireConnection(acceptorUsed,
                                                         connection, this, wf);
      owConn.init();

      return new ConnectionEntry(owConn, null, System.currentTimeMillis(),
                                 1 * 60 * 1000);
   }

   @Override
   public MessageConverter getConverter()
   {
      return new OpenWireMessageConverter();
   }

   @Override
   public void removeHandler(String name)
   {
      // TODO Auto-generated method stub
   }

   @Override
   public void handleBuffer(RemotingConnection connection, ActiveMQBuffer buffer)
   {
   }

   @Override
   public void addChannelHandlers(ChannelPipeline pipeline)
   {
      // TODO Auto-generated method stub

   }

   @Override
   public boolean isProtocol(byte[] array)
   {
      if (array.length < 8)
      {
         throw new IllegalArgumentException("Protocol header length changed "
                                               + array.length);
      }

      int start = this.prefixPacketSize ? 4 : 0;
      int j = 0;
      // type
      if (array[start] != WireFormatInfo.DATA_STRUCTURE_TYPE)
      {
         return false;
      }
      start++;
      WireFormatInfo info = new WireFormatInfo();
      final byte[] magic = info.getMagic();
      int remainingLen = array.length - start;
      int useLen = remainingLen > magic.length ? magic.length : remainingLen;
      useLen += start;
      // magic
      for (int i = start; i < useLen; i++)
      {
         if (array[i] != magic[j])
         {
            return false;
         }
         j++;
      }
      return true;
   }

   @Override
   public void handshake(NettyServerConnection connection, ActiveMQBuffer buffer)
   {
      // TODO Auto-generated method stub

   }

   public void handleCommand(OpenWireConnection openWireConnection,
                             Object command)
   {
      Command amqCmd = (Command) command;
      byte type = amqCmd.getDataStructureType();
      switch (type)
      {
         case CommandTypes.CONNECTION_INFO:
            break;
         case CommandTypes.CONNECTION_CONTROL:
            /** The ConnectionControl packet sent from client informs the broker that is capable of supporting dynamic
             * failover and load balancing.  These features are not yet implemented for Artemis OpenWire.  Instead we
             * simply drop the packet.  See: ACTIVEMQ6-108 */
            break;
         case CommandTypes.CONSUMER_CONTROL:
            break;
         default:
            throw new IllegalStateException("Cannot handle command: " + command);
      }
   }

   public void sendReply(final OpenWireConnection connection,
                         final Command command)
   {
      server.getStorageManager().afterCompleteOperations(new IOAsyncTask()
      {
         public void onError(final int errorCode, final String errorMessage)
         {
            ActiveMQServerLogger.LOGGER.errorProcessingIOCallback(errorCode,
                                                                  errorMessage);
         }

         public void done()
         {
            send(connection, command);
         }
      });
   }

   public boolean send(final OpenWireConnection connection, final Command command)
   {
      if (ActiveMQServerLogger.LOGGER.isTraceEnabled())
      {
         ActiveMQServerLogger.LOGGER.trace("sending " + command);
      }
      synchronized (connection)
      {
         if (connection.isDestroyed())
         {
            return false;
         }

         try
         {
            connection.physicalSend(command);
         }
         catch (Exception e)
         {
            return false;
         }
         catch (Throwable t)
         {
            return false;
         }
         return true;
      }
   }

   public Map<ConnectionId, ConnectionState> getConnectionStates()
   {
      return this.brokerConnectionStates;
   }

   public void addConnection(AMQConnectionContext context, ConnectionInfo info) throws Exception
   {
      String username = info.getUserName();
      String password = info.getPassword();

      if (!this.validateUser(username, password))
      {
         throw new SecurityException("User name [" + username + "] or password is invalid.");
      }
      String clientId = info.getClientId();
      if (clientId == null)
      {
         throw new InvalidClientIDException(
            "No clientID specified for connection request");
      }
      synchronized (clientIdSet)
      {
         AMQConnectionContext oldContext = clientIdSet.get(clientId);
         if (oldContext != null)
         {
            if (context.isAllowLinkStealing())
            {
               clientIdSet.remove(clientId);
               if (oldContext.getConnection() != null)
               {
                  OpenWireConnection connection = oldContext.getConnection();
                  connection.disconnect(true);
               }
               else
               {
                  // log error
               }
            }
            else
            {
               throw new InvalidClientIDException("Broker: " + getBrokerName()
                                                     + " - Client: " + clientId + " already connected from "
                                                     + oldContext.getConnection().getRemoteAddress());
            }
         }
         else
         {
            clientIdSet.put(clientId, context);
         }
      }

      connections.add(context.getConnection());

      ActiveMQTopic topic = AdvisorySupport.getConnectionAdvisoryTopic();
      // do not distribute passwords in advisory messages. usernames okay
      ConnectionInfo copy = info.copy();
      copy.setPassword("");
      fireAdvisory(context, topic, copy);
      connectionInfos.put(copy.getConnectionId(), copy);

      // init the conn
      addSessions(context.getConnection(), context.getConnectionState()
         .getSessionIds());
   }

   private void fireAdvisory(AMQConnectionContext context, ActiveMQTopic topic,
                             Command copy) throws Exception
   {
      this.fireAdvisory(context, topic, copy, null);
   }

   public BrokerId getBrokerId()
   {
      if (brokerId == null)
      {
         brokerId = new BrokerId(BROKER_ID_GENERATOR.generateId());
      }
      return brokerId;
   }

   /*
    * See AdvisoryBroker.fireAdvisory()
    */
   private void fireAdvisory(AMQConnectionContext context, ActiveMQTopic topic,
                             Command command, ConsumerId targetConsumerId) throws Exception
   {
      ActiveMQMessage advisoryMessage = new ActiveMQMessage();
      advisoryMessage.setStringProperty(
         AdvisorySupport.MSG_PROPERTY_ORIGIN_BROKER_NAME, getBrokerName());
      String id = getBrokerId() != null ? getBrokerId().getValue() : "NOT_SET";
      advisoryMessage.setStringProperty(
         AdvisorySupport.MSG_PROPERTY_ORIGIN_BROKER_ID, id);

      String url = "tcp://localhost:61616";

      advisoryMessage.setStringProperty(
         AdvisorySupport.MSG_PROPERTY_ORIGIN_BROKER_URL, url);

      // set the data structure
      advisoryMessage.setDataStructure(command);
      advisoryMessage.setPersistent(false);
      advisoryMessage.setType(AdvisorySupport.ADIVSORY_MESSAGE_TYPE);
      advisoryMessage.setMessageId(new MessageId(advisoryProducerId,
                                                 messageIdGenerator.getNextSequenceId()));
      advisoryMessage.setTargetConsumerId(targetConsumerId);
      advisoryMessage.setDestination(topic);
      advisoryMessage.setResponseRequired(false);
      advisoryMessage.setProducerId(advisoryProducerId);
      boolean originalFlowControl = context.isProducerFlowControl();
      final AMQProducerBrokerExchange producerExchange = new AMQProducerBrokerExchange();
      producerExchange.setConnectionContext(context);
      producerExchange.setMutable(true);
      producerExchange.setProducerState(new ProducerState(new ProducerInfo()));
      try
      {
         context.setProducerFlowControl(false);
         AMQSession sess = context.getConnection().getAdvisorySession();
         if (sess != null)
         {
            sess.send(producerExchange, advisoryMessage, false);
         }
      }
      finally
      {
         context.setProducerFlowControl(originalFlowControl);
      }
   }

   public String getBrokerName()
   {
      if (brokerName == null)
      {
         try
         {
            brokerName = InetAddressUtil.getLocalHostName().toLowerCase(
               Locale.ENGLISH);
         }
         catch (Exception e)
         {
            brokerName = "localhost";
         }
      }
      return brokerName;
   }

   public boolean isFaultTolerantConfiguration()
   {
      return false;
   }

   public void postProcessDispatch(MessageDispatch md)
   {
      // TODO Auto-generated method stub

   }

   public boolean isStopped()
   {
      // TODO Auto-generated method stub
      return false;
   }

   public void preProcessDispatch(MessageDispatch messageDispatch)
   {
      // TODO Auto-generated method stub

   }

   public boolean isStopping()
   {
      return false;
   }

   public void addProducer(OpenWireConnection theConn, ProducerInfo info) throws Exception
   {
      SessionId sessionId = info.getProducerId().getParentId();
      ConnectionId connectionId = sessionId.getParentId();
      AMQTransportConnectionState cs = theConn
         .lookupConnectionState(connectionId);
      if (cs == null)
      {
         throw new IllegalStateException(
            "Cannot add a producer to a connection that had not been registered: "
               + connectionId);
      }
      SessionState ss = cs.getSessionState(sessionId);
      if (ss == null)
      {
         throw new IllegalStateException(
            "Cannot add a producer to a session that had not been registered: "
               + sessionId);
      }
      // Avoid replaying dup commands
      if (!ss.getProducerIds().contains(info.getProducerId()))
      {
         ActiveMQDestination destination = info.getDestination();
         if (destination != null
            && !AdvisorySupport.isAdvisoryTopic(destination))
         {
            if (theConn.getProducerCount(connectionId) >= theConn
               .getMaximumProducersAllowedPerConnection())
            {
               throw new IllegalStateException(
                  "Can't add producer on connection " + connectionId
                     + ": at maximum limit: "
                     + theConn.getMaximumProducersAllowedPerConnection());
            }
         }

         AMQSession amqSession = sessions.get(sessionId);
         if (amqSession == null)
         {
            throw new IllegalStateException("Session not exist! : " + sessionId);
         }

         amqSession.createProducer(info);

         try
         {
            ss.addProducer(info);
         }
         catch (IllegalStateException e)
         {
            amqSession.removeProducer(info);
         }

      }

   }

   public void addConsumer(OpenWireConnection theConn, ConsumerInfo info) throws Exception
   {
      // Todo: add a destination interceptors holder here (amq supports this)
      SessionId sessionId = info.getConsumerId().getParentId();
      ConnectionId connectionId = sessionId.getParentId();
      AMQTransportConnectionState cs = theConn
         .lookupConnectionState(connectionId);
      if (cs == null)
      {
         throw new IllegalStateException(
            "Cannot add a consumer to a connection that had not been registered: "
               + connectionId);
      }
      SessionState ss = cs.getSessionState(sessionId);
      if (ss == null)
      {
         throw new IllegalStateException(
            this.server
               + " Cannot add a consumer to a session that had not been registered: "
               + sessionId);
      }
      // Avoid replaying dup commands
      if (!ss.getConsumerIds().contains(info.getConsumerId()))
      {
         ActiveMQDestination destination = info.getDestination();
         if (destination != null
            && !AdvisorySupport.isAdvisoryTopic(destination))
         {
            if (theConn.getConsumerCount(connectionId) >= theConn
               .getMaximumConsumersAllowedPerConnection())
            {
               throw new IllegalStateException(
                  "Can't add consumer on connection " + connectionId
                     + ": at maximum limit: "
                     + theConn.getMaximumConsumersAllowedPerConnection());
            }
         }

         AMQSession amqSession = sessions.get(sessionId);
         if (amqSession == null)
         {
            throw new IllegalStateException("Session not exist! : " + sessionId);
         }

         amqSession.createConsumer(info);

         try
         {
            ss.addConsumer(info);
            theConn.addConsumerBrokerExchange(info.getConsumerId());
         }
         catch (IllegalStateException e)
         {
            amqSession.removeConsumer(info);
         }
      }
   }

   public void addSessions(OpenWireConnection theConn, Set<SessionId> sessionSet)
   {
      Iterator<SessionId> iter = sessionSet.iterator();
      while (iter.hasNext())
      {
         SessionId sid = iter.next();
         addSession(theConn, theConn.getState().getSessionState(sid).getInfo(),
                    true);
      }
   }

   public AMQSession addSession(OpenWireConnection theConn, SessionInfo ss)
   {
      return addSession(theConn, ss, false);
   }

   public AMQSession addSession(OpenWireConnection theConn, SessionInfo ss,
                                boolean internal)
   {
      AMQSession amqSession = new AMQSession(theConn.getState().getInfo(), ss,
                                             server, theConn, this);
      amqSession.initialize();
      amqSession.setInternal(internal);
      sessions.put(ss.getSessionId(), amqSession);
      sessionIdMap.put(amqSession.getCoreSession().getName(), ss.getSessionId());
      return amqSession;
   }

   public void removeConnection(AMQConnectionContext context,
                                ConnectionInfo info, Throwable error)
   {
      // todo roll back tx
      this.connections.remove(context.getConnection());
      this.connectionInfos.remove(info.getConnectionId());
      String clientId = info.getClientId();
      if (clientId != null)
      {
         this.clientIdSet.remove(clientId);
      }
   }

   public void removeSession(AMQConnectionContext context, SessionInfo info) throws Exception
   {
      AMQSession session = sessions.remove(info.getSessionId());
      if (session != null)
      {
         session.close();
      }
   }

   public void removeConsumer(AMQConnectionContext context, ConsumerInfo info) throws Exception
   {
      SessionId sessionId = info.getConsumerId().getParentId();
      AMQSession session = sessions.get(sessionId);
      session.removeConsumer(info);
   }

   public void removeProducer(ProducerId id)
   {
      SessionId sessionId = id.getParentId();
      AMQSession session = sessions.get(sessionId);
      session.removeProducer(id);
   }

   public AMQPersistenceAdapter getPersistenceAdapter()
   {
      // TODO Auto-generated method stub
      return null;
   }

   public AMQSession getSession(SessionId sessionId)
   {
      return sessions.get(sessionId);
   }

   public void addDestination(OpenWireConnection connection,
                              DestinationInfo info) throws Exception
   {
      ActiveMQDestination dest = info.getDestination();
      if (dest.isQueue())
      {
         SimpleString qName = new SimpleString("jms.queue."
                                                  + dest.getPhysicalName());
         ConnectionState state = connection.brokerConnectionStates.get(info.getConnectionId());
         ConnectionInfo connInfo = state.getInfo();
         if (connInfo != null)
         {
            String user = connInfo.getUserName();
            String pass = connInfo.getPassword();

            AMQServerSession fakeSession = new AMQServerSession(user, pass);
            CheckType checkType = dest.isTemporary() ? CheckType.CREATE_NON_DURABLE_QUEUE : CheckType.CREATE_DURABLE_QUEUE;
            ((ActiveMQServerImpl) server).getSecurityStore().check(qName, checkType, fakeSession);

            ((ActiveMQServerImpl) server).checkQueueCreationLimit(user);
         }
         this.server.createQueue(qName, qName, null, connInfo == null ? null : SimpleString.toSimpleString(connInfo.getUserName()), false, true);
         if (dest.isTemporary())
         {
            connection.registerTempQueue(qName);
         }
      }

      if (!AdvisorySupport.isAdvisoryTopic(dest))
      {
         AMQConnectionContext context = connection.getConext();
         DestinationInfo advInfo = new DestinationInfo(context.getConnectionId(), DestinationInfo.ADD_OPERATION_TYPE, dest);

         ActiveMQTopic topic = AdvisorySupport.getDestinationAdvisoryTopic(dest);
         fireAdvisory(context, topic, advInfo);
      }
   }

   public void deleteQueue(String q) throws Exception
   {
      server.destroyQueue(new SimpleString(q));
   }

   public void commitTransactionOnePhase(TransactionInfo info) throws Exception
   {
      AMQSession txSession = transactions.get(info.getTransactionId());

      if (txSession != null)
      {
         txSession.commitOnePhase(info);
      }
      transactions.remove(info.getTransactionId());
   }

   public void prepareTransaction(TransactionInfo info) throws Exception
   {
      XATransactionId xid = (XATransactionId) info.getTransactionId();
      AMQSession txSession = transactions.get(xid);
      if (txSession != null)
      {
         txSession.prepareTransaction(xid);
      }
   }

   public void commitTransactionTwoPhase(TransactionInfo info) throws Exception
   {
      XATransactionId xid = (XATransactionId) info.getTransactionId();
      AMQSession txSession = transactions.get(xid);
      if (txSession != null)
      {
         txSession.commitTwoPhase(xid);
      }
      transactions.remove(xid);
   }

   public void rollbackTransaction(TransactionInfo info) throws Exception
   {
      AMQSession txSession = transactions.get(info.getTransactionId());
      if (txSession != null)
      {
         txSession.rollback(info);
      }
      transactions.remove(info.getTransactionId());
   }

   public TransactionId[] recoverTransactions(Set<SessionId> sIds)
   {
      List<TransactionId> recovered = new ArrayList<TransactionId>();
      if (sIds != null)
      {
         for (SessionId sid : sIds)
         {
            AMQSession s = this.sessions.get(sid);
            if (s != null)
            {
               s.recover(recovered);
            }
         }
      }
      return recovered.toArray(new TransactionId[0]);
   }

   public boolean validateUser(String login, String passcode)
   {
      boolean validated = true;

      ActiveMQSecurityManager sm = server.getSecurityManager();

      if (sm != null && server.getConfiguration().isSecurityEnabled())
      {
         validated = sm.validateUser(login, passcode);
      }

      return validated;
   }

   public void forgetTransaction(TransactionId xid) throws Exception
   {
      AMQSession txSession = transactions.get(xid);
      if (txSession != null)
      {
         txSession.forget(xid);
      }
      transactions.remove(xid);
   }

   public void registerTx(TransactionId txId, AMQSession amqSession)
   {
      transactions.put(txId, amqSession);
   }

   //advisory support
   @Override
   public void onNotification(Notification notif)
   {
      try
      {
         if (notif.getType() instanceof CoreNotificationType)
         {
            CoreNotificationType type = (CoreNotificationType)notif.getType();
            switch (type)
            {
               case CONSUMER_SLOW:
                  fireSlowConsumer(notif);
                  break;
               default:
                  break;
            }
         }
      }
      catch (Exception e)
      {
         ActiveMQServerLogger.LOGGER.error("Failed to send notification " + notif, e);
      }
   }

   private void fireSlowConsumer(Notification notif) throws Exception
   {
      SimpleString coreSessionId = notif.getProperties().getSimpleStringProperty(ManagementHelper.HDR_SESSION_NAME);
      Long coreConsumerId = notif.getProperties().getLongProperty(ManagementHelper.HDR_CONSUMER_NAME);
      SessionId sessionId = sessionIdMap.get(coreSessionId.toString());
      AMQSession session = sessions.get(sessionId);
      AMQConsumer consumer = session.getConsumer(coreConsumerId);
      ActiveMQDestination destination = consumer.getDestination();

      if (!AdvisorySupport.isAdvisoryTopic(destination))
      {
         ActiveMQTopic topic = AdvisorySupport.getSlowConsumerAdvisoryTopic(destination);
         ConnectionId connId = sessionId.getParentId();
         AMQTransportConnectionState cc = (AMQTransportConnectionState)this.brokerConnectionStates.get(connId);
         OpenWireConnection conn = cc.getConnection();
         ActiveMQMessage advisoryMessage = new ActiveMQMessage();
         advisoryMessage.setStringProperty(AdvisorySupport.MSG_PROPERTY_CONSUMER_ID, consumer.getId().toString());

         fireAdvisory(conn.getConext(), topic, advisoryMessage, consumer.getId());
      }
   }
}
