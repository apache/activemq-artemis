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
import javax.transaction.xa.XAException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;

import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.BaseInterceptor;
import org.apache.activemq.artemis.api.core.Interceptor;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClusterTopologyListener;
import org.apache.activemq.artemis.api.core.client.TopologyMember;
import org.apache.activemq.artemis.core.protocol.openwire.amq.AMQConnectionContext;
import org.apache.activemq.artemis.core.protocol.openwire.amq.AMQProducerBrokerExchange;
import org.apache.activemq.artemis.core.protocol.openwire.amq.AMQSession;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyServerConnection;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.cluster.ClusterConnection;
import org.apache.activemq.artemis.core.server.cluster.ClusterManager;
import org.apache.activemq.artemis.spi.core.protocol.ConnectionEntry;
import org.apache.activemq.artemis.spi.core.protocol.MessageConverter;
import org.apache.activemq.artemis.spi.core.protocol.ProtocolManager;
import org.apache.activemq.artemis.spi.core.protocol.ProtocolManagerFactory;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.remoting.Acceptor;
import org.apache.activemq.artemis.spi.core.remoting.Connection;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager;
import org.apache.activemq.artemis.utils.DataConstants;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.BrokerId;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.ConnectionControl;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.TransactionInfo;
import org.apache.activemq.command.WireFormatInfo;
import org.apache.activemq.command.XATransactionId;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.openwire.OpenWireFormatFactory;
import org.apache.activemq.state.ProducerState;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.InetAddressUtil;
import org.apache.activemq.util.LongSequenceGenerator;

public class OpenWireProtocolManager implements ProtocolManager<Interceptor>, ClusterTopologyListener {

   private static final IdGenerator BROKER_ID_GENERATOR = new IdGenerator();
   private static final IdGenerator ID_GENERATOR = new IdGenerator();

   private final LongSequenceGenerator messageIdGenerator = new LongSequenceGenerator();
   private final ActiveMQServer server;

   private final OpenWireProtocolManagerFactory factory;

   private OpenWireFormatFactory wireFactory;

   private boolean prefixPacketSize = true;

   private BrokerId brokerId;
   protected final ProducerId advisoryProducerId = new ProducerId();

   private final CopyOnWriteArrayList<OpenWireConnection> connections = new CopyOnWriteArrayList<>();

   // TODO-NOW: this can probably go away
   private final Map<String, AMQConnectionContext> clientIdSet = new HashMap<String, AMQConnectionContext>();

   private String brokerName;

   // Clebert: Artemis already has a Resource Manager. Need to remove this..
   //          The TransactionID extends XATransactionID, so all we need is to convert the XID here
   private Map<TransactionId, AMQSession> transactions = new ConcurrentHashMap<>();

   private final Map<String, TopologyMember> topologyMap = new ConcurrentHashMap<>();

   private final LinkedList<TopologyMember> members = new LinkedList<>();

   private final ScheduledExecutorService scheduledPool;

   //bean properties
   //http://activemq.apache.org/failover-transport-reference.html
   private boolean rebalanceClusterClients = false;
   private boolean updateClusterClients = false;
   private boolean updateClusterClientsOnRemove = false;

   private final OpenWireMessageConverter messageConverter;

   public OpenWireProtocolManager(OpenWireProtocolManagerFactory factory, ActiveMQServer server) {
      this.factory = factory;
      this.server = server;
      this.wireFactory = new OpenWireFormatFactory();
      // preferred prop, should be done via config
      wireFactory.setCacheEnabled(false);
      advisoryProducerId.setConnectionId(ID_GENERATOR.generateId());
      scheduledPool = server.getScheduledPool();
      this.messageConverter = new OpenWireMessageConverter(wireFactory.createWireFormat());

      final ClusterManager clusterManager = this.server.getClusterManager();

      // TODO-NOW: use a property name for the cluster connection
      ClusterConnection cc = clusterManager.getDefaultConnection(null);

      if (cc != null) {
         cc.addClusterTopologyListener(this);
      }
   }

   public OpenWireFormat getNewWireFormat() {
      return (OpenWireFormat)wireFactory.createWireFormat();
   }

   @Override
   public void nodeUP(TopologyMember member, boolean last) {
      if (topologyMap.put(member.getNodeId(), member) == null) {
         updateClientClusterInfo();
      }
   }

   public void nodeDown(long eventUID, String nodeID) {
      if (topologyMap.remove(nodeID) != null) {
         updateClientClusterInfo();
      }
   }


   public void removeConnection(ConnectionInfo info,
                                Throwable error) throws InvalidClientIDException {
      synchronized (clientIdSet) {
         String clientId = info.getClientId();
         if (clientId != null) {
            AMQConnectionContext context = this.clientIdSet.get(clientId);
            if (context != null && context.decRefCount() == 0) {
               //connection is still there and need to close
               context.getConnection().disconnect(error != null);
               this.connections.remove(this);//what's that for?
               this.clientIdSet.remove(clientId);
            }
         }
         else {
            throw new InvalidClientIDException("No clientID specified for connection disconnect request");
         }
      }
   }


   public ScheduledExecutorService getScheduledPool() {
      return scheduledPool;
   }

   public ActiveMQServer getServer() {
      return server;
   }

   private void updateClientClusterInfo() {

      synchronized (members) {
         members.clear();
         members.addAll(topologyMap.values());
      }

      for (OpenWireConnection c : this.connections) {
         ConnectionControl control = newConnectionControl();
         try {
            c.updateClient(control);
         }
         catch (Exception e) {
            ActiveMQServerLogger.LOGGER.warn(e.getMessage(), e);
            c.sendException(e);
         }
      }
   }

   @Override
   public boolean acceptsNoHandshake() {
      return false;
   }

   @Override
   public ProtocolManagerFactory<Interceptor> getFactory() {
      return factory;
   }

   @Override
   public void updateInterceptors(List<BaseInterceptor> incomingInterceptors,
                                  List<BaseInterceptor> outgoingInterceptors) {
      // NO-OP
   }

   @Override
   public ConnectionEntry createConnectionEntry(Acceptor acceptorUsed, Connection connection) {
      OpenWireFormat wf = (OpenWireFormat) wireFactory.createWireFormat();
      OpenWireConnection owConn = new OpenWireConnection(connection, server.getExecutorFactory().getExecutor(), this, wf);
      owConn.sendHandshake();

      // TODO CLEBERT What is this constant here? we should get it from TTL initial pings
      return new ConnectionEntry(owConn, null, System.currentTimeMillis(), 1 * 60 * 1000);
   }

   @Override
   public MessageConverter getConverter() {
      return messageConverter;
   }

   @Override
   public void removeHandler(String name) {
   }

   @Override
   public void handleBuffer(RemotingConnection connection, ActiveMQBuffer buffer) {
   }

   @Override
   public void addChannelHandlers(ChannelPipeline pipeline) {
      // each read will have a full packet with this
      pipeline.addLast("packet-decipher", new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, DataConstants.SIZE_INT));
   }

   @Override
   public boolean isProtocol(byte[] array) {
      if (array.length < 8) {
         throw new IllegalArgumentException("Protocol header length changed " + array.length);
      }

      int start = this.prefixPacketSize ? 4 : 0;
      int j = 0;
      // type
      if (array[start] != WireFormatInfo.DATA_STRUCTURE_TYPE) {
         return false;
      }
      start++;
      WireFormatInfo info = new WireFormatInfo();
      final byte[] magic = info.getMagic();
      int remainingLen = array.length - start;
      int useLen = remainingLen > magic.length ? magic.length : remainingLen;
      useLen += start;
      // magic
      for (int i = start; i < useLen; i++) {
         if (array[i] != magic[j]) {
            return false;
         }
         j++;
      }
      return true;
   }

   @Override
   public void handshake(NettyServerConnection connection, ActiveMQBuffer buffer) {
   }

   public void addConnection(OpenWireConnection connection, ConnectionInfo info) throws Exception {
      String username = info.getUserName();
      String password = info.getPassword();

      if (!this.validateUser(username, password)) {
         throw new SecurityException("User name [" + username + "] or password is invalid.");
      }

      String clientId = info.getClientId();
      if (clientId == null) {
         throw new InvalidClientIDException("No clientID specified for connection request");
      }

      synchronized (clientIdSet) {
         AMQConnectionContext context;
         context = clientIdSet.get(clientId);
         if (context != null) {
            if (info.isFailoverReconnect()) {
               OpenWireConnection oldConnection = context.getConnection();
               oldConnection.disconnect(true);
               connections.remove(oldConnection);
               connection.reconnect(context, info);
            }
            else {
               throw new InvalidClientIDException("Broker: " + getBrokerName() + " - Client: " + clientId + " already connected from " + context.getConnection().getRemoteAddress());
            }
         }
         else {
            //new connection
            context = connection.initContext(info);
            clientIdSet.put(clientId, context);
         }

         connections.add(connection);

         ActiveMQTopic topic = AdvisorySupport.getConnectionAdvisoryTopic();
         // do not distribute passwords in advisory messages. usernames okay
         ConnectionInfo copy = info.copy();
         copy.setPassword("");
         fireAdvisory(context, topic, copy);

         // init the conn
         context.getConnection().addSessions( context.getConnectionState().getSessionIds());
      }
   }

   public void fireAdvisory(AMQConnectionContext context, ActiveMQTopic topic, Command copy) throws Exception {
      this.fireAdvisory(context, topic, copy, null);
   }

   public BrokerId getBrokerId() {
      // TODO: Use the Storage ID here...
      if (brokerId == null) {
         brokerId = new BrokerId(BROKER_ID_GENERATOR.generateId());
      }
      return brokerId;
   }

   /*
    * See AdvisoryBroker.fireAdvisory()
    */
   public void fireAdvisory(AMQConnectionContext context,
                             ActiveMQTopic topic,
                             Command command,
                             ConsumerId targetConsumerId) throws Exception {
      ActiveMQMessage advisoryMessage = new ActiveMQMessage();
      advisoryMessage.setStringProperty(AdvisorySupport.MSG_PROPERTY_ORIGIN_BROKER_NAME, getBrokerName());
      String id = getBrokerId() != null ? getBrokerId().getValue() : "NOT_SET";
      advisoryMessage.setStringProperty(AdvisorySupport.MSG_PROPERTY_ORIGIN_BROKER_ID, id);

      String url = context.getConnection().getLocalAddress();

      advisoryMessage.setStringProperty(AdvisorySupport.MSG_PROPERTY_ORIGIN_BROKER_URL, url);

      // set the data structure
      advisoryMessage.setDataStructure(command);
      advisoryMessage.setPersistent(false);
      advisoryMessage.setType(AdvisorySupport.ADIVSORY_MESSAGE_TYPE);
      advisoryMessage.setMessageId(new MessageId(advisoryProducerId, messageIdGenerator.getNextSequenceId()));
      advisoryMessage.setTargetConsumerId(targetConsumerId);
      advisoryMessage.setDestination(topic);
      advisoryMessage.setResponseRequired(false);
      advisoryMessage.setProducerId(advisoryProducerId);
      boolean originalFlowControl = context.isProducerFlowControl();
      final AMQProducerBrokerExchange producerExchange = new AMQProducerBrokerExchange();
      producerExchange.setConnectionContext(context);
      producerExchange.setProducerState(new ProducerState(new ProducerInfo()));
      try {
         context.setProducerFlowControl(false);
         AMQSession sess = context.getConnection().getAdvisorySession();
         if (sess != null) {
            sess.send(producerExchange.getProducerState().getInfo(), advisoryMessage, false);
         }
      }
      finally {
         context.setProducerFlowControl(originalFlowControl);
      }
   }

   public String getBrokerName() {
      if (brokerName == null) {
         try {
            brokerName = InetAddressUtil.getLocalHostName().toLowerCase(Locale.ENGLISH);
         }
         catch (Exception e) {
            brokerName = server.getNodeID().toString();
         }
      }
      return brokerName;
   }

   protected ConnectionControl newConnectionControl() {
      ConnectionControl control = new ConnectionControl();

      String uri = generateMembersURI(rebalanceClusterClients);
      control.setConnectedBrokers(uri);

      control.setRebalanceConnection(rebalanceClusterClients);
      return control;
   }

   private String generateMembersURI(boolean flip) {
      String uri;
      StringBuffer connectedBrokers = new StringBuffer();
      String separator = "";

      synchronized (members) {
         if (members.size() > 0) {
            for (TopologyMember member : members) {
               connectedBrokers.append(separator).append(member.toURI());
               separator = ",";
            }

            // The flip exists to guarantee even distribution of URIs when sent to the client
            // in case of failures you won't get all the connections failing to a single server.
            if (flip && members.size() > 1) {
               members.addLast(members.removeFirst());
            }
         }
      }

      uri = connectedBrokers.toString();
      return uri;
   }

   public boolean isFaultTolerantConfiguration() {
      return false;
   }

   public void postProcessDispatch(MessageDispatch md) {
      // TODO Auto-generated method stub

   }

   public boolean isStopped() {
      // TODO Auto-generated method stub
      return false;
   }

   public void preProcessDispatch(MessageDispatch messageDispatch) {
      // TODO Auto-generated method stub

   }

   public boolean isStopping() {
      return false;
   }
   public void endTransaction(TransactionInfo info) throws Exception {
      AMQSession txSession = transactions.get(info.getTransactionId());

      if (txSession != null) {
         txSession.endTransaction(info);
      }
   }

   public void commitTransactionOnePhase(TransactionInfo info) throws Exception {
      AMQSession txSession = transactions.get(info.getTransactionId());

      if (txSession != null) {
         txSession.commitOnePhase(info);
      }
      transactions.remove(info.getTransactionId());
   }

   public void prepareTransaction(TransactionInfo info) throws Exception {
      XATransactionId xid = (XATransactionId) info.getTransactionId();
      AMQSession txSession = transactions.get(xid);
      if (txSession != null) {
         txSession.prepareTransaction(xid);
      }
   }

   public void commitTransactionTwoPhase(TransactionInfo info) throws Exception {
      XATransactionId xid = (XATransactionId) info.getTransactionId();
      AMQSession txSession = transactions.get(xid);
      if (txSession != null) {
         txSession.commitTwoPhase(xid);
      }
      transactions.remove(xid);
   }

   public void rollbackTransaction(TransactionInfo info) throws Exception {
      AMQSession txSession = transactions.get(info.getTransactionId());
      if (txSession != null) {
         txSession.rollback(info);
      }
      else if (info.getTransactionId().isLocalTransaction()) {
         //during a broker restart, recovered local transaction may not be registered
         //in that case we ignore and let the tx removed silently by connection.
         //see AMQ1925Test.testAMQ1925_TXBegin
      }
      else {
         throw newXAException("Transaction '" + info.getTransactionId() + "' has not been started.", XAException.XAER_NOTA);
      }
      transactions.remove(info.getTransactionId());
   }

   public boolean validateUser(String login, String passcode) {
      boolean validated = true;

      ActiveMQSecurityManager sm = server.getSecurityManager();

      if (sm != null && server.getConfiguration().isSecurityEnabled()) {
         validated = sm.validateUser(login, passcode);
      }

      return validated;
   }

   public void forgetTransaction(TransactionId xid) throws Exception {
      AMQSession txSession = transactions.get(xid);
      if (txSession != null) {
         txSession.forget(xid);
      }
      transactions.remove(xid);
   }

   /**
    * TODO: remove this, use the regular ResourceManager from the Server's
    */
   public void registerTx(TransactionId txId, AMQSession amqSession) {
      transactions.put(txId, amqSession);
   }

   public void removeSubscription(RemoveSubscriptionInfo subInfo) throws Exception {
      SimpleString subQueueName = new SimpleString(org.apache.activemq.artemis.jms.client.ActiveMQDestination.createQueueNameForDurableSubscription(true, subInfo.getClientId(), subInfo.getSubscriptionName()));
      server.destroyQueue(subQueueName);
   }

   public void sendBrokerInfo(OpenWireConnection connection) throws Exception {
      BrokerInfo brokerInfo = new BrokerInfo();
      brokerInfo.setBrokerName(getBrokerName());
      brokerInfo.setBrokerId(new BrokerId("" + server.getNodeID()));
      brokerInfo.setPeerBrokerInfos(null);
      brokerInfo.setFaultTolerantConfiguration(false);
      brokerInfo.setBrokerURL(connection.getLocalAddress());

      //cluster support yet to support
      brokerInfo.setPeerBrokerInfos(null);
      connection.dispatch(brokerInfo);
   }

   public void setRebalanceClusterClients(boolean rebalance) {
      this.rebalanceClusterClients = rebalance;
   }

   public boolean isRebalanceClusterClients() {
      return this.rebalanceClusterClients;
   }

   public void setUpdateClusterClients(boolean updateClusterClients) {
      this.updateClusterClients = updateClusterClients;
   }

   public boolean isUpdateClusterClients() {
      return this.updateClusterClients;
   }

   public void setUpdateClusterClientsOnRemove(boolean updateClusterClientsOnRemove) {
      this.updateClusterClientsOnRemove = updateClusterClientsOnRemove;
   }

   public boolean isUpdateClusterClientsOnRemove() {
      return this.updateClusterClientsOnRemove;
   }

   public void setBrokerName(String name) {
      this.brokerName = name;
   }

   public static XAException newXAException(String s, int errorCode) {
      XAException xaException = new XAException(s + " " + "xaErrorCode:" + errorCode);
      xaException.errorCode = errorCode;
      return xaException;
   }

}
