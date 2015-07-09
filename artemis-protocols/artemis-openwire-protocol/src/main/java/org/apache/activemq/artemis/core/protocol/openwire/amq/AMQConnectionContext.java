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
package org.apache.activemq.artemis.core.protocol.openwire.amq;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.WireFormatInfo;
import org.apache.activemq.command.XATransactionId;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.apache.activemq.state.ConnectionState;
import org.apache.activemq.artemis.core.protocol.openwire.OpenWireConnection;
import org.apache.activemq.artemis.core.protocol.openwire.OpenWireProtocolManager;

public class AMQConnectionContext
{
   private OpenWireConnection connection;
   private AMQConnector connector;
   private OpenWireProtocolManager broker; //use protocol manager to represent the broker
   private boolean inRecoveryMode;
   private AMQTransaction transaction;
   private ConcurrentHashMap<TransactionId, AMQTransaction> transactions;
   private AMQSecurityContext securityContext;
   private ConnectionId connectionId;
   private String clientId;
   private String userName;
   private boolean reconnect;
   private WireFormatInfo wireFormatInfo;
   private Object longTermStoreContext;
   private boolean producerFlowControl = true;
   private AMQMessageAuthorizationPolicy messageAuthorizationPolicy;
   private boolean networkConnection;
   private boolean faultTolerant;
   private final AtomicBoolean stopping = new AtomicBoolean();
   private final MessageEvaluationContext messageEvaluationContext;
   private boolean dontSendResponse;
   private boolean clientMaster = true;
   private ConnectionState connectionState;
   private XATransactionId xid;

   public AMQConnectionContext()
   {
      this.messageEvaluationContext = new MessageEvaluationContext();
   }

   public AMQConnectionContext(MessageEvaluationContext messageEvaluationContext)
   {
      this.messageEvaluationContext = messageEvaluationContext;
   }

   public AMQConnectionContext(ConnectionInfo info)
   {
      this();
      setClientId(info.getClientId());
      setUserName(info.getUserName());
      setConnectionId(info.getConnectionId());
   }

   public AMQConnectionContext copy()
   {
      AMQConnectionContext rc = new AMQConnectionContext(
            this.messageEvaluationContext);
      rc.connection = this.connection;
      rc.connector = this.connector;
      rc.broker = this.broker;
      rc.inRecoveryMode = this.inRecoveryMode;
      rc.transaction = this.transaction;
      rc.transactions = this.transactions;
      rc.securityContext = this.securityContext;
      rc.connectionId = this.connectionId;
      rc.clientId = this.clientId;
      rc.userName = this.userName;
      rc.reconnect = this.reconnect;
      rc.wireFormatInfo = this.wireFormatInfo;
      rc.longTermStoreContext = this.longTermStoreContext;
      rc.producerFlowControl = this.producerFlowControl;
      rc.messageAuthorizationPolicy = this.messageAuthorizationPolicy;
      rc.networkConnection = this.networkConnection;
      rc.faultTolerant = this.faultTolerant;
      rc.stopping.set(this.stopping.get());
      rc.dontSendResponse = this.dontSendResponse;
      rc.clientMaster = this.clientMaster;
      return rc;
   }

   public AMQSecurityContext getSecurityContext()
   {
      return securityContext;
   }

   public void setSecurityContext(AMQSecurityContext subject)
   {
      this.securityContext = subject;
      if (subject != null)
      {
         setUserName(subject.getUserName());
      }
      else
      {
         setUserName(null);
      }
   }

   /**
    * @return the broker being used.
    */
   public OpenWireProtocolManager getBroker()
   {
      return broker;
   }

   /**
    * @param broker
    *           being used
    */
   public void setBroker(OpenWireProtocolManager broker)
   {
      this.broker = broker;
   }

   /**
    * @return the connection being used
    */
   public OpenWireConnection getConnection()
   {
      return connection;
   }

   /**
    * @param connection
    *           being used
    */
   public void setConnection(OpenWireConnection connection)
   {
      this.connection = connection;
   }

   /**
    * @return the transaction being used.
    */
   public AMQTransaction getTransaction()
   {
      return transaction;
   }

   /**
    * @param transaction
    *           being used.
    */
   public void setTransaction(AMQTransaction transaction)
   {
      this.transaction = transaction;
   }

   /**
    * @return the connector being used.
    */
   public AMQConnector getConnector()
   {
      return connector;
   }

   /**
    * @param connector
    *           being used.
    */
   public void setConnector(AMQConnector connector)
   {
      this.connector = connector;
   }

   public AMQMessageAuthorizationPolicy getMessageAuthorizationPolicy()
   {
      return messageAuthorizationPolicy;
   }

   /**
    * Sets the policy used to decide if the current connection is authorized to
    * consume a given message
    */
   public void setMessageAuthorizationPolicy(
         AMQMessageAuthorizationPolicy messageAuthorizationPolicy)
   {
      this.messageAuthorizationPolicy = messageAuthorizationPolicy;
   }

   /**
    * @return
    */
   public boolean isInRecoveryMode()
   {
      return inRecoveryMode;
   }

   public void setInRecoveryMode(boolean inRecoveryMode)
   {
      this.inRecoveryMode = inRecoveryMode;
   }

   public ConcurrentHashMap<TransactionId, AMQTransaction> getTransactions()
   {
      return transactions;
   }

   public void setTransactions(
         ConcurrentHashMap<TransactionId, AMQTransaction> transactions)
   {
      this.transactions = transactions;
   }

   public boolean isInTransaction()
   {
      return transaction != null;
   }

   public String getClientId()
   {
      return clientId;
   }

   public void setClientId(String clientId)
   {
      this.clientId = clientId;
   }

   public boolean isReconnect()
   {
      return reconnect;
   }

   public void setReconnect(boolean reconnect)
   {
      this.reconnect = reconnect;
   }

   public WireFormatInfo getWireFormatInfo()
   {
      return wireFormatInfo;
   }

   public void setWireFormatInfo(WireFormatInfo wireFormatInfo)
   {
      this.wireFormatInfo = wireFormatInfo;
   }

   public ConnectionId getConnectionId()
   {
      return connectionId;
   }

   public void setConnectionId(ConnectionId connectionId)
   {
      this.connectionId = connectionId;
   }

   public String getUserName()
   {
      return userName;
   }

   public void setUserName(String userName)
   {
      this.userName = userName;
   }

   public MessageEvaluationContext getMessageEvaluationContext()
   {
      return messageEvaluationContext;
   }

   public Object getLongTermStoreContext()
   {
      return longTermStoreContext;
   }

   public void setLongTermStoreContext(Object longTermStoreContext)
   {
      this.longTermStoreContext = longTermStoreContext;
   }

   public boolean isProducerFlowControl()
   {
      return producerFlowControl;
   }

   public void setProducerFlowControl(boolean disableProducerFlowControl)
   {
      this.producerFlowControl = disableProducerFlowControl;
   }

   public boolean isAllowedToConsume(MessageReference n) throws IOException
   {
      if (messageAuthorizationPolicy != null)
      {
         return messageAuthorizationPolicy.isAllowedToConsume(this,
               n.getMessage());
      }
      return true;
   }

   public synchronized boolean isNetworkConnection()
   {
      return networkConnection;
   }

   public synchronized void setNetworkConnection(boolean networkConnection)
   {
      this.networkConnection = networkConnection;
   }

   public AtomicBoolean getStopping()
   {
      return stopping;
   }

   public void setDontSendReponse(boolean b)
   {
      this.dontSendResponse = b;
   }

   public boolean isDontSendReponse()
   {
      return dontSendResponse;
   }

   /**
    * @return the clientMaster
    */
   public boolean isClientMaster()
   {
      return this.clientMaster;
   }

   /**
    * @param clientMaster
    *           the clientMaster to set
    */
   public void setClientMaster(boolean clientMaster)
   {
      this.clientMaster = clientMaster;
   }

   public boolean isFaultTolerant()
   {
      return faultTolerant;
   }

   public void setFaultTolerant(boolean faultTolerant)
   {
      this.faultTolerant = faultTolerant;
   }

   public void setConnectionState(ConnectionState connectionState)
   {
      this.connectionState = connectionState;
   }

   public ConnectionState getConnectionState()
   {
      return this.connectionState;
   }

   public void setXid(XATransactionId id)
   {
      this.xid = id;
   }

   public XATransactionId getXid()
   {
      return xid;
   }

   public boolean isAllowLinkStealing()
   {
      return connector != null && connector.isAllowLinkStealing();
   }

}
