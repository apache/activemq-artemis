/**
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
package org.apache.activemq.artemis.jms.management.impl;

import javax.management.MBeanInfo;
import javax.management.NotCompliantMBeanException;
import javax.management.StandardMBean;

import org.apache.activemq.artemis.api.core.DiscoveryGroupConfiguration;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.management.Parameter;
import org.apache.activemq.artemis.api.jms.management.ConnectionFactoryControl;
import org.apache.activemq.artemis.core.management.impl.MBeanInfoHelper;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.server.JMSServerManager;
import org.apache.activemq.artemis.jms.server.config.ConnectionFactoryConfiguration;

public class JMSConnectionFactoryControlImpl extends StandardMBean implements ConnectionFactoryControl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final ConnectionFactoryConfiguration cfConfig;

   private ActiveMQConnectionFactory cf;

   private final String name;

   private final JMSServerManager jmsManager;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public JMSConnectionFactoryControlImpl(final ConnectionFactoryConfiguration cfConfig,
                                          final ActiveMQConnectionFactory cf,
                                          final JMSServerManager jmsManager,
                                          final String name) throws NotCompliantMBeanException
   {
      super(ConnectionFactoryControl.class);
      this.cfConfig = cfConfig;
      this.cf = cf;
      this.name = name;
      this.jmsManager = jmsManager;
   }

   // Public --------------------------------------------------------

   // ManagedConnectionFactoryMBean implementation ------------------

   public String[] getRegistryBindings()
   {
      return jmsManager.getBindingsOnConnectionFactory(name);
   }

   public boolean isCompressLargeMessages()
   {
      return cf.isCompressLargeMessage();
   }

   public void setCompressLargeMessages(final boolean compress)
   {
      cfConfig.setCompressLargeMessages(compress);
      recreateCF();
   }

   public boolean isHA()
   {
      return cfConfig.isHA();
   }

   public int getFactoryType()
   {
      return cfConfig.getFactoryType().intValue();
   }

   public String getClientID()
   {
      return cfConfig.getClientID();
   }

   public long getClientFailureCheckPeriod()
   {
      return cfConfig.getClientFailureCheckPeriod();
   }

   public void setClientID(String clientID)
   {
      cfConfig.setClientID(clientID);
      recreateCF();
   }

   public void setDupsOKBatchSize(int dupsOKBatchSize)
   {
      cfConfig.setDupsOKBatchSize(dupsOKBatchSize);
      recreateCF();
   }

   public void setTransactionBatchSize(int transactionBatchSize)
   {
      cfConfig.setTransactionBatchSize(transactionBatchSize);
      recreateCF();
   }

   public void setClientFailureCheckPeriod(long clientFailureCheckPeriod)
   {
      cfConfig.setClientFailureCheckPeriod(clientFailureCheckPeriod);
      recreateCF();
   }

   public void setConnectionTTL(long connectionTTL)
   {
      cfConfig.setConnectionTTL(connectionTTL);
      recreateCF();
   }

   public void setCallTimeout(long callTimeout)
   {
      cfConfig.setCallTimeout(callTimeout);
      recreateCF();
   }

   public void setCallFailoverTimeout(long callTimeout)
   {
      cfConfig.setCallFailoverTimeout(callTimeout);
      recreateCF();
   }

   public void setConsumerWindowSize(int consumerWindowSize)
   {
      cfConfig.setConsumerWindowSize(consumerWindowSize);
      recreateCF();
   }

   public void setConsumerMaxRate(int consumerMaxRate)
   {
      cfConfig.setConsumerMaxRate(consumerMaxRate);
      recreateCF();
   }

   public void setConfirmationWindowSize(int confirmationWindowSize)
   {
      cfConfig.setConfirmationWindowSize(confirmationWindowSize);
      recreateCF();
   }

   public void setProducerMaxRate(int producerMaxRate)
   {
      cfConfig.setProducerMaxRate(producerMaxRate);
      recreateCF();
   }

   public int getProducerWindowSize()
   {
      return cfConfig.getProducerWindowSize();
   }

   public void setProducerWindowSize(int producerWindowSize)
   {
      cfConfig.setProducerWindowSize(producerWindowSize);
      recreateCF();
   }

   public void setCacheLargeMessagesClient(boolean cacheLargeMessagesClient)
   {
      cfConfig.setCacheLargeMessagesClient(cacheLargeMessagesClient);
      recreateCF();
   }

   public boolean isCacheLargeMessagesClient()
   {
      return cfConfig.isCacheLargeMessagesClient();
   }

   public void setMinLargeMessageSize(int minLargeMessageSize)
   {
      cfConfig.setMinLargeMessageSize(minLargeMessageSize);
      recreateCF();
   }

   public void setBlockOnNonDurableSend(boolean blockOnNonDurableSend)
   {
      cfConfig.setBlockOnNonDurableSend(blockOnNonDurableSend);
      recreateCF();
   }

   public void setBlockOnAcknowledge(boolean blockOnAcknowledge)
   {
      cfConfig.setBlockOnAcknowledge(blockOnAcknowledge);
      recreateCF();
   }

   public void setBlockOnDurableSend(boolean blockOnDurableSend)
   {
      cfConfig.setBlockOnDurableSend(blockOnDurableSend);
      recreateCF();
   }

   public void setAutoGroup(boolean autoGroup)
   {
      cfConfig.setAutoGroup(autoGroup);
      recreateCF();
   }

   public void setPreAcknowledge(boolean preAcknowledge)
   {
      cfConfig.setPreAcknowledge(preAcknowledge);
      recreateCF();
   }

   public void setMaxRetryInterval(long retryInterval)
   {
      cfConfig.setMaxRetryInterval(retryInterval);
      recreateCF();
   }

   public void setRetryIntervalMultiplier(double retryIntervalMultiplier)
   {
      cfConfig.setRetryIntervalMultiplier(retryIntervalMultiplier);
      recreateCF();
   }

   public void setReconnectAttempts(int reconnectAttempts)
   {
      cfConfig.setReconnectAttempts(reconnectAttempts);
      recreateCF();
   }

   public void setFailoverOnInitialConnection(boolean failover)
   {
      cfConfig.setFailoverOnInitialConnection(failover);
      recreateCF();
   }

   public boolean isUseGlobalPools()
   {
      return cfConfig.isUseGlobalPools();
   }

   public void setScheduledThreadPoolMaxSize(int scheduledThreadPoolMaxSize)
   {
      cfConfig.setScheduledThreadPoolMaxSize(scheduledThreadPoolMaxSize);
      recreateCF();
   }

   public int getThreadPoolMaxSize()
   {
      return cfConfig.getThreadPoolMaxSize();
   }

   public void setThreadPoolMaxSize(int threadPoolMaxSize)
   {
      cfConfig.setThreadPoolMaxSize(threadPoolMaxSize);
      recreateCF();
   }

   public int getInitialMessagePacketSize()
   {
      return cf.getInitialMessagePacketSize();
   }

   public void setGroupID(String groupID)
   {
      cfConfig.setGroupID(groupID);
      recreateCF();
   }

   public String getGroupID()
   {
      return cfConfig.getGroupID();
   }

   public void setUseGlobalPools(boolean useGlobalPools)
   {
      cfConfig.setUseGlobalPools(useGlobalPools);
      recreateCF();
   }

   public int getScheduledThreadPoolMaxSize()
   {
      return cfConfig.getScheduledThreadPoolMaxSize();
   }

   public void setRetryInterval(long retryInterval)
   {
      cfConfig.setRetryInterval(retryInterval);
      recreateCF();
   }

   public long getMaxRetryInterval()
   {
      return cfConfig.getMaxRetryInterval();
   }

   public String getConnectionLoadBalancingPolicyClassName()
   {
      return cfConfig.getLoadBalancingPolicyClassName();
   }

   public void setConnectionLoadBalancingPolicyClassName(String name)
   {
      cfConfig.setLoadBalancingPolicyClassName(name);
      recreateCF();
   }

   public TransportConfiguration[] getStaticConnectors()
   {
      return cf.getStaticConnectors();
   }

   public DiscoveryGroupConfiguration getDiscoveryGroupConfiguration()
   {
      return cf.getDiscoveryGroupConfiguration();
   }

   public void addBinding(@Parameter(name = "binding", desc = "the name of the binding for the Registry") String binding) throws Exception
   {
      jmsManager.addConnectionFactoryToBindingRegistry(name, binding);
   }

   public void removeBinding(@Parameter(name = "binding", desc = "the name of the binding for the Registry") String binding) throws Exception
   {
      jmsManager.removeConnectionFactoryFromBindingRegistry(name, binding);
   }

   public long getCallTimeout()
   {
      return cfConfig.getCallTimeout();
   }

   public long getCallFailoverTimeout()
   {
      return cfConfig.getCallFailoverTimeout();
   }

   public int getConsumerMaxRate()
   {
      return cfConfig.getConsumerMaxRate();
   }

   public int getConsumerWindowSize()
   {
      return cfConfig.getConsumerWindowSize();
   }

   public int getProducerMaxRate()
   {
      return cfConfig.getProducerMaxRate();
   }

   public int getConfirmationWindowSize()
   {
      return cfConfig.getConfirmationWindowSize();
   }

   public int getDupsOKBatchSize()
   {
      return cfConfig.getDupsOKBatchSize();
   }

   public boolean isBlockOnAcknowledge()
   {
      return cfConfig.isBlockOnAcknowledge();
   }

   public boolean isBlockOnNonDurableSend()
   {
      return cfConfig.isBlockOnNonDurableSend();
   }

   public boolean isBlockOnDurableSend()
   {
      return cfConfig.isBlockOnDurableSend();
   }

   public boolean isPreAcknowledge()
   {
      return cfConfig.isPreAcknowledge();
   }

   public String getName()
   {
      return name;
   }

   public long getConnectionTTL()
   {
      return cfConfig.getConnectionTTL();
   }

   public int getReconnectAttempts()
   {
      return cfConfig.getReconnectAttempts();
   }

   public boolean isFailoverOnInitialConnection()
   {
      return cfConfig.isFailoverOnInitialConnection();
   }

   public int getMinLargeMessageSize()
   {
      return cfConfig.getMinLargeMessageSize();
   }

   public long getRetryInterval()
   {
      return cfConfig.getRetryInterval();
   }

   public double getRetryIntervalMultiplier()
   {
      return cfConfig.getRetryIntervalMultiplier();
   }

   public int getTransactionBatchSize()
   {
      return cfConfig.getTransactionBatchSize();
   }

   public boolean isAutoGroup()
   {
      return cfConfig.isAutoGroup();
   }

   @Override
   public MBeanInfo getMBeanInfo()
   {
      MBeanInfo info = super.getMBeanInfo();
      return new MBeanInfo(info.getClassName(),
                           info.getDescription(),
                           info.getAttributes(),
                           info.getConstructors(),
                           MBeanInfoHelper.getMBeanOperationsInfo(ConnectionFactoryControl.class),
                           info.getNotifications());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   private void recreateCF()
   {
      try
      {
         this.cf = jmsManager.recreateCF(this.name, this.cfConfig);
      }
      catch (Exception e)
      {
         throw new RuntimeException(e.getMessage(), e);
      }
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
