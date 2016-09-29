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

public class JMSConnectionFactoryControlImpl extends StandardMBean implements ConnectionFactoryControl {
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
                                          final String name) throws NotCompliantMBeanException {
      super(ConnectionFactoryControl.class);
      this.cfConfig = cfConfig;
      this.cf = cf;
      this.name = name;
      this.jmsManager = jmsManager;
   }

   // Public --------------------------------------------------------

   // ManagedConnectionFactoryMBean implementation ------------------

   @Override
   public String[] getRegistryBindings() {
      return jmsManager.getBindingsOnConnectionFactory(name);
   }

   @Override
   public boolean isCompressLargeMessages() {
      return cf.isCompressLargeMessage();
   }

   @Override
   public void setCompressLargeMessages(final boolean compress) {
      cfConfig.setCompressLargeMessages(compress);
      recreateCF();
   }

   @Override
   public boolean isHA() {
      return cfConfig.isHA();
   }

   @Override
   public int getFactoryType() {
      return cfConfig.getFactoryType().intValue();
   }

   @Override
   public String getClientID() {
      return cfConfig.getClientID();
   }

   @Override
   public long getClientFailureCheckPeriod() {
      return cfConfig.getClientFailureCheckPeriod();
   }

   @Override
   public void setClientID(String clientID) {
      cfConfig.setClientID(clientID);
      recreateCF();
   }

   @Override
   public void setDupsOKBatchSize(int dupsOKBatchSize) {
      cfConfig.setDupsOKBatchSize(dupsOKBatchSize);
      recreateCF();
   }

   @Override
   public void setTransactionBatchSize(int transactionBatchSize) {
      cfConfig.setTransactionBatchSize(transactionBatchSize);
      recreateCF();
   }

   @Override
   public void setClientFailureCheckPeriod(long clientFailureCheckPeriod) {
      cfConfig.setClientFailureCheckPeriod(clientFailureCheckPeriod);
      recreateCF();
   }

   @Override
   public void setConnectionTTL(long connectionTTL) {
      cfConfig.setConnectionTTL(connectionTTL);
      recreateCF();
   }

   @Override
   public void setCallTimeout(long callTimeout) {
      cfConfig.setCallTimeout(callTimeout);
      recreateCF();
   }

   @Override
   public void setCallFailoverTimeout(long callTimeout) {
      cfConfig.setCallFailoverTimeout(callTimeout);
      recreateCF();
   }

   @Override
   public void setConsumerWindowSize(int consumerWindowSize) {
      cfConfig.setConsumerWindowSize(consumerWindowSize);
      recreateCF();
   }

   @Override
   public void setConsumerMaxRate(int consumerMaxRate) {
      cfConfig.setConsumerMaxRate(consumerMaxRate);
      recreateCF();
   }

   @Override
   public void setConfirmationWindowSize(int confirmationWindowSize) {
      cfConfig.setConfirmationWindowSize(confirmationWindowSize);
      recreateCF();
   }

   @Override
   public void setProducerMaxRate(int producerMaxRate) {
      cfConfig.setProducerMaxRate(producerMaxRate);
      recreateCF();
   }

   @Override
   public int getProducerWindowSize() {
      return cfConfig.getProducerWindowSize();
   }

   @Override
   public void setProducerWindowSize(int producerWindowSize) {
      cfConfig.setProducerWindowSize(producerWindowSize);
      recreateCF();
   }

   @Override
   public void setCacheLargeMessagesClient(boolean cacheLargeMessagesClient) {
      cfConfig.setCacheLargeMessagesClient(cacheLargeMessagesClient);
      recreateCF();
   }

   @Override
   public boolean isCacheLargeMessagesClient() {
      return cfConfig.isCacheLargeMessagesClient();
   }

   @Override
   public void setMinLargeMessageSize(int minLargeMessageSize) {
      cfConfig.setMinLargeMessageSize(minLargeMessageSize);
      recreateCF();
   }

   @Override
   public void setBlockOnNonDurableSend(boolean blockOnNonDurableSend) {
      cfConfig.setBlockOnNonDurableSend(blockOnNonDurableSend);
      recreateCF();
   }

   @Override
   public void setBlockOnAcknowledge(boolean blockOnAcknowledge) {
      cfConfig.setBlockOnAcknowledge(blockOnAcknowledge);
      recreateCF();
   }

   @Override
   public void setBlockOnDurableSend(boolean blockOnDurableSend) {
      cfConfig.setBlockOnDurableSend(blockOnDurableSend);
      recreateCF();
   }

   @Override
   public void setAutoGroup(boolean autoGroup) {
      cfConfig.setAutoGroup(autoGroup);
      recreateCF();
   }

   @Override
   public void setPreAcknowledge(boolean preAcknowledge) {
      cfConfig.setPreAcknowledge(preAcknowledge);
      recreateCF();
   }

   @Override
   public void setMaxRetryInterval(long retryInterval) {
      cfConfig.setMaxRetryInterval(retryInterval);
      recreateCF();
   }

   @Override
   public void setRetryIntervalMultiplier(double retryIntervalMultiplier) {
      cfConfig.setRetryIntervalMultiplier(retryIntervalMultiplier);
      recreateCF();
   }

   @Override
   public void setReconnectAttempts(int reconnectAttempts) {
      cfConfig.setReconnectAttempts(reconnectAttempts);
      recreateCF();
   }

   @Override
   public void setFailoverOnInitialConnection(boolean failover) {
      cfConfig.setFailoverOnInitialConnection(failover);
      recreateCF();
   }

   @Override
   public boolean isUseGlobalPools() {
      return cfConfig.isUseGlobalPools();
   }

   @Override
   public void setScheduledThreadPoolMaxSize(int scheduledThreadPoolMaxSize) {
      cfConfig.setScheduledThreadPoolMaxSize(scheduledThreadPoolMaxSize);
      recreateCF();
   }

   @Override
   public int getThreadPoolMaxSize() {
      return cfConfig.getThreadPoolMaxSize();
   }

   @Override
   public void setThreadPoolMaxSize(int threadPoolMaxSize) {
      cfConfig.setThreadPoolMaxSize(threadPoolMaxSize);
      recreateCF();
   }

   @Override
   public int getInitialMessagePacketSize() {
      return cf.getInitialMessagePacketSize();
   }

   @Override
   public void setGroupID(String groupID) {
      cfConfig.setGroupID(groupID);
      recreateCF();
   }

   @Override
   public String getGroupID() {
      return cfConfig.getGroupID();
   }

   @Override
   public void setUseGlobalPools(boolean useGlobalPools) {
      cfConfig.setUseGlobalPools(useGlobalPools);
      recreateCF();
   }

   @Override
   public int getScheduledThreadPoolMaxSize() {
      return cfConfig.getScheduledThreadPoolMaxSize();
   }

   @Override
   public void setRetryInterval(long retryInterval) {
      cfConfig.setRetryInterval(retryInterval);
      recreateCF();
   }

   @Override
   public long getMaxRetryInterval() {
      return cfConfig.getMaxRetryInterval();
   }

   @Override
   public String getConnectionLoadBalancingPolicyClassName() {
      return cfConfig.getLoadBalancingPolicyClassName();
   }

   @Override
   public void setConnectionLoadBalancingPolicyClassName(String name) {
      cfConfig.setLoadBalancingPolicyClassName(name);
      recreateCF();
   }

   @Override
   public TransportConfiguration[] getStaticConnectors() {
      return cf.getStaticConnectors();
   }

   @Override
   public DiscoveryGroupConfiguration getDiscoveryGroupConfiguration() {
      return cf.getDiscoveryGroupConfiguration();
   }

   @Override
   public void addBinding(@Parameter(name = "binding", desc = "the name of the binding for the Registry") String binding) throws Exception {
      jmsManager.addConnectionFactoryToBindingRegistry(name, binding);
   }

   @Override
   public void removeBinding(@Parameter(name = "binding", desc = "the name of the binding for the Registry") String binding) throws Exception {
      jmsManager.removeConnectionFactoryFromBindingRegistry(name, binding);
   }

   @Override
   public long getCallTimeout() {
      return cfConfig.getCallTimeout();
   }

   @Override
   public long getCallFailoverTimeout() {
      return cfConfig.getCallFailoverTimeout();
   }

   @Override
   public int getConsumerMaxRate() {
      return cfConfig.getConsumerMaxRate();
   }

   @Override
   public int getConsumerWindowSize() {
      return cfConfig.getConsumerWindowSize();
   }

   @Override
   public int getProducerMaxRate() {
      return cfConfig.getProducerMaxRate();
   }

   @Override
   public int getConfirmationWindowSize() {
      return cfConfig.getConfirmationWindowSize();
   }

   @Override
   public int getDupsOKBatchSize() {
      return cfConfig.getDupsOKBatchSize();
   }

   @Override
   public boolean isBlockOnAcknowledge() {
      return cfConfig.isBlockOnAcknowledge();
   }

   @Override
   public boolean isBlockOnNonDurableSend() {
      return cfConfig.isBlockOnNonDurableSend();
   }

   @Override
   public boolean isBlockOnDurableSend() {
      return cfConfig.isBlockOnDurableSend();
   }

   @Override
   public boolean isPreAcknowledge() {
      return cfConfig.isPreAcknowledge();
   }

   @Override
   public String getName() {
      return name;
   }

   @Override
   public long getConnectionTTL() {
      return cfConfig.getConnectionTTL();
   }

   @Override
   public int getReconnectAttempts() {
      return cfConfig.getReconnectAttempts();
   }

   @Override
   public boolean isFailoverOnInitialConnection() {
      return cfConfig.isFailoverOnInitialConnection();
   }

   @Override
   public int getMinLargeMessageSize() {
      return cfConfig.getMinLargeMessageSize();
   }

   @Override
   public long getRetryInterval() {
      return cfConfig.getRetryInterval();
   }

   @Override
   public double getRetryIntervalMultiplier() {
      return cfConfig.getRetryIntervalMultiplier();
   }

   @Override
   public int getTransactionBatchSize() {
      return cfConfig.getTransactionBatchSize();
   }

   @Override
   public void setProtocolManagerFactoryStr(String protocolManagerFactoryStr) {
      cfConfig.setProtocolManagerFactoryStr(protocolManagerFactoryStr);
      recreateCF();
   }

   @Override
   public String getProtocolManagerFactoryStr() {
      return cfConfig.getProtocolManagerFactoryStr();
   }

   @Override
   public boolean isAutoGroup() {
      return cfConfig.isAutoGroup();
   }

   @Override
   public MBeanInfo getMBeanInfo() {
      MBeanInfo info = super.getMBeanInfo();
      return new MBeanInfo(info.getClassName(), info.getDescription(), info.getAttributes(), info.getConstructors(), MBeanInfoHelper.getMBeanOperationsInfo(ConnectionFactoryControl.class), info.getNotifications());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   private void recreateCF() {
      try {
         this.cf = jmsManager.recreateCF(this.name, this.cfConfig);
      } catch (Exception e) {
         throw new RuntimeException(e.getMessage(), e);
      }
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
