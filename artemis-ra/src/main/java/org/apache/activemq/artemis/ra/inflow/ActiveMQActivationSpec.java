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
package org.apache.activemq.artemis.ra.inflow;

import javax.jms.Session;
import javax.resource.ResourceException;
import javax.resource.spi.ActivationSpec;
import javax.resource.spi.InvalidPropertyException;
import javax.resource.spi.ResourceAdapter;
import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.Hashtable;

import org.apache.activemq.artemis.ra.ActiveMQRALogger;
import org.apache.activemq.artemis.ra.ActiveMQRaUtils;
import org.apache.activemq.artemis.ra.ActiveMQResourceAdapter;
import org.apache.activemq.artemis.ra.ConnectionFactoryProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * These properties are set on the MDB ActivationProperties
 */
public class ActiveMQActivationSpec extends ConnectionFactoryProperties implements ActivationSpec, Serializable {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private static final long serialVersionUID = -7997041053897964654L;

   private static final int DEFAULT_MAX_SESSION = 15;

   private static final boolean DEFAULT_SINGLE_CONNECTION = false;

   public String strConnectorClassName;

   public String strConnectionParameters;
   protected Boolean allowLocalTransactions;


   private ActiveMQResourceAdapter ra;

   private String connectionFactoryLookup;

   private String destination;

   private String destinationType;

   private String messageSelector;

   private Integer acknowledgeMode;

   private Boolean subscriptionDurability;

   private String subscriptionName;

   /**
    * If this is true, a durable subscription could be shared by multiple MDB instances
    */
   private Boolean shareSubscriptions = false;

   private String user;

   private String password;

   private Integer maxSession;

   private Boolean singleConnection = false;

   @Deprecated(forRemoval = true)
   private Integer transactionTimeout;

   private Boolean useJNDI = true;

   private String jndiParams = null;

   private Hashtable<String, String> parsedJndiParams;

   /**
    *  use local tx instead of XA
    */
   private Boolean localTx;

   /**
    * undefined by default, default is specified at the RA level in ActiveMQRAProperties
    */
   private Integer setupAttempts;

   /**
    * undefined by default, default is specified at the RA level in ActiveMQRAProperties
    */
   private Long setupInterval;

   private Boolean rebalanceConnections = false;

   /**
    * Enables backwards compatibility of the pre 2.x addressing model
    */
   private String topicPrefix;

   private String queuePrefix;

   public ActiveMQActivationSpec() {
      logger.trace("constructor()");

      // we create an Adapter here but only for Application Servers that do introspection on loading to avoid an NPE
      ra = new ActiveMQResourceAdapter();
      destination = null;
      destinationType = null;
      messageSelector = null;
      acknowledgeMode = Session.AUTO_ACKNOWLEDGE;
      subscriptionDurability = false;
      subscriptionName = null;
      user = null;
      password = null;
      maxSession = DEFAULT_MAX_SESSION;
      transactionTimeout = 0;
   }

   @Override
   public ResourceAdapter getResourceAdapter() {
      logger.trace("getResourceAdapter()");

      return ra;
   }

   public Boolean isUseJNDI() {
      if (useJNDI == null) {
         return ra.isUseJNDI();
      }
      return useJNDI;
   }

   public void setUseJNDI(final Boolean value) {
      useJNDI = value;
   }

   public String getJndiParams() {
      if (jndiParams == null) {
         return ra.getJndiParams();
      }
      return jndiParams;
   }

   public void setJndiParams(String jndiParams) {
      this.jndiParams = jndiParams;
      parsedJndiParams = ActiveMQRaUtils.parseHashtableConfig(jndiParams);
   }

   public Hashtable<?, ?> getParsedJndiParams() {
      if (parsedJndiParams == null) {
         return ra.getParsedJndiParams();
      }
      return parsedJndiParams;
   }

   @Override
   public void setResourceAdapter(final ResourceAdapter ra) throws ResourceException {
      logger.trace("setResourceAdapter({})", ra);

      if (ra == null || !(ra instanceof ActiveMQResourceAdapter)) {
         throw new ResourceException("Resource adapter is " + ra);
      }

      this.ra = (ActiveMQResourceAdapter) ra;
   }

   public String getConnectionFactoryLookup() {
      logger.trace("getConnectionFactoryLookup() ->{}", connectionFactoryLookup);

      return connectionFactoryLookup;
   }

   public void setConnectionFactoryLookup(final String value) {
      logger.trace("setConnectionFactoryLookup({})", value);

      connectionFactoryLookup = value;
   }

   public String getDestination() {
      logger.trace("getDestination()");

      return destination;
   }

   public void setDestination(final String value) {
      logger.trace("setDestination({})", value);

      destination = value;
   }

   public String getDestinationLookup() {
      return getDestination();
   }

   public void setDestinationLookup(final String value) {
      setDestination(value);
      setUseJNDI(true);
   }

   public String getDestinationType() {
      logger.trace("getDestinationType()");

      return destinationType;
   }

   public void setDestinationType(final String value) {
      logger.trace("setDestinationType({})", value);

      destinationType = value;
   }

   public String getMessageSelector() {
      logger.trace("getMessageSelector()");

      return messageSelector;
   }

   public void setMessageSelector(final String value) {
      logger.trace("setMessageSelector({})", value);

      messageSelector = value;
   }

   public String getAcknowledgeMode() {
      logger.trace("getAcknowledgeMode()");

      if (Session.DUPS_OK_ACKNOWLEDGE == acknowledgeMode) {
         return "Dups-ok-acknowledge";
      } else {
         return "Auto-acknowledge";
      }
   }

   public void setQueuePrefix(String prefix) {
      this.queuePrefix = prefix;
   }

   public String getQueuePrefix() {
      return queuePrefix;
   }

   public void setTopicPrefix(String prefix) {
      this.topicPrefix = prefix;
   }

   public String getTopicPrefix() {
      return topicPrefix;
   }

   public void setAcknowledgeMode(final String value) {
      logger.trace("setAcknowledgeMode({})", value);

      try {
         this.acknowledgeMode = ActiveMQActivationValidationUtils.validateAcknowledgeMode(value);
      } catch (IllegalArgumentException e) {
         ActiveMQRALogger.LOGGER.invalidAcknowledgementMode(value);
         throw e;
      }
   }

   public Integer getAcknowledgeModeInt() {
      logger.trace("getAcknowledgeMode()");

      return acknowledgeMode;
   }

   public String getSubscriptionDurability() {
      logger.trace("getSubscriptionDurability()");

      if (subscriptionDurability) {
         return "Durable";
      } else {
         return "NonDurable";
      }
   }

   public void setSubscriptionDurability(final String value) {
      logger.trace("setSubscriptionDurability({})", value);

      subscriptionDurability = "Durable".equals(value);
   }

   public Boolean isSubscriptionDurable() {
      logger.trace("isSubscriptionDurable()");

      return subscriptionDurability;
   }

   public String getSubscriptionName() {
      logger.trace("getSubscriptionName()");

      return subscriptionName;
   }

   public void setSubscriptionName(final String value) {
      logger.trace("setSubscriptionName({})", value);

      subscriptionName = value;
   }

   public Boolean isShareSubscriptions() {
      logger.trace("isShareSubscriptions() = {}", shareSubscriptions);

      return shareSubscriptions;
   }

   public void setShareSubscriptions(final Boolean shareSubscriptions) {
      logger.trace("setShareSubscriptions({})", shareSubscriptions);

      this.shareSubscriptions = shareSubscriptions;
   }

   public String getUser() {
      logger.trace("getUser()");

      if (user == null) {
         return ra.getUserName();
      } else {
         return user;
      }
   }

   public void setUser(final String value) {
      logger.trace("setUser()", value);

      user = value;
   }

   public String getUserName() {
      if (logger.isTraceEnabled()) {
         logger.trace("getUserName()");
      }

      if (user == null) {
         return ra.getUserName();
      } else {
         return user;
      }
   }

   public void setUserName(final String value) {
      if (logger.isTraceEnabled()) {
         logger.trace("setUserName(" + value + ")");
      }

      user = value;
   }

   public String getPassword() {
      logger.trace("getPassword()");

      if (password == null) {
         return ra.getPassword();
      } else {
         return password;
      }
   }

   public String getOwnPassword() {
      return password;
   }

   public void setPassword(final String value) throws Exception {
      logger.trace("setPassword(****)");

      password = value;
   }

   public Integer getMaxSession() {
      logger.trace("getMaxSession()");

      if (maxSession == null) {
         return DEFAULT_MAX_SESSION;
      }

      return maxSession;
   }

   public void setMaxSession(final Integer value) {
      logger.trace("setMaxSession({})", value);

      if (value < 1) {
         maxSession = 1;
         ActiveMQRALogger.LOGGER.invalidNumberOfMaxSession(value, maxSession);
      } else
         maxSession = value;
   }

   public Boolean isSingleConnection() {
      logger.trace("getSingleConnection()");

      if (singleConnection == null) {
         return DEFAULT_SINGLE_CONNECTION;
      }

      return singleConnection;
   }

   public void setSingleConnection(final Boolean value) {
      logger.trace("setSingleConnection({})", value);

      singleConnection = value;
   }

   @Deprecated(forRemoval = true)
   public Integer getTransactionTimeout() {
      logger.trace("getTransactionTimeout()");

      return transactionTimeout;
   }

   @Deprecated(forRemoval = true)
   public void setTransactionTimeout(final Integer value) {
      logger.trace("setTransactionTimeout({})", value);

      transactionTimeout = value;
   }

   public Boolean isUseLocalTx() {
      if (localTx == null) {
         return ra.getUseLocalTx();
      } else {
         return localTx;
      }
   }

   public void setUseLocalTx(final Boolean localTx) {
      this.localTx = localTx;
   }

   public Boolean getRebalanceConnections() {
      return rebalanceConnections;
   }

   public Boolean isRebalanceConnections() {
      return rebalanceConnections;
   }

   public void setRebalanceConnections(final Boolean rebalanceConnections) {
      this.rebalanceConnections = rebalanceConnections;
   }

   public Integer getSetupAttempts() {
      logger.trace("getSetupAttempts()");

      if (setupAttempts == null) {
         return ra.getSetupAttempts();
      } else {
         return setupAttempts;
      }
   }

   public void setSetupAttempts(final Integer setupAttempts) {
      logger.trace("setSetupAttempts({})", setupAttempts);

      this.setupAttempts = setupAttempts;
   }

   public Long getSetupInterval() {
      logger.trace("getSetupInterval()");

      if (setupInterval == null) {
         return ra.getSetupInterval();
      } else {
         return setupInterval;
      }
   }

   public void setSetupInterval(final Long setupInterval) {
      logger.trace("setSetupInterval({})", setupInterval);

      this.setupInterval = setupInterval;
   }

   // ARTEMIS-399 - support both "clientId" and "clientID" activation config properties
   public void setClientId(String clientId) {
      setClientID(clientId);
   }

   /**
    * Validate
    *
    * @throws InvalidPropertyException Thrown if a validation exception occurs
    */
   @Override
   public void validate() throws InvalidPropertyException {
      logger.trace("validate()");

      ActiveMQActivationValidationUtils.validate(destination, destinationType, isSubscriptionDurable(), subscriptionName);
   }

   public String getConnectorClassName() {
      return strConnectorClassName;
   }

   public void setConnectorClassName(final String connectorClassName) {
      logger.trace("setConnectorClassName({})", connectorClassName);

      strConnectorClassName = connectorClassName;

      setParsedConnectorClassNames(ActiveMQRaUtils.parseConnectorConnectorConfig(connectorClassName));
   }

   public String getConnectionParameters() {
      return strConnectionParameters;
   }

   public void setConnectionParameters(final String configuration) {
      strConnectionParameters = configuration;
      setParsedConnectionParameters(ActiveMQRaUtils.parseConfig(configuration));
   }

   @Override
   public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(ActiveMQActivationSpec.class.getName()).append('(');
      sb.append("ra=").append(ra);
      if (connectionFactoryLookup != null) {
         sb.append(" connectionFactoryLookup=").append(connectionFactoryLookup);
      }
      sb.append(" destination=").append(destination);
      sb.append(" destinationType=").append(destinationType);
      if (messageSelector != null) {
         sb.append(" selector=").append(messageSelector);
      }
      sb.append(" ack=").append(getAcknowledgeMode());
      sb.append(" durable=").append(subscriptionDurability);
      sb.append(" clientID=").append(getClientID());
      if (subscriptionName != null) {
         sb.append(" subscription=").append(subscriptionName);
      }
      sb.append(" user=").append(user);
      if (password != null) {
         sb.append(" password=").append("****");
      }
      sb.append(" maxSession=").append(maxSession);
      sb.append(')');
      return sb.toString();
   }

   // here for backwards compatibility
   public void setUseDLQ(final Boolean b) {
   }

   public void setDLQJNDIName(final String name) {
   }

   public void setDLQHandler(final String handler) {
   }

   public void setDLQMaxResent(final Integer maxResent) {
   }

   public void setProviderAdapterJNDI(final String jndi) {
   }

   public void setKeepAlive(Boolean keepAlive) {
   }

   public void setKeepAliveMillis(Long keepAliveMillis) {
   }

   public void setReconnectInterval(Long interval) {
   }

   public void setMinSession(final Integer value) {
   }

   public void setMaxMessages(final Integer value) {
   }

   public Boolean isAllowLocalTransactions() {
      return allowLocalTransactions;
   }

   public void setAllowLocalTransactions(final Boolean allowLocalTransactions) {
      this.allowLocalTransactions = allowLocalTransactions;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o)
         return true;
      if (o == null || getClass() != o.getClass())
         return false;
      if (!super.equals(o))
         return false;

      ActiveMQActivationSpec that = (ActiveMQActivationSpec) o;

      if (acknowledgeMode != null ? !acknowledgeMode.equals(that.acknowledgeMode) : that.acknowledgeMode != null)
         return false;
      if (subscriptionDurability != null ? !subscriptionDurability.equals(that.subscriptionDurability) : that.subscriptionDurability != null)
         return false;
      if (shareSubscriptions != null ? !shareSubscriptions.equals(that.shareSubscriptions) : that.shareSubscriptions != null)
         return false;
      if (strConnectorClassName != null ? !strConnectorClassName.equals(that.strConnectorClassName) : that.strConnectorClassName != null)
         return false;
      if (strConnectionParameters != null ? !strConnectionParameters.equals(that.strConnectionParameters) : that.strConnectionParameters != null)
         return false;
      if (ra != null ? !ra.equals(that.ra) : that.ra != null)
         return false;
      if (connectionFactoryLookup != null ? !connectionFactoryLookup.equals(that.connectionFactoryLookup) : that.connectionFactoryLookup != null)
         return false;
      if (destination != null ? !destination.equals(that.destination) : that.destination != null)
         return false;
      if (destinationType != null ? !destinationType.equals(that.destinationType) : that.destinationType != null)
         return false;
      if (messageSelector != null ? !messageSelector.equals(that.messageSelector) : that.messageSelector != null)
         return false;
      if (subscriptionName != null ? !subscriptionName.equals(that.subscriptionName) : that.subscriptionName != null)
         return false;
      if (user != null ? !user.equals(that.user) : that.user != null)
         return false;
      if (password != null ? !password.equals(that.password) : that.password != null)
         return false;
      if (maxSession != null ? !maxSession.equals(that.maxSession) : that.maxSession != null)
         return false;
      if (useJNDI != null ? !useJNDI.equals(that.useJNDI) : that.useJNDI != null)
         return false;
      if (transactionTimeout != null ? !transactionTimeout.equals(that.transactionTimeout) : that.transactionTimeout != null)
         return false;
      if (singleConnection != null ? !singleConnection.equals(that.singleConnection) : that.singleConnection != null)
         return false;
      if (jndiParams != null ? !jndiParams.equals(that.jndiParams) : that.jndiParams != null)
         return false;
      if (parsedJndiParams != null ? !parsedJndiParams.equals(that.parsedJndiParams) : that.parsedJndiParams != null)
         return false;
      if (localTx != null ? !localTx.equals(that.localTx) : that.localTx != null)
         return false;
      if (rebalanceConnections != null ? !rebalanceConnections.equals(that.rebalanceConnections) : that.rebalanceConnections != null)
         return false;
      if (setupAttempts != null ? !setupAttempts.equals(that.setupAttempts) : that.setupAttempts != null)
         return false;
      if (queuePrefix != null ? !queuePrefix.equals(that.queuePrefix) : that.queuePrefix != null)
         return false;
      if (topicPrefix != null ? !topicPrefix.equals(that.topicPrefix) : that.topicPrefix != null)
         return false;
      return !(setupInterval != null ? !setupInterval.equals(that.setupInterval) : that.setupInterval != null);

   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (strConnectorClassName != null ? strConnectorClassName.hashCode() : 0);
      result = 31 * result + (strConnectionParameters != null ? strConnectionParameters.hashCode() : 0);
      result = 31 * result + (ra != null ? ra.hashCode() : 0);
      result = 31 * result + (connectionFactoryLookup != null ? connectionFactoryLookup.hashCode() : 0);
      result = 31 * result + (destination != null ? destination.hashCode() : 0);
      result = 31 * result + (destinationType != null ? destinationType.hashCode() : 0);
      result = 31 * result + (messageSelector != null ? messageSelector.hashCode() : 0);
      result = 31 * result + acknowledgeMode;
      result = 31 * result + (subscriptionDurability ? 1 : 0);
      result = 31 * result + (subscriptionName != null ? subscriptionName.hashCode() : 0);
      result = 31 * result + (shareSubscriptions != null && shareSubscriptions ? 1 : 0);
      result = 31 * result + (user != null ? user.hashCode() : 0);
      result = 31 * result + (password != null ? password.hashCode() : 0);
      result = 31 * result + (maxSession != null ? maxSession.hashCode() : 0);
      result = 31 * result + (singleConnection != null ? singleConnection.hashCode() : 0);
      result = 31 * result + (transactionTimeout != null ? transactionTimeout.hashCode() : 0);
      result = 31 * result + (useJNDI != null ? useJNDI.hashCode() : 0);
      result = 31 * result + (jndiParams != null ? jndiParams.hashCode() : 0);
      result = 31 * result + (parsedJndiParams != null ? parsedJndiParams.hashCode() : 0);
      result = 31 * result + (localTx != null ? localTx.hashCode() : 0);
      result = 31 * result + (rebalanceConnections != null ? rebalanceConnections.hashCode() : 0);
      result = 31 * result + (setupAttempts != null ? setupAttempts.hashCode() : 0);
      result = 31 * result + (setupInterval != null ? setupInterval.hashCode() : 0);
      result = 31 * result + (queuePrefix != null ? queuePrefix.hashCode() : 0);
      result = 31 * result + (topicPrefix != null ? topicPrefix.hashCode() : 0);
      return result;
   }
}
