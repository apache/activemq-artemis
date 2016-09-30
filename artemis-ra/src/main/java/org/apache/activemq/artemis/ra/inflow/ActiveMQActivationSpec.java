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

import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;
import javax.resource.ResourceException;
import javax.resource.spi.ActivationSpec;
import javax.resource.spi.InvalidPropertyException;
import javax.resource.spi.ResourceAdapter;
import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import org.apache.activemq.artemis.ra.ActiveMQRALogger;
import org.apache.activemq.artemis.ra.ActiveMQRaUtils;
import org.apache.activemq.artemis.ra.ActiveMQResourceAdapter;
import org.apache.activemq.artemis.ra.ConnectionFactoryProperties;

/**
 * The activation spec
 * These properties are set on the MDB ActivactionProperties
 */
public class ActiveMQActivationSpec extends ConnectionFactoryProperties implements ActivationSpec, Serializable {

   private static final long serialVersionUID = -7997041053897964654L;

   private static final int DEFAULT_MAX_SESSION = 15;

   /**
    * Whether trace is enabled
    */
   private static boolean trace = ActiveMQRALogger.LOGGER.isTraceEnabled();

   public String strConnectorClassName;

   public String strConnectionParameters;

   /**
    * The resource adapter
    */
   private ActiveMQResourceAdapter ra;

   /**
    * The connection factory lookup
    */
   private String connectionFactoryLookup;

   /**
    * The destination
    */
   private String destination;

   /**
    * The destination type
    */
   private String destinationType;

   /**
    * The message selector
    */
   private String messageSelector;

   /**
    * The acknowledgement mode
    */
   private int acknowledgeMode;

   /**
    * The subscription durability
    */
   private boolean subscriptionDurability;

   /**
    * The subscription name
    */
   private String subscriptionName;

   /**
    * If this is true, a durable subscription could be shared by multiple MDB instances
    */
   private boolean shareSubscriptions;

   /**
    * The user
    */
   private String user;

   /**
    * The password
    */
   private String password;

   /**
    * The maximum number of sessions
    */
   private Integer maxSession;

   /**
    * Transaction timeout
    */
   private Integer transactionTimeout;

   private Boolean useJNDI = true;

   private String jndiParams = null;

   private Hashtable<String, String> parsedJndiParams;

   /* use local tx instead of XA*/
   private Boolean localTx;

   // undefined by default, default is specified at the RA level in ActiveMQRAProperties
   private Integer setupAttempts;

   // undefined by default, default is specified at the RA level in ActiveMQRAProperties
   private Long setupInterval;

   private Boolean rebalanceConnections = false;

   /**
    * Constructor
    */
   public ActiveMQActivationSpec() {
      if (ActiveMQActivationSpec.trace) {
         ActiveMQRALogger.LOGGER.trace("constructor()");
      }

      ra = null;
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

   /**
    * Get the resource adapter
    *
    * @return The resource adapter
    */
   @Override
   public ResourceAdapter getResourceAdapter() {
      if (ActiveMQActivationSpec.trace) {
         ActiveMQRALogger.LOGGER.trace("getResourceAdapter()");
      }

      return ra;
   }

   /**
    * @return the useJNDI
    */
   public boolean isUseJNDI() {
      if (useJNDI == null) {
         return ra.isUseJNDI();
      }
      return useJNDI;
   }

   /**
    * @param value the useJNDI to set
    */
   public void setUseJNDI(final boolean value) {
      useJNDI = value;
   }

   /**
    * @return return the jndi params to use
    */
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

   /**
    * Set the resource adapter
    *
    * @param ra The resource adapter
    * @throws ResourceException Thrown if incorrect resource adapter
    */
   @Override
   public void setResourceAdapter(final ResourceAdapter ra) throws ResourceException {
      if (ActiveMQActivationSpec.trace) {
         ActiveMQRALogger.LOGGER.trace("setResourceAdapter(" + ra + ")");
      }

      if (ra == null || !(ra instanceof ActiveMQResourceAdapter)) {
         throw new ResourceException("Resource adapter is " + ra);
      }

      this.ra = (ActiveMQResourceAdapter) ra;
   }

   /**
    * Get the connection factory lookup
    *
    * @return The value
    */
   public String getConnectionFactoryLookup() {
      if (ActiveMQActivationSpec.trace) {
         ActiveMQRALogger.LOGGER.trace("getConnectionFactoryLookup() ->" + connectionFactoryLookup);
      }

      return connectionFactoryLookup;
   }

   /**
    * Set the connection factory lookup
    *
    * @param value The value
    */
   public void setConnectionFactoryLookup(final String value) {
      if (ActiveMQActivationSpec.trace) {
         ActiveMQRALogger.LOGGER.trace("setConnectionFactoryLookup(" + value + ")");
      }

      connectionFactoryLookup = value;
   }

   /**
    * Get the destination
    *
    * @return The value
    */
   public String getDestination() {
      if (ActiveMQActivationSpec.trace) {
         ActiveMQRALogger.LOGGER.trace("getDestination()");
      }

      return destination;
   }

   /**
    * Set the destination
    *
    * @param value The value
    */
   public void setDestination(final String value) {
      if (ActiveMQActivationSpec.trace) {
         ActiveMQRALogger.LOGGER.trace("setDestination(" + value + ")");
      }

      destination = value;
   }

   /**
    * Get the destination lookup
    *
    * @return The value
    */
   public String getDestinationLookup() {
      return getDestination();
   }

   /**
    * Set the destination
    *
    * @param value The value
    */
   public void setDestinationLookup(final String value) {
      setDestination(value);
      setUseJNDI(true);
   }

   /**
    * Get the destination type
    *
    * @return The value
    */
   public String getDestinationType() {
      if (ActiveMQActivationSpec.trace) {
         ActiveMQRALogger.LOGGER.trace("getDestinationType()");
      }

      return destinationType;
   }

   /**
    * Set the destination type
    *
    * @param value The value
    */
   public void setDestinationType(final String value) {
      if (ActiveMQActivationSpec.trace) {
         ActiveMQRALogger.LOGGER.trace("setDestinationType(" + value + ")");
      }

      destinationType = value;
   }

   /**
    * Get the message selector
    *
    * @return The value
    */
   public String getMessageSelector() {
      if (ActiveMQActivationSpec.trace) {
         ActiveMQRALogger.LOGGER.trace("getMessageSelector()");
      }

      return messageSelector;
   }

   /**
    * Set the message selector
    *
    * @param value The value
    */
   public void setMessageSelector(final String value) {
      if (ActiveMQActivationSpec.trace) {
         ActiveMQRALogger.LOGGER.trace("setMessageSelector(" + value + ")");
      }

      messageSelector = value;
   }

   /**
    * Get the acknowledge mode
    *
    * @return The value
    */
   public String getAcknowledgeMode() {
      if (ActiveMQActivationSpec.trace) {
         ActiveMQRALogger.LOGGER.trace("getAcknowledgeMode()");
      }

      if (Session.DUPS_OK_ACKNOWLEDGE == acknowledgeMode) {
         return "Dups-ok-acknowledge";
      } else {
         return "Auto-acknowledge";
      }
   }

   /**
    * Set the acknowledge mode
    *
    * @param value The value
    */
   public void setAcknowledgeMode(final String value) {
      if (ActiveMQActivationSpec.trace) {
         ActiveMQRALogger.LOGGER.trace("setAcknowledgeMode(" + value + ")");
      }

      if ("DUPS_OK_ACKNOWLEDGE".equalsIgnoreCase(value) || "Dups-ok-acknowledge".equalsIgnoreCase(value)) {
         acknowledgeMode = Session.DUPS_OK_ACKNOWLEDGE;
      } else if ("AUTO_ACKNOWLEDGE".equalsIgnoreCase(value) || "Auto-acknowledge".equalsIgnoreCase(value)) {
         acknowledgeMode = Session.AUTO_ACKNOWLEDGE;
      } else {
         throw new IllegalArgumentException("Unsupported acknowledgement mode " + value);
      }
   }

   /**
    * @return the acknowledgement mode
    */
   public int getAcknowledgeModeInt() {
      if (ActiveMQActivationSpec.trace) {
         ActiveMQRALogger.LOGGER.trace("getAcknowledgeMode()");
      }

      return acknowledgeMode;
   }

   /**
    * Get the subscription durability
    *
    * @return The value
    */
   public String getSubscriptionDurability() {
      if (ActiveMQActivationSpec.trace) {
         ActiveMQRALogger.LOGGER.trace("getSubscriptionDurability()");
      }

      if (subscriptionDurability) {
         return "Durable";
      } else {
         return "NonDurable";
      }
   }

   /**
    * Set the subscription durability
    *
    * @param value The value
    */
   public void setSubscriptionDurability(final String value) {
      if (ActiveMQActivationSpec.trace) {
         ActiveMQRALogger.LOGGER.trace("setSubscriptionDurability(" + value + ")");
      }

      subscriptionDurability = "Durable".equals(value);
   }

   /**
    * Get the status of subscription durability
    *
    * @return The value
    */
   public boolean isSubscriptionDurable() {
      if (ActiveMQActivationSpec.trace) {
         ActiveMQRALogger.LOGGER.trace("isSubscriptionDurable()");
      }

      return subscriptionDurability;
   }

   /**
    * Get the subscription name
    *
    * @return The value
    */
   public String getSubscriptionName() {
      if (ActiveMQActivationSpec.trace) {
         ActiveMQRALogger.LOGGER.trace("getSubscriptionName()");
      }

      return subscriptionName;
   }

   /**
    * Set the subscription name
    *
    * @param value The value
    */
   public void setSubscriptionName(final String value) {
      if (ActiveMQActivationSpec.trace) {
         ActiveMQRALogger.LOGGER.trace("setSubscriptionName(" + value + ")");
      }

      subscriptionName = value;
   }

   /**
    * @return the shareDurableSubscriptions
    */
   public boolean isShareSubscriptions() {
      if (ActiveMQActivationSpec.trace) {
         ActiveMQRALogger.LOGGER.trace("isShareSubscriptions() = " + shareSubscriptions);
      }

      return shareSubscriptions;
   }

   /**
    * @param shareSubscriptions the shareDurableSubscriptions to set
    */
   public void setShareSubscriptions(boolean shareSubscriptions) {
      if (ActiveMQActivationSpec.trace) {
         ActiveMQRALogger.LOGGER.trace("setShareSubscriptions(" + shareSubscriptions + ")");
      }

      this.shareSubscriptions = shareSubscriptions;
   }

   /**
    * Get the user
    *
    * @return The value
    */
   public String getUser() {
      if (ActiveMQActivationSpec.trace) {
         ActiveMQRALogger.LOGGER.trace("getUser()");
      }

      if (user == null) {
         return ra.getUserName();
      } else {
         return user;
      }
   }

   /**
    * Set the user
    *
    * @param value The value
    */
   public void setUser(final String value) {
      if (ActiveMQActivationSpec.trace) {
         ActiveMQRALogger.LOGGER.trace("setUser(" + value + ")");
      }

      user = value;
   }

   /**
    * Get the password
    *
    * @return The value
    */
   public String getPassword() {
      if (ActiveMQActivationSpec.trace) {
         ActiveMQRALogger.LOGGER.trace("getPassword()");
      }

      if (password == null) {
         return ra.getPassword();
      } else {
         return password;
      }
   }

   public String getOwnPassword() {
      return password;
   }

   /**
    * Set the password
    *
    * @param value The value
    */
   public void setPassword(final String value) throws Exception {
      if (ActiveMQActivationSpec.trace) {
         ActiveMQRALogger.LOGGER.trace("setPassword(****)");
      }

      password = value;
   }

   /**
    * Get the number of max session
    *
    * @return The value
    */
   public Integer getMaxSession() {
      if (ActiveMQActivationSpec.trace) {
         ActiveMQRALogger.LOGGER.trace("getMaxSession()");
      }

      if (maxSession == null) {
         return DEFAULT_MAX_SESSION;
      }

      return maxSession;
   }

   /**
    * Set the number of max session
    *
    * @param value The value
    */
   public void setMaxSession(final Integer value) {
      if (ActiveMQActivationSpec.trace) {
         ActiveMQRALogger.LOGGER.trace("setMaxSession(" + value + ")");
      }

      maxSession = value;
   }

   /**
    * Get the transaction timeout
    *
    * @return The value
    */
   public Integer getTransactionTimeout() {
      if (ActiveMQActivationSpec.trace) {
         ActiveMQRALogger.LOGGER.trace("getTransactionTimeout()");
      }

      return transactionTimeout;
   }

   /**
    * Set the transaction timeout
    *
    * @param value The value
    */
   public void setTransactionTimeout(final Integer value) {
      if (ActiveMQActivationSpec.trace) {
         ActiveMQRALogger.LOGGER.trace("setTransactionTimeout(" + value + ")");
      }

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

   public boolean isRebalanceConnections() {
      return rebalanceConnections;
   }

   public void setRebalanceConnections(boolean rebalanceConnections) {
      this.rebalanceConnections = rebalanceConnections;
   }

   public int getSetupAttempts() {
      if (ActiveMQActivationSpec.trace) {
         ActiveMQRALogger.LOGGER.trace("getSetupAttempts()");
      }

      if (setupAttempts == null) {
         return ra.getSetupAttempts();
      } else {
         return setupAttempts;
      }
   }

   public void setSetupAttempts(int setupAttempts) {
      if (ActiveMQActivationSpec.trace) {
         ActiveMQRALogger.LOGGER.trace("setSetupAttempts(" + setupAttempts + ")");
      }

      this.setupAttempts = setupAttempts;
   }

   public long getSetupInterval() {
      if (ActiveMQActivationSpec.trace) {
         ActiveMQRALogger.LOGGER.trace("getSetupInterval()");
      }

      if (setupInterval == null) {
         return ra.getSetupInterval();
      } else {
         return setupInterval;
      }
   }

   public void setSetupInterval(long setupInterval) {
      if (ActiveMQActivationSpec.trace) {
         ActiveMQRALogger.LOGGER.trace("setSetupInterval(" + setupInterval + ")");
      }

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
      if (ActiveMQActivationSpec.trace) {
         ActiveMQRALogger.LOGGER.trace("validate()");
      }

      List<String> errorMessages = new ArrayList<>();
      List<PropertyDescriptor> propsNotSet = new ArrayList<>();

      try {
         if (destination == null || destination.trim().equals("")) {
            propsNotSet.add(new PropertyDescriptor("destination", ActiveMQActivationSpec.class));
            errorMessages.add("Destination is mandatory.");
         }

         if (destinationType != null && !Topic.class.getName().equals(destinationType) && !Queue.class.getName().equals(destinationType)) {
            propsNotSet.add(new PropertyDescriptor("destinationType", ActiveMQActivationSpec.class));
            errorMessages.add("If set, the destinationType must be either 'javax.jms.Topic' or 'javax.jms.Queue'.");
         }

         if ((destinationType == null || destinationType.length() == 0 || Topic.class.getName().equals(destinationType)) && isSubscriptionDurable() && (subscriptionName == null || subscriptionName.length() == 0)) {
            propsNotSet.add(new PropertyDescriptor("subscriptionName", ActiveMQActivationSpec.class));
            errorMessages.add("If subscription is durable then subscription name must be specified.");
         }
      } catch (IntrospectionException e) {
         ActiveMQRALogger.LOGGER.unableToValidateProperties(e);
      }

      if (propsNotSet.size() > 0) {
         StringBuffer b = new StringBuffer();
         b.append("Invalid settings:");
         for (String errorMessage : errorMessages) {
            b.append(" ");
            b.append(errorMessage);
         }
         InvalidPropertyException e = new InvalidPropertyException(b.toString());
         final PropertyDescriptor[] descriptors = propsNotSet.toArray(new PropertyDescriptor[propsNotSet.size()]);
         e.setInvalidPropertyDescriptors(descriptors);
         throw e;
      }
   }

   public String getConnectorClassName() {
      return strConnectorClassName;
   }

   public void setConnectorClassName(final String connectorClassName) {
      if (ActiveMQActivationSpec.trace) {
         ActiveMQRALogger.LOGGER.trace("setConnectorClassName(" + connectorClassName + ")");
      }

      strConnectorClassName = connectorClassName;

      setParsedConnectorClassNames(ActiveMQRaUtils.parseConnectorConnectorConfig(connectorClassName));
   }

   /**
    * @return the connectionParameters
    */
   public String getConnectionParameters() {
      return strConnectionParameters;
   }

   public void setConnectionParameters(final String configuration) {
      strConnectionParameters = configuration;
      setParsedConnectionParameters(ActiveMQRaUtils.parseConfig(configuration));
   }

   /**
    * Get a string representation
    *
    * @return The value
    */
   @Override
   public String toString() {
      StringBuffer buffer = new StringBuffer();
      buffer.append(ActiveMQActivationSpec.class.getName()).append('(');
      buffer.append("ra=").append(ra);
      if (connectionFactoryLookup != null) {
         buffer.append(" connectionFactoryLookup=").append(connectionFactoryLookup);
      }
      buffer.append(" destination=").append(destination);
      buffer.append(" destinationType=").append(destinationType);
      if (messageSelector != null) {
         buffer.append(" selector=").append(messageSelector);
      }
      buffer.append(" ack=").append(getAcknowledgeMode());
      buffer.append(" durable=").append(subscriptionDurability);
      buffer.append(" clientID=").append(getClientID());
      if (subscriptionName != null) {
         buffer.append(" subscription=").append(subscriptionName);
      }
      buffer.append(" user=").append(user);
      if (password != null) {
         buffer.append(" password=").append("****");
      }
      buffer.append(" maxSession=").append(maxSession);
      buffer.append(')');
      return buffer.toString();
   }

   // here for backwards compatibility
   public void setUseDLQ(final boolean b) {
   }

   public void setDLQJNDIName(final String name) {
   }

   public void setDLQHandler(final String handler) {
   }

   public void setDLQMaxResent(final int maxResent) {
   }

   public void setProviderAdapterJNDI(final String jndi) {
   }

   /**
    * @param keepAlive the keepAlive to set
    */
   public void setKeepAlive(boolean keepAlive) {
   }

   /**
    * @param keepAliveMillis the keepAliveMillis to set
    */
   public void setKeepAliveMillis(long keepAliveMillis) {
   }

   public void setReconnectInterval(long interval) {
   }

   public void setMinSession(final Integer value) {
   }

   public void setMaxMessages(final Integer value) {
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

      if (acknowledgeMode != that.acknowledgeMode)
         return false;
      if (subscriptionDurability != that.subscriptionDurability)
         return false;
      if (shareSubscriptions != that.shareSubscriptions)
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
      if (transactionTimeout != null ? !transactionTimeout.equals(that.transactionTimeout) : that.transactionTimeout != null)
         return false;
      if (useJNDI != null ? !useJNDI.equals(that.useJNDI) : that.useJNDI != null)
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
      result = 31 * result + (shareSubscriptions ? 1 : 0);
      result = 31 * result + (user != null ? user.hashCode() : 0);
      result = 31 * result + (password != null ? password.hashCode() : 0);
      result = 31 * result + (maxSession != null ? maxSession.hashCode() : 0);
      result = 31 * result + (transactionTimeout != null ? transactionTimeout.hashCode() : 0);
      result = 31 * result + (useJNDI != null ? useJNDI.hashCode() : 0);
      result = 31 * result + (jndiParams != null ? jndiParams.hashCode() : 0);
      result = 31 * result + (parsedJndiParams != null ? parsedJndiParams.hashCode() : 0);
      result = 31 * result + (localTx != null ? localTx.hashCode() : 0);
      result = 31 * result + (rebalanceConnections != null ? rebalanceConnections.hashCode() : 0);
      result = 31 * result + (setupAttempts != null ? setupAttempts.hashCode() : 0);
      result = 31 * result + (setupInterval != null ? setupInterval.hashCode() : 0);
      return result;
   }
}
