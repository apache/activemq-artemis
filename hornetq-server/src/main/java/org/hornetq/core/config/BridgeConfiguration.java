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
package org.hornetq.core.config;

import java.io.Serializable;
import java.util.List;

import org.hornetq.api.core.client.HornetQClient;

/**
 * A BridgeConfiguration
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public final class BridgeConfiguration implements Serializable
{
   private static final long serialVersionUID = -1057244274380572226L;

   private String name;

   private String queueName;

   private String forwardingAddress;

   private String filterString;

   private List<String> staticConnectors;

   private String discoveryGroupName;

   private boolean ha = false;

   private String transformerClassName = null;

   private long retryInterval = HornetQClient.DEFAULT_RETRY_INTERVAL;

   private double retryIntervalMultiplier = HornetQClient.DEFAULT_RETRY_INTERVAL;

   private int initialConnectAttempts = -1;

   private int reconnectAttempts = -1;

   private int reconnectAttemptsOnSameNode = 10;

   private boolean useDuplicateDetection = true;

   private int confirmationWindowSize = HornetQClient.DEFAULT_CONFIRMATION_WINDOW_SIZE;

   private long clientFailureCheckPeriod = HornetQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD;

   private String user;

   private String password;

   private long connectionTTL = HornetQClient.DEFAULT_CONNECTION_TTL;

   private long maxRetryInterval = HornetQClient.DEFAULT_MAX_RETRY_INTERVAL;

   private int minLargeMessageSize = HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE;

   // At this point this is only changed on testcases
   // The bridge shouldn't be sending blocking anyways
   private long callTimeout = HornetQClient.DEFAULT_CALL_TIMEOUT;


   /**
    * A default constructor for embedded users or testcases to setup defaults
    */
   public BridgeConfiguration()
   {
   }

   public BridgeConfiguration(final String name,
                              final String queueName,
                              final String forwardingAddress,
                              final String filterString,
                              final String transformerClassName,
                              final int minLargeMessageSize,
                              final long clientFailureCheckPeriod,
                              final long connectionTTL,
                              final long retryInterval,
                              final long maxRetryInterval,
                              final double retryIntervalMultiplier,
                              final int initialConnectAttempts,
                              final int reconnectAttempts,
                              final int reconnectAttemptsOnSameNode,
                              final boolean useDuplicateDetection,
                              final int confirmationWindowSize,
                              final List<String> staticConnectors,
                              final boolean ha,
                              final String user,
                              final String password)
   {
      this.name = name;
      this.queueName = queueName;
      this.forwardingAddress = forwardingAddress;
      this.minLargeMessageSize = minLargeMessageSize;
      this.filterString = filterString;
      this.transformerClassName = transformerClassName;
      this.retryInterval = retryInterval;
      this.retryIntervalMultiplier = retryIntervalMultiplier;
      this.initialConnectAttempts = initialConnectAttempts;
      this.reconnectAttempts = reconnectAttempts;
      this.reconnectAttemptsOnSameNode = reconnectAttemptsOnSameNode;
      this.useDuplicateDetection = useDuplicateDetection;
      this.confirmationWindowSize = confirmationWindowSize;
      this.clientFailureCheckPeriod = clientFailureCheckPeriod;
      this.staticConnectors = staticConnectors;
      this.user = user;
      this.password = password;
      this.connectionTTL = connectionTTL;
      this.maxRetryInterval = maxRetryInterval;
      this.ha = ha;
      discoveryGroupName = null;
   }

   public BridgeConfiguration(final String name,
                              final String queueName,
                              final String forwardingAddress,
                              final String filterString,
                              final String transformerClassName,
                              final int minLargeMessageSize,
                              final long clientFailureCheckPeriod,
                              final long connectionTTL,
                              final long retryInterval,
                              final long maxRetryInterval,
                              final double retryIntervalMultiplier,
                              final int initialConnectAttempts,
                              final int reconnectAttempts,
                              final int reconnectAttemptsOnSameNode,
                              final boolean useDuplicateDetection,
                              final int confirmationWindowSize,
                              final String discoveryGroupName,
                              final boolean ha,
                              final String user,
                              final String password)
   {
      this.name = name;
      this.queueName = queueName;
      this.forwardingAddress = forwardingAddress;
      this.filterString = filterString;
      this.transformerClassName = transformerClassName;
      this.minLargeMessageSize = minLargeMessageSize;
      this.retryInterval = retryInterval;
      this.retryIntervalMultiplier = retryIntervalMultiplier;
      this.initialConnectAttempts = initialConnectAttempts;
      this.reconnectAttempts = reconnectAttempts;
      this.reconnectAttemptsOnSameNode = reconnectAttemptsOnSameNode;
      this.useDuplicateDetection = useDuplicateDetection;
      this.confirmationWindowSize = confirmationWindowSize;
      this.clientFailureCheckPeriod = clientFailureCheckPeriod;
      this.staticConnectors = null;
      this.discoveryGroupName = discoveryGroupName;
      this.ha = ha;
      this.user = user;
      this.password = password;
      this.connectionTTL = connectionTTL;
      this.maxRetryInterval = maxRetryInterval;
   }

   public String getName()
   {
      return name;
   }

   /**
    * @param name the name to set
    */
   public void setName(final String name)
   {
      this.name = name;
   }

   public String getQueueName()
   {
      return queueName;
   }

   /**
    * @param queueName the queueName to set
    */
   public void setQueueName(final String queueName)
   {
      this.queueName = queueName;
   }

   /**
    * @return the connectionTTL
    */
   public long getConnectionTTL()
   {
      return connectionTTL;
   }

   public void setConnectionTTL(long connectionTTL)
   {
      this.connectionTTL = connectionTTL;
   }

   /**
    * @return the maxRetryInterval
    */
   public long getMaxRetryInterval()
   {
      return maxRetryInterval;
   }

   public void setMaxRetryInterval(long maxRetryInterval)
   {
      this.maxRetryInterval = maxRetryInterval;
   }

   public String getForwardingAddress()
   {
      return forwardingAddress;
   }

   /**
    * @param forwardingAddress the forwardingAddress to set
    */
   public void setForwardingAddress(final String forwardingAddress)
   {
      this.forwardingAddress = forwardingAddress;
   }

   public String getFilterString()
   {
      return filterString;
   }

   /**
    * @param filterString the filterString to set
    */
   public void setFilterString(final String filterString)
   {
      this.filterString = filterString;
   }

   public String getTransformerClassName()
   {
      return transformerClassName;
   }

   /**
    * @param transformerClassName the transformerClassName to set
    */
   public void setTransformerClassName(final String transformerClassName)
   {
      this.transformerClassName = transformerClassName;
   }

   public List<String> getStaticConnectors()
   {
      return staticConnectors;
   }

   /**
    * @param staticConnectors the staticConnectors to set
    */
   public void setStaticConnectors(final List<String> staticConnectors)
   {
      this.staticConnectors = staticConnectors;
   }

   public String getDiscoveryGroupName()
   {
      return discoveryGroupName;
   }

   /**
    * @param discoveryGroupName the discoveryGroupName to set
    */
   public void setDiscoveryGroupName(final String discoveryGroupName)
   {
      this.discoveryGroupName = discoveryGroupName;
   }

   public boolean isHA()
   {
      return ha;
   }

   /**
    *
    * @param ha is the bridge supporting HA?
    */
   public void setHA(final boolean ha)
   {
      this.ha = ha;
   }

   public long getRetryInterval()
   {
      return retryInterval;
   }

   /**
    * @param retryInterval the retryInterval to set
    */
   public void setRetryInterval(final long retryInterval)
   {
      this.retryInterval = retryInterval;
   }

   public double getRetryIntervalMultiplier()
   {
      return retryIntervalMultiplier;
   }

   /**
    * @param retryIntervalMultiplier the retryIntervalMultiplier to set
    */
   public void setRetryIntervalMultiplier(final double retryIntervalMultiplier)
   {
      this.retryIntervalMultiplier = retryIntervalMultiplier;
   }

   public int getInitialConnectAttempts()
   {
      return initialConnectAttempts;
   }

   /**
    * @param initialConnectAttempts the initialConnectAttempts to set
    */
   public void setInitialConnectAttempts(final int initialConnectAttempts)
   {
      this.initialConnectAttempts = initialConnectAttempts;
   }

   public int getReconnectAttempts()
   {
      return reconnectAttempts;
   }

   /**
    * @param reconnectAttempts the reconnectAttempts to set
    */
   public void setReconnectAttempts(final int reconnectAttempts)
   {
      this.reconnectAttempts = reconnectAttempts;
   }

   public boolean isUseDuplicateDetection()
   {
      return useDuplicateDetection;
   }

   /**
    * @param useDuplicateDetection the useDuplicateDetection to set
    */
   public void setUseDuplicateDetection(final boolean useDuplicateDetection)
   {
      this.useDuplicateDetection = useDuplicateDetection;
   }

   public int getConfirmationWindowSize()
   {
      return confirmationWindowSize;
   }

   /**
    * @param confirmationWindowSize the confirmationWindowSize to set
    */
   public void setConfirmationWindowSize(final int confirmationWindowSize)
   {
      this.confirmationWindowSize = confirmationWindowSize;
   }

   public long getClientFailureCheckPeriod()
   {
      return clientFailureCheckPeriod;
   }

   public void setClientFailureCheckPeriod(long clientFailureCheckPeriod)
   {
      this.clientFailureCheckPeriod = clientFailureCheckPeriod;
   }

   /**
    * @return the minLargeMessageSize
    */
   public int getMinLargeMessageSize()
   {
      return minLargeMessageSize;
   }

   public void setMinLargeMessageSize(int minLargeMessageSize)
   {
      this.minLargeMessageSize = minLargeMessageSize;
   }

   public String getUser()
   {
      return user;
   }

   public void setUser(String user)
   {
      this.user = user;
   }

   public String getPassword()
   {
      return password;
   }

   public void setPassword(String password)
   {
      this.password = password;
   }

   /**
    * @return the callTimeout
    */
   public long getCallTimeout()
   {
      return callTimeout;
   }

   public int getReconnectAttemptsOnSameNode()
   {
      return reconnectAttemptsOnSameNode;
   }

   public void setReconnectAttemptsOnSameNode(int reconnectAttemptsOnSameNode)
   {
      this.reconnectAttemptsOnSameNode = reconnectAttemptsOnSameNode;
   }

   /**
    *
    * At this point this is only changed on testcases
    * The bridge shouldn't be sending blocking anyways
    * @param callTimeout the callTimeout to set
    */
   public void setCallTimeout(long callTimeout)
   {
      this.callTimeout = callTimeout;
   }

   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = 1;
      result = prime * result + (int)(callTimeout ^ (callTimeout >>> 32));
      result = prime * result + (int)(clientFailureCheckPeriod ^ (clientFailureCheckPeriod >>> 32));
      result = prime * result + confirmationWindowSize;
      result = prime * result + (int)(connectionTTL ^ (connectionTTL >>> 32));
      result = prime * result + ((discoveryGroupName == null) ? 0 : discoveryGroupName.hashCode());
      result = prime * result + ((filterString == null) ? 0 : filterString.hashCode());
      result = prime * result + ((forwardingAddress == null) ? 0 : forwardingAddress.hashCode());
      result = prime * result + (ha ? 1231 : 1237);
      result = prime * result + (int)(maxRetryInterval ^ (maxRetryInterval >>> 32));
      result = prime * result + minLargeMessageSize;
      result = prime * result + ((name == null) ? 0 : name.hashCode());
      result = prime * result + ((password == null) ? 0 : password.hashCode());
      result = prime * result + ((queueName == null) ? 0 : queueName.hashCode());
      result = prime * result + initialConnectAttempts;
      result = prime * result + reconnectAttempts;
      result = prime * result + (int)(retryInterval ^ (retryInterval >>> 32));
      long temp;
      temp = Double.doubleToLongBits(retryIntervalMultiplier);
      result = prime * result + (int)(temp ^ (temp >>> 32));
      result = prime * result + ((staticConnectors == null) ? 0 : staticConnectors.hashCode());
      result = prime * result + ((transformerClassName == null) ? 0 : transformerClassName.hashCode());
      result = prime * result + (useDuplicateDetection ? 1231 : 1237);
      result = prime * result + ((user == null) ? 0 : user.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj)
   {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      BridgeConfiguration other = (BridgeConfiguration)obj;
      if (callTimeout != other.callTimeout)
         return false;
      if (clientFailureCheckPeriod != other.clientFailureCheckPeriod)
         return false;
      if (confirmationWindowSize != other.confirmationWindowSize)
         return false;
      if (connectionTTL != other.connectionTTL)
         return false;
      if (discoveryGroupName == null)
      {
         if (other.discoveryGroupName != null)
            return false;
      }
      else if (!discoveryGroupName.equals(other.discoveryGroupName))
         return false;
      if (filterString == null)
      {
         if (other.filterString != null)
            return false;
      }
      else if (!filterString.equals(other.filterString))
         return false;
      if (forwardingAddress == null)
      {
         if (other.forwardingAddress != null)
            return false;
      }
      else if (!forwardingAddress.equals(other.forwardingAddress))
         return false;
      if (ha != other.ha)
         return false;
      if (maxRetryInterval != other.maxRetryInterval)
         return false;
      if (minLargeMessageSize != other.minLargeMessageSize)
         return false;
      if (name == null)
      {
         if (other.name != null)
            return false;
      }
      else if (!name.equals(other.name))
         return false;
      if (password == null)
      {
         if (other.password != null)
            return false;
      }
      else if (!password.equals(other.password))
         return false;
      if (queueName == null)
      {
         if (other.queueName != null)
            return false;
      }
      else if (!queueName.equals(other.queueName))
         return false;
      if (initialConnectAttempts != other.initialConnectAttempts)
         return false;
      if (reconnectAttempts != other.reconnectAttempts)
         return false;
      if (retryInterval != other.retryInterval)
         return false;
      if (Double.doubleToLongBits(retryIntervalMultiplier) != Double.doubleToLongBits(other.retryIntervalMultiplier))
         return false;
      if (staticConnectors == null)
      {
         if (other.staticConnectors != null)
            return false;
      }
      else if (!staticConnectors.equals(other.staticConnectors))
         return false;
      if (transformerClassName == null)
      {
         if (other.transformerClassName != null)
            return false;
      }
      else if (!transformerClassName.equals(other.transformerClassName))
         return false;
      if (useDuplicateDetection != other.useDuplicateDetection)
         return false;
      if (user == null)
      {
         if (other.user != null)
            return false;
      }
      else if (!user.equals(other.user))
         return false;
      return true;
   }

}
