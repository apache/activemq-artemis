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
import java.util.Collections;
import java.util.List;

import org.hornetq.api.config.HornetQDefaultConfiguration;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.core.server.HornetQMessageBundle;

/**
 * A ClusterConnectionConfiguration
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public final class ClusterConnectionConfiguration implements Serializable
{
   private static final long serialVersionUID = 8948303813427795935L;

   private final String name;

   private final String address;

   private final String connectorName;

   private long clientFailureCheckPeriod;

   private long connectionTTL;

   private long retryInterval;

   private double retryIntervalMultiplier;

   private long maxRetryInterval;

   private int initialConnectAttempts;

   private int reconnectAttempts;

   private long callTimeout;

   private long callFailoverTimeout;

   private boolean duplicateDetection;

   private boolean forwardWhenNoConsumers;

   private final List<String> staticConnectors;

   private final String discoveryGroupName;

   private final int maxHops;

   private final int confirmationWindowSize;

   private final boolean allowDirectConnectionsOnly;

   private int minLargeMessageSize;

   private final long clusterNotificationInterval;

   private final int clusterNotificationAttempts;

   public ClusterConnectionConfiguration(final String name,
                                         final String address,
                                         final String connectorName,
                                         final long retryInterval,
                                         final boolean duplicateDetection,
                                         final boolean forwardWhenNoConsumers,
                                         final int maxHops,
                                         final int confirmationWindowSize,
                                         final List<String> staticConnectors,
                                         final boolean allowDirectConnectionsOnly)
   {
      this(name,
         address,
         connectorName,
         HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
         HornetQDefaultConfiguration.getDefaultClusterFailureCheckPeriod(),
         HornetQDefaultConfiguration.getDefaultClusterConnectionTtl(),
         retryInterval,
         HornetQDefaultConfiguration.getDefaultClusterRetryIntervalMultiplier(),
         HornetQDefaultConfiguration.getDefaultClusterMaxRetryInterval(),
         HornetQDefaultConfiguration.getDefaultClusterInitialConnectAttempts(),
         HornetQDefaultConfiguration.getDefaultClusterReconnectAttempts(),
         HornetQClient.DEFAULT_CALL_TIMEOUT,
         HornetQClient.DEFAULT_CALL_FAILOVER_TIMEOUT,
         duplicateDetection,
         forwardWhenNoConsumers,
         maxHops,
         confirmationWindowSize,
         staticConnectors,
         allowDirectConnectionsOnly,
         HornetQDefaultConfiguration.getDefaultClusterNotificationInterval(),
         HornetQDefaultConfiguration.getDefaultClusterNotificationAttempts(),
         null);
   }


   public ClusterConnectionConfiguration(final String name,
                                         final String address,
                                         final String connectorName,
                                         final int minLargeMessageSize,
                                         final long clientFailureCheckPeriod,
                                         final long connectionTTL,
                                         final long retryInterval,
                                         final double retryIntervalMultiplier,
                                         final long maxRetryInterval,
                                         final int initialConnectAttempts,
                                         final int reconnectAttempts,
                                         final long callTimeout,
                                         final long callFailoverTimeout,
                                         final boolean duplicateDetection,
                                         final boolean forwardWhenNoConsumers,
                                         final int maxHops,
                                         final int confirmationWindowSize,
                                         final List<String> staticConnectors,
                                         final boolean allowDirectConnectionsOnly,
                                         final long clusterNotificationInterval,
                                         final int clusterNotificationAttempts,
                                         final String scaleDownConnector)
   {
      this.name = name;
      this.address = address;
      this.connectorName = connectorName;
      this.clientFailureCheckPeriod = clientFailureCheckPeriod;
      this.connectionTTL = connectionTTL;
      if (retryInterval <= 0)
         throw HornetQMessageBundle.BUNDLE.invalidRetryInterval(retryInterval);
      this.retryInterval = retryInterval;
      this.retryIntervalMultiplier = retryIntervalMultiplier;
      this.maxRetryInterval = maxRetryInterval;
      this.initialConnectAttempts = initialConnectAttempts;
      this.reconnectAttempts = reconnectAttempts;
      if (staticConnectors != null)
      {
         this.staticConnectors = staticConnectors;
      }
      else
      {
         this.staticConnectors = Collections.emptyList();
      }
      this.duplicateDetection = duplicateDetection;
      this.callTimeout = callTimeout;
      this.callFailoverTimeout = callFailoverTimeout;
      this.forwardWhenNoConsumers = forwardWhenNoConsumers;
      discoveryGroupName = null;
      this.maxHops = maxHops;
      this.confirmationWindowSize = confirmationWindowSize;
      this.allowDirectConnectionsOnly = allowDirectConnectionsOnly;
      this.minLargeMessageSize = minLargeMessageSize;
      this.clusterNotificationInterval = clusterNotificationInterval;
      this.clusterNotificationAttempts = clusterNotificationAttempts;
   }


   public ClusterConnectionConfiguration(final String name,
                                         final String address,
                                         final String connectorName,
                                         final long retryInterval,
                                         final boolean duplicateDetection,
                                         final boolean forwardWhenNoConsumers,
                                         final int maxHops,
                                         final int confirmationWindowSize,
                                         final String discoveryGroupName)
   {
      this(name,
         address,
         connectorName,
         HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
         HornetQDefaultConfiguration.getDefaultClusterFailureCheckPeriod(),
         HornetQDefaultConfiguration.getDefaultClusterConnectionTtl(),
         retryInterval,
         HornetQDefaultConfiguration.getDefaultClusterRetryIntervalMultiplier(),
         HornetQDefaultConfiguration.getDefaultClusterMaxRetryInterval(),
         HornetQDefaultConfiguration.getDefaultClusterInitialConnectAttempts(),
         HornetQDefaultConfiguration.getDefaultClusterReconnectAttempts(),
         HornetQClient.DEFAULT_CALL_TIMEOUT,
         HornetQClient.DEFAULT_CALL_FAILOVER_TIMEOUT,
         duplicateDetection,
         forwardWhenNoConsumers,
         maxHops,
         confirmationWindowSize,
         discoveryGroupName,
         HornetQDefaultConfiguration.getDefaultClusterNotificationInterval(),
         HornetQDefaultConfiguration.getDefaultClusterNotificationAttempts(),
         null);
   }


   public ClusterConnectionConfiguration(final String name,
                                         final String address,
                                         final String connectorName,
                                         final int minLargeMessageSize,
                                         final long clientFailureCheckPeriod,
                                         final long connectionTTL,
                                         final long retryInterval,
                                         final double retryIntervalMultiplier,
                                         final long maxRetryInterval,
                                         final int initialConnectAttempts,
                                         final int reconnectAttempts,
                                         final long callTimeout,
                                         final long callFailoverTimeout,
                                         final boolean duplicateDetection,
                                         final boolean forwardWhenNoConsumers,
                                         final int maxHops,
                                         final int confirmationWindowSize,
                                         final String discoveryGroupName,
                                         final long clusterNotificationInterval,
                                         final int clusterNotificationAttempts,
                                         final String scaleDownConnector)
   {
      this.name = name;
      this.address = address;
      this.connectorName = connectorName;
      this.clientFailureCheckPeriod = clientFailureCheckPeriod;
      this.connectionTTL = connectionTTL;
      this.retryInterval = retryInterval;
      this.retryIntervalMultiplier = retryIntervalMultiplier;
      this.maxRetryInterval = maxRetryInterval;
      this.initialConnectAttempts = initialConnectAttempts;
      this.reconnectAttempts = reconnectAttempts;
      this.callTimeout = callTimeout;
      this.callFailoverTimeout = callFailoverTimeout;
      this.duplicateDetection = duplicateDetection;
      this.forwardWhenNoConsumers = forwardWhenNoConsumers;
      this.discoveryGroupName = discoveryGroupName;
      this.clusterNotificationInterval = clusterNotificationInterval;
      this.clusterNotificationAttempts = clusterNotificationAttempts;
      this.staticConnectors = Collections.emptyList();
      this.maxHops = maxHops;
      this.confirmationWindowSize = confirmationWindowSize;
      this.minLargeMessageSize = minLargeMessageSize;
      allowDirectConnectionsOnly = false;
   }

   public String getName()
   {
      return name;
   }

   public String getAddress()
   {
      return address;
   }

   /**
    * @return the clientFailureCheckPeriod
    */
   public long getClientFailureCheckPeriod()
   {
      return clientFailureCheckPeriod;
   }

   /**
    * @return the connectionTTL
    */
   public long getConnectionTTL()
   {
      return connectionTTL;
   }

   /**
    * @return the retryIntervalMultiplier
    */
   public double getRetryIntervalMultiplier()
   {
      return retryIntervalMultiplier;
   }

   /**
    * @return the maxRetryInterval
    */
   public long getMaxRetryInterval()
   {
      return maxRetryInterval;
   }

   /**
    * @return the initialConnectAttempts
    */
   public int getInitialConnectAttempts()
   {
      return initialConnectAttempts;
   }

   /**
    * @return the reconnectAttempts
    */
   public int getReconnectAttempts()
   {
      return reconnectAttempts;
   }

   public long getCallTimeout()
   {
      return callTimeout;
   }

   public long getCallFailoverTimeout()
   {
      return callFailoverTimeout;
   }

   public String getConnectorName()
   {
      return connectorName;
   }

   public boolean isDuplicateDetection()
   {
      return duplicateDetection;
   }

   public boolean isForwardWhenNoConsumers()
   {
      return forwardWhenNoConsumers;
   }

   public int getMaxHops()
   {
      return maxHops;
   }

   public int getConfirmationWindowSize()
   {
      return confirmationWindowSize;
   }

   public List<String> getStaticConnectors()
   {
      return staticConnectors;
   }

   public String getDiscoveryGroupName()
   {
      return discoveryGroupName;
   }

   public long getRetryInterval()
   {
      return retryInterval;
   }

   public boolean isAllowDirectConnectionsOnly()
   {
      return allowDirectConnectionsOnly;
   }


   /**
    * @return the minLargeMessageSize
    */
   public int getMinLargeMessageSize()
   {
      return minLargeMessageSize;
   }

   /**
    * @param minLargeMessageSize the minLargeMessageSize to set
    */
   public void setMinLargeMessageSize(final int minLargeMessageSize)
   {
      this.minLargeMessageSize = minLargeMessageSize;
   }

   /**
    * @param clientFailureCheckPeriod the clientFailureCheckPeriod to set
    */
   public void setClientFailureCheckPeriod(long clientFailureCheckPeriod)
   {
      this.clientFailureCheckPeriod = clientFailureCheckPeriod;
   }

   /**
    * @param connectionTTL the connectionTTL to set
    */
   public void setConnectionTTL(long connectionTTL)
   {
      this.connectionTTL = connectionTTL;
   }

   /**
    * @param retryInterval the retryInterval to set
    */
   public void setRetryInterval(long retryInterval)
   {
      this.retryInterval = retryInterval;
   }

   /**
    * @param retryIntervalMultiplier the retryIntervalMultiplier to set
    */
   public void setRetryIntervalMultiplier(double retryIntervalMultiplier)
   {
      this.retryIntervalMultiplier = retryIntervalMultiplier;
   }

   /**
    * @param maxRetryInterval the maxRetryInterval to set
    */
   public void setMaxRetryInterval(long maxRetryInterval)
   {
      this.maxRetryInterval = maxRetryInterval;
   }

   /**
    * @param initialConnectAttempts the reconnectAttempts to set
    */
   public void setInitialConnectAttempts(int initialConnectAttempts)
   {
      this.initialConnectAttempts = initialConnectAttempts;
   }

   /**
    * @param reconnectAttempts the reconnectAttempts to set
    */
   public void setReconnectAttempts(int reconnectAttempts)
   {
      this.reconnectAttempts = reconnectAttempts;
   }

   /**
    * @param callTimeout the callTimeout to set
    */
   public void setCallTimeout(long callTimeout)
   {
      this.callTimeout = callTimeout;
   }

   /**
    * @param callFailoverTimeout the callTimeout to set
    */
   public void setCallFailoverTimeout(long callFailoverTimeout)
   {
      this.callFailoverTimeout = callFailoverTimeout;
   }

   /**
    * @param duplicateDetection the duplicateDetection to set
    */
   public void setDuplicateDetection(boolean duplicateDetection)
   {
      this.duplicateDetection = duplicateDetection;
   }

   /**
    * @param forwardWhenNoConsumers the forwardWhenNoConsumers to set
    */
   public void setForwardWhenNoConsumers(boolean forwardWhenNoConsumers)
   {
      this.forwardWhenNoConsumers = forwardWhenNoConsumers;
   }

   /*
   * returns the cluster update interval
   * */
   public long getClusterNotificationInterval()
   {
      return clusterNotificationInterval;
   }

   public int getClusterNotificationAttempts()
   {
      return clusterNotificationAttempts;
   }

   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((address == null) ? 0 : address.hashCode());
      result = prime * result + (allowDirectConnectionsOnly ? 1231 : 1237);
      result = prime * result + (int)(callFailoverTimeout ^ (callFailoverTimeout >>> 32));
      result = prime * result + (int)(callTimeout ^ (callTimeout >>> 32));
      result = prime * result + (int)(clientFailureCheckPeriod ^ (clientFailureCheckPeriod >>> 32));
      result = prime * result + clusterNotificationAttempts;
      result = prime * result + (int)(clusterNotificationInterval ^ (clusterNotificationInterval >>> 32));
      result = prime * result + confirmationWindowSize;
      result = prime * result + (int)(connectionTTL ^ (connectionTTL >>> 32));
      result = prime * result + ((connectorName == null) ? 0 : connectorName.hashCode());
      result = prime * result + ((discoveryGroupName == null) ? 0 : discoveryGroupName.hashCode());
      result = prime * result + (duplicateDetection ? 1231 : 1237);
      result = prime * result + (forwardWhenNoConsumers ? 1231 : 1237);
      result = prime * result + maxHops;
      result = prime * result + (int)(maxRetryInterval ^ (maxRetryInterval >>> 32));
      result = prime * result + minLargeMessageSize;
      result = prime * result + ((name == null) ? 0 : name.hashCode());
      result = prime * result + initialConnectAttempts;
      result = prime * result + reconnectAttempts;
      result = prime * result + (int)(retryInterval ^ (retryInterval >>> 32));
      long temp;
      temp = Double.doubleToLongBits(retryIntervalMultiplier);
      result = prime * result + (int)(temp ^ (temp >>> 32));
      result = prime * result + ((staticConnectors == null) ? 0 : staticConnectors.hashCode());
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
      ClusterConnectionConfiguration other = (ClusterConnectionConfiguration)obj;
      if (address == null)
      {
         if (other.address != null)
            return false;
      }
      else if (!address.equals(other.address))
         return false;
      if (allowDirectConnectionsOnly != other.allowDirectConnectionsOnly)
         return false;
      if (callFailoverTimeout != other.callFailoverTimeout)
         return false;
      if (callTimeout != other.callTimeout)
         return false;
      if (clientFailureCheckPeriod != other.clientFailureCheckPeriod)
         return false;
      if (clusterNotificationAttempts != other.clusterNotificationAttempts)
         return false;
      if (clusterNotificationInterval != other.clusterNotificationInterval)
         return false;
      if (confirmationWindowSize != other.confirmationWindowSize)
         return false;
      if (connectionTTL != other.connectionTTL)
         return false;
      if (connectorName == null)
      {
         if (other.connectorName != null)
            return false;
      }
      else if (!connectorName.equals(other.connectorName))
         return false;
      if (discoveryGroupName == null)
      {
         if (other.discoveryGroupName != null)
            return false;
      }
      else if (!discoveryGroupName.equals(other.discoveryGroupName))
         return false;
      if (duplicateDetection != other.duplicateDetection)
         return false;
      if (forwardWhenNoConsumers != other.forwardWhenNoConsumers)
         return false;
      if (maxHops != other.maxHops)
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
      return true;
   }
}
