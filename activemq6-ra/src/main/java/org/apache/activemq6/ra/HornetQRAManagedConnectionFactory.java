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
package org.apache.activemq6.ra;

import javax.jms.ConnectionMetaData;
import javax.resource.ResourceException;
import javax.resource.spi.ConnectionManager;
import javax.resource.spi.ConnectionRequestInfo;
import javax.resource.spi.ManagedConnection;
import javax.resource.spi.ManagedConnectionFactory;
import javax.resource.spi.ResourceAdapter;
import javax.resource.spi.ResourceAdapterAssociation;
import javax.security.auth.Subject;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Set;

import org.apache.activemq6.jms.client.HornetQConnectionFactory;
import org.apache.activemq6.jms.server.recovery.XARecoveryConfig;

/**
 * HornetQ ManagedConnectionFactory
 *
 * @author <a href="mailto:adrian@jboss.com">Adrian Brock</a>
 * @author <a href="mailto:jesper.pedersen@jboss.org">Jesper Pedersen</a>.
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public final class HornetQRAManagedConnectionFactory implements ManagedConnectionFactory, ResourceAdapterAssociation
{
   /**
    * Serial version UID
    */
   static final long serialVersionUID = -1452379518562456741L;
   /**
    * Trace enabled
    */
   private static boolean trace = HornetQRALogger.LOGGER.isTraceEnabled();

   /**
    * The resource adapter
    */
   private HornetQResourceAdapter ra;

   /**
    * Connection manager
    */
   private ConnectionManager cm;

   /**
    * The managed connection factory properties
    */
   private final HornetQRAMCFProperties mcfProperties;

   /**
    * Connection Factory used if properties are set
    */
   private HornetQConnectionFactory recoveryConnectionFactory;

   /*
   * The resource recovery if there is one
   * */
   private XARecoveryConfig resourceRecovery;

   /**
    * Constructor
    */
   public HornetQRAManagedConnectionFactory()
   {
      if (HornetQRAManagedConnectionFactory.trace)
      {
         HornetQRALogger.LOGGER.trace("constructor()");
      }

      ra = null;
      cm = null;
      mcfProperties = new HornetQRAMCFProperties();
   }

   /**
    * Creates a Connection Factory instance
    *
    * @return javax.resource.cci.ConnectionFactory instance
    * @throws ResourceException Thrown if a connection factory can't be created
    */
   public Object createConnectionFactory() throws ResourceException
   {
      if (HornetQRAManagedConnectionFactory.trace)
      {
         HornetQRALogger.LOGGER.debug("createConnectionFactory()");
      }

      return createConnectionFactory(new HornetQRAConnectionManager());
   }

   /**
    * Creates a Connection Factory instance
    *
    * @param cxManager The connection manager
    * @return javax.resource.cci.ConnectionFactory instance
    * @throws ResourceException Thrown if a connection factory can't be created
    */
   public Object createConnectionFactory(final ConnectionManager cxManager) throws ResourceException
   {
      if (HornetQRAManagedConnectionFactory.trace)
      {
         HornetQRALogger.LOGGER.trace("createConnectionFactory(" + cxManager + ")");
      }

      cm = cxManager;

      HornetQRAConnectionFactory cf = new HornetQRAConnectionFactoryImpl(this, cm);

      if (HornetQRAManagedConnectionFactory.trace)
      {
         HornetQRALogger.LOGGER.trace("Created connection factory: " + cf +
                                         ", using connection manager: " +
                                         cm);
      }
      return cf;
   }

   /**
    * Creates a new physical connection to the underlying EIS resource manager.
    *
    * @param subject       Caller's security information
    * @param cxRequestInfo Additional resource adapter specific connection request information
    * @return The managed connection
    * @throws ResourceException Thrown if a managed connection can't be created
    */
   public ManagedConnection createManagedConnection(final Subject subject, final ConnectionRequestInfo cxRequestInfo) throws ResourceException
   {
      if (HornetQRAManagedConnectionFactory.trace)
      {
         HornetQRALogger.LOGGER.trace("createManagedConnection(" + subject + ", " + cxRequestInfo + ")");
      }

      HornetQRAConnectionRequestInfo cri = getCRI((HornetQRAConnectionRequestInfo) cxRequestInfo);

      HornetQRACredential credential = HornetQRACredential.getCredential(this, subject, cri);

      if (HornetQRAManagedConnectionFactory.trace)
      {
         HornetQRALogger.LOGGER.trace("jms credential: " + credential);
      }

      HornetQRAManagedConnection mc = new HornetQRAManagedConnection(this,
                                                                     cri,
                                                                     ra,
                                                                     credential.getUserName(),
                                                                     credential.getPassword());

      if (HornetQRAManagedConnectionFactory.trace)
      {
         HornetQRALogger.LOGGER.trace("created new managed connection: " + mc);
      }

      registerRecovery();

      return mc;
   }

   private synchronized void registerRecovery()
   {
      if (recoveryConnectionFactory == null)
      {
         recoveryConnectionFactory = ra.createRecoveryHornetQConnectionFactory(mcfProperties);
         resourceRecovery = ra.getRecoveryManager().register(recoveryConnectionFactory, null, null);
      }
   }

   public XARecoveryConfig getResourceRecovery()
   {
      return resourceRecovery;
   }

   /**
    * Returns a matched connection from the candidate set of connections.
    *
    * @param connectionSet The candidate connection set
    * @param subject       Caller's security information
    * @param cxRequestInfo Additional resource adapter specific connection request information
    * @return The managed connection
    * @throws ResourceException Thrown if the managed connection can not be found
    */
   public ManagedConnection matchManagedConnections(@SuppressWarnings("rawtypes") final Set connectionSet, final Subject subject, final ConnectionRequestInfo cxRequestInfo) throws ResourceException
   {
      if (HornetQRAManagedConnectionFactory.trace)
      {
         HornetQRALogger.LOGGER.trace("matchManagedConnections(" + connectionSet +
                                         ", " +
                                         subject +
                                         ", " +
                                         cxRequestInfo +
                                         ")");
      }

      HornetQRAConnectionRequestInfo cri = getCRI((HornetQRAConnectionRequestInfo) cxRequestInfo);
      HornetQRACredential credential = HornetQRACredential.getCredential(this, subject, cri);

      if (HornetQRAManagedConnectionFactory.trace)
      {
         HornetQRALogger.LOGGER.trace("Looking for connection matching credentials: " + credential);
      }

      Iterator<?> connections = connectionSet.iterator();

      while (connections.hasNext())
      {
         Object obj = connections.next();

         if (obj instanceof HornetQRAManagedConnection)
         {
            HornetQRAManagedConnection mc = (HornetQRAManagedConnection) obj;
            ManagedConnectionFactory mcf = mc.getManagedConnectionFactory();

            if ((mc.getUserName() == null || mc.getUserName() != null && mc.getUserName()
               .equals(credential.getUserName())) && mcf.equals(this))
            {
               if (cri.equals(mc.getCRI()))
               {
                  if (HornetQRAManagedConnectionFactory.trace)
                  {
                     HornetQRALogger.LOGGER.trace("Found matching connection: " + mc);
                  }

                  return mc;
               }
            }
         }
      }

      if (HornetQRAManagedConnectionFactory.trace)
      {
         HornetQRALogger.LOGGER.trace("No matching connection was found");
      }

      return null;
   }

   /**
    * Set the log writer -- NOT SUPPORTED
    *
    * @param out The writer
    * @throws ResourceException Thrown if the writer can't be set
    */
   public void setLogWriter(final PrintWriter out) throws ResourceException
   {
      if (HornetQRAManagedConnectionFactory.trace)
      {
         HornetQRALogger.LOGGER.trace("setLogWriter(" + out + ")");
      }
   }

   /**
    * Get the log writer -- NOT SUPPORTED
    *
    * @return The writer
    * @throws ResourceException Thrown if the writer can't be retrieved
    */
   public PrintWriter getLogWriter() throws ResourceException
   {
      if (HornetQRAManagedConnectionFactory.trace)
      {
         HornetQRALogger.LOGGER.trace("getLogWriter()");
      }

      return null;
   }

   /**
    * Get the resource adapter
    *
    * @return The resource adapter
    */
   public ResourceAdapter getResourceAdapter()
   {
      if (HornetQRAManagedConnectionFactory.trace)
      {
         HornetQRALogger.LOGGER.trace("getResourceAdapter()");
      }

      return ra;
   }

   /**
    * Set the resource adapter
    * <p/>
    * This should ensure that when the RA is stopped, this MCF will be stopped as well.
    *
    * @param ra The resource adapter
    * @throws ResourceException Thrown if incorrect resource adapter
    */
   public void setResourceAdapter(final ResourceAdapter ra) throws ResourceException
   {
      if (HornetQRAManagedConnectionFactory.trace)
      {
         HornetQRALogger.LOGGER.trace("setResourceAdapter(" + ra + ")");
      }

      if (ra == null || !(ra instanceof HornetQResourceAdapter))
      {
         throw new ResourceException("Resource adapter is " + ra);
      }

      this.ra = (HornetQResourceAdapter) ra;
      this.ra.setManagedConnectionFactory(this);
   }

   /**
    * Indicates whether some other object is "equal to" this one.
    *
    * @param obj Object with which to compare
    * @return True if this object is the same as the obj argument; false otherwise.
    */
   @Override
   public boolean equals(final Object obj)
   {
      if (HornetQRAManagedConnectionFactory.trace)
      {
         HornetQRALogger.LOGGER.trace("equals(" + obj + ")");
      }

      if (obj == null)
      {
         return false;
      }

      if (obj instanceof HornetQRAManagedConnectionFactory)
      {
         HornetQRAManagedConnectionFactory other = (HornetQRAManagedConnectionFactory) obj;

         return mcfProperties.equals(other.getProperties()) && ra.equals(other.getResourceAdapter());
      }
      else
      {
         return false;
      }
   }

   /**
    * Return the hash code for the object
    *
    * @return The hash code
    */
   @Override
   public int hashCode()
   {
      if (HornetQRAManagedConnectionFactory.trace)
      {
         HornetQRALogger.LOGGER.trace("hashCode()");
      }

      int hash = mcfProperties.hashCode();
      hash += 31 * ra.hashCode();

      return hash;
   }

   /**
    * Get the default session type
    *
    * @return The value
    */
   public String getSessionDefaultType()
   {
      if (HornetQRAManagedConnectionFactory.trace)
      {
         HornetQRALogger.LOGGER.trace("getSessionDefaultType()");
      }

      return mcfProperties.getSessionDefaultType();
   }

   /**
    * Set the default session type
    *
    * @param type either javax.jms.Topic or javax.jms.Queue
    */
   public void setSessionDefaultType(final String type)
   {
      if (HornetQRAManagedConnectionFactory.trace)
      {
         HornetQRALogger.LOGGER.trace("setSessionDefaultType(" + type + ")");
      }

      mcfProperties.setSessionDefaultType(type);
   }

   /**
    * @return the connectionParameters
    */
   public String getConnectionParameters()
   {
      return mcfProperties.getStrConnectionParameters();
   }

   public void setConnectionParameters(final String configuration)
   {
      mcfProperties.setConnectionParameters(configuration);
   }

   /**
    * @return the transportType
    */
   public String getConnectorClassName()
   {
      return mcfProperties.getConnectorClassName();
   }

   public void setConnectorClassName(final String value)
   {
      mcfProperties.setConnectorClassName(value);
   }

   public String getConnectionLoadBalancingPolicyClassName()
   {
      return mcfProperties.getConnectionLoadBalancingPolicyClassName();
   }

   public void setConnectionLoadBalancingPolicyClassName(final String connectionLoadBalancingPolicyClassName)
   {
      mcfProperties.setConnectionLoadBalancingPolicyClassName(connectionLoadBalancingPolicyClassName);
   }

   public String getDiscoveryAddress()
   {
      return mcfProperties.getDiscoveryAddress();
   }

   public void setDiscoveryAddress(final String discoveryAddress)
   {
      mcfProperties.setDiscoveryAddress(discoveryAddress);
   }

   public Integer getDiscoveryPort()
   {
      return mcfProperties.getDiscoveryPort();
   }

   public void setDiscoveryPort(final Integer discoveryPort)
   {
      mcfProperties.setDiscoveryPort(discoveryPort);
   }

   public Long getDiscoveryRefreshTimeout()
   {
      return mcfProperties.getDiscoveryRefreshTimeout();
   }

   public void setDiscoveryRefreshTimeout(final Long discoveryRefreshTimeout)
   {
      mcfProperties.setDiscoveryRefreshTimeout(discoveryRefreshTimeout);
   }

   public Long getDiscoveryInitialWaitTimeout()
   {
      return mcfProperties.getDiscoveryInitialWaitTimeout();
   }

   public void setDiscoveryInitialWaitTimeout(final Long discoveryInitialWaitTimeout)
   {
      mcfProperties.setDiscoveryInitialWaitTimeout(discoveryInitialWaitTimeout);
   }

   public String getClientID()
   {
      return mcfProperties.getClientID();
   }

   public void setClientID(final String clientID)
   {
      mcfProperties.setClientID(clientID);
   }

   public Integer getDupsOKBatchSize()
   {
      return mcfProperties.getDupsOKBatchSize();
   }

   public void setDupsOKBatchSize(final Integer dupsOKBatchSize)
   {
      mcfProperties.setDupsOKBatchSize(dupsOKBatchSize);
   }

   public Integer getTransactionBatchSize()
   {
      return mcfProperties.getTransactionBatchSize();
   }

   public void setTransactionBatchSize(final Integer transactionBatchSize)
   {
      mcfProperties.setTransactionBatchSize(transactionBatchSize);
   }

   public Long getClientFailureCheckPeriod()
   {
      return mcfProperties.getClientFailureCheckPeriod();
   }

   public void setClientFailureCheckPeriod(final Long clientFailureCheckPeriod)
   {
      mcfProperties.setClientFailureCheckPeriod(clientFailureCheckPeriod);
   }

   public Long getConnectionTTL()
   {
      return mcfProperties.getConnectionTTL();
   }

   public void setConnectionTTL(final Long connectionTTL)
   {
      mcfProperties.setConnectionTTL(connectionTTL);
   }

   public Long getCallTimeout()
   {
      return mcfProperties.getCallTimeout();
   }

   public void setCallTimeout(final Long callTimeout)
   {
      mcfProperties.setCallTimeout(callTimeout);
   }

   public Integer getConsumerWindowSize()
   {
      return mcfProperties.getConsumerWindowSize();
   }

   public void setConsumerWindowSize(final Integer consumerWindowSize)
   {
      mcfProperties.setConsumerWindowSize(consumerWindowSize);
   }

   public Integer getConsumerMaxRate()
   {
      return mcfProperties.getConsumerMaxRate();
   }

   public void setConsumerMaxRate(final Integer consumerMaxRate)
   {
      mcfProperties.setConsumerMaxRate(consumerMaxRate);
   }

   public Integer getConfirmationWindowSize()
   {
      return mcfProperties.getConfirmationWindowSize();
   }

   public void setConfirmationWindowSize(final Integer confirmationWindowSize)
   {
      mcfProperties.setConfirmationWindowSize(confirmationWindowSize);
   }

   public Integer getProducerMaxRate()
   {
      return mcfProperties.getProducerMaxRate();
   }

   public void setProducerMaxRate(final Integer producerMaxRate)
   {
      mcfProperties.setProducerMaxRate(producerMaxRate);
   }

   public Integer getMinLargeMessageSize()
   {
      return mcfProperties.getMinLargeMessageSize();
   }

   public void setMinLargeMessageSize(final Integer minLargeMessageSize)
   {
      mcfProperties.setMinLargeMessageSize(minLargeMessageSize);
   }

   public Boolean isBlockOnAcknowledge()
   {
      return mcfProperties.isBlockOnAcknowledge();
   }

   public void setBlockOnAcknowledge(final Boolean blockOnAcknowledge)
   {
      mcfProperties.setBlockOnAcknowledge(blockOnAcknowledge);
   }

   public Boolean isBlockOnNonDurableSend()
   {
      return mcfProperties.isBlockOnNonDurableSend();
   }

   public void setBlockOnNonDurableSend(final Boolean blockOnNonDurableSend)
   {
      mcfProperties.setBlockOnNonDurableSend(blockOnNonDurableSend);
   }

   public Boolean isBlockOnDurableSend()
   {
      return mcfProperties.isBlockOnDurableSend();
   }

   public void setBlockOnDurableSend(final Boolean blockOnDurableSend)
   {
      mcfProperties.setBlockOnDurableSend(blockOnDurableSend);
   }

   public Boolean isAutoGroup()
   {
      return mcfProperties.isAutoGroup();
   }

   public void setAutoGroup(final Boolean autoGroup)
   {
      mcfProperties.setAutoGroup(autoGroup);
   }

   public Boolean isPreAcknowledge()
   {
      return mcfProperties.isPreAcknowledge();
   }

   public void setPreAcknowledge(final Boolean preAcknowledge)
   {
      mcfProperties.setPreAcknowledge(preAcknowledge);
   }

   public Long getRetryInterval()
   {
      return mcfProperties.getRetryInterval();
   }

   public void setRetryInterval(final Long retryInterval)
   {
      mcfProperties.setRetryInterval(retryInterval);
   }

   public Double getRetryIntervalMultiplier()
   {
      return mcfProperties.getRetryIntervalMultiplier();
   }

   public void setRetryIntervalMultiplier(final Double retryIntervalMultiplier)
   {
      mcfProperties.setRetryIntervalMultiplier(retryIntervalMultiplier);
   }

   public Integer getReconnectAttempts()
   {
      return mcfProperties.getReconnectAttempts();
   }

   public void setReconnectAttempts(final Integer reconnectAttempts)
   {
      mcfProperties.setReconnectAttempts(reconnectAttempts);
   }

   public Boolean isUseGlobalPools()
   {
      return mcfProperties.isUseGlobalPools();
   }

   public void setUseGlobalPools(final Boolean useGlobalPools)
   {
      mcfProperties.setUseGlobalPools(useGlobalPools);
   }

   public Integer getScheduledThreadPoolMaxSize()
   {
      return mcfProperties.getScheduledThreadPoolMaxSize();
   }

   public void setScheduledThreadPoolMaxSize(final Integer scheduledThreadPoolMaxSize)
   {
      mcfProperties.setScheduledThreadPoolMaxSize(scheduledThreadPoolMaxSize);
   }

   public Integer getThreadPoolMaxSize()
   {
      return mcfProperties.getThreadPoolMaxSize();
   }

   public void setThreadPoolMaxSize(final Integer threadPoolMaxSize)
   {
      mcfProperties.setThreadPoolMaxSize(threadPoolMaxSize);
   }

   public Boolean isHA()
   {
      return mcfProperties.isHA();
   }

   public void setHA(Boolean ha)
   {
      mcfProperties.setHA(ha);
   }

   /**
    * Get the useTryLock.
    *
    * @return the useTryLock.
    */
   public Integer getUseTryLock()
   {
      if (HornetQRAManagedConnectionFactory.trace)
      {
         HornetQRALogger.LOGGER.trace("getUseTryLock()");
      }

      return mcfProperties.getUseTryLock();
   }

   /**
    * Set the useTryLock.
    *
    * @param useTryLock the useTryLock.
    */
   public void setUseTryLock(final Integer useTryLock)
   {
      if (HornetQRAManagedConnectionFactory.trace)
      {
         HornetQRALogger.LOGGER.trace("setUseTryLock(" + useTryLock + ")");
      }

      mcfProperties.setUseTryLock(useTryLock);
   }

   /**
    * Get the connection metadata
    *
    * @return The metadata
    */
   public ConnectionMetaData getMetaData()
   {
      if (HornetQRAManagedConnectionFactory.trace)
      {
         HornetQRALogger.LOGGER.trace("getMetadata()");
      }

      return new HornetQRAConnectionMetaData();
   }

   /**
    * Get the managed connection factory properties
    *
    * @return The properties
    */
   protected HornetQRAMCFProperties getProperties()
   {
      if (HornetQRAManagedConnectionFactory.trace)
      {
         HornetQRALogger.LOGGER.trace("getProperties()");
      }

      return mcfProperties;
   }

   /**
    * Get a connection request info instance
    *
    * @param info The instance that should be updated; may be <code>null</code>
    * @return The instance
    */
   private HornetQRAConnectionRequestInfo getCRI(final HornetQRAConnectionRequestInfo info)
   {
      if (HornetQRAManagedConnectionFactory.trace)
      {
         HornetQRALogger.LOGGER.trace("getCRI(" + info + ")");
      }

      if (info == null)
      {
         // Create a default one
         return new HornetQRAConnectionRequestInfo(ra.getProperties(), mcfProperties.getType());
      }
      else
      {
         // Fill the one with any defaults
         info.setDefaults(ra.getProperties());
         return info;
      }
   }

   // this should be called when HornetQResourceAdapter.stop() is called since this MCF is registered with it
   public void stop()
   {
      if (resourceRecovery != null)
      {
         ra.getRecoveryManager().unRegister(resourceRecovery);
      }

      if (recoveryConnectionFactory != null)
      {
         recoveryConnectionFactory.close();
         recoveryConnectionFactory = null;
      }
   }
}
