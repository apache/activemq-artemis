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
package org.apache.activemq.ra;

import javax.jms.Session;
import javax.resource.ResourceException;
import javax.resource.spi.ActivationSpec;
import javax.resource.spi.BootstrapContext;
import javax.resource.spi.ResourceAdapter;
import javax.resource.spi.ResourceAdapterInternalException;
import javax.resource.spi.endpoint.MessageEndpointFactory;
import javax.resource.spi.work.WorkManager;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.activemq.api.core.BroadcastEndpointFactoryConfiguration;
import org.apache.activemq.api.core.DiscoveryGroupConfiguration;
import org.apache.activemq.api.core.HornetQException;
import org.apache.activemq.api.core.JGroupsBroadcastGroupConfiguration;
import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.api.core.UDPBroadcastGroupConfiguration;
import org.apache.activemq.api.core.client.ClientSession;
import org.apache.activemq.api.core.client.ClientSessionFactory;
import org.apache.activemq.api.core.client.HornetQClient;
import org.apache.activemq.api.jms.HornetQJMSClient;
import org.apache.activemq.api.jms.JMSFactoryType;
import org.apache.activemq.jms.client.HornetQConnectionFactory;
import org.apache.activemq.ra.inflow.HornetQActivation;
import org.apache.activemq.ra.inflow.HornetQActivationSpec;
import org.apache.activemq.ra.recovery.RecoveryManager;
import org.apache.activemq.utils.SensitiveDataCodec;
import org.jgroups.JChannel;

/**
 * The resource adapter for HornetQ
 *
 * @author <a href="adrian@jboss.com">Adrian Brock</a>
 * @author <a href="jesper.pedersen@jboss.org">Jesper Pedersen</a>
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 * @author <a href="mailto:mtaylor@redhat.com">Martyn Taylor</a>
 */
public class HornetQResourceAdapter implements ResourceAdapter, Serializable
{
   private static final long serialVersionUID = 4756893709825838770L;

   /**
    * The Name of the product that this resource adapter represents.
    */
   public static final String PRODUCT_NAME = "HornetQ";

   /**
    * Trace enabled
    */
   private static boolean trace = HornetQRALogger.LOGGER.isTraceEnabled();

   /**
    * The bootstrap context
    */
   private BootstrapContext ctx;

   /**
    * The resource adapter properties
    */
   private final HornetQRAProperties raProperties;

   /**
    * The resource adapter properties before parsing
    */
   private String unparsedProperties;


   /**
    * The resource adapter connector classnames before parsing
    */
   private String unparsedConnectors;

   /**
    * Have the factory been configured
    */
   private final AtomicBoolean configured;

   /**
    * The activations by activation spec
    */
   private final Map<ActivationSpec, HornetQActivation> activations;

   private HornetQConnectionFactory defaultHornetQConnectionFactory;

   private HornetQConnectionFactory recoveryHornetQConnectionFactory;

   private TransactionManager tm;

   private String unparsedJndiParams;

   private final RecoveryManager recoveryManager;

   private boolean useAutoRecovery = true;

   private final List<HornetQRAManagedConnectionFactory> managedConnectionFactories = new ArrayList<HornetQRAManagedConnectionFactory>();

   private String entries;

   /**
    * Constructor
    */
   public HornetQResourceAdapter()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("constructor()");
      }

      raProperties = new HornetQRAProperties();
      configured = new AtomicBoolean(false);
      activations = new ConcurrentHashMap<ActivationSpec, HornetQActivation>();
      recoveryManager = new RecoveryManager();
   }

   public TransactionManager getTM()
   {
      return tm;
   }

   /**
    * Endpoint activation
    *
    * @param endpointFactory The endpoint factory
    * @param spec            The activation spec
    * @throws ResourceException Thrown if an error occurs
    */
   public void endpointActivation(final MessageEndpointFactory endpointFactory, final ActivationSpec spec) throws ResourceException
   {
      if (spec == null)
      {
         throw HornetQRABundle.BUNDLE.noActivationSpec();
      }
      if (!configured.getAndSet(true))
      {
         try
         {
            setup();
         }
         catch (HornetQException e)
         {
            throw new ResourceException("Unable to create activation", e);
         }
      }
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("endpointActivation(" + endpointFactory + ", " + spec + ")");
      }

      HornetQActivation activation = new HornetQActivation(this, endpointFactory, (HornetQActivationSpec) spec);
      activations.put(spec, activation);
      activation.start();
   }

   /**
    * Endpoint deactivation
    *
    * @param endpointFactory The endpoint factory
    * @param spec            The activation spec
    */
   public void endpointDeactivation(final MessageEndpointFactory endpointFactory, final ActivationSpec spec)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("endpointDeactivation(" + endpointFactory + ", " + spec + ")");
      }

      HornetQActivation activation = activations.remove(spec);
      if (activation != null)
      {
         activation.stop();
      }
   }

   /**
    * Get XA resources
    *
    * @param specs The activation specs
    * @return The XA resources
    * @throws ResourceException Thrown if an error occurs or unsupported
    */
   public XAResource[] getXAResources(final ActivationSpec[] specs) throws ResourceException
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("getXAResources(" + Arrays.toString(specs) + ")");
      }

      if (useAutoRecovery)
      {
         // let the TM handle the recovery
         return null;
      }
      else
      {
         List<XAResource> xaresources = new ArrayList<XAResource>();
         for (ActivationSpec spec : specs)
         {
            HornetQActivation activation = activations.get(spec);
            if (activation != null)
            {
               xaresources.addAll(activation.getXAResources());
            }
         }
         return xaresources.toArray(new XAResource[xaresources.size()]);
      }
   }

   /**
    * Start
    *
    * @param ctx The bootstrap context
    * @throws ResourceAdapterInternalException Thrown if an error occurs
    */
   public void start(final BootstrapContext ctx) throws ResourceAdapterInternalException
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("start(" + ctx + ")");
      }

      locateTM();

      recoveryManager.start(useAutoRecovery);

      this.ctx = ctx;

      if (!configured.getAndSet(true))
      {
         try
         {
            setup();
         }
         catch (HornetQException e)
         {
            throw new ResourceAdapterInternalException("Unable to create activation", e);
         }
      }

      HornetQRALogger.LOGGER.info("HornetQ resource adaptor started");
   }

   /**
    * Stop
    */
   public void stop()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("stop()");
      }

      for (Map.Entry<ActivationSpec, HornetQActivation> entry : activations.entrySet())
      {
         try
         {
            entry.getValue().stop();
         }
         catch (Exception ignored)
         {
            HornetQRALogger.LOGGER.debug("Ignored", ignored);
         }
      }

      activations.clear();

      for (HornetQRAManagedConnectionFactory managedConnectionFactory : managedConnectionFactories)
      {
         managedConnectionFactory.stop();
      }

      managedConnectionFactories.clear();

      if (defaultHornetQConnectionFactory != null)
      {
         defaultHornetQConnectionFactory.close();
      }

      if (recoveryHornetQConnectionFactory != null)
      {
         recoveryHornetQConnectionFactory.close();
      }

      recoveryManager.stop();

      HornetQRALogger.LOGGER.raStopped();
   }

   public void setUseAutoRecovery(Boolean useAutoRecovery)
   {
      this.useAutoRecovery = useAutoRecovery;
   }

   public Boolean isUseAutoRecovery()
   {
      return this.useAutoRecovery;
   }

   public Boolean isUseMaskedPassword()
   {
      return this.raProperties.isUseMaskedPassword();
   }

   public void setUseMaskedPassword(Boolean useMaskedPassword)
   {
      this.raProperties.setUseMaskedPassword(useMaskedPassword);
   }

   public void setPasswordCodec(String passwordCodec)
   {
      this.raProperties.setPasswordCodec(passwordCodec);
   }

   public String getPasswordCodec()
   {
      return this.raProperties.getPasswordCodec();
   }

   public void setConnectorClassName(final String connectorClassName)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("setTransportType(" + connectorClassName + ")");
      }
      unparsedConnectors = connectorClassName;

      raProperties.setParsedConnectorClassNames(HornetQRaUtils.parseConnectorConnectorConfig(connectorClassName));
   }

   public String getConnectorClassName()
   {
      return unparsedConnectors;
   }

   public String getConnectionParameters()
   {
      return unparsedProperties;
   }

   public void setConnectionParameters(final String config)
   {
      if (config != null)
      {
         this.unparsedProperties = config;
         raProperties.setParsedConnectionParameters(HornetQRaUtils.parseConfig(config));
      }
   }


   public Boolean getHA()
   {
      return raProperties.isHA();
   }

   public void setHA(final Boolean ha)
   {
      this.raProperties.setHA(ha);
   }

   public String getEntries()
   {
      return entries;
   }

   public String getJndiName()
   {
      if (!(entries == null || entries.isEmpty()))
      {
         Matcher m = Pattern.compile("\"(.*?)\"").matcher(entries);
         if (m.find())
         {
            return m.group(1);
         }
      }
      return null;
   }

   public void setEntries(String entries)
   {
      this.entries = entries;
   }

   /**
    * Get the discovery group name
    *
    * @return The value
    */
   public String getDiscoveryAddress()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("getDiscoveryGroupAddress()");
      }

      return raProperties.getDiscoveryAddress();
   }

   public void setJgroupsFile(String jgroupsFile)
   {
      raProperties.setJgroupsFile(jgroupsFile);
   }

   public String getJgroupsFile()
   {
      return raProperties.getJgroupsFile();
   }

   public String getJgroupsChannelName()
   {
      return raProperties.getJgroupsChannelName();
   }

   public void setJgroupsChannelName(String jgroupsChannelName)
   {
      raProperties.setJgroupsChannelName(jgroupsChannelName);
   }

   /**
    * Set the discovery group name
    *
    * @param dgn The value
    */
   public void setDiscoveryAddress(final String dgn)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("setDiscoveryGroupAddress(" + dgn + ")");
      }

      raProperties.setDiscoveryAddress(dgn);
   }

   /**
    * Get the discovery group port
    *
    * @return The value
    */
   public Integer getDiscoveryPort()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("getDiscoveryGroupPort()");
      }

      return raProperties.getDiscoveryPort();
   }

   /**
    * set the discovery local bind address
    *
    * @param discoveryLocalBindAddress the address value
    */
   public void setDiscoveryLocalBindAddress(final String discoveryLocalBindAddress)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("setDiscoveryLocalBindAddress(" + discoveryLocalBindAddress + ")");
      }

      raProperties.setDiscoveryLocalBindAddress(discoveryLocalBindAddress);
   }

   /**
    * get the discovery local bind address
    *
    * @return the address value
    */
   public String getDiscoveryLocalBindAddress()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("getDiscoveryLocalBindAddress()");
      }

      return raProperties.getDiscoveryLocalBindAddress();
   }

   /**
    * Set the discovery group port
    *
    * @param dgp The value
    */
   public void setDiscoveryPort(final Integer dgp)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("setDiscoveryGroupPort(" + dgp + ")");
      }

      raProperties.setDiscoveryPort(dgp);
   }

   /**
    * Get discovery refresh timeout
    *
    * @return The value
    */
   public Long getDiscoveryRefreshTimeout()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("getDiscoveryRefreshTimeout()");
      }

      return raProperties.getDiscoveryRefreshTimeout();
   }

   /**
    * Set discovery refresh timeout
    *
    * @param discoveryRefreshTimeout The value
    */
   public void setDiscoveryRefreshTimeout(final Long discoveryRefreshTimeout)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("setDiscoveryRefreshTimeout(" + discoveryRefreshTimeout + ")");
      }

      raProperties.setDiscoveryRefreshTimeout(discoveryRefreshTimeout);
   }

   /**
    * Get discovery initial wait timeout
    *
    * @return The value
    */
   public Long getDiscoveryInitialWaitTimeout()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("getDiscoveryInitialWaitTimeout()");
      }

      return raProperties.getDiscoveryInitialWaitTimeout();
   }

   /**
    * Set discovery initial wait timeout
    *
    * @param discoveryInitialWaitTimeout The value
    */
   public void setDiscoveryInitialWaitTimeout(final Long discoveryInitialWaitTimeout)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("setDiscoveryInitialWaitTimeout(" + discoveryInitialWaitTimeout + ")");
      }

      raProperties.setDiscoveryInitialWaitTimeout(discoveryInitialWaitTimeout);
   }

   /**
    * Get client failure check period
    *
    * @return The value
    */
   public Long getClientFailureCheckPeriod()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("getClientFailureCheckPeriod()");
      }

      return raProperties.getClientFailureCheckPeriod();
   }

   /**
    * Set client failure check period
    *
    * @param clientFailureCheckPeriod The value
    */
   public void setClientFailureCheckPeriod(final Long clientFailureCheckPeriod)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("setClientFailureCheckPeriod(" + clientFailureCheckPeriod + ")");
      }

      raProperties.setClientFailureCheckPeriod(clientFailureCheckPeriod);
   }

   /**
    * Get connection TTL
    *
    * @return The value
    */
   public Long getConnectionTTL()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("getConnectionTTL()");
      }

      return raProperties.getConnectionTTL();
   }

   /**
    * Set connection TTL
    *
    * @param connectionTTL The value
    */
   public void setConnectionTTL(final Long connectionTTL)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("setConnectionTTL(" + connectionTTL + ")");
      }

      raProperties.setConnectionTTL(connectionTTL);
   }

   /**
    * Get cacheLargeMessagesClient
    *
    * @return The value
    */
   public Boolean isCacheLargeMessagesClient()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("isCacheLargeMessagesClient()");
      }

      return raProperties.isCacheLargeMessagesClient();
   }

   /**
    * Set cacheLargeMessagesClient
    *
    * @param cacheLargeMessagesClient The value
    */
   public void setCacheLargeMessagesClient(final Boolean cacheLargeMessagesClient)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("setCacheLargeMessagesClient(" + cacheLargeMessagesClient + ")");
      }

      raProperties.setCacheLargeMessagesClient(cacheLargeMessagesClient);
   }

   /**
    * Get compressLargeMessage
    *
    * @return The value
    */
   public Boolean isCompressLargeMessage()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("isCompressLargeMessage()");
      }

      return raProperties.isCompressLargeMessage();
   }

   /**
    * Set failoverOnInitialConnection
    *
    * @param failoverOnInitialConnection The value
    */
   public void setFailoverOnInitialConnection(final Boolean failoverOnInitialConnection)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("setFailoverOnInitialConnection(" + failoverOnInitialConnection + ")");
      }

      raProperties.setFailoverOnInitialConnection(failoverOnInitialConnection);
   }

   /**
    * Get isFailoverOnInitialConnection
    *
    * @return The value
    */
   public Boolean isFailoverOnInitialConnection()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("isFailoverOnInitialConnection()");
      }

      return raProperties.isFailoverOnInitialConnection();
   }

   /**
    * Set compressLargeMessage
    *
    * @param compressLargeMessage The value
    */
   public void setCompressLargeMessage(final Boolean compressLargeMessage)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("setCompressLargeMessage(" + compressLargeMessage + ")");
      }

      raProperties.setCompressLargeMessage(compressLargeMessage);
   }

   /**
    * Get call timeout
    *
    * @return The value
    */
   public Long getCallTimeout()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("getCallTimeout()");
      }

      return raProperties.getCallTimeout();
   }

   /**
    * Set call timeout
    *
    * @param callTimeout The value
    */
   public void setCallTimeout(final Long callTimeout)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("setCallTimeout(" + callTimeout + ")");
      }

      raProperties.setCallTimeout(callTimeout);
   }

   /**
    * Get call failover timeout
    *
    * @return The value
    */
   public Long getCallFailoverTimeout()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("getCallFailoverTimeout()");
      }

      return raProperties.getCallFailoverTimeout();
   }

   /**
    * Set call failover timeout
    *
    * @param callFailoverTimeout The value
    */
   public void setCallFailoverTimeout(final Long callFailoverTimeout)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("setCallFailoverTimeout(" + callFailoverTimeout + ")");
      }

      raProperties.setCallFailoverTimeout(callFailoverTimeout);
   }

   /**
    * Get dups ok batch size
    *
    * @return The value
    */
   public Integer getDupsOKBatchSize()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("getDupsOKBatchSize()");
      }

      return raProperties.getDupsOKBatchSize();
   }

   /**
    * Set dups ok batch size
    *
    * @param dupsOKBatchSize The value
    */
   public void setDupsOKBatchSize(final Integer dupsOKBatchSize)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("setDupsOKBatchSize(" + dupsOKBatchSize + ")");
      }

      raProperties.setDupsOKBatchSize(dupsOKBatchSize);
   }

   /**
    * Get transaction batch size
    *
    * @return The value
    */
   public Integer getTransactionBatchSize()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("getTransactionBatchSize()");
      }

      return raProperties.getTransactionBatchSize();
   }

   /**
    * Set transaction batch size
    *
    * @param transactionBatchSize The value
    */
   public void setTransactionBatchSize(final Integer transactionBatchSize)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("setTransactionBatchSize(" + transactionBatchSize + ")");
      }

      raProperties.setTransactionBatchSize(transactionBatchSize);
   }

   /**
    * Get consumer window size
    *
    * @return The value
    */
   public Integer getConsumerWindowSize()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("getConsumerWindowSize()");
      }

      return raProperties.getConsumerWindowSize();
   }

   /**
    * Set consumer window size
    *
    * @param consumerWindowSize The value
    */
   public void setConsumerWindowSize(final Integer consumerWindowSize)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("setConsumerWindowSize(" + consumerWindowSize + ")");
      }

      raProperties.setConsumerWindowSize(consumerWindowSize);
   }

   /**
    * Get consumer max rate
    *
    * @return The value
    */
   public Integer getConsumerMaxRate()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("getConsumerMaxRate()");
      }

      return raProperties.getConsumerMaxRate();
   }

   /**
    * Set consumer max rate
    *
    * @param consumerMaxRate The value
    */
   public void setConsumerMaxRate(final Integer consumerMaxRate)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("setConsumerMaxRate(" + consumerMaxRate + ")");
      }

      raProperties.setConsumerMaxRate(consumerMaxRate);
   }

   /**
    * Get confirmation window size
    *
    * @return The value
    */
   public Integer getConfirmationWindowSize()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("getConfirmationWindowSize()");
      }

      return raProperties.getConfirmationWindowSize();
   }

   /**
    * Set confirmation window size
    *
    * @param confirmationWindowSize The value
    */
   public void setConfirmationWindowSize(final Integer confirmationWindowSize)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("setConfirmationWindowSize(" + confirmationWindowSize + ")");
      }

      raProperties.setConfirmationWindowSize(confirmationWindowSize);
   }

   /**
    * Get producer max rate
    *
    * @return The value
    */
   public Integer getProducerMaxRate()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("getProducerMaxRate()");
      }

      return raProperties.getProducerMaxRate();
   }

   /**
    * Set producer max rate
    *
    * @param producerMaxRate The value
    */
   public void setProducerMaxRate(final Integer producerMaxRate)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("setProducerMaxRate(" + producerMaxRate + ")");
      }

      raProperties.setProducerMaxRate(producerMaxRate);
   }

   /**
    * Get producer window size
    *
    * @return The value
    */
   public Integer getProducerWindowSize()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("getProducerWindowSize()");
      }

      return raProperties.getProducerWindowSize();
   }

   /**
    * Set producer window size
    *
    * @param producerWindowSize The value
    */
   public void setProducerWindowSize(final Integer producerWindowSize)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("setProducerWindowSize(" + producerWindowSize + ")");
      }

      raProperties.setProducerWindowSize(producerWindowSize);
   }

   /**
    * Get min large message size
    *
    * @return The value
    */
   public Integer getMinLargeMessageSize()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("getMinLargeMessageSize()");
      }

      return raProperties.getMinLargeMessageSize();
   }

   /**
    * Set min large message size
    *
    * @param minLargeMessageSize The value
    */
   public void setMinLargeMessageSize(final Integer minLargeMessageSize)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("setMinLargeMessageSize(" + minLargeMessageSize + ")");
      }

      raProperties.setMinLargeMessageSize(minLargeMessageSize);
   }

   /**
    * Get block on acknowledge
    *
    * @return The value
    */
   public Boolean getBlockOnAcknowledge()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("getBlockOnAcknowledge()");
      }

      return raProperties.isBlockOnAcknowledge();
   }

   /**
    * Set block on acknowledge
    *
    * @param blockOnAcknowledge The value
    */
   public void setBlockOnAcknowledge(final Boolean blockOnAcknowledge)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("setBlockOnAcknowledge(" + blockOnAcknowledge + ")");
      }

      raProperties.setBlockOnAcknowledge(blockOnAcknowledge);
   }

   /**
    * Get block on non durable send
    *
    * @return The value
    */
   public Boolean getBlockOnNonDurableSend()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("getBlockOnNonDurableSend()");
      }

      return raProperties.isBlockOnNonDurableSend();
   }

   /**
    * Set block on non durable send
    *
    * @param blockOnNonDurableSend The value
    */
   public void setBlockOnNonDurableSend(final Boolean blockOnNonDurableSend)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("setBlockOnNonDurableSend(" + blockOnNonDurableSend + ")");
      }

      raProperties.setBlockOnNonDurableSend(blockOnNonDurableSend);
   }

   /**
    * Get block on durable send
    *
    * @return The value
    */
   public Boolean getBlockOnDurableSend()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("getBlockOnDurableSend()");
      }

      return raProperties.isBlockOnDurableSend();
   }

   /**
    * Set block on durable send
    *
    * @param blockOnDurableSend The value
    */
   public void setBlockOnDurableSend(final Boolean blockOnDurableSend)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("setBlockOnDurableSend(" + blockOnDurableSend + ")");
      }

      raProperties.setBlockOnDurableSend(blockOnDurableSend);
   }

   /**
    * Get auto group
    *
    * @return The value
    */
   public Boolean getAutoGroup()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("getAutoGroup()");
      }

      return raProperties.isAutoGroup();
   }

   /**
    * Set auto group
    *
    * @param autoGroup The value
    */
   public void setAutoGroup(final Boolean autoGroup)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("setAutoGroup(" + autoGroup + ")");
      }

      raProperties.setAutoGroup(autoGroup);
   }

   /**
    * Get pre acknowledge
    *
    * @return The value
    */
   public Boolean getPreAcknowledge()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("getPreAcknowledge()");
      }

      return raProperties.isPreAcknowledge();
   }

   /**
    * Set pre acknowledge
    *
    * @param preAcknowledge The value
    */
   public void setPreAcknowledge(final Boolean preAcknowledge)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("setPreAcknowledge(" + preAcknowledge + ")");
      }

      raProperties.setPreAcknowledge(preAcknowledge);
   }

   /**
    * Get number of initial connect attempts
    *
    * @return The value
    */
   public Integer getInitialConnectAttempts()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("getInitialConnectAttempts()");
      }

      return raProperties.getInitialConnectAttempts();
   }

   /**
    * Set number of initial connect attempts
    *
    * @param initialConnectAttempts The value
    */
   public void setInitialConnectAttempts(final Integer initialConnectAttempts)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("setInitialConnectionAttempts(" + initialConnectAttempts + ")");
      }

      raProperties.setInitialConnectAttempts(initialConnectAttempts);
   }

   /**
    * Get initial message packet size
    *
    * @return The value
    */
   public Integer getInitialMessagePacketSize()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("getInitialMessagePacketSize()");
      }

      return raProperties.getInitialMessagePacketSize();
   }

   /**
    * Set initial message packet size
    *
    * @param initialMessagePacketSize The value
    */
   public void setInitialMessagePacketSize(final Integer initialMessagePacketSize)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("setInitialMessagePacketSize(" + initialMessagePacketSize + ")");
      }

      raProperties.setInitialMessagePacketSize(initialMessagePacketSize);
   }

   /**
    * Get retry interval
    *
    * @return The value
    */
   public Long getRetryInterval()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("getRetryInterval()");
      }

      return raProperties.getRetryInterval();
   }

   /**
    * Set retry interval
    *
    * @param retryInterval The value
    */
   public void setRetryInterval(final Long retryInterval)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("setRetryInterval(" + retryInterval + ")");
      }

      raProperties.setRetryInterval(retryInterval);
   }

   /**
    * Get retry interval multiplier
    *
    * @return The value
    */
   public Double getRetryIntervalMultiplier()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("getRetryIntervalMultiplier()");
      }

      return raProperties.getRetryIntervalMultiplier();
   }

   /**
    * Set retry interval multiplier
    *
    * @param retryIntervalMultiplier The value
    */
   public void setRetryIntervalMultiplier(final Double retryIntervalMultiplier)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("setRetryIntervalMultiplier(" + retryIntervalMultiplier + ")");
      }

      raProperties.setRetryIntervalMultiplier(retryIntervalMultiplier);
   }

   /**
    * Get maximum time for retry interval
    *
    * @return The value
    */
   public Long getMaxRetryInterval()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("getMaxRetryInterval()");
      }

      return raProperties.getMaxRetryInterval();
   }

   /**
    * Set maximum time for retry interval
    *
    * @param maxRetryInterval The value
    */
   public void setMaxRetryInterval(final Long maxRetryInterval)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("setMaxRetryInterval(" + maxRetryInterval + ")");
      }

      raProperties.setMaxRetryInterval(maxRetryInterval);
   }

   /**
    * Get number of reconnect attempts
    *
    * @return The value
    */
   public Integer getReconnectAttempts()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("getReconnectAttempts()");
      }

      return raProperties.getReconnectAttempts();
   }

   /**
    * Set number of reconnect attempts
    *
    * @param reconnectAttempts The value
    */
   public void setReconnectAttempts(final Integer reconnectAttempts)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("setReconnectAttempts(" + reconnectAttempts + ")");
      }

      raProperties.setReconnectAttempts(reconnectAttempts);
   }

   public String getConnectionLoadBalancingPolicyClassName()
   {
      return raProperties.getConnectionLoadBalancingPolicyClassName();
   }

   public void setConnectionLoadBalancingPolicyClassName(final String connectionLoadBalancingPolicyClassName)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("setFailoverOnServerShutdown(" + connectionLoadBalancingPolicyClassName + ")");
      }
      raProperties.setConnectionLoadBalancingPolicyClassName(connectionLoadBalancingPolicyClassName);
   }

   public Integer getScheduledThreadPoolMaxSize()
   {
      return raProperties.getScheduledThreadPoolMaxSize();
   }

   public void setScheduledThreadPoolMaxSize(final Integer scheduledThreadPoolMaxSize)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("setFailoverOnServerShutdown(" + scheduledThreadPoolMaxSize + ")");
      }
      raProperties.setScheduledThreadPoolMaxSize(scheduledThreadPoolMaxSize);
   }

   public Integer getThreadPoolMaxSize()
   {
      return raProperties.getThreadPoolMaxSize();
   }

   public void setThreadPoolMaxSize(final Integer threadPoolMaxSize)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("setFailoverOnServerShutdown(" + threadPoolMaxSize + ")");
      }
      raProperties.setThreadPoolMaxSize(threadPoolMaxSize);
   }

   public Boolean getUseGlobalPools()
   {
      return raProperties.isUseGlobalPools();
   }

   public void setUseGlobalPools(final Boolean useGlobalPools)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("setFailoverOnServerShutdown(" + useGlobalPools + ")");
      }
      raProperties.setUseGlobalPools(useGlobalPools);
   }

   /**
    * Get the user name
    *
    * @return The value
    */
   public String getUserName()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("getUserName()");
      }

      return raProperties.getUserName();
   }

   /**
    * Set the user name
    *
    * @param userName The value
    */
   public void setUserName(final String userName)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("setUserName(" + userName + ")");
      }

      raProperties.setUserName(userName);
   }

   /**
    * Get the password
    *
    * @return The value
    */
   public String getPassword()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("getPassword()");
      }

      return raProperties.getPassword();
   }

   /**
    * Set the password
    *
    * @param password The value
    */
   public void setPassword(final String password)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("setPassword(****)");
      }

      raProperties.setPassword(password);
   }

   /**
    * @return the useJNDI
    */
   public boolean isUseJNDI()
   {
      return raProperties.isUseJNDI();
   }

   /**
    * @param value the useJNDI to set
    */
   public void setUseJNDI(final Boolean value)
   {
      raProperties.setUseJNDI(value);
   }

   /**
    * @return return the jndi params to use
    */
   public String getJndiParams()
   {
      return unparsedJndiParams;
   }

   public void setJndiParams(String jndiParams)
   {
      unparsedJndiParams = jndiParams;
      raProperties.setParsedJndiParams(HornetQRaUtils.parseHashtableConfig(jndiParams));
   }

   public Hashtable<?, ?> getParsedJndiParams()
   {
      return raProperties.getParsedJndiParams();
   }

   /**
    * Get the client ID
    *
    * @return The value
    */
   public String getClientID()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("getClientID()");
      }

      return raProperties.getClientID();
   }

   /**
    * Set the client ID
    *
    * @param clientID The client id
    */
   public void setClientID(final String clientID)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("setClientID(" + clientID + ")");
      }

      raProperties.setClientID(clientID);
   }

   /**
    * Get the group ID
    *
    * @return The value
    */
   public String getGroupID()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("getGroupID()");
      }

      return raProperties.getGroupID();
   }

   /**
    * Set the group ID
    *
    * @param groupID The group id
    */
   public void setGroupID(final String groupID)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("setGroupID(" + groupID + ")");
      }

      raProperties.setGroupID(groupID);
   }

   /**
    * Get the use XA flag
    *
    * @return The value
    */
   public Boolean getUseLocalTx()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("getUseLocalTx()");
      }

      return raProperties.getUseLocalTx();
   }

   /**
    * Set the use XA flag
    *
    * @param localTx The value
    */
   public void setUseLocalTx(final Boolean localTx)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("setUseXA(" + localTx + ")");
      }

      raProperties.setUseLocalTx(localTx);
   }

   public int getSetupAttempts()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("getSetupAttempts()");
      }
      return raProperties.getSetupAttempts();
   }

   public void setSetupAttempts(Integer setupAttempts)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("setSetupAttempts(" + setupAttempts + ")");
      }
      raProperties.setSetupAttempts(setupAttempts);
   }

   public long getSetupInterval()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("getSetupInterval()");
      }
      return raProperties.getSetupInterval();
   }

   public void setSetupInterval(Long interval)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("setSetupInterval(" + interval + ")");
      }
      raProperties.setSetupInterval(interval);
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
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("equals(" + obj + ")");
      }

      if (obj == null)
      {
         return false;
      }

      if (obj instanceof HornetQResourceAdapter)
      {
         return raProperties.equals(((HornetQResourceAdapter) obj).getProperties());
      }
      return false;
   }

   /**
    * Return the hash code for the object
    *
    * @return The hash code
    */
   @Override
   public int hashCode()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("hashCode()");
      }

      return raProperties.hashCode();
   }

   /**
    * Get the work manager
    *
    * @return The manager
    */
   public WorkManager getWorkManager()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("getWorkManager()");
      }

      if (ctx == null)
      {
         return null;
      }

      return ctx.getWorkManager();
   }

   public ClientSession createSession(final ClientSessionFactory parameterFactory,
                                      final int ackMode,
                                      final String user,
                                      final String pass,
                                      final Boolean preAck,
                                      final Integer dupsOkBatchSize,
                                      final Integer transactionBatchSize,
                                      final boolean deliveryTransacted,
                                      final boolean useLocalTx,
                                      final Integer txTimeout) throws Exception
   {

      ClientSession result;

      // if we are CMP or BMP using local tx we ignore the ack mode as we are transactional
      if (deliveryTransacted || useLocalTx)
      {
         // JBPAPP-8845
         // If transacted we need to send the ack flush as soon as possible
         // as if any transaction times out, we need the ack on the server already
         if (useLocalTx)
         {
            result = parameterFactory.createSession(user, pass, false, false, false, false, 0);
         }
         else
         {
            result = parameterFactory.createSession(user, pass, true, false, false, false, 0);
         }
      }
      else
      {
         if (preAck != null && preAck)
         {
            result = parameterFactory.createSession(user, pass, false, true, true, true, -1);
         }
         else
         {
            // only auto ack and dups ok are supported
            switch (ackMode)
            {
               case Session.AUTO_ACKNOWLEDGE:
                  result = parameterFactory.createSession(user, pass, false, true, true, false, 0);
                  break;
               case Session.DUPS_OK_ACKNOWLEDGE:
                  int actDupsOkBatchSize = dupsOkBatchSize != null ? dupsOkBatchSize
                     : HornetQClient.DEFAULT_ACK_BATCH_SIZE;
                  result = parameterFactory.createSession(user, pass, false, true, true, false, actDupsOkBatchSize);
                  break;
               default:
                  throw new IllegalArgumentException("Invalid ackmode: " + ackMode);
            }
         }
      }

      HornetQRALogger.LOGGER.debug("Using queue connection " + result);

      return result;

   }


   public RecoveryManager getRecoveryManager()
   {
      return recoveryManager;
   }

   /**
    * Get the resource adapter properties
    *
    * @return The properties
    */
   protected HornetQRAProperties getProperties()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQRALogger.LOGGER.trace("getProperties()");
      }

      return raProperties;
   }

   /**
    * Setup the factory
    */
   protected void setup() throws HornetQException
   {
      raProperties.init();
      defaultHornetQConnectionFactory = createHornetQConnectionFactory(raProperties);
      recoveryHornetQConnectionFactory = createRecoveryHornetQConnectionFactory(raProperties);
      recoveryManager.register(recoveryHornetQConnectionFactory, raProperties.getUserName(), raProperties.getPassword());
   }

   public Map<ActivationSpec, HornetQActivation> getActivations()
   {
      return activations;
   }

   public HornetQConnectionFactory getDefaultHornetQConnectionFactory() throws ResourceException
   {
      if (!configured.getAndSet(true))
      {
         try
         {
            setup();
         }
         catch (HornetQException e)
         {
            throw new ResourceException("Unable to create activation", e);
         }
      }
      return defaultHornetQConnectionFactory;
   }

   /**
    * @param transactionManagerLocatorClass
    * @see org.apache.activemq.ra.HornetQRAProperties#setTransactionManagerLocatorClass(java.lang.String)
    */
   public void setTransactionManagerLocatorClass(String transactionManagerLocatorClass)
   {
      raProperties.setTransactionManagerLocatorClass(transactionManagerLocatorClass);
   }

   /**
    * @return
    * @see org.apache.activemq.ra.HornetQRAProperties#getTransactionManagerLocatorClass()
    */
   public String getTransactionManagerLocatorClass()
   {
      return raProperties.getTransactionManagerLocatorClass();
   }

   /**
    * @return
    * @see org.apache.activemq.ra.HornetQRAProperties#getJgroupsChannelLocatorClass()
    */
   public String getJgroupsChannelLocatorClass()
   {
      return raProperties.getJgroupsChannelLocatorClass();
   }

   /**
    * @return
    * @see org.apache.activemq.ra.HornetQRAProperties#setJgroupsChannelLocatorClass()
    */
   public void setJgroupsChannelLocatorClass(String jgroupsChannelLocatorClass)
   {
      raProperties.setJgroupsChannelLocatorClass(jgroupsChannelLocatorClass);
   }

   /**
    * @return
    * @see org.apache.activemq.ra.HornetQRAProperties#getJgroupsChannelRefName()
    */
   public String getJgroupsChannelRefName()
   {
      return raProperties.getJgroupsChannelRefName();
   }

   /**
    * @return
    * @see org.apache.activemq.ra.HornetQRAProperties#setJgroupsChannelRefName()
    */
   public void setJgroupsChannelRefName(String jgroupsChannelRefName)
   {
      raProperties.setJgroupsChannelRefName(jgroupsChannelRefName);
   }

   /**
    * @return
    * @see org.apache.activemq.ra.HornetQRAProperties#getTransactionManagerLocatorMethod()
    */
   public String getTransactionManagerLocatorMethod()
   {
      return raProperties.getTransactionManagerLocatorMethod();
   }

   /**
    * @param transactionManagerLocatorMethod
    * @see org.apache.activemq.ra.HornetQRAProperties#setTransactionManagerLocatorMethod(java.lang.String)
    */
   public void setTransactionManagerLocatorMethod(String transactionManagerLocatorMethod)
   {
      raProperties.setTransactionManagerLocatorMethod(transactionManagerLocatorMethod);
   }

   public HornetQConnectionFactory createHornetQConnectionFactory(final ConnectionFactoryProperties overrideProperties)
   {
      HornetQConnectionFactory cf;
      List<String> connectorClassName = overrideProperties.getParsedConnectorClassNames() != null ? overrideProperties.getParsedConnectorClassNames()
         : raProperties.getParsedConnectorClassNames();

      String discoveryAddress = overrideProperties.getDiscoveryAddress() != null ? overrideProperties.getDiscoveryAddress()
         : getDiscoveryAddress();

      Boolean ha = overrideProperties.isHA() != null ? overrideProperties.isHA() : getHA();

      String jgroupsFileName = overrideProperties.getJgroupsFile() != null ? overrideProperties.getJgroupsFile()
         : getJgroupsFile();

      String jgroupsChannel = overrideProperties.getJgroupsChannelName() != null ? overrideProperties.getJgroupsChannelName()
         : getJgroupsChannelName();

      String jgroupsLocatorClassName = raProperties.getJgroupsChannelLocatorClass();

      if (ha == null)
      {
         ha = HornetQClient.DEFAULT_IS_HA;
      }

      if (discoveryAddress != null || jgroupsFileName != null || jgroupsLocatorClassName != null)
      {
         BroadcastEndpointFactoryConfiguration endpointFactoryConfiguration = null;

         if (jgroupsLocatorClassName != null)
         {
            String jchannelRefName = raProperties.getJgroupsChannelRefName();
            JChannel jchannel = HornetQRaUtils.locateJGroupsChannel(jgroupsLocatorClassName, jchannelRefName);
            endpointFactoryConfiguration = new JGroupsBroadcastGroupConfiguration(jchannel, jgroupsChannel);
         }
         else if (discoveryAddress != null)
         {
            Integer discoveryPort = overrideProperties.getDiscoveryPort() != null ? overrideProperties.getDiscoveryPort()
               : getDiscoveryPort();
            if (discoveryPort == null)
            {
               discoveryPort = HornetQClient.DEFAULT_DISCOVERY_PORT;
            }

            String localBindAddress = overrideProperties.getDiscoveryLocalBindAddress() != null ? overrideProperties.getDiscoveryLocalBindAddress()
               : raProperties.getDiscoveryLocalBindAddress();
            endpointFactoryConfiguration = new UDPBroadcastGroupConfiguration()
               .setGroupAddress(discoveryAddress)
               .setGroupPort(discoveryPort)
               .setLocalBindAddress(localBindAddress)
               .setLocalBindPort(-1);
         }
         else if (jgroupsFileName != null)
         {
            endpointFactoryConfiguration = new JGroupsBroadcastGroupConfiguration(jgroupsFileName, jgroupsChannel);
         }
         Long refreshTimeout = overrideProperties.getDiscoveryRefreshTimeout() != null ? overrideProperties.getDiscoveryRefreshTimeout()
            : raProperties.getDiscoveryRefreshTimeout();
         if (refreshTimeout == null)
         {
            refreshTimeout = HornetQClient.DEFAULT_DISCOVERY_REFRESH_TIMEOUT;
         }

         Long initialTimeout = overrideProperties.getDiscoveryInitialWaitTimeout() != null ? overrideProperties.getDiscoveryInitialWaitTimeout()
            : raProperties.getDiscoveryInitialWaitTimeout();

         if (initialTimeout == null)
         {
            initialTimeout = HornetQClient.DEFAULT_DISCOVERY_INITIAL_WAIT_TIMEOUT;
         }

         DiscoveryGroupConfiguration groupConfiguration = new DiscoveryGroupConfiguration()
            .setRefreshTimeout(refreshTimeout)
            .setDiscoveryInitialWaitTimeout(initialTimeout)
            .setBroadcastEndpointFactoryConfiguration(endpointFactoryConfiguration);

         if (HornetQRALogger.LOGGER.isDebugEnabled())
         {
            HornetQRALogger.LOGGER.debug("Creating Connection Factory on the resource adapter for discovery=" + groupConfiguration + " with ha=" + ha);
         }

         if (ha)
         {
            cf = HornetQJMSClient.createConnectionFactoryWithHA(groupConfiguration, JMSFactoryType.XA_CF);
         }
         else
         {
            cf = HornetQJMSClient.createConnectionFactoryWithoutHA(groupConfiguration, JMSFactoryType.XA_CF);
         }
      }
      else if (connectorClassName != null)
      {
         TransportConfiguration[] transportConfigurations = new TransportConfiguration[connectorClassName.size()];

         List<Map<String, Object>> connectionParams;
         if (overrideProperties.getParsedConnectorClassNames() != null)
         {
            connectionParams = overrideProperties.getParsedConnectionParameters();
         }
         else
         {
            connectionParams = raProperties.getParsedConnectionParameters();
         }

         for (int i = 0; i < connectorClassName.size(); i++)
         {
            TransportConfiguration tc;
            if (connectionParams == null || i >= connectionParams.size())
            {
               tc = new TransportConfiguration(connectorClassName.get(i));
               HornetQRALogger.LOGGER.debug("No connector params provided using default");
            }
            else
            {
               tc = new TransportConfiguration(connectorClassName.get(i), connectionParams.get(i));
            }

            transportConfigurations[i] = tc;
         }


         if (HornetQRALogger.LOGGER.isDebugEnabled())
         {
            HornetQRALogger.LOGGER.debug("Creating Connection Factory on the resource adapter for transport=" +
                                            Arrays.toString(transportConfigurations) + " with ha=" + ha);
         }

         if (ha)
         {
            cf = HornetQJMSClient.createConnectionFactoryWithHA(JMSFactoryType.XA_CF, transportConfigurations);
         }
         else
         {
            cf = HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.XA_CF, transportConfigurations);
         }
      }
      else
      {
         throw new IllegalArgumentException("must provide either TransportType or DiscoveryGroupAddress and DiscoveryGroupPort for HornetQ ResourceAdapter Connection Factory");
      }

      setParams(cf, overrideProperties);
      return cf;
   }

   public HornetQConnectionFactory createRecoveryHornetQConnectionFactory(final ConnectionFactoryProperties overrideProperties)
   {
      HornetQConnectionFactory cf;
      List<String> connectorClassName = overrideProperties.getParsedConnectorClassNames() != null ? overrideProperties.getParsedConnectorClassNames()
         : raProperties.getParsedConnectorClassNames();

      String discoveryAddress = overrideProperties.getDiscoveryAddress() != null ? overrideProperties.getDiscoveryAddress()
         : getDiscoveryAddress();

      String jgroupsFileName = overrideProperties.getJgroupsFile() != null ? overrideProperties.getJgroupsFile()
         : getJgroupsFile();

      String jgroupsChannel = overrideProperties.getJgroupsChannelName() != null ? overrideProperties.getJgroupsChannelName()
         : getJgroupsChannelName();

      if (connectorClassName == null)
      {
         BroadcastEndpointFactoryConfiguration endpointFactoryConfiguration = null;
         if (discoveryAddress != null)
         {
            Integer discoveryPort = overrideProperties.getDiscoveryPort() != null ? overrideProperties.getDiscoveryPort()
               : getDiscoveryPort();
            if (discoveryPort == null)
            {
               discoveryPort = HornetQClient.DEFAULT_DISCOVERY_PORT;
            }

            String localBindAddress = overrideProperties.getDiscoveryLocalBindAddress() != null ? overrideProperties.getDiscoveryLocalBindAddress()
               : raProperties.getDiscoveryLocalBindAddress();
            endpointFactoryConfiguration = new UDPBroadcastGroupConfiguration()
               .setGroupAddress(discoveryAddress)
               .setGroupPort(discoveryPort)
               .setLocalBindAddress(localBindAddress)
               .setLocalBindPort(-1);
         }
         else if (jgroupsFileName != null)
         {
            endpointFactoryConfiguration = new JGroupsBroadcastGroupConfiguration(jgroupsFileName, jgroupsChannel);
         }
         else
         {
            String jgroupsLocatorClass = raProperties.getJgroupsChannelLocatorClass();
            if (jgroupsLocatorClass != null)
            {
               String jgroupsChannelRefName = raProperties.getJgroupsChannelRefName();
               JChannel jchannel = HornetQRaUtils.locateJGroupsChannel(jgroupsLocatorClass, jgroupsChannelRefName);
               endpointFactoryConfiguration = new JGroupsBroadcastGroupConfiguration(jchannel, jgroupsChannel);
            }
            if (endpointFactoryConfiguration == null)
            {
               throw new IllegalArgumentException("must provide either TransportType or DiscoveryGroupAddress and DiscoveryGroupPort for HornetQ ResourceAdapter Connection Factory");
            }
         }

         Long refreshTimeout = overrideProperties.getDiscoveryRefreshTimeout() != null ? overrideProperties.getDiscoveryRefreshTimeout()
            : raProperties.getDiscoveryRefreshTimeout();
         if (refreshTimeout == null)
         {
            refreshTimeout = HornetQClient.DEFAULT_DISCOVERY_REFRESH_TIMEOUT;
         }

         Long initialTimeout = overrideProperties.getDiscoveryInitialWaitTimeout() != null ? overrideProperties.getDiscoveryInitialWaitTimeout()
            : raProperties.getDiscoveryInitialWaitTimeout();
         if (initialTimeout == null)
         {
            initialTimeout = HornetQClient.DEFAULT_DISCOVERY_INITIAL_WAIT_TIMEOUT;
         }
         DiscoveryGroupConfiguration groupConfiguration = new DiscoveryGroupConfiguration()
            .setRefreshTimeout(refreshTimeout)
            .setDiscoveryInitialWaitTimeout(initialTimeout)
            .setBroadcastEndpointFactoryConfiguration(endpointFactoryConfiguration);

         groupConfiguration.setRefreshTimeout(refreshTimeout);

         if (HornetQRALogger.LOGGER.isDebugEnabled())
         {
            HornetQRALogger.LOGGER.debug("Creating Recovery Connection Factory on the resource adapter for discovery=" + groupConfiguration);
         }

         cf = HornetQJMSClient.createConnectionFactoryWithoutHA(groupConfiguration, JMSFactoryType.XA_CF);
      }
      else
      {
         TransportConfiguration[] transportConfigurations = new TransportConfiguration[connectorClassName.size()];

         List<Map<String, Object>> connectionParams;
         if (overrideProperties.getParsedConnectorClassNames() != null)
         {
            connectionParams = overrideProperties.getParsedConnectionParameters();
         }
         else
         {
            connectionParams = raProperties.getParsedConnectionParameters();
         }

         for (int i = 0; i < connectorClassName.size(); i++)
         {
            TransportConfiguration tc;
            if (connectionParams == null || i >= connectionParams.size())
            {
               tc = new TransportConfiguration(connectorClassName.get(i));
               HornetQRALogger.LOGGER.debug("No connector params provided using default");
            }
            else
            {
               tc = new TransportConfiguration(connectorClassName.get(i), connectionParams.get(i));
            }

            transportConfigurations[i] = tc;
         }


         if (HornetQRALogger.LOGGER.isDebugEnabled())
         {
            HornetQRALogger.LOGGER.debug("Creating Recovery Connection Factory on the resource adapter for transport=" +
                                            Arrays.toString(transportConfigurations));
         }

         cf = HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.XA_CF, transportConfigurations);
      }
      setParams(cf, overrideProperties);

      //now make sure we are HA in any way

      cf.setReconnectAttempts(0);
      cf.setInitialConnectAttempts(0);
      return cf;
   }

   public Map<String, Object> overrideConnectionParameters(final Map<String, Object> connectionParams,
                                                           final Map<String, Object> overrideConnectionParams)
   {
      Map<String, Object> map = new HashMap<String, Object>();
      if (connectionParams != null)
      {
         map.putAll(connectionParams);
      }
      if (overrideConnectionParams != null)
      {
         for (Map.Entry<String, Object> stringObjectEntry : overrideConnectionParams.entrySet())
         {
            map.put(stringObjectEntry.getKey(), stringObjectEntry.getValue());
         }
      }
      return map;
   }

   private void locateTM()
   {
      String[] locatorClasses = raProperties.getTransactionManagerLocatorClass().split(";");
      String[] locatorMethods = raProperties.getTransactionManagerLocatorMethod().split(";");

      for (int i = 0; i < locatorClasses.length; i++)
      {
         tm = HornetQRaUtils.locateTM(locatorClasses[i], locatorMethods[i]);
         if (tm != null)
         {
            break;
         }
      }

      if (tm == null)
      {
         HornetQRALogger.LOGGER.noTXLocator();
      }
      else
      {
         HornetQRALogger.LOGGER.debug("TM located = " + tm);
      }
   }


   private void setParams(final HornetQConnectionFactory cf,
                          final ConnectionFactoryProperties overrideProperties)
   {
      Boolean val = overrideProperties.isAutoGroup() != null ? overrideProperties.isAutoGroup()
         : raProperties.isAutoGroup();
      if (val != null)
      {
         cf.setAutoGroup(val);
      }
      val = overrideProperties.isBlockOnAcknowledge() != null ? overrideProperties.isBlockOnAcknowledge()
         : raProperties.isBlockOnAcknowledge();
      if (val != null)
      {
         cf.setBlockOnAcknowledge(val);
      }
      val = overrideProperties.isBlockOnNonDurableSend() != null ? overrideProperties.isBlockOnNonDurableSend()
         : raProperties.isBlockOnNonDurableSend();
      if (val != null)
      {
         cf.setBlockOnNonDurableSend(val);
      }
      val = overrideProperties.isBlockOnDurableSend() != null ? overrideProperties.isBlockOnDurableSend()
         : raProperties.isBlockOnDurableSend();
      if (val != null)
      {
         cf.setBlockOnDurableSend(val);
      }
      val = overrideProperties.isPreAcknowledge() != null ? overrideProperties.isPreAcknowledge()
         : raProperties.isPreAcknowledge();
      if (val != null)
      {
         cf.setPreAcknowledge(val);
      }
      val = overrideProperties.isUseGlobalPools() != null ? overrideProperties.isUseGlobalPools()
         : raProperties.isUseGlobalPools();
      if (val != null)
      {
         cf.setUseGlobalPools(val);
      }

      val = overrideProperties.isCacheLargeMessagesClient() != null ? overrideProperties.isCacheLargeMessagesClient()
         : raProperties.isCacheLargeMessagesClient();
      if (val != null)
      {
         cf.setCacheLargeMessagesClient(val);
      }

      val = overrideProperties.isCompressLargeMessage() != null ? overrideProperties.isCompressLargeMessage()
         : raProperties.isCompressLargeMessage();
      if (val != null)
      {
         cf.setCompressLargeMessage(val);
      }

      val = overrideProperties.isFailoverOnInitialConnection() != null ? overrideProperties.isFailoverOnInitialConnection()
         : raProperties.isFailoverOnInitialConnection();
      if (val != null)
      {
         cf.setFailoverOnInitialConnection(val);
      }

      Integer val2 = overrideProperties.getConsumerMaxRate() != null ? overrideProperties.getConsumerMaxRate()
         : raProperties.getConsumerMaxRate();
      if (val2 != null)
      {
         cf.setConsumerMaxRate(val2);
      }
      val2 = overrideProperties.getConsumerWindowSize() != null ? overrideProperties.getConsumerWindowSize()
         : raProperties.getConsumerWindowSize();
      if (val2 != null)
      {
         cf.setConsumerWindowSize(val2);
      }
      val2 = overrideProperties.getDupsOKBatchSize() != null ? overrideProperties.getDupsOKBatchSize()
         : raProperties.getDupsOKBatchSize();
      if (val2 != null)
      {
         cf.setDupsOKBatchSize(val2);
      }

      val2 = overrideProperties.getMinLargeMessageSize() != null ? overrideProperties.getMinLargeMessageSize()
         : raProperties.getMinLargeMessageSize();
      if (val2 != null)
      {
         cf.setMinLargeMessageSize(val2);
      }
      val2 = overrideProperties.getProducerMaxRate() != null ? overrideProperties.getProducerMaxRate()
         : raProperties.getProducerMaxRate();
      if (val2 != null)
      {
         cf.setProducerMaxRate(val2);
      }
      val2 = overrideProperties.getProducerWindowSize() != null ? overrideProperties.getProducerWindowSize()
         : raProperties.getProducerWindowSize();
      if (val2 != null)
      {
         cf.setProducerWindowSize(val2);
      }
      val2 = overrideProperties.getConfirmationWindowSize() != null ? overrideProperties.getConfirmationWindowSize()
         : raProperties.getConfirmationWindowSize();
      if (val2 != null)
      {
         cf.setConfirmationWindowSize(val2);
      }
      val2 = overrideProperties.getReconnectAttempts() != null ? overrideProperties.getReconnectAttempts()
         : raProperties.getReconnectAttempts();
      if (val2 != null)
      {
         cf.setReconnectAttempts(val2);
      }
      else
      {
         //the global default is 0 but we should always try to reconnect JCA
         cf.setReconnectAttempts(-1);
      }
      val2 = overrideProperties.getThreadPoolMaxSize() != null ? overrideProperties.getThreadPoolMaxSize()
         : raProperties.getThreadPoolMaxSize();
      if (val2 != null)
      {
         cf.setThreadPoolMaxSize(val2);
      }
      val2 = overrideProperties.getScheduledThreadPoolMaxSize() != null ? overrideProperties.getScheduledThreadPoolMaxSize()
         : raProperties.getScheduledThreadPoolMaxSize();
      if (val2 != null)
      {
         cf.setScheduledThreadPoolMaxSize(val2);
      }
      val2 = overrideProperties.getTransactionBatchSize() != null ? overrideProperties.getTransactionBatchSize()
         : raProperties.getTransactionBatchSize();
      if (val2 != null)
      {
         cf.setTransactionBatchSize(val2);
      }
      val2 = overrideProperties.getInitialConnectAttempts() != null ? overrideProperties.getInitialConnectAttempts()
         : raProperties.getInitialConnectAttempts();
      if (val2 != null)
      {
         cf.setInitialConnectAttempts(val2);
      }
      val2 = overrideProperties.getInitialMessagePacketSize() != null ? overrideProperties.getInitialMessagePacketSize()
         : raProperties.getInitialMessagePacketSize();
      if (val2 != null)
      {
         cf.setInitialMessagePacketSize(val2);
      }

      Long val3 = overrideProperties.getClientFailureCheckPeriod() != null ? overrideProperties.getClientFailureCheckPeriod()
         : raProperties.getClientFailureCheckPeriod();
      if (val3 != null)
      {
         cf.setClientFailureCheckPeriod(val3);
      }
      val3 = overrideProperties.getCallTimeout() != null ? overrideProperties.getCallTimeout()
         : raProperties.getCallTimeout();
      if (val3 != null)
      {
         cf.setCallTimeout(val3);
      }
      val3 = overrideProperties.getCallFailoverTimeout() != null ? overrideProperties.getCallFailoverTimeout()
         : raProperties.getCallFailoverTimeout();
      if (val3 != null)
      {
         cf.setCallFailoverTimeout(val3);
      }
      val3 = overrideProperties.getConnectionTTL() != null ? overrideProperties.getConnectionTTL()
         : raProperties.getConnectionTTL();
      if (val3 != null)
      {
         cf.setConnectionTTL(val3);
      }

      val3 = overrideProperties.getRetryInterval() != null ? overrideProperties.getRetryInterval()
         : raProperties.getRetryInterval();
      if (val3 != null)
      {
         cf.setRetryInterval(val3);
      }

      val3 = overrideProperties.getMaxRetryInterval() != null ? overrideProperties.getMaxRetryInterval()
         : raProperties.getMaxRetryInterval();
      if (val3 != null)
      {
         cf.setMaxRetryInterval(val3);
      }

      Double val4 = overrideProperties.getRetryIntervalMultiplier() != null ? overrideProperties.getRetryIntervalMultiplier()
         : raProperties.getRetryIntervalMultiplier();
      if (val4 != null)
      {
         cf.setRetryIntervalMultiplier(val4);
      }
      String val5 = overrideProperties.getClientID() != null ? overrideProperties.getClientID()
         : raProperties.getClientID();
      if (val5 != null)
      {
         cf.setClientID(val5);
      }
      val5 = overrideProperties.getConnectionLoadBalancingPolicyClassName() != null ? overrideProperties.getConnectionLoadBalancingPolicyClassName()
         : raProperties.getConnectionLoadBalancingPolicyClassName();
      if (val5 != null)
      {
         cf.setConnectionLoadBalancingPolicyClassName(val5);
      }
   }

   public void setManagedConnectionFactory(HornetQRAManagedConnectionFactory hornetQRAManagedConnectionFactory)
   {
      managedConnectionFactories.add(hornetQRAManagedConnectionFactory);
   }

   public SensitiveDataCodec<String> getCodecInstance()
   {
      return raProperties.getCodecInstance();
   }
}
