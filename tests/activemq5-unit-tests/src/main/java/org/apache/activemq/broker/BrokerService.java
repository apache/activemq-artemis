/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.broker;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.security.Provider;
import java.security.Security;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.activemq.ActiveMQConnectionMetaData;
import org.apache.activemq.ConfigurationException;
import org.apache.activemq.Service;
import org.apache.activemq.advisory.AdvisoryBroker;
import org.apache.activemq.broker.artemiswrapper.ArtemisBrokerWrapper;
import org.apache.activemq.broker.cluster.ConnectionSplitBroker;
import org.apache.activemq.broker.jmx.AnnotatedMBean;
import org.apache.activemq.broker.jmx.BrokerMBeanSupport;
import org.apache.activemq.broker.jmx.BrokerView;
import org.apache.activemq.broker.jmx.ConnectorView;
import org.apache.activemq.broker.jmx.ConnectorViewMBean;
import org.apache.activemq.broker.jmx.HealthView;
import org.apache.activemq.broker.jmx.HealthViewMBean;
import org.apache.activemq.broker.jmx.JmsConnectorView;
import org.apache.activemq.broker.jmx.JobSchedulerView;
import org.apache.activemq.broker.jmx.JobSchedulerViewMBean;
import org.apache.activemq.broker.jmx.Log4JConfigView;
import org.apache.activemq.broker.jmx.ManagedRegionBroker;
import org.apache.activemq.broker.jmx.ManagementContext;
import org.apache.activemq.broker.jmx.NetworkConnectorView;
import org.apache.activemq.broker.jmx.NetworkConnectorViewMBean;
import org.apache.activemq.broker.jmx.ProxyConnectorView;
import org.apache.activemq.broker.region.CompositeDestinationInterceptor;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DestinationFactory;
import org.apache.activemq.broker.region.DestinationFactoryImpl;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.virtual.MirroredQueue;
import org.apache.activemq.broker.region.virtual.VirtualDestination;
import org.apache.activemq.broker.region.virtual.VirtualDestinationInterceptor;
import org.apache.activemq.broker.region.virtual.VirtualTopic;
import org.apache.activemq.broker.scheduler.JobSchedulerStore;
import org.apache.activemq.broker.scheduler.SchedulerBroker;
import org.apache.activemq.broker.scheduler.memory.InMemoryJobSchedulerStore;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.BrokerId;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.filter.DestinationFilter;
import org.apache.activemq.network.ConnectionFilter;
import org.apache.activemq.network.DiscoveryNetworkConnector;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.network.jms.JmsConnector;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.proxy.ProxyConnector;
import org.apache.activemq.security.MessageAuthorizationPolicy;
import org.apache.activemq.selector.SelectorParser;
import org.apache.activemq.store.JournaledStore;
import org.apache.activemq.store.PListStore;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.PersistenceAdapterFactory;
import org.apache.activemq.store.memory.MemoryPersistenceAdapter;
import org.apache.activemq.thread.Scheduler;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.transport.TransportFactorySupport;
import org.apache.activemq.transport.TransportServer;
import org.apache.activemq.transport.vm.VMTransportFactory;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.util.BrokerSupport;
import org.apache.activemq.util.DefaultIOExceptionHandler;
import org.apache.activemq.util.IOExceptionHandler;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IOHelper;
import org.apache.activemq.util.InetAddressUtil;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.util.ThreadPoolUtils;
import org.apache.activemq.util.TimeUtils;
import org.apache.activemq.util.URISupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Manages the life-cycle of an ActiveMQ Broker. A BrokerService consists of a
 * number of transport connectors, network connectors and a bunch of properties
 * which can be used to configure the broker as its lazily created.
 *
 * @org.apache.xbean.XBean
 */
public class BrokerService implements Service
{
   public static final String DEFAULT_PORT = "61616";
   public static final String DEFAULT_BROKER_NAME = "localhost";
   public static final String BROKER_VERSION;
   public static final int DEFAULT_MAX_FILE_LENGTH = 1024 * 1024 * 32;
   public static final long DEFAULT_START_TIMEOUT = 600000L;

   public String SERVER_SIDE_KEYSTORE;
   public String KEYSTORE_PASSWORD;
   public String SERVER_SIDE_TRUSTSTORE;
   public String TRUSTSTORE_PASSWORD;
   public String storeType;

   private static final Logger LOG = LoggerFactory.getLogger(BrokerService.class);

   @SuppressWarnings("unused")
   private static final long serialVersionUID = 7353129142305630237L;

   private String brokerName = DEFAULT_BROKER_NAME;
   private Broker broker;
   private BrokerId brokerId;
   private Throwable startException = null;
   private boolean startAsync = false;
   public Set<Integer> extraConnectors = new HashSet<Integer>();
   private File dataDirectoryFile;

   static
   {
      InputStream in;
      String version = null;
      if ((in = BrokerService.class.getResourceAsStream("/org/apache/activemq/version.txt")) != null)
      {
         BufferedReader reader = new BufferedReader(new InputStreamReader(in));
         try
         {
            version = reader.readLine();
         }
         catch (Exception e)
         {
         }
      }
      BROKER_VERSION = version;
   }

   @Override
   public String toString()
   {
      return "BrokerService[" + getBrokerName() + "]";
   }

   private String getBrokerVersion()
   {
      String version = ActiveMQConnectionMetaData.PROVIDER_VERSION;
      if (version == null)
      {
         version = BROKER_VERSION;
      }

      return version;
   }


   @Override
   public void start() throws Exception
   {
      startBroker(startAsync);
   }

   private void startBroker(boolean async) throws Exception
   {
      if (async)
      {
         new Thread("Broker Starting Thread")
         {
            @Override
            public void run()
            {
               try
               {
                  doStartBroker();
               }
               catch (Throwable t)
               {
                  startException = t;
               }
            }
         }.start();
      }
      else
      {
         doStartBroker();
      }
   }

   private void doStartBroker() throws Exception
   {
      if (startException != null)
      {
         return;
      }

      broker = getBroker();
      brokerId = broker.getBrokerId();

      LOG.info("Apache ActiveMQ Artemis Wrapper {} ({}, {}) is starting", new Object[]{getBrokerVersion(), getBrokerName(), brokerId});

      try
      {
         broker.start();
      }
      catch (Exception e)
      {
         throw e;
      }
      catch (Throwable t)
      {
         throw new Exception(t);
      }

      LOG.info("Apache ActiveMQ Artemis Wrapper {} ({}, {}) started", new Object[]{getBrokerVersion(), getBrokerName(), brokerId});
      LOG.info("For help or more information please see: http://activemq.apache.org");

   }


   /**
    * @throws Exception
    * @org.apache .xbean.DestroyMethod
    */
   @Override
   public void stop() throws Exception
   {

      LOG.info("Apache ActiveMQ Artemis{} ({}, {}) is shutting down", new Object[]{getBrokerVersion(), getBrokerName(), brokerId});

      if (broker != null)
      {
         broker.stop();
         broker = null;
      }
      LOG.info("Apache ActiveMQ Artemis {} ({}, {}) is shutdown", new Object[]{getBrokerVersion(), getBrokerName(), brokerId});
   }

   // Properties
   // -------------------------------------------------------------------------

   /**
    * Returns the message broker
    */
   public Broker getBroker() throws Exception
   {
      if (broker == null)
      {
         broker = createBroker();
      }
      return broker;
   }

   public String getBrokerName()
   {
      return brokerName;
   }

   /**
    * Sets the name of this broker; which must be unique in the network
    *
    * @param brokerName
    */
   public void setBrokerName(String brokerName)
   {
      if (brokerName == null)
      {
         throw new NullPointerException("The broker name cannot be null");
      }
      String str = brokerName.replaceAll("[^a-zA-Z0-9\\.\\_\\-\\:]", "_");
      if (!str.equals(brokerName))
      {
         LOG.error("Broker Name: {} contained illegal characters - replaced with {}", brokerName, str);
      }
      this.brokerName = str.trim();
   }

   /**
    * Factory method to create a new broker
    *
    * @throws Exception
    * @throws
    * @throws
    */
   protected Broker createBroker() throws Exception
   {
      broker = createBrokerWrapper();
      return broker;
   }

   private Broker createBrokerWrapper()
   {
      return new ArtemisBrokerWrapper(this);
   }

   public void makeSureDestinationExists(ActiveMQDestination activemqDestination) throws Exception
   {
      System.out.println(">>>> making sure dest exits: " + activemqDestination);
      ArtemisBrokerWrapper hqBroker = (ArtemisBrokerWrapper) this.broker;
      //it can be null
      if (activemqDestination == null)
      {
         return;
      }
      if (activemqDestination.isQueue())
      {
         String qname = activemqDestination.getPhysicalName();
         System.out.println("physical name: " + qname);
         hqBroker.makeSureQueueExists(qname);
      }
   }

   public boolean enableSsl()
   {
      return this.SERVER_SIDE_KEYSTORE != null;
   }

   //below are methods called directly by tests
   //we don't actually implement any of these for now,
   //just to make test compile pass.

   //we may get class cast exception as in TestSupport it
   //casts the broker to RegionBroker, which we didn't
   //implement (wrap) yet. Consider solving it later.
   public Broker getRegionBroker()
   {
      return broker;
   }

   public void setPersistenceAdapter(PersistenceAdapter persistenceAdapter) throws IOException
   {
   }

   public File getDataDirectoryFile()
   {
      if (dataDirectoryFile == null)
      {
         dataDirectoryFile = new File(IOHelper.getDefaultDataDirectory());
      }
      return dataDirectoryFile;
   }

   public File getBrokerDataDirectory()
   {
      String brokerDir = getBrokerName();
      return new File(getDataDirectoryFile(), brokerDir);
   }

   public PersistenceAdapter getPersistenceAdapter() throws IOException
   {
      return null;
   }

   public void waitUntilStopped()
   {
   }

   public boolean waitUntilStarted()
   {
      return true;
   }

   public void setDestinationPolicy(PolicyMap policyMap)
   {
   }

   public void setDeleteAllMessagesOnStartup(boolean deletePersistentMessagesOnStartup)
   {
   }

   public void setUseJmx(boolean useJmx)
   {
   }

   public ManagementContext getManagementContext()
   {
      return null;
   }

   public BrokerView getAdminView() throws Exception
   {
      return null;
   }

   public List<TransportConnector> getTransportConnectors()
   {
      return new ArrayList<>();
   }

   public TransportConnector addConnector(String bindAddress) throws Exception
   {
      return addConnector(new URI(bindAddress));
   }

   public void setIoExceptionHandler(IOExceptionHandler ioExceptionHandler)
   {
   }

   public void setPersistent(boolean persistent)
   {
   }

   public boolean isSlave()
   {
      return false;
   }

   public Destination getDestination(ActiveMQDestination destination) throws Exception
   {
      return null;
   }

   public void setAllowTempAutoCreationOnSend(boolean allowTempAutoCreationOnSend)
   {
   }

   public void setDedicatedTaskRunner(boolean dedicatedTaskRunner)
   {
   }

   public void setAdvisorySupport(boolean advisorySupport)
   {
   }

   public void setUseShutdownHook(boolean useShutdownHook)
   {
   }

   public void deleteAllMessages() throws IOException
   {
   }

   public Service[] getServices()
   {
      return null;
   }

   public void setPopulateUserNameInMBeans(boolean value)
   {
   }

   public void setDestinations(ActiveMQDestination[] destinations)
   {
   }

   public URI getVmConnectorURI()
   {
      return null;
   }

   public SystemUsage getSystemUsage()
   {
      return null;
   }

   public synchronized PListStore getTempDataStore()
   {
      return null;
   }

   public void setJmsBridgeConnectors(JmsConnector[] jmsConnectors)
   {
   }

   public void setDestinationInterceptors(DestinationInterceptor[] destinationInterceptors)
   {
   }

   public SslContext getSslContext()
   {
      return null;
   }

   public void setDataDirectory(String dataDirectory)
   {
   }

   public void setPlugins(BrokerPlugin[] plugins)
   {
   }

   public void setKeepDurableSubsActive(boolean keepDurableSubsActive)
   {
   }

   public NetworkConnector addNetworkConnector(String discoveryAddress) throws Exception
   {
      return null;
   }

   public TransportConnector getConnectorByName(String connectorName)
   {
      return null;
   }

   public TransportConnector addConnector(TransportConnector connector) throws Exception
   {
      return connector;
   }

   public void setEnableStatistics(boolean enableStatistics)
   {
   }

   public void setSystemUsage(SystemUsage memoryManager)
   {
   }

   public void setManagementContext(ManagementContext managementContext)
   {
   }

   public void setSchedulerDirectoryFile(File schedulerDirectory)
   {
   }

   public List<NetworkConnector> getNetworkConnectors()
   {
      return new ArrayList<>();
   }

   public void setSchedulerSupport(boolean schedulerSupport)
   {
   }

   public void setPopulateJMSXUserID(boolean populateJMSXUserID)
   {
   }

   public boolean isUseJmx()
   {
      return false;
   }

   public boolean isPersistent()
   {
      return false;
   }

   public TransportConnector getTransportConnectorByScheme(String scheme)
   {
      return null;
   }

   public TaskRunnerFactory getTaskRunnerFactory()
   {
      return null;
   }

   public boolean isStarted()
   {
      if (broker == null) return false;
      return !broker.isStopped();
   }

   public ProxyConnector addProxyConnector(ProxyConnector connector) throws Exception
   {
      return connector;
   }

   public void setDataDirectoryFile(File dataDirectoryFile)
   {
      this.dataDirectoryFile = dataDirectoryFile;
   }

   public PolicyMap getDestinationPolicy()
   {
      return null;
   }

   public void setTransportConnectorURIs(String[] transportConnectorURIs)
   {
   }

   public boolean isPopulateJMSXUserID()
   {
      return false;
   }

   public NetworkConnector getNetworkConnectorByName(String connectorName)
   {
      return null;
   }

   public boolean removeNetworkConnector(NetworkConnector connector)
   {
      return true;
   }

   public void setTransportConnectors(List<TransportConnector> transportConnectors) throws Exception
   {
   }

   public NetworkConnector addNetworkConnector(NetworkConnector connector) throws Exception
   {
      return connector;
   }

   public void setTempDataStore(PListStore tempDataStore)
   {
   }

   public void setJobSchedulerStore(JobSchedulerStore jobSchedulerStore)
   {
   }

   public ObjectName getBrokerObjectName() throws MalformedObjectNameException
   {
      return null;
   }

   public TransportConnector addConnector(URI bindAddress) throws Exception
   {
      Integer port = bindAddress.getPort();
      this.extraConnectors.add(port);
      return null;
   }

   public void setCacheTempDestinations(boolean cacheTempDestinations)
   {
   }

   public void setOfflineDurableSubscriberTimeout(long offlineDurableSubscriberTimeout)
   {
   }

   public void setOfflineDurableSubscriberTaskSchedule(long offlineDurableSubscriberTaskSchedule)
   {
   }

   public boolean isStopped()
   {
      return broker.isStopped();
   }

   public void setBrokerId(String brokerId)
   {
   }

   public BrokerPlugin[] getPlugins()
   {
      return null;
   }

   public void stopAllConnectors(ServiceStopper stopper)
   {
   }

   public void setMessageAuthorizationPolicy(MessageAuthorizationPolicy messageAuthorizationPolicy)
   {
   }

   public void setNetworkConnectorStartAsync(boolean networkConnectorStartAsync)
   {
   }

   public boolean isRestartAllowed()
   {
      return true;
   }

   public void setTaskRunnerFactory(TaskRunnerFactory taskRunnerFactory)
   {
   }

   public void start(boolean force) throws Exception
   {
      this.start();
   }

   public void setMonitorConnectionSplits(boolean monitorConnectionSplits)
   {
   }

   public void setUseMirroredQueues(boolean useMirroredQueues)
   {
   }

   public File getTmpDataDirectory()
   {
      return null;
   }

   public boolean isUseShutdownHook()
   {
      return true;
   }

   public boolean isDeleteAllMessagesOnStartup()
   {
      return false;
   }

   public void setUseVirtualTopics(boolean useVirtualTopics)
   {
   }

   public boolean isUseLoggingForShutdownErrors()
   {
      return true;
   }

   public TransportConnector addConnector(TransportServer transport) throws Exception
   {
      return null;
   }

   public synchronized JobSchedulerStore getJobSchedulerStore()
   {
      return null;
   }

   public boolean removeConnector(TransportConnector connector) throws Exception
   {
      return true;
   }

   public ConnectionContext getAdminConnectionContext() throws Exception {
      return null;
   }

   public void setUseAuthenticatedPrincipalForJMSXUserID(boolean useAuthenticatedPrincipalForJMSXUserID)
   {
   }

   public void setSchedulePeriodForDestinationPurge(int schedulePeriodForDestinationPurge)
   {
   }

   public void setMbeanInvocationTimeout(long mbeanInvocationTimeout)
   {
   }

   public void setNetworkConnectors(List<?> networkConnectors) throws Exception
   {
   }

   public void removeDestination(ActiveMQDestination destination) throws Exception
   {
   }

   public void setMaxPurgedDestinationsPerSweep(int maxPurgedDestinationsPerSweep)
   {
   }

   public void setBrokerObjectName(ObjectName brokerObjectName)
   {
   }

   public Map<String, String> getTransportConnectorURIsAsMap()
   {
      return null;
   }

   public void setSslContext(SslContext sslContext)
   {
   }

   public void setPersistenceFactory(PersistenceAdapterFactory persistenceFactory)
   {
   }

   protected TransportConnector createTransportConnector(URI brokerURI) throws Exception
   {
      return null;
   }

}



