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

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.ActiveMQConnectionMetaData;
import org.apache.activemq.Service;
import org.apache.activemq.broker.artemiswrapper.ArtemisBrokerWrapper;
import org.apache.activemq.broker.jmx.BrokerView;
import org.apache.activemq.broker.jmx.ManagementContext;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.scheduler.JobSchedulerStore;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.BrokerId;
import org.apache.activemq.network.ConnectionFilter;
import org.apache.activemq.network.DiscoveryNetworkConnector;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.network.jms.JmsConnector;
import org.apache.activemq.proxy.ProxyConnector;
import org.apache.activemq.security.MessageAuthorizationPolicy;
import org.apache.activemq.spring.SpringSslContext;
import org.apache.activemq.store.PListStore;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.PersistenceAdapterFactory;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.transport.TransportServer;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.util.IOExceptionHandler;
import org.apache.activemq.util.IOHelper;
import org.apache.activemq.util.ServiceStopper;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the life-cycle of an ActiveMQ Broker. A BrokerService consists of a
 * number of transport connectors, network connectors and a bunch of properties
 * which can be used to configure the broker as its lazily created.
 */
public class BrokerService implements Service {

   public static final String DEFAULT_PORT = "61616";
   public static final AtomicInteger RANDOM_PORT_BASE = new AtomicInteger(51616);
   public static final String DEFAULT_BROKER_NAME = "localhost";
   public static final String BROKER_VERSION;
   public static final int DEFAULT_MAX_FILE_LENGTH = 1024 * 1024 * 32;
   public static final long DEFAULT_START_TIMEOUT = 600000L;
   public static boolean disableWrapper = false;

   public String SERVER_SIDE_KEYSTORE;
   public String KEYSTORE_PASSWORD;
   public String SERVER_SIDE_TRUSTSTORE;
   public String TRUSTSTORE_PASSWORD;
   public String storeType;

   private SslContext sslContext;

   private static final Logger LOG = LoggerFactory.getLogger(BrokerService.class);

   @SuppressWarnings("unused")
   private static final long serialVersionUID = 7353129142305630237L;

   private String brokerName = DEFAULT_BROKER_NAME;
   private Broker broker;
   private BrokerId brokerId;
   private Throwable startException = null;
   private boolean startAsync = false;
   public Set<ConnectorInfo> extraConnectors = new HashSet<>();

   private List<TransportConnector> transportConnectors = new ArrayList<>();
   private File dataDirectoryFile;

   private PolicyMap destinationPolicy;
   private SystemUsage systemUsage;

   private final List<NetworkConnector> networkConnectors = new CopyOnWriteArrayList<NetworkConnector>();

   private TemporaryFolder tmpfolder;

   public static WeakHashMap<Broker, Exception> map = new WeakHashMap<>();

   static {
      InputStream in;
      String version = null;
      if ((in = BrokerService.class.getResourceAsStream("/org/apache/activemq/version.txt")) != null) {
         BufferedReader reader = new BufferedReader(new InputStreamReader(in));
         try {
            version = reader.readLine();
         }
         catch (Exception e) {
         }
      }
      BROKER_VERSION = version;
   }

   @Override
   public String toString() {
      return "BrokerService[" + getBrokerName() + "]" + super.toString();
   }

   private String getBrokerVersion() {
      String version = ActiveMQConnectionMetaData.PROVIDER_VERSION;
      if (version == null) {
         version = BROKER_VERSION;
      }

      return version;
   }

   @Override
   public void start() throws Exception {
      File targetTmp = new File("./target/tmp");
      targetTmp.mkdirs();
      tmpfolder = new TemporaryFolder(targetTmp);
      tmpfolder.create();
      Exception e = new Exception();
      e.fillInStackTrace();
      startBroker(startAsync);
      map.put(broker, e);
   }

   private void startBroker(boolean async) throws Exception {
      if (async) {
         new Thread("Broker Starting Thread") {
            @Override
            public void run() {
               try {
                  doStartBroker();
               }
               catch (Throwable t) {
                  startException = t;
               }
            }
         }.start();
      }
      else {
         doStartBroker();
      }
   }

   private void doStartBroker() throws Exception {
      if (startException != null) {
         return;
      }

      broker = getBroker();
      brokerId = broker.getBrokerId();

      LOG.info("Apache ActiveMQ Artemis Wrapper {} ({}, {}) is starting", new Object[]{getBrokerVersion(), getBrokerName(), brokerId});

      try {
         broker.start();
      }
      catch (Exception e) {
         throw e;
      }
      catch (Throwable t) {
         throw new Exception(t);
      }

      LOG.info("Apache ActiveMQ Artemis Wrapper {} ({}, {}) started", new Object[]{getBrokerVersion(), getBrokerName(), brokerId});
      LOG.info("For help or more information please see: http://activemq.apache.org");

   }

   @Override
   public void stop() throws Exception {
      System.out.println("broker is: " + broker);

      LOG.info("Apache ActiveMQ Artemis{} ({}, {}) is shutting down", new Object[]{getBrokerVersion(), getBrokerName(), brokerId});

      if (broker != null) {
         broker.stop();
         broker = null;
      }
      tmpfolder.delete();
      LOG.info("Apache ActiveMQ Artemis {} ({}, {}) is shutdown", new Object[]{getBrokerVersion(), getBrokerName(), brokerId});
   }

   // Properties
   // -------------------------------------------------------------------------

   public Broker getBroker() throws Exception {
      if (broker == null) {
         broker = createBroker(tmpfolder.getRoot());
      }
      return broker;
   }

   public String getBrokerName() {
      return brokerName;
   }

   public void setBrokerName(String brokerName) {
      if (brokerName == null) {
         throw new NullPointerException("The broker name cannot be null");
      }
      String str = brokerName.replaceAll("[^a-zA-Z0-9\\.\\_\\-\\:]", "_");
      if (!str.equals(brokerName)) {
         LOG.error("Broker Name: {} contained illegal characters - replaced with {}", brokerName, str);
      }
      this.brokerName = str.trim();
   }

   protected Broker createBroker(File temporaryFile) throws Exception {
      broker = createBrokerWrapper(temporaryFile);
      return broker;
   }

   private Broker createBrokerWrapper(File temporaryFile) {
      return new ArtemisBrokerWrapper(this, temporaryFile);
   }

   public void makeSureDestinationExists(ActiveMQDestination activemqDestination) throws Exception {
      ArtemisBrokerWrapper hqBroker = (ArtemisBrokerWrapper) this.broker;
      //it can be null
      if (activemqDestination == null) {
         return;
      }
      if (activemqDestination.isQueue()) {
         String qname = activemqDestination.getPhysicalName();
         System.out.println("physical name: " + qname);
         hqBroker.makeSureQueueExists(qname);
      }
   }

   public boolean enableSsl() {
      return this.SERVER_SIDE_KEYSTORE != null;
   }

   //below are methods called directly by tests
   //we don't actually implement any of these for now,
   //just to make test compile pass.

   //we may get class cast exception as in TestSupport it
   //casts the broker to RegionBroker, which we didn't
   //implement (wrap) yet. Consider solving it later.
   public Broker getRegionBroker() {
      return broker;
   }

   public void setPersistenceAdapter(PersistenceAdapter persistenceAdapter) throws IOException {
   }

   public File getDataDirectoryFile() {
      if (dataDirectoryFile == null) {
         dataDirectoryFile = new File(IOHelper.getDefaultDataDirectory());
      }
      return dataDirectoryFile;
   }

   public File getBrokerDataDirectory() {
      String brokerDir = getBrokerName();
      return new File(getDataDirectoryFile(), brokerDir);
   }

   public PersistenceAdapter getPersistenceAdapter() throws IOException {
      return null;
   }

   public void waitUntilStopped() {
   }

   public boolean waitUntilStarted() {
      return true;
   }

   public void setDestinationPolicy(PolicyMap policyMap) {
      this.destinationPolicy = policyMap;
   }

   public void setDeleteAllMessagesOnStartup(boolean deletePersistentMessagesOnStartup) {
   }

   public void setUseJmx(boolean useJmx) {
   }

   public ManagementContext getManagementContext() {
      return null;
   }

   public BrokerView getAdminView() throws Exception {
      return null;
   }

   public List<TransportConnector> getTransportConnectors() {
      return transportConnectors;
   }

   public TransportConnector addConnector(String bindAddress) throws Exception {
      return addConnector(new URI(bindAddress));
   }

   public void setIoExceptionHandler(IOExceptionHandler ioExceptionHandler) {
   }

   public void setPersistent(boolean persistent) {
   }

   public boolean isSlave() {
      return false;
   }

   public Destination getDestination(ActiveMQDestination destination) throws Exception {
      return null;
   }

   public void setAllowTempAutoCreationOnSend(boolean allowTempAutoCreationOnSend) {
   }

   public void setDedicatedTaskRunner(boolean dedicatedTaskRunner) {
   }

   public void setAdvisorySupport(boolean advisorySupport) {
   }

   public void setUseShutdownHook(boolean useShutdownHook) {
   }

   public void deleteAllMessages() throws IOException {
   }

   public Service[] getServices() {
      return null;
   }

   public void setPopulateUserNameInMBeans(boolean value) {
   }

   public void setDestinations(ActiveMQDestination[] destinations) {
   }

   public URI getVmConnectorURI() {
      return null;
   }

   public SystemUsage getSystemUsage() {
      if (systemUsage == null) {
         systemUsage = new SystemUsage();
      }
      return systemUsage;
   }

   public synchronized PListStore getTempDataStore() {
      return null;
   }

   public void setJmsBridgeConnectors(JmsConnector[] jmsConnectors) {
   }

   public void setDestinationInterceptors(DestinationInterceptor[] destinationInterceptors) {
   }

   public SslContext getSslContext() {
      return this.sslContext;
   }

   public void setDataDirectory(String dataDirectory) {
   }

   public void setPlugins(BrokerPlugin[] plugins) {
   }

   public void setKeepDurableSubsActive(boolean keepDurableSubsActive) {
   }

   public TransportConnector getConnectorByName(String connectorName) {
      return null;
   }

   public TransportConnector addConnector(TransportConnector connector) throws Exception {
      return connector;
   }

   public void setEnableStatistics(boolean enableStatistics) {
   }

   public void setSystemUsage(SystemUsage memoryManager) {
      this.systemUsage = memoryManager;
   }

   public void setManagementContext(ManagementContext managementContext) {
   }

   public void setSchedulerDirectoryFile(File schedulerDirectory) {
   }

   public NetworkConnector addNetworkConnector(String discoveryAddress) throws Exception {
      return addNetworkConnector(new URI(discoveryAddress));
   }

   public NetworkConnector addNetworkConnector(URI discoveryAddress) throws Exception {
      NetworkConnector connector = new DiscoveryNetworkConnector(discoveryAddress);
      return addNetworkConnector(connector);
   }

   public List<NetworkConnector> getNetworkConnectors() {
      return this.networkConnectors;
   }

   public void setSchedulerSupport(boolean schedulerSupport) {
   }

   public void setPopulateJMSXUserID(boolean populateJMSXUserID) {
   }

   public boolean isUseJmx() {
      return false;
   }

   public boolean isPersistent() {
      return false;
   }

   public TransportConnector getTransportConnectorByScheme(String scheme) {
      return null;
   }

   public TaskRunnerFactory getTaskRunnerFactory() {
      return null;
   }

   public boolean isStarted() {
      if (broker == null)
         return false;
      return !broker.isStopped();
   }

   public ProxyConnector addProxyConnector(ProxyConnector connector) throws Exception {
      return connector;
   }

   public void setDataDirectoryFile(File dataDirectoryFile) {
      this.dataDirectoryFile = dataDirectoryFile;
   }

   public PolicyMap getDestinationPolicy() {
      return this.destinationPolicy;
   }

   public void setTransportConnectorURIs(String[] transportConnectorURIs) {
   }

   public boolean isPopulateJMSXUserID() {
      return false;
   }

   public NetworkConnector getNetworkConnectorByName(String connectorName) {
      return null;
   }

   public boolean removeNetworkConnector(NetworkConnector connector) {
      return true;
   }

   public void setTransportConnectors(List<TransportConnector> transportConnectors) throws Exception {
      this.transportConnectors = transportConnectors;
      for (TransportConnector connector : transportConnectors) {
         if (connector.getUri().getScheme().equals("ssl")) {
            boolean added = this.extraConnectors.add(new ConnectorInfo(connector.getUri().getPort(), true));
            if (added) {
               System.out.println("added ssl connector " + connector);
            }
            else {
               System.out.println("WARNing! failed to add ssl connector: " + connector);
            }
         }
         else {
            boolean added = this.extraConnectors.add(new ConnectorInfo(connector.getUri().getPort()));
            if (added) {
               System.out.println("added connector " + connector);
            }
            else {
               System.out.println("WARNing! failed to add connector: " + connector);
            }
         }
      }
   }

   public NetworkConnector addNetworkConnector(NetworkConnector connector) throws Exception {
      connector.setBrokerService(this);

      System.out.println("------------------------ this broker uri: " + this.getConnectURI());
      connector.setLocalUri(this.getConnectURI());
      // Set a connection filter so that the connector does not establish loop
      // back connections.
      connector.setConnectionFilter(new ConnectionFilter() {
         @Override
         public boolean connectTo(URI location) {
            List<TransportConnector> transportConnectors = getTransportConnectors();
            for (Iterator<TransportConnector> iter = transportConnectors.iterator(); iter.hasNext();) {
               try {
                  TransportConnector tc = iter.next();
                  if (location.equals(tc.getConnectUri())) {
                     return false;
                  }
               } catch (Throwable e) {
               }
            }
            return true;
         }
      });

      networkConnectors.add(connector);
      return connector;
   }

   public void setTempDataStore(PListStore tempDataStore) {
   }

   public void setJobSchedulerStore(JobSchedulerStore jobSchedulerStore) {
   }

   public ObjectName getBrokerObjectName() throws MalformedObjectNameException {
      return null;
   }

   public TransportConnector addConnector(URI bindAddress) throws Exception {
      Integer port = bindAddress.getPort();
      String host = bindAddress.getHost();
      FakeTransportConnector connector = null;

      host = (host == null || host.length() == 0) ? "localhost" : host;
      if ("0.0.0.0".equals(host)) {
         host = "localhost";
      }

      if (port == 0) {
         //In actual impl in amq5, after connector has been added the socket
         //is bound already. This means in case of 0 port uri, the random
         //port is available after this call. With artemis wrapper however
         //the real binding happens during broker start. To work around this
         //we use manually calculated port for that.
         port = getPseudoRandomPort();

      }

      System.out.println("Now host is: " + host);
      bindAddress = new URI(bindAddress.getScheme(), bindAddress.getUserInfo(),
              host, port, bindAddress.getPath(), bindAddress.getQuery(), bindAddress.getFragment());

      connector = new FakeTransportConnector(bindAddress);
      this.transportConnectors.add(connector);
      this.extraConnectors.add(new ConnectorInfo(port));

      return connector;
   }

   private int getPseudoRandomPort() {
      int port = RANDOM_PORT_BASE.getAndIncrement();
      int maxTry = 20;
      while (!checkPort(port)) {
         port = RANDOM_PORT_BASE.getAndIncrement();
         maxTry--;
         if (maxTry == 0) {
            LOG.error("Too many port used");
            break;
         }
         try {
            TimeUnit.SECONDS.sleep(5);
         }
         catch (InterruptedException e) {
         }
      }
      return port;
   }

   public static boolean checkPort(final int port) {
      ServerSocket ssocket = null;
      try {
         ssocket = new ServerSocket(port);
      }
      catch (Exception e) {
         LOG.info("port " + port + " is being used.");
         return false;
      }
      finally {
         if (ssocket != null) {
            try {
               ssocket.close();
            }
            catch (IOException e) {
            }
         }
      }
      return true;
   }

   public void setCacheTempDestinations(boolean cacheTempDestinations) {
   }

   public void setOfflineDurableSubscriberTimeout(long offlineDurableSubscriberTimeout) {
   }

   public void setOfflineDurableSubscriberTaskSchedule(long offlineDurableSubscriberTaskSchedule) {
   }

   public boolean isStopped() {
      return broker != null ? broker.isStopped() : true;
   }

   public void setBrokerId(String brokerId) {
   }

   public BrokerPlugin[] getPlugins() {
      return null;
   }

   public void stopAllConnectors(ServiceStopper stopper) {
   }

   public void setMessageAuthorizationPolicy(MessageAuthorizationPolicy messageAuthorizationPolicy) {
   }

   public void setNetworkConnectorStartAsync(boolean networkConnectorStartAsync) {
   }

   public boolean isRestartAllowed() {
      return true;
   }

   public void setTaskRunnerFactory(TaskRunnerFactory taskRunnerFactory) {
   }

   public void start(boolean force) throws Exception {
      this.start();
   }

   public void setMonitorConnectionSplits(boolean monitorConnectionSplits) {
   }

   public void setUseMirroredQueues(boolean useMirroredQueues) {
   }

   public File getTmpDataDirectory() {
      return null;
   }

   public boolean isUseShutdownHook() {
      return true;
   }

   public boolean isDeleteAllMessagesOnStartup() {
      return false;
   }

   public void setUseVirtualTopics(boolean useVirtualTopics) {
   }

   public boolean isUseLoggingForShutdownErrors() {
      return true;
   }

   public TransportConnector addConnector(TransportServer transport) throws Exception {
      return null;
   }

   public synchronized JobSchedulerStore getJobSchedulerStore() {
      return null;
   }

   public boolean removeConnector(TransportConnector connector) throws Exception {
      return true;
   }

   public ConnectionContext getAdminConnectionContext() throws Exception {
      return null;
   }

   public void setUseAuthenticatedPrincipalForJMSXUserID(boolean useAuthenticatedPrincipalForJMSXUserID) {
   }

   public void setSchedulePeriodForDestinationPurge(int schedulePeriodForDestinationPurge) {
   }

   public void setMbeanInvocationTimeout(long mbeanInvocationTimeout) {
   }

   public void setNetworkConnectors(List<?> networkConnectors) throws Exception {
   }

   public void removeDestination(ActiveMQDestination destination) throws Exception {
   }

   public void setMaxPurgedDestinationsPerSweep(int maxPurgedDestinationsPerSweep) {
   }

   public void setBrokerObjectName(ObjectName brokerObjectName) {
   }

   public Map<String, String> getTransportConnectorURIsAsMap() {
      return null;
   }

   public void setSslContext(SslContext sslContext) {
      this.sslContext = sslContext;
      if (sslContext instanceof SpringSslContext) {
         SpringSslContext springContext = (SpringSslContext)sslContext;
         this.SERVER_SIDE_KEYSTORE = springContext.getKeyStore();
         this.KEYSTORE_PASSWORD = springContext.getKeyStorePassword();
         this.SERVER_SIDE_TRUSTSTORE = springContext.getTrustStore();
         this.TRUSTSTORE_PASSWORD = springContext.getTrustStorePassword();
         this.storeType = springContext.getKeyStoreType();
      }
   }

   public void setPersistenceFactory(PersistenceAdapterFactory persistenceFactory) {
   }

   protected TransportConnector createTransportConnector(URI brokerURI) throws Exception {
      return null;
   }

   public String getDefaultUri() {
      return "tcp://localhost:61616";
   }

   public static boolean checkStopped() {
      boolean runningBrokers = false;
      for (Map.Entry<Broker, Exception> brokerExceptionEntry : map.entrySet()) {
         Broker b = brokerExceptionEntry.getKey();
         if (!b.isStopped()) {
            try {
               b.stop();
            }
            catch (Exception e) {
               e.printStackTrace();
            }
            brokerExceptionEntry.getValue().printStackTrace();
            runningBrokers = true;
         }
      }
      map.clear();
      return runningBrokers;
   }

   public URI getConnectURI() {
      URI uri = null;
      try {
         if (this.extraConnectors.size() > 0) {
            ConnectorInfo info = extraConnectors.iterator().next();
            Integer port = info.port;
            String schema = info.ssl ? "ssl" : "tcp";
            uri = new URI(schema + "://localhost:" + port);
         } else {
            uri = new URI(this.getDefaultUri());
         }
      } catch (URISyntaxException e) {
         //ignore
      }
      return uri;
   }

   public static class ConnectorInfo {

      public int port;
      public boolean ssl;

      public ConnectorInfo(int port) {
         this(port, false);
      }

      public ConnectorInfo(int port, boolean ssl) {
         this.port = port;
         this.ssl = ssl;
      }

      @Override
      public int hashCode() {
         return port;
      }

      @Override
      public boolean equals(Object obj) {
         if (obj instanceof ConnectorInfo) {
            return this.port == ((ConnectorInfo)obj).port;
         }
         return false;
      }
   }
}



