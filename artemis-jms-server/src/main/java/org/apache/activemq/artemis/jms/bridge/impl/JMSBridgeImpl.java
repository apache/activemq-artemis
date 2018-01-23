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
package org.apache.activemq.artemis.jms.bridge.impl;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.jms.XASession;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.TransactionRolledbackException;
import javax.transaction.xa.XAResource;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ServiceLoader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQInterruptedException;
import org.apache.activemq.artemis.api.core.client.FailoverEventListener;
import org.apache.activemq.artemis.api.core.client.FailoverEventType;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSConstants;
import org.apache.activemq.artemis.core.client.impl.ClientSessionInternal;
import org.apache.activemq.artemis.jms.bridge.ActiveMQJMSBridgeLogger;
import org.apache.activemq.artemis.jms.bridge.ConnectionFactoryFactory;
import org.apache.activemq.artemis.jms.bridge.DestinationFactory;
import org.apache.activemq.artemis.jms.bridge.JMSBridge;
import org.apache.activemq.artemis.jms.bridge.JMSBridgeControl;
import org.apache.activemq.artemis.jms.bridge.QualityOfServiceMode;
import org.apache.activemq.artemis.jms.client.ActiveMQConnection;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQMessage;
import org.apache.activemq.artemis.jms.server.ActiveMQJMSServerBundle;
import org.apache.activemq.artemis.service.extensions.ServiceUtils;
import org.apache.activemq.artemis.service.extensions.xa.recovery.ActiveMQRegistry;
import org.apache.activemq.artemis.service.extensions.xa.recovery.XARecoveryConfig;
import org.apache.activemq.artemis.utils.PasswordMaskingUtil;

public final class JMSBridgeImpl implements JMSBridge {

   private static final String[] RESOURCE_RECOVERY_CLASS_NAMES = new String[]{"org.jboss.as.messaging.jms.AS7RecoveryRegistry"};

   private static boolean trace = ActiveMQJMSBridgeLogger.LOGGER.isTraceEnabled();

   private static final int TEN_YEARS = 60 * 60 * 24 * 365 * 10; // in ms

   private static final long DEFAULT_FAILOVER_TIMEOUT = 60 * 1000;

   private final Object lock = new Object();

   private String bridgeName = "N/A";

   private String sourceUsername;

   private String sourcePassword;

   private String targetUsername;

   private String targetPassword;

   private TransactionManager tm;

   private String selector;

   private long failureRetryInterval;

   private int maxRetries;

   private QualityOfServiceMode qualityOfServiceMode;

   private int maxBatchSize;

   private long maxBatchTime;

   private String subName;

   private String clientID;

   private volatile boolean addMessageIDInHeader;

   private boolean started;

   private final Object stoppingGuard = new Object();
   private boolean stopping = false;

   private final LinkedList<Message> messages;

   private ConnectionFactoryFactory sourceCff;

   private ConnectionFactoryFactory targetCff;

   private DestinationFactory sourceDestinationFactory;

   private DestinationFactory targetDestinationFactory;

   private Connection sourceConn;

   private Connection targetConn;

   private Destination sourceDestination;

   private Destination targetDestination;

   private Session sourceSession;

   private Session targetSession;

   private MessageConsumer sourceConsumer;

   private MessageProducer targetProducer;

   private BatchTimeChecker timeChecker;

   private ExecutorService executor;

   private long batchExpiryTime;

   private boolean paused;

   private Transaction tx;

   private boolean failed;

   private boolean connectedSource = false;

   private boolean connectedTarget = false;

   private int forwardMode;

   private MBeanServer mbeanServer;

   private ObjectName objectName;

   private Boolean useMaskedPassword;

   private String passwordCodec;

   private long failoverTimeout;

   private static final int FORWARD_MODE_XA = 0;

   private static final int FORWARD_MODE_LOCALTX = 1;

   private static final int FORWARD_MODE_NONTX = 2;

   private ActiveMQRegistry registry;

   private ClassLoader moduleTccl;

   private long messageCount = 0;

   private long abortedMessageCount = 0;

   /*
    * Constructor for MBean
    */
   public JMSBridgeImpl() {
      messages = new LinkedList<>();
      executor = createExecutor();
   }

   public JMSBridgeImpl(final ConnectionFactoryFactory sourceCff,
                        final ConnectionFactoryFactory targetCff,
                        final DestinationFactory sourceDestinationFactory,
                        final DestinationFactory targetDestinationFactory,
                        final String sourceUsername,
                        final String sourcePassword,
                        final String targetUsername,
                        final String targetPassword,
                        final String selector,
                        final long failureRetryInterval,
                        final int maxRetries,
                        final QualityOfServiceMode qosMode,
                        final int maxBatchSize,
                        final long maxBatchTime,
                        final String subName,
                        final String clientID,
                        final boolean addMessageIDInHeader) {

      this(sourceCff, targetCff, sourceDestinationFactory, targetDestinationFactory, sourceUsername, sourcePassword, targetUsername, targetPassword, selector, failureRetryInterval, maxRetries, qosMode, maxBatchSize, maxBatchTime, subName, clientID, addMessageIDInHeader, null, null);
   }

   public JMSBridgeImpl(final ConnectionFactoryFactory sourceCff,
                        final ConnectionFactoryFactory targetCff,
                        final DestinationFactory sourceDestinationFactory,
                        final DestinationFactory targetDestinationFactory,
                        final String sourceUsername,
                        final String sourcePassword,
                        final String targetUsername,
                        final String targetPassword,
                        final String selector,
                        final long failureRetryInterval,
                        final int maxRetries,
                        final QualityOfServiceMode qosMode,
                        final int maxBatchSize,
                        final long maxBatchTime,
                        final String subName,
                        final String clientID,
                        final boolean addMessageIDInHeader,
                        final MBeanServer mbeanServer,
                        final String objectName) {
      this(sourceCff, targetCff, sourceDestinationFactory, targetDestinationFactory, sourceUsername, sourcePassword, targetUsername, targetPassword, selector, failureRetryInterval, maxRetries, qosMode, maxBatchSize, maxBatchTime, subName, clientID, addMessageIDInHeader, mbeanServer, objectName, DEFAULT_FAILOVER_TIMEOUT);
   }

   public JMSBridgeImpl(final ConnectionFactoryFactory sourceCff,
                        final ConnectionFactoryFactory targetCff,
                        final DestinationFactory sourceDestinationFactory,
                        final DestinationFactory targetDestinationFactory,
                        final String sourceUsername,
                        final String sourcePassword,
                        final String targetUsername,
                        final String targetPassword,
                        final String selector,
                        final long failureRetryInterval,
                        final int maxRetries,
                        final QualityOfServiceMode qosMode,
                        final int maxBatchSize,
                        final long maxBatchTime,
                        final String subName,
                        final String clientID,
                        final boolean addMessageIDInHeader,
                        final MBeanServer mbeanServer,
                        final String objectName,
                        final long failoverTimeout) {
      this();

      this.sourceCff = sourceCff;

      this.targetCff = targetCff;

      this.sourceDestinationFactory = sourceDestinationFactory;

      this.targetDestinationFactory = targetDestinationFactory;

      this.sourceUsername = sourceUsername;

      this.sourcePassword = sourcePassword;

      this.targetUsername = targetUsername;

      this.targetPassword = targetPassword;

      this.selector = selector;

      this.failureRetryInterval = failureRetryInterval;

      this.maxRetries = maxRetries;

      qualityOfServiceMode = qosMode;

      this.maxBatchSize = maxBatchSize;

      this.maxBatchTime = maxBatchTime;

      this.subName = subName;

      this.clientID = clientID;

      this.addMessageIDInHeader = addMessageIDInHeader;

      this.failoverTimeout = failoverTimeout;

      checkParams();

      if (mbeanServer != null) {
         if (objectName != null) {
            this.mbeanServer = mbeanServer;

            try {
               JMSBridgeControlImpl controlBean = new JMSBridgeControlImpl(this);
               this.objectName = ObjectName.getInstance(objectName);
               StandardMBean mbean = new StandardMBean(controlBean, JMSBridgeControl.class);
               mbeanServer.registerMBean(mbean, this.objectName);
               ActiveMQJMSBridgeLogger.LOGGER.debug("Registered JMSBridge instance as: " + this.objectName.getCanonicalName());
            } catch (Exception e) {
               throw new IllegalStateException("Failed to register JMSBridge MBean", e);
            }
         } else {
            throw new IllegalArgumentException("objectName is required when specifying an MBeanServer");
         }
      }

      if (JMSBridgeImpl.trace) {
         ActiveMQJMSBridgeLogger.LOGGER.trace("Created " + this);
      }
   }

   // ActiveMQComponent overrides --------------------------------------------------

   @Override
   public JMSBridgeImpl setBridgeName(String name) {
      this.bridgeName = name;
      return this;
   }

   @Override
   public String getBridgeName() {
      return bridgeName;
   }

   @Override
   public synchronized void start() throws Exception {
      synchronized (stoppingGuard) {
         stopping = false;
      }

      moduleTccl = AccessController.doPrivileged(new PrivilegedAction<ClassLoader>() {
         @Override
         public ClassLoader run() {
            return Thread.currentThread().getContextClassLoader();
         }
      });

      locateRecoveryRegistry();

      if (started) {
         ActiveMQJMSBridgeLogger.LOGGER.errorBridgeAlreadyStarted(bridgeName);
         return;
      }

      if (JMSBridgeImpl.trace) {
         ActiveMQJMSBridgeLogger.LOGGER.trace("Starting " + this);
      }

      // bridge has been stopped and is restarted
      if (executor.isShutdown()) {
         executor = createExecutor();
      }

      initPasswords();

      checkParams();

      // There may already be a JTA transaction associated to the thread

      boolean ok;

      // Check to see if the QoSMode requires a TM
      if (qualityOfServiceMode.equals(QualityOfServiceMode.ONCE_AND_ONLY_ONCE) && sourceCff != targetCff) {
         if (tm == null) {
            tm = ServiceUtils.getTransactionManager();
         }

         if (tm == null) {
            ActiveMQJMSBridgeLogger.LOGGER.jmsBridgeTransactionManagerMissing(qualityOfServiceMode, bridgeName);
            throw new RuntimeException();
         }

         // There may already be a JTA transaction associated to the thread

         Transaction toResume = null;
         try {
            toResume = tm.suspend();

            ok = setupJMSObjects();
         } finally {
            if (toResume != null) {
               tm.resume(toResume);
            }
         }
      } else {
         ok = setupJMSObjects();
      }

      if (ok) {
         connectedSource = true;
         connectedTarget = true;
         startSource();
      } else {
         ActiveMQJMSBridgeLogger.LOGGER.errorStartingBridge(bridgeName);
         handleFailureOnStartup();
      }

   }

   private void startSource() throws JMSException {
      // start the source connection

      sourceConn.start();

      started = true;

      if (maxBatchTime != -1) {
         if (JMSBridgeImpl.trace) {
            ActiveMQJMSBridgeLogger.LOGGER.trace("Starting time checker thread");
         }

         timeChecker = new BatchTimeChecker();

         executor.execute(timeChecker);
         batchExpiryTime = System.currentTimeMillis() + maxBatchTime;

         if (JMSBridgeImpl.trace) {
            ActiveMQJMSBridgeLogger.LOGGER.trace("Started time checker thread");
         }
      }

      executor.execute(new SourceReceiver());

      if (JMSBridgeImpl.trace) {
         ActiveMQJMSBridgeLogger.LOGGER.trace("Started " + this);
      }
   }

   private void initPasswords() throws ActiveMQException {
      try {
         if (this.sourcePassword != null) {
            sourcePassword = PasswordMaskingUtil.resolveMask(useMaskedPassword, sourcePassword, passwordCodec);
         }

         if (this.targetPassword != null) {
            targetPassword = PasswordMaskingUtil.resolveMask(useMaskedPassword, targetPassword, passwordCodec);
         }
      } catch (Exception e) {
         throw ActiveMQJMSServerBundle.BUNDLE.errorDecodingPassword(e);
      }
   }

   @Override
   public void stop() throws Exception {
      synchronized (stoppingGuard) {
         if (stopping)
            return;
         stopping = true;
      }

      synchronized (this) {
         if (JMSBridgeImpl.trace) {
            ActiveMQJMSBridgeLogger.LOGGER.trace("Stopping " + this);
         }
         if (!connectedSource && sourceConn != null) {
            sourceConn.close();
         }
         if (!connectedTarget && targetConn != null) {
            targetConn.close();
         }
         synchronized (lock) {
            started = false;

            executor.shutdownNow();
         }

         boolean ok = executor.awaitTermination(60, TimeUnit.SECONDS);

         if (!ok) {
            throw new Exception("fail to stop JMS Bridge");
         }

         if (tx != null) {
            // Terminate any transaction
            if (JMSBridgeImpl.trace) {
               ActiveMQJMSBridgeLogger.LOGGER.trace("Rolling back remaining tx");
            }

            stopSessionFailover();

            try {
               tx.rollback();
               abortedMessageCount += messages.size();
            } catch (Exception ignore) {
               if (JMSBridgeImpl.trace) {
                  ActiveMQJMSBridgeLogger.LOGGER.trace("Failed to rollback", ignore);
               }
            }

            if (JMSBridgeImpl.trace) {
               ActiveMQJMSBridgeLogger.LOGGER.trace("Rolled back remaining tx");
            }
         }

         try {
            sourceConn.close();
         } catch (Exception ignore) {
            if (JMSBridgeImpl.trace) {
               ActiveMQJMSBridgeLogger.LOGGER.trace("Failed to close source conn", ignore);
            }
         }

         if (targetConn != null) {
            try {
               targetConn.close();
            } catch (Exception ignore) {
               if (JMSBridgeImpl.trace) {
                  ActiveMQJMSBridgeLogger.LOGGER.trace("Failed to close target conn", ignore);
               }
            }
         }

         if (JMSBridgeImpl.trace) {
            ActiveMQJMSBridgeLogger.LOGGER.trace("Stopped " + this);
         }
      }
   }

   private void stopSessionFailover() {
      XASession xaSource = (XASession) sourceSession;
      XASession xaTarget = (XASession) targetSession;

      ((ClientSessionInternal) xaSource.getXAResource()).getSessionContext().releaseCommunications();
      ((ClientSessionInternal) xaTarget.getXAResource()).getSessionContext().releaseCommunications();
   }

   @Override
   public synchronized boolean isStarted() {
      return started;
   }

   public void destroy() {
      if (mbeanServer != null && objectName != null) {
         try {
            mbeanServer.unregisterMBean(objectName);
         } catch (Exception e) {
            ActiveMQJMSBridgeLogger.LOGGER.errorUnregisteringBridge(objectName, bridgeName);
         }
      }
   }

   // JMSBridge implementation ------------------------------------------------------------

   @Override
   public synchronized void pause() throws Exception {
      if (JMSBridgeImpl.trace) {
         ActiveMQJMSBridgeLogger.LOGGER.trace("Pausing " + this);
      }

      synchronized (lock) {
         paused = true;

         sourceConn.stop();
      }

      if (JMSBridgeImpl.trace) {
         ActiveMQJMSBridgeLogger.LOGGER.trace("Paused " + this);
      }
   }

   @Override
   public synchronized void resume() throws Exception {
      if (JMSBridgeImpl.trace) {
         ActiveMQJMSBridgeLogger.LOGGER.trace("Resuming " + this);
      }

      synchronized (lock) {
         paused = false;

         sourceConn.start();
      }

      if (JMSBridgeImpl.trace) {
         ActiveMQJMSBridgeLogger.LOGGER.trace("Resumed " + this);
      }
   }

   @Override
   public DestinationFactory getSourceDestinationFactory() {
      return sourceDestinationFactory;
   }

   @Override
   public void setSourceDestinationFactory(final DestinationFactory dest) {
      checkBridgeNotStarted();
      JMSBridgeImpl.checkNotNull(dest, "TargetDestinationFactory");

      sourceDestinationFactory = dest;
   }

   @Override
   public DestinationFactory getTargetDestinationFactory() {
      return targetDestinationFactory;
   }

   @Override
   public void setTargetDestinationFactory(final DestinationFactory dest) {
      checkBridgeNotStarted();
      JMSBridgeImpl.checkNotNull(dest, "TargetDestinationFactory");

      targetDestinationFactory = dest;
   }

   @Override
   public synchronized String getSourceUsername() {
      return sourceUsername;
   }

   @Override
   public synchronized void setSourceUsername(final String name) {
      checkBridgeNotStarted();

      sourceUsername = name;
   }

   @Override
   public synchronized String getSourcePassword() {
      return sourcePassword;
   }

   @Override
   public synchronized void setSourcePassword(final String pwd) {
      checkBridgeNotStarted();

      sourcePassword = pwd;
   }

   @Override
   public synchronized String getTargetUsername() {
      return targetUsername;
   }

   @Override
   public synchronized void setTargetUsername(final String name) {
      checkBridgeNotStarted();

      targetUsername = name;
   }

   @Override
   public synchronized String getTargetPassword() {
      return targetPassword;
   }

   @Override
   public synchronized void setTargetPassword(final String pwd) {
      checkBridgeNotStarted();

      targetPassword = pwd;
   }

   @Override
   public synchronized String getSelector() {
      return selector;
   }

   @Override
   public synchronized void setSelector(final String selector) {
      checkBridgeNotStarted();

      this.selector = selector;
   }

   @Override
   public synchronized long getFailureRetryInterval() {
      return failureRetryInterval;
   }

   @Override
   public synchronized void setFailureRetryInterval(final long interval) {
      checkBridgeNotStarted();
      if (interval < 1) {
         throw new IllegalArgumentException("FailureRetryInterval must be >= 1");
      }

      failureRetryInterval = interval;
   }

   @Override
   public synchronized int getMaxRetries() {
      return maxRetries;
   }

   @Override
   public synchronized void setMaxRetries(final int retries) {
      checkBridgeNotStarted();
      JMSBridgeImpl.checkValidValue(retries, "MaxRetries");

      maxRetries = retries;
   }

   @Override
   public synchronized QualityOfServiceMode getQualityOfServiceMode() {
      return qualityOfServiceMode;
   }

   @Override
   public synchronized void setQualityOfServiceMode(final QualityOfServiceMode mode) {
      checkBridgeNotStarted();
      JMSBridgeImpl.checkNotNull(mode, "QualityOfServiceMode");

      qualityOfServiceMode = mode;
   }

   @Override
   public synchronized int getMaxBatchSize() {
      return maxBatchSize;
   }

   @Override
   public synchronized void setMaxBatchSize(final int size) {
      checkBridgeNotStarted();
      JMSBridgeImpl.checkMaxBatchSize(size);

      maxBatchSize = size;
   }

   @Override
   public synchronized long getMaxBatchTime() {
      return maxBatchTime;
   }

   @Override
   public synchronized void setMaxBatchTime(final long time) {
      checkBridgeNotStarted();
      JMSBridgeImpl.checkValidValue(time, "MaxBatchTime");

      maxBatchTime = time;
   }

   @Override
   public synchronized String getSubscriptionName() {
      return subName;
   }

   @Override
   public synchronized void setSubscriptionName(final String subname) {
      checkBridgeNotStarted();
      subName = subname;
   }

   @Override
   public synchronized String getClientID() {
      return clientID;
   }

   @Override
   public synchronized void setClientID(final String clientID) {
      checkBridgeNotStarted();

      this.clientID = clientID;
   }

   @Override
   public boolean isAddMessageIDInHeader() {
      return addMessageIDInHeader;
   }

   @Override
   public void setAddMessageIDInHeader(final boolean value) {
      addMessageIDInHeader = value;
   }

   @Override
   public synchronized boolean isPaused() {
      return paused;
   }

   @Override
   public synchronized boolean isFailed() {
      return failed;
   }

   @Override
   public synchronized long getMessageCount() {
      return messageCount;
   }

   @Override
   public synchronized long getAbortedMessageCount() {
      return abortedMessageCount;
   }

   @Override
   public synchronized void setSourceConnectionFactoryFactory(final ConnectionFactoryFactory cff) {
      checkBridgeNotStarted();
      JMSBridgeImpl.checkNotNull(cff, "SourceConnectionFactoryFactory");

      sourceCff = cff;
   }

   @Override
   public synchronized void setTargetConnectionFactoryFactory(final ConnectionFactoryFactory cff) {
      checkBridgeNotStarted();
      JMSBridgeImpl.checkNotNull(cff, "TargetConnectionFactoryFactory");

      targetCff = cff;
   }

   @Override
   public void setTransactionManager(final TransactionManager tm) {
      this.tm = tm;
   }

   // Public ---------------------------------------------------------------------------

   // Private -------------------------------------------------------------------

   private synchronized void checkParams() {
      checkNotNull(sourceCff, "sourceCff");
      checkNotNull(targetCff, "targetCff");
      checkNotNull(sourceDestinationFactory, "sourceDestinationFactory");
      checkNotNull(targetDestinationFactory, "targetDestinationFactory");
      checkValidValue(failureRetryInterval, "failureRetryInterval");
      checkValidValue(maxRetries, "maxRetries");
      if (failureRetryInterval == -1 && maxRetries > 0) {
         throw new IllegalArgumentException("If failureRetryInterval == -1 maxRetries must be set to -1");
      }
      checkMaxBatchSize(maxBatchSize);
      checkValidValue(maxBatchTime, "maxBatchTime");
      checkNotNull(qualityOfServiceMode, "qualityOfServiceMode");
   }

   /**
    * Check the object is not null
    *
    * @throws IllegalArgumentException if the object is null
    */
   private static void checkNotNull(final Object obj, final String name) {
      if (obj == null) {
         throw new IllegalArgumentException(name + " cannot be null");
      }
   }

   /**
    * Check the bridge is not started
    *
    * @throws IllegalStateException if the bridge is started
    */
   private void checkBridgeNotStarted() {
      if (started) {
         throw new IllegalStateException("Cannot set bridge attributes while it is started");
      }
   }

   /**
    * Check that value is either equals to -1 or greater than 0
    *
    * @throws IllegalArgumentException if the value is not valid
    */
   private static void checkValidValue(final long value, final String name) {
      if (!(value == -1 || value > 0)) {
         throw new IllegalArgumentException(name + " must be > 0 or -1");
      }
   }

   private static void checkMaxBatchSize(final int size) {
      if (!(size >= 1)) {
         throw new IllegalArgumentException("maxBatchSize must be >= 1");
      }
   }

   private void enlistResources(final Transaction tx) throws Exception {
      if (JMSBridgeImpl.trace) {
         ActiveMQJMSBridgeLogger.LOGGER.trace("Enlisting resources in tx");
      }

      XAResource resSource = ((XASession) sourceSession).getXAResource();

      tx.enlistResource(resSource);

      XAResource resDest = ((XASession) targetSession).getXAResource();

      tx.enlistResource(resDest);

      if (JMSBridgeImpl.trace) {
         ActiveMQJMSBridgeLogger.LOGGER.trace("Enlisted resources in tx");
      }
   }

   private void delistResources(final Transaction tx) {
      if (JMSBridgeImpl.trace) {
         ActiveMQJMSBridgeLogger.LOGGER.trace("Delisting resources from tx");
      }

      XAResource resSource = ((XASession) sourceSession).getXAResource();

      try {
         tx.delistResource(resSource, XAResource.TMSUCCESS);
      } catch (Exception e) {
         if (JMSBridgeImpl.trace) {
            ActiveMQJMSBridgeLogger.LOGGER.trace("Failed to delist source resource", e);
         }
      }

      XAResource resDest = ((XASession) targetSession).getXAResource();

      try {
         tx.delistResource(resDest, XAResource.TMSUCCESS);
      } catch (Exception e) {
         if (JMSBridgeImpl.trace) {
            ActiveMQJMSBridgeLogger.LOGGER.trace("Failed to delist target resource", e);
         }
      }

      if (JMSBridgeImpl.trace) {
         ActiveMQJMSBridgeLogger.LOGGER.trace("Delisted resources from tx");
      }
   }

   private Transaction startTx() throws Exception {
      if (JMSBridgeImpl.trace) {
         ActiveMQJMSBridgeLogger.LOGGER.trace("Starting JTA transaction");
      }

      if (tm == null) {
         tm = ServiceUtils.getTransactionManager();
      }

      // Set timeout to a large value since we do not want to time out while waiting for messages
      // to arrive - 10 years should be enough
      tm.setTransactionTimeout(JMSBridgeImpl.TEN_YEARS);

      tm.begin();

      Transaction tx = tm.getTransaction();

      // Remove the association between current thread - we don't want it
      // we will be committing /rolling back directly on the transaction object

      tm.suspend();

      if (JMSBridgeImpl.trace) {
         ActiveMQJMSBridgeLogger.LOGGER.trace("Started JTA transaction");
      }

      return tx;
   }

   private Connection createConnection(final String username,
                                       final String password,
                                       final ConnectionFactoryFactory cff,
                                       final String clientID,
                                       final boolean isXA,
                                       boolean isSource) throws Exception {
      Connection conn = null;

      try {

         Object cf = cff.createConnectionFactory();

         if (cf instanceof ActiveMQConnectionFactory && registry != null) {
            registry.register(XARecoveryConfig.newConfig((ActiveMQConnectionFactory) cf, username, password, null));
         }

         if (qualityOfServiceMode == QualityOfServiceMode.ONCE_AND_ONLY_ONCE && !(cf instanceof XAConnectionFactory)) {
            throw new IllegalArgumentException("Connection factory must be XAConnectionFactory");
         }

         if (username == null) {
            if (isXA) {
               if (JMSBridgeImpl.trace) {
                  ActiveMQJMSBridgeLogger.LOGGER.trace("Creating an XA connection");
               }
               conn = ((XAConnectionFactory) cf).createXAConnection();
            } else {
               if (JMSBridgeImpl.trace) {
                  ActiveMQJMSBridgeLogger.LOGGER.trace("Creating a non XA connection");
               }
               conn = ((ConnectionFactory) cf).createConnection();
            }
         } else {
            if (isXA) {
               if (JMSBridgeImpl.trace) {
                  ActiveMQJMSBridgeLogger.LOGGER.trace("Creating an XA connection");
               }
               conn = ((XAConnectionFactory) cf).createXAConnection(username, password);
            } else {
               if (JMSBridgeImpl.trace) {
                  ActiveMQJMSBridgeLogger.LOGGER.trace("Creating a non XA connection");
               }
               conn = ((ConnectionFactory) cf).createConnection(username, password);
            }
         }

         if (clientID != null) {
            conn.setClientID(clientID);
         }

         boolean ha = false;
         BridgeFailoverListener failoverListener = null;

         if (conn instanceof ActiveMQConnection) {
            ActiveMQConnectionFactory activeMQCF = (ActiveMQConnectionFactory) cf;
            ha = activeMQCF.isHA();

            if (ha) {
               ActiveMQConnection activeMQConn = (ActiveMQConnection) conn;
               failoverListener = new BridgeFailoverListener(isSource);
               activeMQConn.setFailoverListener(failoverListener);
            }
         }

         conn.setExceptionListener(new BridgeExceptionListener(ha, failoverListener, isSource));

         return conn;
      } catch (JMSException e) {
         try {
            if (conn != null) {
               conn.close();
            }
         } catch (Throwable ignored) {
         }
         throw e;
      }
   }

   /*
    * Source and target on same server
    * --------------------------------
    * If the source and target destinations are on the same server (same resource manager) then,
    * in order to get ONCE_AND_ONLY_ONCE, we simply need to consuming and send in a single
    * local JMS transaction.
    *
    * We actually use a single local transacted session for the other QoS modes too since this
    * is more performant than using DUPS_OK_ACKNOWLEDGE or AUTO_ACKNOWLEDGE session ack modes, so effectively
    * the QoS is upgraded.
    *
    * Source and target on different server
    * -------------------------------------
    * If the source and target destinations are on a different servers (different resource managers) then:
    *
    * If desired QoS is ONCE_AND_ONLY_ONCE, then we start a JTA transaction and enlist the consuming and sending
    * XAResources in that.
    *
    * If desired QoS is DUPLICATES_OK then, we use CLIENT_ACKNOWLEDGE for the consuming session and
    * AUTO_ACKNOWLEDGE (this is ignored) for the sending session if the maxBatchSize == 1, otherwise we
    * use a local transacted session for the sending session where maxBatchSize > 1, since this is more performant
    * When bridging a batch, we make sure to manually acknowledge the consuming session, if it is CLIENT_ACKNOWLEDGE
    * *after* the batch has been sent
    *
    * If desired QoS is AT_MOST_ONCE then, if maxBatchSize == 1, we use AUTO_ACKNOWLEDGE for the consuming session,
    * and AUTO_ACKNOWLEDGE for the sending session.
    * If maxBatchSize > 1, we use CLIENT_ACKNOWLEDGE for the consuming session and a local transacted session for the
    * sending session.
    *
    * When bridging a batch, we make sure to manually acknowledge the consuming session, if it is CLIENT_ACKNOWLEDGE
    * *before* the batch has been sent
    *
    */
   private boolean setupJMSObjects() {
      try {
         if (sourceCff == targetCff) {
            // Source and target destinations are on the server - we can get once and only once
            // just using a local transacted session
            // everything becomes once and only once

            forwardMode = JMSBridgeImpl.FORWARD_MODE_LOCALTX;
         } else {
            // Different servers
            if (qualityOfServiceMode == QualityOfServiceMode.ONCE_AND_ONLY_ONCE) {
               // Use XA
               forwardMode = JMSBridgeImpl.FORWARD_MODE_XA;
            } else {
               forwardMode = JMSBridgeImpl.FORWARD_MODE_NONTX;
            }
         }

         // Lookup the destinations
         sourceDestination = sourceDestinationFactory.createDestination();

         targetDestination = targetDestinationFactory.createDestination();

         // bridging on the same server
         if (forwardMode == JMSBridgeImpl.FORWARD_MODE_LOCALTX) {
            // We simply use a single local transacted session for consuming and sending

            sourceConn = createConnection(sourceUsername, sourcePassword, sourceCff, clientID, false, true);
            sourceSession = sourceConn.createSession(true, Session.SESSION_TRANSACTED);
         } else { // bridging across different servers
            // QoS = ONCE_AND_ONLY_ONCE
            if (forwardMode == JMSBridgeImpl.FORWARD_MODE_XA) {
               // Create an XASession for consuming from the source
               if (JMSBridgeImpl.trace) {
                  ActiveMQJMSBridgeLogger.LOGGER.trace("Creating XA source session");
               }

               sourceConn = createConnection(sourceUsername, sourcePassword, sourceCff, clientID, true, true);
               sourceSession = ((XAConnection) sourceConn).createXASession();
            } else { // QoS = DUPLICATES_OK || AT_MOST_ONCE
               if (JMSBridgeImpl.trace) {
                  ActiveMQJMSBridgeLogger.LOGGER.trace("Creating non XA source session");
               }

               sourceConn = createConnection(sourceUsername, sourcePassword, sourceCff, clientID, false, true);
               if (qualityOfServiceMode == QualityOfServiceMode.AT_MOST_ONCE && maxBatchSize == 1) {
                  sourceSession = sourceConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
               } else {
                  sourceSession = sourceConn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
               }
            }
         }

         if (subName == null) {
            if (selector == null) {
               sourceConsumer = sourceSession.createConsumer(sourceDestination);
            } else {
               sourceConsumer = sourceSession.createConsumer(sourceDestination, selector, false);
            }
         } else {
            // Durable subscription
            if (selector == null) {
               sourceConsumer = sourceSession.createDurableSubscriber((Topic) sourceDestination, subName);
            } else {
               sourceConsumer = sourceSession.createDurableSubscriber((Topic) sourceDestination, subName, selector, false);
            }
         }

         // Now the sending session

         // bridging on the same server
         if (forwardMode == JMSBridgeImpl.FORWARD_MODE_LOCALTX) {
            targetConn = sourceConn;
            targetSession = sourceSession;
         } else { // bridging across different servers
            // QoS = ONCE_AND_ONLY_ONCE
            if (forwardMode == JMSBridgeImpl.FORWARD_MODE_XA) {
               if (JMSBridgeImpl.trace) {
                  ActiveMQJMSBridgeLogger.LOGGER.trace("Creating XA dest session");
               }

               // Create an XA session for sending to the destination

               targetConn = createConnection(targetUsername, targetPassword, targetCff, null, true, false);

               targetSession = ((XAConnection) targetConn).createXASession();
            } else { // QoS = DUPLICATES_OK || AT_MOST_ONCE
               if (JMSBridgeImpl.trace) {
                  ActiveMQJMSBridgeLogger.LOGGER.trace("Creating non XA dest session");
               }

               // Create a standard session for sending to the target

               // If batch size > 1 we use a transacted session since is more efficient

               boolean transacted = maxBatchSize > 1;

               targetConn = createConnection(targetUsername, targetPassword, targetCff, null, false, false);

               targetSession = targetConn.createSession(transacted, transacted ? Session.SESSION_TRANSACTED : Session.AUTO_ACKNOWLEDGE);
            }
         }

         if (forwardMode == JMSBridgeImpl.FORWARD_MODE_XA) {
            if (JMSBridgeImpl.trace) {
               ActiveMQJMSBridgeLogger.LOGGER.trace("Starting JTA transaction");
            }

            tx = startTx();

            enlistResources(tx);
         }

         targetProducer = targetSession.createProducer(null);

         return true;
      } catch (Exception e) {
         // We shouldn't log this, as it's expected when trying to connect when target/source is not available

         // If this fails we should attempt to cleanup or we might end up in some weird state

         // Adding a log.warn, so the use may see the cause of the failure and take actions
         ActiveMQJMSBridgeLogger.LOGGER.bridgeConnectError(e, bridgeName);

         cleanup();

         return false;
      }
   }

   private void cleanup() {
      // Stop the source connection
      try {
         sourceConn.stop();
      } catch (Throwable ignore) {
         if (JMSBridgeImpl.trace) {
            ActiveMQJMSBridgeLogger.LOGGER.trace("Failed to stop source connection", ignore);
         }
      }

      if (tx != null) {
         try {
            delistResources(tx);
         } catch (Throwable ignore) {
            if (JMSBridgeImpl.trace) {
               ActiveMQJMSBridgeLogger.LOGGER.trace("Failed to delist resources", ignore);
            }
         }
         try {
            // Terminate the tx
            tx.rollback();
            abortedMessageCount += messages.size();
         } catch (Throwable ignore) {
            if (JMSBridgeImpl.trace) {
               ActiveMQJMSBridgeLogger.LOGGER.trace("Failed to rollback", ignore);
            }
         }
      }

      // Close the old objects
      try {
         sourceConn.close();
      } catch (Throwable ignore) {
         if (JMSBridgeImpl.trace) {
            ActiveMQJMSBridgeLogger.LOGGER.trace("Failed to close source connection", ignore);
         }
      }
      try {
         if (targetConn != null) {
            targetConn.close();
         }
      } catch (Throwable ignore) {
         if (JMSBridgeImpl.trace) {
            ActiveMQJMSBridgeLogger.LOGGER.trace("Failed to close target connection", ignore);
         }
      }
   }

   private void pause(final long interval) {
      long start = System.currentTimeMillis();
      while (System.currentTimeMillis() - start < failureRetryInterval) {
         try {
            Thread.sleep(failureRetryInterval);
         } catch (InterruptedException ex) {
         }
      }
   }

   private boolean setupJMSObjectsWithRetry() {
      if (JMSBridgeImpl.trace) {
         ActiveMQJMSBridgeLogger.LOGGER.trace("Setting up connections");
      }

      int count = 0;

      while (true && !stopping) {
         boolean ok = setupJMSObjects();

         if (ok) {
            return true;
         }

         count++;

         if (maxRetries != -1 && count == maxRetries) {
            break;
         }

         ActiveMQJMSBridgeLogger.LOGGER.failedToSetUpBridge(failureRetryInterval, bridgeName);

         pause(failureRetryInterval);
      }

      // If we get here then we exceeded maxRetries
      return false;
   }

   private void sendBatch() {
      if (JMSBridgeImpl.trace) {
         ActiveMQJMSBridgeLogger.LOGGER.trace("Sending batch of " + messages.size() + " messages");
      }

      if (paused) {
         // Don't send now
         if (JMSBridgeImpl.trace) {
            ActiveMQJMSBridgeLogger.LOGGER.trace("Paused, so not sending now");
         }

         return;
      }

      if (forwardMode == JMSBridgeImpl.FORWARD_MODE_LOCALTX) {
         sendBatchLocalTx();
      } else if (forwardMode == JMSBridgeImpl.FORWARD_MODE_XA) {
         sendBatchXA();
      } else {
         sendBatchNonTransacted();
      }
   }

   private void sendBatchNonTransacted() {
      try {
         if (qualityOfServiceMode == QualityOfServiceMode.ONCE_AND_ONLY_ONCE || (qualityOfServiceMode == QualityOfServiceMode.AT_MOST_ONCE && maxBatchSize > 1)) {
            // We client ack before sending

            if (JMSBridgeImpl.trace) {
               ActiveMQJMSBridgeLogger.LOGGER.trace("Client acking source session");
            }

            messages.getLast().acknowledge();

            if (JMSBridgeImpl.trace) {
               ActiveMQJMSBridgeLogger.LOGGER.trace("Client acked source session");
            }
         }

         boolean exHappened;

         do {
            exHappened = false;
            try {
               sendMessages();
            } catch (TransactionRolledbackException e) {
               ActiveMQJMSBridgeLogger.LOGGER.transactionRolledBack(e);
               exHappened = true;
            }
         }
         while (exHappened);

         if (maxBatchSize > 1) {
            // The sending session is transacted - we need to commit it

            if (JMSBridgeImpl.trace) {
               ActiveMQJMSBridgeLogger.LOGGER.trace("Committing target session");
            }

            targetSession.commit();

            if (JMSBridgeImpl.trace) {
               ActiveMQJMSBridgeLogger.LOGGER.trace("Committed target session");
            }
         }

         if (qualityOfServiceMode == QualityOfServiceMode.DUPLICATES_OK) {
            // We client ack after sending

            // Note we could actually use Session.DUPS_OK_ACKNOWLEDGE here
            // For a slightly less strong delivery guarantee

            if (JMSBridgeImpl.trace) {
               ActiveMQJMSBridgeLogger.LOGGER.trace("Client acking source session");
            }

            messages.getLast().acknowledge();

            if (JMSBridgeImpl.trace) {
               ActiveMQJMSBridgeLogger.LOGGER.trace("Client acked source session");
            }
         }
      } catch (Exception e) {
         if (!stopping) {
            ActiveMQJMSBridgeLogger.LOGGER.bridgeAckError(e, bridgeName);
         }

         // We don't call failure otherwise failover would be broken with ActiveMQ
         // We let the ExceptionListener to deal with failures

         if (connectedSource) {
            try {
               sourceSession.recover();
            } catch (Throwable ignored) {
            }
         }

      } finally {
         // Clear the messages
         messages.clear();

      }
   }

   private void sendBatchXA() {
      try {
         sendMessages();

         // Commit the JTA transaction and start another
         delistResources(tx);

         if (JMSBridgeImpl.trace) {
            ActiveMQJMSBridgeLogger.LOGGER.trace("Committing JTA transaction");
         }

         tx.commit();

         if (JMSBridgeImpl.trace) {
            ActiveMQJMSBridgeLogger.LOGGER.trace("Committed JTA transaction");
         }
      } catch (Exception e) {
         try {
            // we call this just in case there is a failure other than failover
            tx.rollback();
            abortedMessageCount += messages.size();
         } catch (Throwable ignored) {
         }

         ActiveMQJMSBridgeLogger.LOGGER.bridgeAckError(e, bridgeName);

         //we don't do handle failure here because the tx
         //may be rolledback due to failover. All failure handling
         //will be done through exception listener.
         //handleFailureOnSend();
      } finally {
         try {
            tx = startTx();

            enlistResources(tx);

            // Clear the messages
            messages.clear();

         } catch (Exception e) {
            ActiveMQJMSBridgeLogger.LOGGER.bridgeAckError(e, bridgeName);

            handleFailureOnSend();
         }
      }
   }

   private void sendBatchLocalTx() {
      try {
         sendMessages();

         if (JMSBridgeImpl.trace) {
            ActiveMQJMSBridgeLogger.LOGGER.trace("Committing source session");
         }

         sourceSession.commit();

         if (JMSBridgeImpl.trace) {
            ActiveMQJMSBridgeLogger.LOGGER.trace("Committed source session");
         }

      } catch (Exception e) {
         ActiveMQJMSBridgeLogger.LOGGER.bridgeAckError(e, bridgeName);

         try {
            sourceSession.rollback();
         } catch (Throwable ignored) {
         }
         try {
            targetSession.rollback();
         } catch (Throwable ignored) {
         }

         // We don't call failure here, we let the exception listener to deal with it
      } finally {
         messages.clear();
      }
   }

   private void sendMessages() throws Exception {
      Iterator<Message> iter = messages.iterator();

      Message msg = null;

      while (iter.hasNext()) {
         msg = iter.next();

         if (addMessageIDInHeader) {
            addMessageIDInHeader(msg);
         }

         if (JMSBridgeImpl.trace) {
            ActiveMQJMSBridgeLogger.LOGGER.trace("Sending message " + msg);
         }

         // Make sure the correct time to live gets propagated

         long timeToLive = msg.getJMSExpiration();

         if (timeToLive != 0) {
            timeToLive -= System.currentTimeMillis();

            if (timeToLive <= 0) {
               timeToLive = 1; // Should have already expired - set to 1 so it expires when it is consumed or delivered
            }
         }

         targetProducer.send(targetDestination, msg, msg.getJMSDeliveryMode(), msg.getJMSPriority(), timeToLive);

         messageCount++;
         if (JMSBridgeImpl.trace) {
            ActiveMQJMSBridgeLogger.LOGGER.trace("Sent message " + msg);
         }
      }
   }

   private void handleFailureOnSend() {
      handleFailure(new FailureHandler());
   }

   private void handleFailureOnStartup() {
      handleFailure(new StartupFailureHandler());
   }

   private void handleFailure(final Runnable failureHandler) {
      failed = true;

      // Failure must be handled on a separate thread to the calling thread (either onMessage or start).
      // In the case of onMessage we can't close the connection from inside the onMessage method
      // since it will block waiting for onMessage to complete. In the case of start we want to return
      // from the call before the connections are reestablished so that the caller is not blocked unnecessarily.
      executor.execute(failureHandler);
   }

   private void addMessageIDInHeader(final Message msg) throws Exception {
      // We concatenate the old message id as a header in the message
      // This allows the target to then use this as the JMSCorrelationID of any response message
      // thus enabling a distributed request-response pattern.
      // Each bridge (if there are more than one) in the chain can concatenate the message id
      // So in the case of multiple bridges having routed the message this can be used in a multi-hop
      // distributed request/response
      if (JMSBridgeImpl.trace) {
         ActiveMQJMSBridgeLogger.LOGGER.trace("Adding old message id in Message header");
      }

      JMSBridgeImpl.copyProperties(msg);

      String val = null;

      val = msg.getStringProperty(ActiveMQJMSConstants.AMQ_MESSAGING_BRIDGE_MESSAGE_ID_LIST);

      if (val == null) {
         val = msg.getJMSMessageID();
      } else {
         StringBuffer sb = new StringBuffer(val);

         sb.append(",").append(msg.getJMSMessageID());

         val = sb.toString();
      }

      msg.setStringProperty(ActiveMQJMSConstants.AMQ_MESSAGING_BRIDGE_MESSAGE_ID_LIST, val);
   }

   /*
    * JMS does not let you add a property on received message without first
    * calling clearProperties, so we need to save and re-add all the old properties so we
    * don't lose them!!
    */
   private static void copyProperties(final Message msg) throws JMSException {
      Enumeration<String> en = msg.getPropertyNames();

      Map<String, Object> oldProps = null;

      while (en.hasMoreElements()) {
         String propName = en.nextElement();

         if (oldProps == null) {
            oldProps = new HashMap<>();
         }

         oldProps.put(propName, msg.getObjectProperty(propName));
      }

      msg.clearProperties();

      if (oldProps != null) {

         for (Entry<String, Object> entry : oldProps.entrySet()) {
            String propName = entry.getKey();

            Object val = entry.getValue();

            if (val instanceof byte[] == false) {
               //Can't set byte[] array props through the JMS API - if we're bridging an ActiveMQ Artemis message it might have such props
               msg.setObjectProperty(propName, entry.getValue());
            } else if (msg instanceof ActiveMQMessage) {
               ((ActiveMQMessage) msg).getCoreMessage().putBytesProperty(propName, (byte[]) val);
            }
         }
      }
   }

   /**
    * Creates a 3-sized thread pool executor (1 thread for the sourceReceiver, 1 for the timeChecker
    * and 1 for the eventual failureHandler)
    */
   private ExecutorService createExecutor() {
      ExecutorService service = Executors.newFixedThreadPool(3, new ThreadFactory() {

         ThreadGroup group = new ThreadGroup("JMSBridgeImpl");

         @Override
         public Thread newThread(Runnable r) {
            final Thread thr = new Thread(group, r);
            if (moduleTccl != null) {
               AccessController.doPrivileged(new PrivilegedAction() {
                  @Override
                  public Object run() {
                     thr.setContextClassLoader(moduleTccl);
                     return null;
                  }
               });
            }
            return thr;
         }
      });
      return service;
   }

   // Inner classes ---------------------------------------------------------------

   /**
    * We use a Thread which polls the sourceDestination instead of a MessageListener
    * to ensure that message delivery does not happen concurrently with
    * transaction enlistment of the XAResource (see HORNETQ-27)
    */
   private final class SourceReceiver extends Thread {

      SourceReceiver() {
         super("jmsbridge-source-receiver-thread");
      }

      @Override
      @SuppressWarnings("WaitNotInLoop")
      // both lock.wait(..) either returns, throws or continue, thus avoiding spurious wakes
      public void run() {
         while (started) {
            if (stopping) {
               return;
            }
            synchronized (lock) {
               if (paused || failed) {
                  try {
                     lock.wait(500);
                  } catch (InterruptedException e) {
                     if (stopping) {
                        return;
                     }
                     throw new ActiveMQInterruptedException(e);
                  }
                  continue;
               }

               Message msg = null;
               try {
                  msg = sourceConsumer.receive(1000);

                  if (msg instanceof ActiveMQMessage) {
                     // We need to check the buffer mainly in the case of LargeMessages
                     // As we need to reconstruct the buffer before resending the message
                     ((ActiveMQMessage) msg).checkBuffer();
                  }
               } catch (JMSException jmse) {
                  if (JMSBridgeImpl.trace) {
                     ActiveMQJMSBridgeLogger.LOGGER.trace(this + " exception while receiving a message", jmse);
                  }
               }

               if (msg == null) {
                  try {
                     lock.wait(500);
                  } catch (InterruptedException e) {
                     if (JMSBridgeImpl.trace) {
                        ActiveMQJMSBridgeLogger.LOGGER.trace(this + " thread was interrupted");
                     }
                     if (stopping) {
                        return;
                     }
                     throw new ActiveMQInterruptedException(e);
                  }
                  continue;
               }

               if (JMSBridgeImpl.trace) {
                  ActiveMQJMSBridgeLogger.LOGGER.trace(this + " received message " + msg);
               }

               messages.add(msg);

               batchExpiryTime = System.currentTimeMillis() + maxBatchTime;

               if (JMSBridgeImpl.trace) {
                  ActiveMQJMSBridgeLogger.LOGGER.trace(this + " rescheduled batchExpiryTime to " + batchExpiryTime);
               }

               if (maxBatchSize != -1 && messages.size() >= maxBatchSize) {
                  if (JMSBridgeImpl.trace) {
                     ActiveMQJMSBridgeLogger.LOGGER.trace(this + " maxBatchSize has been reached so sending batch");
                  }

                  sendBatch();

                  if (JMSBridgeImpl.trace) {
                     ActiveMQJMSBridgeLogger.LOGGER.trace(this + " sent batch");
                  }
               }
            }
         }
      }
   }

   private class FailureHandler implements Runnable {

      /**
       * Start the source connection - note the source connection must not be started before
       * otherwise messages will be received and ignored
       */
      protected void startSourceConnection() {
         try {
            sourceConn.start();
         } catch (JMSException e) {
            ActiveMQJMSBridgeLogger.LOGGER.jmsBridgeSrcConnectError(e, bridgeName);
         }
      }

      protected void succeeded() {
         ActiveMQJMSBridgeLogger.LOGGER.bridgeReconnected(bridgeName);
         connectedSource = true;
         connectedTarget = true;
         synchronized (lock) {
            failed = false;

            startSourceConnection();
         }
      }

      protected void failed() {
         // We haven't managed to recreate connections or maxRetries = 0
         ActiveMQJMSBridgeLogger.LOGGER.errorConnectingBridge(bridgeName);

         try {
            stop();
         } catch (Exception ignore) {
         }
      }

      @Override
      public void run() {
         if (JMSBridgeImpl.trace) {
            ActiveMQJMSBridgeLogger.LOGGER.trace("Failure handler running");
         }

         // Clear the messages
         messages.clear();

         cleanup();

         boolean ok = false;

         if (maxRetries > 0 || maxRetries == -1) {
            ActiveMQJMSBridgeLogger.LOGGER.bridgeRetry(failureRetryInterval, bridgeName);

            pause(failureRetryInterval);

            // Now we try
            ok = setupJMSObjectsWithRetry();
         }

         if (!ok) {
            failed();
         } else {
            succeeded();
         }
      }
   }

   private class StartupFailureHandler extends FailureHandler {

      @Override
      protected void failed() {
         // Don't call super
         ActiveMQJMSBridgeLogger.LOGGER.bridgeNotStarted(bridgeName);
      }

      @Override
      protected void succeeded() {
         // Don't call super - a bit ugly in this case but better than taking the lock twice.
         ActiveMQJMSBridgeLogger.LOGGER.bridgeConnected(bridgeName);

         synchronized (lock) {

            connectedSource = true;
            connectedTarget = true;
            failed = false;
            started = true;

            // Start the source connection - note the source connection must not be started before
            // otherwise messages will be received and ignored

            try {
               startSource();
            } catch (JMSException e) {
               ActiveMQJMSBridgeLogger.LOGGER.jmsBridgeSrcConnectError(e, bridgeName);
            }
         }
      }
   }

   private class BatchTimeChecker implements Runnable {

      @Override
      public void run() {
         if (JMSBridgeImpl.trace) {
            ActiveMQJMSBridgeLogger.LOGGER.trace(this + " running");
         }

         synchronized (lock) {
            while (started) {
               long toWait = batchExpiryTime - System.currentTimeMillis();

               if (toWait <= 0) {
                  if (JMSBridgeImpl.trace) {
                     ActiveMQJMSBridgeLogger.LOGGER.trace(this + " waited enough");
                  }

                  synchronized (lock) {
                     if (!failed && !messages.isEmpty()) {
                        if (JMSBridgeImpl.trace) {
                           ActiveMQJMSBridgeLogger.LOGGER.trace(this + " got some messages so sending batch");
                        }

                        sendBatch();

                        if (JMSBridgeImpl.trace) {
                           ActiveMQJMSBridgeLogger.LOGGER.trace(this + " sent batch");
                        }
                     }
                  }

                  batchExpiryTime = System.currentTimeMillis() + maxBatchTime;
               } else {
                  try {
                     if (JMSBridgeImpl.trace) {
                        ActiveMQJMSBridgeLogger.LOGGER.trace(this + " waiting for " + toWait);
                     }

                     lock.wait(toWait);

                     if (JMSBridgeImpl.trace) {
                        ActiveMQJMSBridgeLogger.LOGGER.trace(this + " woke up");
                     }
                  } catch (InterruptedException e) {
                     if (JMSBridgeImpl.trace) {
                        ActiveMQJMSBridgeLogger.LOGGER.trace(this + " thread was interrupted");
                     }
                     if (stopping) {
                        return;
                     }
                     throw new ActiveMQInterruptedException(e);
                  }

               }
            }
         }
      }
   }

   private class BridgeExceptionListener implements ExceptionListener {

      boolean ha;
      BridgeFailoverListener failoverListener;
      private final boolean isSource;

      private BridgeExceptionListener(boolean ha, BridgeFailoverListener failoverListener, boolean isSource) {
         this.ha = ha;
         this.failoverListener = failoverListener;
         this.isSource = isSource;
      }

      @Override
      public void onException(final JMSException e) {
         if (stopping) {
            return;
         }
         ActiveMQJMSBridgeLogger.LOGGER.bridgeFailure(e, bridgeName);
         if (isSource) {
            connectedSource = false;
         } else {
            connectedTarget = false;
         }

         synchronized (lock) {
            if (stopping) {
               return;
            }
            if (failed) {
               // The failure has already been detected and is being handled
               if (JMSBridgeImpl.trace) {
                  ActiveMQJMSBridgeLogger.LOGGER.trace("Failure recovery already in progress");
               }
            } else {
               boolean shouldHandleFailure = true;
               if (ha) {
                  //make sure failover happened
                  shouldHandleFailure = !failoverListener.waitForFailover();
               }

               if (shouldHandleFailure) {
                  handleFailure(new FailureHandler());
               }
            }
         }
      }
   }

   private void locateRecoveryRegistry() {
      if (registry == null) {
         for (String locatorClasse : RESOURCE_RECOVERY_CLASS_NAMES) {
            try {
               ServiceLoader<ActiveMQRegistry> sl = ServiceLoader.load(ActiveMQRegistry.class);
               if (sl.iterator().hasNext()) {
                  registry = sl.iterator().next();
               }
            } catch (Throwable e) {
               ActiveMQJMSBridgeLogger.LOGGER.debug("unable to load  recovery registry " + locatorClasse, e);
            }
            if (registry != null) {
               break;
            }
         }

         if (registry != null) {
            ActiveMQJMSBridgeLogger.LOGGER.debug("Recovery Registry located = " + registry);
         }
      }
   }

   @Override
   public boolean isUseMaskedPassword() {
      return useMaskedPassword;
   }

   @Override
   public void setUseMaskedPassword(boolean maskPassword) {
      this.useMaskedPassword = maskPassword;
   }

   @Override
   public String getPasswordCodec() {
      return passwordCodec;
   }

   @Override
   public void setPasswordCodec(String passwordCodec) {
      this.passwordCodec = passwordCodec;
   }

   private class BridgeFailoverListener implements FailoverEventListener {

      private final boolean isSource;
      volatile FailoverEventType lastEvent;

      private BridgeFailoverListener(boolean isSource) {
         this.isSource = isSource;
      }

      @Override
      public void failoverEvent(FailoverEventType eventType) {
         synchronized (this) {
            lastEvent = eventType;
            if (eventType == FailoverEventType.FAILURE_DETECTED) {
               if (isSource) {
                  connectedSource = false;
               } else {
                  connectedTarget = false;
               }
            }
            this.notify();
         }
      }

      //return true if failover completed successfully
      public boolean waitForFailover() {
         long toWait = failoverTimeout;
         long start = 0;
         long waited = 0;
         boolean timedOut = false;
         FailoverEventType result = null;
         synchronized (this) {
            while ((lastEvent == null || lastEvent == FailoverEventType.FAILURE_DETECTED)) {
               try {
                  if (toWait <= 0) {
                     timedOut = true;
                     break;
                  }
                  start = System.currentTimeMillis();
                  this.wait(toWait);
               } catch (InterruptedException e) {
               } finally {
                  waited = System.currentTimeMillis() - start;
                  toWait = failoverTimeout - waited;
               }
            }
            result = lastEvent;
            lastEvent = null;
         }

         if (timedOut) {
            //timeout, presumably failover failed.
            ActiveMQJMSBridgeLogger.LOGGER.debug("Timed out waiting for failover completion " + this);
            return false;
         }

         /*
         * make sure we reset the connected flags
         * */
         if (result == FailoverEventType.FAILOVER_COMPLETED) {
            if (isSource) {
               connectedSource = true;
            } else {
               connectedTarget = true;
            }
            return true;
         }
         //failover failed, need retry.
         return false;
      }
   }

   public long getFailoverTimeout() {
      return failoverTimeout;
   }

   public void setFailoverTimeout(long failoverTimeout) {
      this.failoverTimeout = failoverTimeout;
   }

}
