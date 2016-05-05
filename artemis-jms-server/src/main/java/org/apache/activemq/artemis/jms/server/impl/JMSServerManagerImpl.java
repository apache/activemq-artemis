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
package org.apache.activemq.artemis.jms.server.impl;

import javax.naming.NamingException;
import javax.transaction.xa.Xid;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.DiscoveryGroupConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.management.AddressControl;
import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.api.jms.JMSFactoryType;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.BindingType;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnectorFactory;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.server.ActivateCallback;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.QueueCreator;
import org.apache.activemq.artemis.core.server.QueueDeleter;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.core.server.management.Notification;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.core.transaction.ResourceManager;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.core.transaction.TransactionDetail;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQDestination;
import org.apache.activemq.artemis.jms.client.ActiveMQQueue;
import org.apache.activemq.artemis.jms.client.ActiveMQTopic;
import org.apache.activemq.artemis.jms.client.SelectorTranslator;
import org.apache.activemq.artemis.jms.persistence.JMSStorageManager;
import org.apache.activemq.artemis.jms.persistence.config.PersistedBindings;
import org.apache.activemq.artemis.jms.persistence.config.PersistedConnectionFactory;
import org.apache.activemq.artemis.jms.persistence.config.PersistedDestination;
import org.apache.activemq.artemis.jms.persistence.config.PersistedType;
import org.apache.activemq.artemis.jms.persistence.impl.journal.JMSJournalStorageManagerImpl;
import org.apache.activemq.artemis.jms.persistence.impl.nullpm.NullJMSStorageManagerImpl;
import org.apache.activemq.artemis.jms.server.ActiveMQJMSServerBundle;
import org.apache.activemq.artemis.jms.server.ActiveMQJMSServerLogger;
import org.apache.activemq.artemis.jms.server.JMSServerManager;
import org.apache.activemq.artemis.jms.server.config.ConnectionFactoryConfiguration;
import org.apache.activemq.artemis.jms.server.config.JMSConfiguration;
import org.apache.activemq.artemis.jms.server.config.JMSQueueConfiguration;
import org.apache.activemq.artemis.jms.server.config.TopicConfiguration;
import org.apache.activemq.artemis.jms.server.config.impl.ConnectionFactoryConfigurationImpl;
import org.apache.activemq.artemis.jms.server.management.JMSManagementService;
import org.apache.activemq.artemis.jms.server.management.JMSNotificationType;
import org.apache.activemq.artemis.jms.server.management.impl.JMSManagementServiceImpl;
import org.apache.activemq.artemis.jms.transaction.JMSTransactionDetail;
import org.apache.activemq.artemis.spi.core.naming.BindingRegistry;
import org.apache.activemq.artemis.utils.TimeAndCounterIDGenerator;
import org.apache.activemq.artemis.utils.TypedProperties;
import org.apache.activemq.artemis.utils.json.JSONArray;
import org.apache.activemq.artemis.utils.json.JSONObject;

/**
 * A Deployer used to create and add to Bindings queues, topics and connection
 * factories. Typically this would only be used in an app server env.
 * <p>
 * JMS Connection Factories and Destinations can be configured either
 * using configuration files or using a JMSConfiguration object.
 * <p>
 * If configuration files are used, JMS resources are redeployed if the
 * files content is changed.
 * If a JMSConfiguration object is used, the JMS resources can not be
 * redeployed.
 */
public class JMSServerManagerImpl implements JMSServerManager, ActivateCallback {

   private static final String REJECT_FILTER = ActiveMQServerImpl.GENERIC_IGNORED_FILTER;

   private BindingRegistry registry;

   private final Map<String, ActiveMQQueue> queues = new HashMap<>();

   private final Map<String, ActiveMQTopic> topics = new HashMap<>();

   private final Map<String, ActiveMQConnectionFactory> connectionFactories = new HashMap<>();

   private final Map<String, List<String>> queueBindings = new HashMap<>();

   private final Map<String, List<String>> topicBindings = new HashMap<>();

   private final Map<String, List<String>> connectionFactoryBindings = new HashMap<>();

   // We keep things cached if objects are created while the JMS is not active
   private final List<Runnable> cachedCommands = new ArrayList<>();

   private final ActiveMQServer server;

   private JMSManagementService jmsManagementService;

   private boolean startCalled;

   private boolean active;

   private JMSConfiguration config;

   private Configuration coreConfig;

   private JMSStorageManager storage;

   private final Map<String, List<String>> unRecoveredBindings = new HashMap<>();

   public JMSServerManagerImpl(final ActiveMQServer server) throws Exception {
      this.server = server;

      this.coreConfig = server.getConfiguration();
   }

   /**
    * This constructor is used by the Application Server's integration
    *
    * @param server
    * @param registry
    * @throws Exception
    */
   public JMSServerManagerImpl(final ActiveMQServer server, final BindingRegistry registry) throws Exception {
      this.server = server;

      this.coreConfig = server.getConfiguration();

      this.registry = registry;
   }

   public JMSServerManagerImpl(final ActiveMQServer server, final JMSConfiguration configuration) throws Exception {
      this.server = server;

      this.coreConfig = server.getConfiguration();

      config = configuration;
   }

   // ActivateCallback implementation -------------------------------------

   @Override
   public void preActivate() {

   }

   @Override
   public synchronized void activated() {
      if (!startCalled) {
         return;
      }

      try {

         jmsManagementService = new JMSManagementServiceImpl(server.getManagementService(), server, this);

         jmsManagementService.registerJMSServer(this);

         // Must be set to active before calling initJournal
         active = true;

         initJournal();

         deploy();

         for (Runnable run : cachedCommands) {
            ActiveMQJMSServerLogger.LOGGER.serverRunningCachedCommand(run);
            run.run();
         }

         // do not clear the cachedCommands - HORNETQ-1047

         recoverBindings();
      }
      catch (Exception e) {
         active = false;
         ActiveMQJMSServerLogger.LOGGER.jmsDeployerStartError(e);
      }
   }

   @Override
   public void deActivate() {
      try {
         synchronized (this) {
            if (!active) {
               return;
            }

            // Storage could be null on a shared store backup server before initialization
            if (storage != null && storage.isStarted()) {
               storage.stop();
            }

            unbindBindings(queueBindings);

            unbindBindings(topicBindings);

            unbindBindings(connectionFactoryBindings);

            for (String connectionFactory : new HashSet<>(connectionFactories.keySet())) {
               shutdownConnectionFactory(connectionFactory);
            }

            connectionFactories.clear();
            connectionFactoryBindings.clear();

            queueBindings.clear();
            queues.clear();

            topicBindings.clear();
            topics.clear();

            // it could be null if a backup
            if (jmsManagementService != null) {
               jmsManagementService.unregisterJMSServer();

               jmsManagementService.stop();
            }

            jmsManagementService = null;

            active = false;
         }
      }
      catch (Exception e) {
         e.printStackTrace();
      }
   }

   @Override
   public void activationComplete() {

   }

   public void recoverregistryBindings(String name, PersistedType type) throws NamingException {
      List<String> bindings = unRecoveredBindings.get(name);
      if ((bindings != null) && (bindings.size() > 0)) {
         Map<String, List<String>> mapBindings;
         Map<String, ?> objects;

         switch (type) {
            case Queue:
               mapBindings = queueBindings;
               objects = queues;
               break;
            case Topic:
               mapBindings = topicBindings;
               objects = topics;
               break;
            default:
            case ConnectionFactory:
               mapBindings = connectionFactoryBindings;
               objects = connectionFactories;
               break;
         }

         Object objectToBind = objects.get(name);

         List<String> bindingsList = mapBindings.get(name);

         if (objectToBind == null) {
            return;
         }

         if (bindingsList == null) {
            bindingsList = new ArrayList<>();
            mapBindings.put(name, bindingsList);
         }

         for (String binding : bindings) {
            bindingsList.add(binding);
            bindToBindings(binding, objectToBind);
         }

         unRecoveredBindings.remove(name);
      }
   }

   private void recoverBindings() throws Exception {
      //now its time to add journal recovered stuff
      List<PersistedBindings> bindingsSpace = storage.recoverPersistedBindings();

      for (PersistedBindings record : bindingsSpace) {
         Map<String, List<String>> mapBindings;
         Map<String, ?> objects;

         switch (record.getType()) {
            case Queue:
               mapBindings = queueBindings;
               objects = queues;
               break;
            case Topic:
               mapBindings = topicBindings;
               objects = topics;
               break;
            default:
            case ConnectionFactory:
               mapBindings = connectionFactoryBindings;
               objects = connectionFactories;
               break;
         }

         Object objectToBind = objects.get(record.getName());
         List<String> bindingsList = mapBindings.get(record.getName());

         if (objectToBind == null) {
            unRecoveredBindings.put(record.getName(), record.getBindings());
            continue;
         }

         if (bindingsList == null) {
            bindingsList = new ArrayList<>();
            mapBindings.put(record.getName(), bindingsList);
         }

         for (String bindings : record.getBindings()) {
            bindingsList.add(bindings);
            bindToBindings(bindings, objectToBind);
         }
      }

   }

   // ActiveMQComponent implementation -----------------------------------

   /**
    * Notice that this component has a {@link #startCalled} boolean to control its internal
    * life-cycle, but its {@link #isStarted()} returns the value of {@code server.isStarted()} and
    * not the value of {@link #startCalled}.
    * <p>
    * This method and {@code server.start()} are interdependent in the following way:
    * <ol>
    * <li>{@link JMSServerManagerImpl#start()} is called, it sets {@code start_called=true}, and
    * calls {@link org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl#start()}
    * <li>{@link org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl#start()} will call {@link JMSServerManagerImpl#activated()}
    * <li>{@link JMSServerManagerImpl#activated()} checks the value of {@link #startCalled}, which
    * must already be true.
    * </ol>
    */
   @Override
   public synchronized void start() throws Exception {
      if (startCalled) {
         return;
      }

      server.setJMSQueueCreator(new JMSQueueCreator());

      server.setJMSQueueDeleter(new JMSQueueDeleter());

      server.registerActivateCallback(this);
      /**
       * See this method's javadoc.
       * <p>
       * start_called MUST be set to true BEFORE calling server.start().
       * <p>
       * start_called is NOT used at {@link JMSServerManager#isStarted()}
       */
      startCalled = true;
      server.start();

   }

   @Override
   public void stop() throws Exception {
      synchronized (this) {
         if (!startCalled) {
            return;
         }
         startCalled = false;
         //deactivate in case we haven't been already
         deActivate();
         if (registry != null) {
            registry.close();
         }
      }
      // We have to perform the server.stop outside of the lock because of backup activation issues.
      // See https://bugzilla.redhat.com/show_bug.cgi?id=959616
      // And org.apache.activemq.extras.tests.StartStopDeadlockTest which is validating for this case here
      server.stop();
   }

   @Override
   public boolean isStarted() {
      return server.isStarted();
   }

   // JMSServerManager implementation -------------------------------

   @Override
   public BindingRegistry getRegistry() {
      return registry;
   }

   @Override
   public void setRegistry(BindingRegistry registry) {
      this.registry = registry;
   }

   @Override
   public ActiveMQServer getActiveMQServer() {
      return server;
   }

   @Override
   public void addAddressSettings(final String address, final AddressSettings addressSettings) {
      server.getAddressSettingsRepository().addMatch(address, addressSettings);
   }

   @Override
   public AddressSettings getAddressSettings(final String address) {
      return server.getAddressSettingsRepository().getMatch(address);
   }

   @Override
   public void addSecurity(final String addressMatch, final Set<Role> roles) {
      server.getSecurityRepository().addMatch(addressMatch, roles);
   }

   @Override
   public Set<Role> getSecurity(final String addressMatch) {
      return server.getSecurityRepository().getMatch(addressMatch);
   }

   @Override
   public synchronized String getVersion() {
      checkInitialised();

      return server.getVersion().getFullVersion();
   }

   @Override
   public synchronized boolean createQueue(final boolean storeConfig,
                                           final String queueName,
                                           final String selectorString,
                                           final boolean durable,
                                           final String... bindings) throws Exception {
      return internalCreateJMSQueue(storeConfig, queueName, selectorString, durable, false, bindings);
   }

   protected boolean internalCreateJMSQueue(final boolean storeConfig,
                                            final String queueName,
                                            final String selectorString,
                                            final boolean durable,
                                            final boolean autoCreated,
                                            final String... bindings) throws Exception {

      if (active && queues.get(queueName) != null) {
         return false;
      }

      runAfterActive(new WrappedRunnable() {
         @Override
         public String toString() {
            return "createQueue for " + queueName;
         }

         @Override
         public void runException() throws Exception {
            checkBindings(bindings);

            if (internalCreateQueue(queueName, selectorString, durable, autoCreated)) {

               ActiveMQDestination destination = queues.get(queueName);
               if (destination == null) {
                  // sanity check. internalCreateQueue should already have done this check
                  throw new IllegalArgumentException("Queue does not exist");
               }

               String[] usedBindings = null;

               if (bindings != null) {
                  ArrayList<String> bindingsToAdd = new ArrayList<>();

                  for (String bindingsItem : bindings) {
                     if (bindToBindings(bindingsItem, destination)) {
                        bindingsToAdd.add(bindingsItem);
                     }
                  }

                  usedBindings = bindingsToAdd.toArray(new String[bindingsToAdd.size()]);
                  addToBindings(queueBindings, queueName, usedBindings);
               }

               if (storeConfig && durable) {
                  storage.storeDestination(new PersistedDestination(PersistedType.Queue, queueName, selectorString, durable));
                  if (usedBindings != null) {
                     storage.addBindings(PersistedType.Queue, queueName, usedBindings);
                  }
               }
            }
         }
      });

      sendNotification(JMSNotificationType.QUEUE_CREATED, queueName);
      return true;
   }

   @Override
   public synchronized boolean createTopic(final boolean storeConfig,
                                           final String topicName,
                                           final String... bindings) throws Exception {
      if (active && topics.get(topicName) != null) {
         return false;
      }

      runAfterActive(new WrappedRunnable() {
         @Override
         public String toString() {
            return "createTopic for " + topicName;
         }

         @Override
         public void runException() throws Exception {
            checkBindings(bindings);

            if (internalCreateTopic(topicName)) {
               ActiveMQDestination destination = topics.get(topicName);

               if (destination == null) {
                  // sanity check. internalCreateQueue should already have done this check
                  throw new IllegalArgumentException("Queue does not exist");
               }

               ArrayList<String> bindingsToAdd = new ArrayList<>();

               if (bindings != null) {
                  for (String bindingsItem : bindings) {
                     if (bindToBindings(bindingsItem, destination)) {
                        bindingsToAdd.add(bindingsItem);
                     }
                  }
               }

               String[] usedBindings = bindingsToAdd.toArray(new String[bindingsToAdd.size()]);
               addToBindings(topicBindings, topicName, usedBindings);

               if (storeConfig) {
                  storage.storeDestination(new PersistedDestination(PersistedType.Topic, topicName));
                  storage.addBindings(PersistedType.Topic, topicName, usedBindings);
               }
            }
         }
      });

      sendNotification(JMSNotificationType.TOPIC_CREATED, topicName);
      return true;

   }

   @Override
   public boolean addTopicToBindingRegistry(final String topicName, final String registryBinding) throws Exception {
      checkInitialised();

      checkBindings(registryBinding);

      ActiveMQTopic destination = topics.get(topicName);
      if (destination == null) {
         throw new IllegalArgumentException("Topic does not exist");
      }
      if (destination.getTopicName() == null) {
         throw new IllegalArgumentException(topicName + " is not a topic");
      }
      boolean added = bindToBindings(registryBinding, destination);

      if (added) {
         addToBindings(topicBindings, topicName, registryBinding);
         storage.addBindings(PersistedType.Topic, topicName, registryBinding);
      }
      return added;
   }

   @Override
   public String[] getBindingsOnQueue(String queue) {
      return getBindingsList(queueBindings, queue);
   }

   @Override
   public String[] getBindingsOnTopic(String topic) {
      return getBindingsList(topicBindings, topic);
   }

   @Override
   public String[] getBindingsOnConnectionFactory(String factoryName) {
      return getBindingsList(connectionFactoryBindings, factoryName);
   }

   @Override
   public boolean addQueueToBindingRegistry(final String queueName, final String registryBinding) throws Exception {
      checkInitialised();

      checkBindings(registryBinding);

      ActiveMQQueue destination = queues.get(queueName);
      if (destination == null) {
         throw new IllegalArgumentException("Queue does not exist");
      }
      if (destination.getQueueName() == null) {
         throw new IllegalArgumentException(queueName + " is not a queue");
      }
      boolean added = bindToBindings(registryBinding, destination);
      if (added) {
         addToBindings(queueBindings, queueName, registryBinding);
         storage.addBindings(PersistedType.Queue, queueName, registryBinding);
      }
      return added;
   }

   @Override
   public boolean addConnectionFactoryToBindingRegistry(final String name,
                                                        final String registryBinding) throws Exception {
      checkInitialised();

      checkBindings(registryBinding);

      ActiveMQConnectionFactory factory = connectionFactories.get(name);
      if (factory == null) {
         throw new IllegalArgumentException("Factory does not exist");
      }
      if (registry.lookup(registryBinding) != null) {
         throw ActiveMQJMSServerBundle.BUNDLE.cfBindingsExists(name);
      }
      boolean added = bindToBindings(registryBinding, factory);
      if (added) {
         addToBindings(connectionFactoryBindings, name, registryBinding);
         storage.addBindings(PersistedType.ConnectionFactory, name, registryBinding);
      }
      return added;
   }

   @Override
   public boolean removeQueueFromBindingRegistry(String name, String bindings) throws Exception {
      checkInitialised();

      boolean removed = removeFromBindings(queueBindings, name, bindings);

      if (removed) {
         storage.deleteBindings(PersistedType.Queue, name, bindings);
      }

      return removed;
   }

   @Override
   public boolean removeQueueFromBindingRegistry(final String name) throws Exception {
      final AtomicBoolean valueReturn = new AtomicBoolean(false);

      // HORNETQ-911 - make this runAfterActive to prevent WARN messages on shutdown/undeployment when the backup was never activated
      runAfterActive(new WrappedRunnable() {
         @Override
         public String toString() {
            return "removeQueueFromBindings for " + name;
         }

         @Override
         public void runException() throws Exception {
            checkInitialised();

            if (removeFromBindings(queues, queueBindings, name)) {
               storage.deleteDestination(PersistedType.Queue, name);
               valueReturn.set(true);
            }
         }
      });

      return valueReturn.get();
   }

   @Override
   public boolean removeTopicFromBindingRegistry(String name, String bindings) throws Exception {
      checkInitialised();

      if (removeFromBindings(topicBindings, name, bindings)) {
         storage.deleteBindings(PersistedType.Topic, name, bindings);
         return true;
      }
      else {
         return false;
      }
   }

   /* (non-Javadoc)
   * @see org.apache.activemq.artemis.jms.server.JMSServerManager#removeTopicFromBindings(java.lang.String, java.lang.String)
   */
   @Override
   public boolean removeTopicFromBindingRegistry(final String name) throws Exception {
      final AtomicBoolean valueReturn = new AtomicBoolean(false);

      // HORNETQ-911 - make this runAfterActive to prevent WARN messages on shutdown/undeployment when the backup was never activated
      runAfterActive(new WrappedRunnable() {
         @Override
         public String toString() {
            return "removeTopicFromBindings for " + name;
         }

         @Override
         public void runException() throws Exception {
            checkInitialised();

            if (removeFromBindings(topics, topicBindings, name)) {
               storage.deleteDestination(PersistedType.Topic, name);
               valueReturn.set(true);
            }
         }
      });

      return valueReturn.get();
   }

   @Override
   public boolean removeConnectionFactoryFromBindingRegistry(String name, String bindings) throws Exception {
      checkInitialised();

      removeFromBindings(connectionFactoryBindings, name, bindings);

      storage.deleteBindings(PersistedType.ConnectionFactory, name, bindings);

      return true;
   }

   @Override
   public boolean removeConnectionFactoryFromBindingRegistry(String name) throws Exception {
      checkInitialised();

      removeFromBindings(connectionFactories, connectionFactoryBindings, name);

      storage.deleteConnectionFactory(name);

      return true;
   }

   @Override
   public synchronized boolean destroyQueue(final String name) throws Exception {
      return destroyQueue(name, true);
   }

   @Override
   public synchronized boolean destroyQueue(final String name, final boolean removeConsumers) throws Exception {
      checkInitialised();

      server.destroyQueue(ActiveMQDestination.createQueueAddressFromName(name), null, !removeConsumers, removeConsumers);

      // if the queue has consumers and 'removeConsumers' is false then the queue won't actually be removed
      // therefore only remove the queue from Bindings, etc. if the queue is actually removed
      if (this.server.getPostOffice().getBinding(ActiveMQDestination.createQueueAddressFromName(name)) == null) {
         removeFromBindings(queues, queueBindings, name);

         queues.remove(name);
         queueBindings.remove(name);

         jmsManagementService.unregisterQueue(name);

         storage.deleteDestination(PersistedType.Queue, name);

         sendNotification(JMSNotificationType.QUEUE_DESTROYED, name);
         return true;
      }
      else {
         return false;
      }
   }

   @Override
   public synchronized boolean destroyTopic(final String name) throws Exception {
      return destroyTopic(name, true);
   }

   @Override
   public synchronized boolean destroyTopic(final String name, final boolean removeConsumers) throws Exception {
      checkInitialised();
      AddressControl addressControl = (AddressControl) server.getManagementService().getResource(ResourceNames.CORE_ADDRESS + ActiveMQDestination.createTopicAddressFromName(name));
      if (addressControl != null) {
         for (String queueName : addressControl.getQueueNames()) {
            Binding binding = server.getPostOffice().getBinding(new SimpleString(queueName));
            if (binding == null) {
               ActiveMQJMSServerLogger.LOGGER.noQueueOnTopic(queueName, name);
               continue;
            }

            // We can't remove the remote binding. As this would be the bridge associated with the topic on this case
            if (binding.getType() != BindingType.REMOTE_QUEUE) {
               server.destroyQueue(SimpleString.toSimpleString(queueName), null, !removeConsumers, removeConsumers);
            }
         }

         if (addressControl.getQueueNames().length == 0) {
            removeFromBindings(topics, topicBindings, name);

            topics.remove(name);
            topicBindings.remove(name);

            jmsManagementService.unregisterTopic(name);

            storage.deleteDestination(PersistedType.Topic, name);

            sendNotification(JMSNotificationType.TOPIC_DESTROYED, name);
            return true;
         }
         else {
            return false;
         }
      }
      else {
         return false;
      }
   }

   @Override
   public synchronized void createConnectionFactory(final String name,
                                                    final boolean ha,
                                                    final JMSFactoryType cfType,
                                                    final List<String> connectorNames,
                                                    String... registryBindings) throws Exception {
      checkInitialised();
      ActiveMQConnectionFactory cf = connectionFactories.get(name);
      if (cf == null) {
         ConnectionFactoryConfiguration configuration = new ConnectionFactoryConfigurationImpl().setName(name).setHA(ha).setConnectorNames(connectorNames).setFactoryType(cfType);

         createConnectionFactory(true, configuration, registryBindings);
      }
   }

   @Override
   public synchronized void createConnectionFactory(final String name,
                                                    final boolean ha,
                                                    JMSFactoryType cfType,
                                                    final List<String> connectorNames,
                                                    final String clientID,
                                                    final long clientFailureCheckPeriod,
                                                    final long connectionTTL,
                                                    final long callTimeout,
                                                    final long callFailoverTimeout,
                                                    final boolean cacheLargeMessagesClient,
                                                    final int minLargeMessageSize,
                                                    final boolean compressLargeMessage,
                                                    final int consumerWindowSize,
                                                    final int consumerMaxRate,
                                                    final int confirmationWindowSize,
                                                    final int producerWindowSize,
                                                    final int producerMaxRate,
                                                    final boolean blockOnAcknowledge,
                                                    final boolean blockOnDurableSend,
                                                    final boolean blockOnNonDurableSend,
                                                    final boolean autoGroup,
                                                    final boolean preAcknowledge,
                                                    final String loadBalancingPolicyClassName,
                                                    final int transactionBatchSize,
                                                    final int dupsOKBatchSize,
                                                    final boolean useGlobalPools,
                                                    final int scheduledThreadPoolMaxSize,
                                                    final int threadPoolMaxSize,
                                                    final long retryInterval,
                                                    final double retryIntervalMultiplier,
                                                    final long maxRetryInterval,
                                                    final int reconnectAttempts,
                                                    final boolean failoverOnInitialConnection,
                                                    final String groupId,
                                                    String... registryBindings) throws Exception {
      checkInitialised();
      ActiveMQConnectionFactory cf = connectionFactories.get(name);
      if (cf == null) {
         ConnectionFactoryConfiguration configuration = new ConnectionFactoryConfigurationImpl().setName(name).setHA(ha).setConnectorNames(connectorNames).setClientID(clientID).setClientFailureCheckPeriod(clientFailureCheckPeriod).setConnectionTTL(connectionTTL).setFactoryType(cfType).setCallTimeout(callTimeout).setCallFailoverTimeout(callFailoverTimeout).setCacheLargeMessagesClient(cacheLargeMessagesClient).setMinLargeMessageSize(minLargeMessageSize).setConsumerWindowSize(consumerWindowSize).setConsumerMaxRate(consumerMaxRate).setConfirmationWindowSize(confirmationWindowSize).setProducerWindowSize(producerWindowSize).setProducerMaxRate(producerMaxRate).setBlockOnAcknowledge(blockOnAcknowledge).setBlockOnDurableSend(blockOnDurableSend).setBlockOnNonDurableSend(blockOnNonDurableSend).setAutoGroup(autoGroup).setPreAcknowledge(preAcknowledge).setLoadBalancingPolicyClassName(loadBalancingPolicyClassName).setTransactionBatchSize(transactionBatchSize).setDupsOKBatchSize(dupsOKBatchSize).setUseGlobalPools(useGlobalPools).setScheduledThreadPoolMaxSize(scheduledThreadPoolMaxSize).setThreadPoolMaxSize(threadPoolMaxSize).setRetryInterval(retryInterval).setRetryIntervalMultiplier(retryIntervalMultiplier).setMaxRetryInterval(maxRetryInterval).setReconnectAttempts(reconnectAttempts).setFailoverOnInitialConnection(failoverOnInitialConnection).setGroupID(groupId);

         createConnectionFactory(true, configuration, registryBindings);
      }
   }

   @Override
   public synchronized void createConnectionFactory(final String name,
                                                    final boolean ha,
                                                    final JMSFactoryType cfType,
                                                    final String discoveryGroupName,
                                                    final String clientID,
                                                    final long clientFailureCheckPeriod,
                                                    final long connectionTTL,
                                                    final long callTimeout,
                                                    final long callFailoverTimeout,
                                                    final boolean cacheLargeMessagesClient,
                                                    final int minLargeMessageSize,
                                                    final boolean compressLargeMessages,
                                                    final int consumerWindowSize,
                                                    final int consumerMaxRate,
                                                    final int confirmationWindowSize,
                                                    final int producerWindowSize,
                                                    final int producerMaxRate,
                                                    final boolean blockOnAcknowledge,
                                                    final boolean blockOnDurableSend,
                                                    final boolean blockOnNonDurableSend,
                                                    final boolean autoGroup,
                                                    final boolean preAcknowledge,
                                                    final String loadBalancingPolicyClassName,
                                                    final int transactionBatchSize,
                                                    final int dupsOKBatchSize,
                                                    final boolean useGlobalPools,
                                                    final int scheduledThreadPoolMaxSize,
                                                    final int threadPoolMaxSize,
                                                    final long retryInterval,
                                                    final double retryIntervalMultiplier,
                                                    final long maxRetryInterval,
                                                    final int reconnectAttempts,
                                                    final boolean failoverOnInitialConnection,
                                                    final String groupId,
                                                    final String... registryBindings) throws Exception {
      checkInitialised();
      ActiveMQConnectionFactory cf = connectionFactories.get(name);
      if (cf == null) {
         ConnectionFactoryConfiguration configuration = new ConnectionFactoryConfigurationImpl().setName(name).setHA(ha).setBindings(registryBindings).setDiscoveryGroupName(discoveryGroupName).setFactoryType(cfType).setClientID(clientID).setClientFailureCheckPeriod(clientFailureCheckPeriod).setConnectionTTL(connectionTTL).setCallTimeout(callTimeout).setCallFailoverTimeout(callFailoverTimeout).setCacheLargeMessagesClient(cacheLargeMessagesClient).setMinLargeMessageSize(minLargeMessageSize).setCompressLargeMessages(compressLargeMessages).setConsumerWindowSize(consumerWindowSize).setConsumerMaxRate(consumerMaxRate).setConfirmationWindowSize(confirmationWindowSize).setProducerWindowSize(producerWindowSize).setProducerMaxRate(producerMaxRate).setBlockOnAcknowledge(blockOnAcknowledge).setBlockOnDurableSend(blockOnDurableSend).setBlockOnNonDurableSend(blockOnNonDurableSend).setAutoGroup(autoGroup).setPreAcknowledge(preAcknowledge).setLoadBalancingPolicyClassName(loadBalancingPolicyClassName).setTransactionBatchSize(transactionBatchSize).setDupsOKBatchSize(dupsOKBatchSize).setUseGlobalPools(useGlobalPools).setScheduledThreadPoolMaxSize(scheduledThreadPoolMaxSize).setThreadPoolMaxSize(threadPoolMaxSize).setRetryInterval(retryInterval).setRetryIntervalMultiplier(retryIntervalMultiplier).setMaxRetryInterval(maxRetryInterval).setReconnectAttempts(reconnectAttempts).setFailoverOnInitialConnection(failoverOnInitialConnection);
         createConnectionFactory(true, configuration, registryBindings);
      }
   }

   @Override
   public synchronized void createConnectionFactory(final String name,
                                                    final boolean ha,
                                                    final JMSFactoryType cfType,
                                                    final String discoveryGroupName,
                                                    final String... registryBindings) throws Exception {
      checkInitialised();
      ActiveMQConnectionFactory cf = connectionFactories.get(name);
      if (cf == null) {
         ConnectionFactoryConfiguration configuration = new ConnectionFactoryConfigurationImpl().setName(name).setHA(ha).setBindings(registryBindings).setDiscoveryGroupName(discoveryGroupName);
         createConnectionFactory(true, configuration, registryBindings);
      }
   }

   @Override
   public synchronized ActiveMQConnectionFactory recreateCF(String name,
                                                            ConnectionFactoryConfiguration cf) throws Exception {
      List<String> bindings = connectionFactoryBindings.get(name);

      if (bindings == null) {
         throw ActiveMQJMSServerBundle.BUNDLE.cfDoesntExist(name);
      }

      String[] usedBindings = bindings.toArray(new String[bindings.size()]);

      ActiveMQConnectionFactory realCF = internalCreateCFPOJO(cf);

      if (cf.isPersisted()) {
         storage.storeConnectionFactory(new PersistedConnectionFactory(cf));
         storage.addBindings(PersistedType.ConnectionFactory, cf.getName(), usedBindings);
      }

      for (String bindingsElement : usedBindings) {
         this.bindToBindings(bindingsElement, realCF);
      }

      return realCF;
   }

   @Override
   public synchronized void createConnectionFactory(final boolean storeConfig,
                                                    final ConnectionFactoryConfiguration cfConfig,
                                                    final String... bindings) throws Exception {
      runAfterActive(new WrappedRunnable() {

         @Override
         public String toString() {
            return "createConnectionFactory for " + cfConfig.getName();
         }

         @Override
         public void runException() throws Exception {
            checkBindings(bindings);

            ActiveMQConnectionFactory cf = internalCreateCF(cfConfig);

            ArrayList<String> bindingsToAdd = new ArrayList<>();

            for (String bindingsItem : bindings) {
               if (bindToBindings(bindingsItem, cf)) {
                  bindingsToAdd.add(bindingsItem);
               }
            }

            String[] usedBindings = bindingsToAdd.toArray(new String[bindingsToAdd.size()]);
            addToBindings(connectionFactoryBindings, cfConfig.getName(), usedBindings);

            if (storeConfig) {
               storage.storeConnectionFactory(new PersistedConnectionFactory(cfConfig));
               storage.addBindings(PersistedType.ConnectionFactory, cfConfig.getName(), usedBindings);
            }

            JMSServerManagerImpl.this.recoverregistryBindings(cfConfig.getName(), PersistedType.ConnectionFactory);
            sendNotification(JMSNotificationType.CONNECTION_FACTORY_CREATED, cfConfig.getName());
         }
      });
   }

   private void sendNotification(JMSNotificationType type, String message) {
      TypedProperties prop = new TypedProperties();
      prop.putSimpleStringProperty(JMSNotificationType.MESSAGE, SimpleString.toSimpleString(message));
      Notification notif = new Notification(null, type, prop);
      try {
         server.getManagementService().sendNotification(notif);
      }
      catch (Exception e) {
         ActiveMQJMSServerLogger.LOGGER.failedToSendNotification(notif.toString());
      }
   }

   public JMSStorageManager getJMSStorageManager() {
      return storage;
   }

   // used on tests only
   public void replaceStorageManager(JMSStorageManager newStorage) {
      this.storage = newStorage;
   }

   private String[] getBindingsList(final Map<String, List<String>> map, final String name) {
      List<String> result = map.get(name);
      if (result == null) {
         return new String[0];
      }
      else {
         String[] strings = new String[result.size()];
         result.toArray(strings);
         return strings;
      }
   }

   private synchronized boolean internalCreateQueue(final String queueName,
                                       final String selectorString,
                                       final boolean durable) throws Exception {
      return internalCreateQueue(queueName, selectorString, durable, false);
   }

   private synchronized boolean internalCreateQueue(final String queueName,
                                       final String selectorString,
                                       final boolean durable,
                                       final boolean autoCreated) throws Exception {
      if (queues.get(queueName) != null) {
         return false;
      }
      else {
         ActiveMQQueue activeMQQueue = ActiveMQDestination.createQueue(queueName);

         // Convert from JMS selector to core filter
         String coreFilterString = null;

         if (selectorString != null) {
            coreFilterString = SelectorTranslator.convertToActiveMQFilterString(selectorString);
         }

         Queue queue = server.deployQueue(SimpleString.toSimpleString(activeMQQueue.getAddress()), SimpleString.toSimpleString(activeMQQueue.getAddress()), SimpleString.toSimpleString(coreFilterString), durable, false, autoCreated);

         queues.put(queueName, activeMQQueue);

         this.recoverregistryBindings(queueName, PersistedType.Queue);

         jmsManagementService.registerQueue(activeMQQueue, queue);

         return true;
      }
   }

   /**
    * Performs the internal creation without activating any storage.
    * The storage load will call this method
    *
    * @param topicName
    * @return
    * @throws Exception
    */
   private synchronized boolean internalCreateTopic(final String topicName) throws Exception {

      if (topics.get(topicName) != null) {
         return false;
      }
      else {
         ActiveMQTopic activeMQTopic = ActiveMQDestination.createTopic(topicName);
         // We create a dummy subscription on the topic, that never receives messages - this is so we can perform JMS
         // checks when routing messages to a topic that
         // does not exist - otherwise we would not be able to distinguish from a non existent topic and one with no
         // subscriptions - core has no notion of a topic
         server.deployQueue(SimpleString.toSimpleString(activeMQTopic.getAddress()), SimpleString.toSimpleString(activeMQTopic.getAddress()), SimpleString.toSimpleString(JMSServerManagerImpl.REJECT_FILTER), true, false);

         topics.put(topicName, activeMQTopic);

         this.recoverregistryBindings(topicName, PersistedType.Topic);

         jmsManagementService.registerTopic(activeMQTopic);

         return true;
      }
   }

   /**
    * @param cfConfig
    * @throws Exception
    */
   private ActiveMQConnectionFactory internalCreateCF(final ConnectionFactoryConfiguration cfConfig) throws Exception {
      checkInitialised();

      ActiveMQConnectionFactory cf = connectionFactories.get(cfConfig.getName());

      if (cf == null) {
         cf = internalCreateCFPOJO(cfConfig);
      }

      connectionFactories.put(cfConfig.getName(), cf);

      jmsManagementService.registerConnectionFactory(cfConfig.getName(), cfConfig, cf);

      return cf;
   }

   /**
    * @param cfConfig
    * @return
    * @throws ActiveMQException
    */
   protected ActiveMQConnectionFactory internalCreateCFPOJO(final ConnectionFactoryConfiguration cfConfig) throws ActiveMQException {
      ActiveMQConnectionFactory cf;
      if (cfConfig.getDiscoveryGroupName() != null) {
         DiscoveryGroupConfiguration groupConfig = server.getConfiguration().getDiscoveryGroupConfigurations().get(cfConfig.getDiscoveryGroupName());

         if (groupConfig == null) {
            throw ActiveMQJMSServerBundle.BUNDLE.discoveryGroupDoesntExist(cfConfig.getDiscoveryGroupName());
         }

         if (cfConfig.isHA()) {
            cf = ActiveMQJMSClient.createConnectionFactoryWithHA(groupConfig, cfConfig.getFactoryType());
         }
         else {
            cf = ActiveMQJMSClient.createConnectionFactoryWithoutHA(groupConfig, cfConfig.getFactoryType());
         }
      }
      else {
         if (cfConfig.getConnectorNames() == null || cfConfig.getConnectorNames().size() == 0) {
            throw ActiveMQJMSServerBundle.BUNDLE.noConnectorNameOnCF();
         }

         TransportConfiguration[] configs = new TransportConfiguration[cfConfig.getConnectorNames().size()];

         int count = 0;
         for (String name : cfConfig.getConnectorNames()) {
            TransportConfiguration connector = server.getConfiguration().getConnectorConfigurations().get(name);
            if (connector == null) {
               throw ActiveMQJMSServerBundle.BUNDLE.noConnectorNameConfiguredOnCF(name);
            }
            correctInvalidNettyConnectorHost(connector);
            configs[count++] = connector;
         }

         if (cfConfig.isHA()) {
            cf = ActiveMQJMSClient.createConnectionFactoryWithHA(cfConfig.getFactoryType(), configs);
         }
         else {
            cf = ActiveMQJMSClient.createConnectionFactoryWithoutHA(cfConfig.getFactoryType(), configs);
         }
      }

      cf.setClientID(cfConfig.getClientID());
      cf.setClientFailureCheckPeriod(cfConfig.getClientFailureCheckPeriod());
      cf.setConnectionTTL(cfConfig.getConnectionTTL());
      cf.setCallTimeout(cfConfig.getCallTimeout());
      cf.setCallFailoverTimeout(cfConfig.getCallFailoverTimeout());
      cf.setCacheLargeMessagesClient(cfConfig.isCacheLargeMessagesClient());
      cf.setMinLargeMessageSize(cfConfig.getMinLargeMessageSize());
      cf.setConsumerWindowSize(cfConfig.getConsumerWindowSize());
      cf.setConsumerMaxRate(cfConfig.getConsumerMaxRate());
      cf.setConfirmationWindowSize(cfConfig.getConfirmationWindowSize());
      cf.setProducerWindowSize(cfConfig.getProducerWindowSize());
      cf.setProducerMaxRate(cfConfig.getProducerMaxRate());
      cf.setBlockOnAcknowledge(cfConfig.isBlockOnAcknowledge());
      cf.setBlockOnDurableSend(cfConfig.isBlockOnDurableSend());
      cf.setBlockOnNonDurableSend(cfConfig.isBlockOnNonDurableSend());
      cf.setAutoGroup(cfConfig.isAutoGroup());
      cf.setPreAcknowledge(cfConfig.isPreAcknowledge());
      cf.setConnectionLoadBalancingPolicyClassName(cfConfig.getLoadBalancingPolicyClassName());
      cf.setTransactionBatchSize(cfConfig.getTransactionBatchSize());
      cf.setDupsOKBatchSize(cfConfig.getDupsOKBatchSize());
      cf.setUseGlobalPools(cfConfig.isUseGlobalPools());
      cf.setScheduledThreadPoolMaxSize(cfConfig.getScheduledThreadPoolMaxSize());
      cf.setThreadPoolMaxSize(cfConfig.getThreadPoolMaxSize());
      cf.setRetryInterval(cfConfig.getRetryInterval());
      cf.setRetryIntervalMultiplier(cfConfig.getRetryIntervalMultiplier());
      cf.setMaxRetryInterval(cfConfig.getMaxRetryInterval());
      cf.setReconnectAttempts(cfConfig.getReconnectAttempts());
      cf.setFailoverOnInitialConnection(cfConfig.isFailoverOnInitialConnection());
      cf.setCompressLargeMessage(cfConfig.isCompressLargeMessages());
      cf.setGroupID(cfConfig.getGroupID());
      cf.setProtocolManagerFactoryStr(cfConfig.getProtocolManagerFactoryStr());
      return cf;
   }

   @Override
   public synchronized boolean destroyConnectionFactory(final String name) throws Exception {
      final AtomicBoolean valueReturn = new AtomicBoolean(false);

      // HORNETQ-911 - make this runAfterActive to prevent WARN messages on shutdown/undeployment when the backup was never activated
      runAfterActive(new WrappedRunnable() {

         @Override
         public String toString() {
            return "destroyConnectionFactory for " + name;
         }

         @Override
         public void runException() throws Exception {
            shutdownConnectionFactory(name);

            storage.deleteConnectionFactory(name);
            valueReturn.set(true);
         }
      });

      if (valueReturn.get()) {
         sendNotification(JMSNotificationType.CONNECTION_FACTORY_DESTROYED, name);
      }

      return valueReturn.get();
   }

   /**
    * @param name
    * @throws Exception
    */
   protected boolean shutdownConnectionFactory(final String name) throws Exception {
      checkInitialised();
      List<String> registryBindings = connectionFactoryBindings.get(name);

      if (registry != null && registryBindings != null) {
         for (String registryBinding : registryBindings) {
            registry.unbind(registryBinding);
         }
      }

      connectionFactoryBindings.remove(name);
      connectionFactories.remove(name);

      jmsManagementService.unregisterConnectionFactory(name);

      return true;
   }

   @Override
   public String[] listRemoteAddresses() throws Exception {
      checkInitialised();
      return server.getActiveMQServerControl().listRemoteAddresses();
   }

   @Override
   public String[] listRemoteAddresses(final String ipAddress) throws Exception {
      checkInitialised();
      return server.getActiveMQServerControl().listRemoteAddresses(ipAddress);
   }

   @Override
   public boolean closeConnectionsForAddress(final String ipAddress) throws Exception {
      checkInitialised();
      return server.getActiveMQServerControl().closeConnectionsForAddress(ipAddress);
   }

   @Override
   public boolean closeConsumerConnectionsForAddress(final String address) throws Exception {
      checkInitialised();
      return server.getActiveMQServerControl().closeConsumerConnectionsForAddress(address);
   }

   @Override
   public boolean closeConnectionsForUser(final String userName) throws Exception {
      checkInitialised();
      return server.getActiveMQServerControl().closeConnectionsForUser(userName);
   }

   @Override
   public String[] listConnectionIDs() throws Exception {
      return server.getActiveMQServerControl().listConnectionIDs();
   }

   @Override
   public String[] listSessions(final String connectionID) throws Exception {
      checkInitialised();
      return server.getActiveMQServerControl().listSessions(connectionID);
   }

   @Override
   public String listPreparedTransactionDetailsAsJSON() throws Exception {
      ResourceManager resourceManager = server.getResourceManager();
      Map<Xid, Long> xids = resourceManager.getPreparedTransactionsWithCreationTime();
      if (xids == null || xids.size() == 0) {
         return "";
      }

      ArrayList<Entry<Xid, Long>> xidsSortedByCreationTime = new ArrayList<>(xids.entrySet());
      Collections.sort(xidsSortedByCreationTime, new Comparator<Entry<Xid, Long>>() {
         @Override
         public int compare(final Entry<Xid, Long> entry1, final Entry<Xid, Long> entry2) {
            // sort by creation time, oldest first
            return (int) (entry1.getValue() - entry2.getValue());
         }
      });

      JSONArray txDetailListJson = new JSONArray();
      for (Map.Entry<Xid, Long> entry : xidsSortedByCreationTime) {
         Xid xid = entry.getKey();
         Transaction tx = resourceManager.getTransaction(xid);
         if (tx == null) {
            continue;
         }
         TransactionDetail detail = new JMSTransactionDetail(xid, tx, entry.getValue());
         txDetailListJson.put(detail.toJSON());
      }
      return txDetailListJson.toString();
   }

   @Override
   public String listPreparedTransactionDetailsAsHTML() throws Exception {
      ResourceManager resourceManager = server.getResourceManager();
      Map<Xid, Long> xids = resourceManager.getPreparedTransactionsWithCreationTime();
      if (xids == null || xids.size() == 0) {
         return "<h3>*** Prepared Transaction Details ***</h3><p>No entry.</p>";
      }

      ArrayList<Entry<Xid, Long>> xidsSortedByCreationTime = new ArrayList<>(xids.entrySet());
      Collections.sort(xidsSortedByCreationTime, new Comparator<Entry<Xid, Long>>() {
         @Override
         public int compare(final Entry<Xid, Long> entry1, final Entry<Xid, Long> entry2) {
            // sort by creation time, oldest first
            return (int) (entry1.getValue() - entry2.getValue());
         }
      });

      StringBuilder html = new StringBuilder();
      html.append("<h3>*** Prepared Transaction Details ***</h3>");

      for (Map.Entry<Xid, Long> entry : xidsSortedByCreationTime) {
         Xid xid = entry.getKey();
         Transaction tx = resourceManager.getTransaction(xid);
         if (tx == null) {
            continue;
         }
         TransactionDetail detail = new JMSTransactionDetail(xid, tx, entry.getValue());
         JSONObject txJson = detail.toJSON();

         html.append("<table border=\"1\">");
         html.append("<tr><th>creation_time</th>");
         html.append("<td>" + txJson.get(TransactionDetail.KEY_CREATION_TIME) + "</td>");
         html.append("<th>xid_as_base_64</th>");
         html.append("<td colspan=\"3\">" + txJson.get(TransactionDetail.KEY_XID_AS_BASE64) + "</td></tr>");
         html.append("<tr><th>xid_format_id</th>");
         html.append("<td>" + txJson.get(TransactionDetail.KEY_XID_FORMAT_ID) + "</td>");
         html.append("<th>xid_global_txid</th>");
         html.append("<td>" + txJson.get(TransactionDetail.KEY_XID_GLOBAL_TXID) + "</td>");
         html.append("<th>xid_branch_qual</th>");
         html.append("<td>" + txJson.get(TransactionDetail.KEY_XID_BRANCH_QUAL) + "</td></tr>");

         html.append("<tr><th colspan=\"6\">Message List</th></tr>");
         html.append("<tr><td colspan=\"6\">");
         html.append("<table border=\"1\" cellspacing=\"0\" cellpadding=\"0\">");

         JSONArray msgs = txJson.getJSONArray(TransactionDetail.KEY_TX_RELATED_MESSAGES);
         for (int i = 0; i < msgs.length(); i++) {
            JSONObject msgJson = msgs.getJSONObject(i);
            JSONObject props = msgJson.getJSONObject(TransactionDetail.KEY_MSG_PROPERTIES);
            StringBuilder propstr = new StringBuilder();
            Iterator<String> propkeys = props.keys();
            while (propkeys.hasNext()) {
               String key = propkeys.next();
               propstr.append(key);
               propstr.append("=");
               propstr.append(props.get(key));
               propstr.append(", ");
            }

            html.append("<th>operation_type</th>");
            html.append("<td>" + msgJson.get(TransactionDetail.KEY_MSG_OP_TYPE) + "</th>");
            html.append("<th>message_type</th>");
            html.append("<td>" + msgJson.get(TransactionDetail.KEY_MSG_TYPE) + "</td></tr>");
            html.append("<tr><th>properties</th>");
            html.append("<td colspan=\"3\">" + propstr.toString() + "</td></tr>");
         }
         html.append("</table></td></tr>");
         html.append("</table><br/>");
      }

      return html.toString();
   }

   // Public --------------------------------------------------------

   // Private -------------------------------------------------------

   private synchronized void checkInitialised() {
      if (!active) {
         throw new IllegalStateException("Cannot access JMS Server, core server is not active");
      }
   }

   private void addToBindings(Map<String, List<String>> map, String name, String... bindings) {
      List<String> list = map.get(name);
      if (list == null) {
         list = new ArrayList<>();
         map.put(name, list);
      }
      for (String bindingsItem : bindings) {
         list.add(bindingsItem);
      }
   }

   private void checkBindings(final String... bindingsNames) throws NamingException {
      if (bindingsNames != null) {
         for (String bindingsName : bindingsNames) {
            if (registry != null && registry.lookup(bindingsName) != null) {
               throw new NamingException(bindingsName + " already has an object bound");
            }
         }
      }
   }

   private boolean bindToBindings(final String bindingsName, final Object objectToBind) throws NamingException {
      if (registry != null) {
         registry.unbind(bindingsName);
         registry.bind(bindingsName, objectToBind);
      }
      return true;
   }

   private void deploy() throws Exception {
      if (config == null) {
         return;
      }

      List<ConnectionFactoryConfiguration> connectionFactoryConfigurations = config.getConnectionFactoryConfigurations();
      for (ConnectionFactoryConfiguration cfConfig : connectionFactoryConfigurations) {
         createConnectionFactory(false, cfConfig, cfConfig.getBindings());
      }

      List<JMSQueueConfiguration> queueConfigs = config.getQueueConfigurations();
      for (JMSQueueConfiguration qConfig : queueConfigs) {
         createQueue(false, qConfig.getName(), qConfig.getSelector(), qConfig.isDurable(), qConfig.getBindings());
      }

      List<TopicConfiguration> topicConfigs = config.getTopicConfigurations();
      for (TopicConfiguration tConfig : topicConfigs) {
         createTopic(false, tConfig.getName(), tConfig.getBindings());
      }
   }

   /**
    * @param param
    */
   private void unbindBindings(Map<String, List<String>> param) {
      if (registry != null) {
         for (List<String> elementList : param.values()) {
            for (String key : elementList) {
               try {
                  registry.unbind(key);
               }
               catch (Exception e) {
                  ActiveMQJMSServerLogger.LOGGER.bindingsUnbindError(e, key);
               }
            }
         }
      }
   }

   /**
    * @throws Exception
    */
   private void initJournal() throws Exception {
      this.coreConfig = server.getConfiguration();

      createJournal();

      storage.load();

      List<PersistedConnectionFactory> cfs = storage.recoverConnectionFactories();

      for (PersistedConnectionFactory cf : cfs) {
         internalCreateCF(cf.getConfig());
      }

      List<PersistedDestination> destinations = storage.recoverDestinations();

      for (PersistedDestination destination : destinations) {
         if (destination.getType() == PersistedType.Queue) {
            internalCreateQueue(destination.getName(), destination.getSelector(), destination.isDurable());
         }
         else if (destination.getType() == PersistedType.Topic) {
            internalCreateTopic(destination.getName());
         }
      }
   }

   /**
    * @throws Exception
    */
   private void createJournal() throws Exception {
      if (storage == null) {
         if (coreConfig.isPersistenceEnabled()) {
            storage = new JMSJournalStorageManagerImpl(new TimeAndCounterIDGenerator(), server.getConfiguration(), server.getReplicationManager());
         }
         else {
            storage = new NullJMSStorageManagerImpl();
         }
      }
      else {
         if (storage.isStarted()) {
            storage.stop();
         }
      }

      storage.start();
   }

   private synchronized boolean removeFromBindings(final Map<String, ?> keys,
                                                   final Map<String, List<String>> bindingsMap,
                                                   final String name) throws Exception {
      checkInitialised();
      List<String> registryBindings = bindingsMap.remove(name);
      if (registryBindings == null || registryBindings.size() == 0) {
         return false;
      }
      else {
         keys.remove(name);
      }
      if (registry != null) {
         Iterator<String> iter = registryBindings.iterator();
         while (iter.hasNext()) {
            String registryBinding = iter.next();
            registry.unbind(registryBinding);
            iter.remove();
         }
      }
      return true;
   }

   private synchronized boolean removeFromBindings(final Map<String, List<String>> bindingsMap,
                                                   final String name,
                                                   final String bindings) throws Exception {
      checkInitialised();
      List<String> registryBindings = bindingsMap.get(name);
      if (registryBindings == null || registryBindings.size() == 0) {
         return false;
      }

      if (registryBindings.remove(bindings)) {
         registry.unbind(bindings);
         return true;
      }
      else {
         return false;
      }
   }

   private boolean runAfterActive(WrappedRunnable runnable) throws Exception {
      if (active) {
         runnable.runException();
         return true;
      }
      else {
         ActiveMQJMSServerLogger.LOGGER.serverCachingCommand(runnable);
         if (!cachedCommands.contains(runnable))
            cachedCommands.add(runnable);
         return false;
      }
   }

   private abstract class WrappedRunnable implements Runnable {

      @Override
      public void run() {
         try {
            runException();
         }
         catch (Exception e) {
            ActiveMQJMSServerLogger.LOGGER.jmsServerError(e);
         }
      }

      public abstract void runException() throws Exception;
   }

   private void correctInvalidNettyConnectorHost(TransportConfiguration transportConfiguration) {
      Map<String, Object> params = transportConfiguration.getParams();

      if (transportConfiguration.getFactoryClassName().equals(NettyConnectorFactory.class.getCanonicalName()) &&
         params.containsKey(TransportConstants.HOST_PROP_NAME) &&
         params.get(TransportConstants.HOST_PROP_NAME).equals("0.0.0.0")) {
         try {
            String newHost = InetAddress.getLocalHost().getHostName();
            ActiveMQJMSServerLogger.LOGGER.invalidHostForConnector(transportConfiguration.getName(), newHost);
            params.put(TransportConstants.HOST_PROP_NAME, newHost);
         }
         catch (UnknownHostException e) {
            ActiveMQJMSServerLogger.LOGGER.failedToCorrectHost(e, transportConfiguration.getName());
         }
      }
   }

   class JMSQueueCreator implements QueueCreator {
      @Override
      public boolean create(SimpleString address) throws Exception {
         AddressSettings settings = server.getAddressSettingsRepository().getMatch(address.toString());
         if (address.toString().startsWith(ActiveMQDestination.JMS_QUEUE_ADDRESS_PREFIX) && settings.isAutoCreateJmsQueues()) {
            JMSServerManagerImpl.this.internalCreateJMSQueue(false, address.toString().substring(ActiveMQDestination.JMS_QUEUE_ADDRESS_PREFIX.length()), null, true, true);
            return true;
         }
         else {
            return false;
         }
      }
   }

   class JMSQueueDeleter implements QueueDeleter {
      @Override
      public boolean delete(SimpleString address) throws Exception {
         return JMSServerManagerImpl.this.destroyQueue(address.toString().substring(ActiveMQDestination.JMS_QUEUE_ADDRESS_PREFIX.length()), false);
      }
   }
}
