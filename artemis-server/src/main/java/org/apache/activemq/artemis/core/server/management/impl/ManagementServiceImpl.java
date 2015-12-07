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
package org.apache.activemq.artemis.core.server.management.impl;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.NotificationBroadcasterSupport;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.activemq.artemis.api.core.BroadcastGroupConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.management.AcceptorControl;
import org.apache.activemq.artemis.api.core.management.BridgeControl;
import org.apache.activemq.artemis.api.core.management.BroadcastGroupControl;
import org.apache.activemq.artemis.api.core.management.ClusterConnectionControl;
import org.apache.activemq.artemis.api.core.management.DivertControl;
import org.apache.activemq.artemis.api.core.management.ManagementHelper;
import org.apache.activemq.artemis.api.core.management.ObjectNameBuilder;
import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.core.config.BridgeConfiguration;
import org.apache.activemq.artemis.core.config.ClusterConnectionConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.DivertConfiguration;
import org.apache.activemq.artemis.core.management.impl.AcceptorControlImpl;
import org.apache.activemq.artemis.core.management.impl.AddressControlImpl;
import org.apache.activemq.artemis.core.management.impl.BridgeControlImpl;
import org.apache.activemq.artemis.core.management.impl.BroadcastGroupControlImpl;
import org.apache.activemq.artemis.core.management.impl.ClusterConnectionControlImpl;
import org.apache.activemq.artemis.core.management.impl.DivertControlImpl;
import org.apache.activemq.artemis.core.management.impl.ActiveMQServerControlImpl;
import org.apache.activemq.artemis.core.management.impl.QueueControlImpl;
import org.apache.activemq.artemis.core.messagecounter.MessageCounter;
import org.apache.activemq.artemis.core.messagecounter.MessageCounterManager;
import org.apache.activemq.artemis.core.messagecounter.impl.MessageCounterManagerImpl;
import org.apache.activemq.artemis.core.paging.PagingManager;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.remoting.server.RemotingService;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.Divert;
import org.apache.activemq.artemis.core.server.ActiveMQMessageBundle;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.QueueFactory;
import org.apache.activemq.artemis.core.server.ServerMessage;
import org.apache.activemq.artemis.core.server.cluster.Bridge;
import org.apache.activemq.artemis.core.server.cluster.BroadcastGroup;
import org.apache.activemq.artemis.core.server.cluster.ClusterConnection;
import org.apache.activemq.artemis.core.server.impl.ServerMessageImpl;
import org.apache.activemq.artemis.core.server.management.ManagementService;
import org.apache.activemq.artemis.core.server.management.Notification;
import org.apache.activemq.artemis.core.server.management.NotificationListener;
import org.apache.activemq.artemis.core.settings.HierarchicalRepository;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.core.transaction.ResourceManager;
import org.apache.activemq.artemis.spi.core.remoting.Acceptor;
import org.apache.activemq.artemis.utils.ConcurrentHashSet;
import org.apache.activemq.artemis.utils.TypedProperties;

public class ManagementServiceImpl implements ManagementService {
   // Constants -----------------------------------------------------

   private static final boolean isTrace = ActiveMQServerLogger.LOGGER.isTraceEnabled();

   private final MBeanServer mbeanServer;

   private final boolean jmxManagementEnabled;

   private final Map<String, Object> registry;

   private final NotificationBroadcasterSupport broadcaster;

   private PostOffice postOffice;

   private PagingManager pagingManager;

   private StorageManager storageManager;

   private ActiveMQServer messagingServer;

   private HierarchicalRepository<Set<Role>> securityRepository;

   private HierarchicalRepository<AddressSettings> addressSettingsRepository;

   private ActiveMQServerControlImpl messagingServerControl;

   private MessageCounterManager messageCounterManager;

   private final SimpleString managementNotificationAddress;

   private final SimpleString managementAddress;

   private boolean started = false;

   private final boolean messageCounterEnabled;

   private boolean notificationsEnabled;

   private final Set<NotificationListener> listeners = new ConcurrentHashSet<NotificationListener>();

   private final ObjectNameBuilder objectNameBuilder;

   // Static --------------------------------------------------------

   // Constructor ----------------------------------------------------

   public ManagementServiceImpl(final MBeanServer mbeanServer, final Configuration configuration) {
      this.mbeanServer = mbeanServer;
      jmxManagementEnabled = configuration.isJMXManagementEnabled();
      messageCounterEnabled = configuration.isMessageCounterEnabled();
      managementAddress = configuration.getManagementAddress();
      managementNotificationAddress = configuration.getManagementNotificationAddress();

      registry = new ConcurrentHashMap<String, Object>();
      broadcaster = new NotificationBroadcasterSupport();
      notificationsEnabled = true;
      objectNameBuilder = ObjectNameBuilder.create(configuration.getJMXDomain());
   }

   // Public --------------------------------------------------------

   // ManagementService implementation -------------------------

   @Override
   public ObjectNameBuilder getObjectNameBuilder() {
      return objectNameBuilder;
   }

   @Override
   public MessageCounterManager getMessageCounterManager() {
      return messageCounterManager;
   }

   @Override
   public void setStorageManager(final StorageManager storageManager) {
      this.storageManager = storageManager;
   }

   @Override
   public ActiveMQServerControlImpl registerServer(final PostOffice postOffice,
                                                   final StorageManager storageManager1,
                                                   final Configuration configuration,
                                                   final HierarchicalRepository<AddressSettings> addressSettingsRepository,
                                                   final HierarchicalRepository<Set<Role>> securityRepository,
                                                   final ResourceManager resourceManager,
                                                   final RemotingService remotingService,
                                                   final ActiveMQServer messagingServer,
                                                   final QueueFactory queueFactory,
                                                   final ScheduledExecutorService scheduledThreadPool,
                                                   final PagingManager pagingManager,
                                                   final boolean backup) throws Exception {
      this.postOffice = postOffice;
      this.addressSettingsRepository = addressSettingsRepository;
      this.securityRepository = securityRepository;
      this.storageManager = storageManager1;
      this.messagingServer = messagingServer;
      this.pagingManager = pagingManager;

      messageCounterManager = new MessageCounterManagerImpl(scheduledThreadPool);
      messageCounterManager.setMaxDayCount(configuration.getMessageCounterMaxDayHistory());
      messageCounterManager.reschedule(configuration.getMessageCounterSamplePeriod());

      messagingServerControl = new ActiveMQServerControlImpl(postOffice, configuration, resourceManager, remotingService, messagingServer, messageCounterManager, storageManager1, broadcaster);
      ObjectName objectName = objectNameBuilder.getActiveMQServerObjectName();
      registerInJMX(objectName, messagingServerControl);
      registerInRegistry(ResourceNames.CORE_SERVER, messagingServerControl);

      return messagingServerControl;
   }

   @Override
   public synchronized void unregisterServer() throws Exception {
      ObjectName objectName = objectNameBuilder.getActiveMQServerObjectName();
      unregisterFromJMX(objectName);
      unregisterFromRegistry(ResourceNames.CORE_SERVER);
   }

   @Override
   public synchronized void registerAddress(final SimpleString address) throws Exception {
      ObjectName objectName = objectNameBuilder.getAddressObjectName(address);
      AddressControlImpl addressControl = new AddressControlImpl(address, postOffice, pagingManager, storageManager, securityRepository);

      registerInJMX(objectName, addressControl);

      registerInRegistry(ResourceNames.CORE_ADDRESS + address, addressControl);

      if (ActiveMQServerLogger.LOGGER.isDebugEnabled()) {
         ActiveMQServerLogger.LOGGER.debug("registered address " + objectName);
      }
   }

   @Override
   public synchronized void unregisterAddress(final SimpleString address) throws Exception {
      ObjectName objectName = objectNameBuilder.getAddressObjectName(address);

      unregisterFromJMX(objectName);
      unregisterFromRegistry(ResourceNames.CORE_ADDRESS + address);
   }

   @Override
   public synchronized void registerQueue(final Queue queue,
                                          final SimpleString address,
                                          final StorageManager storageManager) throws Exception {
      QueueControlImpl queueControl = new QueueControlImpl(queue, address.toString(), postOffice, storageManager, addressSettingsRepository);
      if (messageCounterManager != null) {
         MessageCounter counter = new MessageCounter(queue.getName().toString(), null, queue, false, queue.isDurable(), messageCounterManager.getMaxDayCount());
         queueControl.setMessageCounter(counter);
         messageCounterManager.registerMessageCounter(queue.getName().toString(), counter);
      }
      ObjectName objectName = objectNameBuilder.getQueueObjectName(address, queue.getName());
      registerInJMX(objectName, queueControl);
      registerInRegistry(ResourceNames.CORE_QUEUE + queue.getName(), queueControl);

      if (ActiveMQServerLogger.LOGGER.isDebugEnabled()) {
         ActiveMQServerLogger.LOGGER.debug("registered queue " + objectName);
      }
   }

   @Override
   public synchronized void unregisterQueue(final SimpleString name, final SimpleString address) throws Exception {
      ObjectName objectName = objectNameBuilder.getQueueObjectName(address, name);
      unregisterFromJMX(objectName);
      unregisterFromRegistry(ResourceNames.CORE_QUEUE + name);
      messageCounterManager.unregisterMessageCounter(name.toString());
   }

   @Override
   public synchronized void registerDivert(final Divert divert, final DivertConfiguration config) throws Exception {
      ObjectName objectName = objectNameBuilder.getDivertObjectName(divert.getUniqueName().toString());
      DivertControl divertControl = new DivertControlImpl(divert, storageManager, config);
      registerInJMX(objectName, new StandardMBean(divertControl, DivertControl.class));
      registerInRegistry(ResourceNames.CORE_DIVERT + config.getName(), divertControl);

      if (ActiveMQServerLogger.LOGGER.isDebugEnabled()) {
         ActiveMQServerLogger.LOGGER.debug("registered divert " + objectName);
      }
   }

   @Override
   public synchronized void unregisterDivert(final SimpleString name) throws Exception {
      ObjectName objectName = objectNameBuilder.getDivertObjectName(name.toString());
      unregisterFromJMX(objectName);
      unregisterFromRegistry(ResourceNames.CORE_DIVERT + name);
   }

   @Override
   public synchronized void registerAcceptor(final Acceptor acceptor,
                                             final TransportConfiguration configuration) throws Exception {
      ObjectName objectName = objectNameBuilder.getAcceptorObjectName(configuration.getName());
      AcceptorControl control = new AcceptorControlImpl(acceptor, storageManager, configuration);
      registerInJMX(objectName, new StandardMBean(control, AcceptorControl.class));
      registerInRegistry(ResourceNames.CORE_ACCEPTOR + configuration.getName(), control);
   }

   @Override
   public void unregisterAcceptors() {
      List<String> acceptors = new ArrayList<String>();
      synchronized (this) {
         for (String resourceName : registry.keySet()) {
            if (resourceName.startsWith(ResourceNames.CORE_ACCEPTOR)) {
               acceptors.add(resourceName);
            }
         }
      }

      for (String acceptor : acceptors) {
         String name = acceptor.substring(ResourceNames.CORE_ACCEPTOR.length());
         try {
            unregisterAcceptor(name);
         }
         catch (Exception e) {
            e.printStackTrace();
         }
      }
   }

   public synchronized void unregisterAcceptor(final String name) throws Exception {
      ObjectName objectName = objectNameBuilder.getAcceptorObjectName(name);
      unregisterFromJMX(objectName);
      unregisterFromRegistry(ResourceNames.CORE_ACCEPTOR + name);
   }

   @Override
   public synchronized void registerBroadcastGroup(final BroadcastGroup broadcastGroup,
                                                   final BroadcastGroupConfiguration configuration) throws Exception {
      broadcastGroup.setNotificationService(this);
      ObjectName objectName = objectNameBuilder.getBroadcastGroupObjectName(configuration.getName());
      BroadcastGroupControl control = new BroadcastGroupControlImpl(broadcastGroup, storageManager, configuration);
      registerInJMX(objectName, new StandardMBean(control, BroadcastGroupControl.class));
      registerInRegistry(ResourceNames.CORE_BROADCAST_GROUP + configuration.getName(), control);
   }

   @Override
   public synchronized void unregisterBroadcastGroup(final String name) throws Exception {
      ObjectName objectName = objectNameBuilder.getBroadcastGroupObjectName(name);
      unregisterFromJMX(objectName);
      unregisterFromRegistry(ResourceNames.CORE_BROADCAST_GROUP + name);
   }

   @Override
   public synchronized void registerBridge(final Bridge bridge,
                                           final BridgeConfiguration configuration) throws Exception {
      bridge.setNotificationService(this);
      ObjectName objectName = objectNameBuilder.getBridgeObjectName(configuration.getName());
      BridgeControl control = new BridgeControlImpl(bridge, storageManager, configuration);
      registerInJMX(objectName, new StandardMBean(control, BridgeControl.class));
      registerInRegistry(ResourceNames.CORE_BRIDGE + configuration.getName(), control);
   }

   @Override
   public synchronized void unregisterBridge(final String name) throws Exception {
      ObjectName objectName = objectNameBuilder.getBridgeObjectName(name);
      unregisterFromJMX(objectName);
      unregisterFromRegistry(ResourceNames.CORE_BRIDGE + name);
   }

   @Override
   public synchronized void registerCluster(final ClusterConnection cluster,
                                            final ClusterConnectionConfiguration configuration) throws Exception {
      ObjectName objectName = objectNameBuilder.getClusterConnectionObjectName(configuration.getName());
      ClusterConnectionControl control = new ClusterConnectionControlImpl(cluster, storageManager, configuration);
      registerInJMX(objectName, new StandardMBean(control, ClusterConnectionControl.class));
      registerInRegistry(ResourceNames.CORE_CLUSTER_CONNECTION + configuration.getName(), control);
   }

   @Override
   public synchronized void unregisterCluster(final String name) throws Exception {
      ObjectName objectName = objectNameBuilder.getClusterConnectionObjectName(name);
      unregisterFromJMX(objectName);
      unregisterFromRegistry(ResourceNames.CORE_CLUSTER_CONNECTION + name);
   }

   @Override
   public ServerMessage handleMessage(final ServerMessage message) throws Exception {
      // a reply message is sent with the result stored in the message body.
      ServerMessage reply = new ServerMessageImpl(storageManager.generateID(), 512);

      String resourceName = message.getStringProperty(ManagementHelper.HDR_RESOURCE_NAME);
      if (ActiveMQServerLogger.LOGGER.isDebugEnabled()) {
         ActiveMQServerLogger.LOGGER.debug("handling management message for " + resourceName);
      }

      String operation = message.getStringProperty(ManagementHelper.HDR_OPERATION_NAME);

      if (operation != null) {
         Object[] params = ManagementHelper.retrieveOperationParameters(message);

         if (params == null) {
            params = new Object[0];
         }

         try {
            Object result = invokeOperation(resourceName, operation, params);

            ManagementHelper.storeResult(reply, result);

            reply.putBooleanProperty(ManagementHelper.HDR_OPERATION_SUCCEEDED, true);
         }
         catch (Exception e) {
            ActiveMQServerLogger.LOGGER.managementOperationError(e, operation, resourceName);
            reply.putBooleanProperty(ManagementHelper.HDR_OPERATION_SUCCEEDED, false);
            String exceptionMessage = e.getMessage();
            if (e instanceof InvocationTargetException) {
               exceptionMessage = ((InvocationTargetException) e).getTargetException().getMessage();
            }
            if (e != null) {
               ManagementHelper.storeResult(reply, exceptionMessage);
            }
         }
      }
      else {
         String attribute = message.getStringProperty(ManagementHelper.HDR_ATTRIBUTE);

         if (attribute != null) {
            try {
               Object result = getAttribute(resourceName, attribute);

               ManagementHelper.storeResult(reply, result);

               reply.putBooleanProperty(ManagementHelper.HDR_OPERATION_SUCCEEDED, true);
            }
            catch (Exception e) {
               ActiveMQServerLogger.LOGGER.managementAttributeError(e, attribute, resourceName);
               reply.putBooleanProperty(ManagementHelper.HDR_OPERATION_SUCCEEDED, false);
               String exceptionMessage = e.getMessage();
               if (e instanceof InvocationTargetException) {
                  exceptionMessage = ((InvocationTargetException) e).getTargetException().getMessage();
               }
               if (e != null) {
                  ManagementHelper.storeResult(reply, exceptionMessage);
               }
            }
         }
      }

      return reply;
   }

   @Override
   public synchronized Object getResource(final String resourceName) {
      return registry.get(resourceName);
   }

   @Override
   public synchronized Object[] getResources(final Class<?> resourceType) {
      List<Object> resources = new ArrayList<Object>();
      Collection<Object> clone = new ArrayList<Object>(registry.values());
      for (Object entry : clone) {
         if (resourceType.isAssignableFrom(entry.getClass())) {
            resources.add(entry);
         }
      }
      return resources.toArray(new Object[resources.size()]);
   }

   private final Set<ObjectName> registeredNames = new HashSet<ObjectName>();

   @Override
   public void registerInJMX(final ObjectName objectName, final Object managedResource) throws Exception {
      if (!jmxManagementEnabled) {
         return;
      }

      synchronized (mbeanServer) {
         unregisterFromJMX(objectName);

         mbeanServer.registerMBean(managedResource, objectName);

         registeredNames.add(objectName);
      }
   }

   @Override
   public synchronized void registerInRegistry(final String resourceName, final Object managedResource) {
      unregisterFromRegistry(resourceName);

      registry.put(resourceName, managedResource);
   }

   @Override
   public synchronized void unregisterFromRegistry(final String resourceName) {
      registry.remove(resourceName);
   }

   // the JMX unregistration is synchronized to avoid race conditions if 2 clients tries to
   // unregister the same resource (e.g. a queue) at the same time since unregisterMBean()
   // will throw an exception if the MBean has already been unregistered
   @Override
   public void unregisterFromJMX(final ObjectName objectName) throws MBeanRegistrationException, InstanceNotFoundException {
      if (!jmxManagementEnabled) {
         return;
      }

      synchronized (mbeanServer) {
         if (mbeanServer.isRegistered(objectName)) {
            mbeanServer.unregisterMBean(objectName);

            registeredNames.remove(objectName);
         }
      }
   }

   @Override
   public void addNotificationListener(final NotificationListener listener) {
      listeners.add(listener);
   }

   @Override
   public void removeNotificationListener(final NotificationListener listener) {
      listeners.remove(listener);
   }

   @Override
   public SimpleString getManagementAddress() {
      return managementAddress;
   }

   @Override
   public SimpleString getManagementNotificationAddress() {
      return managementNotificationAddress;
   }

   // ActiveMQComponent implementation -----------------------------

   @Override
   public void start() throws Exception {
      if (messageCounterEnabled) {
         messageCounterManager.start();
      }

      started = true;
   }

   @Override
   public synchronized void stop() throws Exception {
      Set<String> resourceNames = new HashSet<String>(registry.keySet());

      for (String resourceName : resourceNames) {
         unregisterFromRegistry(resourceName);
      }

      if (jmxManagementEnabled) {
         if (!registeredNames.isEmpty()) {
            List<String> unexpectedResourceNames = new ArrayList<String>();
            for (String name : resourceNames) {
               // only addresses and queues should still be registered
               if (!(name.startsWith(ResourceNames.CORE_ADDRESS) || name.startsWith(ResourceNames.CORE_QUEUE))) {
                  unexpectedResourceNames.add(name);
               }
            }
            if (!unexpectedResourceNames.isEmpty()) {
               ActiveMQServerLogger.LOGGER.managementStopError(unexpectedResourceNames.size(), unexpectedResourceNames);
            }

            for (ObjectName on : registeredNames) {
               try {
                  mbeanServer.unregisterMBean(on);
               }
               catch (Exception ignore) {
               }
            }
         }
      }

      if (messageCounterManager != null) {
         messageCounterManager.stop();

         messageCounterManager.resetAllCounters();

         messageCounterManager.resetAllCounterHistories();

         messageCounterManager.clear();
      }

      listeners.clear();

      registry.clear();

      messagingServer = null;

      securityRepository = null;

      addressSettingsRepository = null;

      messagingServerControl = null;

      messageCounterManager = null;

      postOffice = null;

      pagingManager = null;

      storageManager = null;

      messagingServer = null;

      registeredNames.clear();

      started = false;
   }

   @Override
   public boolean isStarted() {
      return started;
   }

   @Override
   public void sendNotification(final Notification notification) throws Exception {
      if (isTrace) {
         ActiveMQServerLogger.LOGGER.trace("Sending Notification = " + notification +
                                              ", notificationEnabled=" + notificationsEnabled +
                                              " messagingServerControl=" + messagingServerControl);
      }
      // This needs to be synchronized since we need to ensure notifications are processed in strict sequence
      synchronized (this) {
         if (messagingServerControl != null && notificationsEnabled) {
            // We also need to synchronize on the post office notification lock
            // otherwise we can get notifications arriving in wrong order / missing
            // if a notification occurs at same time as sendQueueInfoToQueue is processed
            synchronized (postOffice.getNotificationLock()) {

               // First send to any local listeners
               for (NotificationListener listener : listeners) {
                  try {
                     listener.onNotification(notification);
                  }
                  catch (Exception e) {
                     // Exception thrown from one listener should not stop execution of others
                     ActiveMQServerLogger.LOGGER.errorCallingNotifListener(e);
                  }
               }

               // start sending notification *messages* only when server has initialised
               // Note at backup initialisation we don't want to send notifications either
               // https://jira.jboss.org/jira/browse/HORNETQ-317
               if (messagingServer == null || !messagingServer.isActive()) {
                  if (ActiveMQServerLogger.LOGGER.isDebugEnabled()) {
                     ActiveMQServerLogger.LOGGER.debug("ignoring message " + notification + " as the server is not initialized");
                  }
                  return;
               }

               long messageID = storageManager.generateID();

               ServerMessage notificationMessage = new ServerMessageImpl(messageID, 512);

               // Notification messages are always durable so the user can choose whether to add a durable queue to
               // consume them in
               notificationMessage.setDurable(true);
               notificationMessage.setAddress(managementNotificationAddress);

               if (notification.getProperties() != null) {
                  TypedProperties props = notification.getProperties();
                  for (SimpleString name : notification.getProperties().getPropertyNames()) {
                     notificationMessage.putObjectProperty(name, props.getProperty(name));
                  }
               }

               notificationMessage.putStringProperty(ManagementHelper.HDR_NOTIFICATION_TYPE, new SimpleString(notification.getType().toString()));

               notificationMessage.putLongProperty(ManagementHelper.HDR_NOTIFICATION_TIMESTAMP, System.currentTimeMillis());

               if (notification.getUID() != null) {
                  notificationMessage.putStringProperty(new SimpleString("foobar"), new SimpleString(notification.getUID()));
               }

               postOffice.route(notificationMessage, null, false);
            }
         }
      }
   }

   @Override
   public void enableNotifications(final boolean enabled) {
      notificationsEnabled = enabled;
   }

   public Object getAttribute(final String resourceName, final String attribute) {
      try {
         Object resource = registry.get(resourceName);

         if (resource == null) {
            throw ActiveMQMessageBundle.BUNDLE.cannotFindResource(resourceName);
         }

         Method method = null;

         String upperCaseAttribute = attribute.substring(0, 1).toUpperCase() + attribute.substring(1);
         try {
            method = resource.getClass().getMethod("get" + upperCaseAttribute, new Class[0]);
         }
         catch (NoSuchMethodException nsme) {
            try {
               method = resource.getClass().getMethod("is" + upperCaseAttribute, new Class[0]);
            }
            catch (NoSuchMethodException nsme2) {
               throw ActiveMQMessageBundle.BUNDLE.noGetterMethod(attribute);
            }
         }
         return method.invoke(resource, new Object[0]);
      }
      catch (Throwable t) {
         throw new IllegalStateException("Problem while retrieving attribute " + attribute, t);
      }
   }

   private Object invokeOperation(final String resourceName,
                                  final String operation,
                                  final Object[] params) throws Exception {
      Object resource = registry.get(resourceName);

      if (resource == null) {
         throw ActiveMQMessageBundle.BUNDLE.cannotFindResource(resourceName);
      }

      Method method = null;

      Method[] methods = resource.getClass().getMethods();
      for (Method m : methods) {
         if (m.getName().equals(operation) && m.getParameterTypes().length == params.length) {
            boolean match = true;

            Class<?>[] paramTypes = m.getParameterTypes();

            for (int i = 0; i < paramTypes.length; i++) {
               if (params[i] == null) {
                  continue;
               }
               if (paramTypes[i].isAssignableFrom(params[i].getClass()) || paramTypes[i] == Long.TYPE && params[i].getClass() == Integer.class ||
                  paramTypes[i] == Double.TYPE && params[i].getClass() == Integer.class ||
                  paramTypes[i] == Long.TYPE && params[i].getClass() == Long.class ||
                  paramTypes[i] == Double.TYPE && params[i].getClass() == Double.class ||
                  paramTypes[i] == Integer.TYPE && params[i].getClass() == Integer.class ||
                  paramTypes[i] == Boolean.TYPE && params[i].getClass() == Boolean.class) {
                  // parameter match
               }
               else {
                  match = false;
                  break; // parameter check loop
               }
            }

            if (match) {
               method = m;
               break; // method match loop
            }
         }
      }

      if (method == null) {
         throw ActiveMQMessageBundle.BUNDLE.noOperation(operation, params.length);
      }

      Object result = method.invoke(resource, params);

      return result;
   }

   // Inner classes -------------------------------------------------
}
