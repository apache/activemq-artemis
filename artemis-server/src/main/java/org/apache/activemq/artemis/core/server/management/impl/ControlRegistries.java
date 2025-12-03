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


import org.apache.activemq.artemis.api.core.management.AcceptorControl;
import org.apache.activemq.artemis.api.core.management.ActiveMQServerControl;
import org.apache.activemq.artemis.api.core.management.AddressControl;
import org.apache.activemq.artemis.api.core.management.BaseBroadcastGroupControl;
import org.apache.activemq.artemis.api.core.management.BridgeControl;
import org.apache.activemq.artemis.api.core.management.BrokerConnectionControl;
import org.apache.activemq.artemis.api.core.management.ClusterConnectionControl;
import org.apache.activemq.artemis.api.core.management.ConnectionRouterControl;
import org.apache.activemq.artemis.api.core.management.DivertControl;
import org.apache.activemq.artemis.api.core.management.QueueControl;
import org.apache.activemq.artemis.api.core.management.RemoteBrokerConnectionControl;
import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.core.management.impl.ActiveMQServerControlImpl;
import org.apache.activemq.artemis.core.server.management.HawtioSecurityControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;


public class ControlRegistries {
   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   public final Map<String, ActiveMQServerControl> serverControls;
   // we keep a second map just for queues for quick searching when many exist
   public final Map<String, QueueControl> queueControls;

   // we keep a second map just for queues for quick searching when many exist
   public final Map<String, AddressControl> addressControls;

   public final Map<String, AcceptorControl> accepterControls;

   public final Map<String, BaseBroadcastGroupControl> broadcastGroupControls;

   public final Map<String, BrokerConnectionControl> brokerConnectionControls;

   public final Map<String, RemoteBrokerConnectionControl> remoteBrokerConnectionControls;

   public final Map<String, BridgeControl> bridgeControls;

   public final Map<String, ClusterConnectionControl> clusterConnectionControl;

   public final Map<String, ConnectionRouterControl> connectionRouterControls;

   public final Map<String, HawtioSecurityControl> hawtioSecurityControls;

   public final Map<String, Object> amqpControls;

   public final Map<String, DivertControl> divertControls;

   public ControlRegistries() {
      serverControls = new ConcurrentHashMap<>();
      queueControls = new ConcurrentHashMap<>();
      addressControls = new ConcurrentHashMap<>();
      accepterControls = new ConcurrentHashMap<>();
      broadcastGroupControls = new ConcurrentHashMap<>();
      brokerConnectionControls = new ConcurrentHashMap<>();
      remoteBrokerConnectionControls = new ConcurrentHashMap<>();
      bridgeControls = new ConcurrentHashMap<>();
      clusterConnectionControl = new ConcurrentHashMap<>();
      connectionRouterControls = new ConcurrentHashMap<>();
      hawtioSecurityControls = new ConcurrentHashMap<>();
      amqpControls = new ConcurrentHashMap<>();
      divertControls = new ConcurrentHashMap<>();
   }

   public void unregisterQueueControls(final String resourceName) {
      queueControls.remove(resourceName);
   }

   public void registerQueueControls(final String resourceName, final QueueControl queueControl) {
      queueControls.put(resourceName, queueControl);
   }

   public void unregisterAddressControls(final String resourceName) {
      addressControls.remove(resourceName);
   }

   public void registerAddressControls(final String resourceName, final AddressControl addressControl) {
      addressControls.put(resourceName, addressControl);
   }

   public void registerAcceptor(String resourceName, AcceptorControl acceptorControl) {
      accepterControls.put(resourceName, acceptorControl);
   }

   public List<QueueControl> getQueueControls(Predicate<QueueControl> predicate) {
      if (predicate == null) {
         return queueControls.values().stream().toList();
      }
      return queueControls.values().stream().filter(predicate).collect(Collectors.toList());
   }

   public List<AddressControl> getAddressControls(Predicate<AddressControl> predicate) {
      if (predicate == null) {
         return addressControls.values().stream().toList();
      }
      return addressControls.values().stream().filter(predicate).collect(Collectors.toList());
   }

   public Set<String> getAcceptorNames() {
      return accepterControls.keySet();
   }

   public void unregisterAcceptorControls(String resourceName) {
      accepterControls.remove(resourceName);
   }

   public void registerBroadcastGroupControls(String resourceName, BaseBroadcastGroupControl control) {
      broadcastGroupControls.put(resourceName, control);
   }

   public void unRegisterBroadcastGroupControls(String resourceName) {
      broadcastGroupControls.remove(resourceName);
   }

   public void registerBrokerConnectionControl(String resourceName, BrokerConnectionControl control) {
      brokerConnectionControls.put(resourceName, control);
   }

   public void unRegisterBrokerConnectionControl(String resourceName) {
      brokerConnectionControls.remove(resourceName);
   }

   public void registerRemoteBrokerConnectionControl(String resourceName, RemoteBrokerConnectionControl control) {
      remoteBrokerConnectionControls.put(resourceName, control);
   }

   public void unRegisterRemoteBrokerConnectionControl(String resourceName) {
      remoteBrokerConnectionControls.remove(resourceName);
   }

   public void registerBridgeControl(String resourceName, BridgeControl control) {
      bridgeControls.put(resourceName, control);
   }

   public void unRegisterBridgeControl(String resourceName) {
      bridgeControls.remove(resourceName);
   }

   public void registerClusterConnectionControl(String resourceName, ClusterConnectionControl control) {
      clusterConnectionControl.put(resourceName, control);
   }

   public void unRegisterClusterConnectionControl(String resourceName) {
      clusterConnectionControl.remove(resourceName);
   }

   public void registerConnectionRouterControl(String resourceName, ConnectionRouterControl connectionRouterControl) {
      connectionRouterControls.put(resourceName, connectionRouterControl);
   }

   public void unRegisterConnectionRouterControl(String resourceName) {
      connectionRouterControls.remove(resourceName);
   }

   public void registerHawtioSecurityControl(String resourceName, HawtioSecurityControl control) {
      hawtioSecurityControls.put(resourceName, control);
   }

   public void unRegisterHawtioSecurityControl(String resourceName) {
      hawtioSecurityControls.remove(resourceName);
   }

   public AddressControl getAddressControl(String resourceName) {
      return addressControls.get(resourceName);
   }

   public AcceptorControl getAcceptorControl(String resourceName) {
      return accepterControls.get(resourceName);
   }

   public void registerAMQPControl(String amqpResourceName, Object control) {
      amqpControls.put(amqpResourceName, control);
   }

   public void unRegisterAMQPControl(String amqpResourceName) {
      amqpControls.remove(amqpResourceName);
   }

   public void registerBroker(String resourceName, ActiveMQServerControlImpl messagingServerControl) {
      serverControls.put(resourceName, messagingServerControl);
   }

   public void unRegisterBroker(String resourceName) {
      serverControls.remove(resourceName);
   }

   public void registerDivertControl(String resourceName, DivertControl divertControl) {
      Object replaced = divertControls.put(resourceName, divertControl);
      String addendum = "";
      if (replaced != null) {
         addendum = ". Replaced: " + replaced;
      }
      logger.debug("Registered in management: {} as {}{}", resourceName, divertControl, addendum);
   }

   public void unRegisterDivertControl(String resourceName) {
      Object removed = divertControls.remove(resourceName);
      if (removed != null) {
         logger.debug("Unregistered from management: {} as {}", resourceName, removed);
      } else {
         logger.debug("Attempted to unregister {} from management, but it was not registered.");
      }
   }

   public void clear() {
      //todo
   }

   public Object get(String resourceName) {
      String resourceNamePrefix = resourceName;
      int idx = resourceName.indexOf(".");
      if (idx > 0) {
         resourceNamePrefix = resourceName.substring(0, idx + 1);
      }

      switch (resourceNamePrefix) {
         case ResourceNames.BROKER: return serverControls.get(resourceName);
         case ResourceNames.ADDRESS: return addressControls.get(resourceName);
         case ResourceNames.QUEUE: return queueControls.get(resourceName);
         case ResourceNames.ACCEPTOR: return accepterControls.get(resourceName);
         case ResourceNames.BROADCAST_GROUP: return broadcastGroupControls.get(resourceName);
         case ResourceNames.BROKER_CONNECTION: return brokerConnectionControls.get(resourceName);
         case ResourceNames.REMOTE_BROKER_CONNECTION: return remoteBrokerConnectionControls.get(resourceName);
         case ResourceNames.BRIDGE: return bridgeControls.get(resourceName);
         case ResourceNames.CORE_CLUSTER_CONNECTION: return clusterConnectionControl.get(resourceName);
         case ResourceNames.CONNECTION_ROUTER: return connectionRouterControls.get(resourceName);
         case ResourceNames.MANAGEMENT_SECURITY: return hawtioSecurityControls.get(resourceName);
         case ResourceNames.DIVERT: return divertControls.get(resourceName);
         default: {
            if (amqpControls.containsKey(resourceName)) {
               return amqpControls.get(resourceName);
            }
            return null;
         }
      }
   }

   public Object legacyGetResource(String resourceName) {
      Object resource = serverControls.get(resourceName);
      if (resource == null)
         resource = addressControls.get(resourceName);
      if (resource == null)
         resource = queueControls.get(resourceName);
      if (resource == null)
         resource = accepterControls.get(resourceName);
      if (resource == null)
         resource = broadcastGroupControls.get(resourceName);
      if (resource == null)
         resource = brokerConnectionControls.get(resourceName);
      if (resource == null)
         resource = remoteBrokerConnectionControls.get(resourceName);
      if (resource == null)
         resource = bridgeControls.get(resourceName);
      if (resource == null)
         resource = clusterConnectionControl.get(resourceName);
      if (resource == null)
         resource = connectionRouterControls.get(resourceName);
      if (resource == null)
         resource = hawtioSecurityControls.get(resourceName);
      if (resource == null)
         resource = divertControls.get(resourceName);
      if (resource == null)
         resource = amqpControls.get(resourceName);

      return resource;
   }

   public List<DivertControl> getDivertControls() {
      return divertControls.values().stream().toList();
   }

   public List<BridgeControl> getBridgeControls() {
      return bridgeControls.values().stream().toList();
   }

   public Set<String> unRegisterAll() {
      Set<String> names = new HashSet<>();
      names.addAll(unregisterMap(serverControls));
      names.addAll(unregisterMap(addressControls));
      names.addAll(unregisterMap(queueControls));
      names.addAll(unregisterMap(accepterControls));
      names.addAll(unregisterMap(broadcastGroupControls));
      names.addAll(unregisterMap(brokerConnectionControls));
      names.addAll(unregisterMap(remoteBrokerConnectionControls));
      names.addAll(unregisterMap(bridgeControls));
      names.addAll(unregisterMap(clusterConnectionControl));
      names.addAll(unregisterMap(connectionRouterControls));
      names.addAll(unregisterMap(hawtioSecurityControls));
      names.addAll(unregisterMap(amqpControls));
      return names;
   }

   private Collection<String> unregisterMap(Map registry) {
      Set<String> resourceNames = new HashSet<>(registry.keySet());
      for (String resourceName : resourceNames) {
         unregisterFromRegistry(resourceName, registry);
      }
      return resourceNames;
   }

   private void unregisterFromRegistry(final String resourceName, final Map<String, Object> registry) {
      Object removed = registry.remove(resourceName);
      if (removed != null) {
         logger.debug("Unregistered from management: {} as {}", resourceName, removed);
      } else {
         logger.debug("Attempted to unregister {} from management, but it was not registered.");
      }
   }
}
