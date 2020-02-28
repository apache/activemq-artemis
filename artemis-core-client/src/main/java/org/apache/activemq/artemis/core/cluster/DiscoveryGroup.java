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
package org.apache.activemq.artemis.core.cluster;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.api.core.ActiveMQInterruptedException;
import org.apache.activemq.artemis.api.core.BroadcastEndpoint;
import org.apache.activemq.artemis.api.core.BroadcastEndpointFactory;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.management.CoreNotificationType;
import org.apache.activemq.artemis.core.client.ActiveMQClientLogger;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.core.server.management.Notification;
import org.apache.activemq.artemis.core.server.management.NotificationService;
import org.apache.activemq.artemis.utils.ActiveMQThreadFactory;
import org.apache.activemq.artemis.utils.collections.TypedProperties;
import org.jboss.logging.Logger;

/**
 * This class is used to search for members on the cluster through the opaque interface {@link BroadcastEndpoint}.
 *
 * There are two current implementations, and that's probably all we will ever need.
 *
 * We will probably keep both interfaces for a while as UDP is a simple solution requiring no extra dependencies which
 * is suitable for users looking for embedded solutions.
 */
public final class DiscoveryGroup implements ActiveMQComponent {

   private static final Logger logger = Logger.getLogger(DiscoveryGroup.class);

   private final List<DiscoveryListener> listeners = new ArrayList<>();

   private final String name;

   private Thread thread;

   private boolean received;

   private final Object waitLock = new Object();

   private final Map<String, DiscoveryEntry> connectors = new ConcurrentHashMap<>();

   private final long timeout;

   private volatile boolean started;

   private final String nodeID;

   private final Map<String, String> uniqueIDMap = new HashMap<>();

   private final BroadcastEndpoint endpoint;

   private final NotificationService notificationService;

   /**
    * This is the main constructor, intended to be used
    *
    * @param nodeID
    * @param name
    * @param timeout
    * @param endpointFactory
    * @param service
    * @throws Exception
    */
   public DiscoveryGroup(final String nodeID,
                         final String name,
                         final long timeout,
                         BroadcastEndpointFactory endpointFactory,
                         NotificationService service) throws Exception {
      this.nodeID = nodeID;
      this.name = name;
      this.timeout = timeout;
      this.endpoint = endpointFactory.createBroadcastEndpoint();
      this.notificationService = service;
   }

   @Override
   public synchronized void start() throws Exception {
      if (started) {
         return;
      }

      if (logger.isDebugEnabled()) logger.debug("Starting Discovery Group for " + name);

      endpoint.openClient();

      started = true;

      ThreadFactory tfactory = AccessController.doPrivileged(new PrivilegedAction<ThreadFactory>() {
         @Override
         public ThreadFactory run() {
            return new ActiveMQThreadFactory("DiscoveryGroup-" + System.identityHashCode(this), "activemq-discovery-group-thread-" + name, true, DiscoveryGroup.class.getClassLoader());
         }
      });

      thread = tfactory.newThread(new DiscoveryRunnable());

      if (logger.isDebugEnabled()) logger.debug("Starting daemon thread");

      thread.start();

      if (notificationService != null) {
         TypedProperties props = new TypedProperties();

         props.putSimpleStringProperty(new SimpleString("name"), new SimpleString(name));

         Notification notification = new Notification(nodeID, CoreNotificationType.DISCOVERY_GROUP_STARTED, props);

         notificationService.sendNotification(notification);
      }
   }

   /**
    * This will start the DiscoveryRunnable and run it directly.
    * This is useful for a test process where we need this execution blocking a thread.
    */
   public void internalRunning() throws Exception {
      endpoint.openClient();
      started = true;
      DiscoveryRunnable runnable = new DiscoveryRunnable();
      runnable.run();
   }

   @Override
   public void stop() {

      if (logger.isDebugEnabled()) {
         logger.debug("Stopping discovery. There's an exception just as a trace where it happened", new Exception("trace"));
      }
      synchronized (this) {
         if (!started) {
            return;
         }

         started = false;
      }

      synchronized (waitLock) {
         waitLock.notifyAll();
      }

      try {
         endpoint.close(false);
         if (logger.isDebugEnabled()) {
            logger.debug("endpoing closed");
         }
      } catch (Exception e1) {
         ActiveMQClientLogger.LOGGER.errorStoppingDiscoveryBroadcastEndpoint(endpoint, e1);
      }

      try {
         if (thread != null) {
            thread.interrupt();
            thread.join(10000);
            if (thread.isAlive()) {
               ActiveMQClientLogger.LOGGER.timedOutStoppingDiscovery();
            }
         }
      } catch (InterruptedException e) {
         throw new ActiveMQInterruptedException(e);
      }

      thread = null;
      received = false;

      if (notificationService != null) {
         TypedProperties props = new TypedProperties();
         props.putSimpleStringProperty(new SimpleString("name"), new SimpleString(name));
         Notification notification = new Notification(nodeID, CoreNotificationType.DISCOVERY_GROUP_STOPPED, props);
         try {
            notificationService.sendNotification(notification);
         } catch (Exception e) {
            ActiveMQClientLogger.LOGGER.errorSendingNotifOnDiscoveryStop(e);
         }
      }
   }

   @Override
   public boolean isStarted() {
      return started;
   }

   public String getName() {
      return name;
   }

   public synchronized List<DiscoveryEntry> getDiscoveryEntries() {
      return new ArrayList<>(connectors.values());
   }

   public boolean waitForBroadcast(final long timeout) {
      synchronized (waitLock) {
         long start = System.currentTimeMillis();

         long toWait = timeout;

         while (started && !received && (toWait > 0 || timeout == 0)) {
            try {
               waitLock.wait(toWait);
            } catch (InterruptedException e) {
               throw new ActiveMQInterruptedException(e);
            }

            if (timeout != 0) {
               long now = System.currentTimeMillis();

               toWait -= now - start;

               start = now;
            }
         }

         boolean ret = received;

         received = false;

         return ret;
      }
   }

   /*
    * This is a sanity check to catch any cases where two different nodes are broadcasting the same node id either
    * due to misconfiguration or problems in failover
    */
   private void checkUniqueID(final String originatingNodeID, final String uniqueID) {
      String currentUniqueID = uniqueIDMap.get(originatingNodeID);

      if (currentUniqueID == null) {
         uniqueIDMap.put(originatingNodeID, uniqueID);
      } else {
         if (!currentUniqueID.equals(uniqueID)) {
            ActiveMQClientLogger.LOGGER.multipleServersBroadcastingSameNode(originatingNodeID);
            uniqueIDMap.put(originatingNodeID, uniqueID);
         }
      }
   }

   class DiscoveryRunnable implements Runnable {

      @Override
      public void run() {
         byte[] data = null;

         while (started) {
            try {
               try {

                  data = endpoint.receiveBroadcast();
                  if (data == null) {
                     if (started) {
                        ActiveMQClientLogger.LOGGER.unexpectedNullDataReceived();
                     }

                     if (logger.isDebugEnabled()) {
                        logger.debug("Received broadcast data as null");
                     }
                     break;
                  }

                  if (logger.isDebugEnabled()) {
                     logger.debug("receiving " + data.length);
                  }
               } catch (Exception e) {
                  if (!started) {
                     return;
                  } else {
                     ActiveMQClientLogger.LOGGER.errorReceivingPacketInDiscovery(e);
                  }
               }

               ActiveMQBuffer buffer = ActiveMQBuffers.wrappedBuffer(data);

               String originatingNodeID = buffer.readString();

               String uniqueID = buffer.readString();

               checkUniqueID(originatingNodeID, uniqueID);

               if (nodeID.equals(originatingNodeID)) {

                  if (logger.isDebugEnabled()) {
                     logger.debug("ignoring original NodeID" + originatingNodeID + " receivedID = " + nodeID);
                  }
                  if (checkExpiration()) {
                     callListeners();
                  }
                  // Ignore traffic from own node
                  continue;
               }

               if (logger.isDebugEnabled()) {
                  logger.debug("Received nodeID " + nodeID + " with originatingID = " + originatingNodeID);
               }

               int size = buffer.readInt();

               boolean changed = false;

               DiscoveryEntry[] entriesRead = new DiscoveryEntry[size];
               // Will first decode all the elements outside of any lock
               for (int i = 0; i < size; i++) {
                  TransportConfiguration connector = new TransportConfiguration();

                  connector.decode(buffer);

                  entriesRead[i] = new DiscoveryEntry(originatingNodeID, connector, System.currentTimeMillis());
               }


               if (logger.isDebugEnabled()) {
                  logger.debug("Received " + entriesRead.length + " discovery entry elements");
                  for (DiscoveryEntry entryDisco : entriesRead) {
                     logger.debug("" + entryDisco);
                  }
               }


               synchronized (DiscoveryGroup.this) {
                  for (DiscoveryEntry entry : entriesRead) {
                     if (connectors.put(originatingNodeID, entry) == null) {
                        changed = true;
                     }
                  }

                  changed = changed || checkExpiration();

                  if (logger.isDebugEnabled()) {
                     logger.debug("changed = " + changed);
                  }
               }
               //only call the listeners if we have changed
               //also make sure that we aren't stopping to avoid deadlock
               if (changed && started) {
                  if (logger.isTraceEnabled()) {
                     logger.trace("Connectors changed on Discovery:");
                     for (DiscoveryEntry connector : connectors.values()) {
                        logger.trace(connector);
                     }
                  }
                  if (logger.isDebugEnabled()) {
                     logger.debug("Calling listeners");
                  }
                  callListeners();
               }

               synchronized (waitLock) {
                  received = true;

                  if (logger.isDebugEnabled()) {
                     logger.debug("Calling notifyAll");
                  }
                  waitLock.notifyAll();
               }
            } catch (Throwable e) {
               ActiveMQClientLogger.LOGGER.failedToReceiveDatagramInDiscovery(e);
            }
         }
      }

   }

   public synchronized void registerListener(final DiscoveryListener listener) {
      listeners.add(listener);

      if (!connectors.isEmpty()) {
         listener.connectorsChanged(getDiscoveryEntries());
      }
   }

   public synchronized void unregisterListener(final DiscoveryListener listener) {
      listeners.remove(listener);
   }

   private void callListeners() {
      for (DiscoveryListener listener : listeners) {
         try {
            listener.connectorsChanged(getDiscoveryEntries());
         } catch (Throwable t) {
            // Catch it so exception doesn't prevent other listeners from running
            ActiveMQClientLogger.LOGGER.failedToCallListenerInDiscovery(t);
         }
      }
   }

   private boolean checkExpiration() {
      boolean changed = false;
      long now = System.currentTimeMillis();

      Iterator<Map.Entry<String, DiscoveryEntry>> iter = connectors.entrySet().iterator();

      // Weed out any expired connectors

      while (iter.hasNext()) {
         Map.Entry<String, DiscoveryEntry> entry = iter.next();

         if (entry.getValue().getLastUpdate() + timeout <= now) {
            if (logger.isTraceEnabled()) {
               logger.trace("Timed out node on discovery:" + entry.getValue());
            }
            iter.remove();

            changed = true;
         }
      }

      return changed;
   }
}
