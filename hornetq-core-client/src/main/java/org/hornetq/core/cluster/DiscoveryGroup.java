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
package org.hornetq.core.cluster;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.hornetq.api.core.BroadcastEndpoint;
import org.hornetq.api.core.BroadcastEndpointFactory;
import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQBuffers;
import org.hornetq.api.core.HornetQInterruptedException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.management.CoreNotificationType;
import org.hornetq.core.client.HornetQClientLogger;
import org.hornetq.core.server.HornetQComponent;
import org.hornetq.core.server.management.Notification;
import org.hornetq.core.server.management.NotificationService;
import org.hornetq.utils.TypedProperties;

/**
 * This class is used to search for members on the cluster through the opaque interface {@link BroadcastEndpoint}.
 * <p>
 * There are two current implementations, and that's probably all we will ever need.
 * <p>
 * We will probably keep both interfaces for a while as UDP is a simple solution requiring no extra dependencies which
 * is suitable for users looking for embedded solutions.
 * <p>
 * Created 17 Nov 2008 13:21:45
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author Clebert Suconic
 */
public final class DiscoveryGroup implements HornetQComponent
{
   private static final boolean isTrace = HornetQClientLogger.LOGGER.isTraceEnabled();

   private final List<DiscoveryListener> listeners = new ArrayList<DiscoveryListener>();

   private final String name;

   private Thread thread;

   private boolean received;

   private final Object waitLock = new Object();

   private final Map<String, DiscoveryEntry> connectors = new ConcurrentHashMap<String, DiscoveryEntry>();

   private final long timeout;

   private volatile boolean started;

   private final String nodeID;

   private final Map<String, String> uniqueIDMap = new HashMap<String, String>();

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
   public DiscoveryGroup(final String nodeID, final String name, final long timeout,
                         BroadcastEndpointFactory endpointFactory,
                         NotificationService service) throws Exception
   {
      this.nodeID = nodeID;
      this.name = name;
      this.timeout = timeout;
      this.endpoint = endpointFactory.createBroadcastEndpoint();
      this.notificationService = service;
   }

   public synchronized void start() throws Exception
   {
      if (started)
      {
         return;
      }

      endpoint.openClient();

      started = true;

      thread = new Thread(new DiscoveryRunnable(), "hornetq-discovery-group-thread-" + name);

      thread.setDaemon(true);

      thread.start();

      if (notificationService != null)
      {
         TypedProperties props = new TypedProperties();

         props.putSimpleStringProperty(new SimpleString("name"), new SimpleString(name));

         Notification notification = new Notification(nodeID, CoreNotificationType.DISCOVERY_GROUP_STARTED, props);

         notificationService.sendNotification(notification);
      }
   }

   public void stop()
   {
      synchronized (this)
      {
         if (!started)
         {
            return;
         }

         started = false;
      }

      synchronized (waitLock)
      {
         waitLock.notifyAll();
      }

      try
      {
         endpoint.close(false);
      }
      catch (Exception e1)
      {
         HornetQClientLogger.LOGGER.errorStoppingDiscoveryBroadcastEndpoint(endpoint, e1);
      }

      try
      {
         thread.interrupt();
         thread.join(10000);
         if (thread.isAlive())
         {
            HornetQClientLogger.LOGGER.timedOutStoppingDiscovery();
         }
      }
      catch (InterruptedException e)
      {
         throw new HornetQInterruptedException(e);
      }

      thread = null;

      if (notificationService != null)
      {
         TypedProperties props = new TypedProperties();
         props.putSimpleStringProperty(new SimpleString("name"), new SimpleString(name));
         Notification notification = new Notification(nodeID, CoreNotificationType.DISCOVERY_GROUP_STOPPED, props);
         try
         {
            notificationService.sendNotification(notification);
         }
         catch (Exception e)
         {
            HornetQClientLogger.LOGGER.errorSendingNotifOnDiscoveryStop(e);
         }
      }
   }

   public boolean isStarted()
   {
      return started;
   }

   public String getName()
   {
      return name;
   }

   public synchronized List<DiscoveryEntry> getDiscoveryEntries()
   {
      List<DiscoveryEntry> list = new ArrayList<DiscoveryEntry>(connectors.values());

      return list;
   }

   public boolean waitForBroadcast(final long timeout)
   {
      synchronized (waitLock)
      {
         long start = System.currentTimeMillis();

         long toWait = timeout;

         while (started && !received && (toWait > 0 || timeout == 0))
         {
            try
            {
               waitLock.wait(toWait);
            }
            catch (InterruptedException e)
            {
               throw new HornetQInterruptedException(e);
            }

            if (timeout != 0)
            {
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
   private void checkUniqueID(final String originatingNodeID, final String uniqueID)
   {
      String currentUniqueID = uniqueIDMap.get(originatingNodeID);

      if (currentUniqueID == null)
      {
         uniqueIDMap.put(originatingNodeID, uniqueID);
      }
      else
      {
         if (!currentUniqueID.equals(uniqueID))
         {
            HornetQClientLogger.LOGGER.multipleServersBroadcastingSameNode(originatingNodeID);
            uniqueIDMap.put(originatingNodeID, uniqueID);
         }
      }
   }

   class DiscoveryRunnable implements Runnable
   {
      public void run()
      {
         try
         {
            byte[] data = null;

            while (started)
            {
               try
               {

                  data = endpoint.receiveBroadcast();
                  if (data == null)
                  {
                     if (started)
                     {
                        // This is totally unexpected, so I'm not even bothering on creating
                        // a log entry for that
                        HornetQClientLogger.LOGGER.warn("Unexpected null data received from DiscoveryEndpoint");
                     }
                     break;
                  }
               }
               catch (Exception e)
               {
                  if (!started)
                  {
                     return;
                  }
                  else
                  {
                     HornetQClientLogger.LOGGER.errorReceivingPAcketInDiscovery(e);
                  }
               }

               HornetQBuffer buffer = HornetQBuffers.wrappedBuffer(data);

               String originatingNodeID = buffer.readString();

               String uniqueID = buffer.readString();

               checkUniqueID(originatingNodeID, uniqueID);

               if (nodeID.equals(originatingNodeID))
               {
                  if (checkExpiration())
                  {
                     callListeners();
                  }
                  // Ignore traffic from own node
                  continue;
               }

               int size = buffer.readInt();

               boolean changed = false;

               DiscoveryEntry[] entriesRead = new DiscoveryEntry[size];
               // Will first decode all the elements outside of any lock
               for (int i = 0; i < size; i++)
               {
                  TransportConfiguration connector = new TransportConfiguration();

                  connector.decode(buffer);

                  entriesRead[i] = new DiscoveryEntry(originatingNodeID, connector, System.currentTimeMillis());
               }

               synchronized (DiscoveryGroup.this)
               {
                  for (DiscoveryEntry entry : entriesRead)
                  {
                     if (connectors.put(originatingNodeID, entry) == null)
                     {
                        changed = true;
                     }
                  }

                  changed = changed || checkExpiration();
               }
               //only call the listeners if we have changed
               //also make sure that we aren't stopping to avoid deadlock
               if (changed && started)
               {
                  if (isTrace)
                  {
                     HornetQClientLogger.LOGGER.trace("Connectors changed on Discovery:");
                     for (DiscoveryEntry connector : connectors.values())
                     {
                        HornetQClientLogger.LOGGER.trace(connector);
                     }
                  }
                  callListeners();
               }

               synchronized (waitLock)
               {
                  received = true;

                  waitLock.notifyAll();
               }
            }
         }
         catch (Exception e)
         {
            HornetQClientLogger.LOGGER.failedToReceiveDatagramInDiscovery(e);
         }
      }

   }

   public synchronized void registerListener(final DiscoveryListener listener)
   {
      listeners.add(listener);

      if (!connectors.isEmpty())
      {
         listener.connectorsChanged(getDiscoveryEntries());
      }
   }

   public synchronized void unregisterListener(final DiscoveryListener listener)
   {
      listeners.remove(listener);
   }

   private void callListeners()
   {
      for (DiscoveryListener listener : listeners)
      {
         try
         {
            listener.connectorsChanged(getDiscoveryEntries());
         }
         catch (Throwable t)
         {
            // Catch it so exception doesn't prevent other listeners from running
            HornetQClientLogger.LOGGER.failedToCallListenerInDiscovery(t);
         }
      }
   }

   private boolean checkExpiration()
   {
      boolean changed = false;
      long now = System.currentTimeMillis();

      Iterator<Map.Entry<String, DiscoveryEntry>> iter = connectors.entrySet().iterator();

      // Weed out any expired connectors

      while (iter.hasNext())
      {
         Map.Entry<String, DiscoveryEntry> entry = iter.next();

         if (entry.getValue().getLastUpdate() + timeout <= now)
         {
            if (isTrace)
            {
               HornetQClientLogger.LOGGER.trace("Timed out node on discovery:" + entry.getValue());
            }
            iter.remove();

            changed = true;
         }
      }

      return changed;
   }
}
