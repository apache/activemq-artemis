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
package org.hornetq.core.server.cluster.impl;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.filter.impl.FilterImpl;
import org.hornetq.core.message.impl.MessageImpl;
import org.hornetq.core.postoffice.BindingType;
import org.hornetq.core.server.Bindable;
import org.hornetq.core.server.HornetQServerLogger;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.RoutingContext;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.cluster.RemoteQueueBinding;

/**
 * A RemoteQueueBindingImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * Created 21 Jan 2009 18:55:22
 *
 *
 */
public class RemoteQueueBindingImpl implements RemoteQueueBinding
{
   private final SimpleString address;

   private final Queue storeAndForwardQueue;

   private final SimpleString uniqueName;

   private final SimpleString routingName;

   private final long remoteQueueID;

   private final Filter queueFilter;

   private final Set<Filter> filters = new HashSet<Filter>();

   private final Map<SimpleString, Integer> filterCounts = new HashMap<SimpleString, Integer>();

   private int consumerCount;

   private final SimpleString idsHeaderName;

   private final long id;

   private final int distance;

   private boolean connected = true;

   public RemoteQueueBindingImpl(final long id,
                                 final SimpleString address,
                                 final SimpleString uniqueName,
                                 final SimpleString routingName,
                                 final Long remoteQueueID,
                                 final SimpleString filterString,
                                 final Queue storeAndForwardQueue,
                                 final SimpleString bridgeName,
                                 final int distance) throws Exception
   {
      this.id = id;

      this.address = address;

      this.storeAndForwardQueue = storeAndForwardQueue;

      this.uniqueName = uniqueName;

      this.routingName = routingName;

      this.remoteQueueID = remoteQueueID;

      queueFilter = FilterImpl.createFilter(filterString);

      idsHeaderName = MessageImpl.HDR_ROUTE_TO_IDS.concat(bridgeName);

      this.distance = distance;
   }

   public long getID()
   {
      return id;
   }

   public SimpleString getAddress()
   {
      return address;
   }

   public Bindable getBindable()
   {
      return storeAndForwardQueue;
   }

   public Queue getQueue()
   {
      return storeAndForwardQueue;
   }

   public SimpleString getRoutingName()
   {
      return routingName;
   }

   public SimpleString getUniqueName()
   {
      return uniqueName;
   }

   public SimpleString getClusterName()
   {
      return uniqueName;
   }

   public boolean isExclusive()
   {
      return false;
   }

   public BindingType getType()
   {
      return BindingType.REMOTE_QUEUE;
   }

   public Filter getFilter()
   {
      return queueFilter;
   }

   public int getDistance()
   {
      return distance;
   }

   public synchronized boolean isHighAcceptPriority(final ServerMessage message)
   {
      if (consumerCount == 0)
      {
         return false;
      }

      if (filters.isEmpty())
      {
         return true;
      }
      else
      {
         for (Filter filter : filters)
         {
            if (filter.match(message))
            {
               return true;
            }
         }
      }

      return false;
   }


   @Override
   public void unproposed(SimpleString groupID)
   {
   }

   public void route(final ServerMessage message, final RoutingContext context)
   {
      addRouteContextToMessage(message);

      List<Queue> durableQueuesOnContext = context.getDurableQueues(storeAndForwardQueue.getAddress());

      if (!durableQueuesOnContext.contains(storeAndForwardQueue))
      {
         // There can be many remote bindings for the same node, we only want to add the message once to
         // the s & f queue for that node
         context.addQueue(storeAndForwardQueue.getAddress(), storeAndForwardQueue);
      }
   }

   @Override
   public void routeWithAck(ServerMessage message, RoutingContext context)
   {
      addRouteContextToMessage(message);

      List<Queue> durableQueuesOnContext = context.getDurableQueues(storeAndForwardQueue.getAddress());

      if (!durableQueuesOnContext.contains(storeAndForwardQueue))
      {
         // There can be many remote bindings for the same node, we only want to add the message once to
         // the s & f queue for that node
         context.addQueueWithAck(storeAndForwardQueue.getAddress(), storeAndForwardQueue);
      }
   }

   public synchronized void addConsumer(final SimpleString filterString) throws Exception
   {
      if (filterString != null)
      {
         // There can actually be many consumers on the same queue with the same filter, so we need to maintain a ref
         // count

         Integer i = filterCounts.get(filterString);

         if (i == null)
         {
            filterCounts.put(filterString, 1);

            filters.add(FilterImpl.createFilter(filterString));
         }
         else
         {
            filterCounts.put(filterString, i + 1);
         }
      }

      consumerCount++;
   }

   public synchronized void removeConsumer(final SimpleString filterString) throws Exception
   {
      if (filterString != null)
      {
         Integer i = filterCounts.get(filterString);

         if (i != null)
         {
            int ii = i - 1;

            if (ii == 0)
            {
               filterCounts.remove(filterString);

               filters.remove(FilterImpl.createFilter(filterString));
            }
            else
            {
               filterCounts.put(filterString, ii);
            }
         }
      }

      consumerCount--;
   }

   @Override
   public void reset()
   {
      consumerCount = 0;
      filterCounts.clear();
      filters.clear();
   }

   public synchronized int consumerCount()
   {
      return consumerCount;
   }

   @Override
   public String toString()
   {
      return "RemoteQueueBindingImpl(" +
            (connected ? "connected" : "disconnected")
            + ")[address=" + address +
             ", consumerCount=" +
             consumerCount +
             ", distance=" +
             distance +
             ", filters=" +
             filters +
             ", id=" +
             id +
             ", idsHeaderName=" +
             idsHeaderName +
             ", queueFilter=" +
             queueFilter +
             ", remoteQueueID=" +
             remoteQueueID +
             ", routingName=" +
             routingName +
             ", storeAndForwardQueue=" +
             storeAndForwardQueue +
             ", uniqueName=" +
             uniqueName +
             "]";
   }

   @Override
   public String toManagementString()
   {
      return "RemoteQueueBindingImpl [address=" + address +
         ", storeAndForwardQueue=" + storeAndForwardQueue.getName() +
         ", remoteQueueID=" +
         remoteQueueID + "]";
   }

   @Override
   public void disconnect()
   {
      connected = false;
   }

   @Override
   public boolean isConnected()
   {
      return connected;
   }

   @Override
   public void connect()
   {
      connected = true;
   }


   public Set<Filter> getFilters()
   {
      return filters;
   }

   public void close() throws Exception
   {
      storeAndForwardQueue.close();
   }

   /**
    * This will add routing information to the message.
    * This will be later processed during the delivery between the nodes. Because of that this has to be persisted as a property on the message.
    * @param message
    */
   private void addRouteContextToMessage(final ServerMessage message)
   {
      byte[] ids = message.getBytesProperty(idsHeaderName);

      if (ids == null)
      {
         ids = new byte[8];
      }
      else
      {
         byte[] newIds = new byte[ids.length + 8];

         System.arraycopy(ids, 0, newIds, 8, ids.length);

         ids = newIds;
      }

      ByteBuffer buff = ByteBuffer.wrap(ids);

      buff.putLong(remoteQueueID);

      message.putBytesProperty(idsHeaderName, ids);

      if (HornetQServerLogger.LOGGER.isTraceEnabled())
      {
         HornetQServerLogger.LOGGER.trace("Adding remoteQueue ID = " + remoteQueueID + " into message=" + message + " store-forward-queue=" + storeAndForwardQueue);
      }
   }

   public long getRemoteQueueID()
   {
      return  remoteQueueID;
   }
}
