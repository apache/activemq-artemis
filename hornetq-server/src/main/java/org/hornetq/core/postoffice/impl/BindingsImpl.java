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
package org.hornetq.core.postoffice.impl;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.message.impl.MessageImpl;
import org.hornetq.core.paging.PagingStore;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.Bindings;
import org.hornetq.core.server.HornetQServerLogger;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.RoutingContext;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.group.GroupingHandler;
import org.hornetq.core.server.group.impl.Proposal;
import org.hornetq.core.server.group.impl.Response;

/**
 * A BindingsImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *         <p/>
 *         Created 11 Dec 2008 08:34:33
 */
public final class BindingsImpl implements Bindings
{
   // This is public as we use on test assertions
   public static final int MAX_GROUP_RETRY = 10;

   private static boolean isTrace = HornetQServerLogger.LOGGER.isTraceEnabled();

   private final ConcurrentMap<SimpleString, List<Binding>> routingNameBindingMap = new ConcurrentHashMap<SimpleString, List<Binding>>();

   private final Map<SimpleString, Integer> routingNamePositions = new ConcurrentHashMap<SimpleString, Integer>();

   private final Map<Long, Binding> bindingsMap = new ConcurrentHashMap<Long, Binding>();

   private final List<Binding> exclusiveBindings = new CopyOnWriteArrayList<Binding>();

   private volatile boolean routeWhenNoConsumers;

   private final GroupingHandler groupingHandler;

   private final PagingStore pageStore;

   private final SimpleString name;

   public BindingsImpl(final SimpleString name, final GroupingHandler groupingHandler, final PagingStore pageStore)
   {
      this.groupingHandler = groupingHandler;
      this.pageStore = pageStore;
      this.name = name;
   }

   public void setRouteWhenNoConsumers(final boolean routeWhenNoConsumers)
   {
      this.routeWhenNoConsumers = routeWhenNoConsumers;
   }

   public Collection<Binding> getBindings()
   {
      return bindingsMap.values();
   }

   public void unproposed(SimpleString groupID)
   {
      for (Binding binding : bindingsMap.values())
      {
         binding.unproposed(groupID);
      }
   }

   public void addBinding(final Binding binding)
   {
      if (isTrace)
      {
         HornetQServerLogger.LOGGER.trace("addBinding(" + binding + ") being called");
      }
      if (binding.isExclusive())
      {
         exclusiveBindings.add(binding);
      }
      else
      {
         SimpleString routingName = binding.getRoutingName();

         List<Binding> bindings = routingNameBindingMap.get(routingName);

         if (bindings == null)
         {
            bindings = new CopyOnWriteArrayList<Binding>();

            List<Binding> oldBindings = routingNameBindingMap.putIfAbsent(routingName, bindings);

            if (oldBindings != null)
            {
               bindings = oldBindings;
            }
         }

         bindings.add(binding);
      }

      bindingsMap.put(binding.getID(), binding);

      if (isTrace)
      {
         HornetQServerLogger.LOGGER.trace("Adding binding " + binding + " into " + this + " bindingTable: " + debugBindings());
      }

   }

   public void removeBinding(final Binding binding)
   {
      if (binding.isExclusive())
      {
         exclusiveBindings.remove(binding);
      }
      else
      {
         SimpleString routingName = binding.getRoutingName();

         List<Binding> bindings = routingNameBindingMap.get(routingName);

         if (bindings != null)
         {
            bindings.remove(binding);

            if (bindings.isEmpty())
            {
               routingNameBindingMap.remove(routingName);
            }
         }
      }

      bindingsMap.remove(binding.getID());

      if (isTrace)
      {
         HornetQServerLogger.LOGGER.trace("Removing binding " + binding + " into " + this + " bindingTable: " + debugBindings());
      }
   }

   public boolean redistribute(final ServerMessage message, final Queue originatingQueue, final RoutingContext context) throws Exception
   {
      if (routeWhenNoConsumers)
      {
         return false;
      }

      if (isTrace)
      {
         HornetQServerLogger.LOGGER.trace("Redistributing message " + message);
      }

      SimpleString routingName = originatingQueue.getName();

      List<Binding> bindings = routingNameBindingMap.get(routingName);

      if (bindings == null)
      {
         // The value can become null if it's concurrently removed while we're iterating - this is expected
         // ConcurrentHashMap behaviour!
         return false;
      }

      Integer ipos = routingNamePositions.get(routingName);

      int pos = ipos != null ? ipos.intValue() : 0;

      int length = bindings.size();

      int startPos = pos;

      Binding theBinding = null;

      // TODO - combine this with similar logic in route()
      while (true)
      {
         Binding binding;
         try
         {
            binding = bindings.get(pos);
         }
         catch (IndexOutOfBoundsException e)
         {
            // This can occur if binding is removed while in route
            if (!bindings.isEmpty())
            {
               pos = 0;
               startPos = 0;
               length = bindings.size();

               continue;
            }
            else
            {
               break;
            }
         }

         pos = incrementPos(pos, length);

         Filter filter = binding.getFilter();

         boolean highPrior = binding.isHighAcceptPriority(message);

         if (highPrior && binding.getBindable() != originatingQueue && (filter == null || filter.match(message)))
         {
            theBinding = binding;

            break;
         }

         if (pos == startPos)
         {
            break;
         }
      }

      routingNamePositions.put(routingName, pos);

      if (theBinding != null)
      {
         theBinding.route(message, context);

         return true;
      }
      else
      {
         return false;
      }
   }

   public void route(final ServerMessage message, final RoutingContext context) throws Exception
   {
      route(message, context, true);
   }

   private void route(final ServerMessage message, final RoutingContext context, final boolean groupRouting) throws Exception
   {
      boolean routed = false;

      if (!exclusiveBindings.isEmpty())
      {
         for (Binding binding : exclusiveBindings)
         {
            if (binding.getFilter() == null || binding.getFilter().match(message))
            {
               binding.getBindable().route(message, context);

               routed = true;
            }
         }
      }

      if (!routed)
      {
         // Remove the ids now, in order to avoid double check
         byte[] ids = (byte[]) message.removeProperty(MessageImpl.HDR_ROUTE_TO_IDS);

         // Fetch the groupId now, in order to avoid double checking
         SimpleString groupId = message.getSimpleStringProperty(Message.HDR_GROUP_ID);

         if (ids != null)
         {
            routeFromCluster(message, context, ids);
         }
         else if (groupingHandler != null && groupRouting && groupId != null)
         {
            routeUsingStrictOrdering(message, context, groupingHandler, groupId, 0);
         }
         else
         {
            if (isTrace)
            {
               HornetQServerLogger.LOGGER.trace("Routing message " + message + " on binding=" + this);
            }
            for (Map.Entry<SimpleString, List<Binding>> entry : routingNameBindingMap.entrySet())
            {
               SimpleString routingName = entry.getKey();

               List<Binding> bindings = entry.getValue();

               if (bindings == null)
               {
                  // The value can become null if it's concurrently removed while we're iterating - this is expected
                  // ConcurrentHashMap behaviour!
                  continue;
               }

               Binding theBinding = getNextBinding(message, routingName, bindings);

               if (theBinding != null)
               {
                  theBinding.route(message, context);
               }
            }
         }
      }
   }

   @Override
   public String toString()
   {
      return "BindingsImpl [name=" + name + "]";
   }

   /**
    * This code has a race on the assigned value to routing names.
    * <p/>
    * This is not that much of an issue because<br/>
    * Say you have the same queue name bound into two servers. The routing will load balance between
    * these two servers. This will eventually send more messages to one server than the other
    * (depending if you are using multi-thread), and not lose messages.
    */
   private Binding getNextBinding(final ServerMessage message,
                                  final SimpleString routingName,
                                  final List<Binding> bindings)
   {
      Integer ipos = routingNamePositions.get(routingName);

      int pos = ipos != null ? ipos : 0;

      int length = bindings.size();

      int startPos = pos;

      Binding theBinding = null;

      int lastLowPriorityBinding = -1;

      while (true)
      {
         Binding binding;
         try
         {
            binding = bindings.get(pos);
         }
         catch (IndexOutOfBoundsException e)
         {
            // This can occur if binding is removed while in route
            if (!bindings.isEmpty())
            {
               pos = 0;
               startPos = 0;
               length = bindings.size();

               continue;
            }
            else
            {
               break;
            }
         }

         Filter filter = binding.getFilter();

         if (filter == null || filter.match(message))
         {
            // bindings.length == 1 ==> only a local queue so we don't check for matching consumers (it's an
            // unnecessary overhead)
            if (length == 1 || (binding.isConnected() && (routeWhenNoConsumers || binding.isHighAcceptPriority(message))))
            {
               theBinding = binding;

               pos = incrementPos(pos, length);

               break;
            }
            else
            {
               //https://issues.jboss.org/browse/HORNETQ-1254 When !routeWhenNoConsumers,
               // the localQueue should always have the priority over the secondary bindings
               if (lastLowPriorityBinding == -1 || !routeWhenNoConsumers && binding instanceof LocalQueueBinding)
               {
                  lastLowPriorityBinding = pos;
               }
            }
         }

         pos = incrementPos(pos, length);

         if (pos == startPos)
         {

            // if no bindings were found, we will apply a secondary level on the routing logic
            if (lastLowPriorityBinding != -1)
            {
               try
               {
                  theBinding = bindings.get(lastLowPriorityBinding);
               }
               catch (IndexOutOfBoundsException e)
               {
                  // This can occur if binding is removed while in route
                  if (!bindings.isEmpty())
                  {
                     pos = 0;

                     lastLowPriorityBinding = -1;

                     continue;
                  }
                  else
                  {
                     break;
                  }
               }

               pos = incrementPos(lastLowPriorityBinding, length);
            }
            break;
         }
      }
      if (pos != startPos)
      {
         routingNamePositions.put(routingName, pos);
      }
      return theBinding;
   }

   private void routeUsingStrictOrdering(final ServerMessage message,
                                         final RoutingContext context,
                                         final GroupingHandler groupingGroupingHandler,
                                         final SimpleString groupId,
                                         final int tries) throws Exception
   {
      for (Map.Entry<SimpleString, List<Binding>> entry : routingNameBindingMap.entrySet())
      {
         SimpleString routingName = entry.getKey();

         List<Binding> bindings = entry.getValue();

         if (bindings == null)
         {
            // The value can become null if it's concurrently removed while we're iterating - this is expected
            // ConcurrentHashMap behaviour!
            continue;
         }

         // concat a full group id, this is for when a binding has multiple bindings
         // NOTE: In case a dev ever change this rule, QueueImpl::unproposed is using this rule to determine if
         //       the binding belongs to its Queue before removing it
         SimpleString fullID = groupId.concat(".").concat(routingName);

         // see if there is already a response
         Response resp = groupingGroupingHandler.getProposal(fullID, true);

         if (resp == null)
         {
            // ok let's find the next binding to propose
            Binding theBinding = getNextBinding(message, routingName, bindings);
            if (theBinding == null)
            {
               continue;
            }

            resp = groupingGroupingHandler.propose(new Proposal(fullID, theBinding.getClusterName()));

            if (resp == null)
            {
               HornetQServerLogger.LOGGER.debug("it got a timeout on propose, trying again, number of retries: " + tries);
               // it timed out, so we will check it through routeAndcheckNull
               theBinding = null;
            }

            // alternativeClusterName will be != null when by the time we looked at the cachedProposed,
            // another thread already set the proposal, so we use the new alternativeclusterName that's set there
            // if our proposal was declined find the correct binding to use
            if (resp != null && resp.getAlternativeClusterName() != null)
            {
               theBinding = locateBinding(resp.getAlternativeClusterName(), bindings);
            }

            routeAndCheckNull(message, context, resp, theBinding, groupId, tries);
         }
         else
         {
            // ok, we need to find the binding and route it
            Binding chosen = locateBinding(resp.getChosenClusterName(), bindings);

            routeAndCheckNull(message, context, resp, chosen, groupId, tries);
         }
      }
   }

   private Binding locateBinding(SimpleString clusterName, List<Binding> bindings)
   {
      for (Binding binding : bindings)
      {
         if (binding.getClusterName().equals(clusterName))
         {
            return binding;
         }
      }

      return null;
   }

   private void routeAndCheckNull(ServerMessage message, RoutingContext context, Response resp, Binding theBinding, SimpleString groupId, int tries) throws Exception
   {
      // and let's route it
      if (theBinding != null)
      {
         theBinding.route(message, context);
      }
      else
      {
         if (resp != null)
         {
            groupingHandler.forceRemove(resp.getGroupId(), resp.getClusterName());
         }

         //there may be a chance that the binding has been removed from the post office before it is removed from the grouping handler.
         //in this case all we can do is remove it and try again.
         if (tries < MAX_GROUP_RETRY)
         {
            routeUsingStrictOrdering(message, context, groupingHandler, groupId, tries + 1);
         }
         else
         {
            HornetQServerLogger.LOGGER.impossibleToRouteGrouped();
            route(message, context, false);
         }
      }
   }


   private String debugBindings()
   {
      StringWriter writer = new StringWriter();

      PrintWriter out = new PrintWriter(writer);

      out.println("\n***************************************");

      out.println("routingNameBindingMap:");
      if (routingNameBindingMap.isEmpty())
      {
         out.println("EMPTY!");
      }
      for (Map.Entry<SimpleString, List<Binding>> entry : routingNameBindingMap.entrySet())
      {
         out.print("key=" + entry.getKey() + ", value=" + entry.getValue());
//         for (Binding bind : entry.getValue())
//         {
//            out.print(bind + ",");
//         }
         out.println();
      }

      out.println();

      out.println("RoutingNamePositions:");
      if (routingNamePositions.isEmpty())
      {
         out.println("EMPTY!");
      }
      for (Map.Entry<SimpleString, Integer> entry : routingNamePositions.entrySet())
      {
         out.println("key=" + entry.getKey() + ", value=" + entry.getValue());
      }

      out.println();

      out.println("BindingsMap:");

      if (bindingsMap.isEmpty())
      {
         out.println("EMPTY!");
      }
      for (Map.Entry<Long, Binding> entry : bindingsMap.entrySet())
      {
         out.println("Key=" + entry.getKey() + ", value=" + entry.getValue());
      }

      out.println();

      out.println("ExclusiveBindings:");
      if (exclusiveBindings.isEmpty())
      {
         out.println("EMPTY!");
      }

      for (Binding binding : exclusiveBindings)
      {
         out.println(binding);
      }

      out.println("#####################################################");


      return writer.toString();
   }


   private void routeFromCluster(final ServerMessage message, final RoutingContext context, final byte[] ids) throws Exception
   {
      byte[] idsToAck = (byte[]) message.removeProperty(MessageImpl.HDR_ROUTE_TO_ACK_IDS);

      List<Long> idsToAckList = new ArrayList<>();

      if (idsToAck != null)
      {
         ByteBuffer buff = ByteBuffer.wrap(idsToAck);
         while (buff.hasRemaining())
         {
            long bindingID = buff.getLong();
            idsToAckList.add(bindingID);
         }
      }

      ByteBuffer buff = ByteBuffer.wrap(ids);

      while (buff.hasRemaining())
      {
         long bindingID = buff.getLong();

         Binding binding = bindingsMap.get(bindingID);
         if (binding != null)
         {
            if (idsToAckList.contains(bindingID))
            {
               binding.routeWithAck(message, context);
            }
            else
            {
               binding.route(message, context);
            }
         }
         else
         {
            HornetQServerLogger.LOGGER.warn("Couldn't find binding with id=" + bindingID + " on routeFromCluster for message=" + message + " binding = " + this);
         }
      }
   }

   private int incrementPos(int pos, final int length)
   {
      pos++;

      if (pos == length)
      {
         pos = 0;
      }

      return pos;
   }

}
