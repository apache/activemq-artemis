/**
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
package org.apache.activemq.core.server.impl;

import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.core.filter.Filter;
import org.apache.activemq.core.persistence.StorageManager;
import org.apache.activemq.core.postoffice.PostOffice;
import org.apache.activemq.core.server.ActiveMQServerLogger;
import org.apache.activemq.core.server.Divert;
import org.apache.activemq.core.server.RoutingContext;
import org.apache.activemq.core.server.ServerMessage;
import org.apache.activemq.core.server.cluster.Transformer;

/**
 * A DivertImpl simply diverts a message to a different forwardAddress
 */
public class DivertImpl implements Divert
{
   private final PostOffice postOffice;

   private final SimpleString forwardAddress;

   private final SimpleString uniqueName;

   private final SimpleString routingName;

   private final boolean exclusive;

   private final Filter filter;

   private final Transformer transformer;

   private final StorageManager storageManager;

   public DivertImpl(final SimpleString forwardAddress,
                     final SimpleString uniqueName,
                     final SimpleString routingName,
                     final boolean exclusive,
                     final Filter filter,
                     final Transformer transformer,
                     final PostOffice postOffice,
                     final StorageManager storageManager)
   {
      this.forwardAddress = forwardAddress;

      this.uniqueName = uniqueName;

      this.routingName = routingName;

      this.exclusive = exclusive;

      this.filter = filter;

      this.transformer = transformer;

      this.postOffice = postOffice;

      this.storageManager = storageManager;
   }

   public void route(final ServerMessage message, final RoutingContext context) throws Exception
   {
      // We must make a copy of the message, otherwise things like returning credits to the page won't work
      // properly on ack, since the original address will be overwritten

      if (ActiveMQServerLogger.LOGGER.isTraceEnabled())
      {
         ActiveMQServerLogger.LOGGER.trace("Diverting message " + message + " into " + this);
      }

      long id = storageManager.generateID();

      ServerMessage copy = null;

      // Shouldn't copy if it's not routed anywhere else
      if (!forwardAddress.equals(message.getAddress()))
      {
         copy = message.copy(id);
         copy.finishCopy();

         // This will set the original MessageId, and the original address
         copy.setOriginalHeaders(message, null, false);

         copy.setAddress(forwardAddress);

         copy.setExpiration(message.getExpiration());

         if (transformer != null)
         {
            copy = transformer.transform(copy);
         }
      }
      else
      {
         copy = message;
      }

      postOffice.route(copy, context.getTransaction(), false);
   }

   @Override
   public void routeWithAck(ServerMessage message, RoutingContext context) throws Exception
   {
      route(message, context);
   }


   public SimpleString getRoutingName()
   {
      return routingName;
   }

   public SimpleString getUniqueName()
   {
      return uniqueName;
   }

   public boolean isExclusive()
   {
      return exclusive;
   }

   public Filter getFilter()
   {
      return filter;
   }

   /* (non-Javadoc)
    * @see java.lang.Object#toString()
    */
   @Override
   public String toString()
   {
      return "DivertImpl [routingName=" + routingName +
             ", uniqueName=" +
             uniqueName +
             ", forwardAddress=" +
             forwardAddress +
             ", exclusive=" +
             exclusive +
             ", filter=" +
             filter +
             ", transformer=" +
             transformer +
             "]";
   }

}
