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
package org.apache.activemq.core.postoffice.impl;

import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.core.filter.Filter;
import org.apache.activemq.core.postoffice.Binding;
import org.apache.activemq.core.postoffice.BindingType;
import org.apache.activemq.core.server.Bindable;
import org.apache.activemq.core.server.Divert;
import org.apache.activemq.core.server.RoutingContext;
import org.apache.activemq.core.server.ServerMessage;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class DivertBinding implements Binding
{
   private final SimpleString address;

   private final Divert divert;

   private final Filter filter;

   private final SimpleString uniqueName;

   private final SimpleString routingName;

   private final boolean exclusive;

   private final long id;

   public DivertBinding(final long id, final SimpleString address, final Divert divert)
   {
      this.id = id;

      this.address = address;

      this.divert = divert;

      filter = divert.getFilter();

      uniqueName = divert.getUniqueName();

      routingName = divert.getRoutingName();

      exclusive = divert.isExclusive();
   }

   public long getID()
   {
      return id;
   }

   public Filter getFilter()
   {
      return filter;
   }

   public SimpleString getAddress()
   {
      return address;
   }

   public Bindable getBindable()
   {
      return divert;
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
      return exclusive;
   }

   public boolean isHighAcceptPriority(final ServerMessage message)
   {
      return true;
   }

   public void route(final ServerMessage message, final RoutingContext context) throws Exception
   {
      divert.route(message, context);
   }

   public int getDistance()
   {
      return 0;
   }

   public BindingType getType()
   {
      return BindingType.DIVERT;
   }

   @Override
   public void unproposed(SimpleString groupID)
   {
   }

   @Override
   public String toString()
   {
      return "DivertBinding [id=" + id +
             ", address=" +
             address +
             ", divert=" +
             divert +
             ", filter=" +
             filter +
             ", uniqueName=" +
             uniqueName +
             ", routingName=" +
             routingName +
             ", exclusive=" +
             exclusive +
             "]";
   }

   @Override
   public String toManagementString()
   {
      return this.getClass().getSimpleName() + " [id=" + id + "]";
   }

   @Override
   public boolean isConnected()
   {
      return true;
   }

   @Override
   public void routeWithAck(ServerMessage message, RoutingContext context)
   {
     //noop
   }

   public void close() throws Exception
   {
   }

}
