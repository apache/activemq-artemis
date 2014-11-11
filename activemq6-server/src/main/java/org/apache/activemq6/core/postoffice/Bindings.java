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
package org.apache.activemq6.core.postoffice;

import java.util.Collection;

import org.apache.activemq6.core.server.Queue;
import org.apache.activemq6.core.server.RoutingContext;
import org.apache.activemq6.core.server.ServerMessage;
import org.apache.activemq6.core.server.group.UnproposalListener;

/**
 * A Bindings
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *         <p/>
 *         Created 10 Dec 2008 19:10:52
 */
public interface Bindings extends UnproposalListener
{
   Collection<Binding> getBindings();

   void addBinding(Binding binding);

   void removeBinding(Binding binding);

   void setRouteWhenNoConsumers(boolean takePriorityIntoAccount);

   boolean redistribute(ServerMessage message, Queue originatingQueue, RoutingContext context) throws Exception;

   void route(ServerMessage message, RoutingContext context) throws Exception;
}
