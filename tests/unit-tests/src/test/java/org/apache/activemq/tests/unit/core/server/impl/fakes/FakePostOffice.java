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
package org.apache.activemq.tests.unit.core.server.impl.fakes;

import java.util.Map;
import java.util.Set;

import org.apache.activemq.api.core.Pair;
import org.apache.activemq.api.core.SimpleString;
import org.apache.activemq.core.persistence.impl.nullpm.NullStorageManager;
import org.apache.activemq.core.postoffice.Binding;
import org.apache.activemq.core.postoffice.Bindings;
import org.apache.activemq.core.postoffice.DuplicateIDCache;
import org.apache.activemq.core.postoffice.PostOffice;
import org.apache.activemq.core.postoffice.impl.DuplicateIDCacheImpl;
import org.apache.activemq.core.server.MessageReference;
import org.apache.activemq.core.server.Queue;
import org.apache.activemq.core.server.RoutingContext;
import org.apache.activemq.core.server.ServerMessage;
import org.apache.activemq.core.server.impl.MessageReferenceImpl;
import org.apache.activemq.core.transaction.Transaction;

public class FakePostOffice implements PostOffice
{

   @Override
   public boolean isStarted()
   {

      return false;
   }

   @Override
   public Set<SimpleString> getAddresses()
   {
      return null;
   }

   @Override
   public void start() throws Exception
   {


   }

   @Override
   public void stop() throws Exception
   {


   }

   @Override
   public void addBinding(final Binding binding) throws Exception
   {


   }

   @Override
   public Binding getBinding(final SimpleString uniqueName)
   {

      return null;
   }

   @Override
   public Bindings getBindingsForAddress(final SimpleString address) throws Exception
   {

      return null;
   }

   @Override
   public Map<SimpleString, Binding> getAllBindings()
   {
      return null;
   }

   public Bindings lookupBindingsForAddress(final SimpleString address) throws Exception
   {

      return null;
   }

   @Override
   public DuplicateIDCache getDuplicateIDCache(final SimpleString address)
   {
      return new DuplicateIDCacheImpl(address, 2000, new NullStorageManager(), false);
   }

   @Override
   public Bindings getMatchingBindings(final SimpleString address)
   {

      return null;
   }

   @Override
   public Object getNotificationLock()
   {

      return null;
   }

   @Override
   public void startExpiryScanner()
   {
   }

   @Override
   public boolean isAddressBound(SimpleString address) throws Exception
   {
      return false;
   }

   @Override
   public Binding removeBinding(final SimpleString uniqueName, final Transaction tx) throws Exception
   {

      return null;
   }

   @Override
   public void sendQueueInfoToQueue(final SimpleString queueName, final SimpleString address) throws Exception
   {


   }

   public Pair<RoutingContext, ServerMessage> redistribute(final ServerMessage message, final Queue originatingQueue, final Transaction tx) throws Exception
   {
      return null;
   }

   public MessageReference reroute(final ServerMessage message, final Queue queue, final Transaction tx) throws Exception
   {
      message.incrementRefCount();
      return new MessageReferenceImpl();
   }

   public void route(final ServerMessage message, final Transaction tx) throws Exception
   {


   }

   public void route(final ServerMessage message, final RoutingContext context) throws Exception
   {


   }

   public void route(ServerMessage message, boolean direct) throws Exception
   {


   }

   public void route(ServerMessage message, RoutingContext context, boolean direct) throws Exception
   {


   }

   public void route(ServerMessage message, Transaction tx, boolean direct) throws Exception
   {


   }

   @Override
   public void route(ServerMessage message, RoutingContext context, boolean direct, boolean rejectDuplicates) throws Exception
   {


   }

   @Override
   public void route(ServerMessage message, Transaction tx, boolean direct, boolean rejectDuplicates) throws Exception
   {


   }

   @Override
   public void processRoute(ServerMessage message, RoutingContext context, boolean direct) throws Exception
   {


   }

}