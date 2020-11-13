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
package org.apache.activemq.artemis.tests.unit.core.postoffice.impl;

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.WildcardConfiguration;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.BindingType;
import org.apache.activemq.artemis.core.postoffice.Bindings;
import org.apache.activemq.artemis.core.postoffice.BindingsFactory;
import org.apache.activemq.artemis.core.postoffice.QueueBinding;
import org.apache.activemq.artemis.core.postoffice.impl.BindingsImpl;
import org.apache.activemq.artemis.core.postoffice.impl.WildcardAddressManager;
import org.apache.activemq.artemis.core.server.Bindable;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.RoutingContext;
import org.apache.activemq.artemis.core.server.cluster.impl.MessageLoadBalancingType;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.utils.collections.ConcurrentHashSet;
import org.jboss.logging.Logger;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class WildcardAddressManagerPerfTest {
   private static final Logger log = Logger.getLogger(WildcardAddressManagerPerfTest.class);

   @Test
   @Ignore
   public void testConcurrencyAndEfficiency() throws Exception {

      System.out.println("Type so we can go on..");
      //TimeUnit.SECONDS.sleep(20);
      System.out.println("we can go on..");

      final WildcardConfiguration configuration = new WildcardConfiguration();
      configuration.setAnyWords('>');
      final WildcardAddressManager ad = new WildcardAddressManager(new BindingFactoryFake(), configuration, null, null);

      final SimpleString wildCard = SimpleString.toSimpleString("Topic1.>");
      ad.addAddressInfo(new AddressInfo(wildCard, RoutingType.MULTICAST));

      int numSubs = 1000;
      int numThreads = 1;
      ExecutorService executorService = Executors.newFixedThreadPool(numThreads);

      for (int i = 0; i < numSubs; i++ ) {
         final int id = i;

         executorService.submit(() -> {
            try {

               if (id % 500 == 0) {
                  // give gc a chance
                  Thread.yield();
               }

               // subscribe as wildcard
               ad.addBinding(new BindingFake(SimpleString.toSimpleString("Topic1.>"), SimpleString.toSimpleString("" + id), id));

               SimpleString pubAddr = SimpleString.toSimpleString("Topic1." + id );
               // publish
               Bindings binding = ad.getBindingsForRoutingAddress(pubAddr);

               if (id % 100 == 0) {
                  System.err.println("1. Bindings for: " + id + ", " + binding.getBindings().size());
               }

               // publish again
               binding = ad.getBindingsForRoutingAddress(pubAddr);

               if (id % 100 == 0) {
                  System.err.println("2. Bindings for: " + id + ", " + binding.getBindings().size());
               }

               // cluster consumer
               //ad.updateMessageLoadBalancingTypeForAddress(wildCard, MessageLoadBalancingType.ON_DEMAND);

            } catch (Exception e) {
               e.printStackTrace();
            }
         });
      }

      executorService.shutdown();
      assertTrue("finished on time", executorService.awaitTermination(10, TimeUnit.MINUTES));

      // TimeUnit.MINUTES.sleep(5);

      System.out.println("Type so we can go on..");
      // System.in.read();
      System.out.println("we can go on..");


   }
   class BindingFactoryFake implements BindingsFactory {

      @Override
      public Bindings createBindings(SimpleString address) throws Exception {
         return new BindingsImpl(address, null);
      }
   }

   class BindingFake implements Binding {

      final SimpleString address;
      final SimpleString id;
      final Long idl;

      BindingFake(SimpleString addressParameter, SimpleString id, long idl) {
         this.address = addressParameter;
         this.id = id;
         this.idl = idl;
      }

      @Override
      public void unproposed(SimpleString groupID) {

      }

      @Override
      public SimpleString getAddress() {
         return address;
      }

      @Override
      public Bindable getBindable() {
         return null;
      }

      @Override
      public BindingType getType() {
         return BindingType.LOCAL_QUEUE;
      }

      @Override
      public SimpleString getUniqueName() {
         return id;
      }

      @Override
      public SimpleString getRoutingName() {
         return id;
      }

      @Override
      public SimpleString getClusterName() {
         return null;
      }

      @Override
      public Filter getFilter() {
         return null;
      }

      @Override
      public boolean isHighAcceptPriority(Message message) {
         return false;
      }

      @Override
      public boolean isExclusive() {
         return false;
      }

      @Override
      public Long getID() {
         return idl;
      }

      @Override
      public int getDistance() {
         return 0;
      }

      @Override
      public void route(Message message, RoutingContext context) throws Exception {
      }

      @Override
      public void close() throws Exception {
      }

      @Override
      public String toManagementString() {
         return "FakeBiding Address=" + this.address;
      }

      @Override
      public boolean isConnected() {
         return true;
      }

      @Override
      public void routeWithAck(Message message, RoutingContext context) {

      }
   }

   class BindingsFake implements Bindings {

      ConcurrentHashSet<Binding> bindings = new ConcurrentHashSet<>();

      @Override
      public Collection<Binding> getBindings() {
         return bindings;
      }

      @Override
      public void addBinding(Binding binding) {
         bindings.addIfAbsent(binding);
      }

      @Override
      public void removeBinding(Binding binding) {
         bindings.remove(binding);
      }

      @Override
      public void setMessageLoadBalancingType(MessageLoadBalancingType messageLoadBalancingType) {

      }

      @Override
      public MessageLoadBalancingType getMessageLoadBalancingType() {
         return null;
      }

      @Override
      public void unproposed(SimpleString groupID) {
      }

      @Override
      public void updated(QueueBinding binding) {
      }

      @Override
      public boolean redistribute(Message message,
                                  Queue originatingQueue,
                                  RoutingContext context) throws Exception {
         return false;
      }

      @Override
      public void route(Message message, RoutingContext context) throws Exception {
         log.debug("routing message: " + message);
      }

      @Override
      public boolean allowRedistribute() {
         return false;
      }
   }

}
