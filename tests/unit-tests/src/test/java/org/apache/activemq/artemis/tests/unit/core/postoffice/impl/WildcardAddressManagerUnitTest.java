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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.activemq.artemis.api.core.ActiveMQQueueExistsException;
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
import org.apache.activemq.artemis.core.postoffice.impl.WildcardAddressManager;
import org.apache.activemq.artemis.core.server.Bindable;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.RoutingContext;
import org.apache.activemq.artemis.core.server.cluster.impl.MessageLoadBalancingType;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.invoke.MethodHandles;

import java.util.function.BiConsumer;

/**
 * This test is replicating the behaviour from https://issues.jboss.org/browse/HORNETQ-988.
 */
public class WildcardAddressManagerUnitTest extends ActiveMQTestBase {
   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   @Test
   public void testUnitOnWildCardFailingScenario() throws Exception {
      int errors = 0;
      WildcardAddressManager ad = new WildcardAddressManager(new BindingFactoryFake(), null, null);
      ad.addBinding(new BindingFake("Topic1", "Topic1"));
      ad.addBinding(new BindingFake("Topic1", "one"));
      ad.addBinding(new BindingFake("*", "two"));
      ad.removeBinding(SimpleString.of("one"), null);
      try {
         ad.removeBinding(SimpleString.of("two"), null);
      } catch (Throwable e) {
         // We are not failing the test here as this test is replicating the exact scenario
         // that was happening under https://issues.jboss.org/browse/HORNETQ-988
         // In which this would be ignored
         errors++;
         e.printStackTrace();
      }
      try {
         ad.addBinding(new BindingFake("Topic1", "three"));
      } catch (Throwable e) {
         // We are not failing the test here as this test is replicating the exact scenario
         // that was happening under https://issues.jboss.org/browse/HORNETQ-988
         // In which this would be ignored
         errors++;
         e.printStackTrace();
      }

      assertEquals(0, errors, "Exception happened during the process");
   }

   @Test
   public void testUnitOnWildCardFailingScenarioFQQN() throws Exception {
      int errors = 0;
      WildcardAddressManager ad = new WildcardAddressManager(new BindingFactoryFake(), null, null);
      ad.addBinding(new BindingFake("Topic1", "Topic1"));
      ad.addBinding(new BindingFake("Topic1", "one"));
      ad.addBinding(new BindingFake("*", "two"));
      ad.removeBinding(SimpleString.of("Topic1::one"), null);
      try {
         ad.removeBinding(SimpleString.of("*::two"), null);
      } catch (Throwable e) {
         // We are not failing the test here as this test is replicating the exact scenario
         // that was happening under https://issues.jboss.org/browse/HORNETQ-988
         // In which this would be ignored
         errors++;
         e.printStackTrace();
      }
      try {
         ad.addBinding(new BindingFake("Topic1", "three"));
      } catch (Throwable e) {
         // We are not failing the test here as this test is replicating the exact scenario
         // that was happening under https://issues.jboss.org/browse/HORNETQ-988
         // In which this would be ignored
         errors++;
         e.printStackTrace();
      }

      assertEquals(0, errors, "Exception happened during the process");
   }

   /**
    * Test for ARTEMIS-1610
    */
   @Test
   public void testWildCardAddressRemoval() throws Exception {

      WildcardAddressManager ad = new WildcardAddressManager(new BindingFactoryFake(), null, null);
      ad.addAddressInfo(new AddressInfo(SimpleString.of("Queue1.#"), RoutingType.ANYCAST));
      ad.addAddressInfo(new AddressInfo(SimpleString.of("Topic1.#"), RoutingType.MULTICAST));
      ad.addBinding(new BindingFake("Topic1.#", "two"));
      ad.addBinding(new BindingFake("Queue1.#", "one"));

      //Calling this method will trigger the wildcard to be added to the wildcard map internal
      //to WildcardAddressManager
      assertEquals(1, ad.getBindingsForRoutingAddress(SimpleString.of("Topic1.topic")).getBindings().size());

      //Remove the address
      ad.removeAddressInfo(SimpleString.of("Topic1.#"));

      assertNull(ad.getAddressInfo(SimpleString.of("Topic1.#")));
   }

   @Test
   public void testWildCardAddRemoveBinding() throws Exception {
      WildcardAddressManager ad = new WildcardAddressManager(new BindingFactoryFake(), null, null);
      SimpleString address = SimpleString.of("Queue1.1");
      ad.addAddressInfo(new AddressInfo(SimpleString.of("Queue1.#"), RoutingType.ANYCAST));

      BindingFake bindingFake = new BindingFake("Queue1.#", "one");
      assertTrue(ad.addBinding(bindingFake));

      assertEquals(1, ad.getBindingsForRoutingAddress(address).getBindings().size());

      ad.removeBinding(bindingFake.getUniqueName(), null);

      assertNull(ad.getExistingBindingsForRoutingAddress(address));

   }


   @Test
   public void testWildCardAddAlreadyExistingBindingShouldThrowException() throws Exception {
      assertThrows(ActiveMQQueueExistsException.class, () -> {
         WildcardAddressManager ad = new WildcardAddressManager(new BindingFactoryFake(), null, null);
         ad.addAddressInfo(new AddressInfo(SimpleString.of("Queue1.#"), RoutingType.ANYCAST));
         ad.addBinding(new BindingFake("Queue1.#", "one"));
         ad.addBinding(new BindingFake("Queue1.#", "one"));
      });
   }

   @Test
   public void testWildCardAddressRemovalDifferentWildcard() throws Exception {

      final WildcardConfiguration configuration = new WildcardConfiguration();
      configuration.setAnyWords('>');
      WildcardAddressManager ad = new WildcardAddressManager(new BindingFactoryFake(), configuration, null, null);
      ad.addAddressInfo(new AddressInfo(SimpleString.of("Topic1.>"), RoutingType.MULTICAST));
      ad.addAddressInfo(new AddressInfo(SimpleString.of("Topic1.test"), RoutingType.MULTICAST));
      ad.addBinding(new BindingFake("Topic1.>", "one"));

      assertEquals(1, ad.getExistingBindingsForRoutingAddress(SimpleString.of("Topic1.>")).getBindings().size());
      assertEquals(1, ad.getBindingsForRoutingAddress(SimpleString.of("Topic1.test")).getBindings().size());
      assertEquals(0, ad.getDirectBindings(SimpleString.of("Topic1.test")).size());
      assertEquals(1, ad.getDirectBindings(SimpleString.of("Topic1.>")).size());

      //Remove the address
      ad.removeAddressInfo(SimpleString.of("Topic1.test"));

      //should still have 1 address and binding
      assertEquals(1, ad.getAddresses().size());
      assertEquals(1, ad.getBindings().count());

      ad.removeBinding(SimpleString.of("one"), null);
      ad.removeAddressInfo(SimpleString.of("Topic1.>"));

      assertEquals(0, ad.getAddresses().size());
      assertEquals(0, ad.getBindings().count());
   }

   @Test
   public void testWildCardAddressDirectBindings() throws Exception {

      final WildcardConfiguration configuration = new WildcardConfiguration();
      configuration.setAnyWords('>');
      WildcardAddressManager ad = new WildcardAddressManager(new BindingFactoryFake(), configuration, null, null);
      ad.addAddressInfo(new AddressInfo(SimpleString.of("Topic1.>"), RoutingType.MULTICAST));
      ad.addAddressInfo(new AddressInfo(SimpleString.of("Topic1.test"), RoutingType.MULTICAST));
      ad.addAddressInfo(new AddressInfo(SimpleString.of("Topic1.test.test1"), RoutingType.MULTICAST));
      ad.addAddressInfo(new AddressInfo(SimpleString.of("Topic1.test.test2"), RoutingType.MULTICAST));
      ad.addAddressInfo(new AddressInfo(SimpleString.of("Topic2.>"), RoutingType.MULTICAST));
      ad.addAddressInfo(new AddressInfo(SimpleString.of("Topic2.test"), RoutingType.MULTICAST));
      ad.addBinding(new BindingFake("Topic1.>", "one"));
      ad.addBinding(new BindingFake("Topic1.test", "two"));
      ad.addBinding(new BindingFake("Topic2.test", "three"));

      assertEquals(1, ad.getExistingBindingsForRoutingAddress(SimpleString.of("Topic1.>")).getBindings().size());
      assertEquals(2, ad.getBindingsForRoutingAddress(SimpleString.of("Topic1.test")).getBindings().size());
      assertEquals(1, ad.getBindingsForRoutingAddress(SimpleString.of("Topic1.test.test1")).getBindings().size());
      assertEquals(1, ad.getBindingsForRoutingAddress(SimpleString.of("Topic1.test.test2")).getBindings().size());

      assertEquals(1, ad.getDirectBindings(SimpleString.of("Topic1.>")).size());
      assertEquals(1, ad.getDirectBindings(SimpleString.of("Topic1.test")).size());
      assertEquals(0, ad.getDirectBindings(SimpleString.of("Topic1.test1")).size());
      assertEquals(0, ad.getDirectBindings(SimpleString.of("Topic1.test2")).size());
      assertEquals(0, ad.getDirectBindings(SimpleString.of("Topic2.>")).size());
      assertEquals(1, ad.getDirectBindings(SimpleString.of("Topic2.test")).size());

   }

   @Test
   public void testSingleWordWildCardAddressBindingsForRouting() throws Exception {
      WildcardAddressManager ad = new WildcardAddressManager(new BindingFactoryFake(), null, null);
      ad.addAddressInfo(new AddressInfo(SimpleString.of("news.*"), RoutingType.MULTICAST));
      ad.addAddressInfo(new AddressInfo(SimpleString.of("news.*.sport"), RoutingType.MULTICAST));
      ad.addBinding(new BindingFake("news.*", "one"));
      ad.addBinding(new BindingFake("news.*.sport", "two"));

      Collection<Binding> bindings = ad.getBindingsForRoutingAddress(SimpleString.of("news.europe")).getBindings();
      assertEquals(1, bindings.size());
      assertEquals("one", bindings.iterator().next().getUniqueName().toString());
      bindings = ad.getBindingsForRoutingAddress(SimpleString.of("news.usa")).getBindings();
      assertEquals(1, bindings.size());
      assertEquals("one", bindings.iterator().next().getUniqueName().toString());
      bindings = ad.getBindingsForRoutingAddress(SimpleString.of("news.europe.sport")).getBindings();
      assertEquals(1, bindings.size());
      assertEquals("two", bindings.iterator().next().getUniqueName().toString());
      bindings = ad.getBindingsForRoutingAddress(SimpleString.of("news.usa.sport")).getBindings();
      assertEquals(1, bindings.size());
      assertEquals("two", bindings.iterator().next().getUniqueName().toString());
      assertNull(ad.getBindingsForRoutingAddress(SimpleString.of("news.europe.fr.sport")));
   }

   @Test
   public void testAnyWordsWildCardAddressBindingsForRouting() throws Exception {
      WildcardAddressManager ad = new WildcardAddressManager(new BindingFactoryFake(), null, null);
      ad.addAddressInfo(new AddressInfo(SimpleString.of("news.europe.#"), RoutingType.MULTICAST));
      ad.addBinding(new BindingFake("news.europe.#", "one"));

      assertEquals(1, ad.getBindingsForRoutingAddress(SimpleString.of("news.europe")).getBindings().size());
      assertEquals(1, ad.getBindingsForRoutingAddress(SimpleString.of("news.europe.sport")).getBindings().size());
      assertEquals(1, ad.getBindingsForRoutingAddress(SimpleString.of("news.europe.politics.fr")).getBindings().size());
      assertNull(ad.getBindingsForRoutingAddress(SimpleString.of("news.usa")));
      assertNull(ad.getBindingsForRoutingAddress(SimpleString.of("europe")));
   }

   @Test
   public void testAnyWordsMultipleWildCardsAddressBindingsForRouting() throws Exception {
      WildcardAddressManager ad = new WildcardAddressManager(new BindingFactoryFake(), null, null);
      ad.addAddressInfo(new AddressInfo(SimpleString.of("news.#"), RoutingType.MULTICAST));
      ad.addAddressInfo(new AddressInfo(SimpleString.of("news.europe.#"), RoutingType.MULTICAST));
      ad.addBinding(new BindingFake("news.#", "one"));
      ad.addBinding(new BindingFake("news.europe.#", "two"));

      assertEquals(2, ad.getBindingsForRoutingAddress(SimpleString.of("news.europe")).getBindings().size());
      assertEquals(2, ad.getBindingsForRoutingAddress(SimpleString.of("news.europe.sport")).getBindings().size());
      assertEquals(2, ad.getBindingsForRoutingAddress(SimpleString.of("news.europe.politics.fr")).getBindings().size());

      Collection<Binding> bindings = ad.getBindingsForRoutingAddress(SimpleString.of("news.usa")).getBindings();
      assertEquals(1, bindings.size());
      assertEquals("one", bindings.iterator().next().getUniqueName().toString());
      assertNull(ad.getBindingsForRoutingAddress(SimpleString.of("europe")));
   }

   @Test
   public void testNumberOfBindingsThatMatch() throws Exception {

      final WildcardConfiguration configuration = new WildcardConfiguration();
      configuration.setAnyWords('>');
      WildcardAddressManager ad = new WildcardAddressManager(new BindingFactoryFake(), configuration, null, null);

      ad.addBinding(new BindingFake("T.>", "1"));
      ad.addBinding(new BindingFake("T.>", "2"));
      ad.addBinding(new BindingFake("T.>", "3"));

      assertEquals(3, ad.getBindingsForRoutingAddress(SimpleString.of("T.1")).getBindings().size());
      assertEquals(3, ad.getBindingsForRoutingAddress(SimpleString.of("T.2")).getBindings().size());
      assertEquals(3, ad.getBindingsForRoutingAddress(SimpleString.of("T.3")).getBindings().size());


      assertEquals(3, ad.getExistingBindingsForRoutingAddress(SimpleString.of("T.>")).getBindings().size());

      ad.addBinding(new BindingFake("T.*", "10"));
      assertEquals(1, ad.getExistingBindingsForRoutingAddress(SimpleString.of("T.*")).getBindings().size());

      // wildcard binding should not be added to existing matching wildcards, still 3
      assertEquals(3, ad.getExistingBindingsForRoutingAddress(SimpleString.of("T.>")).getBindings().size());

      assertEquals(4, ad.getBindingsForRoutingAddress(SimpleString.of("T.1")).getBindings().size());
      assertEquals(4, ad.getBindingsForRoutingAddress(SimpleString.of("T.2")).getBindings().size());
      assertEquals(4, ad.getBindingsForRoutingAddress(SimpleString.of("T.3")).getBindings().size());


      ad.addBinding(new BindingFake("T.1.>", "11"));
      assertEquals(1, ad.getExistingBindingsForRoutingAddress(SimpleString.of("T.1.>")).getBindings().size());

      assertEquals(5, ad.getBindingsForRoutingAddress(SimpleString.of("T.1")).getBindings().size());
      assertEquals(4, ad.getBindingsForRoutingAddress(SimpleString.of("T.2")).getBindings().size());
      assertEquals(4, ad.getBindingsForRoutingAddress(SimpleString.of("T.3")).getBindings().size());

      ad.addBinding(new BindingFake("T.1.2", "12"));

      assertEquals(5, ad.getBindingsForRoutingAddress(SimpleString.of("T.1.2")).getBindings().size());

      ad.addBinding(new BindingFake("T.1.2.3.4", "13"));
      assertEquals(5, ad.getBindingsForRoutingAddress(SimpleString.of("T.1.2.3.4")).getBindings().size());

      ad.addBinding(new BindingFake("T.>.4", "14"));

      assertEquals(6, ad.getBindingsForRoutingAddress(SimpleString.of("T.1.2.3.4")).getBindings().size());

      ad.addBinding(new BindingFake("T.1.A.3.4", "15"));

      assertEquals(6, ad.getBindingsForRoutingAddress(SimpleString.of("T.1.A.3.4")).getBindings().size());

   }

   @Test
   public void testConcurrentCalls() throws Exception {
      final WildcardConfiguration configuration = new WildcardConfiguration();
      configuration.setAnyWords('>');
      final WildcardAddressManager ad = new WildcardAddressManager(new BindingFactoryFake(), configuration, null, null);

      final SimpleString wildCard = SimpleString.of("Topic1.>");
      ad.addAddressInfo(new AddressInfo(wildCard, RoutingType.MULTICAST));

      AtomicReference<Throwable> oops = new AtomicReference<>();
      int numSubs = 500;
      int numThreads = 2;
      ExecutorService executorService = Executors.newFixedThreadPool(numThreads);

      for (int i = 0; i < numSubs; i++ ) {
         final int id = i;

         executorService.submit(() -> {
            try {

               // add/remove is externally sync via postOffice
               synchronized (executorService) {
                  // subscribe as wildcard
                  ad.addBinding(new BindingFake(SimpleString.of("Topic1.>"), SimpleString.of("" + id)));
               }

               SimpleString pubAddr = SimpleString.of("Topic1." + id );
               // publish to new address, will create
               ad.getBindingsForRoutingAddress(pubAddr);

               // publish again, read only
               ad.getBindingsForRoutingAddress(pubAddr);

            } catch (Exception e) {
               e.printStackTrace();
               oops.set(e);
            }
         });
      }

      executorService.shutdown();
      assertTrue(executorService.awaitTermination(10, TimeUnit.MINUTES), "finished on time");
      assertNull(oops.get(), "no exceptions");
   }

   static class BindingFactoryFake implements BindingsFactory {

      @Override
      public boolean isAddressBound(SimpleString address) throws Exception {
         return false;
      }

      @Override
      public Bindings createBindings(SimpleString address) {
         return new BindingsFake(address);
      }
   }

   static class BindingFake implements Binding {

      final SimpleString address;
      final SimpleString id;

      BindingFake(String addressParameter, String id) {
         this(SimpleString.of(addressParameter), SimpleString.of(id));
      }

      BindingFake(SimpleString addressParameter, SimpleString id) {
         this.address = addressParameter;
         this.id = id;
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
         return null;
      }

      @Override
      public SimpleString getUniqueName() {
         return id;
      }

      @Override
      public SimpleString getRoutingName() {
         return null;
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
         return 0L;
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

   static class BindingsFake implements Bindings {

      SimpleString name;
      ConcurrentHashMap<String, Binding> bindings = new ConcurrentHashMap<>();

      BindingsFake(SimpleString address) {
         this.name = address;
      }

      @Override
      public boolean hasLocalBinding() {
         return false;
      }

      @Override
      public Collection<Binding> getBindings() {
         return bindings.values();
      }

      @Override
      public void addBinding(Binding binding) {
         bindings.put(String.valueOf(binding.getUniqueName()), binding);
      }

      @Override
      public Binding removeBindingByUniqueName(SimpleString uniqueName) {
         return bindings.remove(String.valueOf(uniqueName));
      }

      @Override
      public SimpleString getName() {
         return name;
      }

      @Override
      public void setMessageLoadBalancingType(MessageLoadBalancingType messageLoadBalancingType) {

      }

      @Override
      public Binding getBinding(String name) {
         return bindings.get(name);
      }

      @Override
      public void forEach(BiConsumer<String, Binding> bindingConsumer) {
         bindings.forEach(bindingConsumer);
      }

      @Override
      public int size() {
         return bindings.size();
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
      public Message redistribute(Message message,
                                  Queue originatingQueue,
                                  RoutingContext context) throws Exception {
         return null;
      }

      @Override
      public void route(Message message, RoutingContext context) throws Exception {
         logger.debug("routing message: {}", message);
      }

      @Override
      public boolean allowRedistribute() {
         return false;
      }
   }

}
