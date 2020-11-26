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
import org.jboss.logging.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 * This test is replicating the behaviour from https://issues.jboss.org/browse/HORNETQ-988.
 */
public class WildcardAddressManagerUnitTest extends ActiveMQTestBase {
   private static final Logger log = Logger.getLogger(WildcardAddressManagerUnitTest.class);

   @Test
   public void testUnitOnWildCardFailingScenario() throws Exception {
      int errors = 0;
      WildcardAddressManager ad = new WildcardAddressManager(new BindingFactoryFake(), null, null);
      ad.addBinding(new BindingFake("Topic1", "Topic1"));
      ad.addBinding(new BindingFake("Topic1", "one"));
      ad.addBinding(new BindingFake("*", "two"));
      ad.removeBinding(SimpleString.toSimpleString("one"), null);
      try {
         ad.removeBinding(SimpleString.toSimpleString("two"), null);
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

      assertEquals("Exception happened during the process", 0, errors);
   }

   @Test
   public void testUnitOnWildCardFailingScenarioFQQN() throws Exception {
      int errors = 0;
      WildcardAddressManager ad = new WildcardAddressManager(new BindingFactoryFake(), null, null);
      ad.addBinding(new BindingFake("Topic1", "Topic1"));
      ad.addBinding(new BindingFake("Topic1", "one"));
      ad.addBinding(new BindingFake("*", "two"));
      ad.removeBinding(SimpleString.toSimpleString("Topic1::one"), null);
      try {
         ad.removeBinding(SimpleString.toSimpleString("*::two"), null);
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

      assertEquals("Exception happened during the process", 0, errors);
   }

   /**
    * Test for ARTEMIS-1610
    */
   @Test
   public void testWildCardAddressRemoval() throws Exception {

      WildcardAddressManager ad = new WildcardAddressManager(new BindingFactoryFake(), null, null);
      ad.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("Queue1.#"), RoutingType.ANYCAST));
      ad.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("Topic1.#"), RoutingType.MULTICAST));
      ad.addBinding(new BindingFake("Topic1.#", "two"));
      ad.addBinding(new BindingFake("Queue1.#", "one"));

      //Calling this method will trigger the wildcard to be added to the wildcard map internal
      //to WildcardAddressManager
      assertEquals(1, ad.getBindingsForRoutingAddress(SimpleString.toSimpleString("Topic1.topic")).getBindings().size());

      //Remove the address
      ad.removeAddressInfo(SimpleString.toSimpleString("Topic1.#"));

      assertNull(ad.getAddressInfo(SimpleString.toSimpleString("Topic1.#")));
   }

   @Test
   public void testWildCardAddRemoveBinding() throws Exception {
      WildcardAddressManager ad = new WildcardAddressManager(new BindingFactoryFake(), null, null);
      SimpleString address = SimpleString.toSimpleString("Queue1.1");
      ad.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("Queue1.#"), RoutingType.ANYCAST));

      BindingFake bindingFake = new BindingFake("Queue1.#", "one");
      Assert.assertTrue(ad.addBinding(bindingFake));

      assertEquals(1, ad.getBindingsForRoutingAddress(address).getBindings().size());

      ad.removeBinding(bindingFake.getUniqueName(), null);

      assertNull(ad.getExistingBindingsForRoutingAddress(address));

   }


   @Test(expected = ActiveMQQueueExistsException.class)
   public void testWildCardAddAlreadyExistingBindingShouldThrowException() throws Exception {
      WildcardAddressManager ad = new WildcardAddressManager(new BindingFactoryFake(), null, null);
      ad.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("Queue1.#"), RoutingType.ANYCAST));
      ad.addBinding(new BindingFake("Queue1.#", "one"));
      ad.addBinding(new BindingFake("Queue1.#", "one"));
   }

   @Test
   public void testWildCardAddressRemovalDifferentWildcard() throws Exception {

      final WildcardConfiguration configuration = new WildcardConfiguration();
      configuration.setAnyWords('>');
      WildcardAddressManager ad = new WildcardAddressManager(new BindingFactoryFake(), configuration, null, null);
      ad.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("Topic1.>"), RoutingType.MULTICAST));
      ad.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("Topic1.test"), RoutingType.MULTICAST));
      ad.addBinding(new BindingFake("Topic1.>", "one"));

      assertEquals(1, ad.getExistingBindingsForRoutingAddress(SimpleString.toSimpleString("Topic1.>")).getBindings().size());
      assertEquals(1, ad.getBindingsForRoutingAddress(SimpleString.toSimpleString("Topic1.test")).getBindings().size());
      assertEquals(0, ad.getDirectBindings(SimpleString.toSimpleString("Topic1.test")).size());
      assertEquals(1, ad.getDirectBindings(SimpleString.toSimpleString("Topic1.>")).size());

      //Remove the address
      ad.removeAddressInfo(SimpleString.toSimpleString("Topic1.test"));

      //should still have 1 address and binding
      assertEquals(1, ad.getAddresses().size());
      assertEquals(1, ad.getBindings().count());

      ad.removeBinding(SimpleString.toSimpleString("one"), null);
      ad.removeAddressInfo(SimpleString.toSimpleString("Topic1.>"));

      assertEquals(0, ad.getAddresses().size());
      assertEquals(0, ad.getBindings().count());
   }

   @Test
   public void testWildCardAddressDirectBindings() throws Exception {

      final WildcardConfiguration configuration = new WildcardConfiguration();
      configuration.setAnyWords('>');
      WildcardAddressManager ad = new WildcardAddressManager(new BindingFactoryFake(), configuration, null, null);
      ad.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("Topic1.>"), RoutingType.MULTICAST));
      ad.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("Topic1.test"), RoutingType.MULTICAST));
      ad.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("Topic1.test.test1"), RoutingType.MULTICAST));
      ad.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("Topic1.test.test2"), RoutingType.MULTICAST));
      ad.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("Topic2.>"), RoutingType.MULTICAST));
      ad.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("Topic2.test"), RoutingType.MULTICAST));
      ad.addBinding(new BindingFake("Topic1.>", "one"));
      ad.addBinding(new BindingFake("Topic1.test", "two"));
      ad.addBinding(new BindingFake("Topic2.test", "three"));

      assertEquals(1, ad.getExistingBindingsForRoutingAddress(SimpleString.toSimpleString("Topic1.>")).getBindings().size());
      assertEquals(2, ad.getBindingsForRoutingAddress(SimpleString.toSimpleString("Topic1.test")).getBindings().size());
      assertEquals(1, ad.getBindingsForRoutingAddress(SimpleString.toSimpleString("Topic1.test.test1")).getBindings().size());
      assertEquals(1, ad.getBindingsForRoutingAddress(SimpleString.toSimpleString("Topic1.test.test2")).getBindings().size());

      assertEquals(1, ad.getDirectBindings(SimpleString.toSimpleString("Topic1.>")).size());
      assertEquals(1, ad.getDirectBindings(SimpleString.toSimpleString("Topic1.test")).size());
      assertEquals(0, ad.getDirectBindings(SimpleString.toSimpleString("Topic1.test1")).size());
      assertEquals(0, ad.getDirectBindings(SimpleString.toSimpleString("Topic1.test2")).size());
      assertEquals(0, ad.getDirectBindings(SimpleString.toSimpleString("Topic2.>")).size());
      assertEquals(1, ad.getDirectBindings(SimpleString.toSimpleString("Topic2.test")).size());

   }


   @Test
   public void testNumberOfBindingsThatMatch() throws Exception {

      final WildcardConfiguration configuration = new WildcardConfiguration();
      configuration.setAnyWords('>');
      WildcardAddressManager ad = new WildcardAddressManager(new BindingFactoryFake(), configuration, null, null);

      ad.addBinding(new BindingFake("T.>", "1"));
      ad.addBinding(new BindingFake("T.>", "2"));
      ad.addBinding(new BindingFake("T.>", "3"));

      assertEquals(3, ad.getBindingsForRoutingAddress(SimpleString.toSimpleString("T.1")).getBindings().size());
      assertEquals(3, ad.getBindingsForRoutingAddress(SimpleString.toSimpleString("T.2")).getBindings().size());
      assertEquals(3, ad.getBindingsForRoutingAddress(SimpleString.toSimpleString("T.3")).getBindings().size());


      assertEquals(3, ad.getExistingBindingsForRoutingAddress(SimpleString.toSimpleString("T.>")).getBindings().size());

      ad.addBinding(new BindingFake("T.*", "10"));
      assertEquals(1, ad.getExistingBindingsForRoutingAddress(SimpleString.toSimpleString("T.*")).getBindings().size());

      // wildcard binding should not be added to existing matching wildcards, still 3
      assertEquals(3, ad.getExistingBindingsForRoutingAddress(SimpleString.toSimpleString("T.>")).getBindings().size());

      assertEquals(4, ad.getBindingsForRoutingAddress(SimpleString.toSimpleString("T.1")).getBindings().size());
      assertEquals(4, ad.getBindingsForRoutingAddress(SimpleString.toSimpleString("T.2")).getBindings().size());
      assertEquals(4, ad.getBindingsForRoutingAddress(SimpleString.toSimpleString("T.3")).getBindings().size());


      ad.addBinding(new BindingFake("T.1.>", "11"));
      assertEquals(1, ad.getExistingBindingsForRoutingAddress(SimpleString.toSimpleString("T.1.>")).getBindings().size());

      assertEquals(5, ad.getBindingsForRoutingAddress(SimpleString.toSimpleString("T.1")).getBindings().size());
      assertEquals(4, ad.getBindingsForRoutingAddress(SimpleString.toSimpleString("T.2")).getBindings().size());
      assertEquals(4, ad.getBindingsForRoutingAddress(SimpleString.toSimpleString("T.3")).getBindings().size());

      ad.addBinding(new BindingFake("T.1.2", "12"));

      assertEquals(5, ad.getBindingsForRoutingAddress(SimpleString.toSimpleString("T.1.2")).getBindings().size());

      ad.addBinding(new BindingFake("T.1.2.3.4", "13"));
      assertEquals(5, ad.getBindingsForRoutingAddress(SimpleString.toSimpleString("T.1.2.3.4")).getBindings().size());

      ad.addBinding(new BindingFake("T.>.4", "14"));

      assertEquals(6, ad.getBindingsForRoutingAddress(SimpleString.toSimpleString("T.1.2.3.4")).getBindings().size());

      ad.addBinding(new BindingFake("T.1.A.3.4", "15"));

      assertEquals(6, ad.getBindingsForRoutingAddress(SimpleString.toSimpleString("T.1.A.3.4")).getBindings().size());

   }

   @Test
   public void testConcurrentCalls() throws Exception {
      final WildcardConfiguration configuration = new WildcardConfiguration();
      configuration.setAnyWords('>');
      final WildcardAddressManager ad = new WildcardAddressManager(new BindingFactoryFake(), configuration, null, null);

      final SimpleString wildCard = SimpleString.toSimpleString("Topic1.>");
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
                  ad.addBinding(new BindingFake(SimpleString.toSimpleString("Topic1.>"), SimpleString.toSimpleString("" + id)));
               }

               SimpleString pubAddr = SimpleString.toSimpleString("Topic1." + id );
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
      assertTrue("finished on time", executorService.awaitTermination(10, TimeUnit.MINUTES));
      assertNull("no exceptions", oops.get());
   }

   static class BindingFactoryFake implements BindingsFactory {

      @Override
      public Bindings createBindings(SimpleString address) {
         return new BindingsFake(address);
      }
   }

   static class BindingFake implements Binding {

      final SimpleString address;
      final SimpleString id;

      BindingFake(String addressParameter, String id) {
         this(SimpleString.toSimpleString(addressParameter), SimpleString.toSimpleString(id));
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
         return Long.valueOf(0);
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
      ConcurrentHashMap<SimpleString, Binding> bindings = new ConcurrentHashMap<>();

      BindingsFake(SimpleString address) {
         this.name = address;
      }

      @Override
      public Collection<Binding> getBindings() {
         return bindings.values();
      }

      @Override
      public void addBinding(Binding binding) {
         bindings.put(binding.getUniqueName(), binding);
      }

      @Override
      public Binding removeBindingByUniqueName(SimpleString uniqueName) {
         return bindings.remove(uniqueName);
      }

      @Override
      public SimpleString getName() {
         return name;
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
