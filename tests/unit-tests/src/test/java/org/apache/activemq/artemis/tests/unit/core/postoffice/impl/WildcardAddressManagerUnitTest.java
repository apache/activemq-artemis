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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.WildcardConfiguration;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.postoffice.Address;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.BindingType;
import org.apache.activemq.artemis.core.postoffice.Bindings;
import org.apache.activemq.artemis.core.postoffice.BindingsFactory;
import org.apache.activemq.artemis.core.postoffice.impl.WildcardAddressManager;
import org.apache.activemq.artemis.core.server.Bindable;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.RoutingContext;
import org.apache.activemq.artemis.core.server.cluster.impl.MessageLoadBalancingType;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Test;

/**
 * This test is replicating the behaviour from https://issues.jboss.org/browse/HORNETQ-988.
 */
public class WildcardAddressManagerUnitTest extends ActiveMQTestBase {

   @Test
   public void testUnitOnWildCardFailingScenario() throws Exception {
      int errors = 0;
      WildcardAddressManager ad = new WildcardAddressManager(new BindingFactoryFake(), null);
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
      WildcardAddressManager ad = new WildcardAddressManager(new BindingFactoryFake(), null);
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
    * @throws Exception
    */
   @SuppressWarnings("unchecked")
   @Test
   public void testWildCardAddressRemoval() throws Exception {

      WildcardAddressManager ad = new WildcardAddressManager(new BindingFactoryFake(), null);
      ad.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("Queue1.#"), RoutingType.ANYCAST));
      ad.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("Topic1.#"), RoutingType.MULTICAST));
      ad.addBinding(new BindingFake("Topic1.topic", "two"));
      ad.addBinding(new BindingFake("Queue1.#", "one"));

      Field wildcardAddressField = WildcardAddressManager.class.getDeclaredField("wildCardAddresses");
      wildcardAddressField.setAccessible(true);
      Map<SimpleString, Address> wildcardAddresses = (Map<SimpleString, Address>)wildcardAddressField.get(ad);

      //Calling this method will trigger the wildcard to be added to the wildcard map internal
      //to WildcardAddressManager
      ad.getBindingsForRoutingAddress(SimpleString.toSimpleString("Topic1.#"));

      //Remove the address
      ad.removeAddressInfo(SimpleString.toSimpleString("Topic1.#"));

      //Verify the address was cleaned up properly
      assertEquals(1, wildcardAddresses.size());
      assertNull(ad.getAddressInfo(SimpleString.toSimpleString("Topic1.#")));
      assertNull(wildcardAddresses.get(SimpleString.toSimpleString("Topic1.#")));
   }

   @Test
   public void testWildCardAddressRemovalDifferentWildcard() throws Exception {

      final WildcardConfiguration configuration = new WildcardConfiguration();
      configuration.setAnyWords('>');
      WildcardAddressManager ad = new WildcardAddressManager(new BindingFactoryFake(), configuration, null);
      ad.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("Topic1.>"), RoutingType.MULTICAST));
      ad.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("Topic1.test"), RoutingType.MULTICAST));
      ad.addBinding(new BindingFake("Topic1.>", "one"));

      assertEquals(1, ad.getBindingsForRoutingAddress(SimpleString.toSimpleString("Topic1.>")).getBindings().size());
      assertEquals(1, ad.getBindingsForRoutingAddress(SimpleString.toSimpleString("Topic1.test")).getBindings().size());
      assertEquals(0, ad.getDirectBindings(SimpleString.toSimpleString("Topic1.test")).getBindings().size());
      assertEquals(1, ad.getDirectBindings(SimpleString.toSimpleString("Topic1.>")).getBindings().size());

      //Remove the address
      ad.removeAddressInfo(SimpleString.toSimpleString("Topic1.test"));

      //should still have 1 address and binding
      assertEquals(1, ad.getAddresses().size());
      assertEquals(1, ad.getBindings().size());

      ad.removeBinding(SimpleString.toSimpleString("one"), null);
      ad.removeAddressInfo(SimpleString.toSimpleString("Topic1.>"));

      assertEquals(0, ad.getAddresses().size());
      assertEquals(0, ad.getBindings().size());
   }

   @Test
   public void testWildCardAddressDirectBindings() throws Exception {

      final WildcardConfiguration configuration = new WildcardConfiguration();
      configuration.setAnyWords('>');
      WildcardAddressManager ad = new WildcardAddressManager(new BindingFactoryFake(), configuration, null);
      ad.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("Topic1.>"), RoutingType.MULTICAST));
      ad.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("Topic1.test"), RoutingType.MULTICAST));
      ad.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("Topic1.test.test1"), RoutingType.MULTICAST));
      ad.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("Topic1.test.test2"), RoutingType.MULTICAST));
      ad.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("Topic2.>"), RoutingType.MULTICAST));
      ad.addAddressInfo(new AddressInfo(SimpleString.toSimpleString("Topic2.test"), RoutingType.MULTICAST));
      ad.addBinding(new BindingFake("Topic1.>", "one"));
      ad.addBinding(new BindingFake("Topic1.test", "two"));
      ad.addBinding(new BindingFake("Topic2.test", "three"));

      assertEquals(1, ad.getBindingsForRoutingAddress(SimpleString.toSimpleString("Topic1.>")).getBindings().size());
      assertEquals(2, ad.getBindingsForRoutingAddress(SimpleString.toSimpleString("Topic1.test")).getBindings().size());
      assertEquals(1, ad.getBindingsForRoutingAddress(SimpleString.toSimpleString("Topic1.test.test1")).getBindings().size());
      assertEquals(1, ad.getBindingsForRoutingAddress(SimpleString.toSimpleString("Topic1.test.test2")).getBindings().size());

      assertEquals(1, ad.getDirectBindings(SimpleString.toSimpleString("Topic1.>")).getBindings().size());
      assertEquals(1, ad.getDirectBindings(SimpleString.toSimpleString("Topic1.test")).getBindings().size());
      assertEquals(0, ad.getDirectBindings(SimpleString.toSimpleString("Topic1.test1")).getBindings().size());
      assertEquals(0, ad.getDirectBindings(SimpleString.toSimpleString("Topic1.test2")).getBindings().size());
      assertEquals(0, ad.getDirectBindings(SimpleString.toSimpleString("Topic2.>")).getBindings().size());
      assertEquals(1, ad.getDirectBindings(SimpleString.toSimpleString("Topic2.test")).getBindings().size());

   }

   class BindingFactoryFake implements BindingsFactory {

      @Override
      public Bindings createBindings(SimpleString address) throws Exception {
         return new BindignsFake();
      }
   }

   class BindingFake implements Binding {

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
      public long getID() {
         return 0;
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

   class BindignsFake implements Bindings {

      ArrayList<Binding> bindings = new ArrayList<>();

      @Override
      public Collection<Binding> getBindings() {
         return bindings;
      }

      @Override
      public void addBinding(Binding binding) {
         bindings.add(binding);
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
      public boolean redistribute(Message message,
                                  Queue originatingQueue,
                                  RoutingContext context) throws Exception {
         return false;
      }

      @Override
      public void route(Message message, RoutingContext context) throws Exception {
         System.out.println("routing message: " + message);
      }
   }

}
