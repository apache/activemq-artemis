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

import java.util.ArrayList;
import java.util.Collection;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.BindingType;
import org.apache.activemq.artemis.core.postoffice.Bindings;
import org.apache.activemq.artemis.core.postoffice.BindingsFactory;
import org.apache.activemq.artemis.core.postoffice.impl.WildcardAddressManager;
import org.apache.activemq.artemis.core.server.Bindable;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.RoutingContext;
import org.apache.activemq.artemis.core.server.ServerMessage;
import org.junit.Test;


/**
 * This test is replicating the behaviour from https://issues.jboss.org/browse/HORNETQ-988.
 */
public class WildcardAddressManagerUnitTest extends ActiveMQTestBase
{


   @Test
   public void testUnitOnWildCardFailingScenario() throws Exception
   {
      int errors = 0;
      WildcardAddressManager ad = new WildcardAddressManager(new BindingFactoryFake());
      ad.addBinding(new BindingFake("jms.topic.Topic1", "jms.topic.Topic1"));
      ad.addBinding(new BindingFake("jms.topic.Topic1", "one"));
      ad.addBinding(new BindingFake("jms.topic.*", "two"));
      ad.removeBinding(SimpleString.toSimpleString("one"), null);
      try
      {
         ad.removeBinding(SimpleString.toSimpleString("two"), null);
      }
      catch (Throwable e)
      {
         // We are not failing the test here as this test is replicating the exact scenario
         // that was happening under https://issues.jboss.org/browse/HORNETQ-988
         // In which this would be ignored
         errors++;
         e.printStackTrace();
      }
      try
      {
         ad.addBinding(new BindingFake("jms.topic.Topic1", "three"));
      }
      catch (Throwable e)
      {
         // We are not failing the test here as this test is replicating the exact scenario
         // that was happening under https://issues.jboss.org/browse/HORNETQ-988
         // In which this would be ignored
         errors++;
         e.printStackTrace();
      }

      assertEquals("Exception happened during the process", 0, errors);
   }

   class BindingFactoryFake implements BindingsFactory
   {
      public Bindings createBindings(SimpleString address) throws Exception
      {
         return new BindignsFake();
      }
   }

   class BindingFake implements Binding
   {

      final SimpleString address;
      final SimpleString id;

      public BindingFake(String addressParameter, String id)
      {
         this(SimpleString.toSimpleString(addressParameter), SimpleString.toSimpleString(id));
      }

      public BindingFake(SimpleString addressParameter, SimpleString id)
      {
         this.address = addressParameter;
         this.id = id;
      }


      @Override
      public void unproposed(SimpleString groupID)
      {

      }

      @Override
      public SimpleString getAddress()
      {
         return address;
      }

      @Override
      public Bindable getBindable()
      {
         return null;
      }

      @Override
      public BindingType getType()
      {
         return null;
      }

      @Override
      public SimpleString getUniqueName()
      {
         return id;
      }

      @Override
      public SimpleString getRoutingName()
      {
         return null;
      }

      @Override
      public SimpleString getClusterName()
      {
         return null;
      }

      @Override
      public Filter getFilter()
      {
         return null;
      }

      @Override
      public boolean isHighAcceptPriority(ServerMessage message)
      {
         return false;
      }

      @Override
      public boolean isExclusive()
      {
         return false;
      }

      @Override
      public long getID()
      {
         return 0;
      }

      @Override
      public int getDistance()
      {
         return 0;
      }

      @Override
      public void route(ServerMessage message, RoutingContext context) throws Exception
      {
      }

      @Override
      public void close() throws Exception
      {
      }

      @Override
      public String toManagementString()
      {
         return "FakeBiding Address=" + this.address;
      }

      @Override
      public boolean isConnected()
      {
         return true;
      }

      @Override
      public void routeWithAck(ServerMessage message, RoutingContext context)
      {

      }
   }


   class BindignsFake implements Bindings
   {
      ArrayList<Binding> bindings = new ArrayList<Binding>();


      @Override
      public Collection<Binding> getBindings()
      {
         return bindings;
      }

      @Override
      public void addBinding(Binding binding)
      {
         bindings.add(binding);
      }

      @Override
      public void removeBinding(Binding binding)
      {
         bindings.remove(binding);
      }

      @Override
      public void setRouteWhenNoConsumers(boolean takePriorityIntoAccount)
      {

      }

      @Override
      public void unproposed(SimpleString groupID)
      {
      }

      @Override
      public boolean redistribute(ServerMessage message, Queue originatingQueue, RoutingContext context) throws Exception
      {
         return false;
      }

      @Override
      public void route(ServerMessage message, RoutingContext context) throws Exception
      {
         System.out.println("routing message: " + message);
      }
   }


}
