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

package org.apache.activemq.tests.unit.core.remoting.server.impl;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.activemq.api.core.Interceptor;
import org.apache.activemq.api.core.TransportConfiguration;
import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.core.config.impl.ConfigurationImpl;
import org.apache.activemq.core.remoting.server.impl.RemotingServiceImpl;
import org.apache.activemq.core.server.impl.ServiceRegistry;
import org.apache.activemq.tests.unit.core.remoting.server.impl.fake.FakeInterceptor;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * @author <a href="mailto:mtaylor@redhat.com">Martyn Taylor</a>
 */

public class RemotingServiceImplTest
{
   private ServiceRegistry serviceRegistry;

   private RemotingServiceImpl remotingService;

   private Configuration configuration;

   @Before
   public void setUp() throws Exception
   {
      serviceRegistry = new ServiceRegistry();
      configuration = new ConfigurationImpl();
      configuration.setAcceptorConfigurations(new HashSet<TransportConfiguration>());
      remotingService = new RemotingServiceImpl(null, configuration, null, null, null, null, null, serviceRegistry);
   }

   /**
    * Tests that the method addReflectivelyInstantiatedInterceptors creates new instances of interceptors and adds
    * them to the provided list.
    */
   @Test
   public void testAddReflectivelyInstantiatedInterceptorsAddsNewInstancesToList() throws Exception
   {
      Method method = RemotingServiceImpl.class.getDeclaredMethod("addReflectivelyInstantiatedInterceptors",
                                                                  List.class,
                                                                  List.class);
      method.setAccessible(true);
      List<String> interceptorClassNames = new ArrayList<String>();
      for (int i = 0; i < 5; i++)
      {
         interceptorClassNames.add(FakeInterceptor.class.getCanonicalName());
      }
      List<Interceptor> interceptors = new ArrayList<Interceptor>();
      method.invoke(remotingService, interceptorClassNames, interceptors);

      assertTrue(interceptors.size() == 5);
      assertTrue(interceptors.get(0) instanceof FakeInterceptor);
      assertTrue(interceptors.get(0) != interceptors.get(1));
   }

   /**
    * Tests ensures that setInterceptors methods adds both interceptors from the service registry and also interceptors
    * defined in the configuration.
    */
   @Test
   public void testSetInterceptorsAddsBothInterceptorsFromConfigAndServiceRegistry() throws Exception
   {
      Method method = RemotingServiceImpl.class.getDeclaredMethod("setInterceptors",
                                                                  Configuration.class);
      Field incomingInterceptors = RemotingServiceImpl.class.getDeclaredField("incomingInterceptors");
      Field outgoingInterceptors = RemotingServiceImpl.class.getDeclaredField("outgoingInterceptors");

      method.setAccessible(true);
      incomingInterceptors.setAccessible(true);
      outgoingInterceptors.setAccessible(true);

      serviceRegistry.addIncomingInterceptor("Foo", new FakeInterceptor());
      serviceRegistry.addOutgoingInterceptor("Bar", new FakeInterceptor());

      List<String> interceptorClassNames = new ArrayList<String>();
      interceptorClassNames.add(FakeInterceptor.class.getCanonicalName());
      configuration.setIncomingInterceptorClassNames(interceptorClassNames);
      configuration.setOutgoingInterceptorClassNames(interceptorClassNames);

      method.invoke(remotingService, configuration);

      assertTrue(((List) incomingInterceptors.get(remotingService)).size() == 2 );
      assertTrue(((List) outgoingInterceptors.get(remotingService)).size() == 2 );
      assertTrue(((List) incomingInterceptors.get(remotingService)).contains(serviceRegistry.getIncomingInterceptor("Foo")));
      assertTrue(((List) outgoingInterceptors.get(remotingService)).contains(serviceRegistry.getOutgoingInterceptor("Bar")));
   }

   /**
    * Tests ensures that both interceptors from the service registry and also interceptors defined in the configuration
    * are added to the RemotingServiceImpl on creation
    */
   @Test
   public void testInterceptorsAreAddedOnCreationOfServiceRegistry() throws Exception
   {
      Method method = RemotingServiceImpl.class.getDeclaredMethod("setInterceptors",
                                                                  Configuration.class);
      Field incomingInterceptors = RemotingServiceImpl.class.getDeclaredField("incomingInterceptors");
      Field outgoingInterceptors = RemotingServiceImpl.class.getDeclaredField("outgoingInterceptors");

      method.setAccessible(true);
      incomingInterceptors.setAccessible(true);
      outgoingInterceptors.setAccessible(true);

      serviceRegistry.addIncomingInterceptor("Foo", new FakeInterceptor());
      serviceRegistry.addOutgoingInterceptor("Bar", new FakeInterceptor());

      List<String> interceptorClassNames = new ArrayList<String>();
      interceptorClassNames.add(FakeInterceptor.class.getCanonicalName());
      configuration.setIncomingInterceptorClassNames(interceptorClassNames);
      configuration.setOutgoingInterceptorClassNames(interceptorClassNames);

      remotingService = new RemotingServiceImpl(null, configuration, null, null, null, null, null, serviceRegistry);

      assertTrue(((List) incomingInterceptors.get(remotingService)).size() == 2 );
      assertTrue(((List) outgoingInterceptors.get(remotingService)).size() == 2 );
      assertTrue(((List) incomingInterceptors.get(remotingService)).contains(serviceRegistry.getIncomingInterceptor("Foo")));
      assertTrue(((List) outgoingInterceptors.get(remotingService)).contains(serviceRegistry.getOutgoingInterceptor("Bar")));
   }
}
