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

package org.apache.activemq.service.extensions.tests.xa;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.service.extensions.ServiceUtils;
import org.apache.activemq.service.extensions.xa.ActiveMQXAResourceWrapperFactory;
import org.apache.activemq.service.extensions.xa.ActiveMQXAResourceWrapperFactoryImpl;
import org.junit.Test;

import static org.jgroups.util.Util.assertTrue;

/**
 * @author <a href="mailto:mtaylor@redhat.com">Martyn Taylor</a>
 */

public class ServiceUtilsTest
{
   @Test
   public void testSetActiveMQXAResourceWrapperFactorySetsDefaultImplWhenNoOther() throws Exception
   {
      List<ActiveMQXAResourceWrapperFactory> factories = new ArrayList<ActiveMQXAResourceWrapperFactory>();

      Method method = ServiceUtils.class.getDeclaredMethod("setActiveMQXAResourceWrapperFactory", Iterable.class);
      method.setAccessible(true);
      method.invoke(null, factories);

      Field field = ServiceUtils.class.getDeclaredField("activeMQXAResourceWrapperFactory");
      field.setAccessible(true);
      assertTrue(field.get(null) instanceof ActiveMQXAResourceWrapperFactoryImpl);
   }

   @Test
   public void testSetActiveMQXAResourceWrapperFactorySetsExtensionImplWhenSupplied() throws Exception
   {
      List<ActiveMQXAResourceWrapperFactory> factories = new ArrayList<ActiveMQXAResourceWrapperFactory>();
      factories.add(new MockActiveMQResourceWrapperFactory());

      Method method = ServiceUtils.class.getDeclaredMethod("setActiveMQXAResourceWrapperFactory", Iterable.class);
      method.setAccessible(true);
      method.invoke(null, factories);

      Field field = ServiceUtils.class.getDeclaredField("activeMQXAResourceWrapperFactory");
      field.setAccessible(true);
      assertTrue(field.get(null) instanceof MockActiveMQResourceWrapperFactory);
   }
}
