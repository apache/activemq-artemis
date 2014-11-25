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

package org.apache.activemq.service.extensions;

import javax.transaction.xa.XAResource;
import java.util.Map;
import java.util.ServiceLoader;

import org.apache.activemq.service.extensions.xa.ActiveMQXAResourceWrapper;
import org.apache.activemq.service.extensions.xa.ActiveMQXAResourceWrapperFactory;
import org.apache.activemq.service.extensions.xa.ActiveMQXAResourceWrapperFactoryImpl;

/**
 * @author <a href="mailto:mtaylor@redhat.com">Martyn Taylor</a>
 */

public class ServiceUtils
{
   private static ActiveMQXAResourceWrapperFactory activeMQXAResourceWrapperFactory;

   private static ActiveMQXAResourceWrapperFactory getActiveMQXAResourceWrapperFactory()
   {
      if (activeMQXAResourceWrapperFactory == null)
      {
         setActiveMQXAResourceWrapperFactory(ServiceLoader.load(ActiveMQXAResourceWrapperFactory.class));
      }
      return activeMQXAResourceWrapperFactory;
   }

   public static ActiveMQXAResourceWrapper wrapXAResource(XAResource xaResource, Map<String, Object> properties)
   {
      return getActiveMQXAResourceWrapperFactory().wrap(xaResource, properties);
   }

   private static void setActiveMQXAResourceWrapperFactory(Iterable<ActiveMQXAResourceWrapperFactory> iterable)
   {
      if (iterable.iterator().hasNext())
      {
         activeMQXAResourceWrapperFactory = iterable.iterator().next();
      }
      else
      {
         activeMQXAResourceWrapperFactory = new ActiveMQXAResourceWrapperFactoryImpl();
      }
   }

}
