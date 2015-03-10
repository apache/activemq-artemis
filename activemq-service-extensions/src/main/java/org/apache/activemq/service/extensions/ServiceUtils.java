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
package org.apache.activemq.service.extensions;

import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

import org.apache.activemq.service.extensions.transactions.TransactionManagerLocator;
import org.apache.activemq.service.extensions.xa.ActiveMQXAResourceWrapper;
import org.apache.activemq.service.extensions.xa.ActiveMQXAResourceWrapperFactory;
import org.apache.activemq.service.extensions.xa.ActiveMQXAResourceWrapperFactoryImpl;

public class ServiceUtils
{
   private static ActiveMQXAResourceWrapperFactory activeMQXAResourceWrapperFactory;

   private static TransactionManager transactionManager;

   private static boolean transactionManagerLoaded = false;

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

   public static synchronized TransactionManager getTransactionManager()
   {
      if (!transactionManagerLoaded)
      {
         Iterator<TransactionManagerLocator> it = ServiceLoader.load(TransactionManagerLocator.class).iterator();
         while (it.hasNext() && transactionManager == null)
         {
            transactionManager = it.next().getTransactionManager();
         }

         if (transactionManager != null)
         {
            transactionManagerLoaded = true;
         }
         else
         {
            ActiveMQServiceExtensionLogger.LOGGER.transactionManagerNotFound();
         }
      }
      return transactionManager;
   }

   public static void setTransactionManager(TransactionManager tm)
   {
      transactionManager = tm;
      transactionManagerLoaded = (transactionManager != null);
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
