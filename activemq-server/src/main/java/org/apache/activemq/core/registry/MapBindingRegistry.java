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
package org.apache.activemq.core.registry;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.activemq.spi.core.naming.BindingRegistry;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class MapBindingRegistry implements BindingRegistry
{
   protected ConcurrentMap<String, Object> registry = new ConcurrentHashMap<String, Object>();

   public Object lookup(String name)
   {
      return registry.get(name);
   }

   public boolean bind(String name, Object obj)
   {
      return registry.putIfAbsent(name, obj) == null;
   }

   public void unbind(String name)
   {
      registry.remove(name);
   }

   public void close()
   {
   }

   public Object getContext()
   {
      return registry;
   }

   @Override
   public void setContext(Object ctx)
   {
      registry = (ConcurrentMap)ctx;
   }
}
