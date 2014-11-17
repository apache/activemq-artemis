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
package org.apache.activemq6.integration.spring;

import org.apache.activemq6.spi.core.naming.BindingRegistry;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class SpringBindingRegistry implements BindingRegistry
{
   private ConfigurableBeanFactory factory;

   public SpringBindingRegistry(ConfigurableBeanFactory factory)
   {
      this.factory = factory;
   }

   public Object lookup(String name)
   {
      Object obj = null;
      try
      {
         obj = factory.getBean(name);
      }
      catch (NoSuchBeanDefinitionException e)
      {
         //ignore
      }
      return obj;
   }

   public boolean bind(String name, Object obj)
   {
      factory.registerSingleton(name, obj);
      return true;
   }

   public void unbind(String name)
   {
   }

   public void close()
   {
   }

   @Override
   public Object getContext()
   {
      return this.factory;
   }

   @Override
   public void setContext(Object ctx)
   {
      this.factory = (ConfigurableBeanFactory) ctx;
   }
}
