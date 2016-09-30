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
package org.apache.activemq.artemis.integration.spring;

import org.apache.activemq.artemis.spi.core.naming.BindingRegistry;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;

public class SpringBindingRegistry implements BindingRegistry {

   private ConfigurableBeanFactory factory;

   public SpringBindingRegistry(ConfigurableBeanFactory factory) {
      this.factory = factory;
   }

   @Override
   public Object lookup(String name) {
      Object obj = null;
      try {
         obj = factory.getBean(name);
      } catch (NoSuchBeanDefinitionException e) {
         //ignore
      }
      return obj;
   }

   @Override
   public boolean bind(String name, Object obj) {
      factory.registerSingleton(name, obj);
      return true;
   }

   @Override
   public void unbind(String name) {
   }

   @Override
   public void close() {
   }
}
