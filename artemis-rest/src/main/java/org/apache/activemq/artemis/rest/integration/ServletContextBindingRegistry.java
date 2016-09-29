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
package org.apache.activemq.artemis.rest.integration;

import javax.servlet.ServletContext;

import org.apache.activemq.artemis.spi.core.naming.BindingRegistry;

public class ServletContextBindingRegistry implements BindingRegistry {

   private ServletContext servletContext;

   public ServletContextBindingRegistry(ServletContext servletContext) {
      this.servletContext = servletContext;
   }

   @Override
   public Object lookup(String name) {
      return servletContext.getAttribute(name);
   }

   @Override
   public boolean bind(String name, Object obj) {
      servletContext.setAttribute(name, obj);
      return true;
   }

   @Override
   public void unbind(String name) {
      servletContext.removeAttribute(name);
   }

   @Override
   public void close() {
   }
}
