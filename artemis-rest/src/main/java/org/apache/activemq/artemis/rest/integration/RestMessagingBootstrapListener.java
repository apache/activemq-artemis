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
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.apache.activemq.artemis.jms.client.ConnectionFactoryOptions;
import org.apache.activemq.artemis.rest.MessageServiceManager;
import org.apache.activemq.artemis.utils.ObjectInputStreamWithClassLoader;
import org.jboss.resteasy.spi.Registry;

public class RestMessagingBootstrapListener implements ServletContextListener, ConnectionFactoryOptions {

   MessageServiceManager manager;
   private String deserializationBlackList;
   private String deserializationWhiteList;

   @Override
   public void contextInitialized(ServletContextEvent contextEvent) {
      ServletContext context = contextEvent.getServletContext();
      Registry registry = (Registry) context.getAttribute(Registry.class.getName());
      if (registry == null) {
         throw new RuntimeException("You must install RESTEasy as a Bootstrap Listener and it must be listed before this class");
      }
      String configfile = context.getInitParameter("rest.messaging.config.file");
      deserializationBlackList = context.getInitParameter(ObjectInputStreamWithClassLoader.BLACKLIST_PROPERTY);
      deserializationWhiteList = context.getInitParameter(ObjectInputStreamWithClassLoader.WHITELIST_PROPERTY);
      manager = new MessageServiceManager(this);

      if (configfile != null) {
         manager.setConfigResourcePath(configfile);
      }
      try {
         manager.start();
         registry.addSingletonResource(manager.getQueueManager().getDestination());
         registry.addSingletonResource(manager.getTopicManager().getDestination());
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   @Override
   public void contextDestroyed(ServletContextEvent servletContextEvent) {
      if (manager != null) {
         manager.stop();
      }
   }

   @Override
   public String getDeserializationBlackList() {
      return deserializationBlackList;
   }

   @Override
   public void setDeserializationBlackList(String blackList) {
      deserializationBlackList = blackList;
   }

   @Override
   public String getDeserializationWhiteList() {
      return deserializationWhiteList;
   }

   @Override
   public void setDeserializationWhiteList(String whiteList) {
      deserializationWhiteList = whiteList;
   }
}
