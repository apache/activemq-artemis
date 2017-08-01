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
package org.apache.activemq.hawtio.branding;

import io.hawt.web.plugin.HawtioPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

/**
 * The Plugin Context Listener used to load in the plugin
 **/
public class PluginContextListener implements ServletContextListener {

   private static final Logger LOG = LoggerFactory.getLogger(PluginContextListener.class);

   HawtioPlugin plugin = null;

   @Override
   public void contextInitialized(ServletContextEvent servletContextEvent) {

      ServletContext context = servletContextEvent.getServletContext();

      plugin = new HawtioPlugin();
      plugin.setContext((String)context.getInitParameter("plugin-context"));
      plugin.setName(context.getInitParameter("plugin-name"));
      plugin.setScripts(context.getInitParameter("plugin-scripts"));
      plugin.setDomain(null);

      try {
         plugin.init();
      } catch (Exception e) {
         throw createServletException(e);
      }

      LOG.info("Initialized {} plugin", plugin.getName());
   }

   @Override
   public void contextDestroyed(ServletContextEvent servletContextEvent) {
      try {
         plugin.destroy();
      } catch (Exception e) {
         throw createServletException(e);
      }

      LOG.info("Destroyed {} plugin", plugin.getName());
   }

   protected RuntimeException createServletException(Exception e) {
      return new RuntimeException(e);
   }

}
