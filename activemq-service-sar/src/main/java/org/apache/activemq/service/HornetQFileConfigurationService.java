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
package org.apache.activemq.service;

import org.apache.activemq.core.config.Configuration;
import org.apache.activemq.core.config.impl.FileConfiguration;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         Created Oct 21, 2009
 */
public class HornetQFileConfigurationService implements HornetQFileConfigurationServiceMBean
{
   private FileConfiguration configuration;

   public void create() throws Exception
   {
      configuration = new FileConfiguration();
   }

   public void start() throws Exception
   {
      configuration.start();
   }

   public void stop() throws Exception
   {
      configuration.stop();
   }

   public void setConfigurationUrl(final String configurationUrl)
   {
      configuration.setConfigurationUrl(configurationUrl);
   }

   public Configuration getConfiguration()
   {
      return configuration;
   }
}
