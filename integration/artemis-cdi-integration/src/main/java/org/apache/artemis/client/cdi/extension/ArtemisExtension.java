/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.artemis.client.cdi.extension;

import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.artemis.client.cdi.configuration.ArtemisClientConfiguration;
import org.apache.artemis.client.cdi.logger.ActiveMQCDILogger;

import javax.enterprise.event.Observes;
import javax.enterprise.inject.spi.AfterBeanDiscovery;
import javax.enterprise.inject.spi.Extension;
import javax.enterprise.inject.spi.ProcessAnnotatedType;

public class ArtemisExtension implements Extension {

   private boolean foundEmbeddedConfig = false;
   private boolean foundConfiguration = false;

   <T extends ArtemisClientConfiguration> void foundClientConfig(@Observes ProcessAnnotatedType<T> pat) {
      ActiveMQCDILogger.LOGGER.discoveredConfiguration(pat);
      foundConfiguration = true;
   }

   <T extends Configuration> void foundEmbeddedConfig(@Observes ProcessAnnotatedType<T> pat) {
      ActiveMQCDILogger.LOGGER.discoveredClientConfiguration(pat);
      foundEmbeddedConfig = true;
   }

   void afterBeanDiscovery(@Observes AfterBeanDiscovery afterBeanDiscovery) {
      if (!foundConfiguration) {
         afterBeanDiscovery.addBean(new ArtemisClientConfigBean());
      }
      else {
         ActiveMQCDILogger.LOGGER.notUsingDefaultConfiguration();
      }
      if (!foundEmbeddedConfig) {
         afterBeanDiscovery.addBean(new ArtemisEmbeddedServerConfigBean());
      }
      else {
         ActiveMQCDILogger.LOGGER.notUsingDefaultClientConfiguration();
      }

   }

}
