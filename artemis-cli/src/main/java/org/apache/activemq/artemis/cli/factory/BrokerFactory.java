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
package org.apache.activemq.artemis.cli.factory;

import java.io.IOException;
import java.net.URI;

import org.apache.activemq.artemis.cli.ConfigurationException;
import org.apache.activemq.artemis.dto.BrokerDTO;
import org.apache.activemq.artemis.dto.ServerDTO;
import org.apache.activemq.artemis.integration.Broker;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager;
import org.apache.activemq.artemis.utils.FactoryFinder;

public class BrokerFactory {

   private static BrokerDTO createBrokerConfiguration(URI configURI,
                                                      String artemisHome,
                                                      String artemisInstance,
                                                      URI artemisURIInstance) throws Exception {
      if (configURI.getScheme() == null) {
         throw new ConfigurationException("Invalid configuration URI, no scheme specified: " + configURI);
      }

      BrokerFactoryHandler factory = null;
      try {
         FactoryFinder finder = new FactoryFinder("META-INF/services/org/apache/activemq/artemis/broker/");
         factory = (BrokerFactoryHandler) finder.newInstance(configURI.getScheme());
      } catch (IOException ioe) {
         throw new ConfigurationException("Invalid configuration URI, can't find configuration scheme: " + configURI.getScheme());
      }
      return factory.createBroker(configURI, artemisHome, artemisInstance, artemisURIInstance);
   }

   public static BrokerDTO createBrokerConfiguration(String configuration,
                                                     String artemisHome,
                                                     String artemisInstance,
                                                     URI artemisURIInstance) throws Exception {
      return createBrokerConfiguration(new URI(configuration), artemisHome, artemisInstance, artemisURIInstance);
   }

   public static Broker createServer(ServerDTO brokerDTO, ActiveMQSecurityManager security) throws Exception {
      if (brokerDTO.configuration != null) {
         BrokerHandler handler;
         URI configURI = brokerDTO.getConfigurationURI();

         try {
            FactoryFinder finder = new FactoryFinder("META-INF/services/org/apache/activemq/artemis/broker/server/");
            handler = (BrokerHandler) finder.newInstance(configURI.getScheme());
         } catch (IOException ioe) {
            throw new ConfigurationException("Invalid configuration URI, can't find configuration scheme: " + configURI.getScheme());
         }

         return handler.createServer(brokerDTO, security);
      }
      return null;
   }

}
