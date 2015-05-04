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

package org.apache.activemq.artemis.cli.commands;

import java.io.File;

import io.airlift.airline.Arguments;
import io.airlift.airline.Option;
import org.apache.activemq.artemis.core.config.FileDeploymentManager;
import org.apache.activemq.artemis.core.config.impl.FileConfiguration;
import org.apache.activemq.artemis.dto.BrokerDTO;
import org.apache.activemq.artemis.factory.BrokerFactory;
import org.apache.activemq.artemis.jms.server.config.impl.FileJMSConfiguration;

/**
 * Abstract class where we can replace the configuration in various places *
 */
public abstract class Configurable
{
   @Arguments(description = "Broker Configuration URI, default 'xml:${ARTEMIS_INSTANCE}/etc/bootstrap.xml'")
   String configuration;

   @Option(name = "--broker", description = "This would override the broker configuration from the bootstrap")
   String brokerConfig;

   private BrokerDTO brokerDTO = null;

   private String brokerInstance;

   private FileConfiguration fileConfiguration;

   protected String getBrokerInstance()
   {
      if (brokerInstance == null)
      {
         /* We use File URI for locating files.  The ARTEMIS_HOME variable is used to determine file paths.  For Windows
         the ARTEMIS_HOME variable will include back slashes (An invalid file URI character path separator).  For this
         reason we overwrite the ARTEMIS_HOME variable with backslashes replaced with forward slashes. */
         brokerInstance = System.getProperty("artemis.instance");
         if (brokerInstance != null)
         {
            brokerInstance = brokerInstance.replace("\\", "/");
            System.setProperty("artemis.instance", brokerInstance);
         }
      }
      return brokerInstance;
   }


   protected FileConfiguration getFileConfiguration() throws Exception
   {
      if (fileConfiguration == null)
      {
         if (getBrokerInstance() == null)
         {
            fileConfiguration = new FileConfiguration();
            // These will be the default places in case the file can't be loaded
            fileConfiguration.setBindingsDirectory("../data/bindings");
            fileConfiguration.setJournalDirectory("../data/journal");
            fileConfiguration.setLargeMessagesDirectory("../data/largemessages");
            fileConfiguration.setPagingDirectory("../data/paging");
         }
         else
         {
            fileConfiguration = new FileConfiguration();
            FileJMSConfiguration jmsConfiguration = new FileJMSConfiguration();

            String serverConfiguration = getBrokerDTO().server.configuration;
            FileDeploymentManager fileDeploymentManager = new FileDeploymentManager(serverConfiguration);
            fileDeploymentManager.addDeployable(fileConfiguration).addDeployable(jmsConfiguration);
            fileDeploymentManager.readConfiguration();
         }
      }

      return fileConfiguration;
   }


   protected BrokerDTO getBrokerDTO() throws Exception
   {
      if (brokerDTO == null)
      {
         getConfiguration();


         brokerDTO = BrokerFactory.createBrokerConfiguration(configuration);

         if (brokerConfig != null)
         {
            if (!brokerConfig.startsWith("file:"))
            {
               brokerConfig = "file:" + brokerConfig;
            }

            brokerDTO.server.configuration = brokerConfig;
         }
      }

      return brokerDTO;
   }

   protected String getConfiguration()
   {
      if (configuration == null)
      {
         File xmlFile = new File(new File(new File(getBrokerInstance()), "etc"), "bootstrap.xml");
         configuration = "xml:" + xmlFile.toURI().toString().substring("file:".length());

         // To support Windows paths as explained above.
         configuration = configuration.replace("\\", "/");

         System.out.println("Loading configuration file: " + configuration);
      }

      return configuration;
   }


}
