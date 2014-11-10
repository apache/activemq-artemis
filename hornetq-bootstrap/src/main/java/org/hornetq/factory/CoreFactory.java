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
package org.hornetq.factory;

import org.hornetq.cli.ConfigurationException;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.dto.CoreDTO;
import org.hornetq.utils.FactoryFinder;

import java.io.IOException;
import java.net.URI;

public class CoreFactory
{
   public static Configuration create(CoreDTO core) throws Exception
   {
      if (core.configuration != null)
      {
         CoreFactoryHandler factory = null;
         URI configURI = new URI(core.configuration.replace("\\", "/"));
         try
         {
            FactoryFinder finder = new FactoryFinder("META-INF/services/org/hornetq/broker/core/");
            factory = (CoreFactoryHandler)finder.newInstance(configURI.getScheme());
         }
         catch (IOException ioe )
         {
            throw new ConfigurationException("Invalid configuration URI, can't find configuration scheme: " + configURI.getScheme());
         }

         return factory.createConfiguration(configURI);
      }
      return new ConfigurationImpl();
   }

}
