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
package org.apache.activemq6.core.config.impl;

import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;

import org.apache.activemq6.core.deployers.impl.FileConfigurationParser;
import org.apache.activemq6.core.server.HornetQServerLogger;
import org.apache.activemq6.utils.XMLUtil;
import org.w3c.dom.Element;

/**
 * A {@code FileConfiguration} reads configuration values from a file.
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 */
public final class FileConfiguration extends ConfigurationImpl
{
   private static final long serialVersionUID = -4766689627675039596L;
   // Constants ------------------------------------------------------------------------

   private static final String DEFAULT_CONFIGURATION_URL = "hornetq-configuration.xml";

   // For a bridge confirmations must be activated or send acknowledgments won't return
   public static final int DEFAULT_CONFIRMATION_WINDOW_SIZE = 1024 * 1024;

   public FileConfiguration()
   {
      configurationUrl = DEFAULT_CONFIGURATION_URL;
   }

   public FileConfiguration(String configurationUrl)
   {
      this.configurationUrl = configurationUrl;
   }

   private String configurationUrl = DEFAULT_CONFIGURATION_URL;

   private boolean started;

   public synchronized void start() throws Exception
   {
      if (started)
      {
         return;
      }


      URL url = getClass().getClassLoader().getResource(configurationUrl);

      if (url == null)
      {
         // The URL is outside of the classloader. Trying a pure url now
         url = new URL(configurationUrl);
      }

      HornetQServerLogger.LOGGER.debug("Loading server configuration from " + url);

      Reader reader = new InputStreamReader(url.openStream());
      String xml = org.apache.activemq6.utils.XMLUtil.readerToString(reader);
      xml = XMLUtil.replaceSystemProps(xml);
      Element e = org.apache.activemq6.utils.XMLUtil.stringToElement(xml);

      FileConfigurationParser parser = new FileConfigurationParser();

      // https://jira.jboss.org/browse/HORNETQ-478 - We only want to validate AIO when
      //     starting the server
      //     and we don't want to do it when deploying hornetq-queues.xml which uses the same parser and XML format
      parser.setValidateAIO(true);

      parser.parseMainConfig(e, this);

      started = true;

   }

   public synchronized void stop() throws Exception
   {
      started = false;
   }

   public String getConfigurationUrl()
   {
      return configurationUrl;
   }

   public void setConfigurationUrl(final String configurationUrl)
   {
      this.configurationUrl = configurationUrl;
   }
}
