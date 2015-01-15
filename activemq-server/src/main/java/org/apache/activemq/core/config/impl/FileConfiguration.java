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
package org.apache.activemq.core.config.impl;

import java.util.Map;

import org.apache.activemq.core.deployers.Deployable;
import org.apache.activemq.core.deployers.impl.FileConfigurationParser;
import org.apache.activemq.core.server.ActiveMQComponent;
import org.apache.activemq.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.spi.core.security.ActiveMQSecurityManager;
import org.w3c.dom.Element;

import javax.management.MBeanServer;

/**
 * A {@code FileConfiguration} reads configuration values from a file.
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 */
public final class FileConfiguration extends ConfigurationImpl implements Deployable
{
   private static final long serialVersionUID = -4766689627675039596L;

   private static final String CONFIGURATION_SCHEMA_URL = "schema/activemq-configuration.xsd";

   private static final String CONFIGURATION_SCHEMA_ROOT_ELEMENT = "core";

   // For a bridge confirmations must be activated or send acknowledgments won't return
   public static final int DEFAULT_CONFIRMATION_WINDOW_SIZE = 1024 * 1024;

   private boolean parsed = false;

   @Override
   public void parse(Element config) throws Exception
   {
      FileConfigurationParser parser = new FileConfigurationParser();

      // https://jira.jboss.org/browse/HORNETQ-478 - We only want to validate AIO when
      //     starting the server
      //     and we don't want to do it when deploying activemq-queues.xml which uses the same parser and XML format
      parser.setValidateAIO(true);

      parser.parseMainConfig(config, this);

      parsed = true;
   }

   @Override
   public boolean isParsed()
   {
      return parsed;
   }

   @Override
   public String getRootElement()
   {
      return CONFIGURATION_SCHEMA_ROOT_ELEMENT;
   }

   @Override
   public void buildService(ActiveMQSecurityManager securityManager, MBeanServer mBeanServer, Map<String, Deployable> deployables, Map<String, ActiveMQComponent> components)
   {
      components.put(getRootElement(), new ActiveMQServerImpl(this, mBeanServer, securityManager));
   }

   @Override
   public String getSchema()
   {
      return CONFIGURATION_SCHEMA_URL;
   }
}
