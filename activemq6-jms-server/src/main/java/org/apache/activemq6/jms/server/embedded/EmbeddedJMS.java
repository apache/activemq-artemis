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
package org.apache.activemq6.jms.server.embedded;

import javax.naming.Context;

import org.apache.activemq6.core.registry.JndiBindingRegistry;
import org.apache.activemq6.core.registry.MapBindingRegistry;
import org.apache.activemq6.core.server.embedded.EmbeddedHornetQ;
import org.apache.activemq6.jms.server.config.JMSConfiguration;
import org.apache.activemq6.jms.server.impl.JMSServerManagerImpl;
import org.apache.activemq6.spi.core.naming.BindingRegistry;

/**
 * Simple bootstrap class that parses hornetq config files (server and jms and security) and starts
 * a HornetQServer instance and populates it with configured JMS endpoints.
 * <p>
 * JMS Endpoints are registered with a simple MapBindingRegistry.  If you want to use a different registry
 * you must set the registry property of this class or call the setRegistry() method if you want to use JNDI
 *
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class EmbeddedJMS extends EmbeddedHornetQ
{
   protected JMSServerManagerImpl serverManager;
   protected BindingRegistry registry;
   protected String jmsConfigResourcePath;
   protected JMSConfiguration jmsConfiguration;
   protected Context context;

   /**
    * Classpath resource where JMS config file is.  Defaults to 'hornetq-jms.xml'
    *
    * @param jmsConfigResourcePath
    */
   public void setJmsConfigResourcePath(String jmsConfigResourcePath)
   {
      this.jmsConfigResourcePath = jmsConfigResourcePath;
   }

   public BindingRegistry getRegistry()
   {
      return registry;
   }

   /**
    * Only set this property if you are using a custom BindingRegistry
    *
    * @param registry
    */
   public void setRegistry(BindingRegistry registry)
   {
      this.registry = registry;
   }

   /**
    * By default, this class uses file-based configuration.  Set this property to override it.
    *
    * @param jmsConfiguration
    */
   public void setJmsConfiguration(JMSConfiguration jmsConfiguration)
   {
      this.jmsConfiguration = jmsConfiguration;
   }

   /**
    * If you want to use JNDI instead of an internal map, set this property
    *
    * @param context
    */
   public void setContext(Context context)
   {
      this.context = context;
   }

   /**
    * Lookup in the registry for registered object, i.e. a ConnectionFactory.
    * <p>
    * This is a convenience method.
    * @param name
    */
   public Object lookup(String name)
   {
      return serverManager.getRegistry().lookup(name);
   }

   @Override
   public void start() throws Exception
   {
      super.initStart();
      if (jmsConfiguration != null)
      {
         serverManager = new JMSServerManagerImpl(hornetQServer, jmsConfiguration);
      }
      else if (jmsConfigResourcePath == null) serverManager = new JMSServerManagerImpl(hornetQServer);
      else serverManager = new JMSServerManagerImpl(hornetQServer, jmsConfigResourcePath);

      if (registry == null)
      {
         if (context != null) registry = new JndiBindingRegistry(context);
         else registry = new MapBindingRegistry();
      }
      serverManager.setRegistry(registry);
      serverManager.start();
   }

   @Override
   public void stop() throws Exception
   {
      serverManager.stop();
   }

}
