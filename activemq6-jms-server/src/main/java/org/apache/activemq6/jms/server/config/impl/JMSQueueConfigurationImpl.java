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
package org.apache.activemq6.jms.server.config.impl;

import org.apache.activemq6.jms.server.config.JMSQueueConfiguration;


/**
 * A QueueConfigurationImpl
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 *
 */
public class JMSQueueConfigurationImpl implements JMSQueueConfiguration
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private String name = null;

   private String selector = null;

   private boolean durable = true;

   private String[] bindings = null;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public JMSQueueConfigurationImpl()
   {
   }

   // QueueConfiguration implementation -----------------------------

   public String[] getBindings()
   {
      return bindings;
   }

   public JMSQueueConfigurationImpl setBindings(String... bindings)
   {
      this.bindings = bindings;
      return this;
   }

   public String getName()
   {
      return name;
   }

   public JMSQueueConfigurationImpl setName(String name)
   {
      this.name = name;
      return this;
   }

   public String getSelector()
   {
      return selector;
   }

   public JMSQueueConfigurationImpl setSelector(String selector)
   {
      this.selector = selector;
      return this;
   }

   public boolean isDurable()
   {
      return durable;
   }

   public JMSQueueConfigurationImpl setDurable(boolean durable)
   {
      this.durable = durable;
      return this;
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
