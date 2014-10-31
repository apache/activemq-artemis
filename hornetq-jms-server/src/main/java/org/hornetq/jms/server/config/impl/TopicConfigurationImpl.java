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
package org.hornetq.jms.server.config.impl;

import org.hornetq.jms.server.config.TopicConfiguration;


/**
 * A TopicConfigurationImpl
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 *
 */
public class TopicConfigurationImpl implements TopicConfiguration
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final String name;

   private final String[] bindings;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public TopicConfigurationImpl(final String name, final String... bindings)
   {
      this.name = name;
      this.bindings = new String[bindings.length];
      System.arraycopy(bindings, 0, this.bindings, 0, bindings.length);
   }

   // TopicConfiguration implementation -----------------------------

   public String[] getBindings()
   {
      return bindings;
   }

   public String getName()
   {
      return name;
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
