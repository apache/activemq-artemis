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
package org.apache.activemq6.jms.client;

import javax.jms.QueueConnectionFactory;

import org.apache.activemq6.api.core.DiscoveryGroupConfiguration;
import org.apache.activemq6.api.core.TransportConfiguration;
import org.apache.activemq6.api.core.client.ServerLocator;
import org.apache.activemq6.api.jms.JMSFactoryType;

/**
 * A class that represents a QueueConnectionFactory.
 *
 * @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
 */
public class HornetQQueueConnectionFactory extends HornetQConnectionFactory implements QueueConnectionFactory
{
   private static final long serialVersionUID = 5312455021322463546L;

   /**
    *
    */
   public HornetQQueueConnectionFactory()
   {
      super();
   }

   /**
    * @param serverLocator
    */
   public HornetQQueueConnectionFactory(ServerLocator serverLocator)
   {
      super(serverLocator);
   }

   /**
    * @param ha
    * @param groupConfiguration
    */
   public HornetQQueueConnectionFactory(boolean ha, final DiscoveryGroupConfiguration groupConfiguration)
   {
      super(ha, groupConfiguration);
   }

   /**
    * @param ha
    * @param initialConnectors
    */
   public HornetQQueueConnectionFactory(boolean ha, TransportConfiguration... initialConnectors)
   {
      super(ha, initialConnectors);
   }

   public int getFactoryType()
   {
      return JMSFactoryType.QUEUE_CF.intValue();
   }
}
