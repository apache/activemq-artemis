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
package org.apache.activemq.api.core.management;

import javax.management.ObjectName;

import org.apache.activemq.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.api.core.SimpleString;

/**
 * Helper class to build ObjectNames for HornetQ resources.
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 */
public final class ObjectNameBuilder
{

   // Constants -----------------------------------------------------

   /**
    * Default JMX domain for HornetQ resources.
    */
   public static final ObjectNameBuilder DEFAULT = new ObjectNameBuilder(ActiveMQDefaultConfiguration.getDefaultJmxDomain());

   static final String JMS_MODULE = "JMS";

   static final String CORE_MODULE = "Core";

   // Attributes ----------------------------------------------------

   private final String domain;

   // Static --------------------------------------------------------

   public static ObjectNameBuilder create(final String domain)
   {
      if (domain == null)
      {
         return new ObjectNameBuilder(ActiveMQDefaultConfiguration.getDefaultJmxDomain());
      }
      else
      {
         return new ObjectNameBuilder(domain);
      }
   }

   // Constructors --------------------------------------------------

   private ObjectNameBuilder(final String domain)
   {
      this.domain = domain;
   }

   // Public --------------------------------------------------------

   /**
    * Returns the ObjectName used by the single {@link HornetQServerControl}.
    */
   public ObjectName getHornetQServerObjectName() throws Exception
   {
      return ObjectName.getInstance(domain + ":module=Core,type=Server");
   }

   /**
    * Returns the ObjectName used by AddressControl.
    *
    * @see AddressControl
    */
   public ObjectName getAddressObjectName(final SimpleString address) throws Exception
   {
      return createObjectName(ObjectNameBuilder.CORE_MODULE, "Address", address.toString());
   }

   /**
    * Returns the ObjectName used by QueueControl.
    *
    * @see QueueControl
    */
   public ObjectName getQueueObjectName(final SimpleString address, final SimpleString name) throws Exception
   {
      return ObjectName.getInstance(String.format("%s:module=%s,type=%s,address=%s,name=%s",
                                                  domain,
                                                  ObjectNameBuilder.CORE_MODULE,
                                                  "Queue",
                                                  ObjectName.quote(address.toString()),
                                                  ObjectName.quote(name.toString())));
   }

   /**
    * Returns the ObjectName used by DivertControl.
    *
    * @see DivertControl
    */
   public ObjectName getDivertObjectName(final String name) throws Exception
   {
      return createObjectName(ObjectNameBuilder.CORE_MODULE, "Divert", name.toString());
   }

   /**
    * Returns the ObjectName used by AcceptorControl.
    *
    * @see AcceptorControl
    */
   public ObjectName getAcceptorObjectName(final String name) throws Exception
   {
      return createObjectName(ObjectNameBuilder.CORE_MODULE, "Acceptor", name);
   }

   /**
    * Returns the ObjectName used by BroadcastGroupControl.
    *
    * @see BroadcastGroupControl
    */
   public ObjectName getBroadcastGroupObjectName(final String name) throws Exception
   {
      return createObjectName(ObjectNameBuilder.CORE_MODULE, "BroadcastGroup", name);
   }

   /**
    * Returns the ObjectName used by BridgeControl.
    *
    * @see BridgeControl
    */
   public ObjectName getBridgeObjectName(final String name) throws Exception
   {
      return createObjectName(ObjectNameBuilder.CORE_MODULE, "Bridge", name);
   }

   /**
    * Returns the ObjectName used by ClusterConnectionControl.
    *
    * @see ClusterConnectionControl
    */
   public ObjectName getClusterConnectionObjectName(final String name) throws Exception
   {
      return createObjectName(ObjectNameBuilder.CORE_MODULE, "ClusterConnection", name);
   }

   /**
    * Returns the ObjectName used by DiscoveryGroupControl.
    *
    * @see DiscoveryGroupControl
    */
   public ObjectName getDiscoveryGroupObjectName(final String name) throws Exception
   {
      return createObjectName(ObjectNameBuilder.CORE_MODULE, "DiscoveryGroup", name);
   }

   /**
    * Returns the ObjectName used by JMSServerControl.
    * @see org.apache.activemq.api.jms.management.JMSServerControl
    */
   public ObjectName getJMSServerObjectName() throws Exception
   {
      return ObjectName.getInstance(domain + ":module=JMS,type=Server");
   }

   /**
    * Returns the ObjectName used by JMSQueueControl.
    * @see org.apache.activemq.api.jms.management.JMSQueueControl
    */
   public ObjectName getJMSQueueObjectName(final String name) throws Exception
   {
      return createObjectName(ObjectNameBuilder.JMS_MODULE, "Queue", name);
   }

   /**
    * Returns the ObjectName used by TopicControl.
    *
    * @see TopicControl
    */
   public ObjectName getJMSTopicObjectName(final String name) throws Exception
   {
      return createObjectName(ObjectNameBuilder.JMS_MODULE, "Topic", name);
   }

   /**
    * Returns the ObjectName used by ConnectionFactoryControl.
    * @see org.apache.activemq.api.jms.management.ConnectionFactoryControl
    */
   public ObjectName getConnectionFactoryObjectName(final String name) throws Exception
   {
      return createObjectName(ObjectNameBuilder.JMS_MODULE, "ConnectionFactory", name);
   }

   private ObjectName createObjectName(final String module, final String type, final String name) throws Exception
   {
      return ObjectName.getInstance(String.format("%s:module=%s,type=%s,name=%s",
                                                  domain,
                                                  module,
                                                  type,
                                                  ObjectName.quote(name)));
   }
}
