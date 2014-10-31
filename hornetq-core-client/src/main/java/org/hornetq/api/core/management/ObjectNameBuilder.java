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
package org.hornetq.api.core.management;

import javax.management.ObjectName;

import org.hornetq.api.config.HornetQDefaultConfiguration;
import org.hornetq.api.core.SimpleString;

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
   public static final ObjectNameBuilder DEFAULT = new ObjectNameBuilder(HornetQDefaultConfiguration.getDefaultJmxDomain());

   static final String DEFAULT_SUBSYSTEM = "messaging";
   static final String DEFAULT_SERVER = "default";

   // Attributes ----------------------------------------------------

   private final String domain;
   private final String subsystem;
   private final String serverName;

   // Static --------------------------------------------------------

   public static ObjectNameBuilder create(final String domain)
   {
      if (domain == null)
      {
         return new ObjectNameBuilder(HornetQDefaultConfiguration.getDefaultJmxDomain());
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
      this.subsystem = DEFAULT_SUBSYSTEM;
      this.serverName = DEFAULT_SERVER;
   }

   // Public --------------------------------------------------------

   /**
    * Returns the ObjectName used by the single {@link HornetQServerControl}.
    */
   public ObjectName getHornetQServerObjectName() throws Exception
   {
      String value = String.format("%s:subsystem=%s,hornetq-server=%s",
            domain, subsystem, serverName);
      return ObjectName.getInstance(value);
   }

   /**
    * Returns the ObjectName used by AddressControl.
    *
    * @see AddressControl
    */
   public ObjectName getAddressObjectName(final SimpleString address) throws Exception
   {
      return createObjectName("core-address", address.toString());
   }

   /**
    * Returns the ObjectName used by QueueControl.
    *
    * @see QueueControl
    */
   public ObjectName getQueueObjectName(final SimpleString address, final SimpleString name) throws Exception
   {
      ObjectProperty[] props = new ObjectProperty[] {
         new ObjectProperty("address", address.toString()),
         new ObjectProperty("runtime-queue", name.toString())
      };
      return this.createObjectName(props);
   }

   /**
    * Returns the ObjectName used by DivertControl.
    *
    * @see DivertControl
    */
   public ObjectName getDivertObjectName(final String name) throws Exception
   {
      return createObjectName("divert", name.toString());
   }

   /**
    * Returns the ObjectName used by AcceptorControl.
    *
    * @see AcceptorControl
    */
   public ObjectName getAcceptorObjectName(final String name) throws Exception
   {
      return createObjectName("remote-acceptor", name);
   }

   /**
    * Returns the ObjectName used by BroadcastGroupControl.
    *
    * @see BroadcastGroupControl
    */
   public ObjectName getBroadcastGroupObjectName(final String name) throws Exception
   {
      return createObjectName("broadcast-group", name);
   }

   /**
    * Returns the ObjectName used by BridgeControl.
    *
    * @see BridgeControl
    */
   public ObjectName getBridgeObjectName(final String name) throws Exception
   {
      return createObjectName("bridge", name);
   }

   /**
    * Returns the ObjectName used by ClusterConnectionControl.
    *
    * @see ClusterConnectionControl
    */
   public ObjectName getClusterConnectionObjectName(final String name) throws Exception
   {
      return createObjectName("cluster-connection", name);
   }

   /**
    * Returns the ObjectName used by DiscoveryGroupControl.
    *
    * @see DiscoveryGroupControl
    */
   public ObjectName getDiscoveryGroupObjectName(final String name) throws Exception
   {
      return createObjectName("discovery-group", name);
   }

   /**
    * Returns the ObjectName used by JMSServerControl.
    * @see org.hornetq.api.jms.management.JMSServerControl
    */
   public ObjectName getJMSServerObjectName() throws Exception
   {
      String value = String.format("%s:subsystem=%s,hornetq-server=%s",
            domain, subsystem, serverName);
      return ObjectName.getInstance(value);
   }

   /**
    * Returns the ObjectName used by JMSQueueControl.
    * @see org.hornetq.api.jms.management.JMSQueueControl
    */
   public ObjectName getJMSQueueObjectName(final String name) throws Exception
   {
      return createObjectName("jms-queue", name);
   }

   /**
    * Returns the ObjectName used by TopicControl.
    *
    * @see TopicControl
    */
   public ObjectName getJMSTopicObjectName(final String name) throws Exception
   {
      return createObjectName("jms-topic", name);
   }

   /**
    * Returns the ObjectName used by ConnectionFactoryControl.
    * @see org.hornetq.api.jms.management.ConnectionFactoryControl
    */
   public ObjectName getConnectionFactoryObjectName(final String name) throws Exception
   {
      return createObjectName("connection-factory", name);
   }

   private ObjectName createObjectName(String beanType, String beanName) throws Exception
   {
      return this.createObjectName(new ObjectProperty[] {new ObjectProperty(beanType, beanName)});
   }

   private ObjectName createObjectName(ObjectProperty[] props) throws Exception
   {
      String baseValue = String.format("%s:subsystem=%s,hornetq-server=%s",
                                   domain, subsystem, serverName);
      StringBuilder builder = new StringBuilder(baseValue);
      for (ObjectProperty p : props)
      {
         builder.append(",");
         builder.append(p.key);
         builder.append("=");
         builder.append(ObjectName.quote(p.val));
      }
      return ObjectName.getInstance(builder.toString());
   }

   private static class ObjectProperty
   {
      String key;
      String val;

      public ObjectProperty(String k, String v)
      {
         this.key = k;
         this.val = v;
      }
   }
}
