/*
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
package org.apache.activemq.artemis.api.core.management;

import javax.management.ObjectName;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.RoutingType;

/**
 * Helper class to build ObjectNames for ActiveMQ Artemis resources.
 */
public final class ObjectNameBuilder {

   // Constants -----------------------------------------------------

   /**
    * Default JMX domain for ActiveMQ Artemis resources.
    */
   public static final ObjectNameBuilder DEFAULT = new ObjectNameBuilder(ActiveMQDefaultConfiguration.getDefaultJmxDomain(), "localhost", true);

   // Attributes ----------------------------------------------------

   private final String domain;

   private String brokerName;

   private final boolean jmxUseBrokerName;

   // Static --------------------------------------------------------

   public static ObjectNameBuilder create(final String domain) {
      if (domain == null) {
         return new ObjectNameBuilder(ActiveMQDefaultConfiguration.getDefaultJmxDomain(), null, false);
      } else {
         return new ObjectNameBuilder(domain, null, false);
      }
   }

   public static ObjectNameBuilder create(final String domain, String brokerName) {
      if (domain == null) {
         return new ObjectNameBuilder(ActiveMQDefaultConfiguration.getDefaultJmxDomain(), brokerName, true);
      } else {
         return new ObjectNameBuilder(domain, brokerName, true);
      }
   }

   public static ObjectNameBuilder create(final String domain, String brokerName, boolean jmxUseBrokerName) {
      if (domain == null) {
         return new ObjectNameBuilder(ActiveMQDefaultConfiguration.getDefaultJmxDomain(), brokerName, jmxUseBrokerName);
      } else {
         return new ObjectNameBuilder(domain, brokerName, jmxUseBrokerName);
      }
   }

   // Constructors --------------------------------------------------

   private ObjectNameBuilder(final String domain, final String brokerName, boolean jmxUseBrokerName) {
      this.domain = domain;
      this.brokerName = brokerName;
      this.jmxUseBrokerName = jmxUseBrokerName;
   }

   // Public --------------------------------------------------------

   /**
    * Returns the ObjectName used by the single {@link ActiveMQServerControl}.
    */
   public ObjectName getActiveMQServerObjectName() throws Exception {
      return ObjectName.getInstance(domain + ":" + getBrokerProperties() + getObjectType() + "=Broker");
   }

   /**
    * Returns the ObjectName used by AddressControl.
    *
    * @see AddressControl
    */
   public ObjectName getAddressObjectName(final SimpleString address) throws Exception {
      return createObjectName("Address", address.toString());
   }

   /**
    * Returns the ObjectName used by QueueControl.
    *
    * @see QueueControl
    */
   public ObjectName getQueueObjectName(final SimpleString address, final SimpleString name, RoutingType routingType) throws Exception {
      return ObjectName.getInstance(String.format("%s:" + getBrokerProperties() + "parentType=%s,parentName=%s," + getObjectType() + "=%s, routingType=%s,name=%s", domain, "Address", ObjectName.quote(address.toString()), "Queue", routingType.toString(), ObjectName.quote(name.toString())));
   }

   /**
    * Returns the ObjectName used by QueueControl.
    *
    * @see QueueControl
    */
   public ObjectName getQueueObjectName(final SimpleString address, final SimpleString name) throws Exception {
      return ObjectName.getInstance(String.format("%s:" + getBrokerProperties() + "parentType=%s,parentName=%s," + getObjectType() + "=%s, routingType=%s,name=%s", domain, "Address", ObjectName.quote(address.toString()), "Queue", ActiveMQDefaultConfiguration.getDefaultRoutingType(), ObjectName.quote(name.toString())));
   }


   /**
    * Returns the ObjectName used by DivertControl.
    *
    * @see DivertControl
    */
   public ObjectName getDivertObjectName(final String name, String address) throws Exception {
      return ObjectName.getInstance(String.format("%s:" + getBrokerProperties() + "parentType=%s,parentName=%s," + getObjectType() + "=%s,name=%s", domain, "Address", ObjectName.quote(address.toString()), "Divert", ObjectName.quote(name.toString())));
   }

   /**
    * Returns the ObjectName used by AcceptorControl.
    *
    * @see AcceptorControl
    */
   public ObjectName getAcceptorObjectName(final String name) throws Exception {
      return createObjectName("Acceptor", name);
   }

   /**
    * Returns the ObjectName used by BroadcastGroupControl.
    *
    * @see BroadcastGroupControl
    */
   public ObjectName getBroadcastGroupObjectName(final String name) throws Exception {
      return createObjectName("BroadcastGroup", name);
   }

   /**
    * Returns the ObjectName used by BridgeControl.
    *
    * @see BridgeControl
    */
   public ObjectName getBridgeObjectName(final String name) throws Exception {
      return createObjectName("Bridge", name);
   }

   /**
    * Returns the ObjectName used by ClusterConnectionControl.
    *
    * @see ClusterConnectionControl
    */
   public ObjectName getClusterConnectionObjectName(final String name) throws Exception {
      return createObjectName("ClusterConnection", name);
   }

   /**
    * Returns the ObjectName used by DiscoveryGroupControl.
    */
   public ObjectName getDiscoveryGroupObjectName(final String name) throws Exception {
      return createObjectName("DiscoveryGroup", name);
   }

   /**
    * Returns the ObjectName used by JMSServerControl.
    */
   public ObjectName getJMSServerObjectName() throws Exception {
      return ObjectName.getInstance(domain + ":" + getBrokerProperties() + "module=JMS," + getObjectType() + "=Server");
   }

   /**
    * Returns the ObjectName used by JMSQueueControl.
    */
   public ObjectName getJMSQueueObjectName(final String name) throws Exception {
      return getQueueObjectName(SimpleString.toSimpleString(name), SimpleString.toSimpleString(name), RoutingType.ANYCAST);
   }

   /**
    * Returns the ObjectName used by TopicControl.
    */
   public ObjectName getJMSTopicObjectName(final String name) throws Exception {
      return createObjectName("Topic", name);
   }

   /**
    * Returns the ObjectName used by ConnectionFactoryControl.
    */
   public ObjectName getConnectionFactoryObjectName(final String name) throws Exception {
      return createObjectName("ConnectionFactory", name);
   }

   private ObjectName createObjectName(final String type, final String name) throws Exception {
      String format = String.format("%s:" + getBrokerProperties() + getObjectType() + "=%s,name=%s", domain, type, ObjectName.quote(name));
      return ObjectName.getInstance(format);
   }

   private String getBrokerProperties() {
      if (jmxUseBrokerName && brokerName != null) {
         return String.format("type=Broker,brokerName=%s,", ObjectName.quote(brokerName));
      } else {
         return "";
      }
   }

   private String getObjectType() {
      if (jmxUseBrokerName && brokerName != null) {
         return "serviceType";
      } else {
         return "type";
      }
   }
}
