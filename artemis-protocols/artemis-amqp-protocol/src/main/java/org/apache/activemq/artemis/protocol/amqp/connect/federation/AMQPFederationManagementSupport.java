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
package org.apache.activemq.artemis.protocol.amqp.connect.federation;

import javax.management.ObjectName;

import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.management.ManagementService;

/**
 * Support methods for working with the AMQP Federation management types
 */
public abstract class AMQPFederationManagementSupport {

   /**
    * Template used to denote local federation policy managers in the server management registry.
    */
   public static final String FEDERATION_POLICY_RESOURCE_TEMPLATE = "brokerconnection.local.federation.policy.%s";

   /**
    * Template used to denote local federation consumers in the server management registry. Since policy names
    * are unique on the local broker AMQP federation configuration these names should not collide as each policy
    * will create only one consumer for a given address or queue.
    */
   public static final String FEDERATION_CONSUMER_RESOURCE_TEMPLATE = "brokerconnection.local.federation.policy.%s.consumer.%s";

   /**
    * The template used to create the object name suffix that is appending to the broker connection
    * object name when adding and removing AMQP federation policy control elements.
    */
   public static final String FEDERATION_POLICY_NAME_TEMPLATE = "serviceCatagory=federations,federationName=%s,policyType=%s,policyName=%s";

   /**
    * The template used to create the object name suffix that is appending to the broker connection
    * object name when adding and removing AMQP federation queue consumer control elements.
    */
   public static final String FEDERATION_QUEUE_CONSUMER_NAME_TEMPLATE = FEDERATION_POLICY_NAME_TEMPLATE + ",linkType=consumers,fqqn=%s";

   /**
    * The template used to create the object name suffix that is appending to the broker connection
    * object name when adding and removing AMQP federation address consumer control elements.
    */
   public static final String FEDERATION_ADDRESS_CONSUMER_NAME_TEMPLATE = FEDERATION_POLICY_NAME_TEMPLATE + ",linkType=consumers,address=%s";

   // APIs for federation address and queue policy management

   public static void registerAddressPolicyControl(String brokerConnection, AMQPFederationAddressPolicyManager manager) throws Exception {
      final AMQPFederation federation = manager.getFederation();
      final ActiveMQServer server = federation.getServer();
      final ManagementService management = server.getManagementService();
      final AMQPFederationAddressPolicyControl control = new AMQPFederationAddressPolicyControl(manager);
      final String policyName = manager.getPolicyName();

      management.registerInJMX(getFederationPolicyObjectName(management, brokerConnection, federation.getName(), manager.getPolicyType().toString(), policyName), control);
      management.registerInRegistry(getFederationPolicyResourceName(policyName), control);
   }

   public static void unregisterAddressPolicyControl(String brokerConnection, AMQPFederationAddressPolicyManager manager) throws Exception {
      final AMQPFederation federation = manager.getFederation();
      final ActiveMQServer server = federation.getServer();
      final ManagementService management = server.getManagementService();
      final String policyName = manager.getPolicyName();

      management.unregisterFromJMX(getFederationPolicyObjectName(management, brokerConnection, federation.getName(), manager.getPolicyType().toString(), policyName));
      management.unregisterFromRegistry(getFederationPolicyResourceName(policyName));
   }

   public static void registerQueuePolicyControl(String brokerConnection, AMQPFederationQueuePolicyManager manager) throws Exception {
      final AMQPFederation federation = manager.getFederation();
      final ActiveMQServer server = federation.getServer();
      final ManagementService management = server.getManagementService();
      final AMQPFederationQueuePolicyControl control = new AMQPFederationQueuePolicyControl(manager);
      final String policyName = manager.getPolicyName();

      management.registerInJMX(getFederationPolicyObjectName(management, brokerConnection, federation.getName(), manager.getPolicyType().toString(), policyName), control);
      management.registerInRegistry(getFederationPolicyResourceName(policyName), control);
   }

   public static void unregisterQueuePolicyControl(String brokerConnection, AMQPFederationQueuePolicyManager manager) throws Exception {
      final AMQPFederation federation = manager.getFederation();
      final ActiveMQServer server = federation.getServer();
      final ManagementService management = server.getManagementService();
      final String policyName = manager.getPolicyName();

      management.unregisterFromJMX(getFederationPolicyObjectName(management, brokerConnection, federation.getName(), manager.getPolicyType().toString(), policyName));
      management.unregisterFromRegistry(getFederationPolicyResourceName(policyName));
   }

   public static String getFederationPolicyResourceName(String policyName) {
      return String.format(FEDERATION_POLICY_RESOURCE_TEMPLATE, policyName);
   }

   public static ObjectName getFederationPolicyObjectName(ManagementService management, String brokerConnection, String federationName, String policyType, String policyName) throws Exception {
      final String brokerConnectionName = management.getObjectNameBuilder().getBrokerConnectionBaseObjectNameString(brokerConnection);

      return ObjectName.getInstance(
         String.format("%s," + FEDERATION_POLICY_NAME_TEMPLATE,
            brokerConnectionName,
            ObjectName.quote(federationName),
            ObjectName.quote(policyType),
            ObjectName.quote(policyName)));
   }

   // APIs for federation consumer management

   public static void registerAddressConsumerControl(String brokerConnection, AMQPFederationAddressPolicyManager manager, AMQPFederationAddressConsumer consumer) throws Exception {
      final AMQPFederation federation = consumer.getFederation();
      final ActiveMQServer server = federation.getServer();
      final ManagementService management = server.getManagementService();
      final AMQPFederationConsumerControlType control = new AMQPFederationConsumerControlType(consumer);
      final String policyName = manager.getPolicyName();

      management.registerInJMX(getFederationAddressConsumerObjectName(management, brokerConnection, federation.getName(), manager.getPolicyType().toString(), policyName, consumer.getConsumerInfo().getAddress()), control);
      management.registerInRegistry(getFederationAddressConsumerResourceName(policyName, consumer.getConsumerInfo().getAddress()), control);
   }

   public static void unregisterAddressConsumerControl(String brokerConnection, AMQPFederationAddressPolicyManager manager, AMQPFederationAddressConsumer consumer) throws Exception {
      final AMQPFederation federation = consumer.getFederation();
      final ActiveMQServer server = federation.getServer();
      final ManagementService management = server.getManagementService();
      final String policyName = manager.getPolicyName();

      management.unregisterFromJMX(getFederationAddressConsumerObjectName(management, brokerConnection, federation.getName(), manager.getPolicyType().toString(), policyName, consumer.getConsumerInfo().getAddress()));
      management.unregisterFromRegistry(getFederationAddressConsumerResourceName(policyName, consumer.getConsumerInfo().getAddress()));
   }

   public static void registerQueueConsumerControl(String brokerConnection, AMQPFederationQueuePolicyManager manager, AMQPFederationQueueConsumer consumer) throws Exception {
      final AMQPFederation federation = consumer.getFederation();
      final ActiveMQServer server = federation.getServer();
      final ManagementService management = server.getManagementService();
      final AMQPFederationConsumerControlType control = new AMQPFederationConsumerControlType(consumer);
      final String policyName = manager.getPolicyName();

      management.registerInJMX(getFederationQueueConsumerObjectName(management, brokerConnection, federation.getName(), manager.getPolicyType().toString(), policyName, consumer.getConsumerInfo().getFqqn()), control);
      management.registerInRegistry(getFederationQueueConsumerResourceName(policyName, consumer.getConsumerInfo().getFqqn()), control);
   }

   public static void unregisterQueueConsumerControl(String brokerConnection, AMQPFederationQueuePolicyManager manager, AMQPFederationQueueConsumer consumer) throws Exception {
      final AMQPFederation federation = consumer.getFederation();
      final ActiveMQServer server = federation.getServer();
      final ManagementService management = server.getManagementService();
      final String policyName = manager.getPolicyName();

      management.unregisterFromJMX(getFederationQueueConsumerObjectName(management, brokerConnection, federation.getName(), manager.getPolicyType().toString(), policyName, consumer.getConsumerInfo().getFqqn()));
      management.unregisterFromRegistry(getFederationQueueConsumerResourceName(policyName, consumer.getConsumerInfo().getFqqn()));
   }

   public static String getFederationAddressConsumerResourceName(String policyName, String address) {
      return String.format(FEDERATION_CONSUMER_RESOURCE_TEMPLATE, policyName, address);
   }

   public static String getFederationQueueConsumerResourceName(String policyName, String fqqn) {
      return String.format(FEDERATION_CONSUMER_RESOURCE_TEMPLATE, policyName, fqqn);
   }

   public static ObjectName getFederationAddressConsumerObjectName(ManagementService management, String brokerConnection, String federationName, String policyType, String policyName, String address) throws Exception {
      final String brokerConnectionName = management.getObjectNameBuilder().getBrokerConnectionBaseObjectNameString(brokerConnection);

      return ObjectName.getInstance(
         String.format("%s," + FEDERATION_ADDRESS_CONSUMER_NAME_TEMPLATE,
            brokerConnectionName,
            ObjectName.quote(federationName),
            ObjectName.quote(policyType),
            ObjectName.quote(policyName),
            ObjectName.quote(address)));
   }

   public static ObjectName getFederationQueueConsumerObjectName(ManagementService management, String brokerConnection, String federationName, String policyType, String policyName, String fqqn) throws Exception {
      final String brokerConnectionName = management.getObjectNameBuilder().getBrokerConnectionBaseObjectNameString(brokerConnection);

      return ObjectName.getInstance(
         String.format("%s," + FEDERATION_QUEUE_CONSUMER_NAME_TEMPLATE,
            brokerConnectionName,
            ObjectName.quote(federationName),
            ObjectName.quote(policyType),
            ObjectName.quote(policyName),
            ObjectName.quote(fqqn)));
   }
}
