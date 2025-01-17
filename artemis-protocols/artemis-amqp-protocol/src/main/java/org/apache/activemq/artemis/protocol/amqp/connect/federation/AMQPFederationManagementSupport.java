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
import org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationSenderController.Role;
import org.apache.activemq.artemis.protocol.amqp.federation.FederationConsumerInfo;
import org.apache.activemq.artemis.utils.CompositeAddress;

/**
 * Support methods for working with the AMQP Federation management types
 */
public abstract class AMQPFederationManagementSupport {

   /**
    * Template used to denote federation source instances in the server management registry.
    */
   public static final String FEDERATION_SOURCE_RESOURCE_TEMPLATE = "brokerconnection.local.federation.%s";

   /**
    * Template used to denote local federation policy managers in the server management registry.
    */
   public static final String FEDERATION_POLICY_RESOURCE_TEMPLATE = "brokerconnection.local.federation.%s.policy.%s";

   /**
    * Template used to denote local federation consumers in the server management registry. Since policy names
    * are unique on the local broker AMQP federation configuration these names should not collide as each policy
    * will create only one consumer for a given address or queue.
    */
   public static final String FEDERATION_CONSUMER_RESOURCE_TEMPLATE = "brokerconnection.local.federation.%s.policy.%s.consumer.%s";

   /**
    * Template used to denote local federation producers in the server management registry. Since policy names
    * are unique on the local broker AMQP federation configuration these names should not collide as each policy
    * will create only one producer for a given address or queue.
    */
   public static final String FEDERATION_PRODUCER_RESOURCE_TEMPLATE = "brokerconnection.local.federation.%s.policy.%s.producer.%s";

   /**
    * The template used to create the object name suffix that is appending to the broker connection
    * object name when adding and removing AMQP federation policy control elements.
    */
   public static final String FEDERATION_NAME_TEMPLATE = "serviceCatagory=federations,federationName=%s";

   /**
    * The template used to create the object name suffix that is appending to the broker connection
    * object name when adding and removing AMQP federation policy control elements.
    */
   public static final String FEDERATION_POLICY_NAME_TEMPLATE = FEDERATION_NAME_TEMPLATE + ",policyType=%s,policyName=%s";

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

   /**
    * The template used to create the object name suffix that is appending to the broker connection
    * object name when adding and removing AMQP federation queue producer control elements.
    */
   public static final String FEDERATION_QUEUE_PRODUCER_NAME_TEMPLATE = FEDERATION_POLICY_NAME_TEMPLATE + ",linkType=producers,fqqn=%s";

   /**
    * The template used to create the object name suffix that is appending to the broker connection
    * object name when adding and removing AMQP federation address producer control elements.
    */
   public static final String FEDERATION_ADDRESS_PRODUCER_NAME_TEMPLATE = FEDERATION_POLICY_NAME_TEMPLATE + ",linkType=producers,address=%s";

   // APIS for registration of federation instance with management

   /**
    * Register the given {@link AMQPFederationSource} instance with the broker management services.
    *
    * @param federation
    *    The federation source instance being registered with management.
    *
    * @throws Exception if an error occurs while registering the federation with the management services.
    */
   public static void registerFederationManagement(AMQPFederationSource federation) throws Exception {
      final String federationName = federation.getName();
      final String brokerConnectionName = federation.getBrokerConnection().getName();
      final ActiveMQServer server = federation.getServer();
      final ManagementService management = server.getManagementService();
      final AMQPFederationSourceControlType control = new AMQPFederationSourceControlType(server, federation);

      management.registerInJMX(getFederationSourceObjectName(management, brokerConnectionName, federationName), control);
      management.registerInRegistry(getFederationSourceResourceName(federationName), control);
   }

   /**
    * Unregister the given {@link AMQPFederationSource} instance with the broker management services.
    *
    * @param federation
    *    The federation source instance being unregistered from management.
    *
    * @throws Exception if an error occurs while unregistering the federation with the management services.
    */
   public static void unregisterFederationManagement(AMQPFederationSource federation) throws Exception {
      final String federationName = federation.getName();
      final String brokerConnectionName = federation.getBrokerConnection().getName();
      final ActiveMQServer server = federation.getServer();
      final ManagementService management = server.getManagementService();

      management.unregisterFromJMX(getFederationSourceObjectName(management, brokerConnectionName, federationName));
      management.unregisterFromRegistry(getFederationSourceResourceName(federationName));
   }

   public static String getFederationSourceResourceName(String federationName) {
      return String.format(FEDERATION_SOURCE_RESOURCE_TEMPLATE, federationName);
   }

   public static ObjectName getFederationSourceObjectName(ManagementService management, String brokerConnection, String federationName) throws Exception {
      final String brokerConnectionName = management.getObjectNameBuilder().getBrokerConnectionBaseObjectNameString(brokerConnection);

      return ObjectName.getInstance(
         String.format("%s," + FEDERATION_NAME_TEMPLATE,
            brokerConnectionName,
            ObjectName.quote(federationName)));
   }

   // APIs for federation address and queue policy management

   /**
    * Register a local federation policy manager with the server management services.
    *
    * @param brokerConnectionName
    *    The name of the broker connection that owns the manager being registered.
    * @param manager
    *    The AMQP federation policy manager instance that is being managed.
    *
    * @throws Exception if an error occurs while registering the manager with the management services.
    */
   public static void registerLocalPolicyManagement(String brokerConnectionName, AMQPFederationLocalPolicyManager manager) throws Exception {
      final AMQPFederation federation = manager.getFederation();
      final ActiveMQServer server = federation.getServer();
      final ManagementService management = server.getManagementService();
      final AMQPFederationLocalPolicyControlType control = new AMQPFederationLocalPolicyControlType(manager);
      final String federationName = federation.getName();
      final String policyName = manager.getPolicyName();

      management.registerInJMX(getFederationPolicyObjectName(management, brokerConnectionName, federationName, manager.getPolicyType().toString(), policyName), control);
      management.registerInRegistry(getFederationPolicyResourceName(federationName, policyName), control);
   }

   /**
    * Unregister a local federation policy manager with the server management services.
    *
    * @param brokerConnectionName
    *    The name of the broker connection that owns the manager being unregistered.
    * @param manager
    *    The AMQP federation policy manager instance that is being managed.
    *
    * @throws Exception if an error occurs while unregistering the manager with the management services.
    */
   public static void unregisterLocalPolicyManagement(String brokerConnectionName, AMQPFederationLocalPolicyManager manager) throws Exception {
      final AMQPFederation federation = manager.getFederation();
      final ActiveMQServer server = federation.getServer();
      final ManagementService management = server.getManagementService();
      final String federationName = federation.getName();
      final String policyName = manager.getPolicyName();

      management.unregisterFromJMX(getFederationPolicyObjectName(management, brokerConnectionName, federationName, manager.getPolicyType().toString(), policyName));
      management.unregisterFromRegistry(getFederationPolicyResourceName(federationName, policyName));
   }

   /**
    * Register a remote federation policy manager with the server management services.
    *
    * @param brokerConnectionName
    *    The name of the broker connection that owns the manager being registered.
    * @param manager
    *    The AMQP federation policy manager instance that is being managed.
    *
    * @throws Exception if an error occurs while registering the manager with the management services.
    */
   public static void registerRemotePolicyManagement(String brokerConnectionName, AMQPFederationRemotePolicyManager manager) throws Exception {
      final AMQPFederation federation = manager.getFederation();
      final ActiveMQServer server = federation.getServer();
      final ManagementService management = server.getManagementService();
      final AMQPFederationRemotePolicyControl control = new AMQPFederationRemotePolicyControlType(server, manager);
      final String federationName = federation.getName();
      final String policyName = manager.getPolicyName();

      management.registerInJMX(getFederationPolicyObjectName(management, brokerConnectionName, federationName, manager.getPolicyType().toString(), policyName), control);
      management.registerInRegistry(getFederationPolicyResourceName(federationName, policyName), control);
   }

   /**
    * Unregister a remote federation policy manager with the server management services.
    *
    * @param brokerConnectionName
    *    The name of the broker connection that owns the manager being unregistered.
    * @param manager
    *    The AMQP federation policy manager instance that is being managed.
    *
    * @throws Exception if an error occurs while unregistering the manager with the management services.
    */
   public static void unregisterRemotePolicyManagement(String brokerConnectionName, AMQPFederationRemotePolicyManager manager) throws Exception {
      final AMQPFederation federation = manager.getFederation();
      final ActiveMQServer server = federation.getServer();
      final ManagementService management = server.getManagementService();
      final String federationName = federation.getName();
      final String policyName = manager.getPolicyName();

      management.unregisterFromJMX(getFederationPolicyObjectName(management, brokerConnectionName, federationName, manager.getPolicyType().toString(), policyName));
      management.unregisterFromRegistry(getFederationPolicyResourceName(federationName, policyName));
   }

   public static String getFederationPolicyResourceName(String federationName, String policyName) {
      return String.format(FEDERATION_POLICY_RESOURCE_TEMPLATE, federationName, policyName);
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

   // APIs for federation consumer and producer management

   /**
    * Registers the federation consumer with the server management services.
    *
    * @param brokerConnectionName
    *    The name of the broker connection that owns the consumer being registered.
    * @param consumer
    *    The AMQP federation consumer instance that is being managed.
    *
    * @throws Exception if an error occurs while registering the consumer with the management services.
    */
   public static void registerFederationConsumerManagement(String brokerConnectionName, AMQPFederationConsumer consumer) throws Exception {
      final AMQPFederationPolicyManager manager = consumer.getPolicyManager();
      final AMQPFederation federation = manager.getFederation();
      final ActiveMQServer server = federation.getServer();
      final ManagementService management = server.getManagementService();
      final AMQPFederationConsumerControlType control = new AMQPFederationConsumerControlType(consumer);
      final String federationName = federation.getName();
      final String policyName = manager.getPolicyName();

      if (consumer.getRole() == FederationConsumerInfo.Role.ADDRESS_CONSUMER) {
         management.registerInJMX(getFederationAddressConsumerObjectName(management, brokerConnectionName, federationName, manager.getPolicyType().toString(), policyName, consumer.getConsumerInfo().getAddress()), control);
         management.registerInRegistry(getFederationAddressConsumerResourceName(federationName, policyName, consumer.getConsumerInfo().getAddress()), control);
      } else {
         management.registerInJMX(getFederationQueueConsumerObjectName(management, brokerConnectionName, federationName, manager.getPolicyType().toString(), policyName, consumer.getConsumerInfo().getFqqn()), control);
         management.registerInRegistry(getFederationQueueConsumerResourceName(federationName, policyName, consumer.getConsumerInfo().getFqqn()), control);
      }
   }

   /**
    * Unregisters the federation consumer with the server management services.
    *
    * @param brokerConnectionName
    *    The name of the broker connection that owns the consumer being registered.
    * @param consumer
    *    The AMQP federation consumer instance that is being managed.
    *
    * @throws Exception if an error occurs while registering the consumer with the management services.
    */
   public static void unregisterFederationConsumerManagement(String brokerConnectionName, AMQPFederationConsumer consumer) throws Exception {
      final AMQPFederationPolicyManager manager = consumer.getPolicyManager();
      final AMQPFederation federation = manager.getFederation();
      final ActiveMQServer server = federation.getServer();
      final ManagementService management = server.getManagementService();
      final String federationName = federation.getName();
      final String policyName = manager.getPolicyName();

      if (consumer.getRole() == FederationConsumerInfo.Role.ADDRESS_CONSUMER) {
         management.unregisterFromJMX(getFederationAddressConsumerObjectName(management, brokerConnectionName, federationName, manager.getPolicyType().toString(), policyName, consumer.getConsumerInfo().getAddress()));
         management.unregisterFromRegistry(getFederationAddressConsumerResourceName(federationName, policyName, consumer.getConsumerInfo().getAddress()));
      } else {
         management.unregisterFromJMX(getFederationQueueConsumerObjectName(management, brokerConnectionName, federationName, manager.getPolicyType().toString(), policyName, consumer.getConsumerInfo().getFqqn()));
         management.unregisterFromRegistry(getFederationQueueConsumerResourceName(federationName, policyName, consumer.getConsumerInfo().getFqqn()));
      }
   }

   /**
    * Registers the federation producer with the server management services.
    *
    * @param brokerConnectionName
    *    The name of the broker connection that owns the producer being registered.
    * @param sender
    *    The AMQP federation sender controller that manages the federation producer.
    *
    * @throws Exception if an error occurs while registering the producer with the management services.
    */
   public static void registerFederationProducerManagement(String brokerConnectionName, AMQPFederationSenderController sender) throws Exception {
      final AMQPFederationPolicyManager manager = sender.getPolicyManager();
      final AMQPFederation federation = manager.getFederation();
      final ActiveMQServer server = federation.getServer();
      final ManagementService management = server.getManagementService();
      final AMQPFederationProducerControlType control = new AMQPFederationProducerControlType(sender);
      final String federationName = federation.getName();
      final String policyName = manager.getPolicyName();

      if (sender.getRole() == Role.ADDRESS_PRODUCER) {
         final String address = control.getAddress();

         management.registerInJMX(getFederationAddressProducerObjectName(management, brokerConnectionName, federationName, manager.getPolicyType().toString(), policyName, address), control);
         management.registerInRegistry(getFederationAddressProducerResourceName(federationName, policyName, address), control);
      } else {
         final String fqqn = control.getFqqn();

         management.registerInJMX(getFederationQueueProducerObjectName(management, brokerConnectionName, federationName, manager.getPolicyType().toString(), policyName, fqqn), control);
         management.registerInRegistry(getFederationQueueProducerResourceName(federationName, policyName, fqqn), control);
      }
   }

   /**
    * Unregisters the federation producer with the server management services.
    *
    * @param brokerConnectionName
    *    The name of the broker connection that owns the producer being registered.
    * @param sender
    *    The AMQP federation sender controller that manages the federation producer.
    *
    * @throws Exception if an error occurs while registering the producer with the management services.
    */
   public static void unregisterFederationProducerManagement(String brokerConnectionName, AMQPFederationSenderController sender) throws Exception {
      final AMQPFederationPolicyManager manager = sender.getPolicyManager();
      final AMQPFederation federation = manager.getFederation();
      final ActiveMQServer server = federation.getServer();
      final ManagementService management = server.getManagementService();
      final String federationName = federation.getName();
      final String policyName = manager.getPolicyName();

      if (sender.getRole() == Role.ADDRESS_PRODUCER) {
         final String address = sender.getServerConsumer().getQueueAddress().toString();

         management.unregisterFromJMX(getFederationAddressProducerObjectName(management, brokerConnectionName, federationName, manager.getPolicyType().toString(), policyName, address));
         management.unregisterFromRegistry(getFederationAddressProducerResourceName(federationName, policyName, address));
      } else {
         final String fqqn = CompositeAddress.toFullyQualified(sender.getServerConsumer().getQueueAddress().toString(), sender.getServerConsumer().getQueueName().toString());

         management.unregisterFromJMX(getFederationQueueProducerObjectName(management, brokerConnectionName, federationName, manager.getPolicyType().toString(), policyName, fqqn));
         management.unregisterFromRegistry(getFederationQueueProducerResourceName(federationName, policyName, fqqn));
      }
   }

   public static String getFederationAddressConsumerResourceName(String federationName, String policyName, String address) {
      return String.format(FEDERATION_CONSUMER_RESOURCE_TEMPLATE, federationName, policyName, address);
   }

   public static String getFederationQueueConsumerResourceName(String federationName, String policyName, String fqqn) {
      return String.format(FEDERATION_CONSUMER_RESOURCE_TEMPLATE, federationName, policyName, fqqn);
   }

   public static String getFederationAddressProducerResourceName(String federationName, String policyName, String address) {
      return String.format(FEDERATION_PRODUCER_RESOURCE_TEMPLATE, federationName, policyName, address);
   }

   public static String getFederationQueueProducerResourceName(String federationName, String policyName, String fqqn) {
      return String.format(FEDERATION_PRODUCER_RESOURCE_TEMPLATE, federationName, policyName, fqqn);
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

   public static ObjectName getFederationAddressProducerObjectName(ManagementService management, String brokerConnection, String federationName, String policyType, String policyName, String address) throws Exception {
      final String brokerConnectionName = management.getObjectNameBuilder().getBrokerConnectionBaseObjectNameString(brokerConnection);

      return ObjectName.getInstance(
         String.format("%s," + FEDERATION_ADDRESS_PRODUCER_NAME_TEMPLATE,
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

   public static ObjectName getFederationQueueProducerObjectName(ManagementService management, String brokerConnection, String federationName, String policyType, String policyName, String fqqn) throws Exception {
      final String brokerConnectionName = management.getObjectNameBuilder().getBrokerConnectionBaseObjectNameString(brokerConnection);

      return ObjectName.getInstance(
         String.format("%s," + FEDERATION_QUEUE_PRODUCER_NAME_TEMPLATE,
            brokerConnectionName,
            ObjectName.quote(federationName),
            ObjectName.quote(policyType),
            ObjectName.quote(policyName),
            ObjectName.quote(fqqn)));
   }
}
