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

import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationPolicySupport.generateAddressFilter;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.QueueBinding;
import org.apache.activemq.artemis.core.postoffice.impl.DivertBinding;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerAddressPlugin;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.protocol.amqp.federation.FederationConsumerInfo;
import org.apache.activemq.artemis.protocol.amqp.federation.FederationReceiveFromAddressPolicy;
import org.apache.activemq.artemis.protocol.amqp.federation.FederationConsumerInfo.Role;
import org.apache.activemq.artemis.utils.CompositeAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The AMQP Federation implementation of an federation address policy manager.
 */
public final class AMQPFederationAddressPolicyManager extends AMQPFederationLocalPolicyManager implements ActiveMQServerAddressPlugin {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   protected final String remoteQueueFilter;
   protected final FederationReceiveFromAddressPolicy policy;
   protected final Map<String, AMQPFederationAddressConsumerManager> federationConsumers = new HashMap<>();
   protected final Map<DivertBinding, Set<QueueBinding>> divertsTracking = new HashMap<>();

   public AMQPFederationAddressPolicyManager(AMQPFederation federation, AMQPFederationMetrics metrics, FederationReceiveFromAddressPolicy addressPolicy) throws ActiveMQException {
      super(federation, metrics, addressPolicy);

      Objects.requireNonNull(addressPolicy, "The Address match policy cannot be null");

      this.policy = addressPolicy;
      this.remoteQueueFilter = generateAddressFilter(policy.getMaxHops());
   }

   /**
    * @return the receive from address policy that backs the address policy manager
    */
   @Override
   public FederationReceiveFromAddressPolicy getPolicy() {
      return policy;
   }

   @Override
   protected void safeCleanupManagerResources(boolean force) {
      try {
         federationConsumers.values().forEach((entry) -> {
            if (entry != null) {
               if (isConnected() && !force) {
                  entry.shutdown();
               } else {
                  entry.shutdownNow();
               }
            }
         });
      } finally {
         federationConsumers.clear();
         divertsTracking.clear();
      }
   }

   @Override
   public synchronized void afterRemoveAddress(SimpleString address, AddressInfo addressInfo) throws ActiveMQException {
      if (isActive()) {
         final AMQPFederationConsumerManager entry = federationConsumers.remove(address.toString());

         if (entry != null) {
            logger.trace("Federated address {} was removed, closing federation consumer", address);

            // Demand is gone because the Address is gone and any in-flight messages can be
            // allowed to be released back to the remote as they will not be processed.
            // We removed the tracking information from matched address data to prevent build up
            // of data for entries that may never return and to prevent interference from the
            // next set of events which will be the close of all local consumers for this now
            // removed Address.
            entry.shutdownNow();
         }
      }
   }

   @Override
   public synchronized void afterRemoveBinding(Binding binding, Transaction tx, boolean deleteData) throws ActiveMQException {
      if (isActive()) {
         if (binding instanceof QueueBinding) {
            final AMQPFederationAddressConsumerManager entry = federationConsumers.get(binding.getAddress().toString());

            logger.trace("Federated address {} binding was removed, reducing demand.", binding.getAddress());

            if (entry != null) {
               // This is QueueBinding that was mapped to a federated address so we can directly remove
               // demand from the federation consumer and close it if demand is now gone.
               tryRemoveDemandOnAddress(entry, binding);
            } else if (policy.isEnableDivertBindings()) {
               // See if there is any matching diverts that are forwarding to an address where this QueueBinding
               // is bound and remove the mapping for any matches, diverts can have a composite set of address
               // forwards so each divert must be checked in turn to see if it contains the address the removed
               // binding was bound to.
               divertsTracking.entrySet().forEach(divertEntry -> {
                  final String sourceAddress = divertEntry.getKey().getAddress().toString();
                  final SimpleString forwardAddress = divertEntry.getKey().getDivert().getForwardAddress();

                  if (isAddressInDivertForwards(binding.getAddress(), forwardAddress)) {
                     // Try and remove the queue binding from the set of registered bindings
                     // for the divert and if that removes all mapped bindings then we can
                     // remove the divert from the federated address entry and check if that
                     // removed all local demand which means we can close the consumer.
                     divertEntry.getValue().remove(binding);

                     if (divertEntry.getValue().isEmpty()) {
                        tryRemoveDemandOnAddress(federationConsumers.get(sourceAddress), divertEntry.getKey());
                     }
                  }
               });
            }
         } else if (policy.isEnableDivertBindings() && binding instanceof DivertBinding divert) {

            if (divertsTracking.remove(divert) != null) {
               // The divert binding is treated as one unit of demand on a federated address and
               // when the divert is removed that unit of demand is removed regardless of existing
               // bindings still remaining on the divert forwards. If the divert demand was the
               // only thing keeping the federated address consumer open this will result in it
               // being closed.
               try {
                  tryRemoveDemandOnAddress(federationConsumers.get(divert.getAddress().toString()), divert);
               } catch (Exception e) {
                  ActiveMQServerLogger.LOGGER.federationBindingsLookupError(divert.getDivert().getForwardAddress(), e);
               }
            }
         }
      }
   }

   private void tryRemoveDemandOnAddress(AMQPFederationAddressConsumerManager entry, Binding binding) {
      if (entry != null) {
         logger.trace("Reducing demand on federated address {}", entry.getAddress());
         entry.removeDemand(binding);
      }
   }

   @Override
   protected void scanAllBindings() {
      server.getPostOffice()
            .getAllBindings()
            .filter(binding -> binding instanceof QueueBinding || (policy.isEnableDivertBindings() && binding instanceof DivertBinding))
            .forEach(binding -> afterAddBinding(binding));
   }

   @Override
   public synchronized void afterAddAddress(AddressInfo addressInfo, boolean reload) {
      if (isActive() && policy.isEnableDivertBindings() && policy.test(addressInfo)) {
         try {
            // A Divert can exist in configuration prior to the address having been auto created
            // etc so upon address add this check needs to be run to capture addresses that now
            // match the divert.
            server.getPostOffice()
                  .getDirectBindings(addressInfo.getName())
                  .stream()
                  .filter(binding -> binding instanceof DivertBinding)
                  .forEach(this::checkBindingForMatch);
         } catch (Exception e) {
            ActiveMQServerLogger.LOGGER.federationBindingsLookupError(addressInfo.getName(), e);
         }
      }
   }

   @Override
   public synchronized void afterAddBinding(Binding binding) {
      if (isActive()) {
         checkBindingForMatch(binding);
      }
   }

   /**
    * Called under lock this method should check if the given {@link Binding} matches the configured address federation
    * policy and federate the address if so.  The incoming {@link Binding} can be either a {@link QueueBinding} or a
    * {@link DivertBinding} so the code should check both.
    *
    * @param binding The binding that should be checked against the federated address policy,
    */
   private void checkBindingForMatch(Binding binding) {
      if (binding instanceof QueueBinding queueBinding) {
         final AddressInfo addressInfo = server.getPostOffice().getAddressInfo(binding.getAddress());

         if (testIfAddressMatchesPolicy(addressInfo)) {
            // A plugin can block address federation for a given queue and if another queue
            // binding does trigger address federation we don't want to track the rejected
            // queue as demand so we always run this check before trying to create the address
            // consumer.
            if (isPluginBlockingFederationConsumerCreate(queueBinding.getQueue())) {
               return;
            }

            createOrUpdateFederatedAddressConsumerForBinding(addressInfo, queueBinding);
         } else {
            reactIfQueueBindingMatchesAnyDivertTarget(queueBinding);
         }
      } else if (binding instanceof DivertBinding divertBinding) {
         reactIfAnyQueueBindingMatchesDivertTarget(divertBinding);
      }
   }

   private void reactIfAnyQueueBindingMatchesDivertTarget(DivertBinding divertBinding) {
      if (!policy.isEnableDivertBindings()) {
         return;
      }

      final AddressInfo addressInfo = server.getPostOffice().getAddressInfo(divertBinding.getAddress());

      if (!testIfAddressMatchesPolicy(addressInfo)) {
         return;
      }

      // We only need to check if we've never seen the divert before, afterwards we will
      // be checking it any time a new QueueBinding is added instead.
      if (divertsTracking.get(divertBinding) == null) {
         final Set<QueueBinding> matchingQueues = new HashSet<>();
         divertsTracking.put(divertBinding, matchingQueues);

         // We must account for the composite divert case by splitting the address and
         // getting the bindings on each one.
         final SimpleString forwardAddress = divertBinding.getDivert().getForwardAddress();
         final SimpleString[] forwardAddresses = forwardAddress.split(',');

         try {
            for (SimpleString forward : forwardAddresses) {
               server.getPostOffice().getBindingsForAddress(forward).getBindings()
                     .stream()
                     .filter(b -> b instanceof QueueBinding)
                     .map(b -> (QueueBinding) b)
                     .forEach(queueBinding -> {
                        // The plugin can block the demand totally here either based on the divert itself
                        // or the queue that's attached to the divert.
                        if (isPluginBlockingFederationConsumerCreate(divertBinding.getDivert(), queueBinding.getQueue())) {
                           return;
                        }

                        // The plugin can block the demand selectively based on a single queue attached to
                        // the divert target(s).
                        if (isPluginBlockingFederationConsumerCreate(queueBinding.getQueue())) {
                           return;
                        }

                        matchingQueues.add(queueBinding);

                        createOrUpdateFederatedAddressConsumerForBinding(addressInfo, divertBinding);
                     });
            }
         } catch (Exception e) {
            ActiveMQServerLogger.LOGGER.federationBindingsLookupError(forwardAddress, e);
         }
      }
   }

   private void reactIfQueueBindingMatchesAnyDivertTarget(QueueBinding queueBinding) {
      if (!policy.isEnableDivertBindings()) {
         return;
      }

      final SimpleString queueAddress = queueBinding.getAddress();

      divertsTracking.entrySet().forEach((e) -> {
         final SimpleString forwardAddress = e.getKey().getDivert().getForwardAddress();
         final DivertBinding divertBinding = e.getKey();

         // Check matched diverts to see if the QueueBinding address matches the address or
         // addresses (composite diverts) of the Divert and if so then we can check if we need
         // to create demand on the source address on the remote if we haven't done so already.

         if (!e.getValue().contains(queueBinding) && isAddressInDivertForwards(queueAddress, forwardAddress)) {
            // The plugin can block the demand totally here either based on the divert itself
            // or the queue that's attached to the divert.
            if (isPluginBlockingFederationConsumerCreate(divertBinding.getDivert(), queueBinding.getQueue())) {
               return;
            }

            // The plugin can block the demand selectively based on a single queue attached to
            // the divert target(s).
            if (isPluginBlockingFederationConsumerCreate(queueBinding.getQueue())) {
               return;
            }

            // Each divert that forwards to the address the queue is bound to we add demand
            // in the diverts tracker.
            e.getValue().add(queueBinding);

            final AddressInfo addressInfo = server.getPostOffice().getAddressInfo(divertBinding.getAddress());

            createOrUpdateFederatedAddressConsumerForBinding(addressInfo, divertBinding);
         }
      });
   }

   private void createOrUpdateFederatedAddressConsumerForBinding(AddressInfo addressInfo, Binding binding) {
      logger.trace("Federation Address Policy matched on for demand on address: {} : binding: {}", addressInfo, binding);

      final String addressName = addressInfo.getName().toString();
      final AMQPFederationAddressConsumerManager entry;

      // Check for existing consumer add demand from a additional local consumer to ensure
      // the remote consumer remains active until all local demand is withdrawn.
      if (federationConsumers.containsKey(addressName)) {
         entry = federationConsumers.get(addressName);
      } else {
         entry = new AMQPFederationAddressConsumerManager(this, addressInfo);
         federationConsumers.put(addressName, entry);
      }

      // Demand passed all binding plugin blocking checks so we track it, plugin can still
      // stop federation of the address based on some external criteria but once it does
      // (if ever) allow it we will have tracked all allowed demand.
      entry.addDemand(binding);
   }

   /**
    * Checks if the remote address added falls within the set of addresses that match the configured address policy and
    * if so scans for local demand on that address to see if a new attempt to federate the address is needed.
    *
    * @param addressName The address that was added on the remote.
    * @throws Exception if an error occurs while processing the address added event.
    */
   synchronized void afterRemoteAddressAdded(String addressName) throws Exception {
      // Assume that the remote address that matched a previous federation attempt is MULTICAST
      // so that we retry if current local state matches the policy and if it isn't we will once
      // again record the federation attempt with the remote and be updated if the remote removes
      // and adds the address again (hopefully with the correct routing type). We retrain all the
      // current demand and don't need to re-check the server state before trying to create the
      // remote address consumer.
      if (isActive() && testIfAddressMatchesPolicy(addressName, RoutingType.MULTICAST)) {
         final AMQPFederationConsumerManager entry = federationConsumers.get(addressName);

         if (entry != null) {
            entry.recover();
         }
      }
   }

   /**
    * Performs the test against the configured address policy to check if the target address is a match or not. A
    * subclass can override this method and provide its own match tests in combination with the configured matching
    * policy.
    *
    * @param addressInfo The address that is being tested for a policy match.
    * @return {@code true} if the address given is a match against the policy
    */
   private boolean testIfAddressMatchesPolicy(AddressInfo addressInfo) {
      if (!policy.test(addressInfo)) {
         return false;
      }

      // Address consumers can't pull as we have no real metric to indicate when / how much
      // we should pull so instead we refuse to match if credit set to zero.
      if (configuration.getReceiverCredits() <= 0) {
         logger.debug("Federation address policy rejecting match on {} because credit is set to zero:", addressInfo.getName());
         return false;
      } else {
         return true;
      }
   }

   /**
    * Performs the test against the configured address policy to check if the target address is a match or not. A
    * subclass can override this method and provide its own match tests in combination with the configured matching
    * policy.
    *
    * @param address The address that is being tested for a policy match.
    * @param type    The routing type of the address to test against the policy.
    * @return {@code true} if the address given is a match against the policy
    */
   private boolean testIfAddressMatchesPolicy(String address, RoutingType type) {
      return policy.test(address, type);
   }

   /**
    * Create a new {@link FederationConsumerInfo} based on the given {@link AddressInfo} and the configured
    * {@link FederationReceiveFromAddressPolicy}. A subclass must override this method to return a consumer information
    * object with the data used be that implementation.
    *
    * @param address The {@link AddressInfo} to use as a basis for the consumer information object.
    * @return a new {@link FederationConsumerInfo} instance based on the given address
    */
   private AMQPFederationGenericConsumerInfo createConsumerInfo(AddressInfo address) {
      final String addressName = address.getName().toString();
      final String generatedQueueName = generateQueueName(address);

      return new AMQPFederationGenericConsumerInfo(Role.ADDRESS_CONSUMER,
         addressName,
         generatedQueueName,
         address.getRoutingType(),
         remoteQueueFilter,
         CompositeAddress.toFullyQualified(addressName, generatedQueueName),
         ActiveMQDefaultConfiguration.getDefaultConsumerPriority());
   }

   @Override
   protected AMQPFederationConsumer createFederationConsumer(FederationConsumerInfo consumerInfo) {
      Objects.requireNonNull(consumerInfo, "Federation Address consumer information object was null");

      if (logger.isTraceEnabled()) {
         logger.trace("AMQP Federation {} creating address consumer: {} for policy: {}", federation.getName(), consumerInfo, policy.getPolicyName());
      }

      // Don't initiate anything yet as the caller might need to register error handlers etc
      // before the attach is sent otherwise they could miss the failure case.
      return new AMQPFederationAddressConsumer(this, configuration, session, consumerInfo, metrics.newConsumerMetrics());
   }

   private String generateQueueName(AddressInfo address) {
      return "federation." + federation.getName() + ".address." + address.getName() + ".node." + server.getNodeID();
   }

   private static boolean isAddressInDivertForwards(final SimpleString targetAddress, final SimpleString forwardAddress) {
      final SimpleString[] forwardAddresses = forwardAddress.split(',');

      for (SimpleString forward : forwardAddresses) {
         if (targetAddress.equals(forward)) {
            return true;
         }
      }

      return false;
   }

   private static class AMQPFederationAddressConsumerManager extends AMQPFederationConsumerManager {

      private final AMQPFederationAddressPolicyManager manager;
      private final AddressInfo addressInfo;

      AMQPFederationAddressConsumerManager(AMQPFederationAddressPolicyManager manager, AddressInfo addressInfo) {
         super(manager);

         this.manager = manager;
         this.addressInfo = addressInfo;
      }

      /**
       * {@return the address information that this entry is acting to federate}
       */
      public AddressInfo getAddressInfo() {
         return addressInfo;
      }

      /**
       * {@return the address that this entry is acting to federate}
       */
      public String getAddress() {
         return getAddressInfo().getName().toString();
      }

      @Override
      protected AMQPFederationConsumer createFederationConsumer() {
         return manager.createFederationConsumer(manager.createConsumerInfo(addressInfo));
      }

      @Override
      protected boolean isPluginBlockingFederationConsumerCreate() {
         return manager.isPluginBlockingFederationConsumerCreate(addressInfo);
      }
   }
}
