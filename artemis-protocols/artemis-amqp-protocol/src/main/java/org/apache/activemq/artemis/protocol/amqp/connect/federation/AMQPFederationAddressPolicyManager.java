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
   protected final Map<String, AMQPFederationAddressEntry> demandTracking = new HashMap<>();
   protected final Map<DivertBinding, Set<QueueBinding>> divertsTracking = new HashMap<>();

   public AMQPFederationAddressPolicyManager(AMQPFederation federation, AMQPFederationMetrics metrics, FederationReceiveFromAddressPolicy addressPolicy) throws ActiveMQException {
      super(federation, metrics, addressPolicy);

      Objects.requireNonNull(addressPolicy, "The Address match policy cannot be null");

      this.policy = addressPolicy;
      this.remoteQueueFilter = generateAddressFilter(policy.getMaxHops());
   }

   /**
    * @return the receive from address policy that backs the address policy manager.
    */
   @Override
   public FederationReceiveFromAddressPolicy getPolicy() {
      return policy;
   }

   @Override
   protected void safeCleanupConsumerDemandTracking(boolean force) {
      try {
         demandTracking.values().forEach((entry) -> {
            if (entry != null) {
               if (isConnected() && !force) {
                  tryStopFederationConsumer(entry.removeAllDemand());
               } else {
                  tryCloseFederationConsumer(entry.removeAllDemand().clearConsumer());
               }
            }
         });
      } finally {
         demandTracking.clear();
         divertsTracking.clear();
      }
   }

   @Override
   public synchronized void afterRemoveAddress(SimpleString address, AddressInfo addressInfo) throws ActiveMQException {
      if (isActive()) {
         final AMQPFederationAddressEntry entry = demandTracking.remove(address.toString());

         if (entry != null && entry.removeAllDemand().hasConsumer()) {
            logger.trace("Federated address {} was removed, closing federation consumer", address);

            // Demand is gone because the Address is gone and any in-flight messages can be
            // allowed to be released back to the remote as they will not be processed.
            // We removed the consumer information from demand tracking to prevent build up
            // of data for entries that may never return and to prevent interference from the
            // next set of events which will be the close of all local consumers for this now
            // removed Address.
            tryCloseFederationConsumer(entry.clearConsumer());
         }
      }
   }

   @Override
   public synchronized void afterRemoveBinding(Binding binding, Transaction tx, boolean deleteData) throws ActiveMQException {
      if (isActive()) {
         if (binding instanceof QueueBinding) {
            final AMQPFederationAddressEntry entry = demandTracking.get(binding.getAddress().toString());

            logger.trace("Federated address {} binding was removed, stopping federation consumer", binding.getAddress());

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
                        tryRemoveDemandOnAddress(demandTracking.get(sourceAddress), divertEntry.getKey());
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
                  tryRemoveDemandOnAddress(demandTracking.get(divert.getAddress().toString()), divert);
               } catch (Exception e) {
                  ActiveMQServerLogger.LOGGER.federationBindingsLookupError(divert.getDivert().getForwardAddress(), e);
               }
            }
         }
      }
   }

   private void tryRemoveDemandOnAddress(AMQPFederationAddressEntry entry, Binding binding) {
      if (entry != null) {
         entry.removeDemand(binding);

         logger.trace("Reducing demand on federated address {}, remaining demand? {}", entry.getAddress(), entry.hasDemand());

         if (!entry.hasDemand() && entry.hasConsumer()) {
            // A started consumer should be allowed to stop before possible close either because demand
            // is still not present or the remote did not respond before the configured stop timeout elapsed.
            // A successfully stopped receiver can be restarted but if the stop times out the receiver should
            // be closed and a new receiver created if demand is present. The completions occur on the connection
            // thread which requires the handler method to use synchronized to ensure thread safety.
            tryStopFederationConsumer(entry);
         }
      }
   }

   private void tryStopFederationConsumer(AMQPFederationAddressEntry entry) {
      if (entry.hasConsumer()) {
         entry.getConsumer().stopAsync(new AMQPFederationAsyncCompletion<AMQPFederationConsumer>() {

            @Override
            public void onComplete(AMQPFederationConsumer context) {
               handleFederationConsumerStopped(entry, true);
            }

            @Override
            public void onException(AMQPFederationConsumer context, Exception error) {
               logger.trace("Stop of federation consumer {} failed, closing consumer: ", context, error);
               handleFederationConsumerStopped(entry, false);
            }
         });
      }
   }

   private synchronized void handleFederationConsumerStopped(AMQPFederationAddressEntry entry, boolean didStop) {
      final AMQPFederationConsumer federationConsuner = entry.getConsumer();

      // Remote close or local address remove could have beaten us here and already cleaned up the consumer.
      if (federationConsuner != null) {
         // If the consumer has no demand or it didn't stop in time or some other error occurred we
         // assume the worst and close it here, the follow on code will recreate or cleanup as needed.
         if (!didStop || !entry.hasDemand()) {
            tryCloseFederationConsumer(entry.clearConsumer());
         }

         // Demand may have returned while the consumer was stopping in which case
         // we either restart an existing stopped consumer or recreate if the stop
         // timed out and we closed it above. If there's still no demand then we
         // should remove it from demand tracking to reduce resource consumption.
         if (isActive() && entry.hasDemand()) {
            tryRestartFederationConsumerForAddress(entry);
         } else {
            demandTracking.remove(entry.getAddress());
         }
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
    * Called under lock this method should check if the given {@link Binding} matches the
    * configured address federation policy and federate the address if so.  The incoming
    * {@link Binding} can be either a {@link QueueBinding} or a {@link DivertBinding} so
    * the code should check both.
    *
    * @param binding
    *       The binding that should be checked against the federated address policy,
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
      final AMQPFederationAddressEntry entry;

      // Check for existing consumer add demand from a additional local consumer to ensure
      // the remote consumer remains active until all local demand is withdrawn.
      if (demandTracking.containsKey(addressName)) {
         entry = demandTracking.get(addressName);
      } else {
         entry = new AMQPFederationAddressEntry(addressInfo);
         demandTracking.put(addressName, entry);
      }

      // Demand passed all binding plugin blocking checks so we track it, plugin can still
      // stop federation of the address based on some external criteria but once it does
      // (if ever) allow it we will have tracked all allowed demand.
      entry.addDemand(binding);

      // This will create a new consumer only if there isn't one currently assigned to the entry
      // and any configured federation plugins don't block it from doing so.
      tryCreateFederationConsumerForAddress(entry);
   }

   private void tryCreateFederationConsumerForAddress(AMQPFederationAddressEntry entry) {
      if (entry.hasDemand()) {
         final AddressInfo addressInfo = entry.getAddressInfo();

         if (!entry.hasConsumer() && !isPluginBlockingFederationConsumerCreate(addressInfo)) {
            logger.trace("Federation Address Policy manager creating remote consumer for address: {}", addressInfo);

            final FederationConsumerInfo consumerInfo = createConsumerInfo(addressInfo);
            final AMQPFederationConsumer addressConsumer = createFederationConsumer(consumerInfo);

            signalPluginBeforeCreateFederationConsumer(consumerInfo);

            // Handle remote close with remove of consumer which means that future demand will
            // attempt to create a new consumer for that demand. Ensure that thread safety is
            // accounted for here as the notification can be asynchronous.
            addressConsumer.setRemoteClosedHandler((closedConsumer) -> {
               synchronized (AMQPFederationAddressPolicyManager.this) {
                  try {
                     final AMQPFederationAddressEntry tracked = demandTracking.get(closedConsumer.getConsumerInfo().getAddress());

                     if (tracked != null) {
                        tracked.clearConsumer();
                     }
                  } finally {
                     closedConsumer.close();
                  }
               }
            });

            entry.setConsumer(addressConsumer);

            // Now that we are tracking it we can initialize it which will start it once
            // the link has fully attached.
            addressConsumer.initialize();

            signalPluginAfterCreateFederationConsumer(addressConsumer);
         }
      }
   }

   private void tryRestartFederationConsumerForAddress(AMQPFederationAddressEntry entry) {
      // There might be a consumer that was previously stopped due to demand having been
      // removed in which case we can attempt to recover it with a simple restart but if
      // that fails ensure the old consumer is closed and then attempt to recreate as we
      // know there is demand currently.
      if (entry.hasConsumer()) {
         final AMQPFederationConsumer federationConsuner = entry.getConsumer();

         try {
            federationConsuner.startAsync(new AMQPFederationAsyncCompletion<AMQPFederationConsumer>() {

               @Override
               public void onComplete(AMQPFederationConsumer context) {
                  logger.trace("Restarted federation consumer after new demand added.");
               }

               @Override
               public void onException(AMQPFederationConsumer context, Exception error) {
                  if (error instanceof IllegalStateException) {
                     // The receiver might be stopping or it could be closed, either of which
                     // was initiated from this manager so we can ignore and let those complete.
                     return;
                  } else {
                     // This is unexpected and our reaction is to close the consumer since we
                     // have no idea what its state is now. Later new demand or remote events
                     // will trigger a new consumer to get added.
                     logger.trace("Start of federation consumer {} threw unexpected error, closing consumer: ", context, error);
                     tryCloseFederationConsumer(entry.clearConsumer());
                  }
               }
            });
         } catch (Exception ex) {
            // The consumer might have been remotely closed, we can't be certain but since we
            // are responding to demand having been added we will close it and clear the entry
            // so that the follow on code can try and create a new one.
            logger.trace("Caught error on attempted restart of existing federation consumer", ex);
            tryCloseFederationConsumer(entry.clearConsumer());
            tryCreateFederationConsumerForAddress(entry);
         }
      } else {
         // The consumer was likely closed because it didn't stop in time, create a new one and
         // let the normal setup process start federation again.
         tryCreateFederationConsumerForAddress(entry);
      }
   }

   /**
    * Checks if the remote address added falls within the set of addresses that match the
    * configured address policy and if so scans for local demand on that address to see
    * if a new attempt to federate the address is needed.
    *
    * @param addressName
    *    The address that was added on the remote.
    *
    * @throws Exception if an error occurs while processing the address added event.
    */
   synchronized void afterRemoteAddressAdded(String addressName) throws Exception {
      // Assume that the remote address that matched a previous federation attempt is MULTICAST
      // so that we retry if current local state matches the policy and if it isn't we will once
      // again record the federation attempt with the remote and be updated if the remote removes
      // and adds the address again (hopefully with the correct routing type). We retrain all the
      // current demand and don't need to re-check the server state before trying to create the
      // remote address consumer.
      if (isActive() && testIfAddressMatchesPolicy(addressName, RoutingType.MULTICAST) && demandTracking.containsKey(addressName)) {
         tryCreateFederationConsumerForAddress(demandTracking.get(addressName));
      }
   }

   /**
    * Performs the test against the configured address policy to check if the target
    * address is a match or not. A subclass can override this method and provide its
    * own match tests in combination with the configured matching policy.
    *
    * @param addressInfo
    *    The address that is being tested for a policy match.
    *
    * @return <code>true</code> if the address given is a match against the policy.
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
    * Performs the test against the configured address policy to check if the target
    * address is a match or not. A subclass can override this method and provide its
    * own match tests in combination with the configured matching policy.
    *
    * @param address
    *    The address that is being tested for a policy match.
    * @param type
    *    The routing type of the address to test against the policy.
    *
    * @return <code>true</code> if the address given is a match against the policy.
    */
   private boolean testIfAddressMatchesPolicy(String address, RoutingType type) {
      return policy.test(address, type);
   }

   /**
    * Create a new {@link FederationConsumerInfo} based on the given {@link AddressInfo}
    * and the configured {@link FederationReceiveFromAddressPolicy}. A subclass must override this
    * method to return a consumer information object with the data used be that implementation.
    *
    * @param address
    *    The {@link AddressInfo} to use as a basis for the consumer information object.
    *
    * @return a new {@link FederationConsumerInfo} instance based on the given address.
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
}
