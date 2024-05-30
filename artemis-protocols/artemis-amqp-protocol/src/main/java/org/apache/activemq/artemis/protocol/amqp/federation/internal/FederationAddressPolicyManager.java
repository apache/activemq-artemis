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

package org.apache.activemq.artemis.protocol.amqp.federation.internal;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.QueueBinding;
import org.apache.activemq.artemis.core.postoffice.impl.DivertBinding;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.core.server.Divert;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.federation.Federation;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerAddressPlugin;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerBindingPlugin;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.protocol.amqp.federation.FederationConsumer;
import org.apache.activemq.artemis.protocol.amqp.federation.FederationConsumerInfo;
import org.apache.activemq.artemis.protocol.amqp.federation.FederationReceiveFromAddressPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manager for a federation which has address federation configuration which requires
 * monitoring broker addresses and diverts for demand and creating a consumer on the
 * remote side to federate messages back to this peer.
 *
 * Address federation replicates messages from the remote broker's address to an address
 * on this broker but only when there is local demand on that address. If there is no
 * local demand then federation if already established is halted. The manager creates
 * a remote consumer on the federated address without any filtering other than that
 * required for internal functionality in order to allow for a single remote consumer
 * which can federate all messages to the local side where the existing queues can apply
 * any filtering they have in place.
 */
public abstract class FederationAddressPolicyManager implements ActiveMQServerBindingPlugin, ActiveMQServerAddressPlugin {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   protected final ActiveMQServer server;
   protected final FederationInternal federation;
   protected final FederationReceiveFromAddressPolicy policy;
   protected final Map<String, FederationAddressEntry> demandTracking = new HashMap<>();
   protected final Map<DivertBinding, Set<QueueBinding>> divertsTracking = new HashMap<>();

   private volatile boolean started;

   public FederationAddressPolicyManager(FederationInternal federation, FederationReceiveFromAddressPolicy addressPolicy) throws ActiveMQException {
      Objects.requireNonNull(federation, "The Federation instance cannot be null");
      Objects.requireNonNull(addressPolicy, "The Address match policy cannot be null");

      this.federation = federation;
      this.policy = addressPolicy;
      this.server = federation.getServer();
   }

   /**
    * Start the address policy manager which will initiate a scan of all broker divert
    * bindings and create and matching remote receivers. Start on a policy manager
    * should only be called after its parent {@link Federation} is started and the
    * federation connection has been established.
    */
   public synchronized void start() {
      if (!started) {
         started = true;
         handlePolicyManagerStarted(policy);
         server.registerBrokerPlugin(this);
         scanAllBindings(); // Create remote consumers for existing addresses with demand.
      }
   }

   /**
    * Stops the address policy manager which will close any open remote receivers that are
    * active for local queue demand. Stop should generally be called whenever the parent
    * {@link Federation} loses its connection to the remote.
    */
   public synchronized void stop() {
      if (started) {
         started = false;
         server.unRegisterBrokerPlugin(this);
         demandTracking.forEach((k, v) -> {
            if (v.hasConsumer()) {
               v.getConsumer().close();
            }
         });
         demandTracking.clear();
         divertsTracking.clear();
      }
   }

   @Override
   public synchronized void afterRemoveAddress(SimpleString address, AddressInfo addressInfo) throws ActiveMQException {
      if (started) {
         final FederationAddressEntry entry = demandTracking.remove(address.toString());

         if (entry != null && entry.hasConsumer()) {
            entry.getConsumer().close();
         }
      }
   }

   @Override
   public synchronized void afterRemoveBinding(Binding binding, Transaction tx, boolean deleteData) throws ActiveMQException {
      if (started) {
         if (binding instanceof QueueBinding) {
            final FederationAddressEntry entry = demandTracking.get(binding.getAddress().toString());

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
         } else if (policy.isEnableDivertBindings() && binding instanceof DivertBinding) {
            final DivertBinding divert = (DivertBinding) binding;

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

   protected final void tryRemoveDemandOnAddress(FederationAddressEntry entry, Binding binding) {
      if (entry != null) {
         entry.removeDemand(binding);

         logger.trace("Reducing demand on federated address {}, remaining demand? {}", entry.getAddress(), entry.hasDemand());

         if (!entry.hasDemand() && entry.hasConsumer()) {
            final FederationConsumerInternal federationConsuner = entry.getConsumer();

            try {
               signalBeforeCloseFederationConsumer(federationConsuner);
               federationConsuner.close();
               signalAfterCloseFederationConsumer(federationConsuner);
            } finally {
               demandTracking.remove(entry.getAddress());
            }
         }
      }
   }

   /**
    * Scans all bindings and push them through the normal bindings checks that
    * would be done on an add. We filter here based on whether diverts are enabled
    * just to reduce the result set but the check call should also filter as
    * during normal operations divert bindings could be added.
    */
   protected final void scanAllBindings() {
      server.getPostOffice()
            .getAllBindings()
            .filter(binding -> binding instanceof QueueBinding || (policy.isEnableDivertBindings() && binding instanceof DivertBinding))
            .forEach(binding -> afterAddBinding(binding));
   }

   @Override
   public synchronized void afterAddAddress(AddressInfo addressInfo, boolean reload) {
      if (started && policy.isEnableDivertBindings() && policy.test(addressInfo)) {
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
      if (started) {
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
   protected final void checkBindingForMatch(Binding binding) {
      if (binding instanceof QueueBinding) {
         final QueueBinding queueBinding = (QueueBinding) binding;
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
      } else if (binding instanceof DivertBinding) {
         reactIfAnyQueueBindingMatchesDivertTarget((DivertBinding) binding);
      }
   }

   protected final void reactIfAnyQueueBindingMatchesDivertTarget(DivertBinding divertBinding) {
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

   protected final void reactIfQueueBindingMatchesAnyDivertTarget(QueueBinding queueBinding) {
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

   private static boolean isAddressInDivertForwards(final SimpleString targetAddress, final SimpleString forwardAddress) {
      final SimpleString[] forwardAddresses = forwardAddress.split(',');

      for (SimpleString forward : forwardAddresses) {
         if (targetAddress.equals(forward)) {
            return true;
         }
      }

      return false;
   }

   protected final void createOrUpdateFederatedAddressConsumerForBinding(AddressInfo addressInfo, Binding binding) {
      logger.trace("Federation Address Policy matched on for demand on address: {} : binding: {}", addressInfo, binding);

      final String addressName = addressInfo.getName().toString();
      final FederationAddressEntry entry;

      // Check for existing consumer add demand from a additional local consumer to ensure
      // the remote consumer remains active until all local demand is withdrawn.
      if (demandTracking.containsKey(addressName)) {
         entry = demandTracking.get(addressName);
      } else {
         entry = new FederationAddressEntry(addressInfo);
         demandTracking.put(addressName, entry);
      }

      // Demand passed all binding plugin blocking checks so we track it, plugin can still
      // stop federation of the address based on some external criteria but once it does
      // (if ever) allow it we will have tracked all allowed demand.
      entry.addDemand(binding);

      tryCreateFederationConsumerForAddress(entry);
   }

   private void tryCreateFederationConsumerForAddress(FederationAddressEntry addressEntry) {
      final AddressInfo addressInfo = addressEntry.getAddressInfo();

      if (addressEntry.hasDemand() && !addressEntry.hasConsumer() && !isPluginBlockingFederationConsumerCreate(addressInfo)) {
         logger.trace("Federation Address Policy manager creating remote consumer for address: {}", addressInfo);

         final FederationConsumerInfo consumerInfo = createConsumerInfo(addressInfo);
         final FederationConsumerInternal addressConsumer = createFederationConsumer(consumerInfo);

         signalBeforeCreateFederationConsumer(consumerInfo);

         // Handle remote close with remove of consumer which means that future demand will
         // attempt to create a new consumer for that demand. Ensure that thread safety is
         // accounted for here as the notification can be asynchronous.
         addressConsumer.setRemoteClosedHandler((closedConsumer) -> {
            synchronized (this) {
               try {
                  final FederationAddressEntry tracked = demandTracking.get(closedConsumer.getConsumerInfo().getAddress());

                  if (tracked != null) {
                     tracked.clearConsumer();
                  }
               } finally {
                  closedConsumer.close();
               }
            }
         });

         addressEntry.setConsumer(addressConsumer);

         addressConsumer.start();

         signalAfterCreateFederationConsumer(addressConsumer);
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
   public synchronized void afterRemoteAddressAdded(String addressName) throws Exception {
      // Assume that the remote address that matched a previous federation attempt is MULTICAST
      // so that we retry if current local state matches the policy and if it isn't we will once
      // again record the federation attempt with the remote and be updated if the remote removes
      // and adds the address again (hopefully with the correct routing type). We retrain all the
      // current demand and don't need to re-check the server state before trying to create the
      // remote address consumer.
      if (started && testIfAddressMatchesPolicy(addressName, RoutingType.MULTICAST) && demandTracking.containsKey(addressName)) {
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
   protected boolean testIfAddressMatchesPolicy(AddressInfo addressInfo) {
      return policy.test(addressInfo);
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
   protected boolean testIfAddressMatchesPolicy(String address, RoutingType type) {
      return policy.test(address, type);
   }

   /**
    * Called on start of the manager before any other actions are taken to allow the subclass time
    * to configure itself and prepare any needed state prior to starting management of federated
    * resources.
    *
    * @param policy
    *    The policy configuration for this policy manager.
    */
   protected abstract void handlePolicyManagerStarted(FederationReceiveFromAddressPolicy policy);

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
   protected abstract FederationConsumerInfo createConsumerInfo(AddressInfo address);

   /**
    * Creates a {@link FederationAddressEntry} instance that will be used to store an instance of
    * an {@link FederationConsumer} along with other state data needed to manage a federation consumer
    * instance lifetime. A subclass can override this method to return a more customized entry type with
    * additional state data.
    *
    * @param addressInfo
    *    The address information that the created entry is meant to track demand for.
    *
    * @return a new {@link FederationAddressEntry} that tracks demand on an address.
    */
   protected FederationAddressEntry createConsumerEntry(AddressInfo addressInfo) {
      return new FederationAddressEntry(addressInfo);
   }

   /**
    * Create a new {@link FederationConsumerInternal} instance using the consumer information
    * given. This is called when local demand for a matched queue requires a new consumer to
    * be created. This method by default will call the configured consumer factory function that
    * was provided when the manager was created, a subclass can override this to perform additional
    * actions for the create operation.
    *
    * @param consumerInfo
    *    The {@link FederationConsumerInfo} that defines the consumer to be created.
    *
    * @return a new {@link FederationConsumerInternal} instance that will reside in this manager.
    */
   protected abstract FederationConsumerInternal createFederationConsumer(FederationConsumerInfo consumerInfo);

   /**
    * Signal any registered plugins for this federation instance that a remote Address consumer
    * is being created.
    *
    * @param info
    *    The {@link FederationConsumerInfo} that describes the remote Address consumer
    */
   protected abstract void signalBeforeCreateFederationConsumer(FederationConsumerInfo info);

   /**
    * Signal any registered plugins for this federation instance that a remote Address consumer
    * has been created.
    *
    * @param consumer
    *    The {@link FederationConsumerInfo} that describes the remote Address consumer
    */
   protected abstract void signalAfterCreateFederationConsumer(FederationConsumer consumer);

   /**
    * Signal any registered plugins for this federation instance that a remote Address consumer
    * is about to be closed.
    *
    * @param consumer
    *    The {@link FederationConsumer} that that is about to be closed.
    */
   protected abstract void signalBeforeCloseFederationConsumer(FederationConsumer consumer);

   /**
    * Signal any registered plugins for this federation instance that a remote Address consumer
    * has now been closed.
    *
    * @param consumer
    *    The {@link FederationConsumer} that that has been closed.
    */
   protected abstract void signalAfterCloseFederationConsumer(FederationConsumer consumer);

   /**
    * Query all registered plugins for this federation instance to determine if any wish to
    * prevent a federation consumer from being created for the given Queue.
    *
    * @param address
    *    The address on which the manager is intending to create a remote consumer for.
    *
    * @return true if any registered plugin signaled that creation should be suppressed.
    */
   protected abstract boolean isPluginBlockingFederationConsumerCreate(AddressInfo address);

   /**
    * Query all registered plugins for this federation instance to determine if any wish to
    * prevent a federation consumer from being created for the given Queue.
    *
    * @param divert
    *    The {@link Divert} that triggered the manager to attempt to create a remote consumer.
    * @param queue
    *    The {@link Queue} that triggered the manager to attempt to create a remote consumer.
    *
    * @return true if any registered plugin signaled that creation should be suppressed.
    */
   protected abstract boolean isPluginBlockingFederationConsumerCreate(Divert divert, Queue queue);

   /**
    * Query all registered plugins for this federation instance to determine if any wish to
    * prevent a federation consumer from being created for the given Queue.
    *
    * @param queue
    *    The {@link Queue} that triggered the manager to attempt to create a remote consumer.
    *
    * @return true if any registered plugin signaled that creation should be suppressed.
    */
   protected abstract boolean isPluginBlockingFederationConsumerCreate(Queue queue);

}
