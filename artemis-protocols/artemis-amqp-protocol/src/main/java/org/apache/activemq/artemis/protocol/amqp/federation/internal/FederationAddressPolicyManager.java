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
   protected final FederationReceiveFromAddressPolicy policy;
   protected final Map<FederationConsumerInfo, FederationConsumerEntry> remoteConsumers = new HashMap<>();
   protected final FederationInternal federation;
   protected final Map<DivertBinding, Set<SimpleString>> matchingDiverts = new HashMap<>();

   private volatile boolean started;

   public FederationAddressPolicyManager(FederationInternal federation, FederationReceiveFromAddressPolicy addressPolicy) throws ActiveMQException {
      Objects.requireNonNull(federation, "The Federation instance cannot be null");
      Objects.requireNonNull(addressPolicy, "The Address match policy cannot be null");

      this.federation = federation;
      this.policy = addressPolicy;
      this.server = federation.getServer();
      this.server.registerBrokerPlugin(this);
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
         remoteConsumers.forEach((k, v) -> v.getConsumer().close()); // Cleanup and recreate if ever reconnected.
         remoteConsumers.clear();
         matchingDiverts.clear();
      }
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
                  .stream().filter(binding -> binding instanceof DivertBinding)
                  .forEach(this::afterAddBinding);
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

   @Override
   public synchronized void beforeRemoveBinding(SimpleString bindingName, Transaction tx, boolean deleteData) {
      final Binding binding = server.getPostOffice().getBinding(bindingName);
      final AddressInfo addressInfo = server.getPostOffice().getAddressInfo(binding.getAddress());

      if (binding instanceof QueueBinding) {
         tryRemoveDemandOnAddress(addressInfo);

         if (policy.isEnableDivertBindings()) {
            // See if there is any matching diverts that match this queue binding and remove demand now that
            // the queue is going away. Since a divert can be composite we need to check for a match of the
            // queue address on each of the forwards if there are any.
            matchingDiverts.entrySet().forEach(entry -> {
               final SimpleString forwardAddress = entry.getKey().getDivert().getForwardAddress();

               if (isAddressInDivertForwards(binding.getAddress(), forwardAddress)) {
                  final AddressInfo srcAddressInfo = server.getPostOffice().getAddressInfo(entry.getKey().getAddress());

                  if (entry.getValue().remove(((QueueBinding) binding).getQueue().getName())) {
                     tryRemoveDemandOnAddress(srcAddressInfo);
                  }
               }
            });
         }
      } else if (policy.isEnableDivertBindings() || binding instanceof DivertBinding) {
         final DivertBinding divertBinding = (DivertBinding) binding;
         final Set<SimpleString> matchingQueues = matchingDiverts.remove(binding);

         // Each entry in the matching queues set is one instance of demand that was
         // registered on the source address which would have been federated from the
         // remote so on remove we deduct each and if that removes all demand the remote
         // consumer will be closed.
         if (matchingQueues != null) {
            try {
               matchingQueues.forEach((queueName) -> tryRemoveDemandOnAddress(addressInfo));
            } catch (Exception e) {
               ActiveMQServerLogger.LOGGER.federationBindingsLookupError(divertBinding.getDivert().getForwardAddress(), e);
            }
         }
      }
   }

   protected final void tryRemoveDemandOnAddress(AddressInfo addressInfo) {
      final FederationConsumerInfo consumerInfo = createConsumerInfo(addressInfo);
      final FederationConsumerEntry entry = remoteConsumers.get(consumerInfo);

      if (entry != null && entry.reduceDemand()) {
         final FederationConsumerInternal federationConsuner = entry.getConsumer();

         try {
            signalBeforeCloseFederationConsumer(federationConsuner);
            federationConsuner.close();
            signalAfterCloseFederationConsumer(federationConsuner);
         } finally {
            remoteConsumers.remove(consumerInfo);
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
            .filter(bind -> bind instanceof QueueBinding || (policy.isEnableDivertBindings() && bind instanceof DivertBinding))
            .forEach(bind -> checkBindingForMatch(bind));
   }

   protected final void checkBindingForMatch(Binding binding) {
      if (binding instanceof QueueBinding) {
         final QueueBinding queueBinding = (QueueBinding) binding;
         final AddressInfo addressInfo = server.getPostOffice().getAddressInfo(binding.getAddress());

         reactIfBindingMatchesPolicy(addressInfo, queueBinding);
         reactIfQueueBindingMatchesAnyDivertTarget(queueBinding);
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
      if (matchingDiverts.get(divertBinding) == null) {
         final Set<SimpleString> matchingQueues = new HashSet<>();
         matchingDiverts.put(divertBinding, matchingQueues);

         // We must account for the composite divert case by splitting the address and
         // getting the bindings on each one.
         final SimpleString forwardAddress = divertBinding.getDivert().getForwardAddress();
         final SimpleString[] forwardAddresses = forwardAddress.split(',');

         try {
            for (SimpleString forward : forwardAddresses) {
               server.getPostOffice().getBindingsForAddress(forward).getBindings()
                     .stream().filter(b -> b instanceof QueueBinding)
                     .map(b -> (QueueBinding) b)
                     .forEach(queueBinding -> {
                        if (isPluginBlockingFederationConsumerCreate(divertBinding.getDivert(), queueBinding.getQueue())) {
                           return;
                        }

                        if (reactIfBindingMatchesPolicy(addressInfo, queueBinding)) {
                           matchingQueues.add(queueBinding.getQueue().getName());
                        }
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
      final SimpleString queueName = queueBinding.getQueue().getName();

      matchingDiverts.entrySet().forEach((e) -> {
         final SimpleString forwardAddress = e.getKey().getDivert().getForwardAddress();
         final DivertBinding divertBinding = e.getKey();

         // Check matched diverts to see if the QueueBinding address matches the address or
         // addresses (composite diverts) of the Divert and if so then we can check if we need
         // to create demand on the source address on the remote if we haven't done so already.

         if (!e.getValue().contains(queueName) && isAddressInDivertForwards(queueAddress, forwardAddress)) {
            if (isPluginBlockingFederationConsumerCreate(divertBinding.getDivert(), queueBinding.getQueue())) {
               return;
            }

            final AddressInfo addressInfo = server.getPostOffice().getAddressInfo(divertBinding.getAddress());

            // We know it matches address policy at this point but we don't yet know if any other
            // remote demand exists and we want to check here if the react method did indeed add
            // demand on the address and if so add this queue into the diverts matching queues set.
            if (reactIfBindingMatchesPolicy(addressInfo, queueBinding)) {
               e.getValue().add(queueName);
            }
         }
      });
   }

   private static boolean isAddressInDivertForwards(final SimpleString queueAddress, final SimpleString forwardAddress) {
      final SimpleString[] forwardAddresses = forwardAddress.split(',');

      for (SimpleString forward : forwardAddresses) {
         if (queueAddress.equals(forward)) {
            return true;
         }
      }

      return false;
   }

   protected final boolean reactIfBindingMatchesPolicy(AddressInfo address, QueueBinding binding) {
      if (testIfAddressMatchesPolicy(address)) {
         logger.trace("Federation Address Policy matched on for demand on address: {} : binding: {}", address, binding);

         final FederationConsumerInfo consumerInfo = createConsumerInfo(address);

         // Check for existing consumer add demand from a additional local consumer
         // to ensure the remote consumer remains active until all local demand is
         // withdrawn.
         if (remoteConsumers.containsKey(consumerInfo)) {
            logger.trace("Federation Address Policy manager found existing demand for address: {}", address);
            remoteConsumers.get(consumerInfo).addDemand();
         } else {
            if (isPluginBlockingFederationConsumerCreate(address)) {
               return false;
            }

            if (isPluginBlockingFederationConsumerCreate(binding.getQueue())) {
               return false;
            }

            logger.trace("Federation Address Policy manager creating remote consumer for address: {}", address);

            signalBeforeCreateFederationConsumer(consumerInfo);

            final FederationConsumerInternal queueConsumer = createFederationConsumer(consumerInfo);
            final FederationConsumerEntry entry = createConsumerEntry(queueConsumer);

            // Handle remote close with remove of consumer which means that future demand will
            // attempt to create a new consumer for that demand. Ensure that thread safety is
            // accounted for here as the notification can be asynchronous.
            queueConsumer.setRemoteClosedHandler((closedConsumer) -> {
               synchronized (this) {
                  try {
                     remoteConsumers.remove(closedConsumer.getConsumerInfo());
                  } finally {
                     closedConsumer.close();
                  }
               }
            });

            // Called under lock so state should stay in sync
            remoteConsumers.put(consumerInfo, entry);

            // Now that we are tracking it we can start it
            queueConsumer.start();

            signalAfterCreateFederationConsumer(queueConsumer);
         }

         return true;
      }

      return false;
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
    * Creates a {@link FederationConsumerEntry} instance that will be used to store a {@link FederationConsumer}
    * along with other state data needed to manage a federation consumer instance. A subclass can override
    * this method to return a more customized entry type with additional state data.
    *
    * @param consumer
    *    The {@link FederationConsumerInternal} instance that will be housed in this entry.
    *
    * @return a new {@link FederationConsumerEntry} that holds the given federation consumer.
    */
   protected FederationConsumerEntry createConsumerEntry(FederationConsumerInternal consumer) {
      return new FederationConsumerEntry(consumer);
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
