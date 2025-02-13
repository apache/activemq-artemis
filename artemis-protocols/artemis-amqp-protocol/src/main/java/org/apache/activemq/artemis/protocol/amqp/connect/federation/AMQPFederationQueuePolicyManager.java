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

import static org.apache.activemq.artemis.protocol.amqp.federation.FederationConstants.FEDERATION_NAME;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.filter.impl.FilterImpl;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.QueueBinding;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.ServerConsumer;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerBindingPlugin;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerConsumerPlugin;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.protocol.amqp.federation.FederationConsumerInfo;
import org.apache.activemq.artemis.protocol.amqp.federation.FederationReceiveFromQueuePolicy;
import org.apache.activemq.artemis.protocol.amqp.federation.FederationConsumerInfo.Role;
import org.apache.activemq.artemis.utils.CompositeAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The AMQP Federation implementation of an federation queue policy manager.
 */
public final class AMQPFederationQueuePolicyManager extends AMQPFederationLocalPolicyManager implements ActiveMQServerConsumerPlugin, ActiveMQServerBindingPlugin {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   protected final Predicate<ServerConsumer> federationConsumerMatcher;
   protected final FederationReceiveFromQueuePolicy policy;
   protected final Map<FederationConsumerInfo, AMQPFederationQueueConsumerManager> federationConsumers = new HashMap<>();

   public AMQPFederationQueuePolicyManager(AMQPFederation federation, AMQPFederationMetrics metrics, FederationReceiveFromQueuePolicy queuePolicy) throws ActiveMQException {
      super(federation, metrics, queuePolicy);

      Objects.requireNonNull(queuePolicy, "The Queue match policy cannot be null");

      this.policy = queuePolicy;
      this.federationConsumerMatcher = createFederationConsumerMatcher(server, queuePolicy);
   }

   /**
    * {@return the receive from address policy that backs the address policy manager}
    */
   @Override
   public FederationReceiveFromQueuePolicy getPolicy() {
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
      }
   }

   @Override
   public synchronized void afterCreateConsumer(ServerConsumer consumer) {
      if (isActive()) {
         reactIfConsumerMatchesPolicy(consumer);
      }
   }

   @Override
   public synchronized void afterCloseConsumer(ServerConsumer consumer, boolean failed) {
      if (isActive()) {
         final FederationConsumerInfo consumerInfo = createConsumerInfo(consumer);
         final AMQPFederationQueueConsumerManager entry = federationConsumers.get(consumerInfo);

         if (entry != null) {
            logger.trace("Reducing demand on federated queue {}", entry.getQueueName());
            entry.removeDemand(identifyConsumer(consumer));
         }
      }
   }

   @Override
   public synchronized void afterRemoveBinding(Binding binding, Transaction tx, boolean deleteData) throws ActiveMQException {
      if (binding instanceof QueueBinding queueBinding) {
         final String queueName = queueBinding.getQueue().getName().toString();

         federationConsumers.values().removeIf((entry) -> {
            if (entry.getQueueName().equals(queueName)) {
               logger.trace("Federated queue {} was removed, closing federation consumer", queueName);

               // Demand is gone because the Queue binding is gone and any in-flight messages
               // can be allowed to be released back to the remote as they will not be processed.
               // We removed the consumer information from demand tracking to prevent build up
               // of data for entries that may never return and to prevent interference from the
               // next set of events which will be the close of all local consumers for this now
               // removed Queue.
               entry.shutdownNow();

               return true;
            } else {
               return false;
            }
         });
      }
   }

   @Override
   protected void scanAllBindings() {
      server.getPostOffice()
            .getAllBindings()
            .filter(b -> b instanceof QueueBinding)
            .map(b -> (QueueBinding) b)
            .forEach(b -> checkQueueForMatch(b.getQueue()));
   }

   private void checkQueueForMatch(Queue queue) {
      queue.getConsumers()
           .stream()
           .filter(consumer -> consumer instanceof ServerConsumer)
           .map(c -> (ServerConsumer) c)
           .forEach(this::reactIfConsumerMatchesPolicy);
   }

   private void reactIfConsumerMatchesPolicy(ServerConsumer consumer) {
      final String queueName = consumer.getQueue().getName().toString();

      if (testIfQueueMatchesPolicy(consumer.getQueueAddress().toString(), queueName)) {
         // We should ignore federation consumers from remote peers but configuration does allow
         // these to be federated again for some very specific use cases so we check before then
         // moving onto any server plugin checks kick in.
         if (federationConsumerMatcher.test(consumer)) {
            return;
         }

         logger.trace("Federation Policy matched on consumer for binding: {}", consumer.getBinding());

         final AMQPFederationQueueConsumerManager entry;
         final FederationConsumerInfo consumerInfo = createConsumerInfo(consumer);

         // Check for existing consumer add demand from a additional local consumer to ensure
         // the remote consumer remains active until all local demand is withdrawn.
         if (federationConsumers.containsKey(consumerInfo)) {
            logger.trace("Federation Queue Policy manager found existing demand for queue: {}, adding demand", queueName);
            entry = federationConsumers.get(consumerInfo);
         } else {
            federationConsumers.put(consumerInfo, entry = new AMQPFederationQueueConsumerManager(this, consumerInfo, consumer.getQueue()));
         }

         // Demand passed all binding plugin blocking checks so we track it, plugin can still
         // stop federation of the queue based on some external criteria but once it does
         // (if ever) allow it we will have tracked all allowed demand.
         entry.addDemand(identifyConsumer(consumer));
      }
   }

   /**
    * Checks if the remote queue added falls within the set of queues that match the configured queue policy and if so
    * scans for local demand on that queue to see if a new attempt to federate the queue is needed.
    *
    * @param addressName The address that was added on the remote.
    * @param queueName   The queue that was added on the remote.
    * @throws Exception if an error occurs while processing the queue added event.
    */
   public synchronized void afterRemoteQueueAdded(String addressName, String queueName) throws Exception {
      // We ignore the remote address as locally the policy can be a wild card match and we can
      // try to federate based on the Queue only, if the remote rejects the federation consumer
      // binding again the request will once more be recorded and we will get another event if
      // the queue were recreated such that a match could be made. We retain all the current
      // demand and don't need to re-check the server state before trying to create the
      // remote queue consumer.
      if (isActive() && testIfQueueMatchesPolicy(queueName)) {
         final Queue queue = server.locateQueue(queueName);

         if (queue != null) {
            federationConsumers.forEach((k, v) -> {
               if (k.getQueueName().equals(queueName)) {
                  v.recover();
               }
            });
         }
      }
   }

   /**
    * Performs the test against the configured queue policy to check if the target queue and its associated address is a
    * match or not. A subclass can override this method and provide its own match tests in combination with the
    * configured matching policy.
    *
    * @param address   The address that is being tested for a policy match.
    * @param queueName The name of the queue that is being tested for a policy match.
    * @return {@code true} if the address given is a match against the policy
    */
   private boolean testIfQueueMatchesPolicy(String address, String queueName) {
      return policy.test(address, queueName);
   }

   /**
    * Performs the test against the configured queue policy to check if the target queue minus its associated address is
    * a match or not. A subclass can override this method and provide its own match tests in combination with the
    * configured matching policy.
    *
    * @param queueName The name of the queue that is being tested for a policy match.
    * @return {@code true} if the address given is a match against the policy
    */
   private boolean testIfQueueMatchesPolicy(String queueName) {
      return policy.testQueue(queueName);
   }

   /**
    * Create a new {@link FederationConsumerInfo} based on the given {@link ServerConsumer} and the configured
    * {@link FederationReceiveFromQueuePolicy}. A subclass must override this method to return a consumer information
    * object with additional data used be that implementation.
    *
    * @param consumer The {@link ServerConsumer} to use as a basis for the consumer information object.
    * @return a new {@link FederationConsumerInfo} instance based on the server consumer
    */
   private FederationConsumerInfo createConsumerInfo(ServerConsumer consumer) {
      final Queue queue = consumer.getQueue();
      final String queueName = queue.getName().toString();
      final String address = queue.getAddress().toString();

      final int priority = configuration.isIgnoreSubscriptionPriorities() ?
         ActiveMQDefaultConfiguration.getDefaultConsumerPriority() + policy.getPriorityAjustment() :
         consumer.getPriority() + policy.getPriorityAjustment();

      final String filterString =
         selectFilter(queue.getFilter(), configuration.isIgnoreSubscriptionFilters() ? null : consumer.getFilter());

      return new AMQPFederationGenericConsumerInfo(Role.QUEUE_CONSUMER,
                                                   address,
                                                   queueName,
                                                   queue.getRoutingType(),
                                                   filterString,
                                                   CompositeAddress.toFullyQualified(address, queueName),
                                                   priority);
   }

   @Override
   protected AMQPFederationConsumer createFederationConsumer(FederationConsumerInfo consumerInfo) {
      Objects.requireNonNull(consumerInfo, "Federation Queue consumer information object was null");

      if (logger.isTraceEnabled()) {
         logger.trace("AMQP Federation {} creating queue consumer: {} for policy: {}", federation.getName(), consumerInfo, policy.getPolicyName());
      }

      // Don't initiate anything yet as the caller might need to register error handlers etc
      // before the attach is sent otherwise they could miss the failure case.
      return new AMQPFederationQueueConsumer(this, configuration, session, consumerInfo, metrics.newConsumerMetrics());
   }

   /**
    * Creates a {@link Predicate} that should return true if the given consumer is a federation created consumer which
    * should not be further federated.
    *
    * @param server The server instance for use in creating the filtering {@link Predicate}.
    * @param policy The configured Queue matching policy that can provide additional match criteria.
    * @return a {@link Predicate} that will return true if the consumer should be filtered
    * @throws ActiveMQException if an error occurs while creating the new consumer filter.
    */
   private Predicate<ServerConsumer> createFederationConsumerMatcher(ActiveMQServer server, FederationReceiveFromQueuePolicy policy) throws ActiveMQException {
      if (policy.isIncludeFederated()) {
         return (consumer) -> false; // Configuration says to federate these
      } else {
         // This filter matches on the same criteria as the original Core client based
         // Federation code which allows this implementation to see those consumers as
         // well as its own which in this methods implementation must also use this same
         // mechanism to mark federation resources.

         final Filter metaDataMatcher =
            FilterImpl.createFilter("\"" + FEDERATION_NAME + "\" IS NOT NULL");

         return (consumer) -> {
            final ServerSession serverSession = server.getSessionByID(consumer.getSessionID());

            if (serverSession != null && serverSession.getMetaData() != null) {
               return metaDataMatcher.match(serverSession.getMetaData());
            } else {
               return false;
            }
         };
      }
   }

   private static String identifyConsumer(ServerConsumer consumer) {
      return consumer.getConnectionID().toString() + ":" +
             consumer.getSessionID() + ":" +
             consumer.getID();
   }

   private static String selectFilter(Filter queueFilter, Filter consumerFilter) {
      if (consumerFilter != null) {
         return consumerFilter.getFilterString().toString();
      } else {
         return queueFilter != null ? queueFilter.getFilterString().toString() : null;
      }
   }

   private static class AMQPFederationQueueConsumerManager extends AMQPFederationConsumerManager {

      private final AMQPFederationQueuePolicyManager manager;
      private final Queue queue;
      private final FederationConsumerInfo consumerInfo;

      AMQPFederationQueueConsumerManager(AMQPFederationQueuePolicyManager manager, FederationConsumerInfo consumerInfo, Queue queue) {
         super(manager);

         this.manager = manager;
         this.queue = queue;
         this.consumerInfo = consumerInfo;
      }

      /**
       * {@return the name of the Queue this federation consumer manager is attached to}
       */
      public String getQueueName() {
         return queue.getName().toString();
      }

      @Override
      protected AMQPFederationConsumer createFederationConsumer() {
         return manager.createFederationConsumer(consumerInfo);
      }

      @Override
      protected boolean isPluginBlockingFederationConsumerCreate() {
         return manager.isPluginBlockingFederationConsumerCreate(queue);
      }
   }
}
