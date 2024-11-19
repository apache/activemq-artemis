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

import java.util.Objects;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerBindingPlugin;
import org.apache.activemq.artemis.protocol.amqp.federation.FederationConsumer;
import org.apache.activemq.artemis.protocol.amqp.federation.FederationConsumerInfo;
import org.apache.activemq.artemis.protocol.amqp.federation.FederationReceiveFromResourcePolicy;
import org.apache.activemq.artemis.protocol.amqp.federation.FederationType;

/**
 * Base Federation policy manager that declares some common APIs that address or queue policy
 * managers must provide implementations for.
 */
public abstract class FederationPolicyManager implements ActiveMQServerBindingPlugin {

   protected final ActiveMQServer server;
   protected final FederationInternal federation;

   protected volatile boolean started;

   public FederationPolicyManager(FederationInternal federation) throws ActiveMQException {
      Objects.requireNonNull(federation, "The Federation instance cannot be null");

      this.federation = federation;
      this.server = federation.getServer();
   }

   /**
    * @return the immutable federation policy configuration that backs this manager.
    */
   public abstract FederationReceiveFromResourcePolicy getPolicy();

   /**
    * @return the federation type this policy manager implements.
    */
   public final FederationType getPolicyType() {
      return getPolicy().getPolicyType();
   }

   /**
    * @return the assigned name of the policy that is being managed.
    */
   public final String getPolicyName() {
      return getPolicy().getPolicyName();
   }

   /**
    * @return <code>true</code> if the policy is started at the time this method was called.
    */
   public final boolean isStarted() {
      return started;
   }

   /**
    * @return the {@link FederationInternal} instance that owns this policy manager.
    */
   public FederationInternal getFederation() {
      return federation;
   }

   /**
    * Called on start of the manager before any other actions are taken to allow the subclass time
    * to configure itself and prepare any needed state prior to starting management of federated
    * resources.
    *
    * @param policy
    *    The policy configuration for this policy manager.
    */
   protected abstract void handlePolicyManagerStarted(FederationReceiveFromResourcePolicy policy);

   /**
    * Create a new {@link FederationConsumerInternal} instance using the consumer information
    * given. This is called when local demand for a matched resource requires a new consumer to
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
    * Signal any registered plugins for this federation instance that a remote consumer
    * is being created.
    *
    * @param info
    *    The {@link FederationConsumerInfo} that describes the remote federation consumer
    */
   protected abstract void signalBeforeCreateFederationConsumer(FederationConsumerInfo info);

   /**
    * Signal any registered plugins for this federation instance that a remote consumer
    * has been created.
    *
    * @param consumer
    *    The {@link FederationConsumerInfo} that describes the remote consumer
    */
   protected abstract void signalAfterCreateFederationConsumer(FederationConsumer consumer);

   /**
    * Signal any registered plugins for this federation instance that a remote consumer
    * is about to be closed.
    *
    * @param consumer
    *    The {@link FederationConsumer} that that is about to be closed.
    */
   protected abstract void signalBeforeCloseFederationConsumer(FederationConsumer consumer);

   /**
    * Signal any registered plugins for this federation instance that a remote consumer
    * has now been closed.
    *
    * @param consumer
    *    The {@link FederationConsumer} that that has been closed.
    */
   protected abstract void signalAfterCloseFederationConsumer(FederationConsumer consumer);

}
