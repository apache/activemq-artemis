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

import java.util.List;
import java.util.Map;

import org.apache.activemq.artemis.core.server.transformer.Transformer;
import org.apache.qpid.proton.amqp.Symbol;

/**
 * Constants class for values used in the AMQP Federation implementation.
 */
public final class AMQPFederationConstants {

   /**
    * Address used by a remote broker instance to validate that an incoming federation connection
    * has access right to perform federation operations. The user that connects to the AMQP federation
    * endpoint and attempt to create the control link must have write access to this address.
    */
   public static final String FEDERATION_CONTROL_LINK_VALIDATION_ADDRESS = "$ACTIVEMQ_ARTEMIS_FEDERATION";

   /**
    * A desired capability added to the federation control link that must be offered
    * in return for a federation connection to be successfully established.
    */
   public static final Symbol FEDERATION_CONTROL_LINK = Symbol.getSymbol("AMQ_FEDERATION_CONTROL_LINK");

   /**
    * Property name used to embed a nested map of properties meant to be applied if the federation
    * resources created on the remote end of the control link if configured to do so. These properties
    * essentially carry local configuration to the remote side that would otherwise use broker defaults
    * and not match behaviors of resources created on the local side of the connection.
    */
   public static final Symbol FEDERATION_CONFIGURATION = Symbol.getSymbol("federation-configuration");

   /**
    * Property value that can be applied to federation configuration that controls the timeout value
    * for a link attach to complete before the attach attempt is considered to have failed. The value
    * is configured in seconds (default is 30 seconds).
    */
   public static final String LINK_ATTACH_TIMEOUT = "attach-timeout";

   /**
    * Configuration property that defines the amount of credits to batch to an AMQP receiver link
    * and the top up limit when sending more credit once the credits are determined to be running
    * low. this can be sent to the peer so that dual federation configurations share the same
    * configuration on both sides of the connection.
    */
   public static final String RECEIVER_CREDITS = "amqpCredits";

   /**
    * A low water mark for receiver credits that indicates more should be sent to top it up to the
    * original credit batch size. this can be sent to the peer so that dual federation configurations
    * share the same configuration on both sides of the connection.
    */
   public static final String RECEIVER_CREDITS_LOW = "amqpLowCredits";

   /**
    * Configuration property used to convey the local side value to use when considering if a message
    * is a large message, this can be sent to the peer so that dual federation configurations share
    * the same configuration on both sides of the connection.
    */
   public static final String LARGE_MESSAGE_THRESHOLD = "minLargeMessageSize";

   /**
    * A desired capability added to the federation queue receiver link that must be offered
    * in return for a federation queue receiver to be successfully opened.  On the remote the
    * presence of this capability indicates that the matching queue should be present on the
    * remote and its absence constitutes a failure that should result in the attach request
    * being failed.
    */
   public static final Symbol FEDERATION_QUEUE_RECEIVER = Symbol.getSymbol("AMQ_FEDERATION_QUEUE_RECEIVER");

   /**
    * A desired capability added to the federation address receiver link that must be offered
    * in return for a federation address receiver to be successfully opened.
    */
   public static final Symbol FEDERATION_ADDRESS_RECEIVER = Symbol.getSymbol("AMQ_FEDERATION_ADDRESS_RECEIVER");

   /**
    * Property added to the receiver properties when opening an AMQP federation address or queue consumer
    * that indicates the consumer priority that should be used when creating the remote consumer. The
    * value assign to the properties {@link Map} is a signed integer value.
    */
   public static final Symbol FEDERATION_RECEIVER_PRIORITY = Symbol.getSymbol("priority");

   /**
    * Commands sent across the control link will each carry an operation type to indicate
    * the desired action the remote should take upon receipt of the command. The type of
    * command infers the payload of the structure of the message payload.
    */
   public static final Symbol OPERATION_TYPE = Symbol.getSymbol("x-opt-amq-federation-op-type");

   /**
    * Indicates that the message carries a federation queue match policy that should be
    * added to the remote for reverse federation of matching queue from the remote peer.
    */
   public static final String ADD_QUEUE_POLICY = "ADD_QUEUE_POLICY";

   /**
    * Indicates that the message carries a federation address match policy that should be
    * added to the remote for reverse federation of matching queue from the remote peer.
    */
   public static final String ADD_ADDRESS_POLICY = "ADD_ADDRESS_POLICY";

   /**
    * Both Queue and Address policies carry a unique name that will always be encoded.
    */
   public static final String POLICY_NAME = "policy-name";

   /**
    * Queue policy includes are encoded as a {@link List} of flattened key / value pairs when configured.
    */
   public static final String QUEUE_INCLUDES = "queue-includes";

   /**
    * Queue policy excludes are encoded as a {@link List} of flattened key / value pairs when configured.
    */
   public static final String QUEUE_EXCLUDES = "queue-excludes";

   /**
    * Encodes a boolean value that indicates if the include federation option should be enabled.
    */
   public static final String QUEUE_INCLUDE_FEDERATED = "include-federated";

   /**
    * Encodes a signed integer value that adjusts the priority of the any created queue receivers.
    */
   public static final String QUEUE_PRIORITY_ADJUSTMENT = "priority-adjustment";

   /**
    * Address policy includes are encoded as a {@link List} of string entries when configured.
    */
   public static final String ADDRESS_INCLUDES = "address-includes";

   /**
    * Address policy excludes are encoded as a {@link List} of string entries when configured.
    */
   public static final String ADDRESS_EXCLUDES = "address-excludes";

   /**
    * Encodes a boolean value that indicates if queue auto delete option should be enabled.
    */
   public static final String ADDRESS_AUTO_DELETE = "auto-delete";

   /**
    * Encodes a signed long value that controls the delay before auto deletion if auto delete is enabled.
    */
   public static final String ADDRESS_AUTO_DELETE_DELAY = "auto-delete-delay";

   /**
    * Encodes a signed long value that controls the message count value that allows for address auto delete.
    */
   public static final String ADDRESS_AUTO_DELETE_MSG_COUNT = "auto-delete-msg-count";

   /**
    * Encodes a signed integer value that controls the maximum number of hops allowed for federated messages.
    */
   public static final String ADDRESS_MAX_HOPS = "max-hops";

   /**
    * Encodes boolean value that controls if the address federation should include divert bindings.
    */
   public static final String ADDRESS_ENABLE_DIVERT_BINDINGS = "enable-divert-bindings";

   /**
    * Encodes a {@link Map} of String keys and values that are carried along in the federation
    * policy (address or queue). These values can be used to add extended configuration to the
    * policy object such as overriding settings from the connection URI.
    */
   public static final String POLICY_PROPERTIES_MAP = "policy-properties-map";

   /**
    * Encodes a string value carrying the name of the {@link Transformer} class to use.
    */
   public static final String TRANSFORMER_CLASS_NAME = "transformer-class-name";

   /**
    * Encodes a {@link Map} of String keys and values that are applied to the transformer
    * configuration for the policy.
    */
   public static final String TRANSFORMER_PROPERTIES_MAP = "transformer-properties-map";

}
