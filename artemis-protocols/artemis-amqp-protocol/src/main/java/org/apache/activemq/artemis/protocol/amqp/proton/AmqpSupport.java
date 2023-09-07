/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.protocol.amqp.proton;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Objects;

import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.engine.Link;

/**
 * Set of useful methods and definitions used in the AMQP protocol handling
 */
public class AmqpSupport {

   // Key used to add a Runnable initializer to a link that the broker has opened
   // which will be called when the remote responds to the broker outgoing Attach
   // with its own Attach response.
   public static final Object AMQP_LINK_INITIALIZER_KEY = Runnable.class;

   // Default thresholds/values used for granting credit to producers
   public static final int AMQP_CREDITS_DEFAULT = 1000;
   public static final int AMQP_LOW_CREDITS_DEFAULT = 300;

   // Defaults for controlling the interpretation of AMQP dispositions
   public static final boolean AMQP_TREAT_REJECT_AS_UNMODIFIED_DELIVERY_FAILURE = false;

   // Defaults for controlling the behaviour of AMQP dispositions
   public static final boolean AMQP_USE_MODIFIED_FOR_TRANSIENT_DELIVERY_ERRORS = false;

   // Identification values used to locating JMS selector types.
   public static final Symbol JMS_SELECTOR_KEY = Symbol.valueOf("jms-selector");
   public static final UnsignedLong JMS_SELECTOR_CODE = UnsignedLong.valueOf(0x0000468C00000004L);
   public static final Symbol JMS_SELECTOR_NAME = Symbol.valueOf("apache.org:selector-filter:string");
   public static final Object[] JMS_SELECTOR_FILTER_IDS = new Object[]{JMS_SELECTOR_CODE, JMS_SELECTOR_NAME};
   public static final UnsignedLong NO_LOCAL_CODE = UnsignedLong.valueOf(0x0000468C00000003L);
   public static final Symbol NO_LOCAL_NAME = Symbol.valueOf("apache.org:no-local-filter:list");
   public static final Object[] NO_LOCAL_FILTER_IDS = new Object[]{NO_LOCAL_CODE, NO_LOCAL_NAME};

   // Capabilities used to identify destination type in some requests.
   public static final Symbol TEMP_QUEUE_CAPABILITY = Symbol.valueOf("temporary-queue");
   public static final Symbol TEMP_TOPIC_CAPABILITY = Symbol.valueOf("temporary-topic");
   public static final Symbol QUEUE_CAPABILITY = Symbol.valueOf("queue");
   public static final Symbol TOPIC_CAPABILITY = Symbol.valueOf("topic");

   // Symbols used to announce connection information to remote peer.
   public static final Symbol INVALID_FIELD = Symbol.valueOf("invalid-field");
   public static final Symbol CONTAINER_ID = Symbol.valueOf("container-id");

   // Symbols used to announce connection information to remote peer.
   public static final Symbol ANONYMOUS_RELAY = Symbol.valueOf("ANONYMOUS-RELAY");
   public static final Symbol DELAYED_DELIVERY = Symbol.valueOf("DELAYED_DELIVERY");
   public static final Symbol QUEUE_PREFIX = Symbol.valueOf("queue-prefix");
   public static final Symbol TOPIC_PREFIX = Symbol.valueOf("topic-prefix");
   public static final Symbol CONNECTION_OPEN_FAILED = Symbol.valueOf("amqp:connection-establishment-failed");
   public static final Symbol PRODUCT = Symbol.valueOf("product");
   public static final Symbol VERSION = Symbol.valueOf("version");
   public static final Symbol PLATFORM = Symbol.valueOf("platform");
   public static final Symbol RESOURCE_DELETED = Symbol.valueOf("amqp:resource-deleted");
   public static final Symbol CONNECTION_FORCED = Symbol.valueOf("amqp:connection:forced");
   public static final Symbol DETACH_FORCED = Symbol.valueOf("amqp:link:detach-forced");
   public static final Symbol NOT_FOUND = Symbol.valueOf("amqp:not-found");
   public static final Symbol SHARED_SUBS = Symbol.valueOf("SHARED-SUBS");
   public static final Symbol NETWORK_HOST = Symbol.valueOf("network-host");
   public static final Symbol PORT = Symbol.valueOf("port");
   static final Symbol SCHEME = Symbol.valueOf("scheme");
   static final Symbol HOSTNAME = Symbol.valueOf("hostname");

   static final Symbol FAILOVER_SERVER_LIST = Symbol.valueOf("failover-server-list");

   public static final int MAX_FRAME_SIZE_DEFAULT = 128 * 1024;

   // Symbols used in configuration of newly opened links.
   public static final Symbol COPY = Symbol.getSymbol("copy");

   // Lifetime policy symbols
   public static final Symbol LIFETIME_POLICY = Symbol.valueOf("lifetime-policy");

   public static final Symbol SOLE_CONNECTION_CAPABILITY = Symbol.valueOf("sole-connection-for-container");

   /**
    * Search for a given Symbol in a given array of Symbol object.
    *
    * @param symbols the set of Symbols to search.
    * @param key     the value to try and find in the Symbol array.
    * @return true if the key is found in the given Symbol array.
    */
   public static boolean contains(Symbol[] symbols, Symbol key) {
      if (symbols == null || symbols.length == 0) {
         return false;
      }

      for (Symbol symbol : symbols) {
         if (symbol.equals(key)) {
            return true;
         }
      }

      return false;
   }

   /**
    * Search for a particular filter using a set of known identification values
    * in the Map of filters.
    *
    * @param filters   The filters map that should be searched.
    * @param filterIds The aliases for the target filter to be located.
    * @return the filter if found in the mapping or null if not found.
    */
   public static Map.Entry<Symbol, DescribedType> findFilter(Map<Symbol, Object> filters, Object[] filterIds) {

      if (filterIds == null || filterIds.length == 0) {
         StringBuilder ids = new StringBuilder();
         if (filterIds != null) {
            for (Object filterId : filterIds) {
               ids.append(filterId).append(" ");
            }
         }
         throw new IllegalArgumentException("Invalid Filter Ids array passed: " + ids);
      }

      if (filters == null || filters.isEmpty()) {
         return null;
      }

      for (Map.Entry<Symbol, Object> filter : filters.entrySet()) {
         if (filter.getValue() instanceof DescribedType) {
            DescribedType describedType = ((DescribedType) filter.getValue());
            Object descriptor = describedType.getDescriptor();

            for (Object filterId : filterIds) {
               if (descriptor.equals(filterId)) {
                  return new AbstractMap.SimpleImmutableEntry<>(filter.getKey(), describedType);
               }
            }
         }
      }

      return null;
   }

   private static final Symbol[] EMPTY_CAPABILITIES = new Symbol[0];

   /**
    * Verifies that the desired capabilities that were sent to the remote were indeed
    * offered in return. If the remote has not offered a capability that was desired then
    * the initiating resource should determine if the offered set is still acceptable or
    * it should close the link and report the reason.
    * <p>
    * The remote could have offered more capabilities than the requested desired capabilities,
    * this method does not validate that or consider that a failure.
    *
    * @param link
    *    The link in question (Sender or Receiver).
    *
    * @return true if the remote offered all of the capabilities that were desired.
    */
   public static boolean verifyOfferedCapabilities(final Link link) {
      return verifyOfferedCapabilities(link, link.getDesiredCapabilities());
   }

   /**
    * Verifies that the given set of desired capabilities (which should be the full set of
    * desired capabilities configured on the link or a subset of those values) are indeed
    * offered in return. If the remote has not offered a capability that was desired then
    * the initiating resource should determine if the offered set is still acceptable or
    * it should close the link and report the reason.
    * <p>
    * The remote could have offered more capabilities than the requested desired capabilities,
    * this method does not validate that or consider that a failure.
    *
    * @param link
    *    The link in question (Sender or Receiver).
    * @param capabilities
    *    The capabilities that are required being checked for.
    *
    * @return true if the remote offered all of the capabilities that were desired.
    */
   public static boolean verifyOfferedCapabilities(final Link link, final Symbol... capabilities) {
      final Symbol[] desiredCapabilites = capabilities == null ? EMPTY_CAPABILITIES : capabilities;
      final Symbol[] remoteOfferedCapabilites =
         link.getRemoteOfferedCapabilities() == null ? EMPTY_CAPABILITIES : link.getRemoteOfferedCapabilities();

      for (Symbol desired : desiredCapabilites) {
         boolean foundCurrent = false;

         for (Symbol offered : remoteOfferedCapabilites) {
            if (desired.equals(offered)) {
               foundCurrent = true;
               break;
            }
         }

         if (!foundCurrent) {
            return false;
         }
      }

      return true;
   }

   /**
    * Verifies that the given remote desired capability is present in the remote link details.
    * <p>
    * The remote could have desired more capabilities than the one given, this method does
    * not validate that or consider that a failure.
    *
    * @param link
    *    The link in question (Sender or Receiver).
    * @param desiredCapability
    *    The non-null capability that is being checked as being desired.
    *
    * @return true if the remote desired all of the capabilities that were given.
    */
   public static boolean verifyDesiredCapability(final Link link, final Symbol desiredCapability) {
      Objects.requireNonNull(desiredCapability, "Desired capability to verifiy cannot be null");

      if (link.getRemoteDesiredCapabilities() == null) {
         return false;
      }

      for (Symbol capability : link.getRemoteDesiredCapabilities()) {
         if (capability.equals(desiredCapability)) {
            return true;
         }
      }

      return false;
   }
}
