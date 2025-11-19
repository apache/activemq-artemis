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
package org.apache.activemq.artemis.core.protocol.openwire.util;

import java.util.function.Function;

import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.WildcardConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.transaction.impl.XidImpl;
import org.apache.activemq.artemis.utils.CompositeAddress;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.XATransactionId;

public class OpenWireUtil {

   private static final String SCHEME_SEPARATOR = "://";
   private static final String QUEUE_SCHEME = "queue";
   private static final String TOPIC_SCHEME = "topic";
   private static final String CONSUMER_PREFIX = "Consumer.";
   private static final String VIRTUAL_TOPIC_MARKER = ".VirtualTopic.";

   public static final WildcardConfiguration OPENWIRE_WILDCARD = new WildcardConfiguration().setDelimiter('.')
                                                                                            .setAnyWords('>')
                                                                                            .setSingleWord('*');

   public static final String SELECTOR_AWARE_OPTION = "selectorAware";

   public static String extractFilterStringOrNull(final ConsumerInfo info, final ActiveMQDestination openWireDest) {
      if (info.getSelector() != null) {
         if (openWireDest.getOptions() != null) {
            if (Boolean.valueOf(openWireDest.getOptions().get(SELECTOR_AWARE_OPTION))) {
               return info.getSelector();
            }
         }
      }
      return null;
   }

   /**
    * We convert the core address to an ActiveMQ Destination. We use the actual address on the message rather than the
    * destination set on the consumer because it maybe different and the JMS spec says that it should be what ever was
    * set on publish/send so a divert or wildcard may mean thats its different to the destination subscribed to by the
    * consumer
    */
   public static ActiveMQDestination toAMQAddress(Message message, ActiveMQDestination actualDestination) {
      String address = message.getAddress();

      if (address == null || address.equals(actualDestination.getPhysicalName())) {
         return actualDestination;
      }

      if (actualDestination.isQueue()) {
         return new ActiveMQQueue(address);
      } else {
         return new ActiveMQTopic(address);
      }
   }

   public static XidImpl toXID(TransactionId xaXid) {
      return toXID((XATransactionId) xaXid);
   }

   public static XidImpl toXID(XATransactionId xaXid) {
      return new XidImpl(xaXid.getBranchQualifier(), xaXid.getFormatId(), xaXid.getGlobalTransactionId());
   }

   /**
    * Converts an ActiveMQ destination to a core produce address.
    * This method handles different types of destinations:
    * - Returns the physical name for temporary or advisory destinations
    * - For FQQN addresses, escapes backslashes in the address part only
    * - For other addresses, returns the stripped and escaped address
    *
    * @param destination the ActiveMQ destination to convert
    * @return the core produce address, or null if the destination is null
    */
   public static String toCoreProduceAddress(ActiveMQDestination destination) {
      if (destination == null) {
         return null;
      }

      if (destination.isTemporary() || AdvisorySupport.isAdvisoryTopic(destination)) {
         return destination.getPhysicalName();
      }

      final String strippedAddress = stripAddressScheme(destination.getPhysicalName(), destination);

      if (isFqqn(strippedAddress)) {
         return handleFqqn(strippedAddress, OpenWireUtil::escapeBackslashes);
      }

      return escapeBackslashes(strippedAddress);
   }

   /**
    * Converts an ActiveMQ destination to a core consume pattern.
    * This method handles different types of destinations:
    * - Returns the physical name for temporary or advisory destinations
    * - For FQQN addresses, converts wildcards in the address part only
    * - For virtual topic consumer queues, returns the stripped address
    * - For other addresses, returns the wildcard-converted pattern
    *
    * @param destination the ActiveMQ destination to convert
    * @param server      the ActiveMQ server for wildcard configuration
    * @return the core consume pattern, or null if the destination is null
    */
   public static String toCoreConsumePattern(ActiveMQDestination destination, ActiveMQServer server) {
      if (destination == null) {
         return null;
      }

      if (destination.isTemporary() || AdvisorySupport.isAdvisoryTopic(destination)) {
         return destination.getPhysicalName();
      }

      String strippedAddress = stripAddressScheme(destination.getPhysicalName(), destination);

      if (isFqqn(strippedAddress)) {
         return handleFqqn(strippedAddress, addr -> convertWildcard(addr, server));
      }

      if (destination.isQueue() && isVirtualTopicConsumerName(strippedAddress)) {
         return strippedAddress;
      }

      return convertWildcard(strippedAddress, server);
   }

   private static String stripAddressScheme(String address, ActiveMQDestination destination) {
      if (address == null || !address.contains(SCHEME_SEPARATOR)) {
         return address;
      }

      int schemeIndex = address.indexOf(SCHEME_SEPARATOR);
      String scheme = address.substring(0, schemeIndex).toLowerCase();

      if (destination == null || (destination.isQueue() && QUEUE_SCHEME.equals(scheme)) || (destination.isTopic() && TOPIC_SCHEME.equals(scheme))) {
         return address.substring(schemeIndex + SCHEME_SEPARATOR.length());
      }

      return address;
   }

   private static boolean isVirtualTopicConsumerName(String address) {
      if (address == null || isFqqn(address)) {
         return false;
      }

      String normalizedAddr = stripAddressScheme(address, null);
      return normalizedAddr.startsWith(CONSUMER_PREFIX) && normalizedAddr.contains(VIRTUAL_TOPIC_MARKER);
   }

   private static String handleFqqn(String address, Function<String, String> addressConverter) {
      SimpleString fullAddress = SimpleString.of(address);
      // Extract address and queue parts from the FQQN
      SimpleString addr = CompositeAddress.extractAddressName(fullAddress);
      SimpleString queue = CompositeAddress.extractQueueName(fullAddress);
      // Apply the converter function to the address part
      String convertedAddr = addressConverter.apply(addr.toString());
      // Reconstruct the FQQN with the converted address
      return CompositeAddress.toFullyQualified(SimpleString.of(convertedAddr), queue).toString();
   }

   private static String escapeBackslashes(String input) {
      return input == null ? null : input.replace("\\", "\\\\");
   }

   private static String convertWildcard(String address, ActiveMQServer server) {
      return OPENWIRE_WILDCARD.convert(address, server.getConfiguration().getWildcardConfiguration());
   }

   private static boolean isFqqn(String address) {
      if (address != null) {
         return CompositeAddress.isFullyQualified(SimpleString.of(address));
      }
      return false;
   }
}
