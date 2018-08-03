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
package org.apache.activemq.artemis.api.core.management;

import java.util.Map;

/**
 * A BridgeControl is used to manage a Bridge.
 */
public interface BridgeControl extends ActiveMQComponentControl {

   /**
    * Returns the name of this bridge
    */
   @Attribute(desc = "name of this bridge")
   String getName();

   /**
    * Returns the name of the queue this bridge is consuming messages from.
    */
   @Attribute(desc = "name of the queue this bridge is consuming messages from")
   String getQueueName();

   /**
    * Returns the address this bridge will forward messages to.
    */
   @Attribute(desc = "address this bridge will forward messages to")
   String getForwardingAddress();

   /**
    * Returns the filter string associated with this bridge.
    */
   @Attribute(desc = "filter string associated with this bridge")
   String getFilterString();

   /**
    * Return the name of the org.apache.activemq.artemis.core.server.cluster.Transformer implementation associated with this bridge.
    */
   @Attribute(desc = "name of the org.apache.activemq.artemis.core.server.cluster.Transformer implementation associated with this bridge")
   String getTransformerClassName();

   /**
    * Returns a map of the properties configured for the transformer.
    */
   @Attribute(desc = "map of key, value pairs used to configure the transformer in JSON form")
   String getTransformerPropertiesAsJSON() throws Exception;

   /**
    * Returns a map of the properties configured for the transformer.
    */
   @Attribute(desc = "map of key, value pairs used to configure the transformer")
   Map<String, String> getTransformerProperties() throws Exception;

   /**
    * Returns any list of static connectors used by this bridge
    */
   @Attribute(desc = "list of static connectors used by this bridge")
   String[] getStaticConnectors() throws Exception;

   /**
    * Returns the name of the discovery group used by this bridge.
    */
   @Attribute(desc = "name of the discovery group used by this bridge")
   String getDiscoveryGroupName();

   /**
    * Returns the retry interval used by this bridge.
    */
   @Attribute(desc = "retry interval used by this bridge")
   long getRetryInterval();

   /**
    * Returns the retry interval multiplier used by this bridge.
    */
   @Attribute(desc = "retry interval multiplier used by this bridge")
   double getRetryIntervalMultiplier();

   /**
    * Returns the number of reconnection attempts used by this bridge.
    */
   @Attribute(desc = "number of reconnection attempts used by this bridge")
   int getReconnectAttempts();

   /**
    * Returns whether this bridge is using duplicate detection.
    */
   @Attribute(desc = "whether this bridge is using duplicate detection")
   boolean isUseDuplicateDetection();

   /**
    * Returns whether this bridge is using high availability
    */
   @Attribute(desc = "whether this bridge is using high availability")
   boolean isHA();

   /**
    * The messagesPendingAcknowledgement counter is incremented when the bridge is has forwarded a message but
    * is waiting acknowledgement from the other broker. This is a cumulative total and the number of outstanding
    * pending messages can be computed by subtracting messagesAcknowledged from messagesPendingAcknowledgement.
    *
    */
   @Attribute(desc = "The messagesPendingAcknowledgement counter is incremented when the bridge is has forwarded a message but is waiting acknowledgement from the remote broker.")
   long getMessagesPendingAcknowledgement();

   /**
    * The messagesAcknowledged counter is the number of messages actually received by the remote broker.
    * This is a cumulative total and the number of outstanding pending messages can be computed by subtracting
    * messagesAcknowledged from messagesPendingAcknowledgement.
    *
    */
   @Attribute(desc = "The messagesAcknowledged counter is the number of messages actually received by the remote broker.")
   long getMessagesAcknowledged();

   /**
    * The bridge metrics for this bridge
    *
    * The messagesPendingAcknowledgement counter is incremented when the bridge is has forwarded a message but is waiting acknowledgement from the other broker.
    * The messagesAcknowledged counter is the number of messages actually received by the remote broker.
    *
    */
   @Attribute(desc = "The metrics for this bridge. The messagesPendingAcknowledgement counter is incremented when the bridge is has forwarded a message but is waiting acknowledgement from the remote broker. The messagesAcknowledged counter is the number of messages actually received by the remote broker.")
   Map<String, Object> getMetrics();

}
