/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.activemq.api.core.management;


/**
 * A BridgeControl is used to manage a Bridge.
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 *
 */
public interface BridgeControl extends ActiveMQComponentControl
{
   /**
    * Returns the name of this bridge
    */
   String getName();

   /**
    * Returns the name of the queue this bridge is consuming messages from.
    */
   String getQueueName();

   /**
    * Returns the address this bridge will forward messages to.
    */
   String getForwardingAddress();

   /**
    * Returns the filter string associated to this bridge.
    */
   String getFilterString();

   /**
    * Return the name of the org.apache.activemq.core.server.cluster.Transformer implementation associated to this bridge.
    */
   String getTransformerClassName();

   /**
    * Returns any list of static connectors used by this bridge
    */
   String[] getStaticConnectors() throws Exception;

   /**
    * Returns the name of the discovery group used by this bridge.
    */
   String getDiscoveryGroupName();

   /**
    * Returns the retry interval used by this bridge.
    */
   long getRetryInterval();

   /**
    * Returns the retry interval multiplier used by this bridge.
    */
   double getRetryIntervalMultiplier();

   /**
    * Returns the number of reconnection attempts used by this bridge.
    */
   int getReconnectAttempts();

   /**
    * Returns whether this bridge is using duplicate detection.
    */
   boolean isUseDuplicateDetection();

   /**
    * Returns whether this bridge is using high availability
    */
   boolean isHA();
}
