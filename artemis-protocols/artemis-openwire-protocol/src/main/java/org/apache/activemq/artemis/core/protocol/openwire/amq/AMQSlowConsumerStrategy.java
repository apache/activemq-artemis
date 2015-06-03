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
package org.apache.activemq.artemis.core.protocol.openwire.amq;

public interface AMQSlowConsumerStrategy
{

   /**
    * Slow consumer event.
    *
    * @param context
    *      Connection context of the subscription.
    * @param subs
    *      The subscription object for the slow consumer.
    */
   void slowConsumer(AMQConnectionContext context, AMQSubscription subs);

   /**
    * For Strategies that need to examine assigned destination for slow consumers
    * periodically the destination is assigned here.
    *
    * If the strategy doesn't is event driven it can just ignore assigned destination.
    *
    * @param destination
    *      A destination to add to a watch list.
    */
   void addDestination(AMQDestination destination);

}
