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

/**
 * A BridgeControl is used to manage a federation stream.
 */
public interface FederationRemoteConsumerControl {

   /**
    * Returns the name of the queue that is being federated too
    */
   @Attribute(desc = "name of the queue that is being federated too")
   String getQueueName();

   /**
    * Returns the address this remote consumer will forward messages from.
    */
   @Attribute(desc = "address this remote consumer will forward messages from")
   String getAddress();

   /**
    * Returns the priority of this remote consumer will consumer messages.
    */
   @Attribute(desc = "address this remote consumer will consumer messages")
   int getPriority();

   /**
    * Returns the routing type associated with this address.
    */
   @Attribute(desc = "routing type for this address")
   String getRoutingType();

   /**
    * Returns the filter string associated with this remote consumer.
    */
   @Attribute(desc = "filter string associated with this remote consumer")
   String getFilterString();

   /**
    * Returns the queue filter string associated with this remote consumer.
    */
   @Attribute(desc = "queue filter string associated with this remote consumer")
   String getQueueFilterString();

   /**
    * Returns the fully qualified queue name associated with this remote consumer.
    */
//   @Attribute(desc = "fully qualified queue name associated with this remote consumer")
//   String getFqqn();

   /**
    * Returns the number of messages that have been federated for this address.
    */
   @Attribute(desc = "number of messages that have been federated for this address")
   long getFederatedMessageCount();
}
