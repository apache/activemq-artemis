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
package org.apache.activemq.artemis.core.server;

import java.util.List;

import org.apache.activemq.artemis.api.config.ActiveMQDefaultConfiguration;
import org.apache.activemq.artemis.core.PriorityAware;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.spi.core.protocol.SessionCallback;

public interface Consumer extends PriorityAware {

   interface GroupHandler {
      MessageReference handleMessageGroup(MessageReference ref, Consumer consumer, boolean newGroup);
   }

   /**
    *
    * @see SessionCallback#supportsDirectDelivery()
    */
   default boolean supportsDirectDelivery() {
      return true;
   }

   /**

    *
    * @param reference
    * @return
    * @throws Exception
    */
   HandleStatus handle(MessageReference reference) throws Exception;

   /**
    * This will return {@link HandleStatus#BUSY} if busy, {@link HandleStatus#NO_MATCH} if no match, or the MessageReference is handled
    * This should return busy if handle is called before proceed deliver is called
    * @param groupHandler
    * @param reference
    * @return
    * @throws Exception
    */
   default Object handleWithGroup(GroupHandler groupHandler, boolean newGroup, MessageReference reference) throws Exception {
      return handle(reference);
   }

   /** wakes up internal threads to deliver more messages */
   default void promptDelivery() {
   }

   /**
    * This will called after delivery
    * Giving protocols a chance to complete their deliveries doing things such as individualACK outside of main locks
    *
    * @throws Exception
    */
   void afterDeliver(MessageReference reference) throws Exception;

   Filter getFilter();

   /**
    * @return the list of messages being delivered
    */
   List<MessageReference> getDeliveringMessages();

   String debug();

   /**
    * This method will create a string representation meant for management operations.
    * This is different from the toString method that's meant for debugging and will contain information that regular users won't understand well
    *
    * @return
    */
   String toManagementString();

   /**
    * disconnect the consumer
    */
   void disconnect();

   /** an unique sequential ID for this consumer */
   long sequentialID();

   @Override
   default int getPriority() {
      return ActiveMQDefaultConfiguration.getDefaultConsumerPriority();
   }

   default void errorProcessing(Throwable e, MessageReference reference) {

   }
}
