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


/**
 * A reference to a message.
 *
 * Channels store message references rather than the messages themselves.
 */
public interface MessageReference
{

   boolean isPaged();

   ServerMessage getMessage();

   /**
    * We define this method aggregation here because on paging we need to hold the original estimate,
    * so we need to perform some extra steps on paging.
    * @return
    */
   int getMessageMemoryEstimate();

   MessageReference copy(Queue queue);

   /**
    *
    * @return The time in the future that delivery will be delayed until, or zero if
    * no scheduled delivery will occur
    */
   long getScheduledDeliveryTime();

   void setScheduledDeliveryTime(long scheduledDeliveryTime);

   int getDeliveryCount();

   void setDeliveryCount(int deliveryCount);

   void setPersistedCount(int deliveryCount);

   int getPersistedCount();

   void incrementDeliveryCount();

   void decrementDeliveryCount();

   Queue getQueue();

   void acknowledge() throws Exception;

   void setConsumerId(Long consumerID);

   Long getConsumerId();

   void handled();

   void setAlreadyAcked();

   boolean isAlreadyAcked();
}
