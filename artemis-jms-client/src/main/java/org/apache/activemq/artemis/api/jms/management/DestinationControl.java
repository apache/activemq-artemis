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
package org.apache.activemq.artemis.api.jms.management;

import javax.management.MBeanOperationInfo;

import org.apache.activemq.artemis.api.core.management.Operation;
import org.apache.activemq.artemis.api.core.management.Parameter;

/**
 * A DestinationControl is used to manage a JMS Destination.
 */
public interface DestinationControl
{
   // Attributes ----------------------------------------------------

   /**
    * Returns the name of this destination.
    */
   String getName();

   /**
    * Returns the ActiveMQ Artemis address corresponding to this destination.
    */
   String getAddress();

   /**
    * Returns whether this destination is temporary.
    */
   boolean isTemporary();

   /**
    * Returns the number of messages currently in this destination.
    */
   long getMessageCount() throws Exception;

   /**
    * Returns the number of messages that this queue is currently delivering to its consumers.
    */
   int getDeliveringCount();

   /**
    * Returns the number of messages added to this queue since it was created.
    */
   long getMessagesAdded();

   // Operations ----------------------------------------------------

   /**
    * Removed all the messages which matches the specified JMS filter from this destination.
    * <br>
    * Using {@code null} or an empty filter will remove <em>all</em> messages from this destination.
    *
    * @return the number of removed messages
    */
   @Operation(desc = "Remove messages matching the given filter from the destination", impact = MBeanOperationInfo.ACTION)
   int removeMessages(@Parameter(name = "filter", desc = "A JMS message filter (can be empty)") String filter) throws Exception;

}