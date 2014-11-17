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

package org.apache.activemq6.core.client.impl;

import org.apache.activemq6.api.core.SimpleString;
import org.apache.activemq6.api.core.client.ClientSession;

public class QueueQueryImpl implements ClientSession.QueueQuery
{

   private final boolean exists;

   private final boolean durable;

   private final boolean temporary;

   private final long messageCount;

   private final SimpleString filterString;

   private final int consumerCount;

   private final SimpleString address;

   private final SimpleString name;

   public QueueQueryImpl(final boolean durable,
                         final boolean temporary,
                         final int consumerCount,
                         final long messageCount,
                         final SimpleString filterString,
                         final SimpleString address,
                         final SimpleString name,
                         final boolean exists)
   {

      this.durable = durable;
      this.temporary = temporary;
      this.consumerCount = consumerCount;
      this.messageCount = messageCount;
      this.filterString = filterString;
      this.address = address;
      this.name = name;
      this.exists = exists;
   }

   public SimpleString getName()
   {
      return name;
   }

   public SimpleString getAddress()
   {
      return address;
   }

   public int getConsumerCount()
   {
      return consumerCount;
   }

   public SimpleString getFilterString()
   {
      return filterString;
   }

   public long getMessageCount()
   {
      return messageCount;
   }

   public boolean isDurable()
   {
      return durable;
   }

   public boolean isTemporary()
   {
      return temporary;
   }

   public boolean isExists()
   {
      return exists;
   }

}

