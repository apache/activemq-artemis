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
package org.apache.activemq.core.server;

import org.apache.activemq.api.core.SimpleString;

/**
 *
 * A QueueQueryResult
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class QueueQueryResult
{
   private SimpleString name;

   private boolean exists;

   private boolean durable;

   private int consumerCount;

   private long messageCount;

   private SimpleString filterString;

   private SimpleString address;

   private boolean temporary;

   public QueueQueryResult(final SimpleString name,
                                           final SimpleString address,
                                           final boolean durable,
                                           final boolean temporary,
                                           final SimpleString filterString,
                                           final int consumerCount,
                                           final long messageCount)
   {
      this(name, address, durable, temporary, filterString, consumerCount, messageCount, true);
   }

   public QueueQueryResult()
   {
      this(null, null, false, false, null, 0, 0, false);
   }

   private QueueQueryResult(final SimpleString name,
                                            final SimpleString address,
                                            final boolean durable,
                                            final boolean temporary,
                                            final SimpleString filterString,
                                            final int consumerCount,
                                            final long messageCount,
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

   public boolean isExists()
   {
      return exists;
   }

   public boolean isDurable()
   {
      return durable;
   }

   public int getConsumerCount()
   {
      return consumerCount;
   }

   public long getMessageCount()
   {
      return messageCount;
   }

   public SimpleString getFilterString()
   {
      return filterString;
   }

   public SimpleString getAddress()
   {
      return address;
   }

   public SimpleString getName()
   {
      return name;
   }

   public boolean isTemporary()
   {
      return temporary;
   }

}
