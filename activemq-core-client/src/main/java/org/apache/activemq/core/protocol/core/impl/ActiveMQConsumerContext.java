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

package org.apache.activemq.core.protocol.core.impl;

import org.apache.activemq.spi.core.remoting.ConsumerContext;

/**
 * @author Clebert Suconic
 */

public class ActiveMQConsumerContext extends ConsumerContext
{
   private long id;

   public ActiveMQConsumerContext(long id)
   {
      this.id = id;
   }

   public long getId()
   {
      return id;
   }

   @Override
   public boolean equals(Object o)
   {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ActiveMQConsumerContext that = (ActiveMQConsumerContext) o;

      if (id != that.id) return false;

      return true;
   }

   @Override
   public int hashCode()
   {
      return (int) (id ^ (id >>> 32));
   }
}
