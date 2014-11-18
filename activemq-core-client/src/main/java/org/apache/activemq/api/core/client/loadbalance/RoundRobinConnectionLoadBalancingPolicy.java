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
package org.apache.activemq.api.core.client.loadbalance;

import java.io.Serializable;

import org.apache.activemq.utils.Random;

/**
 * RoundRobinConnectionLoadBalancingPolicy corresponds to a round-robin load-balancing policy.
 *
 * <br>
 * The first call to {@link #select(int)} will return a random integer between {@code 0} (inclusive) and {@code max} (exclusive).
 * Subsequent calls will then return an integer in a round-robin fashion.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * Created 28 Nov 2008 10:21:08
 *
 *
 */
public final class RoundRobinConnectionLoadBalancingPolicy implements ConnectionLoadBalancingPolicy, Serializable
{
   private static final long serialVersionUID = 7511196010141439559L;

   private final Random random = new Random();

   private boolean first = true;

   private int pos;

   public int select(final int max)
   {
      if (first)
      {
         // We start on a random one
         pos = random.getRandom().nextInt(max);

         first = false;
      }
      else
      {
         pos++;

         if (pos >= max)
         {
            pos = 0;
         }
      }

      return pos;
   }
}
