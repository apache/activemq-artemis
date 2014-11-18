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

import org.apache.activemq.utils.Random;

/**
 * {@link RandomConnectionLoadBalancingPolicy#select(int)} returns a (pseudo) random integer between
 * {@code 0} (inclusive) and {@code max} (exclusive).
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a> Created 28 Nov 2008 10:24:11
 */
public final class RandomConnectionLoadBalancingPolicy implements ConnectionLoadBalancingPolicy
{
   private final Random random = new Random();

   /**
    * Returns a pseudo random number between {@code 0} (inclusive) and {@code max} exclusive.
    * @param max the upper limit of the random number selection
    * @see java.util.Random#nextInt(int)
    */
   public int select(final int max)
   {
      return random.getRandom().nextInt(max);
   }
}
