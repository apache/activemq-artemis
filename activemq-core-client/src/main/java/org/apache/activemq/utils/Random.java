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
package org.apache.activemq.utils;

import java.io.Serializable;

/**
 * A Random
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * Created 28 Nov 2008 10:28:28
 *
 *
 */
public class Random implements Serializable
{
   private static int extraSeed;

   private static final long serialVersionUID = 40335522290950498L;

   private static synchronized long getSeed()
   {
      long seed = System.currentTimeMillis() + Random.extraSeed++;

      return seed;
   }

   private final java.util.Random random = new java.util.Random(Random.getSeed());

   public java.util.Random getRandom()
   {
      return random;
   }

}
