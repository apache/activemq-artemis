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

package org.apache.activemq.artemis.core.protocol.core.impl;

import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.TopologyMember;

/**
 * This is a utility class to house any HornetQ client specific backwards compatibility methods.
 */
public class BackwardsCompatibilityUtils {

   private static int INITIAL_ACTIVEMQ_INCREMENTING_VERSION = 126;

   public static Pair<TransportConfiguration, TransportConfiguration> getTCPair(int clientIncrementingVersion,
                                                                                TopologyMember member) {
      if (clientIncrementingVersion < INITIAL_ACTIVEMQ_INCREMENTING_VERSION) {
         return new Pair<>(replaceClassName(member.getLive()), replaceClassName(member.getBackup()));
      }
      return new Pair<>(member.getLive(), member.getBackup());
   }

   private static TransportConfiguration replaceClassName(TransportConfiguration tc) {
      if (tc != null) {
         String className = tc.getFactoryClassName().replace("org.apache.activemq.artemis", "org.hornetq").replace("ActiveMQ", "HornetQ");
         return new TransportConfiguration(className, tc.getParams(), tc.getName());
      }
      return tc;
   }
}
