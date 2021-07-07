/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.core.server.balancing.policies;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class DefaultPolicyFactory extends PolicyFactory {
   private static final Map<String, Supplier<AbstractPolicy>> supportedPolicies = new HashMap<>();

   static {
      supportedPolicies.put(ConsistentHashPolicy.NAME, () -> new ConsistentHashPolicy());
      supportedPolicies.put(FirstElementPolicy.NAME, () -> new FirstElementPolicy());
      supportedPolicies.put(LeastConnectionsPolicy.NAME, () -> new LeastConnectionsPolicy());
      supportedPolicies.put(RoundRobinPolicy.NAME, () -> new RoundRobinPolicy());
   }

   @Override
   public String[] getSupportedPolicies() {
      return supportedPolicies.keySet().toArray(new String[supportedPolicies.size()]);
   }

   @Override
   public AbstractPolicy createPolicy(String policyName) {
      Supplier<AbstractPolicy> policySupplier = supportedPolicies.get(policyName);

      if (policySupplier == null) {
         throw new IllegalArgumentException("Policy not supported: " + policyName);
      }

      return policySupplier.get();
   }
}
