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

import org.apache.activemq.artemis.core.server.balancing.BrokerBalancer;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

public class PolicyFactoryResolver {
   private static PolicyFactoryResolver instance;

   public static PolicyFactoryResolver getInstance() {
      if (instance == null) {
         instance = new PolicyFactoryResolver();
      }
      return instance;
   }

   private final Map<String, PolicyFactory> policyFactories = new HashMap<>();

   private PolicyFactoryResolver() {
      registerPolicyFactory(new DefaultPolicyFactory());

      loadPolicyFactories();
   }

   public PolicyFactory resolve(String policyName) throws ClassNotFoundException {
      PolicyFactory policyFactory = policyFactories.get(policyName);

      if (policyFactory == null) {
         throw new ClassNotFoundException("No PolicyFactory found for the policy " + policyName);
      }

      return policyFactory;
   }

   private void loadPolicyFactories() {
      ServiceLoader<PolicyFactory> serviceLoader = ServiceLoader.load(
         PolicyFactory.class, BrokerBalancer.class.getClassLoader());

      for (PolicyFactory policyFactory : serviceLoader) {
         registerPolicyFactory(policyFactory);
      }
   }

   public void registerPolicyFactory(PolicyFactory policyFactory) {
      for (String policyName : policyFactory.getSupportedPolicies()) {
         policyFactories.put(policyName, policyFactory);
      }
   }

   public void unregisterPolicyFactory(PolicyFactory policyFactory) {
      for (String policyName : policyFactory.getSupportedPolicies()) {
         policyFactories.remove(policyName, policyFactory);
      }
   }
}
