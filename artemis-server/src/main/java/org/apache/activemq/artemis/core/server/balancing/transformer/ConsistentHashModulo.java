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

package org.apache.activemq.artemis.core.server.balancing.transformer;

import java.util.Map;

import org.apache.activemq.artemis.core.server.balancing.policies.ConsistentHashPolicy;
import org.apache.activemq.artemis.core.server.balancing.targets.TargetKeyResolver;

public class ConsistentHashModulo implements KeyTransformer {
   public static final String NAME = "CONSISTENT_HASH_MODULO";
   public static final String MODULO = "modulo";
   int modulo = 0;

   @Override
   public String transform(String str) {
      if (TargetKeyResolver.DEFAULT_KEY_VALUE.equals(str)) {
         // we only want to transform resolved keys
         return str;
      }
      if (modulo == 0) {
         return str;
      }
      int hash = ConsistentHashPolicy.getHash(str);
      return String.valueOf( hash % modulo );
   }

   @Override
   public void init(Map<String, String> properties) {
      modulo = Integer.parseInt(properties.get(MODULO));
   }
}
