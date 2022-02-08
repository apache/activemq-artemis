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

package org.apache.activemq.artemis.core.config.balancing;

import java.io.Serializable;

public class CacheConfiguration implements Serializable {
   private boolean persisted = false;

   private int timeout = 0;

   public CacheConfiguration() {
   }

   public boolean isPersisted() {
      return persisted;
   }

   public CacheConfiguration setPersisted(boolean persisted) {
      this.persisted = persisted;
      return this;
   }

   public int getTimeout() {
      return timeout;
   }

   public CacheConfiguration setTimeout(int timeout) {
      this.timeout = timeout;
      return this;
   }
}
