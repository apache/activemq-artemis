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
package org.apache.activemq.artemis.core.config;

import java.io.Serializable;

public interface HAPolicyConfiguration extends Serializable {

   enum TYPE {
      PRIMARY_ONLY("Primary Only"),
      REPLICATED("Replicated"),
      REPLICA("Replica"),
      SHARED_STORE_PRIMARY("Shared Store Primary"),
      SHARED_STORE_BACKUP("Shared Store Backup"),
      COLOCATED("Colocated"),
      REPLICATION_PRIMARY("Replication Primary w/pluggable quorum voting"),
      REPLICATION_BACKUP("Replication Backup w/pluggable quorum voting");

      private String name;

      TYPE(String name) {
         this.name = name;
      }

      public String getName() {
         return name;
      }
   }

   TYPE getType();
}
